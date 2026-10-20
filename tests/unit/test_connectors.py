import json
import threading
from datetime import datetime

import pytest

from backend.connectors import ENGINES, create_connector
from backend.connectors.base import ConnectorError, Operation, Position, keyset_params, keyset_predicate
from backend.connectors.mariadb import MariaDBConnector, binlog_key
from backend.connectors.mongodb import MongoConnector
from backend.connectors.mssql import MSSQLConnector
from backend.connectors.oracle import OracleConnector
from backend.connectors.oracle_redo import RedoParseError, parse_redo
from backend.connectors.postgresql import PostgresConnector, int_to_lsn, lsn_to_int
from backend.connectors.watermark import AdaptiveInterval
from tests.fakes import FakeDbApiConnection


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------
def test_registry_and_secret_resolution(monkeypatch):
    monkeypatch.setenv("PW", "x")
    c = create_connector({"engine": "postgres", "id": "a", "password": "${env:PW}"})
    assert isinstance(c, PostgresConnector) and c.config["password"] == "x"
    assert isinstance(create_connector({"id": "legacy"}), MSSQLConnector)   # engine defaults to mssql
    with pytest.raises(ConnectorError):
        create_connector({"engine": "db2"})
    assert set(ENGINES) >= {"mssql", "postgresql", "mariadb", "mongodb", "oracle"}


def test_keyset_predicate_forms():
    q = lambda s: f"[{s}]"
    sql, n = keyset_predicate(["a", "b"], q)
    assert sql == "(([a] > ?) OR ([a] = ? AND [b] > ?))" and n == 3
    assert keyset_params([1, 2]) == [1, 1, 2]
    sql, n = keyset_predicate(["a", "b"], q, "%s", row_values=True)
    assert sql == "([a], [b]) > (%s, %s)" and keyset_params([1, 2], row_values=True) == [1, 2]


def test_adaptive_interval_backoff():
    ai = AdaptiveInterval(0.25, 4.0)
    waits = [ai.idle() for _ in range(6)]
    assert waits == [0.25, 0.5, 1.0, 2.0, 4.0, 4.0]
    assert ai.busy() == 0.25 and ai.idle() == 0.25
    assert ai.error() >= 4.0 or ai.error() == ai.error_s
    with pytest.raises(ValueError):
        AdaptiveInterval(0, 1)


# ---------------------------------------------------------------------------
# MSSQL
# ---------------------------------------------------------------------------
LSN1, LSN2, LSN3 = bytes.fromhex("0000002a000001f80003"), bytes.fromhex("0000002a000002000001"), bytes.fromhex("0000002a000002100001")
SEQ1, SEQ2 = bytes.fromhex("0000002a000001f80002"), bytes.fromhex("0000002a000001f80004")


def mssql_with(responders):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = MSSQLConnector({"id": "ms", "server": "s", "database": "db", "username": "u", "password": "p"}, connection_factory=lambda self: conn)
    c.connect()
    return c, conn


def test_mssql_connection_string_and_redaction():
    c = MSSQLConnector({"server": "srv", "port": 1433, "database": "db", "username": "u", "password": "secret", "read_only_intent": True})
    cs = c.connection_string()
    assert "SERVER=srv,1433" in cs and "PWD=secret" in cs and "ApplicationIntent=ReadOnly" in cs and "Connection Timeout=15" in cs
    assert "secret" not in c.connection_string(redact=True)
    assert "Trusted_Connection=yes" in MSSQLConnector({"use_windows_auth": True}).connection_string()


def test_mssql_read_changes_uses_increment_and_seqval_resume():
    change_rows = [
        {"__$start_lsn": LSN2, "__$seqval": SEQ1, "__$operation": 2, "__$update_mask": b"\x01", "Id": 1, "Name": "a", "tran_end_time": datetime(2024, 1, 1)},
        {"__$start_lsn": LSN2, "__$seqval": SEQ2, "__$operation": 4, "__$update_mask": b"\x01", "Id": 1, "Name": "b", "tran_end_time": datetime(2024, 1, 1)},
        {"__$start_lsn": LSN3, "__$seqval": SEQ1, "__$operation": 1, "__$update_mask": b"\x01", "Id": 1, "Name": "b", "tran_end_time": datetime(2024, 1, 2)},
    ]
    cols = list(change_rows[0].keys())
    c, conn = mssql_with([
        (r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN3]])),
        (r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN1]])),
        (r"fn_cdc_increment_lsn", (["nxt"], [[LSN2]])),
        (r"fn_cdc_get_all_changes_dbo_T", lambda sql, p: (cols, [[r[k] for k in cols] for r in change_rows])),
    ])
    events, commit = c.read_changes([("dbo", "T")], Position("mssql_lsn", LSN1.hex()))
    assert [e.operation for e in events] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert events[0].after == {"Id": 1, "Name": "a"} and events[2].before == {"Id": 1, "Name": "b"} and events[2].after is None
    assert events[1].position.value == LSN2.hex() and events[1].position.extra["seqval"] == SEQ2.hex()
    assert commit.value == LSN3.hex()
    assert conn.sql_calls("fn_cdc_increment_lsn")             # start = increment(last)
    read_call = conn.sql_calls("fn_cdc_get_all_changes")[0]
    assert read_call[1][0] == LSN2 and read_call[1][1] == LSN3 and read_call[1][2] == "all"
    # resume inside an LSN: no increment, seqval filter present
    conn.calls.clear()
    c.read_changes([("dbo", "T")], Position("mssql_lsn", LSN2.hex(), {"seqval": SEQ1.hex()}))
    assert not conn.sql_calls("fn_cdc_increment_lsn")
    call = conn.sql_calls("fn_cdc_get_all_changes")[0]
    assert "__\\$seqval <= ?" in call[0].replace("$", "\\$") and call[1][-1] == SEQ1


def test_mssql_purged_position_raises_instead_of_skipping():
    c, conn = mssql_with([
        (r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN3]])),
        (r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN2]])),      # oldest retained change is newer than our position
    ])
    with pytest.raises(ConnectorError, match="older than the oldest retained change"):
        c.read_changes([("dbo", "T")], Position("mssql_lsn", LSN1.hex()))
    assert not conn.sql_calls("fn_cdc_get_all_changes")


def test_mssql_read_changes_limit_returns_last_position_not_watermark():
    rows = [{"__$start_lsn": LSN2, "__$seqval": SEQ1, "__$operation": 2, "__$update_mask": None, "Id": i, "tran_end_time": None} for i in range(3)]
    cols = list(rows[0].keys())
    c, conn = mssql_with([
        (r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN3]])),
        (r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN1]])),
        (r"fn_cdc_get_all_changes", lambda sql, p: (cols, [[r[k] for k in cols] for r in rows[:2]])),
    ])
    events, commit = c.read_changes([("dbo", "T")], None, limit=2)
    assert len(events) == 2 and commit.value == LSN2.hex() and commit.extra["seqval"] == SEQ1.hex()
    assert "TOP (2)" in conn.sql_calls("fn_cdc_get_all_changes")[0][0]


def test_mssql_changed_capture_instances_single_roundtrip():
    c, conn = mssql_with([(r"UNION ALL", (["tbl", "n"], [["dbo.A", 1], ["dbo.B", 0]]))])
    c.capture_instances["dbo.B"] = "custom_B"
    res = c.changed_capture_instances([("dbo", "A"), ("dbo", "B")], {"dbo.A": Position("mssql_lsn", LSN1.hex()), "dbo.B": None})
    assert res == {"dbo.A": 1}
    sql, params = conn.calls[-1]
    assert sql.count("UNION ALL") == 1 and "[dbo_A_CT]" in sql and "[custom_B_CT]" in sql and list(params) == [LSN1]


def test_mssql_prerequisites_report_problems():
    c, conn = mssql_with([
        (r"@@VERSION", (["v", "edition", "pv"], [["x", "Express Edition", "15.0"]])),
        (r"EngineEdition", (["ee", "ed", "major"], [[4, "Express Edition", 15]])),
        (r"dm_server_services", (["servicename", "status_desc"], [["SQL Server Agent (MSSQLSERVER)", "Stopped"]])),
        (r"IS_SRVROLEMEMBER", (["sa", "dbo"], [[0, 1]])),
        (r"FROM sys.databases", (["is_cdc_enabled", "recovery_model_desc", "is_read_committed_snapshot_on", "snapshot_isolation_state_desc"],
                                 [[1, "SIMPLE", 0, "OFF"]])),
        (r"msdb.dbo.cdc_jobs", (["job_type", "retention", "threshold", "pollinginterval", "maxscans", "maxtrans"],
                                [["capture", None, 5000, 5, 10, 500], ["cleanup", 60, 5000, None, None, None]])),
        (r"dm_cdc_log_scan_sessions", (["end_time", "latency", "error_count", "empty_scan_count", "failed_sessions_count"], [[None, 1, 0, 0, 0]])),
        (r"dm_cdc_errors", (["n"], [[0]])),
        (r"FROM sys.columns", (["name", "data_type", "is_nullable", "max_length", "column_id", "is_pk"],
                               [["Id", "int", 0, 4, 1, 0], ["Doc", "xml", 1, -1, 2, 0]])),
        (r"is_tracked_by_cdc FROM sys.tables", (["is_tracked_by_cdc"], [[1]])),
        (r"cdc.change_tables", (["capture_instance", "supports_net_changes", "start_lsn", "create_date"], [["other_ci", 0, LSN1, None]])),
    ])
    checks = {ch.name: ch for ch in c.check_prerequisites([("dbo", "T")])}
    assert not checks["edition_supports_cdc"].ok
    assert not checks["sql_agent_running"].ok
    assert not checks["permission_sysadmin"].ok and checks["permission_db_owner"].ok
    assert checks["database_cdc_enabled"].ok and checks["recovery_model"].severity == "warning"
    assert not checks["read_committed_snapshot"].ok
    assert not checks["cdc_retention"].ok and "60 min" in checks["cdc_retention"].detail
    assert not checks["primary_key:dbo.T"].ok
    assert not checks["capture_instance_name:dbo.T"].ok
    assert checks["lob_columns:dbo.T"].severity == "info"


def test_mssql_enable_and_status_and_snapshot_chunk():
    c, conn = mssql_with([
        (r"is_tracked_by_cdc FROM sys.tables", (["is_tracked_by_cdc"], [[0]])),
        (r"FROM sys.databases", (["is_cdc_enabled"], [[1]])),
        (r"cdc.change_tables", (["source_schema", "source_table", "capture_instance", "supports_net_changes", "create_date", "start_lsn"],
                                [["dbo", "T", "dbo_T", 0, None, LSN1]])),
        (r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN3]])),
        (r"SELECT TOP \(100\)", (["Id", "Name"], [[1, "a"], [2, "b"]])),
        (r"dm_db_partition_stats", (["n"], [[12345]])),
    ])
    ok, msg = c.enable_capture_table("dbo", "T", supports_net_changes=True, captured_column_list="Id,Name")
    assert ok and "dbo_T" in msg
    sql, params = conn.sql_calls("sp_cdc_enable_table")[0]
    assert list(params) == ["dbo", "T", None, "dbo_T", 1, "Id,Name"]
    st = c.capture_status()
    assert st["cdc_enabled"] and st["tables"][0]["start_lsn"] == LSN1.hex() and st["max_lsn"] == LSN3.hex()
    rows = c.read_snapshot_chunk("dbo", "T", ["Id", "Name"], ["Id"], [10], 100, where="Active = 1")
    assert rows == [{"Id": 1, "Name": "a"}, {"Id": 2, "Name": "b"}]
    sql, params = conn.sql_calls("SELECT TOP")[0]
    assert "WHERE (([Id] > ?)) AND (Active = 1) ORDER BY [Id]" in sql and list(params) == [10]
    assert c.count_rows("dbo", "T") == 12345
    assert "cdcsync_notifier" in MSSQLConnector.install_service_broker_sql()
    assert c.disable_capture_table("dbo", "T")[0]


def test_mssql_stream_changes_uses_watermark_then_probe():
    calls = {"wm": 0}
    rows = [{"__$start_lsn": LSN2, "__$seqval": SEQ1, "__$operation": 2, "__$update_mask": None, "Id": 1, "tran_end_time": None}]
    cols = list(rows[0].keys())

    def max_lsn(sql, p):
        calls["wm"] += 1
        return (["max_lsn"], [[LSN2 if calls["wm"] < 3 else LSN3]])
    c, conn = mssql_with([
        (r"fn_cdc_get_max_lsn", max_lsn),
        (r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN1]])),
        (r"CASE WHEN EXISTS", lambda s, p: (["tbl", "n"], [["dbo.T", 1 if calls["wm"] >= 3 else 0]])),
        (r"fn_cdc_increment_lsn", (["nxt"], [[LSN2]])),
        (r"fn_cdc_get_all_changes", lambda s, p: (cols, [[r[k] for k in cols] for r in rows])),
    ])
    c.config["poll_min_seconds"] = 0.001
    c.config["poll_max_seconds"] = 0.002
    stop = threading.Event()
    got = []
    for ev in c.stream_changes([("dbo", "T")], {"dbo.T": Position("mssql_lsn", LSN1.hex())}, stop):
        got.append(ev)
        stop.set()
    assert len(got) == 1 and got[0].operation == Operation.INSERT
    assert calls["wm"] >= 3 and len(conn.sql_calls("fn_cdc_get_all_changes")) == 1


def test_mssql_retention_status_flags_purged_position():
    c, conn = mssql_with([
        (r"msdb.dbo.cdc_jobs", (["job_type", "retention"], [["cleanup", 4320]])),
        (r"dm_cdc_errors", (["n"], [[2]])),
        (r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN2]])),
        (r"fn_cdc_map_lsn_to_time", (["t"], [[datetime.now()]])),
    ])
    st = c.retention_status({"dbo.T": Position("mssql_lsn", LSN1.hex())})
    assert st["position_before_min"] is True and st["capture_errors"] == 2 and st["retention_minutes"] == 4320
    assert st["retention_margin_minutes"] is not None and st["retention_margin_minutes"] <= 4320


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
def pg_with(responders, **cfg):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = PostgresConnector({"id": "pg", "database": "db", **cfg}, connection_factory=lambda self: conn)
    c.connect()
    return c, conn


def test_pg_lsn_math():
    assert lsn_to_int("0/16B3748") == 0x16B3748 and lsn_to_int("1/0") == 1 << 32
    assert int_to_lsn(lsn_to_int("A/1F")) == "A/1F"


def test_pg_wal2json_decoding_and_slot_advance():
    msgs = [
        ("0/100", 10, json.dumps({"action": "B", "xid": 10, "timestamp": "2024-01-01 10:00:00.5+00"})),
        ("0/110", 10, json.dumps({"action": "I", "schema": "public", "table": "t", "columns": [{"name": "id", "value": 1}, {"name": "n", "value": "a"}],
                                  "pk": [{"name": "id"}]})),
        ("0/120", 10, json.dumps({"action": "U", "schema": "public", "table": "t", "columns": [{"name": "id", "value": 1}, {"name": "n", "value": "b"}],
                                  "identity": [{"name": "id", "value": 1}], "pk": [{"name": "id"}]})),
        ("0/130", 10, json.dumps({"action": "D", "schema": "public", "table": "t", "identity": [{"name": "id", "value": 1}], "pk": [{"name": "id"}]})),
        ("0/131", 10, json.dumps({"action": "I", "schema": "public", "table": "other", "columns": [{"name": "id", "value": 9}]})),
        ("0/140", 10, json.dumps({"action": "C", "xid": 10, "timestamp": "2024-01-01 10:00:01+00"})),
    ]
    c, conn = pg_with([(r"pg_logical_slot_peek_changes", (["lsn", "xid", "data"], [list(m) for m in msgs]))])
    events, commit = c.read_changes([("public", "t")], Position("pg_lsn", "0/100"))
    assert [e.operation for e in events] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert events[0].after == {"id": 1, "n": "a"} and events[0].key == {"id": 1} and events[0].source_ts.year == 2024
    assert events[1].before == {"id": 1} and events[2].after is None and events[2].key == {"id": 1}
    assert commit.value == "0/140"
    sql, params = conn.calls[-1]
    assert "add-tables" in params and "public.t" in params and params[0] == c.slot_name
    c.confirm_position(commit)
    assert conn.sql_calls("pg_replication_slot_advance")[0][1] == (c.slot_name, "0/140")


def test_pg_test_decoding_parser():
    c = PostgresConnector({"id": "pg", "plugin": "test_decoding"})
    r = c._parse_test_decoding("table public.t1: INSERT: id[integer]:1 name[text]:'O''Brien' flag[boolean]:true price[numeric]:9.5 x[text]:null")
    assert r["op"] == Operation.INSERT and r["after"] == {"id": 1, "name": "O'Brien", "flag": True, "price": 9.5, "x": None}
    r = c._parse_test_decoding("table public.t1: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'b'")
    assert r["op"] == Operation.UPDATE and r["before"] == {"id": 1} and r["after"] == {"id": 1, "name": "b"}
    r = c._parse_test_decoding("table public.t1: DELETE: id[integer]:1")
    assert r["op"] == Operation.DELETE and r["before"] == {"id": 1}
    assert c._parse_test_decoding("BEGIN 123 (at 2024-01-01 10:00:00+00)")["_tx_ts"].year == 2024
    assert c._parse_test_decoding("garbage") is None


def test_pg_prerequisites_and_capture_management():
    c, conn = pg_with([
        (r"version\(\)", (["v"], [["PostgreSQL 15.1"]])),
        (r"FROM pg_settings", (["name", "setting"], [["wal_level", "replica"], ["max_replication_slots", "10"], ["server_version_num", "150001"]])),
        (r"COUNT\(\*\) AS n FROM pg_replication_slots", (["n"], [[10]])),
        (r"SELECT 1 FROM pg_replication_slots", (["?column?"], [])),
        (r"rolsuper", (["rolsuper", "rolreplication"], [[False, False]])),
        (r"pg_attribute", (["name", "data_type", "nullable", "ordinal", "is_pk"], [["id", "integer", False, 1, False]])),
        (r"relreplident", (["relreplident"], [["n"]])),
    ])
    checks = {ch.name: ch for ch in c.check_prerequisites([("public", "t")])}
    assert not checks["wal_level_logical"].ok and not checks["replication_slots_available"].ok
    assert not checks["role_replication"].ok and not checks["slot_exists"].ok
    assert not checks["primary_key:public.t"].ok and not checks["replica_identity:public.t"].ok
    ok, msg = c.enable_capture_database()
    assert ok and conn.sql_calls("pg_create_logical_replication_slot")[0][1] == (c.slot_name, "wal2json")
    assert c.enable_capture_table("public", "t", replica_identity_full=True)[0]
    assert 'REPLICA IDENTITY FULL' in conn.calls[-1][0]
    conn.add(r"ORDER BY \"id\" LIMIT 10", (["id"], [[1]]))
    assert c.read_snapshot_chunk("public", "t", ["id"], ["id"], [5], 10) == [{"id": 1}]
    assert '("id") > (%s)' in conn.calls[-1][0] and conn.calls[-1][1] == (5,)


# ---------------------------------------------------------------------------
# MariaDB
# ---------------------------------------------------------------------------
class FakeBinlogEvent:
    def __init__(self, kind, schema, table, rows, ts=1704067200, pk="id"):
        self.schema, self.table, self.rows, self.timestamp, self.primary_key = schema, table, rows, ts, pk
        self.__class__ = type(kind, (FakeBinlogEvent,), {})


class FakeStream:
    def __init__(self, events, blocking=False):
        self.events, self.log_file, self.log_pos, self.closed = list(events), "mysql-bin.000002", 100, False
        self.blocking = blocking

    def __iter__(self):
        for e in self.events:
            self.log_pos += 10
            yield e
        if self.blocking:
            threading.Event().wait(0.2)

    def close(self):
        self.closed = True


def test_mariadb_binlog_mapping_poll_and_stream():
    events = [
        FakeBinlogEvent("WriteRowsEvent", "shop", "orders", [{"values": {"id": 1, "amount": 5}}]),
        FakeBinlogEvent("UpdateRowsEvent", "shop", "orders", [{"before_values": {"id": 1, "amount": 5}, "after_values": {"id": 1, "amount": 6}}]),
        FakeBinlogEvent("DeleteRowsEvent", "shop", "orders", [{"values": {"id": 1, "amount": 6}}]),
        FakeBinlogEvent("WriteRowsEvent", "shop", "ignored", [{"values": {"id": 9}}]),
    ]
    streams = []

    def factory(self, tables, from_position, blocking):
        s = FakeStream(events, blocking)
        streams.append((s, from_position))
        return s
    c = MariaDBConnector({"id": "mdb", "database": "shop"}, connection_factory=lambda self: FakeDbApiConnection(), stream_factory=factory)
    evs, commit = c.read_changes([("shop", "orders")], Position("binlog", {"file": "mysql-bin.000002", "pos": 4}))
    assert [e.operation for e in evs] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert evs[1].before == {"id": 1, "amount": 5} and evs[1].after["amount"] == 6 and evs[1].key == {"id": 1}
    assert commit.value == {"file": "mysql-bin.000002", "pos": 140} and streams[0][0].closed
    stop = threading.Event()
    got = []
    for ev in c.stream_changes([("shop", "orders")], None, stop):
        got.append(ev)
        if len(got) == 3:
            stop.set()
    assert len(got) == 3 and streams[-1][0].closed
    assert binlog_key({"file": "mysql-bin.000010", "pos": 5}) > binlog_key({"file": "mysql-bin.000009", "pos": 999})


def test_mariadb_prerequisites_and_position():
    conn = FakeDbApiConnection()
    vars_ = {"log_bin": "ON", "binlog_format": "STATEMENT", "binlog_row_image": "MINIMAL", "server_id": "4379",
             "binlog_expire_logs_seconds": "3600"}
    conn.add(r"SHOW GLOBAL VARIABLES LIKE '(\w+)'", lambda sql, p: (["Variable_name", "Value"],
             [[n, vars_[n]] for n in vars_ if f"'{n}'" in sql]))
    conn.add(r"VERSION\(\)", (["v"], [["10.11.6-MariaDB"]]))
    conn.add(r"SHOW GRANTS", (["Grants"], [["GRANT SELECT ON *.* TO u"]]))
    conn.add(r"SHOW BINARY LOG STATUS", lambda s, p: (_ for _ in ()).throw(RuntimeError("syntax")))
    conn.add(r"SHOW MASTER STATUS", (["File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"], [["mysql-bin.000003", 4321, "", ""]]))
    conn.add(r"information_schema.columns", (["column_name", "column_type", "is_nullable", "ordinal_position", "column_key", "character_maximum_length"],
                                              [["id", "int", "NO", 1, "", None]]))
    c = MariaDBConnector({"id": "mdb", "database": "shop"}, connection_factory=lambda self: conn)
    checks = {ch.name: ch for ch in c.check_prerequisites([("shop", "t")])}
    assert checks["log_bin"].ok and not checks["binlog_format_row"].ok and not checks["binlog_row_image_full"].ok
    assert not checks["server_id_unique"].ok and not checks["binlog_retention"].ok
    assert not checks["replication_privileges"].ok and checks["binlog_status_readable"].ok
    assert not checks["primary_key:shop.t"].ok
    assert c.current_position().value == {"file": "mysql-bin.000003", "pos": 4321}
    ok, msg = c.enable_capture_database()
    assert ok and "binlog_format=ROW" in msg and conn.sql_calls("SET GLOBAL binlog_format")


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
class FakeTs:
    def __init__(self, t, i=1):
        self.time, self.inc = t, i


def test_mongo_change_event_mapping():
    c = MongoConnector({"id": "mg", "database": "app"}, client_factory=lambda self: object())
    ins = {"_id": {"_data": "82AB"}, "operationType": "insert", "ns": {"db": "app", "coll": "users"}, "documentKey": {"_id": 1},
           "fullDocument": {"_id": 1, "name": "a", "tags": ["x"]}, "clusterTime": FakeTs(1704067200)}
    ev = c._to_event(ins)
    assert ev.operation == Operation.INSERT and ev.schema == "app" and ev.table == "users" and ev.key == {"_id": 1}
    assert ev.position.value == {"_data": "82AB"} and ev.source_ts.year == 2024
    upd = {"_id": {"_data": "82AC"}, "operationType": "update", "ns": {"db": "app", "coll": "users"}, "documentKey": {"_id": 1},
           "fullDocument": {"_id": 1, "name": "b"}, "fullDocumentBeforeChange": {"_id": 1, "name": "a"},
           "updateDescription": {"updatedFields": {"name": "b"}}}
    ev = c._to_event(upd)
    assert ev.operation == Operation.UPDATE and ev.before["name"] == "a" and ev.metadata["update_description"]["updatedFields"] == {"name": "b"}
    dele = {"_id": {"_data": "82AD"}, "operationType": "delete", "ns": {"db": "app", "coll": "users"}, "documentKey": {"_id": 1}}
    ev = c._to_event(dele)
    assert ev.operation == Operation.DELETE and ev.after is None and ev.key == {"_id": 1}
    assert c._to_event({"operationType": "drop"}) is None
    assert c._pipeline([("app", "users"), ("app", "orders")])[0]["$match"]["ns.coll"] == {"$in": ["orders", "users"]}


def test_mongo_read_changes_with_fake_stream():
    class Stream:
        def __init__(self, docs):
            self.kwargs = None
            self.docs, self.resume_token = docs, {"_data": "LAST"}

        def try_next(self):
            return self.docs.pop(0) if self.docs else None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    class DB:
        name = "app"

        def watch(self, **kwargs):
            self.kwargs = kwargs
            return Stream([{"_id": {"_data": "1"}, "operationType": "insert", "ns": {"db": "app", "coll": "u"}, "documentKey": {"_id": 1},
                            "fullDocument": {"_id": 1}}])

    class Client:
        def __getitem__(self, name):
            return DB()
    c = MongoConnector({"id": "mg", "database": "app"}, client_factory=lambda self: Client())
    evs, commit = c.read_changes([("app", "u")], Position("mongo_token", {"_data": "0"}))
    assert len(evs) == 1 and commit.value == {"_data": "LAST"}
    stop = threading.Event()
    got = []
    for ev in c.stream_changes([("app", "u")], None, stop):
        got.append(ev)
        stop.set()
    assert len(got) == 1


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------
def test_oracle_redo_parser_edge_cases():
    r = parse_redo("""insert into "HR"."EMP"("ID","NAME","HIRED") values ('1','O''Brien',TO_DATE('2024-01-02 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));""")
    assert r.values == {"ID": "1", "NAME": "O'Brien", "HIRED": datetime(2024, 1, 2, 10, 0)}
    r = parse_redo("""update "HR"."EMP" set "SAL" = 10.5, "NOTE" = NULL where "ID" = '1' and "SAL" IS NULL and ROWID = 'AAA';""")
    assert r.values == {"SAL": 10.5, "NOTE": None} and r.where == {"ID": "1", "SAL": None}
    r = parse_redo("""delete from "HR"."EMP" where "ID" = 5 and "RAW" = HEXTORAW('ff00') and "U" = UNISTR('\\00e9');""")
    assert r.where == {"ID": 5, "RAW": b"\xff\x00", "U": "é"}
    r = parse_redo("""insert into "A"."B"("C") values (TO_TIMESTAMP('2024-01-02 10:00:00.123456789', 'YYYY-MM-DD HH24:MI:SS.FF'))""")
    assert r.values["C"].microsecond == 123456
    with pytest.raises(RedoParseError):
        parse_redo("merge into x")


def test_oracle_read_changes_logminer_flow():
    conn = FakeDbApiConnection()
    conn.add(r"^SELECT current_scn FROM v\$database", (["CURRENT_SCN"], [[5000]]))
    conn.add(r"MIN\(start_scn\)", (["SCN"], [[3900]]))
    conn.add(r"v\$logfile", (["NAME", "FIRST_CHANGE#", "NEXT_CHANGE#"], [["/redo01.log", 3000, 6000]]))
    conn.add(r"v\$logmnr_contents", (["SCN", "COMMIT_SCN", "TIMESTAMP", "COMMIT_TIMESTAMP", "OPERATION_CODE", "SEG_OWNER", "TABLE_NAME",
                                      "SQL_REDO", "XID", "RS_ID", "SSN", "CSF"], [
        [4001, 4010, None, datetime(2024, 1, 1), 1, "HR", "EMP", """insert into "HR"."EMP"("ID","NAME") values ('1','a');""", b"\x01", "r1", 0, 0],
        [4002, 4010, None, datetime(2024, 1, 1), 3, "HR", "EMP", """update "HR"."EMP" set "NAME" = 'b' where "ID" = '1' and "NAME" = 'a';""", b"\x01", "r2", 0, 0],
        [4020, 4030, None, datetime(2024, 1, 1), 2, "HR", "EMP", """delete from "HR"."EMP" where "ID" = '1' """, b"\x02", "r3", 0, 1],
        [4020, 4030, None, datetime(2024, 1, 1), 2, "HR", "EMP", """and "NAME" = 'b';""", b"\x02", "r3", 1, 0],
        [4021, 4030, None, datetime(2024, 1, 1), 1, "HR", "OTHER", """insert into "HR"."OTHER"("ID") values (1);""", b"\x02", "r4", 0, 0],
    ]))
    c = OracleConnector({"id": "ora", "username": "HR"}, connection_factory=lambda self: conn)
    evs, commit = c.read_changes([("HR", "EMP")], Position("oracle_scn", 4000))
    assert [e.operation for e in evs] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert evs[1].before == {"ID": "1", "NAME": "a"} and evs[1].after == {"ID": "1", "NAME": "b"}
    assert evs[2].before == {"ID": "1", "NAME": "b"}          # CSF continuation concatenated
    assert commit.value == 5000
    start = conn.sql_calls("START_LOGMNR")[0]
    assert list(start[1]) == [3900, 5000]                              # mines from oldest open transaction
    assert conn.sql_calls("END_LOGMNR") and conn.sql_calls("ADD_LOGFILE")
    # first start without position: capture from now, no mining
    conn.calls.clear()
    evs, commit = c.read_changes([("HR", "EMP")], None)
    assert evs == [] and commit.value == 5000 and not conn.sql_calls("START_LOGMNR")


def test_oracle_batch_cut_at_transaction_boundary_and_prereqs():
    conn = FakeDbApiConnection()
    conn.add(r"^SELECT current_scn FROM v\$database", (["CURRENT_SCN"], [[5000]]))
    conn.add(r"MIN\(start_scn\)", (["SCN"], [[None]]))
    conn.add(r"v\$logfile", (["NAME", "FIRST_CHANGE#", "NEXT_CHANGE#"], [["/redo01.log", 3000, 6000]]))
    rows = []
    for commit_scn in (4010, 4020, 4030):
        for i in range(2):
            rows.append([commit_scn - 5 + i, commit_scn, None, None, 1, "HR", "EMP", f"""insert into "HR"."EMP"("ID") values ({commit_scn}{i});""",
                         b"\x01", f"r{commit_scn}{i}", 0, 0])
    conn.add(r"v\$logmnr_contents", (["SCN", "COMMIT_SCN", "TIMESTAMP", "COMMIT_TIMESTAMP", "OPERATION_CODE", "SEG_OWNER", "TABLE_NAME",
                                      "SQL_REDO", "XID", "RS_ID", "SSN", "CSF"], rows))
    c = OracleConnector({"id": "ora", "username": "HR"}, connection_factory=lambda self: conn)
    evs, commit = c.read_changes([("HR", "EMP")], Position("oracle_scn", 4000), limit=3)
    assert len(evs) == 4 and commit.value == 4020   # never cut inside a transaction; commit = last full commit scn
    conn.add(r"v\$version", (["BANNER"], [["Oracle 19c"]]))
    conn.add(r"supplemental_log_data_min", (["LOG_MODE", "SUPPLEMENTAL_LOG_DATA_MIN", "SUPPLEMENTAL_LOG_DATA_PK", "SUPPLEMENTAL_LOG_DATA_ALL", "CDB", "CURRENT_SCN"],
                                             [["NOARCHIVELOG", "NO", "NO", "NO", "NO", 5000]]))
    conn.add(r"session_privs", (["PRIVILEGE"], [["CREATE SESSION"]]))
    conn.add(r"all_tab_columns", (["COLUMN_NAME", "DATA_TYPE", "NULLABLE", "COLUMN_ID", "DATA_LENGTH", "IS_PK"], [["ID", "NUMBER", "N", 1, 22, 1], ["DOC", "CLOB", "Y", 2, 4000, 0]]))
    conn.add(r"all_log_groups", (["LOG_GROUP_TYPE"], []))
    checks = {ch.name: ch for ch in c.check_prerequisites([("HR", "EMP")])}
    assert not checks["archivelog_mode"].ok and not checks["supplemental_log_min"].ok and not checks["priv_logmining"].ok
    assert checks["primary_key:HR.EMP"].ok and not checks["supplemental_log_table:HR.EMP"].ok and checks["lob_columns:HR.EMP"].severity == "warning"
    assert c.enable_capture_table("HR", "EMP")[0] and "(ALL) COLUMNS" in conn.calls[-1][0]
    conn.add(r"FETCH FIRST", (["ID"], [[1]]))
    c.read_snapshot_chunk("HR", "EMP", ["ID", "NAME"], ["ID"], [7], 50)
    assert 'WHERE (("ID" > :1)) ORDER BY "ID" FETCH FIRST 50 ROWS ONLY' in conn.calls[-1][0]


def test_demo_engine_streams_and_snapshots():
    import threading
    from backend.connectors.demo import DemoConnector
    c = DemoConnector({"id": "demo-test", "initial_rows": 50, "rate_per_second": 1000, "poison_ratio": 0.0})
    c.connect()
    assert c.count_rows("demo", "Customers") == 50 and c.get_primary_key("demo", "Customers") == ["CustomerID"]
    stop = threading.Event()
    got = []
    for ev in c.stream_changes([("demo", "Customers")], None, stop):
        got.append(ev)
        if len(got) >= 20:
            stop.set()
    assert len(got) == 20 and all(e.position.value > 0 for e in got)
    evs, pos = c.read_changes([("demo", "Customers")], Position("demo", 10))
    assert evs and evs[0].position.value == 11
    rows = c.read_snapshot_chunk("demo", "Customers", ["CustomerID", "Name"], ["CustomerID"], [10], 5)
    assert [r["CustomerID"] for r in rows][0] > 10 and len(rows) == 5
    assert isinstance(create_connector({"engine": "demo", "id": "x"}), DemoConnector)
