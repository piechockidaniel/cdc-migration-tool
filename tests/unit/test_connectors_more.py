"""Second connector test layer: metadata, capture management, streaming loops, retention facts, error paths."""
import threading

import pytest

from backend.connectors.base import ConnectorError, Operation, Position
from backend.connectors.mariadb import MariaDBConnector
from backend.connectors.mongodb import MongoConnector
from backend.connectors.mssql import MSSQLConnector
from backend.connectors.oracle import OracleConnector
from backend.connectors.postgresql import PostgresConnector
from tests.fakes import FakeDbApiConnection

LSN1, LSN2 = bytes.fromhex("00" * 9 + "01"), bytes.fromhex("00" * 9 + "05")


def failing_factory(self):
    raise RuntimeError("connection refused")


# ---------------------------------------------------------------------------
# MongoDB with a fake client
# ---------------------------------------------------------------------------
class FakeTs:
    def __init__(self, t, i=1):
        self.time, self.inc = t, i


class FakeCursor(list):
    def sort(self, *a, **k):
        return self

    def limit(self, n):
        return FakeCursor(self[:n])


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, flt=None, projection=None, limit=0, sort=None):
        out = [d for d in self.docs if all((d.get(k) > v["$gt"]) if isinstance(v, dict) and "$gt" in v else d.get(k) == v for k, v in (flt or {}).items())]
        if sort:
            key, direction = sort[0]
            if key == "$natural":
                out = list(reversed(out)) if direction < 0 else list(out)
            else:
                out.sort(key=lambda d: d.get(key), reverse=direction < 0)
        return FakeCursor(out[:limit] if limit else out)

    def find_one(self, flt=None, projection=None, sort=None):
        r = self.find(flt, projection, 0, sort)
        return r[0] if r else None

    def estimated_document_count(self):
        return len(self.docs)

    def count_documents(self, flt):
        return len(self.docs)


class FakeDb:
    def __init__(self, name, client):
        self.watch_kwargs = None
        self.name, self.client = name, client
        self.cmds = []

    def list_collections(self, filter=None):
        cols = [{"name": "users", "type": "collection", "options": {"changeStreamPreAndPostImages": {"enabled": True}}},
                {"name": "orders", "type": "collection", "options": {}}, {"name": "v", "type": "view"}, {"name": "system.x", "type": "collection"}]
        return [c for c in cols if not filter or c["name"] == filter.get("name")]

    def command(self, name, *args, **kwargs):
        self.cmds.append((name, args, kwargs))
        if name == "collStats":
            return {"count": 3, "storageSize": 1024}
        if name == "ping":
            return {"ok": 1}
        if name == "hello":
            return {"setName": self.client.set_name, "primary": "h:27017", "lastWrite": {"opTime": {"ts": FakeTs(1700000000)}}}
        if name == "replSetGetStatus":
            if not self.client.set_name:
                raise RuntimeError("not running with --replSet")
            return {"ok": 1}
        if name == "connectionStatus":
            return {"authInfo": {"authenticatedUserRoles": [{"role": "read"}]}}
        if name == "collMod":
            if self.client.collmod_fails:
                raise RuntimeError("no such command")
            return {"ok": 1}
        return {"ok": 1}

    def __getitem__(self, coll):
        if self.name == "local" and coll == "oplog.rs":
            return FakeCollection([{"ts": FakeTs(1700000000 - 7200)}, {"ts": FakeTs(1700000000)}])
        return FakeCollection([{"_id": 1, "name": "a", "tags": ["x"]}, {"_id": 2, "name": "b"}, {"_id": 3, "name": "c"}])

    def watch(self, **kwargs):
        self.watch_kwargs = kwargs
        raise RuntimeError("stream closed")


class FakeMongoClient:
    def __init__(self, set_name="rs0", version="7.0.1"):
        self.set_name, self.version, self.collmod_fails = set_name, version, False
        self.admin = FakeDb("admin", self)
        self.closed = False

    def __getitem__(self, name):
        return FakeDb(name, self)

    def server_info(self):
        return {"version": self.version}

    def close(self):
        self.closed = True


def mongo(**cfg):
    client = FakeMongoClient(**{k: v for k, v in cfg.items() if k in ("set_name", "version")})
    c = MongoConnector({"id": "mg", "database": "app", "server": "h", "username": "u", "password": "p", **{k: v for k, v in cfg.items() if k not in ("set_name", "version")}},
                       client_factory=lambda self: client)
    c.connect()
    return c, client


def test_mongo_metadata_and_capture():
    c, client = mongo()
    assert "***" in c.uri(redact=True) and "u:p@h:27017" in c.uri()
    assert MongoConnector({"uri": "mongodb://x"}).uri(redact=True) == "mongodb://***"
    assert c.is_connected() and c.server_version() == "7.0.1"
    tables = c.list_tables()
    assert [t.table for t in tables] == ["orders", "users"] and tables[1].extra["pre_post_images"] and tables[0].row_estimate == 3
    cols = c.get_columns("app", "users")
    assert cols[0].name == "_id" and cols[0].is_primary_key and {x.name for x in cols} == {"_id", "name", "tags"}
    assert c.get_primary_key("app", "users") == ["_id"]
    checks = {ch.name: ch for ch in c.check_prerequisites([("app", "users"), ("app", "orders"), ("app", "missing")])}
    assert checks["replica_set_or_sharded"].ok and checks["pre_post_images_supported"].ok and checks["oplog_window"].ok is False
    assert checks["pre_post_images:app.users"].ok and not checks["pre_post_images:app.orders"].ok and not checks["collection_exists:app.missing"].ok
    assert c.capture_status()["replica_set"] == "rs0" and c.enable_capture_database()[0]
    assert c.enable_capture_table("app", "orders")[0] and c.disable_capture_table("app", "orders")[0]
    client.collmod_fails = True
    assert not c.enable_capture_table("app", "orders")[0] and not c.disable_capture_table("app", "orders")[0]
    assert c.current_position().value == {"t": 1700000000, "i": 1}
    assert c.count_rows("app", "users") == 3 and c.count_rows("app", "users", approximate=False) == 3
    rows = c.read_snapshot_chunk("app", "users", ["_id", "name"], ["_id"], [1], 10, where='{"name": "c"}')
    assert rows == [{"_id": 3, "name": "c"}]
    assert c.read_snapshot_chunk("app", "users", [], [], None, 2) and c.key_bounds("app", "users", ["_id"]) == ((1,), (3,))
    rs = c.retention_status({"__source__": Position("mongo_token", {"_data": "82" + format(1700000000 - 3600, "08x") + "ff"})})
    assert rs["retention_minutes"] == 120 and rs["retention_margin_minutes"] == 60 and not rs["position_before_min"]
    with pytest.raises(ConnectorError):
        list(c.stream_changes([("app", "users")], None, threading.Event()))
    c.disconnect()
    assert client.closed and not c.is_connected()


def test_mongo_standalone_and_connect_failure():
    c, client = mongo(set_name=None, version="4.4.0")
    checks = {ch.name: ch for ch in c.check_prerequisites()}
    assert not checks["replica_set_or_sharded"].ok and not checks["pre_post_images_supported"].ok
    assert not c.enable_capture_database()[0] and c.capture_status()["cdc_enabled"] is False
    bad = MongoConnector({"id": "x"}, client_factory=failing_factory)
    with pytest.raises(ConnectorError):
        bad.connect()
    assert bad.test_connection()[0] is False and bad.current_position() is None


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
def pg(responders=(), **cfg):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = PostgresConnector({"id": "pg", "database": "db", "server": "h", "username": "u", "password": "p", **cfg}, connection_factory=lambda self: conn)
    c.connect()
    return c, conn


def test_pg_metadata_capture_and_retention():
    c, conn = pg([
        (r"FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace\s+WHERE c.relkind", (["schema_name", "table_name", "row_estimate", "size_bytes", "relreplident"], [["public", "t", -1, 10, "f"]])),
        (r"pg_attribute", (["name", "data_type", "nullable", "ordinal", "is_pk"], [["id", "integer", False, 1, True]])),
        (r"slot_name, plugin", (["slot_name", "plugin", "active", "restart_lsn", "confirmed_flush_lsn", "lag_bytes"], [["cdcsync_pg", "wal2json", True, "0/1", "0/2", 10]])),
        (r"pg_current_wal_lsn\(\)::text", (["lsn"], [["0/9"]])),
        (r"SELECT 1 FROM pg_replication_slots", (["x"], [[1]])),
        (r"SELECT active, pg_wal_lsn_diff", (["active", "lag", "wal_status"], [[True, 2048, "lost"]])),
        (r"reltuples", (["n"], [[-1]])),
        (r"COUNT\(\*\)", (["n"], [[7]])),
        (r"MIN\(", (["lo", "hi"], [[1, 9]])),
    ])
    assert "password=***" in c.dsn(redact=True) and "sslmode" not in c.dsn()
    assert "sslmode=require" in PostgresConnector({"sslmode": "require"}).dsn()
    assert c.is_connected()
    t = c.list_tables()[0]
    assert t.row_estimate == 0 and t.extra["replica_identity"] == "f"
    assert c.get_columns("public", "t")[0].is_primary_key
    st = c.capture_status()
    assert st["cdc_enabled"] and st["current_wal_lsn"] == "0/9"
    assert c.enable_capture_database() == (True, "slot 'cdcsync_pg' already exists")
    assert c.drop_slot()[0] and conn.sql_calls("pg_drop_replication_slot")
    assert c.enable_capture_table("public", "t")[0] and c.disable_capture_table("public", "t")[0]
    assert c.count_rows("public", "t") == 0 and c.count_rows("public", "t", approximate=False) == 7
    assert c.key_bounds("public", "t", ["id"]) == ((1,), (9,))
    rs = c.retention_status({})
    assert rs["slot_lag_bytes"] == 2048 and rs["position_before_min"] is True
    c.disconnect()
    assert not c.is_connected()


def test_pg_stream_loop_and_errors():
    calls = {"wm": 0}

    def wm(sql, p):
        calls["wm"] += 1
        return (["lsn"], [["0/%X" % (0x100 * calls["wm"])]])
    msgs = [("0/150", 1, '{"action":"I","schema":"s","table":"t","columns":[{"name":"id","value":1}]}')]
    c, conn = pg([(r"pg_current_wal_lsn", wm), (r"pg_logical_slot_peek_changes", lambda s, p: (["lsn", "xid", "data"], [list(m) for m in msgs] if calls["wm"] >= 2 else []))],
                 poll_min_seconds=0.001, poll_max_seconds=0.002)
    stop = threading.Event()
    got = []
    for ev in c.stream_changes([("s", "t")], Position("pg_lsn", "0/100"), stop):
        got.append(ev)
        stop.set()
    assert got and got[0].operation == Operation.INSERT
    # driver error inside the loop is logged and retried, not raised
    c2, conn2 = pg([(r"pg_current_wal_lsn", lambda s, p: (_ for _ in ()).throw(RuntimeError("boom")))], poll_min_seconds=0.001, poll_max_seconds=0.002)
    stop2 = threading.Event()
    threading.Timer(0.05, stop2.set).start()
    assert list(c2.stream_changes([("s", "t")], None, stop2)) == []
    bad = PostgresConnector({"id": "x"}, connection_factory=failing_factory)
    with pytest.raises(ConnectorError):
        bad.connect()
    assert bad.test_connection()[0] is False
    checks = {ch.name: ch for ch in bad.check_prerequisites()}
    assert not checks["connection"].ok
    # enable slot failure + unknown wal2json action ignored + test_decoding unknown line
    c3, conn3 = pg([(r"SELECT 1 FROM pg_replication_slots", (["x"], [])), (r"pg_create_logical_replication_slot", lambda s, p: (_ for _ in ()).throw(RuntimeError("permission denied")))])
    assert not c3.enable_capture_database()[0]
    assert PostgresConnector._parse_wal2json('{"action":"M"}') is None
    assert PostgresConnector._parse_wal2json('{"action":"B"}') is None


# ---------------------------------------------------------------------------
# MariaDB
# ---------------------------------------------------------------------------
def mdb(responders=(), **cfg):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = MariaDBConnector({"id": "mdb", "database": "shop", **cfg}, connection_factory=lambda self: conn, stream_factory=cfg.get("stream_factory"))
    c.connect()
    return c, conn


def test_mariadb_metadata_status_and_retention():
    vars_ = {"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL", "binlog_expire_logs_seconds": "86400"}
    c, conn = mdb([
        (r"SHOW GLOBAL VARIABLES LIKE '(\w+)'", lambda sql, p: (["Variable_name", "Value"], [[n, vars_[n]] for n in vars_ if f"'{n}'" in sql])),
        (r"information_schema.tables WHERE table_type", (["table_schema", "table_name", "table_rows", "size_bytes"], [["shop", "customers", 10, 4096]])),
        (r"information_schema.columns", (["column_name", "column_type", "is_nullable", "ordinal_position", "column_key", "character_maximum_length"], [["id", "int", "NO", 1, "PRI", None]])),
        (r"SHOW BINARY LOG STATUS", (["File", "Position", "Executed_Gtid_Set"], [["mysql-bin.000003", 100, "uuid:1-5"]])),
        (r"SHOW BINARY LOGS", (["Log_name", "File_size"], [["mysql-bin.000002", 1], ["mysql-bin.000003", 1]])),
        (r"SELECT table_rows AS n", (["n"], [[10]])),
        (r"COUNT\(\*\)", (["n"], [[11]])),
        (r"MIN\(", (["lo", "hi"], [[1, 10]])),
    ])
    assert c.is_connected()
    assert c.list_tables()[0].table == "customers" and c.get_columns("shop", "customers")[0].is_primary_key
    assert c.get_primary_key("shop", "customers") == ["id"]
    pos = c.current_position()
    assert pos.value["pos"] == 100 and pos.extra["gtid"] == "uuid:1-5"
    st = c.capture_status()
    assert st["cdc_enabled"] and st["position"]["value"]["file"] == "mysql-bin.000003"
    assert c.enable_capture_database() == (True, "binary log already configured for row-based capture")
    assert c.enable_capture_table("shop", "customers")[0] and c.disable_capture_table("shop", "customers")[0]
    assert c.count_rows("shop", "customers") == 10 and c.count_rows("shop", "customers", approximate=False) == 11
    assert c.key_bounds("shop", "customers", ["id"]) == ((1,), (10,))
    rs = c.retention_status({"__source__": Position("binlog", {"file": "mysql-bin.000001", "pos": 4})})
    assert rs["position_before_min"] is True and rs["retention_minutes"] == 1440
    assert c.retention_status({"__source__": Position("binlog", {"file": "mysql-bin.000003", "pos": 4})})["position_before_min"] is False
    conn.responders = []
    conn.add(r"SHOW GLOBAL VARIABLES LIKE '(\w+)'", lambda sql, p: (["Variable_name", "Value"], [[n, "OFF"] for n in vars_ if f"'{n}'" in sql]))
    assert not c.enable_capture_database()[0]
    c.disconnect()
    assert not c.is_connected()


def test_mariadb_stream_failure_and_connect_error():
    class BrokenStream:
        log_file, log_pos = "mysql-bin.000001", 4

        def __iter__(self):
            raise RuntimeError("lost connection to MySQL server")

        def close(self):
            pass
    c, conn = mdb(stream_factory=lambda self, tables, pos, blocking: BrokenStream())
    with pytest.raises(ConnectorError):
        list(c.stream_changes([("shop", "t")], None, threading.Event()))
    bad = MariaDBConnector({"id": "x"}, connection_factory=failing_factory)
    with pytest.raises(ConnectorError):
        bad.connect()
    assert not bad.check_prerequisites()[0].ok
    assert mdb()[0].current_position() is None


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------
def ora(responders=(), **cfg):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = OracleConnector({"id": "ora", "username": "HR", "server": "h", "service_name": "ORCL", **cfg}, connection_factory=lambda self: conn)
    c.connect()
    return c, conn


def test_oracle_metadata_capture_and_stream():
    c, conn = ora([
        (r"^SELECT current_scn FROM v\$database", (["CURRENT_SCN"], [[5000]])),
        (r"FROM all_tables t", (["OWNER", "TABLE_NAME", "NUM_ROWS", "SIZE_BYTES"], [["HR", "EMP", 10, 8192]])),
        (r"SELECT DISTINCT table_name FROM all_log_groups", (["TABLE_NAME"], [["EMP"]])),
        (r"all_tab_columns", (["COLUMN_NAME", "DATA_TYPE", "NULLABLE", "COLUMN_ID", "DATA_LENGTH", "IS_PK"], [["ID", "NUMBER", "N", 1, 22, 1]])),
        (r"SELECT log_mode, supplemental_log_data_min, current_scn", (["LOG_MODE", "SUPPLEMENTAL_LOG_DATA_MIN", "CURRENT_SCN"], [["ARCHIVELOG", "YES", 5000]])),
        (r"SELECT owner, table_name, log_group_type", (["OWNER", "TABLE_NAME", "LOG_GROUP_TYPE"], [["HR", "EMP", "ALL COLUMN LOGGING"]])),
        (r"SELECT log_mode, supplemental_log_data_min FROM", (["LOG_MODE", "SUPPLEMENTAL_LOG_DATA_MIN"], [["ARCHIVELOG", "NO"]])),
        (r"SELECT num_rows FROM all_tables", (["NUM_ROWS"], [[10]])),
        (r"COUNT\(\*\)", (["N"], [[12]])),
        (r"MIN\(first_change#\) AS lo FROM v\$archived_log", (["LO"], [[4000]])),
        (r"MIN\(first_change#\) AS lo FROM v\$log", (["LO"], [[4500]])),
        (r"MIN\(", (["LO", "HI"], [[1, 10]])),
    ])
    assert c.dsn() == "h:1521/ORCL" and OracleConnector({"dsn": "tns"}).dsn() == "tns"
    assert c.is_connected() and c.list_tables()[0].cdc_enabled and c.get_columns("HR", "EMP")[0].is_primary_key
    st = c.capture_status()
    assert st["cdc_enabled"] and st["current_scn"] == 5000 and st["tables"][0]["TABLE_NAME"] == "EMP"
    ok, msg = c.enable_capture_database()
    assert ok and "minimal supplemental logging enabled" in msg and conn.sql_calls("ADD SUPPLEMENTAL LOG DATA")
    assert c.disable_capture_table("HR", "EMP", supplemental_mode="PRIMARY KEY")[0] and "(PRIMARY KEY) COLUMNS" in conn.calls[-1][0]
    assert c.count_rows("HR", "EMP") == 10 and c.count_rows("HR", "EMP", approximate=False) == 12
    assert c.key_bounds("HR", "EMP", ["ID"]) == ((1,), (10,))
    assert c.retention_status({"__source__": Position("oracle_scn", 3000)})["position_before_min"] is True
    assert c.retention_status({"__source__": Position("oracle_scn", 4800)})["position_before_min"] is False
    # stream: first watermark -> "start from now", nothing to yield; stop after two cycles
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    c.config["poll_min_seconds"], c.config["poll_max_seconds"] = 0.001, 0.002
    assert list(c.stream_changes([("HR", "EMP")], None, stop)) == []
    # already-enabled supplemental logging is not an error
    conn.add(r"ADD SUPPLEMENTAL LOG DATA \(ALL\)", lambda s, p: (_ for _ in ()).throw(RuntimeError("ORA-32588: supplemental logging attribute all column exists")))
    assert c.enable_capture_table("HR", "EMP")[0]
    c.disconnect()
    assert not c.is_connected()


def test_oracle_error_paths():
    c, conn = ora([
        (r"^SELECT current_scn FROM v\$database", (["CURRENT_SCN"], [[5000]])),
        (r"MIN\(start_scn\)", (["SCN"], [[None]])),
        (r"v\$logfile", (["NAME", "FIRST_CHANGE#", "NEXT_CHANGE#"], [])),
    ])
    with pytest.raises(ConnectorError, match="no redo/archive logs"):
        c.read_changes([("HR", "EMP")], Position("oracle_scn", 4000))
    conn.add(r"SELECT log_mode, supplemental_log_data_min FROM", (["LOG_MODE", "SUPPLEMENTAL_LOG_DATA_MIN"], [["NOARCHIVELOG", "NO"]]))
    assert not c.enable_capture_database()[0]
    conn.add(r"DROP SUPPLEMENTAL", lambda s, p: (_ for _ in ()).throw(RuntimeError("ORA-01031")))
    assert not c.disable_capture_table("HR", "EMP")[0]
    bad = OracleConnector({"id": "x"}, connection_factory=failing_factory)
    with pytest.raises(ConnectorError):
        bad.connect()
    assert not bad.check_prerequisites()[0].ok and "error" in bad.capture_status()
    assert bad.retention_status({}) == {"position_before_min": False, "capture_errors": 0, "retention_margin_minutes": None}


# ---------------------------------------------------------------------------
# MSSQL remaining paths
# ---------------------------------------------------------------------------
def ms(responders=(), **cfg):
    conn = FakeDbApiConnection()
    for pat, res in responders:
        conn.add(pat, res)
    c = MSSQLConnector({"id": "ms", "server": "s", "database": "db", "username": "u", "password": "p", **cfg}, connection_factory=lambda self: conn)
    c.connect()
    return c, conn


def test_mssql_metadata_broker_and_errors():
    c, conn = ms([
        (r"dm_db_partition_stats ps", (["schema_name", "table_name", "is_tracked_by_cdc", "row_estimate", "size_bytes"], [["dbo", "T", 1, 10, 8192]])),
        (r"SELECT SUM\(row_count\)", (["n"], [[10]])),
        (r"COUNT_BIG\(\*\) AS n FROM \[dbo\]", (["n"], [[11]])),
        (r"MIN\(", (["lo", "hi"], [[1, 10]])),
        (r"WAITFOR", (["message_body", "message_type_name"], [[LSN2, "cdcsync_lsn_advanced"]])),
        (r"^RECEIVE", (["message_body"], [])),
        (r"FROM sys.databases", (["is_cdc_enabled"], [[0]])),
        (r"sp_cdc_enable_db", lambda s, p: (_ for _ in ()).throw(RuntimeError("permission denied"))),
        (r"sp_cdc_disable_table", lambda s, p: (_ for _ in ()).throw(RuntimeError("not found"))),
    ], service_broker_queue="dbo.q")
    assert c.is_connected()
    t = c.list_tables()[0]
    assert t.cdc_enabled and t.row_estimate == 10
    assert c.count_rows("dbo", "T") == 10 and c.count_rows("dbo", "T", approximate=False) == 11
    assert c.key_bounds("dbo", "T", ["Id"]) == ((1,), (10,))
    assert c.wait_for_notification(10).value == LSN2.hex()
    assert not c.enable_capture_database()[0] and not c.disable_capture_table("dbo", "T")[0]
    assert c.capture_status()["cdc_enabled"] is False
    # push-mode stream: notification drives the watermark
    conn.add(r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN2]]))
    conn.add(r"CASE WHEN EXISTS", (["tbl", "n"], [["dbo.T", 0]]))
    c.config["poll_min_seconds"], c.config["poll_max_seconds"] = 0.001, 0.002
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    assert list(c.stream_changes([("dbo", "T")], None, stop)) == []
    c.disconnect()
    assert not c.is_connected()
    assert MSSQLConnector({"service_broker_queue": None}).wait_for_notification.__name__  # exists
    with pytest.raises(ConnectorError):
        MSSQLConnector({"id": "x"}).wait_for_notification()
    bad = MSSQLConnector({"id": "x", "server": "s", "database": "d"}, connection_factory=failing_factory)
    with pytest.raises(ConnectorError):
        bad.connect()
    assert not bad.check_prerequisites()[0].ok


def test_mssql_stream_survives_driver_errors():
    c, conn = ms([(r"fn_cdc_get_max_lsn", lambda s, p: (_ for _ in ()).throw(RuntimeError("network")))], poll_min_seconds=0.001, poll_max_seconds=0.002)
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    assert list(c.stream_changes([("dbo", "T")], None, stop)) == []
