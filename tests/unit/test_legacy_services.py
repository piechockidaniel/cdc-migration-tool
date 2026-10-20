"""Tests for the 1.x services that are still wired into the API (SQL Server driver stubbed with a fake pyodbc)."""
import json
from datetime import date, datetime

import pytest

from backend.models.schemas import (
    CDCEvent, CDCOperation, ColumnMapping, ConnectionConfig, DataSourceConfig, IngestionColumnMapping, IngestionPattern,
    Mapping, MappingType, MergeOperation, MergeOperationType, MergePattern, OutputColumnMapping, PatternType, SourceConfig,
)
from tests.fakes import FakeDbApiConnection


@pytest.fixture
def fake_pyodbc(monkeypatch):
    """Replace pyodbc.connect with a scriptable connection; returns the connection so tests can add responders."""
    conn = FakeDbApiConnection()
    import pyodbc
    monkeypatch.setattr(pyodbc, "connect", lambda *a, **k: conn)
    return conn


def cfg(**kw):
    base = dict(name="c", server="srv", database="db", username="u", password="p", use_windows_auth=False)
    base.update(kw)
    return ConnectionConfig(**base)


# ---------------------------------------------------------------------------
# MSSQLConnection / CDCOperations
# ---------------------------------------------------------------------------
def test_mssql_connection_basics(fake_pyodbc):
    from backend.db.mssql_manager import MSSQLConnection, ConnectionPool
    c = MSSQLConnection(cfg(port=1433))
    cs = c._build_connection_string()
    assert "SERVER=srv,1433" in cs and "UID=u" in cs and "Connection Timeout=15" in cs and "Trusted_Connection" not in cs
    assert "Trusted_Connection=yes" in MSSQLConnection(cfg(use_windows_auth=True))._build_connection_string()
    assert c.connect() and c.is_connected()
    fake_pyodbc.add(r"@@VERSION", (["version"], [["Microsoft SQL Server 2022"]]))
    assert c.get_server_version().startswith("Microsoft")
    ok, msg = c.test_connection()
    assert ok and "Connected" in msg
    fake_pyodbc.add(r"FROM sys.databases", (["name", "cdc_enabled"], [["db", 1]]))
    assert c.get_databases()[0].cdc_enabled
    fake_pyodbc.add(r"FROM sys.tables t", (["schema_name", "table_name", "full_name", "cdc_enabled", "row_count"], [["dbo", "T", "dbo.T", 1, 5]]))
    assert c.get_tables()[0].row_count == 5
    fake_pyodbc.add(r"FROM sys.columns c", (["column_name", "data_type", "is_nullable", "max_length", "is_primary_key"], [["Id", "int", 0, 4, 1]]))
    assert c.get_columns("dbo", "T")[0].is_primary_key
    assert c.execute_non_query("UPDATE t SET a = ?", (1,)) == 1
    assert c.bulk_insert("dbo", "T", [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], batch_size=1) == 2
    assert c.bulk_insert("dbo", "T", []) == 0
    assert fake_pyodbc.sql_calls(r"INSERT INTO \[dbo\].\[T\]")
    c.disconnect()
    assert not c.is_connected()
    pool = ConnectionPool()
    pool.set_source(cfg()); pool.set_destination(cfg())
    assert pool.get_source() and pool.get_destination()
    pool.close_all()
    assert pool.get_source() is None


def test_mssql_connection_failures(monkeypatch):
    import pyodbc
    from backend.db.mssql_manager import MSSQLConnection

    def boom(*a, **k):
        raise RuntimeError("login failed")
    monkeypatch.setattr(pyodbc, "connect", boom)
    c = MSSQLConnection(cfg())
    assert c.connect() is False
    assert c.test_connection()[0] is False
    with pytest.raises(Exception):
        c.execute_query("SELECT 1")
    assert c.get_databases() == [] and c.get_tables() == [] and c.get_columns("a", "b") == []


def test_cdc_operations(fake_pyodbc):
    from backend.db.cdc_operations import CDCOperations
    from backend.db.mssql_manager import MSSQLConnection
    c = MSSQLConnection(cfg())
    ops = CDCOperations(c)
    fake_pyodbc.add(r"is_cdc_enabled FROM sys.databases", (["is_cdc_enabled"], [[0]]))
    assert ops.is_cdc_enabled_on_database() is False
    ok, msg = ops.enable_cdc_on_database()
    assert not ok and "verification failed" in msg
    fake_pyodbc.responders = []
    fake_pyodbc.add(r"is_cdc_enabled FROM sys.databases", (["is_cdc_enabled"], [[1]]))
    assert ops.enable_cdc_on_database() == (True, "CDC is already enabled on this database")
    tracked = {"v": 0}
    fake_pyodbc.add(r"is_tracked_by_cdc", lambda s, p: (["is_tracked_by_cdc"], [[tracked["v"]]]))

    def enable(sql, p):
        tracked["v"] = 1
        return None
    fake_pyodbc.add(r"sp_cdc_enable_table", enable)
    ok, msg = ops.enable_cdc_on_table("dbo", "T")
    assert ok and "dbo.T" in msg
    assert ops.enable_cdc_on_table("dbo", "T")[0]
    fake_pyodbc.add(r"cdc.change_tables", (["source_schema", "source_table", "capture_instance", "start_lsn", "create_date", "role_name"],
                                          [["dbo", "T", "dbo_T", b"\x00" * 10, None, None]]))
    assert ops.get_cdc_enabled_tables()[0]["capture_instance"] == "dbo_T"
    lsn1, lsn2 = bytes.fromhex("00" * 9 + "01"), bytes.fromhex("00" * 9 + "05")
    fake_pyodbc.add(r"fn_cdc_get_min_lsn", (["min_lsn"], [[lsn1]]))
    fake_pyodbc.add(r"fn_cdc_get_max_lsn", (["max_lsn"], [[lsn2]]))
    fake_pyodbc.add(r"fn_cdc_increment_lsn", (["next_lsn"], [[lsn2]]))
    fake_pyodbc.add(r"fn_cdc_get_all_changes", (["__$operation", "Id"], [[2, 1]]))
    assert ops.get_min_lsn("dbo_T") == lsn1 and ops.get_max_lsn() == lsn2 and ops.increment_lsn(lsn1) == lsn2
    assert ops.get_cdc_changes("dbo", "T")[0]["Id"] == 1
    assert ops.get_cdc_changes("dbo", "T", lsn1, lsn2)[0]["__$operation"] == 2


def test_cdc_operations_error_paths(monkeypatch):
    import pyodbc
    from backend.db.cdc_operations import CDCOperations
    from backend.db.mssql_manager import MSSQLConnection
    monkeypatch.setattr(pyodbc, "connect", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
    ops = CDCOperations(MSSQLConnection(cfg()))
    assert ops.is_cdc_enabled_on_database() is False and ops.is_cdc_enabled_on_table("a", "b") is False
    assert ops.enable_cdc_on_table("a", "b")[0] is False
    assert ops.get_cdc_enabled_tables() == [] and ops.get_cdc_changes("a", "b") == []
    assert ops.get_min_lsn("x") is None and ops.get_max_lsn() is None and ops.increment_lsn(b"\x00") is None


# ---------------------------------------------------------------------------
# SQL mapping service + executor
# ---------------------------------------------------------------------------
def sql_mapping(mode, **kw):
    return Mapping(id="sqlm", name="sqlm", mapping_type=MappingType.SQL, destination_schema="dbo", destination_table="T",
                   source_query="SELECT 1 AS id, 'a' AS v", sync_mode=mode, batch_size=10, **kw)


def test_sql_mapping_service_modes(fake_pyodbc):
    from backend.core.sql_mapping_service import SQLMappingService
    from backend.db.mssql_manager import MSSQLConnection
    src, dst = MSSQLConnection(cfg()), MSSQLConnection(cfg())
    svc = SQLMappingService()
    fake_pyodbc.add(r"SELECT 1 AS id", (["id", "v"], [[1, "a"], [2, "b"]]))
    ok, msg, n = svc.execute_mapping(sql_mapping("full"), src, dst)
    assert ok and n == 2 and fake_pyodbc.sql_calls("TRUNCATE TABLE")
    ok, msg, n = svc.execute_mapping(sql_mapping("incremental"), src, dst)
    assert ok and "2 rows inserted" in msg
    assert svc.execute_mapping(sql_mapping("upsert"), src, dst)[0] is False          # no key columns
    fake_pyodbc.add(r"SELECT COUNT\(\*\) as cnt", lambda s, p: (["cnt"], [[1 if p[0] == 1 else 0]]))
    ok, msg, n = svc.execute_mapping(sql_mapping("upsert", key_columns=["id"]), src, dst)
    assert ok and "1 inserted, 1 updated" in msg
    ok, msg, n = svc.execute_mapping(sql_mapping("upsert", key_columns=["id"], update_query="UPDATE T SET v = v", insert_query="INSERT INTO T DEFAULT VALUES"), src, dst)
    assert ok
    assert svc.execute_mapping(sql_mapping("bogus"), src, dst)[0] is False
    table_m = Mapping(id="t", name="t", mapping_type=MappingType.TABLE, destination_schema="dbo", destination_table="T")
    assert svc.execute_mapping(table_m, src, dst)[0] is False
    fake_pyodbc.responders = []
    fake_pyodbc.add(r"SELECT 1 AS id", (["id"], []))
    assert svc.execute_mapping(sql_mapping("full"), src, dst) == (True, "No data to synchronize", 0)


def test_mapping_executor_groups_and_transactions(fake_pyodbc):
    from backend.core.mapping_executor import MappingExecutor
    from backend.db.mssql_manager import MSSQLConnection
    src, dst = MSSQLConnection(cfg()), MSSQLConnection(cfg())
    fake_pyodbc.add(r"SELECT 1 AS id", (["id"], [[1]]))
    m1 = sql_mapping("incremental"); m2 = sql_mapping("incremental"); m2.id = "sqlm2"; m2.destination_table = "U"
    m3 = sql_mapping("incremental"); m3.id = "off"; m3.enabled = False
    results = MappingExecutor().execute_mappings_transactionally([m1, m2, m3], src, dst)
    assert set(results) == {"sqlm", "sqlm2"} and all(r["success"] for r in results.values())
    assert MappingExecutor().execute_mappings_transactionally([], src, dst) == {}


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------
def test_data_quality_verificator(fake_pyodbc):
    from backend.core.data_quality import DataQualityVerificator
    from backend.db.mssql_manager import MSSQLConnection
    src, dst = MSSQLConnection(cfg()), MSSQLConnection(cfg())
    m = Mapping(id="m", name="m", mapping_type=MappingType.TABLE, source_schema="dbo", source_table="S", destination_schema="dbo",
                destination_table="D", key_columns=["id"],
                column_mappings=[ColumnMapping(source_column="Id", destination_column="id"), ColumnMapping(source_column="N", destination_column="n")])
    fake_pyodbc.add(r"COUNT_BIG\(\*\)", (["count"], [[2]]))
    fake_pyodbc.add(r"TABLESAMPLE", (["Id", "N"], [[1, "a"], [2, "b"]]))
    fake_pyodbc.add(r"FROM \[dbo\].\[D\]\s+WHERE", lambda s, p: (["id", "n"], [[p[0], "a" if p[0] == 1 else "zzz"]]))
    fake_pyodbc.add(r"FROM \[dbo\].\[S\]\s+WHERE", (["Id", "N"], [[1, "a"]]))
    r = DataQualityVerificator(src, dst, None).verify_mapping(m, sample_size=2)
    assert r.row_count_match and r.samples_checked == 2 and r.samples_matched == 1 and len(r.mismatches) == 1
    assert 0.0 <= r.reverse_mapping_success_rate <= 1.0
    # TABLESAMPLE unsupported -> TOP fallback
    fake_pyodbc.responders = []
    fake_pyodbc.add(r"TABLESAMPLE", lambda s, p: (_ for _ in ()).throw(RuntimeError("no tablesample")))
    fake_pyodbc.add(r"SELECT TOP", (["Id", "N"], [[1, "a"]])); fake_pyodbc.add(r"COUNT_BIG", (["count"], [[1]]))
    fake_pyodbc.add(r"WHERE", (["id", "n"], [[1, "a"]]))
    r = DataQualityVerificator(src, dst, None).verify_mapping(m, sample_size=1)
    assert r.samples_matched == 1


# ---------------------------------------------------------------------------
# Data ingestion pattern detectors
# ---------------------------------------------------------------------------
def pattern(pid, ptype, **config):
    return IngestionPattern(id=pid, name=pid, description=pid, type=ptype, config=config)


def test_pattern_detectors():
    from backend.core.data_ingestion_engine import (CaseMismatchDetector, DiacriticMismatchDetector, VerticalShiftDetector,
                                                    WhitespaceMismatchDetector)
    swap = VerticalShiftDetector(pattern("swap", PatternType.COLUMN_SWAP, threshold=0.5))
    assert swap.detect({"a": "John Smith", "b": "Smith John"})[0]
    assert not swap.detect({"a": "John", "b": "John"})[0] and not swap.detect({"a": ""})[0]
    assert swap.normalize(" x ") == " x "   # vertical shift has no normalisation
    dia = DiacriticMismatchDetector(pattern("dia", PatternType.CHARACTER_NORMALIZATION))
    assert dia.detect({"a": "Łódź", "b": "Lodz"})[0] and dia.normalize("Łódź").lower() in ("lodz", "łodz")
    assert not dia.detect({"a": "x", "b": "y"})[0]
    case = CaseMismatchDetector(pattern("case", PatternType.CASE_NORMALIZATION))
    assert case.detect({"a": "ABC", "b": "abc"})[0] and case.normalize("ABC") == "abc"
    ws = WhitespaceMismatchDetector(pattern("ws", PatternType.WHITESPACE_NORMALIZATION))
    assert ws.detect({"a": " a  b ", "b": "a b"})[0] and ws.normalize(" a  b ") == "a b"
    assert not ws.detect({"a": "a", "b": "b"})[0]


def test_ingestion_engine_analyze(fake_pyodbc, tmp_path, monkeypatch):
    import backend.core.data_ingestion_engine as die
    cfg_dir = tmp_path / "cfg"; cfg_dir.mkdir()
    (cfg_dir / "ingestion_patterns.json").write_text(json.dumps({"patterns": [
        {"id": "case", "name": "case", "description": "", "type": "case_normalization", "enabled": True},
        {"id": "off", "name": "off", "description": "", "type": "column_swap", "enabled": False}]}))
    monkeypatch.setenv("CDCSYNC_CONFIG_DIR", str(cfg_dir))
    monkeypatch.setattr(die.config_manager, "get_connection", lambda cid: {"id": cid, "server": "s", "database": "d", "username": "u", "password": "p"})
    engine = die.DataIngestionEngine()
    assert list(engine.detectors) == ["case"]
    fake_pyodbc.add(r"FROM dbo.A", (["id", "name"], [[1, "ANN"], [2, "Bob"]]))
    fake_pyodbc.add(r"FROM dbo.B", (["id", "name"], [[1, "ann"], [2, "Bob"]]))
    sources = [DataSourceConfig(connection_id="c1", schema_name="dbo", table_name="A", alias="a", columns=["id", "name"]),
               DataSourceConfig(connection_id="c2", schema_name="dbo", table_name="B", alias="b", columns=["id", "name"], where_clause="1=1")]
    res = engine.analyze_sources(sources, [IngestionColumnMapping(name="name", source_columns={"a": "name", "b": "name"})], ["id"])
    assert res.total_records == 2 and res.matched_records == 1 and res.mismatches[0].detected_patterns == ["case"]
    assert engine.get_patterns()[0].id == "case"
    engine.reload_patterns()
    monkeypatch.setattr(die.config_manager, "get_connection", lambda cid: None)
    with pytest.raises(ValueError):
        engine.analyze_sources(sources, [], ["id"])


# ---------------------------------------------------------------------------
# Workflow manager + DuckDB script manager + history manager + metrics history
# ---------------------------------------------------------------------------
def test_workflow_manager_crud_and_validation(tmp_path, monkeypatch):
    import backend.core.workflow_manager as wm
    monkeypatch.setattr(wm, "WORKFLOWS_BASE_DIR", tmp_path); monkeypatch.setattr(wm, "VISUAL_DIR", tmp_path / "visual"); monkeypatch.setattr(wm, "COMPILED_DIR", tmp_path / "compiled")
    from backend.models.workflow_schemas import NodePosition, VisualWorkflow, WorkflowEdge, WorkflowNode
    mgr = wm.WorkflowManager()
    n = lambda i, t, c=None: WorkflowNode(id=i, type=t, position=NodePosition(x=0, y=0), config=c or {})
    wf = VisualWorkflow(id="w1", name="w", nodes=[n("s", "start"), n("src", "source_table", {"schema": "dbo", "table": "T"}), n("d", "dest_table", {"schema": "dbo", "table": "U"})],
                        edges=[WorkflowEdge(id="e1", source="s", target="src"), WorkflowEdge(id="e2", source="src", target="d")])
    assert mgr.create_workflow(wf)[0] and not mgr.create_workflow(wf)[0]
    assert mgr.get_workflow("w1").name == "w" and mgr.list_workflows()[0].id == "w1"
    wf.name = "w2"
    assert mgr.update_workflow(wf)[0] and mgr.get_workflow("w1").name == "w2"
    v = mgr.validate_workflow(wf)
    assert v.valid and v.node_count == 3
    bad = VisualWorkflow(id="w2", name="bad", nodes=[n("a", "source_table"), n("b", "stop")],
                         edges=[WorkflowEdge(id="e", source="a", target="b"), WorkflowEdge(id="f", source="b", target="a")])
    v = mgr.validate_workflow(bad)
    assert not v.valid and any("Start" in e for e in v.errors) and any("cycle" in e.lower() for e in v.errors)
    assert not mgr.validate_workflow(VisualWorkflow(id="e", name="e")).valid
    assert mgr.set_compiled_mapping("w1", "m1")[0] and mgr.get_workflow("w1").compiled_mapping_id == "m1"
    assert mgr.delete_workflow("w1")[0] and not mgr.delete_workflow("w1")[0]
    mgr2 = wm.WorkflowManager()   # reload from disk
    assert mgr2.list_workflows() == []


def test_duckdb_script_manager(tmp_path, monkeypatch):
    import backend.core.duckdb_script_manager as dsm
    monkeypatch.setattr(dsm, "SCRIPTS_BASE_DIR", tmp_path); monkeypatch.setattr(dsm, "TEMPLATES_DIR", tmp_path / "templates"); monkeypatch.setattr(dsm, "CUSTOM_DIR", tmp_path / "custom")
    mgr = dsm.DuckDBScriptManager()
    (tmp_path / "templates" / "basic.sql").write_text("-- Filter rows\nSELECT * FROM {source_table};")
    ok, msg = mgr.save_script("mine", "-- My script\nSELECT 1;")
    assert ok
    names = {s["name"]: s for s in mgr.list_scripts()}
    assert "basic" in names and names["mine"]["category"] == "custom"
    assert mgr.list_scripts("custom")[0]["name"] == "mine"
    assert "SELECT 1" in mgr.get_script("mine") and mgr.get_script("nope") is None
    assert mgr.validate_script_syntax("SELECT 1")[0] and not mgr.validate_script_syntax("")[0]
    assert not mgr.validate_script_syntax("SELECT (1")[0]
    assert mgr.delete_script("mine")[0] and not mgr.delete_script("mine")[0]


def test_history_manager_jsonl(tmp_path, monkeypatch):
    import backend.core.history_manager as hm
    monkeypatch.setattr(hm, "HISTORY_DIR", tmp_path)
    h = hm.HistoryManager()
    h.buffer_size = 2
    ev = CDCEvent(id="e1", timestamp=datetime.now(), source_table="dbo.T", operation=CDCOperation.INSERT, lsn="00", seqval="00", data={"a": 1})
    h.log_event(ev, "processed"); h.log_event(ev, "failed", error="boom")
    assert (tmp_path / f"cdc_events_{date.today().strftime('%Y%m%d')}.jsonl").exists()
    h.log_event(ev, "processed")
    rows = h.query_events()
    assert len(rows) == 3 and h.get_recent_errors()[0]["error"] == "boom"
    st = h.get_statistics()
    assert st["total_events"] == 3 and st["failed_events"] == 1 and st["operations"] == {"INSERT": 3}
    assert h.query_events(operation="DELETE") == [] and len(h.query_events(source_table="dbo.T", limit=1)) == 1
    # legacy json array file is still readable
    (tmp_path / f"cdc_events_{date.today().strftime('%Y%m%d')}.json").write_text(json.dumps([{"event_id": "old", "status": "processed", "operation": "UPDATE", "source_table": "x"}]))
    assert len(h.query_events()) == 4


def test_metrics_history_sampling():
    from backend.core import metrics
    from backend.core.metrics_history import MetricsHistory
    metrics.EVENTS_APPLIED.labels(table="h.t", op="INSERT").inc(10)
    metrics.END_TO_END_LAG.labels(table="h.t").set(2.5)
    metrics.QUEUE_DEPTH.labels(pipeline="hp").set(3)
    metrics.APPLY_LATENCY.labels(table="h.t").observe(0.2)
    metrics.SNAPSHOT_ACTIVE.set(1)
    import time
    mh = MetricsHistory(interval_s=0.01, max_points=10)
    t0 = time.time() - 10
    mh.sample(now=t0)
    metrics.EVENTS_APPLIED.labels(table="h.t", op="INSERT").inc(5)
    mh.sample(now=t0 + 5)
    s = mh.series(minutes=60)
    assert s["rate.applied"][-1][1] == pytest.approx(1.0)
    assert s["lag.h.t"][-1][1] == 2.5 and s["queue.hp"][-1][1] == 3 and s["apply_p95.h.t"][-1][1] > 0 and s["snapshots.active"]
    assert "lag.h.t" in mh.series(minutes=60, prefix="lag.", max_points=1) and all(k.startswith("lag.") for k in mh.series(minutes=60, prefix="lag."))
    mh.start(); mh.stop()


# ---------------------------------------------------------------------------
# Multi-source executor / sync engine / monitor
# ---------------------------------------------------------------------------
def multi_mapping():
    return Mapping(id="ms", name="ms", mapping_type=MappingType.TABLE, destination_schema="dbo", destination_table="Merged", is_multi_source=True,
                   key_columns=["customer_id"],
                   sources=[SourceConfig(id="s1", connection_id="c1", alias="crm", schema="dbo", table="Customers"),
                            SourceConfig(id="s2", connection_id="c2", alias="erp", schema="dbo", table="Accounts", where_clause="Active = 1")],
                   merge_pattern=MergePattern(operations=[MergeOperation(type=MergeOperationType.JOIN, left_source="crm", right_source="erp", join_type="INNER",
                                                                         on_condition="crm.customer_id = erp.customer_id")],
                                              output_columns=[OutputColumnMapping(source_alias="crm", source_column="customer_id", destination_column="customer_id"),
                                                              OutputColumnMapping(source_alias="erp", source_column="balance", destination_column="balance", transformation="erp.balance * 2")]))


def test_multi_source_executor_validation_query_and_preview(fake_pyodbc):
    from backend.core.multi_source_executor import MergePatternExecutor
    from backend.db.mssql_manager import MSSQLConnection
    ex = MergePatternExecutor({})
    m = multi_mapping()
    assert ex.validate_merge_pattern(m) == (True, "Validation successful")
    q = ex.build_merge_query(m, limit=5)
    assert "WITH" in q and "INNER JOIN erp" in q and "TOP 5" in q and "Active = 1" in q
    for op_type in (MergeOperationType.UNION, MergeOperationType.INTERSECT, MergeOperationType.EXCEPT, MergeOperationType.CUSTOM):
        m2 = multi_mapping(); m2.merge_pattern.operations[0].type = op_type; m2.merge_pattern.operations[0].custom_expression = "SELECT 1"
        assert ex.build_merge_query(m2)
    bad = multi_mapping(); bad.merge_pattern.operations[0].right_source = "nope"
    assert not ex.validate_merge_pattern(bad)[0]
    single = multi_mapping(); single.sources = single.sources[:1]
    assert not ex.validate_merge_pattern(single)[0]
    plain = Mapping(id="p", name="p", mapping_type=MappingType.TABLE, destination_schema="dbo", destination_table="x")
    assert not ex.validate_merge_pattern(plain)[0]
    conn = MSSQLConnection(cfg())
    fake_pyodbc.add(r"WITH", (["customer_id", "balance"], [[1, 2.0]]))
    prev = ex.execute_merge_preview(m, conn, limit=1)
    assert prev["success"] and prev["columns"] == ["customer_id", "balance"]
    assert ex.execute_merge_for_sync(m, conn)[0]["customer_id"] == 1
    assert not ex.execute_merge_preview(bad, conn)["success"]


def test_multi_source_sync_engine_helpers():
    from backend.core.multi_source_sync_engine import ConflictResolver, MultiSourceSyncEngine
    from backend.models.schemas import ConflictResolutionStrategy
    m = multi_mapping()
    assert ConflictResolver.resolve(m, "s1", None, {"a": 1}) == {"a": 1}
    for strat in ConflictResolutionStrategy:
        m.conflict_resolution = strat
        assert ConflictResolver.resolve(m, "s1", {"a": 0}, {"a": 1}) == {"a": 1}
    eng = MultiSourceSyncEngine()
    eng.set_mapping(m)
    where, params = eng._get_primary_key_condition({"customer_id": 7})
    assert where == "[customer_id] = ?" and params == [7]
    assert eng._get_primary_key_condition({})[0] == "1=0"
    with pytest.raises(ValueError):
        eng.set_mapping(Mapping(id="p", name="p", mapping_type=MappingType.TABLE, destination_schema="d", destination_table="t"))
    st = eng.get_statistics()
    assert st["mapping_name"] == "ms" and st["events_processed"] == 0


def test_multi_source_cdc_monitor_polls_with_library_dicts(fake_pyodbc, monkeypatch):
    import asyncio
    import backend.core.multi_source_cdc_monitor as mon
    monkeypatch.setattr(mon.config_manager, "get_connection", lambda cid: {"id": cid, "server": "s", "database": "d", "username": "u", "password": "p"})
    positions = {}
    monkeypatch.setattr(mon.config_manager, "get_last_lsn", lambda key: positions.get(key))
    monkeypatch.setattr(mon.config_manager, "set_last_lsn", lambda key, v: positions.__setitem__(key, v))
    lsn1, lsn2 = bytes.fromhex("00" * 9 + "01"), bytes.fromhex("00" * 9 + "05")
    fake_pyodbc.add(r"fn_cdc_get_min_lsn", (["min_lsn"], [[lsn1]]))
    fake_pyodbc.add(r"fn_cdc_get_max_lsn", (["max_lsn"], [[lsn2]]))
    fake_pyodbc.add(r"fn_cdc_increment_lsn", (["next_lsn"], [[lsn2]]))
    fake_pyodbc.add(r"fn_cdc_get_all_changes", (["__$operation", "__$start_lsn", "__$seqval", "customer_id"], [[2, lsn2, lsn1, 1]]))
    m = multi_mapping()
    monitor = mon.MultiSourceCDCMonitor(m)
    monitor._create_connections()
    assert set(monitor.source_monitors) == {"s1", "s2"}
    changes = asyncio.run(monitor.source_monitors["s1"].poll_changes())
    assert changes[0]["_source_alias"] == "crm" and positions["multi_s1_dbo.Customers"] == lsn2.hex()
    monitor.source_monitors["s1"].load_lsn_state()
    assert monitor.source_monitors["s1"].last_lsn == lsn2
    assert asyncio.run(monitor.source_monitors["s1"].poll_changes()) == []   # max == last -> nothing new
    st = monitor.get_status()
    assert st["source_count"] == 2 and not st["is_running"]
    monitor._close_connections()
    assert monitor.source_monitors == {}
    with pytest.raises(ValueError):
        mon.MultiSourceCDCMonitor(Mapping(id="p", name="p", mapping_type=MappingType.TABLE, destination_schema="d", destination_table="t"))
