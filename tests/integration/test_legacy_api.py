"""1.x API surface (admin scan/CDC/worksets/scripts/workflows/ingestion, operations SQL + multi-source, monitoring)
exercised through the TestClient with a fake pyodbc driver."""

import pytest
from fastapi.testclient import TestClient

from tests.fakes import FakeDbApiConnection

LSN1, LSN2 = bytes.fromhex("00" * 9 + "01"), bytes.fromhex("00" * 9 + "05")


@pytest.fixture(scope="module")
def client():
    from backend.api.app import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def fake_pyodbc(monkeypatch):
    conn = FakeDbApiConnection()
    import pyodbc
    monkeypatch.setattr(pyodbc, "connect", lambda *a, **k: conn)
    conn.add(r"@@VERSION", (["version"], [["Microsoft SQL Server 2022"]]))
    conn.add(r"FROM sys.databases", (["name", "cdc_enabled", "is_cdc_enabled"], [["db", 1, 1]]))
    conn.add(r"SELECT is_tracked_by_cdc", (["is_tracked_by_cdc"], [[1]]))
    conn.add(r"FROM sys.tables t", (["schema_name", "table_name", "full_name", "cdc_enabled", "row_count"], [["dbo", "T", "dbo.T", 1, 5]]))
    conn.add(r"FROM sys.columns c", (["column_name", "data_type", "is_nullable", "max_length", "is_primary_key"], [["Id", "int", 0, 4, 1]]))
    conn.add(r"cdc.change_tables", (["source_schema", "source_table", "capture_instance", "start_lsn", "create_date", "role_name"], [["dbo", "T", "dbo_T", LSN1, None, None]]))
    conn.add(r"fn_cdc_get_min_lsn", (["min_lsn"], [[LSN1]]))
    conn.add(r"fn_cdc_get_max_lsn", (["max_lsn"], [[LSN2]]))
    conn.add(r"fn_cdc_increment_lsn", (["next_lsn"], [[LSN2]]))
    conn.add(r"fn_cdc_get_all_changes", (["__$operation", "__$start_lsn", "__$seqval", "customer_id", "name"], [[2, LSN2, LSN1, 1, "a"]]))
    conn.add(r"SELECT TOP 10 \* FROM \(", (["id", "v"], [[1, "x"]]))
    conn.add(r"SELECT 1 AS id", (["id", "v"], [[1, "x"]]))
    conn.add(r"WITH", (["customer_id", "balance"], [[1, 2.0]]))
    conn.add(r"COUNT_BIG", (["count"], [[1]]))
    conn.add(r"TABLESAMPLE", (["Id"], [[1]]))
    conn.add(r"WHERE \[Id\]|WHERE Id", (["Id"], [[1]]))
    return conn


MSSQL = {"name": "legacy src", "server": "srv", "database": "db", "username": "u", "password": "p", "use_windows_auth": False}


def test_admin_connection_and_scan_endpoints(client, fake_pyodbc):
    assert client.post("/api/admin/connect/test", json=MSSQL).json()["success"]
    assert client.post("/api/admin/connect/set?connection_type=source", json=MSSQL).json()["success"]
    assert client.post("/api/admin/connect/set?connection_type=destination", json=MSSQL).json()["success"]
    assert client.get("/api/admin/scan/databases?connection_type=source").json()[0]["name"] == "db"
    assert client.get("/api/admin/scan/tables?connection_type=source").json()[0]["table_name"] == "T"
    assert client.get("/api/admin/scan/columns?connection_type=source&schema_name=dbo&table_name=T").json()[0]["is_primary_key"]
    assert client.get("/api/admin/cdc/status?connection_type=source").json()["cdc_enabled"]
    assert client.post("/api/admin/cdc/enable-db", json={"connection_type": "source"}).json()["success"]
    assert client.post("/api/admin/cdc/enable-table", json={"connection_type": "source", "schema_name": "dbo", "table_name": "T"}).json()["success"]
    assert client.post("/api/admin/cdc/enable-table", json={"connection_type": "source"}).status_code == 400
    assert client.post("/api/admin/cdc/check-status", json={"connection": MSSQL}).json()["cdc_enabled"]
    assert client.post("/api/admin/cdc/enable-database", json={"connection": MSSQL}).json()["success"]
    assert client.post("/api/admin/cdc/enable-table-with-connection", json={"connection": MSSQL, "schema_name": "dbo", "table_name": "T"}).json()["success"]
    assert client.post("/api/admin/cdc/enable-table-with-connection", json={"connection": MSSQL, "schema_name": "", "table_name": ""}).status_code == 400


def test_admin_mapping_test_query_merge_and_worksets(client, fake_pyodbc):
    sql_mapping = {"id": "lg_sql", "name": "sql", "mapping_type": "sql", "destination_schema": "dbo", "destination_table": "T",
                   "source_query": "SELECT 1 AS id, 'x' AS v", "sync_mode": "incremental"}
    assert client.post("/api/admin/mapping/create", json=sql_mapping).status_code == 200
    assert client.post("/api/admin/mapping/create", json=sql_mapping).status_code == 400   # duplicate
    assert client.get("/api/admin/mapping/list?mapping_type=sql").json()
    assert client.get("/api/admin/mapping/list?mapping_type=bogus").status_code == 400
    assert client.get("/api/admin/mapping/nope").status_code == 404
    bad = dict(sql_mapping, id="bad", source_query=None)
    assert client.post("/api/admin/mapping/create", json=bad).status_code == 400
    # workset with ids resolved from the library
    src = client.post("/api/admin/connection/save", json=dict(MSSQL, name="lg src")).json()["connection_id"]
    ws = {"id": "lg_ws", "name": "legacy ws", "source_connection_id": src, "destination_connection_id": src, "mappings": ["lg_sql"]}
    assert client.post("/api/admin/workset/create", json=ws).status_code == 200
    assert client.post("/api/admin/workset/create", json=ws).status_code == 400
    assert client.post("/api/admin/workset/create", json={"id": "x", "name": "x"}).status_code == 400     # no connections at all
    assert client.put("/api/admin/workset/update", json=dict(ws, name="renamed")).json()["success"]
    assert client.put("/api/admin/workset/update", json=dict(ws, id="missing")).status_code == 404
    assert client.get("/api/admin/workset/lg_ws").json()["name"] == "renamed"
    assert client.get("/api/admin/workset/missing").status_code == 404
    assert client.put("/api/admin/workset/activate/lg_ws").status_code == 200
    assert client.put("/api/admin/workset/activate/missing").status_code == 404
    active = client.get("/api/admin/workset/active").json()
    assert active["id"] == "lg_ws" and active["source_connection"]["server"] == "srv"
    # SQL mapping test runs against the active workset's source
    r = client.post("/api/admin/mapping/lg_sql/test").json()
    assert r["success"] and r["row_count"] == 1
    assert client.post("/api/admin/mapping/nope/test").status_code == 404
    r = client.post("/api/admin/mapping/test-query", json={"connection": MSSQL, "query": "SELECT 1 AS id, 'x' AS v", "limit": 5}).json()
    assert r["success"] and r["columns"] == ["id", "v"]
    # multi-source validate / preview
    multi = {"id": "lg_ms", "name": "ms", "mapping_type": "table", "destination_schema": "dbo", "destination_table": "Merged", "is_multi_source": True,
             "key_columns": ["customer_id"],
             "sources": [{"id": "s1", "connection_id": src, "alias": "crm", "schema": "dbo", "table": "Customers"},
                         {"id": "s2", "connection_id": src, "alias": "erp", "schema": "dbo", "table": "Accounts"}],
             "merge_pattern": {"operations": [{"type": "join", "left_source": "crm", "right_source": "erp", "join_type": "INNER", "on_condition": "crm.customer_id = erp.customer_id"}],
                               "output_columns": [{"source_alias": "crm", "source_column": "customer_id", "destination_column": "customer_id"}]}}
    assert client.post("/api/admin/mapping/validate-merge", json=multi).json()["is_valid"]
    prev = client.post("/api/admin/mapping/preview-merge", json={"mapping": multi, "connection_id": src, "limit": 3}).json()
    assert prev["success"] and prev["columns"] == ["customer_id", "balance"]
    assert client.post("/api/admin/mapping/preview-merge", json={"mapping": sql_mapping}).json()["success"] is False
    assert client.post("/api/admin/mapping/create", json=multi).status_code == 200
    # operations: SQL mapping execution paths
    r = client.post("/api/operations/mapping/lg_sql/execute").json()
    assert r["success"] and r["rows_processed"] == 1
    assert client.post("/api/operations/mapping/lg_ms/execute").status_code == 400
    assert client.post("/api/operations/mapping/nope/execute").status_code == 404
    r = client.post("/api/operations/mapping/execute-transactional", json=["lg_sql", "nope"]).json()
    assert r["success"] and "lg_sql" in r["results"]
    r = client.post("/api/operations/mapping/execute-all-sql").json()
    assert r["success"] and r["total_rows_processed"] >= 1
    # multi-source runtime with the fake driver
    r = client.post("/api/operations/multi-source/start?mapping_id=lg_ms")
    assert r.status_code == 200, r.text
    assert r.json()["source_count"] == 2
    st = client.get("/api/operations/multi-source/status").json()
    assert st["is_running"] and st["source_count"] == 2
    assert client.post("/api/operations/multi-source/pause").json()["success"]
    assert client.post("/api/operations/multi-source/resume").json()["success"]
    assert client.post("/api/operations/multi-source/stop").json()["success"]
    assert client.post("/api/operations/multi-source/stop").json()["success"]   # idempotent
    assert client.post("/api/operations/multi-source/pause").status_code == 400
    assert client.post("/api/operations/multi-source/start?mapping_id=lg_sql").status_code == 400
    assert client.post("/api/operations/multi-source/start?mapping_id=nope").status_code == 404
    assert client.post("/api/operations/multi-source/start?mapping_id=lg_ms&destination_connection_id=nope").status_code == 404
    # monitoring: verify uses the legacy pool, history endpoints
    table_mapping = {"id": "lg_tab", "name": "t", "mapping_type": "table", "source_schema": "dbo", "source_table": "T", "destination_schema": "dbo",
                     "destination_table": "U", "key_columns": ["Id"], "column_mappings": [{"source_column": "Id", "destination_column": "Id"}]}
    assert client.post("/api/admin/mapping/create", json=table_mapping).status_code == 200
    v = client.post("/api/monitoring/verify", json={"mapping_id": "lg_tab", "sample_size": 1}).json()
    assert v["mapping_id"] == "lg_tab" and v["samples_checked"] == 1
    assert client.post("/api/monitoring/verify", json={"mapping_id": "nope"}).status_code == 404
    assert client.get("/api/monitoring/verify/results/lg_tab").json()["mapping_id"] == "lg_tab"
    assert isinstance(client.get("/api/monitoring/errors").json(), list)
    assert isinstance(client.get("/api/monitoring/history?start_date=2024-01-01&end_date=2024-01-02").json(), list)
    assert client.get("/api/monitoring/history?start_date=nope").status_code == 400
    assert "total_events" in client.get("/api/monitoring/history/statistics").json()
    assert client.get("/api/monitoring/history/statistics?start_date=bad").status_code == 400
    assert client.get("/api/operations/latency/stats").json()["hours"] == 24
    assert "records" in client.get("/api/operations/latency/records").json()
    assert client.get("/api/operations/statistics").json()["monitor_status"]["poll_interval"] == "adaptive"
    assert client.delete("/api/admin/workset/lg_ws").status_code == 200
    assert client.delete("/api/admin/workset/lg_ws").status_code == 404
    for mid in ("lg_sql", "lg_ms", "lg_tab"):
        assert client.delete(f"/api/admin/mapping/{mid}").status_code == 200
    assert client.delete("/api/admin/mapping/lg_sql").status_code == 404


def test_operations_start_without_workset_or_mappings(client):
    for ws in client.get("/api/admin/workset/list").json():
        client.delete(f"/api/admin/workset/{ws['id']}")
    assert client.post("/api/operations/start").status_code == 400
    assert client.post("/api/operations/pause").status_code == 400
    assert client.post("/api/operations/resume").status_code == 400
    assert client.post("/api/operations/stop").json()["success"]
    assert client.get("/api/operations/status").json()["status"] == "stopped"


def test_duckdb_scripts_and_workflows_endpoints(client, tmp_path, monkeypatch):
    import backend.core.duckdb_script_manager as dsm
    monkeypatch.setattr(dsm, "SCRIPTS_BASE_DIR", tmp_path); monkeypatch.setattr(dsm, "TEMPLATES_DIR", tmp_path / "templates"); monkeypatch.setattr(dsm, "CUSTOM_DIR", tmp_path / "custom")
    (tmp_path / "templates").mkdir(); (tmp_path / "custom").mkdir()
    r = client.post("/api/admin/duckdb/scripts/save?script_name=api_script&content=SELECT%201%3B")
    assert r.status_code == 200, r.text
    assert client.post("/api/admin/duckdb/scripts/save?script_name=api_script&content=SELECT%201%3B").status_code == 400
    assert any(s["name"] == "api_script" for s in client.get("/api/admin/duckdb/scripts/list").json())
    assert "SELECT 1" in client.get("/api/admin/duckdb/scripts/api_script").json()["content"]
    assert client.get("/api/admin/duckdb/scripts/nope").status_code == 404
    assert client.post("/api/admin/duckdb/scripts/validate", json={"content": "SELECT (1"}).json()["valid"] is False
    assert client.delete("/api/admin/duckdb/scripts/api_script").status_code == 200
    assert client.delete("/api/admin/duckdb/scripts/api_script").status_code == 400
    # workflows
    node = lambda i, t, c=None: {"id": i, "type": t, "position": {"x": 0, "y": 0}, "config": c or {}}
    wf = {"id": "api_wf", "name": "wf", "nodes": [node("s", "start"), node("src", "source_table", {"schema": "dbo", "table": "T", "columns": ["Id"]}),
                                                node("d", "dest_table", {"schema": "dbo", "table": "U"})],
          "edges": [{"id": "e1", "source": "s", "target": "src"}, {"id": "e2", "source": "src", "target": "d"}]}
    assert client.post("/api/admin/workflow/visual/create", json=wf).status_code == 200
    assert client.post("/api/admin/workflow/visual/create", json=wf).status_code == 400
    assert client.get("/api/admin/workflow/visual/list").json()[0]["id"] == "api_wf"
    assert client.get("/api/admin/workflow/visual/api_wf").json()["name"] == "wf"
    assert client.get("/api/admin/workflow/visual/nope").status_code == 404
    assert client.put("/api/admin/workflow/visual/update", json=dict(wf, name="wf2")).status_code == 200
    assert client.post("/api/admin/workflow/validate/api_wf").json()["valid"]
    assert client.post("/api/admin/workflow/validate/nope").status_code == 404
    r = client.post("/api/admin/workflow/compile/api_wf").json()
    assert r["success"] and r["mapping_id"] == "mapping_api_wf"
    assert client.get("/api/admin/mapping/mapping_api_wf").status_code == 200
    assert client.post("/api/admin/workflow/compile/nope").status_code == 404
    assert client.delete("/api/admin/workflow/visual/api_wf").status_code == 200
    assert client.delete("/api/admin/workflow/visual/api_wf").status_code == 404
    client.delete("/api/admin/mapping/mapping_api_wf")


def test_ingestion_endpoints(client, fake_pyodbc, monkeypatch):
    import backend.core.data_ingestion_engine as die
    die.ingestion_engine.detectors = {}
    pat = die.IngestionPattern(id="case", name="case", description="", type=die.PatternType.CASE_NORMALIZATION)
    die.ingestion_engine.detectors["case"] = die.CaseMismatchDetector(pat)
    assert client.get("/api/admin/ingestion/patterns").json()["patterns"][0]["id"] == "case"
    r = client.post("/api/admin/ingestion/test-pattern", json={"pattern_id": "case", "sample_data": [{"a": "ABC", "b": "abc"}, {"a": "x", "b": "x"}]}).json()
    assert r["success"] and r["result"]["matches_found"] == 1
    assert client.post("/api/admin/ingestion/test-pattern", json={"pattern_id": "nope", "sample_data": []}).status_code == 404
    src = client.post("/api/admin/connection/save", json=dict(MSSQL, name="ing src")).json()["connection_id"]
    fake_pyodbc.add(r"FROM dbo.A", (["id", "name"], [[1, "ANN"]]))
    fake_pyodbc.add(r"FROM dbo.B", (["id", "name"], [[1, "ann"]]))
    body = {"sources": [{"connection_id": src, "schema_name": "dbo", "table_name": "A", "alias": "a", "columns": ["id", "name"]},
                        {"connection_id": src, "schema_name": "dbo", "table_name": "B", "alias": "b", "columns": ["id", "name"]}],
            "column_mappings": [{"name": "name", "source_columns": {"a": "name", "b": "name"}}], "join_keys": ["id"], "apply_patterns": ["case"]}
    r = client.post("/api/admin/ingestion/analyze", json=body).json()
    r = r.get("result", r)
    assert r["unmatched_records"] == 1 and r["mismatches"][0]["detected_patterns"] == ["case"]
    monkeypatch.setenv("CDCSYNC_CONFIG_DIR", "/nonexistent")
    assert client.post("/api/admin/ingestion/patterns/reload").json()["success"]


def test_monitoring_websocket_ping(client):
    with client.websocket_connect("/api/monitoring/events") as ws:
        ws.send_text("ping")
        assert ws.receive_text() == "pong"
    assert client.get("/api/monitoring/health").json()["websocket_connections"] == 0
