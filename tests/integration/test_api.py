"""API level tests: the whole stack with a fake source engine and a SQLite destination."""
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from backend.connectors import ENGINES
from backend.connectors.base import Operation
from tests.fakes import FakeConnector, make_mapping


@pytest.fixture(scope="module")
def client():
    from backend.api.app import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def fake_engine(tmp_path, monkeypatch):
    """Register a 'fake' engine whose instances share one in-memory source."""
    src = FakeConnector({"id": "fake-src"})
    src.add_table("dbo", "Customers", [{"CustomerID": i, "Name": f"n{i}", "Email": f"{i}@x", "City": "c"} for i in range(1, 301)], ["CustomerID"])
    src.position_counter = 10

    class SharedFake(FakeConnector):
        def __new__(cls, config):
            return src
    monkeypatch.setitem(ENGINES, "fake", SharedFake)
    db_path = tmp_path / "dest.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);")
    conn.close()
    return src, str(db_path)


def wait_for(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _setup_connections_and_mapping(client, db_path, mapping_id="m_api"):
    r = client.post("/api/admin/connection/save", json={"name": "fake src", "server": "x", "database": "db", "engine": "fake", "use_windows_auth": False})
    assert r.status_code == 200, r.text
    src_id = r.json()["connection_id"]
    r = client.post("/api/admin/connection/save", json={"name": "sqlite dst", "server": "x", "database": "db", "engine": "sqlite",
                                                       "use_windows_auth": False, "options": {"path": db_path}})
    dst_id = r.json()["connection_id"]
    m = make_mapping(mapping_id)
    r = client.post("/api/admin/mapping/create", json=m.model_dump(mode="json"))
    assert r.status_code == 200, r.text
    return src_id, dst_id


def test_health_metrics_and_engines(client):
    assert client.get("/health").json()["status"] == "healthy"
    assert "cdcsync_" in client.get("/metrics").text
    assert "metrics" in client.get("/api/metrics").json()
    engines = {e["engine"] for e in client.get("/api/connectors/engines").json()}
    assert {"mssql", "postgresql", "mariadb", "mongodb", "oracle"} <= engines


def test_connector_endpoints_with_inline_connection(client, fake_engine):
    src, db_path = fake_engine
    body = {"connection": {"engine": "fake", "id": "x"}, "tables": ["dbo.Customers"]}
    assert client.post("/api/connectors/test", json=body).json()["success"]
    pre = client.post("/api/connectors/prerequisites", json=body).json()
    assert pre["ready"] and pre["checks"][0]["name"] == "connection"
    assert client.post("/api/connectors/tables", json=body).json()[0]["key"] == "dbo.Customers"
    cols = client.post("/api/connectors/columns", json=body).json()["dbo.Customers"]
    assert any(c["name"] == "CustomerID" and c["is_primary_key"] for c in cols)
    assert client.post("/api/connectors/capture/status", json=body).json()["cdc_enabled"]
    assert client.post("/api/connectors/capture/enable-tables", json=body).json()["success"]
    assert client.post("/api/connectors/position", json=body).json()["position"]["value"] == 10
    assert client.post("/api/connectors/prerequisites", json={"connection_id": "nope"}).status_code == 404
    assert client.post("/api/connectors/prerequisites", json={"tables": ["bad"]}).status_code == 400
    assert "cdcsync_notifier" in client.get("/api/connectors/mssql/service-broker-script").json()["sql"]
    assert client.post("/api/connectors/test", json={"connection": {"engine": "nope"}}).status_code == 400


def test_pipeline_and_snapshot_lifecycle_via_api(client, fake_engine):
    src, db_path = fake_engine
    src_id, dst_id = _setup_connections_and_mapping(client, db_path, "m_pipe")
    # snapshot first
    r = client.post("/api/snapshots", json={"mapping_id": "m_pipe", "source_connection_id": src_id, "destination_connection_id": dst_id,
                                            "pipeline_id": "p_api", "chunk_size": 100, "max_chunk_size": 100})
    assert r.status_code == 200, r.text
    job_id = r.json()["id"]
    assert wait_for(lambda: client.get(f"/api/snapshots/{job_id}").json()["state"] == "completed")
    job = client.get(f"/api/snapshots/{job_id}").json()
    assert job["rows_copied"] == 300 and job["chunks_done"] == 3 and job["progress"] == 1.0
    assert client.get("/api/snapshots?state=completed").json()[0]["id"] == job_id
    assert client.get("/api/snapshots?state=bogus").status_code == 400
    # CDC pipeline starts from the snapshot position
    r = client.post("/api/pipelines", json={"id": "p_api", "source_connection_id": src_id, "destination_connection_id": dst_id, "mapping_ids": ["m_pipe"]})
    assert r.status_code == 200, r.text
    st = client.post("/api/pipelines/p_api/start").json()
    assert st["state"] == "running" and st["positions"]["__source__"]["value"] == 10
    src.script("dbo", "Customers", Operation.UPDATE, {"CustomerID": 1, "Name": "changed", "Email": "", "City": ""})
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 999, "Name": None, "Email": "", "City": ""})  # poison row
    assert wait_for(lambda: client.get("/api/pipelines/p_api/status").json()["events_applied"] + client.get("/api/pipelines/p_api/status").json()["events_failed"] == 2)
    status = client.get("/api/pipelines/p_api/status").json()
    assert status["events_failed"] == 1 and status["positions"]["__source__"]["value"] == 12
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT name FROM customers WHERE id = 1").fetchone()[0] == "changed"
    conn.close()
    # pause / resume / health
    assert client.post("/api/pipelines/p_api/pause").json()["state"] == "paused"
    assert client.post("/api/pipelines/p_api/resume").json()["state"] == "running"
    assert client.get("/api/pipelines/p_api/health").json()["pipeline"]["events_failed"] == 1
    assert any(p["runtime"] and p["runtime"]["id"] == "p_api" for p in client.get("/api/pipelines").json())
    # DLQ: failure recorded, classified, replay still fails, resolve manually
    failures = client.get("/api/audit/failures").json()
    assert failures and failures[0]["dest_table"] == "main.customers"
    patterns = client.get("/api/audit/patterns").json()
    assert patterns["summary"]["failures"] >= 1 and patterns["patterns"][0]["category"] == "fixable"
    rep = client.post("/api/audit/failures/replay", json={"ids": [failures[0]["id"]], "pipeline_id": "p_api"}).json()
    assert rep == {"replayed": 0, "still_failing": 1}
    assert client.get(f"/api/audit/failures/{failures[0]['id']}").json()["attempts"] == 2
    assert client.post("/api/audit/failures/resolve", json={"ids": [failures[0]["id"]], "resolution": "dropped"}).json()["resolved"] == 1
    assert client.get("/api/audit/failures").json() == []
    assert client.get("/api/audit/stats").json()["applied"] >= 1
    assert client.get("/api/audit/events?category=batch").json()
    # positions reset refused while running, ok after stop
    assert client.post("/api/pipelines/p_api/positions/reset").status_code == 409
    assert client.post("/api/pipelines/p_api/stop").json()["state"] == "stopped"
    assert client.post("/api/pipelines/p_api/positions/reset").json()["removed"] == ["p_api:__source__"]
    assert client.delete("/api/pipelines/p_api").json()["success"]
    assert client.get("/api/pipelines/p_api/status").status_code == 404
    assert client.delete(f"/api/snapshots/{job_id}").json()["success"]
    assert client.get("/api/pipelines/bus/stats").json()["published"] >= 2


def test_alert_rule_management(client):
    sets = client.get("/api/alerts/rule-sets").json()
    assert {s["id"] for s in sets} >= {"pipeline-health", "data-loss-guard"}
    bad = client.put("/api/alerts/rules", json={"id": "x", "name": "x", "condition": "pipeline.lag >>"})
    assert bad.status_code == 400
    ok = client.put("/api/alerts/rules", json={"id": "custom_lag", "name": "custom", "condition": "pipeline.lag_seconds > 1", "scope": "per_pipeline"})
    assert ok.status_code == 200
    assert any(r["id"] == "custom_lag" for r in client.get("/api/alerts/rules").json())
    assert client.put("/api/alerts/rule-sets", json={"enabled": ["custom", "data-loss-guard"]}).json()["enabled"] == ["custom", "data-loss-guard"]
    ev = client.post("/api/alerts/evaluate").json()
    assert "global" in ev["contexts"]
    snap = client.get("/api/alerts").json()
    assert snap["enabled_rule_sets"] == ["custom", "data-loss-guard"]
    assert client.delete("/api/alerts/rules/custom_lag").json()["success"]
    assert client.put("/api/alerts/rule-sets", json={"enabled": None}).status_code == 200
    assert "log" in client.post("/api/alerts/test").json()["sent_to"]
    assert client.put("/api/alerts/channels", json={"webhook_url": "http://localhost:1/hook"}).json()["channels"].count("webhook") == 1
    client.put("/api/alerts/channels", json={"webhook_url": None})


def test_legacy_operations_endpoints_use_pipeline(client, fake_engine):
    src, db_path = fake_engine
    m = make_mapping("m_legacy")
    client.post("/api/admin/mapping/create", json=m.model_dump(mode="json"))
    ws = {"id": "ws_legacy", "name": "legacy", "mappings": ["m_legacy"],
          "source_connection": {"name": "s", "server": "x", "database": "db", "engine": "fake", "use_windows_auth": False},
          "destination_connection": {"name": "d", "server": "x", "database": "db", "engine": "sqlite", "use_windows_auth": False, "options": {"path": db_path}}}
    assert client.post("/api/admin/workset/create", json=ws).status_code == 200
    assert client.put("/api/admin/workset/activate/ws_legacy").status_code == 200
    r = client.post("/api/operations/start")
    assert r.status_code == 200, r.text
    assert r.json()["pipeline_id"] == "ws_ws_legacy"
    assert client.post("/api/operations/start").status_code == 409
    st = client.get("/api/operations/status").json()
    assert st["status"] == "running" and st["working_set_id"] == "ws_legacy"
    assert client.post("/api/operations/pause").json()["success"]
    assert client.get("/api/operations/status").json()["status"] == "paused"
    assert client.post("/api/operations/resume").json()["success"]
    stats = client.get("/api/operations/statistics").json()
    assert stats["monitor_status"]["is_running"] and stats["monitor_status"]["poll_interval"] == "adaptive"
    assert client.post("/api/operations/stop").json()["success"]
    assert client.get("/api/operations/status").json()["status"] == "stopped"
    assert client.get("/api/monitoring/health").json()["status"] == "healthy"


def test_admin_test_connection_routes_non_mssql_engines(client, fake_engine):  # fake_engine registers the engine
    r = client.post("/api/admin/connect/test", json={"name": "f", "server": "x", "database": "d", "engine": "fake", "use_windows_auth": False})
    assert r.status_code == 200 and r.json()["success"] and r.json()["server_version"] == "fake 1.0"


def test_monitoring_statistics_aggregate_pipelines(client, fake_engine):
    src, db_path = fake_engine
    src_id, dst_id = _setup_connections_and_mapping(client, db_path, "m_stats")
    client.post("/api/pipelines", json={"id": "p_stats", "source_connection_id": src_id, "destination_connection_id": dst_id, "mapping_ids": ["m_stats"]})
    client.post("/api/pipelines/p_stats/start")
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 5000, "Name": "x", "Email": "", "City": ""})
    assert wait_for(lambda: client.get("/api/pipelines/p_stats/status").json()["events_applied"] >= 1)
    st = client.get("/api/monitoring/statistics").json()
    assert st["successful_events"] >= 1 and st["uptime_seconds"] > 0
    assert client.get("/api/monitoring/health").json()["pipelines"]["p_stats"] == "running"
    client.post("/api/pipelines/p_stats/stop")


def test_connection_library_redacts_secrets_and_keeps_them_on_update(client):
    r = client.post("/api/admin/connection/save", json={"name": "secret conn", "server": "s", "database": "d", "use_windows_auth": False,
                                                       "username": "u", "password": "hunter2"})
    cid = r.json()["connection_id"]
    assert client.get(f"/api/admin/connection/{cid}").json()["password"] == "***"
    assert all(c.get("password") in (None, "***", "") or c["password"].startswith("${") for c in client.get("/api/admin/connection/list").json())
    r = client.put("/api/admin/connection/update", json={"id": cid, "name": "secret conn", "server": "s2", "database": "d", "use_windows_auth": False,
                                                        "username": "u", "password": "***"})
    assert r.status_code == 200
    from backend.core.config_manager import config_manager
    stored = config_manager.get_connection(cid)
    assert stored["password"] == "hunter2" and stored["server"] == "s2"


def test_pattern_apply_endpoint(client):
    from backend.core import runtime
    from backend.connectors.base import ChangeEvent, Position
    m = make_mapping("m_pattern", dest_table="customers")
    client.post("/api/admin/mapping/create", json=m.model_dump(mode="json"))
    ev = ChangeEvent(source="s", schema="dbo", table="Customers", operation=Operation.INSERT, position=Position("fake", 1), after={"CustomerID": 1})
    runtime.audit.record_failure(ev, "main.customers", "Cannot insert the value NULL into column 'name', table 'x'", "sig-null-name")
    r = client.post("/api/audit/patterns/apply", json={"signature": "sig-null-name", "action": "set_default_value", "mapping_id": "m_pattern",
                                                      "params": {"value": "unknown"}})
    assert r.status_code == 200, r.text
    assert r.json()["change"]["column"] == "name" and r.json()["resolved_failures"] == 1
    updated = client.get("/api/admin/mapping/m_pattern").json()
    assert next(c for c in updated["column_mappings"] if c["destination_column"] == "name")["default_value"] == "unknown"
    assert client.post("/api/audit/patterns/apply", json={"signature": "nope", "action": "disable_mapping"}).status_code == 404
    assert client.post("/api/audit/patterns/apply", json={"signature": "sig-null-name", "action": "add_transformation", "mapping_id": "m_pattern"}).status_code == 400


def test_destination_adapter_browses_sqlite(client, fake_engine):
    src, db_path = fake_engine
    r = client.post("/api/admin/connection/save", json={"name": "browse dst", "server": "x", "database": "d", "engine": "sqlite", "use_windows_auth": False, "options": {"path": db_path}})
    cid = r.json()["connection_id"]
    tables = client.post("/api/connectors/tables", json={"connection_id": cid}).json()
    assert any(t["key"] == "main.customers" for t in tables)
    cols = client.post("/api/connectors/columns", json={"connection_id": cid, "tables": ["main.customers"]}).json()["main.customers"]
    assert any(c["name"] == "id" and c["is_primary_key"] for c in cols)
    assert client.post("/api/connectors/test", json={"connection_id": cid}).json()["success"]
    pre = client.post("/api/connectors/prerequisites", json={"connection_id": cid, "tables": ["main.customers"]}).json()
    assert pre["ready"]
