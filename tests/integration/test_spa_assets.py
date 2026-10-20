"""The SPA must be served with all its modules; page modules must be valid ES modules."""
import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "frontend" / "app"


@pytest.fixture(scope="module")
def client():
    from backend.api.app import app
    with TestClient(app) as c:
        yield c


def test_spa_index_and_assets_are_served(client):
    r = client.get("/")
    assert r.status_code == 200 and 'id="app"' in r.text and "/app/js/main.js" in r.text
    for path in ["/app/js/main.js", "/app/js/api.js", "/app/js/store.js", "/app/js/ui.js", "/app/js/help.js", "/app/css/app.css",
                 "/app/vendor/vue.global.prod.js", "/app-docs/user-guide/02-ui.md", "/app/vendor/chart.umd.js", "/static/bootstrap/dist/css/bootstrap.min.css"]:
        assert client.get(path).status_code == 200, path
    # legacy pages and redirects for old bookmarks
    assert client.get("/legacy/admin").status_code == 200
    assert client.get("/admin", follow_redirects=False).status_code == 307


def test_every_page_module_exists_and_is_imported():
    main = (APP / "js" / "main.js").read_text()
    for page in ["dashboard", "connections", "mappings", "pipelines", "snapshots", "alerts", "failures", "audit", "metrics", "settings"]:
        assert (APP / "js" / "pages" / f"{page}.js").exists(), page
        assert f"./pages/{page}.js" in main
        # every page carries a context hint; every tour step has an anchor somewhere in the UI
        assert "<help-hint" in (APP / "js" / "pages" / f"{page}.js").read_text(), f"{page}: no help hint"
    import re
    targets = re.findall(r"target: '([a-z-]+)'", (APP / "js" / "help.js").read_text())
    ui_text = (APP / "index.html").read_text() + "".join(f.read_text() for f in (APP / "js" / "pages").glob("*.js"))
    for t in targets:
        assert f'data-tour="{t}"' in ui_text, f"tour step target {t} has no data-tour anchor"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_page_modules_parse_as_es_modules():
    for f in list((APP / "js").glob("*.js")) + list((APP / "js" / "pages").glob("*.js")):
        proc = subprocess.run(["node", "--input-type=module", "--check"], input=f.read_bytes(), capture_output=True)
        assert proc.returncode == 0, f"{f.name}: {proc.stderr.decode()[:300]}"


def test_new_backend_endpoints_for_the_ui(client):
    assert client.get("/api/settings/info").json()["version"] == "2.0.0"
    exp = client.get("/api/settings/export").json()
    assert set(exp) >= {"mappings", "connections", "pipelines", "alert_rules", "positions"}
    imp = client.post("/api/settings/import", json={"data": {"alert_rules": {"ui_rule": {"id": "ui_rule", "name": "ui", "condition": "False"}}}, "overwrite": True}).json()
    assert imp["imported"] == 1
    assert any(r["id"] == "ui_rule" for r in client.get("/api/alerts/rules").json())
    v = client.post("/api/alerts/rules/validate", json={"condition": "failures.total_1h >= 0"}).json()
    assert v["valid"] and v["results"]["global"] is True
    assert client.post("/api/alerts/rules/validate", json={"condition": "1 +"}).json()["valid"] is False
    h = client.get("/api/metrics/history?minutes=5").json()
    assert "series" in h and h["interval_s"] > 0
