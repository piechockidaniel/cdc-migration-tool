"""Shared fixtures.

The process-wide singletons (``config_manager``, ``audit``...) are created at
import time, so the config directory is redirected to a temporary folder
*before* anything from ``backend`` is imported.
"""
import os
import sys
import tempfile
from pathlib import Path

_TMP = tempfile.mkdtemp(prefix="cdcsync-tests-")
os.environ["CDCSYNC_CONFIG_DIR"] = str(Path(_TMP) / "config")
os.environ["CDCSYNC_HISTORY_DIR"] = str(Path(_TMP) / "history")
os.environ["CDCSYNC_CONFIG_BACKEND"] = "sqlite"
os.environ["CDCSYNC_DISABLE_BACKGROUND"] = "1"
os.environ["CDCSYNC_WORKFLOWS_DIR"] = str(Path(_TMP) / "workflows")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from tests.fakes import FakeConnector, make_mapping, sqlite_destination  # noqa: E402


@pytest.fixture
def tmp_config_dir(tmp_path):
    return tmp_path / "config"


@pytest.fixture
def sqlite_store(tmp_path):
    from backend.storage.sqlite_store import SqliteConfigStore
    store = SqliteConfigStore(tmp_path / "state.sqlite3")
    yield store
    store.close()


@pytest.fixture
def json_store(tmp_path):
    from backend.storage.json_store import JsonFileConfigStore
    store = JsonFileConfigStore(tmp_path / "json")
    yield store
    store.close()


@pytest.fixture
def audit_store(tmp_path):
    from backend.core.audit.store import AuditStore
    store = AuditStore(tmp_path / "audit.sqlite3", flush_interval_s=0.0)
    yield store
    store.close()


@pytest.fixture
def destination():
    return sqlite_destination()


@pytest.fixture
def fake_connector():
    return FakeConnector({"id": "src"})


@pytest.fixture
def mapping():
    return make_mapping()


@pytest.fixture
def metrics_registry():
    from backend.core import metrics
    return metrics.registry
