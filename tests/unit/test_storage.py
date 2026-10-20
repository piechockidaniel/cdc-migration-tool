import json
import multiprocessing
import threading

import pytest

from backend.storage import base
from backend.storage.base import ConflictError, Document
from backend.storage.factory import open_store
from backend.storage.json_store import JsonFileConfigStore, atomic_write_json
from backend.storage.sqlite_store import SqliteConfigStore
from backend.storage.secrets import redact, resolve_secrets


@pytest.fixture(params=["sqlite", "json"])
def store(request, tmp_path):
    if request.param == "sqlite":
        s = SqliteConfigStore(tmp_path / "s.sqlite3")
    else:
        s = JsonFileConfigStore(tmp_path / "json")
    yield s
    s.close()


def test_put_get_delete_roundtrip(store):
    doc = store.put("mappings", "m1", {"name": "x", "n": 1})
    assert doc.version == 1
    got = store.get("mappings", "m1")
    assert got.body == {"name": "x", "n": 1} and got.version == 1
    doc2 = store.put("mappings", "m1", {"name": "y"})
    assert doc2.version == 2
    assert [d.key for d in store.get_all("mappings")] == ["m1"]
    assert store.delete("mappings", "m1") is True
    assert store.delete("mappings", "m1") is False
    assert store.get("mappings", "m1") is None


def test_optimistic_concurrency(store):
    store.put("c", "k", {"a": 1}, expected_version=0)
    with pytest.raises(ConflictError):
        store.put("c", "k", {"a": 2}, expected_version=0)   # already exists
    store.put("c", "k", {"a": 2}, expected_version=1)
    with pytest.raises(ConflictError):
        store.put("c", "k", {"a": 3}, expected_version=1)   # stale writer loses
    assert store.get("c", "k").body == {"a": 2}
    with pytest.raises(ConflictError):
        store.delete("c", "k", expected_version=1)


def test_put_many_and_batch(store):
    with store.batch() as docs:
        docs.append(Document("w", "a", {"x": 1}))
        docs.append(Document("w", "b", {"x": 2}))
    assert {d.key for d in store.get_all("w")} == {"a", "b"}


def test_threads_do_not_lose_updates(store):
    store.put("cnt", "k", {"n": 0})
    errors = []

    def worker():
        for _ in range(50):
            while True:
                doc = store.get("cnt", "k")
                try:
                    store.put("cnt", "k", {"n": doc.body["n"] + 1}, expected_version=doc.version)
                    break
                except ConflictError:
                    continue
    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert store.get("cnt", "k").body["n"] == 300


def _proc_increment(kind, path, n):
    s = SqliteConfigStore(path) if kind == "sqlite" else JsonFileConfigStore(path)
    for _ in range(n):
        while True:
            doc = s.get("cnt", "k")
            try:
                s.put("cnt", "k", {"n": doc.body["n"] + 1}, expected_version=doc.version)
                break
            except ConflictError:
                continue
    s.close()


@pytest.mark.parametrize("kind", ["sqlite", "json"])
def test_multiprocess_safety(kind, tmp_path):
    """Several OS processes hammer the same store: no update may be lost."""
    path = tmp_path / ("s.sqlite3" if kind == "sqlite" else "json")
    s = SqliteConfigStore(path) if kind == "sqlite" else JsonFileConfigStore(path)
    s.put("cnt", "k", {"n": 0})
    s.close()
    ctx = multiprocessing.get_context("spawn")
    procs = [ctx.Process(target=_proc_increment, args=(kind, str(path), 25)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
    s = SqliteConfigStore(path) if kind == "sqlite" else JsonFileConfigStore(path)
    assert s.get("cnt", "k").body["n"] == 100
    s.close()


def test_sqlite_history_tracks_changes(tmp_path):
    s = SqliteConfigStore(tmp_path / "h.sqlite3")
    s.put("mappings", "m", {"v": 1}, updated_by="alice")
    s.put("mappings", "m", {"v": 2}, updated_by="bob")
    s.delete("mappings", "m")
    hist = s.history("mappings", "m")
    assert [h.body["_action"] for h in hist] == ["delete", "update", "insert"]
    assert hist[1].updated_by == "bob" and hist[1].body["v"] == 2


def test_atomic_write_leaves_no_partial_file(tmp_path):
    target = tmp_path / "x.json"
    atomic_write_json(target, {"a": 1})
    assert json.loads(target.read_text()) == {"a": 1}

    class Exploding:
        def __str__(self):
            raise RuntimeError("boom mid-serialisation")

    # a failing serialisation must neither clobber the existing file nor leave temp files behind
    with pytest.raises(RuntimeError):
        atomic_write_json(target, {"a": Exploding()})
    assert json.loads(target.read_text()) == {"a": 1}
    assert not list(tmp_path.glob(".x.json.*.tmp"))


def test_json_store_sees_external_modification(tmp_path):  # noqa
    s1 = JsonFileConfigStore(tmp_path / "j")
    s2 = JsonFileConfigStore(tmp_path / "j")
    s1.put("c", "k", {"a": 1})
    assert s2.get("c", "k").body == {"a": 1}
    s2.put("c", "k", {"a": 2})
    assert s1.get("c", "k").body == {"a": 2}   # mtime based invalidation


def test_migration_from_legacy_files(tmp_path):
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "connections.json").write_text(json.dumps({"src": {"id": "src", "server": "s", "database": "d"}}))
    (cfg / "mappings.json").write_text(json.dumps({"m1": {"id": "m1", "name": "m1", "mapping_type": "table",
                                                          "destination_schema": "dbo", "destination_table": "t"}}))
    (cfg / "lsn_state.json").write_text(json.dumps({"dbo.t": "0000002a000001f80003"}))
    store = open_store(cfg, backend="sqlite")
    assert store.get(base.CONNECTIONS, "src").body["server"] == "s"
    assert store.get(base.MAPPINGS, "m1").body["destination_table"] == "t"
    assert store.get(base.POSITIONS, "dbo.t").body == {"kind": "mssql_lsn", "value": "0000002a000001f80003"}
    assert not (cfg / "connections.json").exists()
    assert list(cfg.glob("connections.json.migrated-*"))
    # second open: nothing to migrate, nothing lost
    store2 = open_store(cfg, backend="sqlite")
    assert len(store2.get_all(base.CONNECTIONS)) == 1
    store.close(); store2.close()


def test_open_store_rejects_unknown_backend(tmp_path):
    with pytest.raises(ValueError):
        open_store(tmp_path, backend="etcd")


def test_secret_resolution_and_redaction(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PW", "s3cret")
    f = tmp_path / "pw"
    f.write_text("filepw\n")
    doc = {"username": "u", "password": "${env:DB_PW}", "nested": {"token": f"${{file:{f}}}"}, "plain": "x"}
    resolved = resolve_secrets(doc)
    assert resolved["password"] == "s3cret" and resolved["nested"]["token"] == "filepw"
    red = redact({"password": "abc", "api_key": "k", "name": "n", "password_ref": "${env:DB_PW}"})
    assert red == {"password": "***", "api_key": "***", "name": "n", "password_ref": "${env:DB_PW}"}
    with pytest.raises(KeyError):
        resolve_secrets({"password": "${env:MISSING_VAR_XYZ}"})
