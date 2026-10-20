import time
from datetime import datetime

import pytest

from backend.core.snapshot_manager import MaintenanceWindow, SnapshotManager
from backend.storage.base import SNAPSHOT_JOBS
from backend.storage.sqlite_store import SqliteConfigStore
from tests.fakes import FakeConnector, make_mapping, sqlite_destination


class MemoryPositions:
    def __init__(self):
        self.data = {}

    def get_position(self, key):
        return self.data.get(key)

    def set_position(self, key, kind, value, extra=None):
        self.data[key] = {"kind": kind, "value": value, **(extra or {})}


def wait_for(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


def build(tmp_path, rows=1000, dest=None, src=None, **kw):
    store = SqliteConfigStore(tmp_path / "s.sqlite3")
    src = src or FakeConnector({"id": "src"})
    if rows:
        src.add_table("dbo", "Customers", [{"CustomerID": i, "Name": f"n{i}", "Email": f"{i}@x", "City": "c"} for i in range(1, rows + 1)],
                      ["CustomerID"])
        src.position_counter = 77
    dest = dest or sqlite_destination()
    mapping = make_mapping()
    positions = MemoryPositions()
    mgr = SnapshotManager(store, connector_factory=lambda cid: src, destination_factory=lambda cid: dest,
                          mapping_lookup=lambda mid: mapping if mid == mapping.id else None, position_store=positions, **kw)
    return mgr, src, dest, mapping, positions, store


def test_maintenance_window_logic():
    w = MaintenanceWindow("22:00", "06:00", [0, 1, 2, 3, 4])
    assert w.contains(datetime(2024, 1, 1, 23, 0))       # Monday 23:00
    assert w.contains(datetime(2024, 1, 2, 5, 30))
    assert not w.contains(datetime(2024, 1, 1, 12, 0))
    assert not w.contains(datetime(2024, 1, 6, 23, 0))   # Saturday
    assert MaintenanceWindow("09:00", "17:00").contains(datetime(2024, 1, 1, 10, 0))


def test_snapshot_runs_in_chunks_and_hands_off_position(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=1000)
    job = mgr.create_job(mapping, "src", "dst", pipeline_id="p1", chunk_size=100, max_chunk_size=100)
    assert job.key_columns == ["CustomerID"] and job.estimated_rows == 1000 and job.key_lo == [1] and job.key_hi == [1000]
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "completed"), mgr.get(job.id).to_json()
    job = mgr.get(job.id)
    assert job.rows_copied == 1000 and job.chunks_done == 10 and job.progress() == 1.0
    assert dest.row_count("", "customers") == 1000
    assert positions.data["p1:__source__"]["value"] == 77 and positions.data["p1:__source__"]["origin"] == f"snapshot:{job.id}"
    assert wait_for(lambda: store.get(SNAPSHOT_JOBS, job.id).body["state"] == "completed")   # persisted right after the in-memory flip
    persisted = store.get(SNAPSHOT_JOBS, job.id).body
    assert persisted["last_key"] == [1000]


def test_snapshot_pause_resume_and_restart_resumes_from_last_key(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=600)
    src.read_delay = 0.0
    job = mgr.create_job(mapping, "src", "dst", chunk_size=50, max_chunk_size=50, pause_between_chunks_ms=30)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).chunks_done >= 3)
    mgr.pause(job.id)
    assert mgr.get(job.id).state == "paused"
    done = mgr.get(job.id).chunks_done
    time.sleep(0.2)
    assert mgr.get(job.id).chunks_done <= done + 1
    # simulate process restart: new manager loads persisted job
    mgr.cancel(job.id)
    time.sleep(0.2)
    reads_before = src.snapshot_reads
    mgr2 = SnapshotManager(store, lambda c: src, lambda c: dest, lambda m: mapping, position_store=positions)
    job2 = mgr2.get(job.id)
    job2.state = "paused"  # cancelled -> allow resume for the test
    mgr2.resume(job.id)
    assert wait_for(lambda: mgr2.get(job.id).state == "completed")
    assert dest.row_count("", "customers") == 600
    assert src.snapshot_reads - reads_before < 600 // 50 + 2  # did not start over


def test_snapshot_is_idempotent_on_rerun_and_reset(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=120)
    job = mgr.create_job(mapping, "src", "dst", chunk_size=50)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "completed")
    mgr.reset(job.id)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "completed")
    assert dest.row_count("", "customers") == 120  # upserts, no duplicates


def test_snapshot_requires_key_columns_without_pk(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=0)
    src.add_table("dbo", "Customers", [{"CustomerID": 1, "Name": "", "Email": "", "City": ""}], pk=[])
    with pytest.raises(ValueError):
        mgr.create_job(mapping, "src", "dst")
    job = mgr.create_job(mapping, "src", "dst", key_columns=["CustomerID"])
    assert job.key_columns == ["CustomerID"]


def test_snapshot_retries_then_fails_and_reports_health(tmp_path):
    src = FakeConnector({"id": "src"})
    calls = {"n": 0}
    orig = src.read_snapshot_chunk

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("transient source error")
        if calls["n"] == 5:
            raise RuntimeError("fatal")
        return orig(*a, **k)
    src.read_snapshot_chunk = flaky
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=300, src=src)
    job = mgr.create_job(mapping, "src", "dst", chunk_size=100, max_chunk_size=100, max_retries=0)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "failed")
    j = mgr.get(job.id)
    assert j.chunk_errors >= 1 and "transient" in j.error
    ctx = mgr.health_contexts()[f"snapshot:{job.id}"]["snapshot"]
    assert ctx["state"] == "failed" and ctx["chunk_errors"] >= 1


def test_snapshot_waits_outside_window(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=10)
    job = mgr.create_job(mapping, "src", "dst", windows=[{"start": "00:00", "end": "00:00", "days": []}])
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).waiting_for_window, 3)
    assert mgr.get(job.id).rows_copied == 0
    mgr.cancel(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "cancelled")


def test_adaptive_chunk_size_shrinks_when_slow(tmp_path):
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=400)
    orig = src.read_snapshot_chunk

    def slow(*a, **k):
        time.sleep(0.05)
        return orig(*a, **k)
    src.read_snapshot_chunk = slow
    job = mgr.create_job(mapping, "src", "dst", chunk_size=200, min_chunk_size=50, max_chunk_seconds=0.01)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "completed")
    assert mgr.get(job.id).chunk_size < 200 and dest.row_count("", "customers") == 400


def test_snapshot_poison_rows_go_to_dlq_and_job_completes(tmp_path):
    from backend.core.audit.store import AuditStore
    audit = AuditStore(tmp_path / "a.sqlite3", flush_interval_s=0)
    dest = sqlite_destination("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);")
    src = FakeConnector({"id": "src"})
    rows = [{"CustomerID": i, "Name": None if i % 10 == 0 else f"n{i}", "Email": "", "City": ""} for i in range(1, 101)]
    src.add_table("dbo", "Customers", rows, ["CustomerID"])
    mgr, src, dest, mapping, positions, store = build(tmp_path, rows=0, dest=dest, src=src, audit=audit)
    job = mgr.create_job(mapping, "src", "dst", chunk_size=25, max_chunk_size=25)
    mgr.start(job.id)
    assert wait_for(lambda: mgr.get(job.id).state == "completed")
    j = mgr.get(job.id)
    assert j.rows_copied == 100 and j.rows_failed == 10 and j.chunk_errors == 0
    assert dest.row_count("", "customers") == 90
    assert len(audit.failures()) == 10
