"""Throughput benchmarks (``pytest -m perf -s``). Numbers are printed, thresholds are loose sanity bounds."""
import time

import pytest

from backend.connectors.base import ChangeEvent, Operation, Position
from backend.core import metrics
from backend.core.applier import RowApplier
from backend.core.audit.store import AuditStore
from backend.core.event_bus import EventBus
from backend.core.pipeline import ChangePipeline
from tests.fakes import FakeConnector, make_mapping, sqlite_destination

pytestmark = pytest.mark.perf


class MemoryPositions:
    def __init__(self):
        self.data = {}

    def get_position(self, key):
        return self.data.get(key)

    def set_position(self, key, kind, value, extra=None):
        self.data[key] = {"kind": kind, "value": value}


def _events(n):
    return [ChangeEvent(source="s", schema="dbo", table="Customers", operation=Operation.INSERT, position=Position("fake", i),
                        after={"CustomerID": i, "Name": f"n{i}", "Email": f"{i}@x", "City": "c"}) for i in range(1, n + 1)]


def test_applier_batch_throughput(capsys):
    dest = sqlite_destination()
    applier = RowApplier(dest)
    applier.register_mappings([make_mapping()])
    n = 50_000
    evs = _events(n)
    t0 = time.perf_counter()
    for i in range(0, n, 1000):
        r = applier.apply(evs[i:i + 1000])
        assert r.failed == 0
    dt = time.perf_counter() - t0
    rate = n / dt
    with capsys.disabled():
        print(f"\napplier upsert throughput (sqlite, batch=1000): {rate:,.0f} rows/s")
    assert rate > 5_000
    h = metrics.APPLY_LATENCY.labels(table="main.customers").snapshot()
    assert h["count"] >= 50


def test_pipeline_end_to_end_throughput(capsys, tmp_path):
    src = FakeConnector({"id": "src"})
    dest = sqlite_destination()
    audit = AuditStore(tmp_path / "a.sqlite3")
    p = ChangePipeline("perf", src, dest, [make_mapping()], MemoryPositions(), audit=audit, bus=EventBus(default_maxsize=50_000),
                       batch_size=2000, batch_wait_s=0.05)
    n = 30_000
    for ev in _events(n):
        src.scripted.append(ev)
    src.position_counter = n
    t0 = time.perf_counter()
    p.start()
    while p.events_applied < n and time.perf_counter() - t0 < 60:
        time.sleep(0.05)
    dt = time.perf_counter() - t0
    p.stop()
    with capsys.disabled():
        print(f"pipeline end-to-end (fake source -> bus -> applier -> sqlite): {n/dt:,.0f} events/s, batches={p.batches}")
    assert p.events_applied == n and dest.row_count("main", "customers") == n


def test_audit_store_write_throughput(capsys, tmp_path):
    audit = AuditStore(tmp_path / "a.sqlite3", flush_interval_s=1.0, buffer_size=1000)
    n = 20_000
    t0 = time.perf_counter()
    for i in range(n):
        audit.record("batch", "applier", "x", table="t", payload={"applied": 1, "failed": 0, "duration_ms": 1})
    audit.flush()
    dt = time.perf_counter() - t0
    with capsys.disabled():
        print(f"audit store buffered writes: {n/dt:,.0f} records/s")
    assert audit.stats(1)["applied"] == n
    audit.close()
