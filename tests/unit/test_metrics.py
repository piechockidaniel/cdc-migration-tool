import threading

import pytest

from backend.core.metrics import MetricsRegistry, timed_query


def test_counter_and_labels():
    reg = MetricsRegistry("t")
    c = reg.counter("events_total", "events", ["table", "op"])
    c.labels(table="a", op="INSERT").inc()
    c.labels(table="a", op="INSERT").inc(2)
    c.labels(table="b", op="DELETE").inc()
    assert c.labels(table="a", op="INSERT").value == 3
    assert c.labels(table="b", op="DELETE").value == 1
    with pytest.raises(ValueError):
        c.labels(table="x")           # wrong label set
    with pytest.raises(ValueError):
        c.labels(table="a", op="x").inc(-1)


def test_gauge_inprogress_and_histogram_percentiles():
    reg = MetricsRegistry("t")
    g = reg.gauge("inflight")
    g.inc()
    assert g.value == 1
    g.dec()
    assert g.value == 0
    h = reg.histogram("lat", buckets=(0.1, 1, 10))
    for v in [0.05, 0.2, 0.3, 5, 20]:
        h.observe(v)
    snap = h.snapshot()
    assert snap["count"] == 5 and snap["min"] == 0.05 and snap["max"] == 20
    assert snap["p50"] == 0.3
    buckets = dict(h.cumulative_buckets())
    assert buckets["0.1"] == 1 and buckets["1.0"] == 3 and buckets["10.0"] == 4 and buckets["+Inf"] == 5


def test_registry_export_formats_and_duplicate_registration():
    reg = MetricsRegistry("ns")
    reg.counter("a", "desc a", ["k"]).labels(k="v").inc()
    reg.histogram("h").observe(0.5)
    same = reg.counter("a", "desc a", ["k"])
    assert same is reg.get("a")
    with pytest.raises(ValueError):
        reg.gauge("a")
    text = reg.to_prometheus()
    assert '# TYPE ns_a counter' in text and 'ns_a{k="v"} 1.0' in text
    assert 'ns_h_bucket{le="+Inf"} 1' in text and "ns_h_count 1" in text
    d = reg.to_dict()
    assert d["metrics"]["a"]["series"][0]["labels"] == {"k": "v"}


def test_thread_safety_of_counters():
    reg = MetricsRegistry("t")
    c = reg.counter("n")

    def work():
        for _ in range(10_000):
            c.inc()
    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert c.value == 80_000


def test_timed_query_records_duration_and_roundtrip():
    from backend.core import metrics as m
    before = m.SOURCE_ROUNDTRIPS.labels(source="x", kind="k").value
    with timed_query("x", "k"):
        pass
    assert m.SOURCE_ROUNDTRIPS.labels(source="x", kind="k").value == before + 1
    assert m.DB_QUERY_DURATION.labels(connection="x", kind="k").snapshot()["count"] >= 1
