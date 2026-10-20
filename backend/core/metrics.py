"""Lightweight, dependency-free performance counters.

The registry is process-wide and thread-safe. It deliberately mirrors the
Prometheus data model (counter / gauge / histogram) so the ``/metrics`` endpoint
can expose the text exposition format and be scraped by Prometheus, while the
JSON view is used by the UI.

Design notes
------------
* Counters and gauges are plain floats protected by a lock - they are hit on
  the hot path (every CDC event) so they must stay cheap.
* Histograms keep a bounded reservoir (ring buffer) for percentile estimates
  plus cumulative ``sum``/``count`` and Prometheus-style buckets. The reservoir
  keeps the *most recent* N observations, which is what operators want when
  looking at p95/p99 of "now", not of the last 3 weeks.
* Labels are supported via a ``labels(**kv)`` child accessor, like the
  Prometheus client library.
"""
from __future__ import annotations

import math
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_DEFAULT_BUCKETS: Tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
)

LabelKey = Tuple[Tuple[str, str], ...]


def _label_key(labels: Dict[str, str]) -> LabelKey:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


class _Metric:
    kind = "untyped"

    def __init__(self, name: str, description: str = "", label_names: Sequence[str] = ()):
        self.name = name
        self.description = description
        self.label_names = tuple(label_names)
        self._lock = threading.Lock()
        self._children: Dict[LabelKey, "_Metric"] = {}

    # -- labelled children -------------------------------------------------
    def labels(self, **labels: str) -> "_Metric":
        if set(labels) != set(self.label_names):
            raise ValueError(
                f"metric {self.name} expects labels {self.label_names}, got {tuple(labels)}"
            )
        key = _label_key(labels)
        with self._lock:
            child = self._children.get(key)
            if child is None:
                child = self._new_child()
                self._children[key] = child
            return child

    def _new_child(self) -> "_Metric":  # pragma: no cover - overridden
        raise NotImplementedError

    def iter_series(self) -> Iterable[Tuple[LabelKey, "_Metric"]]:
        with self._lock:
            items = list(self._children.items())
        if not self.label_names:
            items.append(((), self))
        return items


class Counter(_Metric):
    kind = "counter"

    def __init__(self, name: str, description: str = "", label_names: Sequence[str] = ()):
        super().__init__(name, description, label_names)
        self._value = 0.0

    def _new_child(self) -> "Counter":
        return Counter(self.name, self.description)

    def inc(self, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("counters only go up")
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value


class Gauge(_Metric):
    kind = "gauge"

    def __init__(self, name: str, description: str = "", label_names: Sequence[str] = ()):
        super().__init__(name, description, label_names)
        self._value = 0.0

    def _new_child(self) -> "Gauge":
        return Gauge(self.name, self.description)

    def set(self, value: float) -> None:
        with self._lock:
            self._value = float(value)

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value



class Histogram(_Metric):
    kind = "histogram"

    def __init__(
        self,
        name: str,
        description: str = "",
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = _DEFAULT_BUCKETS,
        reservoir_size: int = 2048,
    ):
        super().__init__(name, description, label_names)
        self.buckets = tuple(sorted(buckets))
        self.reservoir_size = reservoir_size
        self._bucket_counts = [0] * (len(self.buckets) + 1)
        self._sum = 0.0
        self._count = 0
        self._min = math.inf
        self._max = -math.inf
        self._reservoir: deque = deque(maxlen=reservoir_size)

    def _new_child(self) -> "Histogram":
        return Histogram(self.name, self.description, buckets=self.buckets, reservoir_size=self.reservoir_size)

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            self._min = min(self._min, value)
            self._max = max(self._max, value)
            self._reservoir.append(value)
            for i, upper in enumerate(self.buckets):
                if value <= upper:
                    self._bucket_counts[i] += 1
                    break
            else:
                self._bucket_counts[-1] += 1

    @contextmanager
    def time(self):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.observe(time.perf_counter() - start)

    def percentile(self, pct: float) -> float:
        """Percentile (0-100) over the reservoir of recent observations."""
        with self._lock:
            data = sorted(self._reservoir)
        if not data:
            return 0.0
        k = (len(data) - 1) * (pct / 100.0)
        f = math.floor(k)
        c = min(f + 1, len(data) - 1)
        if f == c:
            return data[int(k)]
        return data[f] + (data[c] - data[f]) * (k - f)

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            count = self._count
            total = self._sum
            mn = self._min if count else 0.0
            mx = self._max if count else 0.0
        return {
            "count": count,
            "sum": total,
            "avg": (total / count) if count else 0.0,
            "min": mn,
            "max": mx,
            "p50": self.percentile(50),
            "p95": self.percentile(95),
            "p99": self.percentile(99),
        }

    def cumulative_buckets(self) -> List[Tuple[str, int]]:
        with self._lock:
            counts = list(self._bucket_counts)
        out, running = [], 0
        for upper, c in zip(self.buckets, counts[:-1]):
            running += c
            out.append((repr(float(upper)), running))
        running += counts[-1]
        out.append(("+Inf", running))
        return out


class MetricsRegistry:
    """Container for all metrics of the process."""

    def __init__(self, namespace: str = "cdcsync"):
        self.namespace = namespace
        self._lock = threading.Lock()
        self._metrics: Dict[str, _Metric] = {}
        self.created_at = time.time()

    # -- registration ------------------------------------------------------
    def _register(self, metric: _Metric) -> _Metric:
        with self._lock:
            existing = self._metrics.get(metric.name)
            if existing is not None:
                if type(existing) is not type(metric) or existing.label_names != metric.label_names:
                    raise ValueError(f"metric {metric.name} already registered with a different type/labels")
                return existing
            self._metrics[metric.name] = metric
            return metric

    def counter(self, name: str, description: str = "", label_names: Sequence[str] = ()) -> Counter:
        return self._register(Counter(name, description, label_names))  # type: ignore[return-value]

    def gauge(self, name: str, description: str = "", label_names: Sequence[str] = ()) -> Gauge:
        return self._register(Gauge(name, description, label_names))  # type: ignore[return-value]

    def histogram(
        self,
        name: str,
        description: str = "",
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = _DEFAULT_BUCKETS,
    ) -> Histogram:
        return self._register(Histogram(name, description, label_names, buckets))  # type: ignore[return-value]

    def get(self, name: str) -> Optional[_Metric]:
        with self._lock:
            return self._metrics.get(name)

    # -- export ------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"uptime_seconds": time.time() - self.created_at, "metrics": {}}
        metrics: Dict[str, Any] = out["metrics"]
        with self._lock:
            items = list(self._metrics.values())
        for metric in items:
            series = []
            for key, child in metric.iter_series():
                labels = dict(key)
                if isinstance(child, Histogram):
                    series.append({"labels": labels, **child.snapshot()})
                else:
                    series.append({"labels": labels, "value": child.value})  # type: ignore[attr-defined]
            metrics[metric.name] = {
                "type": metric.kind,
                "description": metric.description,
                "series": series,
            }
        return out

    def to_prometheus(self) -> str:
        lines: List[str] = []
        with self._lock:
            items = list(self._metrics.values())

        def fmt_labels(series_labels: Dict[str, str], extra: Optional[Dict[str, str]] = None) -> str:
            merged = dict(series_labels)
            if extra:
                merged.update(extra)
            if not merged:
                return ""
            body = ",".join(f'{k}="{str(v).replace(chr(34), chr(92) + chr(34))}"' for k, v in merged.items())
            return "{" + body + "}"

        for metric in items:
            full = f"{self.namespace}_{metric.name}"
            lines.append(f"# HELP {full} {metric.description}")
            lines.append(f"# TYPE {full} {metric.kind}")
            for key, child in metric.iter_series():
                labels = dict(key)
                if isinstance(child, Histogram):
                    for le, cnt in child.cumulative_buckets():
                        lines.append(f"{full}_bucket{fmt_labels(labels, {'le': le})} {cnt}")
                    snap = child.snapshot()
                    lines.append(f"{full}_sum{fmt_labels(labels)} {snap['sum']}")
                    lines.append(f"{full}_count{fmt_labels(labels)} {snap['count']}")
                else:
                    lines.append(f"{full}{fmt_labels(labels)} {child.value}")  # type: ignore[attr-defined]
        return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Global registry + the well-known metrics used across the application.
# ---------------------------------------------------------------------------
registry = MetricsRegistry()

# CDC / pipeline
EVENTS_READ = registry.counter("cdc_events_read_total", "Change events read from a source", ["source", "table", "op"])
EVENTS_APPLIED = registry.counter("cdc_events_applied_total", "Change events applied to destination", ["table", "op"])
EVENTS_FAILED = registry.counter("cdc_events_failed_total", "Change events that failed to apply", ["table", "op"])
EVENTS_SKIPPED = registry.counter("cdc_events_skipped_total", "Change events skipped by mapping rules", ["table"])
QUEUE_DEPTH = registry.gauge("pipeline_queue_depth", "Events waiting to be applied", ["pipeline"])
APPLY_LATENCY = registry.histogram("apply_latency_seconds", "Time to apply one event/batch to destination", ["table"])
END_TO_END_LAG = registry.gauge("replication_lag_seconds", "Source commit time -> destination apply time", ["table"])
POLL_COUNT = registry.counter("source_polls_total", "Polls issued to a source", ["source", "outcome"])
WATERMARK_CHECKS = registry.counter("watermark_checks_total", "Cheap watermark checks (LSN/SCN/binlog pos)", ["source", "outcome"])
SOURCE_ROUNDTRIPS = registry.counter("source_roundtrips_total", "Queries sent to the source DB", ["source", "kind"])
DB_QUERY_DURATION = registry.histogram("db_query_seconds", "Duration of database round trips", ["connection", "kind"])
BATCH_SIZE = registry.histogram(
    "apply_batch_size", "Rows per applied batch", ["table"], buckets=(1, 5, 10, 50, 100, 500, 1000, 5000, 10000)
)
CONNECTION_ERRORS = registry.counter("connection_errors_total", "Connection failures", ["connection"])

# Snapshots
SNAPSHOT_ROWS = registry.counter("snapshot_rows_total", "Rows copied by snapshot jobs", ["job"])
SNAPSHOT_CHUNKS = registry.counter("snapshot_chunks_total", "Chunks processed by snapshot jobs", ["job", "outcome"])
SNAPSHOT_CHUNK_DURATION = registry.histogram("snapshot_chunk_seconds", "Duration of one snapshot chunk", ["job"])
SNAPSHOT_PROGRESS = registry.gauge("snapshot_progress_ratio", "Snapshot completion 0..1", ["job"])
SNAPSHOT_ACTIVE = registry.gauge("snapshot_jobs_active", "Snapshot jobs currently running")

# Audit / alerting
ALERTS_FIRED = registry.counter("alerts_fired_total", "Alerts raised by the rule engine", ["rule", "severity"])
AUDIT_RECORDS = registry.counter("audit_records_total", "Audit records written", ["category"])

# Storage
CONFIG_WRITES = registry.counter("config_store_writes_total", "Writes to the configuration store", ["collection"])
CONFIG_WRITE_DURATION = registry.histogram("config_store_write_seconds", "Configuration store write latency", ["backend"])


@contextmanager
def timed_query(connection_name: str, kind: str):
    """Instrument a DB round trip (duration histogram + counter)."""
    start = time.perf_counter()
    try:
        yield
    finally:
        DB_QUERY_DURATION.labels(connection=connection_name, kind=kind).observe(time.perf_counter() - start)
        SOURCE_ROUNDTRIPS.labels(source=connection_name, kind=kind).inc()
