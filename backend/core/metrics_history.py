"""Server-side time series for the dashboards.

Prometheus would normally keep the history; for deployments without it the
process samples its own registry every ``interval_s`` seconds into a ring
buffer so charts survive page reloads and several browsers see the same data.

Memory: ~15 series x 2880 points (4 h at 5 s) of floats - negligible.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Dict, List, Optional

from backend.core import metrics


class MetricsHistory:
    def __init__(self, interval_s: float = 5.0, max_points: int = 2880):
        self.interval_s = interval_s
        self.max_points = max_points
        self._series: Dict[str, deque] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._last_totals: Dict[str, float] = {}
        self._last_ts: Optional[float] = None

    # -- sampling ----------------------------------------------------------
    def _push(self, name: str, ts: float, value: float) -> None:
        s = self._series.get(name)
        if s is None:
            s = self._series[name] = deque(maxlen=self.max_points)
        s.append((round(ts, 1), value))

    @staticmethod
    def _sum_series(metric) -> float:
        if metric is None:
            return 0.0
        return float(sum(child.value for _, child in metric.iter_series()))

    def sample(self, now: Optional[float] = None) -> None:
        now = now or time.time()
        reg = metrics.registry
        with self._lock:
            # rates from monotonically increasing counters
            for key, metric_name in (("applied", "cdc_events_applied_total"), ("failed", "cdc_events_failed_total"),
                                     ("read", "cdc_events_read_total"), ("roundtrips", "source_roundtrips_total"),
                                     ("snapshot_rows", "snapshot_rows_total")):
                total = self._sum_series(reg.get(metric_name))
                prev = self._last_totals.get(key)
                if prev is not None and self._last_ts:
                    dt = max(now - self._last_ts, 1e-6)
                    self._push(f"rate.{key}", now, max(total - prev, 0.0) / dt)
                self._last_totals[key] = total
            # gauges per label
            lag = reg.get("replication_lag_seconds")
            if lag is not None:
                for labels, child in lag.iter_series():
                    self._push(f"lag.{dict(labels).get('table', '')}", now, child.value)
            depth = reg.get("pipeline_queue_depth")
            if depth is not None:
                for labels, child in depth.iter_series():
                    name = dict(labels).get("pipeline", "")
                    if not name.startswith("bus:"):
                        self._push(f"queue.{name}", now, child.value)
            hist = reg.get("apply_latency_seconds")
            if hist is not None:
                for labels, child in hist.iter_series():
                    self._push(f"apply_p95.{dict(labels).get('table', '')}", now, child.percentile(95) * 1000.0)
            active = reg.get("snapshot_jobs_active")
            if active is not None:
                self._push("snapshots.active", now, active.value)
            self._last_ts = now

    # -- access ------------------------------------------------------------
    def series(self, minutes: float = 60.0, prefix: Optional[str] = None, max_points: int = 400) -> Dict[str, List[List[float]]]:
        since = time.time() - minutes * 60.0
        out: Dict[str, List[List[float]]] = {}
        with self._lock:
            for name, points in self._series.items():
                if prefix and not name.startswith(prefix):
                    continue
                pts = [[t, v] for t, v in points if t >= since]
                if len(pts) > max_points:  # downsample by striding, keep the last point
                    step = len(pts) / max_points
                    pts = [pts[int(i * step)] for i in range(max_points)] + [pts[-1]]
                out[name] = pts
        return out

    # -- background --------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()

        def loop():
            while not self._stop.is_set():
                try:
                    self.sample()
                except Exception:  # pragma: no cover
                    pass
                self._stop.wait(self.interval_s)

        self._thread = threading.Thread(target=loop, name="metrics-history", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)


history = MetricsHistory()
