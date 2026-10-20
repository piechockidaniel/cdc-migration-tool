"""Change pipeline: source connector -> event bus -> row applier -> position commit.

Guarantees
----------
* **At-least-once**: a position is committed only after every event up to it
  has been either applied or written to the DLQ. A crash between apply and
  commit replays the tail; the applier uses upserts so replays are idempotent.
* **Back-pressure**: the reader publishes into a bounded bus subscription; if
  the destination is slow the reader blocks instead of buffering without limit.
* **Non-blocking API**: all database work happens in worker threads; the
  FastAPI event loop only starts/stops threads and reads status.

Position scope: SQL Server CDC keeps one position *per capture instance*
(``position_scope == "table"``), the others keep one cursor per source.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.connectors.base import ChangeEvent, ConnectorError, Position, SourceConnector
from backend.core import metrics
from backend.core.applier import RowApplier
from backend.core.event_bus import EventBus
from backend.models.schemas import Mapping, MappingType

logger = logging.getLogger(__name__)

class ChangePipeline:
    def __init__(
        self,
        pipeline_id: str,
        connector: SourceConnector,
        destination,
        mappings: Sequence[Mapping],
        position_store,                # object with get_position/set_position (ConfigManager)
        audit=None,
        bus: Optional[EventBus] = None,
        duckdb=None,
        batch_size: int = 500,
        batch_wait_s: float = 0.5,
        initial_position: str = "earliest",
        insert_mode: str = "upsert",
        expect_traffic: bool = False,
    ):
        self.id = pipeline_id
        self.connector = connector
        self.destination = destination
        self.mappings = [m for m in mappings if m.mapping_type == MappingType.TABLE and m.enabled and not m.is_multi_source]
        self.tables: List[Tuple[str, str]] = [(m.source_schema, m.source_table) for m in self.mappings]
        self.position_store = position_store
        self.audit = audit
        self.bus = bus or EventBus()
        self.batch_size = batch_size
        self.batch_wait_s = batch_wait_s
        self.initial_position = initial_position
        self.expect_traffic = expect_traffic
        self.position_scope = "table" if connector.engine == "mssql" else "source"
        self.applier = RowApplier(destination, duckdb=duckdb, audit=audit,
                                  source_pk_lookup=lambda s, t: connector.get_primary_key(s, t), insert_mode=insert_mode)
        self.topic = f"changes.{pipeline_id}"

        self.state = "stopped"
        self.expected_running = False
        self.last_error: Optional[str] = None
        self.started_at: Optional[float] = None
        self.last_event_at: Optional[float] = None
        self.events_read = 0
        self.events_applied = 0
        self.events_failed = 0
        self.events_skipped = 0
        self.batches = 0
        self.lag_seconds: Optional[float] = None
        self.positions: Dict[str, Optional[Position]] = {}
        self._connection_error_times: List[float] = []

        self._stop = threading.Event()
        self._pause = threading.Event()
        self._reader: Optional[threading.Thread] = None
        self._applier_thread: Optional[threading.Thread] = None
        self._sub = None
        self._health_conn: Optional[SourceConnector] = None
        self._lock = threading.RLock()
        self._listeners: List[Callable[[ChangeEvent, str, Optional[str]], None]] = []

    # ------------------------------------------------------------------
    # positions
    # ------------------------------------------------------------------
    def _position_key(self, table_key: Optional[str] = None) -> str:
        return f"{self.id}:{table_key}" if self.position_scope == "table" else f"{self.id}:__source__"

    def load_positions(self) -> None:
        if self.position_scope == "table":
            for s, t in self.tables:
                key = f"{s}.{t}"
                pos = Position.from_json(self.position_store.get_position(self._position_key(key)))
                if pos is None:
                    # continuity with 1.x: lsn_state.json was keyed by "schema.table" only
                    legacy = self.position_store.get_position(key)
                    if legacy and legacy.get("kind") == "mssql_lsn":
                        pos = Position.from_json(legacy)
                        logger.info("adopted legacy LSN position for %s: %s", key, pos)
                self.positions[key] = pos
        else:
            self.positions["__source__"] = Position.from_json(self.position_store.get_position(self._position_key()))

    def _initial_positions(self) -> Any:
        """Positions to hand to the connector (dict for table scope, Position for source scope)."""
        if self.position_scope == "table":
            out: Dict[str, Optional[Position]] = {}
            for s, t in self.tables:
                key = f"{s}.{t}"
                pos = self.positions.get(key)
                if pos is None and self.initial_position == "latest":
                    pos = self.connector.current_position()
                out[key] = pos
            return out
        pos = self.positions.get("__source__")
        if pos is None and self.initial_position == "latest":
            pos = self.connector.current_position()
        return pos

    def commit_positions(self, events: Sequence[ChangeEvent], stream_position: Optional[Position] = None) -> None:
        """Persist positions after a batch was fully handled."""
        if self.position_scope == "table":
            latest: Dict[str, Position] = {}
            for ev in events:
                latest[ev.table_key] = ev.position  # events arrive in order per table
            for key, pos in latest.items():
                self.positions[key] = pos
                self.position_store.set_position(self._position_key(key), pos.kind, pos.value, {"extra": pos.extra, "pipeline": self.id})
        else:
            pos = stream_position or (events[-1].position if events else None)
            if pos is not None:
                self.positions["__source__"] = pos
                self.position_store.set_position(self._position_key(), pos.kind, pos.value, {"extra": pos.extra, "pipeline": self.id})
                confirm = getattr(self.connector, "confirm_position", None)
                if confirm:
                    try:
                        confirm(pos)
                    except Exception as exc:
                        logger.warning("confirm_position failed on %s: %s", self.connector.name, exc)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def add_listener(self, fn: Callable[[ChangeEvent, str, Optional[str]], None]) -> None:
        self._listeners.append(fn)

    def start(self) -> None:
        with self._lock:
            if self.state in ("running", "starting", "paused"):
                return
            self.state = "starting"
            self.expected_running = True
            self.last_error = None
            self._stop.clear()
            self._pause.clear()
            try:
                self.connector.connect()
                self.destination.connect()
                self.applier.register_mappings(self.mappings)
                self.load_positions()
            except Exception as exc:
                self.state = "error"
                self.last_error = str(exc)
                self._audit("error", f"pipeline {self.id} failed to start: {exc}")
                raise
            self._sub = self.bus.subscribe(self.topic, maxsize=max(self.batch_size * 20, 2000), name=f"applier:{self.id}")
            self._applier_thread = threading.Thread(target=self._apply_loop, name=f"apply-{self.id}", daemon=True)
            self._reader = threading.Thread(target=self._read_loop, name=f"read-{self.id}", daemon=True)
            self.started_at = time.time()
            self.state = "running"
            self._applier_thread.start()
            self._reader.start()
            self._audit("info", f"pipeline {self.id} started ({self.connector.engine} -> {self.destination.dialect.name}, {len(self.tables)} tables)")

    def stop(self, timeout: float = 30.0) -> None:
        with self._lock:
            if self.state == "stopped":
                return
            self.state = "stopping"
            self.expected_running = False
            self._stop.set()
            self._pause.clear()
        for t in (self._reader, self._applier_thread):
            if t and t.is_alive():
                t.join(timeout=timeout)
        if self._sub:
            self._sub.close()
        for c in (self.connector, self._health_conn):
            try:
                if c is not None:
                    c.disconnect()
            except Exception:  # pragma: no cover
                pass
        self._health_conn = None
        self.state = "stopped"
        self._audit("info", f"pipeline {self.id} stopped")

    def pause(self) -> None:
        if self.state == "running":
            self._pause.set()
            self.state = "paused"
            self._audit("info", f"pipeline {self.id} paused")

    def resume(self) -> None:
        if self.state == "paused":
            self._pause.clear()
            self.state = "running"
            self._audit("info", f"pipeline {self.id} resumed")

    # ------------------------------------------------------------------
    # worker loops
    # ------------------------------------------------------------------
    def _read_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                start_pos = self._initial_positions()
                for ev in self.connector.stream_changes(self.tables, start_pos, self._stop):
                    if self._stop.is_set():
                        break
                    if self.state == "error":      # recovered after a reader failure
                        self.state = "running" if not self._pause.is_set() else "paused"
                        self._audit("info", "reader recovered")
                    self.events_read += 1
                    self.last_event_at = time.time()
                    self.bus.publish(self.topic, ev, key=ev.table_key)
                    backoff = 1.0
                if not self._stop.is_set():
                    # generator ended without stop (stream closed) -> reconnect
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, 60)
            except Exception as exc:
                self.last_error = str(exc)
                self._connection_error_times.append(time.time())
                if isinstance(exc, ConnectorError):
                    self.state = "error"          # non-transient (e.g. position purged): visible to the rules
                self._audit("error", f"reader failed: {exc}")
                metrics.CONNECTION_ERRORS.labels(connection=self.connector.name).inc()
                try:
                    self.connector.disconnect()
                except Exception:  # pragma: no cover
                    pass
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 60)
                # positions may have advanced while events were in flight; restart from committed ones
                self.load_positions()

    def _apply_loop(self) -> None:
        assert self._sub is not None
        while not self._stop.is_set() or self._sub.depth > 0:
            if self._pause.is_set():
                time.sleep(0.2)
                continue
            msgs = self._sub.get_batch(self.batch_size, self.batch_wait_s)
            if not msgs:
                if self._stop.is_set():
                    break
                continue
            events: List[ChangeEvent] = [m.payload for m in msgs]
            while self._pause.is_set() and not self._stop.is_set():   # hold the batch while paused
                time.sleep(0.1)
            metrics.QUEUE_DEPTH.labels(pipeline=self.id).set(self._sub.depth)
            try:
                result = self.applier.apply(events)
            except Exception as exc:  # applier crashed (e.g. destination down) - do NOT commit, retry later
                self.last_error = str(exc)
                self._audit("error", f"apply batch failed: {exc}")
                self._connection_error_times.append(time.time())
                self._requeue(events)
                self._stop.wait(2.0)
                continue
            try:
                self.commit_positions(events)
            except Exception as exc:
                self.last_error = f"position commit failed: {exc}"
                self._audit("error", self.last_error)
            # counters are updated after the commit so "applied == n" implies "position committed"
            self.events_applied += result.applied
            self.events_failed += result.failed
            self.events_skipped += result.skipped
            self.batches += 1
            if result.failed:
                self.last_error = result.failures[-1].error
            if self.audit:
                by_table: Dict[Tuple[str, str], List[int]] = {}
                failed_ids = {id(f.event) for f in result.failures}
                for ev in events:
                    c = by_table.setdefault((ev.table_key, ev.operation.value), [0, 0])
                    c[1 if id(ev) in failed_ids else 0] += 1
                for (tbl, op), (ok, bad) in by_table.items():
                    self.audit.record_batch(self.connector.name, tbl, op, ok, bad, result.duration_s, events[-1].position.to_json())
            last_ts = next((ev.source_ts for ev in reversed(events) if ev.source_ts), None)
            if last_ts:
                try:
                    now = datetime.now(last_ts.tzinfo) if last_ts.tzinfo else datetime.now()
                    self.lag_seconds = max((now - last_ts).total_seconds(), 0.0)
                except Exception:  # pragma: no cover
                    pass
            failed_ids = {id(f.event) for f in result.failures}
            for ev in events:
                status = "failed" if id(ev) in failed_ids else "processed"
                err = next((f.error for f in result.failures if f.event is ev), None)
                if self.audit is not None and getattr(self.audit, "audit_level", "summary") == "all":
                    rt = self.applier.runtime_for(ev)
                    self.audit.record_event(ev, status, rt.dest_key if rt else "", err)
                for fn in self._listeners:
                    try:
                        fn(ev, status, err)
                    except Exception as exc:  # pragma: no cover
                        logger.error("listener failed: %s", exc)

    def _requeue(self, events: List[ChangeEvent]) -> None:
        """Put events back at the front of the subscription queue (best effort)."""
        assert self._sub is not None
        pending = []
        while True:
            m = self._sub.get(timeout=0)
            if m is None:
                break
            pending.append(m)
        from backend.core.event_bus import Message
        for ev in events + [m.payload for m in pending]:
            self._sub.queue.put(Message(self.topic, ev, ev.table_key))

    # ------------------------------------------------------------------
    def _audit(self, severity: str, message: str) -> None:
        logger.log({"error": logging.ERROR, "warning": logging.WARNING}.get(severity, logging.INFO), "[%s] %s", self.id, message)
        if self.audit:
            self.audit.record("pipeline", "pipeline", message, severity=severity, source=self.connector.name)

    def status(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "id": self.id,
            "state": self.state,
            "engine": self.connector.engine,
            "source": self.connector.name,
            "destination": self.destination.name,
            "tables": [f"{s}.{t}" for s, t in self.tables],
            "started_at": datetime.fromtimestamp(self.started_at, timezone.utc).isoformat() if self.started_at else None,
            "uptime_seconds": round(now - self.started_at, 1) if self.started_at and self.state != "stopped" else 0,
            "events_read": self.events_read,
            "events_applied": self.events_applied,
            "events_failed": self.events_failed,
            "events_skipped": self.events_skipped,
            "batches": self.batches,
            "queue_depth": self._sub.depth if self._sub else 0,
            "lag_seconds": self.lag_seconds,
            "last_event_at": datetime.fromtimestamp(self.last_event_at, timezone.utc).isoformat() if self.last_event_at else None,
            "last_error": self.last_error,
            "positions": {k: (v.to_json() if v else None) for k, v in self.positions.items()},
            "unsafe_key_tables": list(self.applier.unsafe_key_warnings.keys()),
        }

    def health_context(self) -> Dict[str, Any]:
        """Facts for the rule engine (``pipeline.*`` and ``source.*``)."""
        now = time.time()
        recent_errors = [t for t in self._connection_error_times if now - t <= 300]
        self._connection_error_times = recent_errors
        ctx = {
            "pipeline": {
                "id": self.id,
                "running": self.state in ("running", "paused"),
                "paused": self.state == "paused",
                "expected_running": self.expected_running,
                "lag_seconds": self.lag_seconds,
                "queue_depth": self._sub.depth if self._sub else 0,
                "idle_seconds": (now - self.last_event_at) if self.last_event_at else ((now - self.started_at) if self.started_at else None),
                "expect_traffic": self.expect_traffic,
                "connection_errors_5m": len(recent_errors),
                "events_failed": self.events_failed,
                "events_applied": self.events_applied,
                "unsafe_key_tables": len(self.applier.unsafe_key_warnings),
                "last_error": self.last_error,
            },
            "source": {},
        }
        if self.state in ("running", "paused", "error"):
            try:
                # separate connection: the reader may be blocked in a long server-side wait (WAITFOR / change stream)
                hc = self._health_connector()
                retention = getattr(hc, "retention_status", None)
                if retention:
                    ctx["source"] = retention(self.positions) or {}
            except Exception as exc:
                ctx["source"] = {"error": str(exc)}
        return ctx

    def _health_connector(self) -> SourceConnector:
        hc = self._health_conn
        if hc is None:
            try:
                hc = type(self.connector)(dict(self.connector.config))
            except Exception:
                hc = self.connector
            self._health_conn = hc
        return hc


class PipelineRegistry:
    """All pipelines of this process (thread-safe)."""

    def __init__(self):
        self._pipelines: Dict[str, ChangePipeline] = {}
        self._lock = threading.RLock()

    def add(self, pipeline: ChangePipeline) -> None:
        with self._lock:
            existing = self._pipelines.get(pipeline.id)
            if existing and existing.state not in ("stopped", "error"):
                raise RuntimeError(f"pipeline {pipeline.id} is already running")
            self._pipelines[pipeline.id] = pipeline

    def get(self, pipeline_id: str) -> Optional[ChangePipeline]:
        with self._lock:
            return self._pipelines.get(pipeline_id)

    def remove(self, pipeline_id: str) -> None:
        with self._lock:
            self._pipelines.pop(pipeline_id, None)

    def all(self) -> List[ChangePipeline]:
        with self._lock:
            return list(self._pipelines.values())

    def stop_all(self) -> None:
        for p in self.all():
            try:
                p.stop()
            except Exception as exc:  # pragma: no cover
                logger.error("stopping %s failed: %s", p.id, exc)

    def health_contexts(self) -> Dict[str, Dict[str, Any]]:
        return {f"pipeline:{p.id}": p.health_context() for p in self.all()}


pipelines = PipelineRegistry()
