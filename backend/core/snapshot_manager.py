"""Chunked, resumable, throttled initial-load ("snapshot") jobs.

The original ``snapshot_service`` did ``TRUNCATE dest; SELECT * FROM source``
on the request thread - for a 16 TB table that is a multi-day shared-lock
scan that blocks the API and leaves the destination empty until it finishes.

This manager instead:

* reads the table in **keyset-paginated chunks** (``WHERE key > last ORDER BY key
  LIMIT n``) - every chunk is an independent, index-seek-only query; there is
  no long running cursor and no offset scan;
* **persists progress after every chunk** (last key, counters) so a crash or
  an operator pause resumes exactly where it stopped;
* **throttles** (rows/second, pause between chunks, adaptive chunk size that
  shrinks when the source is slow) and honours **maintenance windows**;
* writes chunks as **upserts in one transaction per chunk** - the destination
  is never truncated and stays usable while the load runs;
* records the source position *before* the first chunk and hands it to the
  CDC pipeline, so changes made during the load are replayed afterwards
  (idempotently, thanks to upserts) - no lost or duplicated rows;
* can read from a **read replica / AG secondary** simply by pointing the job at
  a different source connection.

Very large tables can additionally be split into several jobs with disjoint
``where`` ranges (e.g. by partition key) and run in parallel.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from backend.connectors.base import ChangeEvent, Operation, Position, SourceConnector, json_safe
from backend.core import metrics
from backend.core.applier import RowApplier, TableRuntime
from backend.models.schemas import Mapping
from backend.storage.base import SNAPSHOT_JOBS

logger = logging.getLogger(__name__)

JOB_STATES = ("pending", "running", "paused", "completed", "failed", "cancelled")


@dataclass
class MaintenanceWindow:
    """Allowed execution window, e.g. weekdays 22:00-06:00 (local time)."""

    start: str = "00:00"          # HH:MM
    end: str = "23:59"
    days: List[int] = field(default_factory=lambda: [0, 1, 2, 3, 4, 5, 6])  # 0 = Monday

    def contains(self, when: Optional[datetime] = None) -> bool:
        when = when or datetime.now()
        if when.weekday() not in self.days:
            return False
        s_h, s_m = (int(x) for x in self.start.split(":"))
        e_h, e_m = (int(x) for x in self.end.split(":"))
        start_min, end_min, now_min = s_h * 60 + s_m, e_h * 60 + e_m, when.hour * 60 + when.minute
        if start_min <= end_min:
            return start_min <= now_min <= end_min
        return now_min >= start_min or now_min <= end_min  # window crosses midnight


@dataclass
class SnapshotJob:
    id: str
    mapping_id: str
    source_connection_id: str
    destination_connection_id: str
    source_schema: str
    source_table: str
    dest_schema: str
    dest_table: str
    key_columns: List[str]
    columns: List[str]
    dest_key_columns: List[str] = field(default_factory=list)
    pipeline_id: Optional[str] = None
    where: Optional[str] = None
    chunk_size: int = 5000
    min_chunk_size: int = 500
    max_chunk_size: int = 50000
    max_chunk_seconds: float = 20.0
    max_rows_per_second: Optional[int] = None
    pause_between_chunks_ms: int = 0
    max_retries: int = 5
    windows: List[Dict[str, Any]] = field(default_factory=list)
    state: str = "pending"
    rows_copied: int = 0
    chunks_done: int = 0
    chunk_errors: int = 0
    rows_failed: int = 0                 # poison rows parked in the dead-letter queue
    estimated_rows: Optional[int] = None
    last_key: Optional[List[Any]] = None
    key_lo: Optional[List[Any]] = None
    key_hi: Optional[List[Any]] = None
    start_position: Optional[Dict[str, Any]] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    finished_at: Optional[str] = None
    last_chunk_at: Optional[float] = None
    avg_chunk_seconds: float = 0.0
    rows_per_second: float = 0.0
    error: Optional[str] = None
    waiting_for_window: bool = False

    def to_json(self) -> Dict[str, Any]:
        d = asdict(self)
        d["progress"] = self.progress()
        return d

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "SnapshotJob":
        names = {f.name for f in fields(cls)}
        known = {k: v for k, v in data.items() if k in names}
        return cls(**known)

    @property
    def source_key(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    def progress(self) -> Optional[float]:
        if self.state == "completed":
            return 1.0
        # numeric single-column keys: position within [lo, hi]
        lo_key, hi_key, last = self.key_lo, self.key_hi, self.last_key
        if lo_key and hi_key and last and len(self.key_columns) == 1:
            try:
                lo, hi, cur = float(lo_key[0]), float(hi_key[0]), float(last[0])
                if hi > lo:
                    return round(min(max((cur - lo) / (hi - lo), 0.0), 1.0), 4)
            except (TypeError, ValueError):
                pass
        if self.estimated_rows:
            return round(min(self.rows_copied / self.estimated_rows, 0.999), 4)
        return None

    def in_window(self) -> bool:
        if not self.windows:
            return True
        return any(MaintenanceWindow(**w).contains() for w in self.windows)


class SnapshotManager:
    def __init__(self, store, connector_factory: Callable[[str], SourceConnector], destination_factory: Callable[[str], Any],
                 mapping_lookup: Callable[[str], Optional[Mapping]], position_store=None, audit=None, duckdb=None,
                 max_parallel_jobs: int = 2):
        self.store = store
        self.connector_factory = connector_factory
        self.destination_factory = destination_factory
        self.mapping_lookup = mapping_lookup
        self.position_store = position_store
        self.audit = audit
        self.duckdb = duckdb
        self._jobs: Dict[str, SnapshotJob] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._controls: Dict[str, Dict[str, threading.Event]] = {}
        self._lock = threading.RLock()
        self._semaphore = threading.BoundedSemaphore(max_parallel_jobs)
        self._load()

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _load(self) -> None:
        for doc in self.store.get_all(SNAPSHOT_JOBS):
            try:
                job = SnapshotJob.from_json(doc.body)
                if job.state == "running":       # process died mid-run -> resumable
                    job.state = "paused"
                    job.error = (job.error or "") + " [interrupted by restart]"
                self._jobs[job.id] = job
            except Exception as exc:
                logger.error("cannot load snapshot job %s: %s", doc.key, exc)

    def _save(self, job: SnapshotJob) -> None:
        job.updated_at = datetime.now(timezone.utc).isoformat()
        body = asdict(job)
        body["last_key"] = [json_safe(v) for v in job.last_key] if job.last_key else None
        body["key_lo"] = [json_safe(v) for v in job.key_lo] if job.key_lo else None
        body["key_hi"] = [json_safe(v) for v in job.key_hi] if job.key_hi else None
        self.store.put(SNAPSHOT_JOBS, job.id, body)
        metrics.SNAPSHOT_PROGRESS.labels(job=job.id).set(job.progress() or 0.0)

    # ------------------------------------------------------------------
    # job creation
    # ------------------------------------------------------------------
    def create_job(self, mapping: Mapping, source_connection_id: str, destination_connection_id: str,
                   pipeline_id: Optional[str] = None, **options: Any) -> SnapshotJob:
        connector = self.connector_factory(source_connection_id)
        rt = TableRuntime.from_mapping(mapping)
        source_columns = sorted({cm.source_column for cm in mapping.column_mappings if cm.source_column} |
                                {c for cm in mapping.column_mappings for c in (cm.source_columns or [])})
        key_columns = list(options.pop("key_columns", None) or [])
        if not key_columns:
            if mapping.key_columns:
                # destination key names -> source names through the rename map
                inverse = {d: s for s, d in rt.renames.items()}
                key_columns = [inverse[k] for k in mapping.key_columns if k in inverse]
            if not key_columns:
                connector.connect()
                key_columns = connector.get_primary_key(mapping.source_schema, mapping.source_table)
        if not key_columns:
            raise ValueError(f"{mapping.source_schema}.{mapping.source_table} has no primary key; pass key_columns "
                             f"(a unique, indexed column set) to chunk the snapshot")
        for k in key_columns:
            if k not in source_columns:
                source_columns.append(k)
        dest_keys = [rt.renames.get(k, k) for k in key_columns]
        job = SnapshotJob(
            id=options.pop("id", None) or f"snap_{mapping.id}_{uuid.uuid4().hex[:8]}",
            mapping_id=mapping.id, source_connection_id=source_connection_id, destination_connection_id=destination_connection_id,
            source_schema=mapping.source_schema, source_table=mapping.source_table,
            dest_schema=mapping.destination_schema, dest_table=mapping.destination_table,
            key_columns=key_columns, columns=source_columns, dest_key_columns=list(mapping.key_columns or dest_keys),
            pipeline_id=pipeline_id, **{k: v for k, v in options.items() if k in {f.name for f in fields(SnapshotJob)}},
        )
        try:
            connector.connect()
            job.estimated_rows = connector.count_rows(mapping.source_schema, mapping.source_table, approximate=True)
            lo, hi = connector.key_bounds(mapping.source_schema, mapping.source_table, key_columns)
            job.key_lo, job.key_hi = (list(lo) if lo else None), (list(hi) if hi else None)
        except Exception as exc:
            logger.warning("cannot estimate snapshot size for %s: %s", job.source_key, exc)
        finally:
            connector.disconnect()
        with self._lock:
            self._jobs[job.id] = job
            self._save(job)
        self._audit(job, "info", f"snapshot job created ({job.estimated_rows or '?'} rows estimated, chunk={job.chunk_size})")
        return job

    # ------------------------------------------------------------------
    # control
    # ------------------------------------------------------------------
    def start(self, job_id: str) -> SnapshotJob:
        with self._lock:
            job = self._require(job_id)
            if job.state == "completed":
                return job
            if job.state == "running":
                return job
            t = self._threads.get(job_id)
            if t and t.is_alive():
                return job
            job.state = "running"
            job.error = None
            job.started_at = job.started_at or datetime.now(timezone.utc).isoformat()
            self._save(job)
            controls = {"stop": threading.Event(), "pause": threading.Event()}
            self._controls[job_id] = controls
            t = threading.Thread(target=self._run, args=(job, controls), name=f"snapshot-{job_id}", daemon=True)
            self._threads[job_id] = t
            t.start()
            return job

    def pause(self, job_id: str) -> SnapshotJob:
        with self._lock:
            job = self._require(job_id)
            if job.state == "running":
                self._controls[job_id]["pause"].set()
                job.state = "paused"
                self._save(job)
            return job

    def resume(self, job_id: str) -> SnapshotJob:
        with self._lock:
            job = self._require(job_id)
            if job.state == "paused":
                ctl = self._controls.get(job_id)
                t = self._threads.get(job_id)
                if ctl and t and t.is_alive():
                    ctl["pause"].clear()
                    job.state = "running"
                    self._save(job)
                    return job
                return self.start(job_id)
            return job

    def cancel(self, job_id: str) -> SnapshotJob:
        with self._lock:
            job = self._require(job_id)
            ctl = self._controls.get(job_id)
            if ctl:
                ctl["stop"].set()
                ctl["pause"].clear()
            if job.state in ("running", "paused", "pending"):
                job.state = "cancelled"
                job.finished_at = datetime.now(timezone.utc).isoformat()
                self._save(job)
            return job

    def delete(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            if job.state == "running":
                raise RuntimeError("cancel the job before deleting it")
            del self._jobs[job_id]
            self.store.delete(SNAPSHOT_JOBS, job_id)
            return True

    def reset(self, job_id: str) -> SnapshotJob:
        """Restart from the beginning (keeps configuration)."""
        with self._lock:
            job = self._require(job_id)
            if job.state == "running":
                raise RuntimeError("pause or cancel the job first")
            job.state, job.rows_copied, job.chunks_done, job.chunk_errors = "pending", 0, 0, 0
            job.last_key, job.start_position, job.error = None, None, None
            job.started_at = job.finished_at = None
            self._save(job)
            return job

    def get(self, job_id: str) -> Optional[SnapshotJob]:
        return self._jobs.get(job_id)

    def list(self) -> List[SnapshotJob]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)

    def _require(self, job_id: str) -> SnapshotJob:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"snapshot job {job_id} not found")
        return job

    def stop_all(self) -> None:
        for job_id in list(self._controls):
            ctl = self._controls[job_id]
            ctl["stop"].set()
        for t in list(self._threads.values()):
            t.join(timeout=10)

    # ------------------------------------------------------------------
    # worker
    # ------------------------------------------------------------------
    def _run(self, job: SnapshotJob, controls: Dict[str, threading.Event]) -> None:
        stop, pause = controls["stop"], controls["pause"]
        connector = None
        destination = None
        acquired = False
        try:
            self._semaphore.acquire()
            acquired = True
            metrics.SNAPSHOT_ACTIVE.inc()
            mapping = self.mapping_lookup(job.mapping_id)
            if mapping is None:
                raise RuntimeError(f"mapping {job.mapping_id} not found")
            connector = self.connector_factory(job.source_connection_id)
            destination = self.destination_factory(job.destination_connection_id)
            connector.connect()
            destination.connect()
            applier = RowApplier(destination, duckdb=self.duckdb, audit=self.audit)
            applier.register_mappings([mapping])
            rt = applier.runtime_for_key(job.source_key)
            if rt is None:
                raise RuntimeError(f"mapping {job.mapping_id} is disabled, multi-source or not a TABLE mapping")
            if not rt.key_columns:
                rt.key_columns = job.dest_key_columns
            if job.start_position is None:
                pos = connector.current_position()
                job.start_position = pos.to_json() if pos else None
                self._hand_off_position(job, pos)
                self._save(job)
            self._audit(job, "info", f"snapshot started from key {job.last_key} (source position {job.start_position})")
            retries = 0
            chunk_times: List[float] = []
            while not stop.is_set():
                if pause.is_set():
                    stop.wait(0.5)
                    continue
                if not job.in_window():
                    if not job.waiting_for_window:
                        job.waiting_for_window = True
                        self._save(job)
                    stop.wait(30)
                    continue
                job.waiting_for_window = False
                t0 = time.perf_counter()
                try:
                    rows = connector.read_snapshot_chunk(job.source_schema, job.source_table, job.columns, job.key_columns,
                                                         job.last_key, job.chunk_size, job.where)
                    if not rows:
                        break
                    # Chunks go through the applier: transformation + upsert in one executemany, and when a chunk
                    # fails it is retried row by row so a single poison row lands in the DLQ instead of blocking
                    # the whole snapshot (the original service died on the first bad row).
                    events = [ChangeEvent(source=connector.name, schema=job.source_schema, table=job.source_table, operation=Operation.INSERT,
                                          position=Position("snapshot", f"{job.id}:{json_safe(r.get(job.key_columns[0]))}"),
                                          key={k: r.get(k) for k in job.key_columns}, after=r,
                                          metadata={"snapshot_job": job.id}) for r in rows]
                    result = applier.apply(events)
                    if result.applied == 0 and result.failed:
                        raise RuntimeError(f"{result.failed} rows failed: {result.failures[0].error}")
                    job.rows_failed += result.failed
                    retries = 0
                except Exception as exc:
                    job.chunk_errors += 1
                    retries += 1
                    metrics.SNAPSHOT_CHUNKS.labels(job=job.id, outcome="error").inc()
                    self._audit(job, "warning", f"chunk after key {job.last_key} failed ({retries}/{job.max_retries}): {exc}")
                    if retries > job.max_retries:
                        raise
                    stop.wait(min(2 ** retries, 60))
                    continue
                elapsed = time.perf_counter() - t0
                job.last_key = [rows[-1][k] for k in job.key_columns]
                job.rows_copied += len(rows)
                job.chunks_done += 1
                job.last_chunk_at = time.time()
                chunk_times.append(elapsed)
                chunk_times = chunk_times[-50:]
                job.avg_chunk_seconds = round(sum(chunk_times) / len(chunk_times), 3)
                job.rows_per_second = round(len(rows) / elapsed, 1) if elapsed > 0 else 0.0
                metrics.SNAPSHOT_ROWS.labels(job=job.id).inc(len(rows))
                metrics.SNAPSHOT_CHUNKS.labels(job=job.id, outcome="ok").inc()
                metrics.SNAPSHOT_CHUNK_DURATION.labels(job=job.id).observe(elapsed)
                # adaptive chunk size: protect the source
                if elapsed > job.max_chunk_seconds and job.chunk_size > job.min_chunk_size:
                    job.chunk_size = max(job.min_chunk_size, job.chunk_size // 2)
                elif elapsed < job.max_chunk_seconds / 4 and job.chunk_size < job.max_chunk_size:
                    job.chunk_size = min(job.max_chunk_size, int(job.chunk_size * 1.5))
                self._save(job)
                # throttling
                if job.max_rows_per_second:
                    budget = len(rows) / job.max_rows_per_second
                    if budget > elapsed:
                        stop.wait(budget - elapsed)
                if job.pause_between_chunks_ms:
                    stop.wait(job.pause_between_chunks_ms / 1000.0)
            if stop.is_set():
                if job.state not in ("cancelled",):
                    job.state = "paused"
                self._save(job)
                self._audit(job, "info", f"snapshot stopped at key {job.last_key} ({job.rows_copied} rows)")
                return
            job.state = "completed"
            job.finished_at = datetime.now(timezone.utc).isoformat()
            self._save(job)
            self._audit(job, "warning" if job.rows_failed else "info",
                        f"snapshot completed: {job.rows_copied} rows in {job.chunks_done} chunks, {job.rows_failed} rows in the DLQ")
            self._mark_mapping_completed(job)
        except Exception as exc:
            job.state = "failed"
            job.error = str(exc)
            job.finished_at = datetime.now(timezone.utc).isoformat()
            self._save(job)
            self._audit(job, "error", f"snapshot failed: {exc}")
        finally:
            if acquired:
                metrics.SNAPSHOT_ACTIVE.dec()
                self._semaphore.release()
            for obj in (connector, destination):
                try:
                    if obj:
                        obj.disconnect()
                except Exception:  # pragma: no cover
                    pass

    def _hand_off_position(self, job: SnapshotJob, position: Optional[Position]) -> None:
        """Make the CDC pipeline start from the snapshot's start position (if it has none yet)."""
        if not (self.position_store and job.pipeline_id and position):
            return
        table_scoped_key = f"{job.pipeline_id}:{job.source_key}"
        source_key = f"{job.pipeline_id}:__source__"
        key = table_scoped_key if position.kind == "mssql_lsn" else source_key
        if self.position_store.get_position(key) is None:
            self.position_store.set_position(key, position.kind, position.value,
                                             {"extra": position.extra, "pipeline": job.pipeline_id, "origin": f"snapshot:{job.id}"})

    @staticmethod
    def _mark_mapping_completed(job: SnapshotJob) -> None:
        try:
            from backend.core.config_manager import config_manager
            mapping = config_manager.get_mapping(job.mapping_id)
            if mapping:
                mapping.snapshot_completed_at = datetime.now()
                config_manager.update_mapping(mapping)
        except Exception as exc:  # pragma: no cover
            logger.warning("cannot mark mapping snapshot completed: %s", exc)

    def _audit(self, job: SnapshotJob, severity: str, message: str) -> None:
        logger.log({"error": logging.ERROR, "warning": logging.WARNING}.get(severity, logging.INFO), "[snapshot %s] %s", job.id, message)
        if self.audit:
            self.audit.record("snapshot", "snapshot", message, severity=severity, table=job.source_key,
                              payload={"job_id": job.id, "rows_copied": job.rows_copied, "last_key": [json_safe(v) for v in job.last_key] if job.last_key else None})

    # ------------------------------------------------------------------
    def health_contexts(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        out = {}
        for job in self.list():
            out[f"snapshot:{job.id}"] = {"snapshot": {
                "id": job.id, "state": job.state, "table": job.source_key, "rows_copied": job.rows_copied,
                "chunk_errors": job.chunk_errors, "avg_chunk_seconds": job.avg_chunk_seconds,
                "stalled_seconds": (now - job.last_chunk_at) if (job.state == "running" and job.last_chunk_at and not job.waiting_for_window) else 0,
                "progress": job.progress(), "waiting_for_window": job.waiting_for_window,
            }}
        return out
