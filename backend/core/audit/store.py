"""Append-only audit store (SQLite, WAL) with a dead-letter queue for failed changes.

Replaces the ``history/cdc_events_<day>.json`` approach whose "load whole
file, append, rewrite" made every flush O(events-of-the-day).

Tables
------
* ``audit``     - everything noteworthy: pipeline life-cycle, batch summaries,
                  config changes, prerequisite results, alerts, warnings.
* ``failures``  - the DLQ: one row per change that could not be applied,
                  with the full event payload so it can be replayed once the
                  mapping/destination is fixed, plus a normalised error
                  *signature* used by the pattern miner.

Per-event audit rows are optional (``audit_level="all"``); by default only
batch summaries and failures are written, which keeps the store small at
bank-grade volumes.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from backend.core import metrics

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL,
    category  TEXT NOT NULL,
    severity  TEXT NOT NULL DEFAULT 'info',
    component TEXT,
    source    TEXT,
    table_key TEXT,
    operation TEXT,
    status    TEXT,
    message   TEXT,
    signature TEXT,
    event_id  TEXT,
    position  TEXT,
    count     INTEGER DEFAULT 1,
    payload   TEXT
);
CREATE INDEX IF NOT EXISTS ix_audit_ts ON audit (ts);
CREATE INDEX IF NOT EXISTS ix_audit_cat_ts ON audit (category, ts);
CREATE INDEX IF NOT EXISTS ix_audit_table_ts ON audit (table_key, ts);

CREATE TABLE IF NOT EXISTS failures (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    source      TEXT,
    source_table TEXT,
    dest_table  TEXT,
    operation   TEXT,
    event_id    TEXT,
    position    TEXT,
    error       TEXT,
    signature   TEXT,
    payload     TEXT,
    attempts    INTEGER DEFAULT 1,
    resolved    INTEGER DEFAULT 0,
    resolution  TEXT,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_fail_sig ON failures (signature, resolved);
CREATE INDEX IF NOT EXISTS ix_fail_table ON failures (dest_table, resolved, ts);
CREATE INDEX IF NOT EXISTS ix_fail_ts ON failures (ts);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _since(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


class AuditStore:
    def __init__(self, path: str | Path = "history/audit.sqlite3", audit_level: str = "summary",
                 flush_interval_s: float = 1.0, buffer_size: int = 500):
        self.path = str(path)
        self.audit_level = audit_level  # summary | all
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._mem_conn: Optional[sqlite3.Connection] = None
        self._mem_lock = threading.Lock()
        self._buffer: List[tuple] = []
        self._buffer_lock = threading.Lock()
        self.buffer_size = buffer_size
        self.flush_interval_s = flush_interval_s
        self._last_flush = time.time()
        with self._conn_ctx() as conn:
            conn.executescript(_SCHEMA)

    # -- connections -------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        if self.path == ":memory:":
            with self._mem_lock:
                if self._mem_conn is None:
                    self._mem_conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
                return self._mem_conn
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=10000")
            self._local.conn = conn
        return conn

    def _conn_ctx(self):
        """Connection as a (non-closing) context manager - connections are per thread and reused."""
        return nullcontext(self._conn())

    # -- writing -----------------------------------------------------------
    def record(self, category: str, component: str, message: str, severity: str = "info", source: Optional[str] = None,
               table: Optional[str] = None, operation: Optional[str] = None, status: Optional[str] = None,
               signature: Optional[str] = None, event_id: Optional[str] = None, position: Any = None, count: int = 1,
               payload: Any = None) -> None:
        row = (_now(), category, severity, component, source, table, operation, status, message, signature, event_id,
               json.dumps(position, default=str) if position is not None else None, count,
               json.dumps(payload, default=str) if payload is not None else None)
        metrics.AUDIT_RECORDS.labels(category=category).inc()
        with self._buffer_lock:
            self._buffer.append(row)
            should_flush = len(self._buffer) >= self.buffer_size or severity in ("error", "critical") \
                or (time.time() - self._last_flush) >= self.flush_interval_s
        if should_flush:
            self.flush()

    def record_batch(self, source: str, table: str, operation: str, applied: int, failed: int, duration_s: float,
                     last_position: Any = None) -> None:
        self.record("batch", "applier", f"{applied} applied, {failed} failed in {duration_s*1000:.0f} ms",
                    severity="warning" if failed else "info", source=source, table=table, operation=operation,
                    status="partial" if failed and applied else ("failed" if failed else "ok"),
                    position=last_position, count=applied + failed,
                    payload={"applied": applied, "failed": failed, "duration_ms": round(duration_s * 1000, 1)})

    def record_event(self, event, status: str, dest_table: str, error: Optional[str] = None) -> None:
        """Per-event row (only when ``audit_level == 'all'`` or the event failed)."""
        if self.audit_level != "all" and status != "failed":
            return
        self.record("cdc_event", "applier", error or f"{event.operation.value} applied", severity="error" if error else "info",
                    source=event.source, table=event.table_key, operation=event.operation.value, status=status,
                    event_id=event.event_id, position=event.position.to_json(), payload={"dest": dest_table, "key": event.key})

    def record_failure(self, event, dest_table: str, error: str, signature: str) -> int:
        self.flush()
        with self._conn_ctx() as conn:
            cur = conn.execute(
                "INSERT INTO failures (ts, source, source_table, dest_table, operation, event_id, position, error, signature, payload) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (_now(), event.source, event.table_key, dest_table, event.operation.value, event.event_id,
                 json.dumps(event.position.to_json(), default=str), error, signature, json.dumps(event.to_json(), default=str)))
            metrics.AUDIT_RECORDS.labels(category="failure").inc()
            return int(cur.lastrowid or 0)

    def flush(self) -> int:
        with self._buffer_lock:
            rows, self._buffer = self._buffer, []
            self._last_flush = time.time()
        if not rows:
            return 0
        with self._conn_ctx() as conn:
            conn.execute("BEGIN")
            try:
                conn.executemany(
                    "INSERT INTO audit (ts, category, severity, component, source, table_key, operation, status, message, signature, "
                    "event_id, position, count, payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    # -- reading -----------------------------------------------------------
    @staticmethod
    def _rows(cur) -> List[Dict[str, Any]]:
        cols = [d[0] for d in cur.description]
        out = []
        for r in cur.fetchall():
            d = dict(zip(cols, r))
            for k in ("payload", "position"):
                if d.get(k):
                    try:
                        d[k] = json.loads(d[k])
                    except Exception:
                        pass
            out.append(d)
        return out

    def query(self, category: Optional[str] = None, table: Optional[str] = None, severity: Optional[str] = None,
              status: Optional[str] = None, hours: Optional[float] = 24, limit: int = 200, offset: int = 0,
              since: Optional[str] = None) -> List[Dict[str, Any]]:
        self.flush()
        clauses, params = [], []
        if category:
            clauses.append("category = ?")
            params.append(category)
        if table:
            clauses.append("table_key = ?")
            params.append(table)
        if severity:
            clauses.append("severity = ?")
            params.append(severity)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if since:
            clauses.append("ts >= ?")
            params.append(since)
        elif hours:
            clauses.append("ts >= ?")
            params.append(_since(hours))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._conn_ctx() as conn:
            cur = conn.execute(f"SELECT * FROM audit{where} ORDER BY id DESC LIMIT ? OFFSET ?", params + [limit, offset])
            return self._rows(cur)

    def stats(self, hours: float = 24) -> Dict[str, Any]:
        self.flush()
        since = _since(hours)
        with self._conn_ctx() as conn:
            batches = conn.execute(
                "SELECT table_key, operation, SUM(json_extract(payload, '$.applied')) AS applied, "
                "SUM(json_extract(payload, '$.failed')) AS failed, COUNT(*) AS batches, AVG(json_extract(payload, '$.duration_ms')) AS avg_ms "
                "FROM audit WHERE category = 'batch' AND ts >= ? GROUP BY table_key, operation", (since,)).fetchall()
            failures = conn.execute("SELECT COUNT(*), SUM(resolved) FROM failures WHERE ts >= ?", (since,)).fetchone()
            by_sev = conn.execute("SELECT severity, COUNT(*) FROM audit WHERE ts >= ? GROUP BY severity", (since,)).fetchall()
        applied = sum(int(b[2] or 0) for b in batches)
        failed = sum(int(b[3] or 0) for b in batches)
        return {
            "hours": hours,
            "applied": applied,
            "failed": failed,
            "success_rate": round(applied / (applied + failed) * 100, 2) if applied + failed else 100.0,
            "tables": [{"table": b[0], "operation": b[1], "applied": int(b[2] or 0), "failed": int(b[3] or 0),
                        "batches": b[4], "avg_batch_ms": round(b[5] or 0, 1)} for b in batches],
            "failures_total": int(failures[0] or 0),
            "failures_resolved": int(failures[1] or 0),
            "by_severity": {s: c for s, c in by_sev},
        }

    # -- DLQ ---------------------------------------------------------------
    def failures(self, resolved: Optional[bool] = False, table: Optional[str] = None, signature: Optional[str] = None,
                 hours: Optional[float] = None, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
        clauses, params = [], []
        if resolved is not None:
            clauses.append("resolved = ?")
            params.append(1 if resolved else 0)
        if table:
            clauses.append("(dest_table = ? OR source_table = ?)")
            params.extend([table, table])
        if signature:
            clauses.append("signature = ?")
            params.append(signature)
        if hours:
            clauses.append("ts >= ?")
            params.append(_since(hours))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._conn_ctx() as conn:
            cur = conn.execute(f"SELECT * FROM failures{where} ORDER BY id DESC LIMIT ? OFFSET ?", params + [limit, offset])
            return self._rows(cur)

    def failure_groups(self, hours: Optional[float] = 24, include_resolved: bool = False) -> List[Dict[str, Any]]:
        clauses, params = [], []
        if not include_resolved:
            clauses.append("resolved = 0")
        if hours:
            clauses.append("ts >= ?")
            params.append(_since(hours))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._conn_ctx() as conn:
            cur = conn.execute(
                f"SELECT signature, dest_table, source_table, operation, COUNT(*) AS count, MIN(ts) AS first_seen, MAX(ts) AS last_seen, "
                f"MIN(error) AS sample_error, MIN(id) AS sample_id FROM failures{where} "
                f"GROUP BY signature, dest_table, source_table, operation ORDER BY count DESC", params)
            return self._rows(cur)

    def failure(self, failure_id: int) -> Optional[Dict[str, Any]]:
        with self._conn_ctx() as conn:
            rows = self._rows(conn.execute("SELECT * FROM failures WHERE id = ?", (failure_id,)))
            return rows[0] if rows else None

    def resolve_failures(self, ids: Optional[Iterable[int]] = None, signature: Optional[str] = None, resolution: str = "manual") -> int:
        with self._conn_ctx() as conn:
            if ids:
                ids = list(ids)
                cur = conn.execute(f"UPDATE failures SET resolved = 1, resolution = ?, resolved_at = ? WHERE id IN ({','.join('?' * len(ids))})",
                                   [resolution, _now()] + ids)
            elif signature:
                cur = conn.execute("UPDATE failures SET resolved = 1, resolution = ?, resolved_at = ? WHERE signature = ? AND resolved = 0",
                                   (resolution, _now(), signature))
            else:
                return 0
            return int(cur.rowcount or 0)

    def bump_attempt(self, failure_id: int, error: Optional[str] = None) -> None:
        with self._conn_ctx() as conn:
            if error:
                conn.execute("UPDATE failures SET attempts = attempts + 1, error = ?, ts = ? WHERE id = ?", (error, _now(), failure_id))
            else:
                conn.execute("UPDATE failures SET attempts = attempts + 1 WHERE id = ?", (failure_id,))

    # -- retention ---------------------------------------------------------
    def purge(self, audit_days: int = 30, failures_days: int = 90) -> Dict[str, int]:
        self.flush()
        with self._conn_ctx() as conn:
            a = conn.execute("DELETE FROM audit WHERE ts < ?", (_since(audit_days * 24),)).rowcount
            f = conn.execute("DELETE FROM failures WHERE resolved = 1 AND ts < ?", (_since(failures_days * 24),)).rowcount
        return {"audit_deleted": a, "failures_deleted": f}

    def close(self) -> None:
        self.flush()
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
        if self._mem_conn is not None:
            self._mem_conn.close()
            self._mem_conn = None
