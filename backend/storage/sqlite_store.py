"""SQLite (WAL mode) backed configuration store.

Why SQLite for a *single node* deployment:

* Writes are transactional and crash-safe (journal + fsync) - a killed process
  can never leave a half-written ``mappings.json`` behind.
* WAL mode gives "readers never block writers, writers never block readers";
  the single-writer constraint is enforced by SQLite itself with a busy
  timeout, so several worker processes (``uvicorn --workers``) or a CLI and
  the API can share the same file safely.
* Optimistic versioning + a history table give an audit trail of *who changed
  which mapping when* - a hard requirement for the "documentation and
  transparency from the very beginning" goal.

For multi-node / HA the same interface can be implemented on PostgreSQL; the
schema below is deliberately plain SQL for that reason.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.core import metrics
from backend.storage.base import ConfigStore, ConflictError, Document

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    collection TEXT NOT NULL,
    key        TEXT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT,
    body       TEXT NOT NULL,
    PRIMARY KEY (collection, key)
);
CREATE TABLE IF NOT EXISTS document_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    collection TEXT NOT NULL,
    key        TEXT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT,
    action     TEXT NOT NULL,
    body       TEXT
);
CREATE INDEX IF NOT EXISTS ix_history_doc ON document_history (collection, key, id DESC);
"""


class SqliteConfigStore(ConfigStore):
    backend_name = "sqlite"

    def __init__(self, path: str | Path, busy_timeout_ms: int = 5000, keep_history: bool = True):
        self.path = str(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.keep_history = keep_history
        self._local = threading.local()
        self._init_lock = threading.Lock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._shared_memory_conn: Optional[sqlite3.Connection] = None
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    # -- connection handling (one per thread, SQLite objects are not shareable) --
    def _conn(self) -> sqlite3.Connection:
        if self.path == ":memory:":
            # in-memory DBs are per-connection; keep one shared connection (tests only)
            with self._init_lock:
                if self._shared_memory_conn is None:
                    self._shared_memory_conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
                return self._shared_memory_conn
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000.0, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    @staticmethod
    def _row_to_doc(row) -> Document:
        collection, key, version, updated_at, updated_by, body = row
        return Document(collection, key, json.loads(body), version, updated_at, updated_by)

    # -- reads -------------------------------------------------------------
    def get(self, collection: str, key: str) -> Optional[Document]:
        cur = self._conn().execute(
            "SELECT collection, key, version, updated_at, updated_by, body FROM documents WHERE collection=? AND key=?",
            (collection, key),
        )
        row = cur.fetchone()
        return self._row_to_doc(row) if row else None

    def get_all(self, collection: str) -> List[Document]:
        cur = self._conn().execute(
            "SELECT collection, key, version, updated_at, updated_by, body FROM documents WHERE collection=? ORDER BY key",
            (collection,),
        )
        return [self._row_to_doc(r) for r in cur.fetchall()]

    # -- writes ------------------------------------------------------------
    def _write_one(self, conn: sqlite3.Connection, collection: str, key: str, body: Dict[str, Any],
                   expected_version: Optional[int], updated_by: Optional[str]) -> Document:
        row = conn.execute(
            "SELECT version FROM documents WHERE collection=? AND key=?", (collection, key)
        ).fetchone()
        current = row[0] if row else 0
        if expected_version is not None and expected_version != current:
            raise ConflictError(
                f"{collection}/{key}: expected version {expected_version}, stored version is {current}"
            )
        new_version = current + 1
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(body, default=str, separators=(",", ":"))
        conn.execute(
            "INSERT INTO documents (collection, key, version, updated_at, updated_by, body) VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(collection, key) DO UPDATE SET version=excluded.version, updated_at=excluded.updated_at, "
            "updated_by=excluded.updated_by, body=excluded.body",
            (collection, key, new_version, now, updated_by, payload),
        )
        if self.keep_history:
            conn.execute(
                "INSERT INTO document_history (collection, key, version, updated_at, updated_by, action, body) "
                "VALUES (?,?,?,?,?,?,?)",
                (collection, key, new_version, now, updated_by, "insert" if current == 0 else "update", payload),
            )
        metrics.CONFIG_WRITES.labels(collection=collection).inc()
        return Document(collection, key, body, new_version, now, updated_by)

    def put(self, collection, key, body, expected_version=None, updated_by=None) -> Document:
        start = time.perf_counter()
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            doc = self._write_one(conn, collection, key, body, expected_version, updated_by)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        metrics.CONFIG_WRITE_DURATION.labels(backend=self.backend_name).observe(time.perf_counter() - start)
        return doc

    def put_many(self, documents: List[Document]) -> None:
        if not documents:
            return
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            for d in documents:
                self._write_one(conn, d.collection, d.key, d.body, None, d.updated_by)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def delete(self, collection: str, key: str, expected_version: Optional[int] = None) -> bool:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT version, body FROM documents WHERE collection=? AND key=?", (collection, key)
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return False
            if expected_version is not None and expected_version != row[0]:
                raise ConflictError(f"{collection}/{key}: expected version {expected_version}, stored {row[0]}")
            conn.execute("DELETE FROM documents WHERE collection=? AND key=?", (collection, key))
            if self.keep_history:
                conn.execute(
                    "INSERT INTO document_history (collection, key, version, updated_at, updated_by, action, body) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (collection, key, row[0] + 1, datetime.now(timezone.utc).isoformat(), None, "delete", row[1]),
                )
            conn.execute("COMMIT")
            metrics.CONFIG_WRITES.labels(collection=collection).inc()
            return True
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def history(self, collection: str, key: str, limit: int = 50) -> List[Document]:
        cur = self._conn().execute(
            "SELECT collection, key, version, updated_at, updated_by, body, action FROM document_history "
            "WHERE collection=? AND key=? ORDER BY id DESC LIMIT ?",
            (collection, key, limit),
        )
        out = []
        for row in cur.fetchall():
            body = json.loads(row[5]) if row[5] else {}
            body["_action"] = row[6]
            out.append(Document(row[0], row[1], body, row[2], row[3], row[4]))
        return out

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
        if self._shared_memory_conn is not None:
            self._shared_memory_conn.close()
            self._shared_memory_conn = None
