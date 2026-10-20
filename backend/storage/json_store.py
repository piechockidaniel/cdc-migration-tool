"""Hardened JSON-file configuration store.

Kept for compatibility (``config/*.json`` is human readable and easy to diff
in git) but fixed so it is at least *safe*:

* **Atomic writes** - write to a temp file in the same directory, ``fsync``,
  then ``os.replace``. A crash mid-write can no longer truncate the file.
* **Cross-process advisory lock** (``fcntl.flock`` / ``msvcrt.locking``) around
  every read-modify-write cycle, so two processes cannot interleave.
* **Optimistic versioning** - every document carries ``_version``; a stale
  writer gets ``ConflictError`` instead of silently winning.
* **mtime based cache invalidation** - a process notices that another one
  rewrote the file.

It is still a *whole-file rewrite per change* (O(size) per write, one global
lock per collection) which is why it is not the recommended backend - see
``docs/developer-guide/03-storage.md``.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from backend.core import metrics
from backend.storage.base import ConfigStore, ConflictError, Document

if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import msvcrt

    def _lock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    def _unlock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def atomic_write_json(path: Path, data: Any) -> None:
    """Write ``data`` to ``path`` atomically (temp file + fsync + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        # fsync the directory so the rename itself is durable (POSIX)
        if sys.platform != "win32":
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class JsonFileConfigStore(ConfigStore):
    backend_name = "json"

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._thread_lock = threading.RLock()
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_mtime: Dict[str, float] = {}

    # -- files -------------------------------------------------------------
    def _file(self, collection: str) -> Path:
        return self.directory / f"{collection}.json"

    def _lock_file(self, collection: str) -> Path:
        return self.directory / f".{collection}.lock"

    @contextmanager
    def _locked(self, collection: str) -> Iterator[None]:
        """Process-wide *and* cross-process exclusive lock for one collection."""
        with self._thread_lock:
            lock_path = self._lock_file(collection)
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a+") as fh:
                _lock(fh)
                try:
                    yield
                finally:
                    _unlock(fh)

    def _load(self, collection: str) -> Dict[str, Any]:
        path = self._file(collection)
        if not path.exists():
            self._cache[collection] = {}
            self._cache_mtime[collection] = 0.0
            return {}
        mtime = path.stat().st_mtime_ns
        if self._cache_mtime.get(collection) == mtime and collection in self._cache:
            return self._cache[collection]
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
        data = json.loads(content) if content else {}
        self._cache[collection] = data
        self._cache_mtime[collection] = mtime
        return data

    def _save(self, collection: str, data: Dict[str, Any]) -> None:
        path = self._file(collection)
        atomic_write_json(path, data)
        self._cache[collection] = data
        self._cache_mtime[collection] = path.stat().st_mtime_ns

    @staticmethod
    def _to_doc(collection: str, key: str, raw: Dict[str, Any]) -> Document:
        body = {k: v for k, v in raw.items() if not k.startswith("_meta_")}
        return Document(
            collection,
            key,
            body,
            int(raw.get("_meta_version", 1)),
            raw.get("_meta_updated_at", ""),
            raw.get("_meta_updated_by"),
        )

    # -- reads -------------------------------------------------------------
    def get(self, collection: str, key: str) -> Optional[Document]:
        with self._locked(collection):
            data = self._load(collection)
        raw = data.get(key)
        return self._to_doc(collection, key, raw) if raw is not None else None

    def get_all(self, collection: str) -> List[Document]:
        with self._locked(collection):
            data = self._load(collection)
        return [self._to_doc(collection, k, v) for k, v in sorted(data.items())]

    # -- writes ------------------------------------------------------------
    def put(self, collection, key, body, expected_version=None, updated_by=None) -> Document:
        start = time.perf_counter()
        with self._locked(collection):
            data = dict(self._load(collection))
            current = int(data.get(key, {}).get("_meta_version", 0)) if key in data else 0
            if expected_version is not None and expected_version != current:
                raise ConflictError(f"{collection}/{key}: expected version {expected_version}, stored {current}")
            now = datetime.now(timezone.utc).isoformat()
            raw = dict(body)
            raw["_meta_version"] = current + 1
            raw["_meta_updated_at"] = now
            raw["_meta_updated_by"] = updated_by
            data[key] = raw
            self._save(collection, data)
        metrics.CONFIG_WRITES.labels(collection=collection).inc()
        metrics.CONFIG_WRITE_DURATION.labels(backend=self.backend_name).observe(time.perf_counter() - start)
        return Document(collection, key, body, current + 1, now, updated_by)

    def put_many(self, documents: List[Document]) -> None:
        # Group by collection so each file is rewritten once; not atomic across
        # collections (one more reason to prefer SQLite).
        by_collection: Dict[str, List[Document]] = {}
        for d in documents:
            by_collection.setdefault(d.collection, []).append(d)
        for collection, docs in by_collection.items():
            with self._locked(collection):
                data = dict(self._load(collection))
                now = datetime.now(timezone.utc).isoformat()
                for d in docs:
                    current = int(data.get(d.key, {}).get("_meta_version", 0)) if d.key in data else 0
                    raw = dict(d.body)
                    raw["_meta_version"] = current + 1
                    raw["_meta_updated_at"] = now
                    raw["_meta_updated_by"] = d.updated_by
                    data[d.key] = raw
                self._save(collection, data)

    def delete(self, collection: str, key: str, expected_version: Optional[int] = None) -> bool:
        with self._locked(collection):
            data = dict(self._load(collection))
            if key not in data:
                return False
            current = int(data[key].get("_meta_version", 1))
            if expected_version is not None and expected_version != current:
                raise ConflictError(f"{collection}/{key}: expected version {expected_version}, stored {current}")
            del data[key]
            self._save(collection, data)
        metrics.CONFIG_WRITES.labels(collection=collection).inc()
        return True

    def close(self) -> None:
        self._cache.clear()
        self._cache_mtime.clear()
