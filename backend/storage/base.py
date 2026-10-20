"""Abstract configuration / state store.

The store is a small document database: ``collection -> key -> JSON document``.
Every document carries a monotonically increasing ``version`` which enables
optimistic concurrency control (``put(..., expected_version=n)``) so two
operators / processes editing the same mapping cannot silently overwrite each
other - the classic lost-update problem of the original ``config/*.json``
approach.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional


class ConflictError(RuntimeError):
    """Raised when ``expected_version`` does not match the stored version."""


@dataclass
class Document:
    collection: str
    key: str
    body: Dict[str, Any]
    version: int = 0
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_by: Optional[str] = None


# Well-known collections. Using constants avoids typos spreading through the codebase.
CONNECTIONS = "connections"
MAPPINGS = "mappings"
WORKSETS = "worksets"
POSITIONS = "positions"           # CDC positions (LSN / SCN / binlog / resume token) per table
SNAPSHOT_JOBS = "snapshot_jobs"
ALERT_RULES = "alert_rules"
ALERT_STATE = "alert_state"
SETTINGS = "settings"
INGESTION_PATTERNS = "ingestion_patterns"

ALL_COLLECTIONS = (
    CONNECTIONS, MAPPINGS, WORKSETS, POSITIONS, SNAPSHOT_JOBS,
    ALERT_RULES, ALERT_STATE, SETTINGS, INGESTION_PATTERNS,
)


class ConfigStore(ABC):
    """Interface implemented by all storage backends."""

    backend_name = "abstract"

    # -- reads -------------------------------------------------------------
    @abstractmethod
    def get(self, collection: str, key: str) -> Optional[Document]:
        ...

    @abstractmethod
    def get_all(self, collection: str) -> List[Document]:
        ...

    def get_body(self, collection: str, key: str) -> Optional[Dict[str, Any]]:
        doc = self.get(collection, key)
        return doc.body if doc else None

    def get_all_bodies(self, collection: str) -> Dict[str, Dict[str, Any]]:
        return {d.key: d.body for d in self.get_all(collection)}

    # -- writes ------------------------------------------------------------
    @abstractmethod
    def put(
        self,
        collection: str,
        key: str,
        body: Dict[str, Any],
        expected_version: Optional[int] = None,
        updated_by: Optional[str] = None,
    ) -> Document:
        """Insert or replace a document.

        ``expected_version``: ``None`` = unconditional write, ``0`` = must not
        exist yet, ``n`` = current version must equal ``n``.
        """

    @abstractmethod
    def delete(self, collection: str, key: str, expected_version: Optional[int] = None) -> bool:
        ...

    @abstractmethod
    def put_many(self, documents: List[Document]) -> None:
        """Atomically write several documents (all-or-nothing)."""

    # -- history / ops -----------------------------------------------------
    def history(self, collection: str, key: str, limit: int = 50) -> List[Document]:
        """Previous versions of a document, newest first. Optional feature."""
        return []

    @abstractmethod
    def close(self) -> None:
        ...

    @contextmanager
    def batch(self) -> Iterator[List[Document]]:
        """Collect documents and write them atomically on exit."""
        docs: List[Document] = []
        yield docs
        if docs:
            self.put_many(docs)

    def stats(self) -> Dict[str, Any]:
        return {"backend": self.backend_name, "collections": {c: len(self.get_all(c)) for c in ALL_COLLECTIONS}}
