"""Connector abstraction shared by all CDC sources.

A *connector* knows how to:

1. validate the prerequisites for change capture on a given engine
   (``check_prerequisites``) - the thing you want to run *before* touching a
   production instance;
2. enable / disable capture at database and table level;
3. read changes either by polling a position range (``read_changes``) or by
   streaming from a push-capable mechanism (``stream_changes``: change streams,
   binlog stream, Service-Broker queue...);
4. read snapshot chunks with keyset pagination so a multi-TB table can be
   copied in small, resumable, throttled steps.

Everything the pipeline sees is a ``ChangeEvent`` with an opaque, serialisable
``Position`` - LSNs, SCNs, binlog coordinates and Mongo resume tokens all fit.
"""
from __future__ import annotations

import enum
import json
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple


class Operation(str, enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    DDL = "DDL"


class Capability(str, enum.Enum):
    NATIVE_CDC = "native_cdc"            # engine has log based change capture
    PUSH_EVENTS = "push_events"          # changes can be received without polling
    BEFORE_IMAGE = "before_image"        # UPDATE/DELETE carry the old row
    KEYSET_SNAPSHOT = "keyset_snapshot"  # ordered keyset pagination is possible
    TRANSACTION_BOUNDARIES = "tx_boundaries"
    CONSISTENT_SNAPSHOT = "consistent_snapshot"  # snapshot can be pinned to a position


@dataclass(frozen=True)
class Position:
    """Opaque, totally-ordered-per-source, JSON-serialisable stream position."""

    kind: str          # e.g. "mssql_lsn", "pg_lsn", "binlog", "oracle_scn", "mongo_token"
    value: Any         # hex string, int, (file, pos) tuple, dict...
    extra: Dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def to_json(self) -> Dict[str, Any]:
        return {"kind": self.kind, "value": self.value, "extra": self.extra}

    @classmethod
    def from_json(cls, data: Optional[Dict[str, Any]]) -> Optional["Position"]:
        if not data:
            return None
        return cls(data["kind"], data["value"], data.get("extra") or {})

    def __str__(self) -> str:
        return f"{self.kind}:{self.value}"


@dataclass
class ChangeEvent:
    """One row-level change."""

    source: str                       # connection id / alias
    schema: str
    table: str
    operation: Operation
    position: Position
    key: Dict[str, Any] = field(default_factory=dict)       # primary key values (after image for inserts/updates)
    after: Optional[Dict[str, Any]] = None
    before: Optional[Dict[str, Any]] = None
    source_ts: Optional[datetime] = None                     # commit / change time at the source
    tx_id: Optional[str] = None
    received_at: datetime = field(default_factory=datetime.now)
    event_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def table_key(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def data(self) -> Dict[str, Any]:
        """Row image the destination should end up with (after, or before for deletes)."""
        if self.after is not None:
            return self.after
        return self.before or {}

    def to_json(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "schema": self.schema,
            "table": self.table,
            "operation": self.operation.value,
            "position": self.position.to_json(),
            "key": self.key,
            "after": self.after,
            "before": self.before,
            "source_ts": self.source_ts.isoformat() if self.source_ts else None,
            "tx_id": self.tx_id,
            "received_at": self.received_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class PrerequisiteCheck:
    name: str
    ok: bool
    detail: str = ""
    fix_hint: str = ""
    severity: str = "error"   # error | warning | info

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class TableRef:
    schema: str
    table: str
    cdc_enabled: bool = False
    row_estimate: Optional[int] = None
    size_bytes: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class ColumnRef:
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    ordinal: int = 0
    max_length: Optional[int] = None


class ConnectorError(RuntimeError):
    pass


class SourceConnector(ABC):
    """Base class for every CDC source implementation."""

    engine: str = "abstract"
    capabilities: frozenset = frozenset()

    def __init__(self, config: Dict[str, Any], **_options: Any):
        self.config = dict(config)
        self.name: str = str(config.get("id") or config.get("name") or self.engine)
        self._lock = threading.RLock()

    # -- lifecycle ---------------------------------------------------------
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def is_connected(self) -> bool: ...

    def test_connection(self) -> Tuple[bool, str]:
        try:
            self.connect()
            version = self.server_version()
            return True, f"Connected. Server: {version}"
        except Exception as exc:
            return False, str(exc)

    @abstractmethod
    def server_version(self) -> str: ...

    # -- metadata ----------------------------------------------------------
    @abstractmethod
    def list_tables(self) -> List[TableRef]: ...

    @abstractmethod
    def get_columns(self, schema: str, table: str) -> List[ColumnRef]: ...

    def get_primary_key(self, schema: str, table: str) -> List[str]:
        return [c.name for c in sorted(self.get_columns(schema, table), key=lambda c: c.ordinal) if c.is_primary_key]

    @abstractmethod
    def quote(self, identifier: str) -> str: ...

    def qualified(self, schema: str, table: str) -> str:
        return f"{self.quote(schema)}.{self.quote(table)}" if schema else self.quote(table)

    # -- prerequisites / capture management --------------------------------
    @abstractmethod
    def check_prerequisites(self, tables: Optional[Sequence[Tuple[str, str]]] = None) -> List[PrerequisiteCheck]: ...

    @abstractmethod
    def enable_capture_database(self) -> Tuple[bool, str]: ...

    @abstractmethod
    def enable_capture_table(self, schema: str, table: str, **options: Any) -> Tuple[bool, str]: ...

    @abstractmethod
    def disable_capture_table(self, schema: str, table: str, **options: Any) -> Tuple[bool, str]: ...

    @abstractmethod
    def capture_status(self) -> Dict[str, Any]: ...

    # -- change reading ----------------------------------------------------
    @abstractmethod
    def current_position(self) -> Optional[Position]:
        """Cheap "watermark" read: the newest position available at the source."""

    @abstractmethod
    def read_changes(
        self,
        tables: Sequence[Tuple[str, str]],
        from_position: Optional[Position],
        to_position: Optional[Position] = None,
        limit: int = 10_000,
    ) -> Tuple[List[ChangeEvent], Optional[Position]]:
        """Poll changes strictly *after* ``from_position``.

        Returns the events and the position the caller should commit once they
        are applied (may be ``to_position`` even if no events were found).
        """

    def stream_changes(
        self,
        tables: Sequence[Tuple[str, str]],
        from_position: Optional[Position],
        stop_event: threading.Event,
    ) -> Iterator[ChangeEvent]:
        """Blocking push-style iterator. Default: not supported."""
        raise ConnectorError(f"{self.engine} connector does not support streaming")

    # -- snapshot support --------------------------------------------------
    @abstractmethod
    def count_rows(self, schema: str, table: str, approximate: bool = True) -> Optional[int]: ...

    @abstractmethod
    def read_snapshot_chunk(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        after_key: Optional[Sequence[Any]],
        chunk_size: int,
        where: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Read ``chunk_size`` rows ordered by ``key_columns`` strictly after ``after_key``."""

    def key_bounds(self, schema: str, table: str, key_columns: Sequence[str]) -> Tuple[Optional[Sequence[Any]], Optional[Sequence[Any]]]:
        """(min, max) of the key - optional, used for progress estimation."""
        return None, None

    # -- helpers -----------------------------------------------------------
    def describe(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "name": self.name,
            "capabilities": sorted(c.value for c in self.capabilities),
        }


# ---------------------------------------------------------------------------
# Keyset pagination helpers shared by the SQL connectors
# ---------------------------------------------------------------------------
def keyset_predicate(key_columns: Sequence[str], quote, placeholder: str = "?", row_values: bool = False) -> Tuple[str, int]:
    """Build ``(k1, k2) > (?, ?)`` or its expanded OR/AND form.

    Returns the SQL fragment and the number of parameters it consumes. The
    expanded form is needed for engines without row-value comparison
    (SQL Server).  Parameters must be supplied as ``[k1, k1, k2]`` order for
    the expanded form - use :func:`keyset_params`.
    """
    cols = [quote(c) for c in key_columns]
    if row_values:
        return f"({', '.join(cols)}) > ({', '.join([placeholder] * len(cols))})", len(cols)
    clauses = []
    for i, col in enumerate(cols):
        eqs = " AND ".join(f"{cols[j]} = {placeholder}" for j in range(i))
        clause = f"{col} > {placeholder}" if not eqs else f"{eqs} AND {col} > {placeholder}"
        clauses.append(f"({clause})")
    n_params = sum(i + 1 for i in range(len(cols)))
    return "(" + " OR ".join(clauses) + ")", n_params


def keyset_params(after_key: Sequence[Any], row_values: bool = False) -> List[Any]:
    if row_values:
        return list(after_key)
    params: List[Any] = []
    for i in range(len(after_key)):
        params.extend(after_key[: i + 1])
    return params


def json_safe(value: Any) -> Any:
    """Best-effort conversion of driver-specific values to JSON-serialisable ones."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)
