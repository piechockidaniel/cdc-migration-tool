"""Test doubles: a scriptable DB-API connection, an in-memory source connector and helpers."""
from __future__ import annotations

import re
import sqlite3
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, Operation, Position, PrerequisiteCheck, SourceConnector, TableRef,
)
from backend.destinations.sql_destination import SqlDestination
from backend.models.schemas import ColumnMapping, Mapping, MappingType


# ---------------------------------------------------------------------------
# scriptable DB-API connection
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, conn: "FakeDbApiConnection"):
        self.conn = conn
        self.description = None
        self._rows: List[tuple] = []
        self.rowcount = -1
        self.fast_executemany = False

    def execute(self, sql: str, params: Any = None):
        self.conn.calls.append((sql, params))
        result = self.conn.respond(sql, params)
        if result is None:
            self.description = None
            self._rows = []
            self.rowcount = 1
        else:
            cols, rows = result
            self.description = [(c, None, None, None, None, None, None) for c in cols]
            self._rows = [tuple(r) for r in rows]
            self.rowcount = len(rows)
        return self

    def executemany(self, sql: str, seq: Sequence[Any]):
        for p in seq:
            self.conn.calls.append((sql, p))
        self.rowcount = len(seq)
        self.description = None
        return self

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


class FakeDbApiConnection:
    """``responders`` is a list of ``(regex, callable(sql, params) -> (cols, rows) | None)``."""

    def __init__(self, responders: Optional[List[Tuple[str, Callable]]] = None):
        self.responders = responders or []
        self.calls: List[Tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.autocommit = False

    def add(self, pattern: str, result):
        fn = result if callable(result) else (lambda sql, params: result)
        self.responders.append((pattern, fn))
        return self

    def respond(self, sql: str, params: Any):
        for pattern, fn in self.responders:
            if re.search(pattern, sql, re.I | re.S):
                return fn(sql, params)
        return None

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True

    def sql_calls(self, pattern: str) -> List[Tuple[str, Any]]:
        return [c for c in self.calls if re.search(pattern, c[0], re.I | re.S)]


# ---------------------------------------------------------------------------
# in-memory source connector
# ---------------------------------------------------------------------------
class FakeConnector(SourceConnector):
    """Source with an in-memory table store and a scripted change stream."""

    engine = "fake"
    capabilities = frozenset({Capability.NATIVE_CDC, Capability.KEYSET_SNAPSHOT, Capability.PUSH_EVENTS})

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.tables: Dict[str, List[Dict[str, Any]]] = {}
        self.pks: Dict[str, List[str]] = {}
        self.scripted: List[ChangeEvent] = []
        self.connected = False
        self.position_counter = 0
        self.confirmed: List[Position] = []
        self.stream_started = threading.Event()
        self.read_delay = 0.0
        self.fail_connect = False
        self.snapshot_reads = 0

    # data helpers
    def add_table(self, schema: str, table: str, rows: List[Dict[str, Any]], pk: List[str]):
        self.tables[f"{schema}.{table}"] = list(rows)
        self.pks[f"{schema}.{table}"] = pk

    def script(self, schema: str, table: str, op: Operation, after=None, before=None, key=None, ts=None) -> ChangeEvent:
        self.position_counter += 1
        ev = ChangeEvent(source=self.name, schema=schema, table=table, operation=op,
                         position=Position("fake", self.position_counter), key=key or {}, after=after, before=before,
                         source_ts=ts or datetime.now(), event_id=str(uuid.uuid4()))
        self.scripted.append(ev)
        return ev

    # lifecycle
    def connect(self):
        if self.fail_connect:
            raise RuntimeError("cannot connect")
        self.connected = True

    def disconnect(self):
        self.connected = False

    def is_connected(self):
        return self.connected

    def server_version(self):
        return "fake 1.0"

    def list_tables(self):
        return [TableRef(k.split(".")[0], k.split(".")[1], True, len(v)) for k, v in self.tables.items()]

    def get_columns(self, schema, table):
        rows = self.tables.get(f"{schema}.{table}", [])
        cols = list(rows[0].keys()) if rows else []
        pk = self.pks.get(f"{schema}.{table}", [])
        return [ColumnRef(c, "text", True, c in pk, i) for i, c in enumerate(cols)]

    def quote(self, identifier):
        return identifier

    def check_prerequisites(self, tables=None):
        return [PrerequisiteCheck("connection", True, "fake")]

    def enable_capture_database(self):
        return True, "ok"

    def enable_capture_table(self, schema, table, **options):
        return True, "ok"

    def disable_capture_table(self, schema, table, **options):
        return True, "ok"

    def capture_status(self):
        return {"cdc_enabled": True}

    def current_position(self):
        return Position("fake", self.position_counter)

    def confirm_position(self, position):
        self.confirmed.append(position)

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        start = from_position.value if from_position else 0
        wanted = {(s, t) for s, t in tables}
        # positions are monotonically increasing -> binary search instead of a full scan
        import bisect
        idx = bisect.bisect_right([e.position.value for e in self.scripted], start) if len(self.scripted) < 1000 else \
            bisect.bisect_right(self._positions_cache(), start)
        evs = []
        for e in self.scripted[idx:]:
            if (e.schema, e.table) in wanted:
                evs.append(e)
                if len(evs) >= limit:
                    break
        return evs, (evs[-1].position if evs else from_position)

    def _positions_cache(self):
        cache = getattr(self, "_pos_cache", None)
        if cache is None or len(cache) != len(self.scripted):
            cache = [e.position.value for e in self.scripted]
            self._pos_cache = cache
        return cache

    def stream_changes(self, tables, from_position, stop_event) -> Iterator[ChangeEvent]:
        self.stream_started.set()
        pos = from_position.value if isinstance(from_position, Position) else 0
        if isinstance(from_position, dict):
            pos = max((p.value for p in from_position.values() if p), default=0)
        while not stop_event.is_set():
            evs, last = self.read_changes(tables, Position("fake", pos), None, 100)
            if not evs:
                stop_event.wait(0.02)
                continue
            for ev in evs:
                if self.read_delay:
                    stop_event.wait(self.read_delay)
                yield ev
            pos = last.value

    def count_rows(self, schema, table, approximate=True):
        return len(self.tables.get(f"{schema}.{table}", []))

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        self.snapshot_reads += 1
        rows = sorted(self.tables.get(f"{schema}.{table}", []), key=lambda r: tuple(r[k] for k in key_columns))
        if after_key is not None:
            rows = [r for r in rows if tuple(r[k] for k in key_columns) > tuple(after_key)]
        return [{c: r.get(c) for c in columns} for r in rows[:chunk_size]]

    def key_bounds(self, schema, table, key_columns):
        rows = self.tables.get(f"{schema}.{table}", [])
        if not rows:
            return None, None
        vals = sorted(r[key_columns[0]] for r in rows)
        return (vals[0],), (vals[-1],)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
_keepers: List[sqlite3.Connection] = []


def sqlite_destination(schema_sql: Optional[str] = None) -> SqlDestination:
    """Shared-cache in-memory SQLite that survives disconnect/reconnect cycles."""
    uri = f"file:dest_{uuid.uuid4().hex}?mode=memory&cache=shared"
    keeper = sqlite3.connect(uri, uri=True, check_same_thread=False)
    _keepers.append(keeper)
    keeper.executescript(schema_sql or """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, city TEXT, updated TEXT);
        CREATE TABLE orders (order_id INTEGER, line INTEGER, amount REAL, PRIMARY KEY(order_id, line));
    """)
    return SqlDestination("sqlite", lambda: sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=5), name="dest")


def make_mapping(mapping_id: str = "m1", source_table: str = "Customers", dest_table: str = "customers", dest_schema: str = "main",
                 key_columns: Optional[List[str]] = None, transformations: bool = False, **kw) -> Mapping:
    cms = [
        ColumnMapping(source_column="CustomerID", destination_column="id"),
        ColumnMapping(source_column="Name", destination_column="name"),
        ColumnMapping(source_column="Email", destination_column="email"),
    ]
    if transformations:
        cms.append(ColumnMapping(source_column="City", destination_column="city", transformation='upper("City")'))
    else:
        cms.append(ColumnMapping(source_column="City", destination_column="city"))
    return Mapping(id=mapping_id, name=mapping_id, mapping_type=MappingType.TABLE, source_schema="dbo", source_table=source_table,
                   destination_schema=dest_schema, destination_table=dest_table, column_mappings=cms, key_columns=key_columns, **kw)
