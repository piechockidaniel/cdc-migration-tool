"""Demo source engine - synthetic change stream, no database required.

Purpose: try the UI / pipeline / snapshot / DLQ / alerting machinery before a
real source is available, and smoke-test deployments. It keeps an in-memory
``Customers`` table (``schema`` ``demo``) and emits INSERT/UPDATE/DELETE events
at ``rate_per_second``. ``poison_ratio`` makes a share of rows carry a NULL
``Name`` so that a destination with ``NOT NULL`` produces dead-letter entries.

State is process-wide per connection id so several pipelines/snapshots see the
same data.
"""
from __future__ import annotations

import random
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, Operation, Position, PrerequisiteCheck, SourceConnector, TableRef,
)
from backend.core import metrics

_CITIES = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz", "Oslo", "Rome", "Lisbon", "Vienna"]
_STATE: Dict[str, "_DemoState"] = {}
_STATE_LOCK = threading.Lock()


class _DemoState:
    def __init__(self, rows: int, seed: int):
        self.rng = random.Random(seed)
        self.rows: Dict[int, Dict[str, Any]] = {}
        self.next_id = 1
        self.log: List[ChangeEvent] = []
        self.position = 0
        self.lock = threading.RLock()
        for _ in range(rows):
            self._insert_row(record=False)

    def _row(self, cid: int) -> Dict[str, Any]:
        return {"CustomerID": cid, "Name": f"Customer {cid}", "Email": f"c{cid}@example.com",
                "City": self.rng.choice(_CITIES), "Balance": round(self.rng.uniform(-500, 5000), 2),
                "ModifiedDate": datetime.now()}

    def _insert_row(self, record: bool = True, poison: bool = False) -> Dict[str, Any]:
        cid = self.next_id
        self.next_id += 1
        row = self._row(cid)
        if poison:
            row["Name"] = None
        self.rows[cid] = row
        if record:
            self._emit(Operation.INSERT, after=dict(row))
        return row

    def _emit(self, op: Operation, after=None, before=None) -> ChangeEvent:
        self.position += 1
        src = after if after is not None else before
        ev = ChangeEvent(source="demo", schema="demo", table="Customers", operation=op, position=Position("demo", self.position),
                         key={"CustomerID": src["CustomerID"]}, after=after, before=before, source_ts=datetime.now(),
                         tx_id=str(self.position), event_id=str(uuid.uuid4()))
        self.log.append(ev)
        if len(self.log) > 100_000:
            del self.log[:50_000]
        return ev

    def generate(self, poison_ratio: float) -> ChangeEvent:
        with self.lock:
            r = self.rng.random()
            if r < 0.5 or not self.rows:
                return self._emit(Operation.INSERT, after=dict(self._insert_row(record=False, poison=self.rng.random() < poison_ratio)))
            cid = self.rng.choice(list(self.rows))
            if r < 0.9:
                before = dict(self.rows[cid])
                row = self.rows[cid]
                row["City"] = self.rng.choice(_CITIES)
                row["Balance"] = round(row["Balance"] + self.rng.uniform(-100, 100), 2)
                row["ModifiedDate"] = datetime.now()
                return self._emit(Operation.UPDATE, after=dict(row), before=before)
            before = self.rows.pop(cid)
            return self._emit(Operation.DELETE, before=dict(before))


def _state(name: str, rows: int, seed: int) -> _DemoState:
    with _STATE_LOCK:
        st = _STATE.get(name)
        if st is None:
            st = _STATE[name] = _DemoState(rows, seed)
        return st


class DemoConnector(SourceConnector):
    engine = "demo"
    capabilities = frozenset({Capability.NATIVE_CDC, Capability.PUSH_EVENTS, Capability.BEFORE_IMAGE, Capability.KEYSET_SNAPSHOT,
                              Capability.TRANSACTION_BOUNDARIES})

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.rate = float(config.get("rate_per_second", 5))
        self.poison_ratio = float(config.get("poison_ratio", 0.0))
        self.state = _state(self.name, int(config.get("initial_rows", 5000)), int(config.get("seed", 42)))
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def server_version(self) -> str:
        return "demo 1.0 (synthetic change generator)"

    def quote(self, identifier: str) -> str:
        return identifier

    def list_tables(self) -> List[TableRef]:
        return [TableRef("demo", "Customers", True, len(self.state.rows), len(self.state.rows) * 120)]

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        return [ColumnRef("CustomerID", "int", False, True, 1), ColumnRef("Name", "nvarchar(100)", True, False, 2),
                ColumnRef("Email", "nvarchar(200)", True, False, 3), ColumnRef("City", "nvarchar(100)", True, False, 4),
                ColumnRef("Balance", "decimal(12,2)", True, False, 5), ColumnRef("ModifiedDate", "datetime2", True, False, 6)]

    def check_prerequisites(self, tables=None) -> List[PrerequisiteCheck]:
        return [PrerequisiteCheck("connection", True, self.server_version()),
                PrerequisiteCheck("demo_engine", True, f"rate={self.rate}/s poison_ratio={self.poison_ratio}", severity="info"),
                PrerequisiteCheck("not_for_production", True, "synthetic data only", severity="warning")]

    def enable_capture_database(self) -> Tuple[bool, str]:
        return True, "demo capture is always on"

    def enable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        return True, f"{schema}.{table}: demo capture is always on"

    def disable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        return True, "noop"

    def capture_status(self) -> Dict[str, Any]:
        return {"database": "demo", "cdc_enabled": True, "position": self.state.position, "rows": len(self.state.rows)}

    def current_position(self) -> Optional[Position]:
        return Position("demo", self.state.position)

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        start = int(from_position.value) if from_position and from_position.value is not None else 0
        with self.state.lock:
            evs = [e for e in self.state.log if e.position.value > start][:limit]
        for e in evs:
            metrics.EVENTS_READ.labels(source=self.name, table=e.table_key, op=e.operation.value).inc()
        return evs, (evs[-1].position if evs else (from_position or self.current_position()))

    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        pos = from_position if isinstance(from_position, Position) else None
        if isinstance(from_position, dict):
            vals = [p.value for p in from_position.values() if p]
            pos = Position("demo", max(vals)) if vals else None
        while not stop_event.is_set():
            evs, pos = self.read_changes(tables, pos, None, 500)
            for ev in evs:
                yield ev
            if not evs:
                self.state.generate(self.poison_ratio)
                stop_event.wait(1.0 / max(self.rate, 0.01))

    def count_rows(self, schema, table, approximate=True):
        return len(self.state.rows)

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        with self.state.lock:
            rows = sorted(self.state.rows.values(), key=lambda r: r["CustomerID"])
        if after_key is not None:
            rows = [r for r in rows if r["CustomerID"] > after_key[0]]
        time.sleep(0.02)  # make progress visible
        return [{c: r.get(c) for c in columns} for r in rows[:chunk_size]]

    def key_bounds(self, schema, table, key_columns):
        if not self.state.rows:
            return None, None
        ids = sorted(self.state.rows)
        return (ids[0],), (ids[-1],)

    def retention_status(self, _positions):
        return {"retention_minutes": 4320, "retention_margin_minutes": 4300, "position_before_min": False, "capture_errors": 0}
