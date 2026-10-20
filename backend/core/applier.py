"""Row applier: turns ``ChangeEvent``s into destination writes.

Responsibilities
----------------
* resolve the mapping for an event's source table (column renames,
  transformations, column controls: ``ignore_changes``, ``default_value``,
  ``auto_generate`` - which the original engine validated but never applied);
* resolve **real** key columns (mapping -> destination PK -> source PK ->
  heuristic with an audit warning) instead of ``'id' in name``;
* batch consecutive events of the same (table, operation) into
  ``executemany`` calls, falling back to row-by-row isolation when a batch
  fails so one poison row does not block the stream;
* hand every failure to the audit store with a normalised error signature so
  patterns can be mined later.

Transformations that contain SQL expressions run in DuckDB on the whole batch
(one round trip), plain renames are done in Python.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.connectors.base import ChangeEvent, Operation
from backend.core import metrics
from backend.destinations.sql_destination import SqlDestination
from backend.models.schemas import AutoGenerateMode, ColumnMapping, Mapping, MappingType

logger = logging.getLogger(__name__)

_SQL_FUNCTION_HINT = re.compile(r"[()+\-*/|]|\b(CASE|CAST|COALESCE|CONCAT|UPPER|LOWER|TRIM|SUBSTR|JSON|NOW|CURRENT)\b", re.I)


@dataclass
class ApplyFailure:
    event: ChangeEvent
    error: str
    signature: str
    destination_table: str


@dataclass
class ApplyResult:
    applied: int = 0
    skipped: int = 0
    failures: List[ApplyFailure] = field(default_factory=list)
    duration_s: float = 0.0
    record_failures: bool = True

    @property
    def failed(self) -> int:
        return len(self.failures)


def error_signature(message: str) -> str:
    """Normalise an error message so similar failures share one signature.

    Numbers, quoted literals, GUIDs and hex blobs are replaced by placeholders;
    ``"String or binary data would be truncated in table 'X', column 'Y'"`` and
    the same error for another row therefore produce the same signature.
    """
    s = message.strip()
    s = re.sub(r"0x[0-9a-fA-F]+", "<hex>", s)
    s = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<guid>", s)
    s = re.sub(r"'(?:[^']|'')*'", "'<str>'", s)
    s = re.sub(r'"(?:[^"]|"")*"', '"<str>"', s)
    s = re.sub(r"\b\d+(\.\d+)?\b", "<n>", s)
    s = re.sub(r"\s+", " ", s)
    return s[:300]


@dataclass
class TableRuntime:
    """Pre-computed view of a TABLE mapping for the hot path."""

    mapping: Mapping
    source_key: str
    dest_schema: str
    dest_table: str
    renames: Dict[str, str]                       # source column -> destination column (plain copies)
    expressions: List[ColumnMapping]              # mappings with SQL transformations (DuckDB)
    ignored: set
    defaults: Dict[str, Any]                      # destination column -> default value
    auto_on_insert: Dict[str, str]                # destination column -> SQL expression (evaluated in DuckDB)
    auto_on_init: Dict[str, str]
    script: Optional[str] = None                  # batch-level DuckDB script (use_duckdb_transformation)
    key_columns: List[str] = field(default_factory=list)      # destination key columns
    key_source: str = "unresolved"
    sync_inserts: bool = True
    sync_updates: bool = True
    sync_deletes: bool = True
    insert_mode: str = "upsert"                   # upsert | insert

    @classmethod
    def from_mapping(cls, mapping: Mapping, insert_mode: str = "upsert") -> "TableRuntime":
        renames: Dict[str, str] = {}
        expressions: List[ColumnMapping] = []
        ignored = set()
        defaults: Dict[str, Any] = {}
        auto_insert: Dict[str, str] = {}
        auto_init: Dict[str, str] = {}
        for cm in mapping.column_mappings:
            if cm.ignore_changes:
                ignored.add(cm.destination_column)
                continue
            if cm.auto_generate == AutoGenerateMode.ON_INSERT and cm.auto_generate_expression:
                auto_insert[cm.destination_column] = cm.auto_generate_expression
            elif cm.auto_generate == AutoGenerateMode.ON_INIT and cm.auto_generate_expression:
                auto_init[cm.destination_column] = cm.auto_generate_expression
            if cm.default_value is not None:
                defaults[cm.destination_column] = cm.default_value
            if cm.transformation:
                expressions.append(cm)
            elif cm.source_column:
                renames[cm.source_column] = cm.destination_column
        return cls(
            mapping=mapping, source_key=f"{mapping.source_schema}.{mapping.source_table}",
            dest_schema=mapping.destination_schema, dest_table=mapping.destination_table,
            renames=renames, expressions=expressions, ignored=ignored, defaults=defaults,
            auto_on_insert=auto_insert, auto_on_init=auto_init,
            script=_resolve_script(mapping),
            key_columns=list(mapping.key_columns or []), key_source="mapping" if mapping.key_columns else "unresolved",
            sync_inserts=mapping.sync_inserts, sync_updates=mapping.sync_updates, sync_deletes=mapping.sync_deletes,
            insert_mode=insert_mode if mapping.sync_mode != "incremental" else "insert",
        )

    @property
    def dest_key(self) -> str:
        return f"{self.dest_schema}.{self.dest_table}"


class RowApplier:
    def __init__(self, destination: SqlDestination, duckdb=None, audit=None, source_pk_lookup: Optional[Callable[[str, str], List[str]]] = None,
                 insert_mode: str = "upsert"):
        self.destination = destination
        self.duckdb = duckdb
        self.audit = audit
        self.source_pk_lookup = source_pk_lookup
        self.insert_mode = insert_mode
        self._runtimes: Dict[str, TableRuntime] = {}
        self.unsafe_key_warnings: Dict[str, str] = {}

    # -- mapping registration ---------------------------------------------
    def register_mappings(self, mappings: Sequence[Mapping]) -> None:
        for m in mappings:
            if m.mapping_type != MappingType.TABLE or not m.enabled or m.is_multi_source:
                continue
            rt = TableRuntime.from_mapping(m, self.insert_mode)
            self._runtimes[rt.source_key] = rt

    def runtime_for(self, event: ChangeEvent) -> Optional[TableRuntime]:
        rt = self._runtimes.get(event.table_key)
        if rt is None:
            # case-insensitive fallback (Oracle upper-cases identifiers)
            for k, v in self._runtimes.items():
                if k.lower() == event.table_key.lower():
                    return v
        return rt

    # -- key resolution ----------------------------------------------------
    def resolve_keys(self, rt: TableRuntime) -> List[str]:
        if rt.key_columns:
            return rt.key_columns
        try:
            pk = self.destination.get_primary_key(rt.dest_schema, rt.dest_table)
        except Exception as exc:
            logger.warning("cannot read destination PK for %s: %s", rt.dest_key, exc)
            pk = []
        if pk:
            rt.key_columns, rt.key_source = pk, "destination_pk"
            return pk
        if self.source_pk_lookup:
            try:
                src_pk = self.source_pk_lookup(rt.mapping.source_schema, rt.mapping.source_table)
            except Exception:
                src_pk = []
            mapped = [rt.renames[c] for c in src_pk if c in rt.renames]
            if mapped and len(mapped) == len(src_pk):
                rt.key_columns, rt.key_source = mapped, "source_pk"
                return mapped
        # last resort: heuristic, flagged as unsafe
        dest_cols = list(rt.renames.values()) + [cm.destination_column for cm in rt.expressions]
        guess = [c for c in dest_cols if c.lower() == "id" or c.lower().endswith("_id") or c.lower().endswith("id")]
        guess = guess[:1] or dest_cols[:1]
        rt.key_columns, rt.key_source = guess, "heuristic"
        msg = f"No primary key found for {rt.dest_key}; guessed key {guess}. Define key_columns in the mapping."
        if rt.dest_key not in self.unsafe_key_warnings:
            self.unsafe_key_warnings[rt.dest_key] = msg
            logger.warning(msg)
            if self.audit:
                self.audit.record("warning", "applier", msg, table=rt.dest_key)
        return guess

    # -- transformation ----------------------------------------------------
    def transform_rows(self, rt: TableRuntime, rows: List[Dict[str, Any]], operation: Operation) -> List[Dict[str, Any]]:
        if rt.script and operation != Operation.DELETE:
            return self._duckdb_script(rt, rows)
        out: List[Dict[str, Any]] = []
        for row in rows:
            dest = {d: json_safe_for_sql(row.get(s)) for s, d in rt.renames.items() if s in row}
            out.append(dest)
        needs_duck = rt.expressions or (operation == Operation.INSERT and (rt.auto_on_insert or rt.auto_on_init)) or \
            (operation == Operation.UPDATE and rt.auto_on_insert)
        if needs_duck:
            if self.duckdb is None:
                raise RuntimeError(f"mapping {rt.mapping.id} needs DuckDB for transformations but no processor is configured")
            out = self._duckdb_transform(rt, rows, out, operation)
        for dest in out:
            for col, default in rt.defaults.items():
                if dest.get(col) is None:
                    dest[col] = default
            for col in rt.ignored:
                dest.pop(col, None)
        return out

    def _duckdb_script(self, rt: TableRuntime, source_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Batch-level DuckDB script (``use_duckdb_transformation``): the batch is loaded into
        ``{source_table}`` and the script's final SELECT defines the destination rows/columns.

        Scripts may filter, aggregate or reshape the batch; the output columns must include the key
        columns. (1.x stored this setting but never executed it.)
        """
        from backend.core.duckdb_processor import infer_duckdb_type
        if self.duckdb is None:
            raise RuntimeError(f"mapping {rt.mapping.id} has a DuckDB script but no processor is configured")
        staging = "applier_script_input"
        all_cols: List[str] = []
        for r in source_rows:
            for k in r.keys():
                if k not in all_cols:
                    all_cols.append(k)
        types = {c: infer_duckdb_type(next((r[c] for r in source_rows if r.get(c) is not None), None)) for c in all_cols}
        self.duckdb.create_staging_table(staging, types)
        self.duckdb.load_data(staging, source_rows)
        script = rt.script.replace("{source_table}", staging).replace("{destination_table}", rt.dest_table).replace("{mapping_id}", rt.mapping.id)
        statements = [s.strip() for s in script.split(";") if s.strip() and not all(line.strip().startswith("--") for line in s.strip().splitlines())]
        if not statements:
            raise RuntimeError(f"mapping {rt.mapping.id}: DuckDB script is empty")
        for stmt in statements[:-1]:
            self.duckdb.execute_query(stmt)
        rows = self.duckdb.execute_query(statements[-1])
        # column renames still apply to the script output (script columns named like source columns)
        out = []
        for r in rows:
            out.append({rt.renames.get(k, k): json_safe_for_sql(v) for k, v in r.items()})
        return out

    def _duckdb_transform(self, rt: TableRuntime, source_rows, dest_rows, operation: Operation):
        """Evaluate expression columns in one DuckDB query over the whole batch."""
        from backend.core.duckdb_processor import infer_duckdb_type
        staging = "applier_batch"
        all_cols: List[str] = []
        for r in source_rows:
            for k in r.keys():
                if k not in all_cols:
                    all_cols.append(k)
        types = {c: infer_duckdb_type(next((r[c] for r in source_rows if r.get(c) is not None), None)) for c in all_cols}
        types["__rownum"] = "BIGINT"
        self.duckdb.create_staging_table(staging, types)
        self.duckdb.load_data(staging, [dict(r, __rownum=i) for i, r in enumerate(source_rows)])
        select = ["__rownum"]
        names = []
        for cm in rt.expressions:
            select.append(f"({cm.transformation}) AS {self.duckdb.quote(cm.destination_column)}")
            names.append(cm.destination_column)
        autos = dict(rt.auto_on_insert)
        if operation == Operation.INSERT:
            autos.update(rt.auto_on_init)
        for col, expr in autos.items():
            select.append(f"({expr}) AS {self.duckdb.quote(col)}")
            names.append(col)
        rows = self.duckdb.execute_query(f"SELECT {', '.join(select)} FROM {staging} ORDER BY __rownum")
        for r in rows:
            idx = int(r["__rownum"])
            for n in names:
                dest_rows[idx][n] = json_safe_for_sql(r[n])
        return dest_rows

    # -- applying ----------------------------------------------------------
    def apply(self, events: Sequence[ChangeEvent], record_failures: bool = True) -> ApplyResult:
        """Apply events in order. ``record_failures=False`` is used for DLQ replays (no new DLQ rows)."""
        result = ApplyResult()
        result.record_failures = record_failures
        start = time.perf_counter()
        group: List[ChangeEvent] = []
        group_key: Optional[Tuple[str, Operation]] = None
        for ev in events:
            rt = self.runtime_for(ev)
            if rt is None:
                result.skipped += 1
                metrics.EVENTS_SKIPPED.labels(table=ev.table_key).inc()
                continue
            key = (rt.source_key, ev.operation)
            if group and key != group_key:
                self._apply_group(self._runtimes_by_key(group_key[0]), group_key[1], group, result)
                group = []
            group_key = key
            group.append(ev)
        if group:
            self._apply_group(self._runtimes_by_key(group_key[0]), group_key[1], group, result)
        result.duration_s = time.perf_counter() - start
        return result

    def runtime_for_key(self, source_key: str) -> TableRuntime:
        return self._runtimes_by_key(source_key)

    def _runtimes_by_key(self, source_key: str) -> TableRuntime:
        rt = self._runtimes.get(source_key)
        if rt is None:
            for k, v in self._runtimes.items():
                if k.lower() == source_key.lower():
                    return v
        return rt  # type: ignore[return-value]

    def _apply_group(self, rt: TableRuntime, op: Operation, events: List[ChangeEvent], result: ApplyResult) -> None:
        if (op == Operation.INSERT and not rt.sync_inserts) or (op == Operation.UPDATE and not rt.sync_updates) or \
                (op == Operation.DELETE and not rt.sync_deletes):
            result.skipped += len(events)
            metrics.EVENTS_SKIPPED.labels(table=rt.source_key).inc(len(events))
            return
        if op in (Operation.TRUNCATE, Operation.DDL):
            result.skipped += len(events)
            if self.audit:
                self.audit.record("warning", "applier", f"{op.value} on {rt.source_key} ignored (schema/truncate events are not replayed)",
                                  table=rt.source_key)
            return
        keys = self.resolve_keys(rt)
        try:
            with metrics.APPLY_LATENCY.labels(table=rt.dest_key).time():
                self._write(rt, op, events, keys)
            n = len(events)
            result.applied += n
            metrics.EVENTS_APPLIED.labels(table=rt.dest_key, op=op.value).inc(n)
            metrics.BATCH_SIZE.labels(table=rt.dest_key).observe(n)
            self._record_lag(rt, events[-1])
        except Exception as batch_exc:
            if len(events) == 1:
                self._fail(rt, events[0], batch_exc, result)
                return
            logger.warning("batch of %d %s on %s failed (%s); retrying row by row", len(events), op.value, rt.dest_key, batch_exc)
            for ev in events:
                try:
                    with metrics.APPLY_LATENCY.labels(table=rt.dest_key).time():
                        self._write(rt, op, [ev], keys)
                    result.applied += 1
                    metrics.EVENTS_APPLIED.labels(table=rt.dest_key, op=op.value).inc()
                except Exception as row_exc:
                    self._fail(rt, ev, row_exc, result)

    def _write(self, rt: TableRuntime, op: Operation, events: List[ChangeEvent], keys: List[str]) -> None:
        d = self.destination
        if op == Operation.DELETE:
            key_rows = []
            for ev in events:
                src = ev.before or ev.key or ev.after or {}
                dest_row = self.transform_rows(rt, [src], op)[0]
                key_rows.append({k: dest_row.get(k) for k in keys})
            d.delete_rows(rt.dest_schema, rt.dest_table, key_rows, keys)
            return
        rows = self.transform_rows(rt, [ev.data for ev in events], op)
        # all rows in a batch must share the same column set for executemany
        columns: List[str] = []
        for r in rows:
            for c in r:
                if c not in columns:
                    columns.append(c)
        rows = [{c: r.get(c) for c in columns} for r in rows]
        if any(k not in columns for k in keys):
            raise RuntimeError(f"key columns {keys} are not produced by mapping {rt.mapping.id} (columns: {columns})")
        if op == Operation.INSERT:
            if rt.insert_mode == "upsert":
                d.upsert_rows(rt.dest_schema, rt.dest_table, rows, keys)
            else:
                d.insert_rows(rt.dest_schema, rt.dest_table, rows)
        else:  # UPDATE -> upsert so that a missed insert is repaired instead of silently lost
            if len(columns) == len(keys):
                return
            d.upsert_rows(rt.dest_schema, rt.dest_table, rows, keys)

    def _fail(self, rt: TableRuntime, ev: ChangeEvent, exc: Exception, result: ApplyResult) -> None:
        msg = str(exc)
        sig = error_signature(msg)
        result.failures.append(ApplyFailure(ev, msg, sig, rt.dest_key))
        metrics.EVENTS_FAILED.labels(table=rt.dest_key, op=ev.operation.value).inc()
        logger.error("failed to apply %s on %s: %s", ev.operation.value, rt.dest_key, msg)
        if self.audit and result.record_failures:
            self.audit.record_failure(ev, rt.dest_key, msg, sig)

    @staticmethod
    def _record_lag(rt: TableRuntime, ev: ChangeEvent) -> None:
        if ev.source_ts:
            try:
                lag = (datetime.now(ev.source_ts.tzinfo) - ev.source_ts).total_seconds()
                metrics.END_TO_END_LAG.labels(table=rt.dest_key).set(max(lag, 0.0))
            except Exception:  # pragma: no cover
                pass


def _resolve_script(mapping: Mapping) -> Optional[str]:
    """Script content for ``use_duckdb_transformation`` (inline content wins over a library script name)."""
    if not mapping.use_duckdb_transformation:
        return None
    if mapping.duckdb_script_content:
        return mapping.duckdb_script_content
    if mapping.duckdb_script_name:
        try:
            from backend.core.duckdb_script_manager import duckdb_script_manager
            return duckdb_script_manager.get_script(mapping.duckdb_script_name)
        except Exception as exc:  # pragma: no cover
            logger.warning("cannot load DuckDB script %s: %s", mapping.duckdb_script_name, exc)
    return None


def json_safe_for_sql(value: Any) -> Any:
    """Values that DB-API drivers cannot bind (dicts/lists from Mongo, etc.) become JSON strings."""
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value, default=str)
    return value
