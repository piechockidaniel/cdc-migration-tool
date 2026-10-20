"""PostgreSQL connector - logical decoding through a replication slot.

Mechanism
---------
* A *logical replication slot* is created with the ``wal2json`` output plugin
  (preferred - JSON, filterable by table) or the always-available
  ``test_decoding`` plugin (text, parsed here).
* Changes are read with ``pg_logical_slot_peek_changes`` (non destructive) and
  the slot is advanced with ``pg_replication_slot_advance`` only after the
  pipeline has durably committed the position -> at-least-once, no loss.
* Watermark = ``pg_current_wal_lsn()`` (one cheap call). Optional push
  wake-up via ``LISTEN <channel>``; the customer decides whether a trigger or
  the application issues ``NOTIFY`` - the connector never installs triggers.
* Before-images for UPDATE/DELETE need ``REPLICA IDENTITY FULL`` (or a PK for
  key-only identity); ``check_prerequisites`` reports that per table.

The SQL-function based approach works with plain ``psycopg`` (no replication
protocol support needed) and through connection poolers such as PgBouncer in
session mode.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, ConnectorError, Operation, Position,
    PrerequisiteCheck, SourceConnector, TableRef, keyset_params, keyset_predicate,
)
from backend.connectors.watermark import AdaptiveInterval
from backend.core import metrics

logger = logging.getLogger(__name__)

POSITION_KIND = "pg_lsn"
_ACTION_MAP = {"I": Operation.INSERT, "U": Operation.UPDATE, "D": Operation.DELETE, "T": Operation.TRUNCATE}


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def int_to_lsn(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


class PostgresConnector(SourceConnector):
    engine = "postgresql"
    capabilities = frozenset({
        Capability.NATIVE_CDC, Capability.BEFORE_IMAGE, Capability.KEYSET_SNAPSHOT,
        Capability.TRANSACTION_BOUNDARIES, Capability.PUSH_EVENTS, Capability.CONSISTENT_SNAPSHOT,
    })

    def __init__(self, config: Dict[str, Any], connection_factory=None):
        super().__init__(config)
        self._conn = None
        self._connection_factory = connection_factory
        self.slot_name: str = config.get("slot_name") or f"cdcsync_{re.sub(r'[^a-z0-9_]', '_', self.name.lower())}"
        self.plugin: str = config.get("plugin", "wal2json")
        self.notify_channel: Optional[str] = config.get("notify_channel")
        self.batch_limit = int(config.get("batch_limit", 5000))

    # ------------------------------------------------------------------
    def dsn(self, redact: bool = False) -> str:
        c = self.config
        parts = [
            f"host={c.get('server', c.get('host', 'localhost'))}",
            f"port={c.get('port') or 5432}",
            f"dbname={c.get('database', '')}",
            f"user={c.get('username', '')}",
            f"password={'***' if redact else c.get('password', '')}",
            f"application_name={c.get('application_name', 'cdcsync')}",
            f"connect_timeout={int(c.get('login_timeout', 15))}",
        ]
        if c.get("sslmode"):
            parts.append(f"sslmode={c['sslmode']}")
        return " ".join(parts)

    def connect(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            try:
                if self._connection_factory is not None:
                    self._conn = self._connection_factory(self)
                else:
                    import psycopg
                    self._conn = psycopg.connect(self.dsn(), autocommit=True)
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                raise ConnectorError(f"PostgreSQL connect failed ({self.dsn(redact=True)}): {exc}") from exc

    def disconnect(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:  # pragma: no cover
                    pass
                self._conn = None

    def is_connected(self) -> bool:
        with self._lock:
            if self._conn is None:
                return False
            try:
                self._query("SELECT 1", kind="ping")
                return True
            except Exception:
                return False

    def _ensure(self):
        if self._conn is None:
            self.connect()
        return self._conn

    def _query(self, sql: str, params: Sequence[Any] = (), kind: str = "query") -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._ensure()
            with metrics.timed_query(self.name, kind):
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(params) if params else None)
                    if cur.description is None:
                        return []
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, r)) for r in cur.fetchall()]

    def server_version(self) -> str:
        rows = self._query("SELECT version() AS v", kind="meta")
        return rows[0]["v"] if rows else "unknown"

    def quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    # ------------------------------------------------------------------
    def list_tables(self) -> List[TableRef]:
        pub_filter = ""
        rows = self._query(
            """
            SELECT n.nspname AS schema_name, c.relname AS table_name, c.reltuples::bigint AS row_estimate,
                   pg_total_relation_size(c.oid) AS size_bytes, c.relreplident
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
            ORDER BY 1, 2
            """ + pub_filter, kind="meta")
        return [TableRef(r["schema_name"], r["table_name"], True, max(int(r["row_estimate"] or 0), 0), int(r["size_bytes"] or 0),
                         {"replica_identity": r["relreplident"]}) for r in rows]

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        rows = self._query(
            """
            SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS data_type, NOT a.attnotnull AS nullable,
                   a.attnum AS ordinal,
                   EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY(i.indkey)) AS is_pk
            FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """, (schema, table), kind="meta")
        return [ColumnRef(r["name"], r["data_type"], bool(r["nullable"]), bool(r["is_pk"]), int(r["ordinal"])) for r in rows]

    # ------------------------------------------------------------------
    def check_prerequisites(self, tables=None) -> List[PrerequisiteCheck]:
        checks: List[PrerequisiteCheck] = []

        def add(name, passed, detail="", fix="", severity="error"):
            checks.append(PrerequisiteCheck(name, bool(passed), detail, fix, severity))

        try:
            self.connect()
            add("connection", True, self.server_version())
        except Exception as exc:
            add("connection", False, str(exc), "Check host/port/credentials/pg_hba.conf")
            return checks
        try:
            settings = {r["name"]: r["setting"] for r in self._query(
                "SELECT name, setting FROM pg_settings WHERE name IN "
                "('wal_level','max_replication_slots','max_wal_senders','server_version_num','wal_sender_timeout')", kind="meta")}
            ver = int(settings.get("server_version_num", "0"))
            add("server_version", ver >= 100000, settings.get("server_version_num"), "PostgreSQL 10+ required for logical decoding via SQL functions")
            add("wal_level_logical", settings.get("wal_level") == "logical", f"wal_level={settings.get('wal_level')}",
                "ALTER SYSTEM SET wal_level = 'logical'; then restart the server")
            slots = int(settings.get("max_replication_slots", "0"))
            used = self._query("SELECT COUNT(*) AS n FROM pg_replication_slots", kind="meta")[0]["n"]
            add("replication_slots_available", slots - int(used) > 0 or self._slot_exists(),
                f"max_replication_slots={slots}, in use={used}", "Increase max_replication_slots (restart required)")
        except Exception as exc:
            add("server_settings", False, str(exc))
        try:
            r = self._query("SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user", kind="meta")[0]
            add("role_replication", bool(r["rolsuper"] or r["rolreplication"]),
                f"superuser={r['rolsuper']}, replication={r['rolreplication']}",
                "ALTER ROLE <user> REPLICATION; (needed to create/advance slots)")
        except Exception as exc:
            add("role_replication", False, str(exc))
        add("plugin", True, f"output plugin '{self.plugin}'",
            "wal2json must be installed on the server (package postgresql-<ver>-wal2json); test_decoding is built in", "info")
        if self._slot_exists():
            try:
                r = self._query("SELECT active, restart_lsn, confirmed_flush_lsn, "
                                "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes, wal_status "
                                "FROM pg_replication_slots WHERE slot_name = %s", (self.slot_name,), kind="meta")[0]
                lag = int(r["lag_bytes"] or 0)
                add("slot_lag", lag < 1024 ** 3, f"slot '{self.slot_name}' retains {lag/1024**2:.1f} MB of WAL (status={r.get('wal_status')})",
                    "An inactive slot retains WAL forever and fills the disk - keep the pipeline running or drop the slot", "warning")
            except Exception:
                pass
        else:
            add("slot_exists", False, f"slot '{self.slot_name}' not created yet",
                "enable_capture_database() creates it", "warning")
        for schema, table in tables or []:
            try:
                cols = self.get_columns(schema, table)
                if not cols:
                    add(f"table_exists:{schema}.{table}", False, "table not found")
                    continue
                pk = [c.name for c in cols if c.is_primary_key]
                ident = self._query("SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                                    "WHERE n.nspname=%s AND c.relname=%s", (schema, table), kind="meta")[0]["relreplident"]
                add(f"primary_key:{schema}.{table}", bool(pk), f"pk={pk}",
                    "Without a PK set REPLICA IDENTITY FULL and define key columns in the mapping")
                add(f"replica_identity:{schema}.{table}", ident in ("f", "d") and (bool(pk) or ident == "f"),
                    f"replica identity={ident} (d=default/PK, f=full, n=nothing, i=index)",
                    f"ALTER TABLE {self.qualified(schema, table)} REPLICA IDENTITY FULL; (required for UPDATE/DELETE old values)",
                    "warning" if ident == "d" else "error")
            except Exception as exc:
                add(f"table:{schema}.{table}", False, str(exc))
        return checks

    def _slot_exists(self) -> bool:
        rows = self._query("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (self.slot_name,), kind="meta")
        return bool(rows)

    # ------------------------------------------------------------------
    def capture_status(self) -> Dict[str, Any]:
        slot = self._query("SELECT slot_name, plugin, active, restart_lsn::text AS restart_lsn, confirmed_flush_lsn::text AS confirmed_flush_lsn, "
                           "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes "
                           "FROM pg_replication_slots WHERE slot_name = %s", (self.slot_name,), kind="meta")
        wm = self.current_position()
        return {"database": self.config.get("database"), "cdc_enabled": bool(slot), "slot": slot[0] if slot else None,
                "current_wal_lsn": wm.value if wm else None}

    def enable_capture_database(self) -> Tuple[bool, str]:
        try:
            if self._slot_exists():
                return True, f"slot '{self.slot_name}' already exists"
            self._query("SELECT pg_create_logical_replication_slot(%s, %s)", (self.slot_name, self.plugin), kind="ddl")
            return True, f"created logical replication slot '{self.slot_name}' with plugin '{self.plugin}'"
        except Exception as exc:
            return False, f"cannot create slot: {exc}"

    def drop_slot(self) -> Tuple[bool, str]:
        try:
            if self._slot_exists():
                self._query("SELECT pg_drop_replication_slot(%s)", (self.slot_name,), kind="ddl")
            return True, "slot dropped"
        except Exception as exc:
            return False, str(exc)

    def enable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        """Tables are selected client-side (wal2json add-tables); optionally set REPLICA IDENTITY FULL."""
        try:
            if options.get("replica_identity_full", False):
                self._query(f"ALTER TABLE {self.qualified(schema, table)} REPLICA IDENTITY FULL", kind="ddl")
                return True, f"REPLICA IDENTITY FULL set on {schema}.{table}"
            return True, f"{schema}.{table} will be decoded from slot '{self.slot_name}' (no server change needed)"
        except Exception as exc:
            return False, str(exc)

    def disable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        return True, f"{schema}.{table} removed from the decoded table list (client side)"

    # ------------------------------------------------------------------
    def current_position(self) -> Optional[Position]:
        rows = self._query("SELECT pg_current_wal_lsn()::text AS lsn", kind="watermark")
        return Position(POSITION_KIND, rows[0]["lsn"]) if rows else None

    def confirm_position(self, position: Position) -> None:
        """Advance the slot -> WAL before ``position`` may be recycled."""
        self._query("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (self.slot_name, position.value), kind="slot_advance")

    @staticmethod
    def _table_filter(tables: Sequence[Tuple[str, str]]) -> Optional[str]:
        if not tables:
            return None
        return ",".join(f"{s}.{t}" for s, t in tables)

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        wanted = {(s, t) for s, t in tables}
        options: List[Any] = []
        if self.plugin == "wal2json":
            options = ["format-version", "2", "include-timestamp", "1", "include-xids", "1", "include-types", "0",
                       "include-pk", "1"]
            flt = self._table_filter(tables)
            if flt:
                options += ["add-tables", flt]
        elif self.plugin == "test_decoding":
            options = ["include-timestamp", "on", "include-xids", "on"]
        placeholders = ", ".join(["%s"] * len(options))
        sql = f"SELECT lsn::text AS lsn, xid, data FROM pg_logical_slot_peek_changes(%s, NULL, %s{', ' + placeholders if options else ''})"
        rows = self._query(sql, [self.slot_name, limit * 3] + options, kind="cdc_read")
        events: List[ChangeEvent] = []
        last_lsn: Optional[str] = None
        from_int = lsn_to_int(from_position.value) if from_position and from_position.value else -1
        tx_ts: Optional[datetime] = None
        for row in rows:
            lsn = row["lsn"]
            parsed = self._parse(row["data"])
            if parsed is None:
                continue
            if parsed.get("_tx_ts"):
                tx_ts = parsed["_tx_ts"]   # BEGIN/COMMIT records carry the transaction timestamp
                if lsn_to_int(lsn) > from_int:
                    last_lsn = lsn
                continue
            if lsn_to_int(lsn) <= from_int:
                continue  # already committed (peek returns from confirmed_flush)
            last_lsn = lsn
            if (parsed["schema"], parsed["table"]) not in wanted and wanted:
                continue
            op = parsed["op"]
            ev = ChangeEvent(
                source=self.name, schema=parsed["schema"], table=parsed["table"], operation=op,
                position=Position(POSITION_KIND, lsn), key=parsed.get("key") or {},
                after=parsed.get("after"), before=parsed.get("before"),
                source_ts=parsed.get("ts") or tx_ts, tx_id=str(row.get("xid")), event_id=str(uuid.uuid4()),
            )
            events.append(ev)
            metrics.EVENTS_READ.labels(source=self.name, table=ev.table_key, op=op.value).inc()
            if len(events) >= limit:
                break
        metrics.POLL_COUNT.labels(source=self.name, outcome="ok").inc()
        commit = Position(POSITION_KIND, last_lsn) if last_lsn else (to_position or from_position)
        return events, commit

    # -- decoding ----------------------------------------------------------
    def _parse(self, data: str) -> Optional[Dict[str, Any]]:
        if self.plugin == "wal2json":
            return self._parse_wal2json(data)
        return self._parse_test_decoding(data)

    @staticmethod
    def _parse_wal2json(data: str) -> Optional[Dict[str, Any]]:
        msg = json.loads(data)
        action = msg.get("action")
        if action in ("B", "C"):
            ts = msg.get("timestamp")
            return {"_tx_ts": _parse_pg_ts(ts)} if ts else None
        if action not in _ACTION_MAP:
            return None
        op = _ACTION_MAP[action]
        after = {c["name"]: c.get("value") for c in msg.get("columns", [])} if msg.get("columns") else None
        before = {c["name"]: c.get("value") for c in msg.get("identity", [])} if msg.get("identity") else None
        pk_names = [p["name"] for p in msg.get("pk", [])] if msg.get("pk") else []
        src = after if after is not None else (before or {})
        key = {k: src.get(k) for k in pk_names if k in src} if pk_names else dict(before or {})
        return {"schema": msg.get("schema"), "table": msg.get("table"), "op": op, "after": after, "before": before,
                "key": key, "ts": _parse_pg_ts(msg.get("timestamp"))}

    _TD_ROW = re.compile(r"^table (?P<schema>[^.]+)\.(?P<table>\S+): (?P<op>INSERT|UPDATE|DELETE|TRUNCATE):(?P<rest>.*)$", re.S)
    _TD_COL = re.compile(r"(?P<name>\S+?)\[(?P<type>[^\]]+)\]:(?P<val>'(?:[^']|'')*'|\S+)")

    @classmethod
    def _parse_test_decoding(cls, data: str) -> Optional[Dict[str, Any]]:
        if data.startswith("BEGIN") or data.startswith("COMMIT"):
            m = re.search(r"\(at (?P<ts>[^)]+)\)", data)
            return {"_tx_ts": _parse_pg_ts(m.group("ts"))} if m else None
        m = cls._TD_ROW.match(data)
        if not m:
            return None
        schema, table, op_name, rest = m.group("schema"), m.group("table"), m.group("op"), m.group("rest").strip()
        op = Operation(op_name)

        def parse_cols(segment: str) -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            for cm in cls._TD_COL.finditer(segment):
                raw = cm.group("val")
                if raw.startswith("'"):
                    val: Any = raw[1:-1].replace("''", "'")
                elif raw == "null":
                    val = None
                elif cm.group("type") in ("integer", "bigint", "smallint"):
                    val = int(raw)
                elif cm.group("type") in ("numeric", "double precision", "real"):
                    try:
                        val = float(raw)
                    except ValueError:
                        val = raw
                elif cm.group("type") == "boolean":
                    val = raw == "true"
                else:
                    val = raw
                out[cm.group("name")] = val
            return out

        before = after = None
        if "old-key:" in rest:
            old_part, new_part = rest.split("new-tuple:", 1) if "new-tuple:" in rest else (rest, "")
            before = parse_cols(old_part.replace("old-key:", ""))
            after = parse_cols(new_part) if new_part else None
        else:
            cols = parse_cols(rest)
            if op == Operation.DELETE:
                before = cols
            else:
                after = cols
        key = dict(before) if before else {}
        return {"schema": schema, "table": table.strip(':'), "op": op, "after": after, "before": before, "key": key, "ts": None}

    # ------------------------------------------------------------------
    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        interval = AdaptiveInterval(float(self.config.get("poll_min_seconds", 0.25)), float(self.config.get("poll_max_seconds", 30.0)))
        position = from_position
        last_wm: Optional[str] = None
        listener = None
        if self.notify_channel:
            listener = self._open_listener()
        while not stop_event.is_set():
            try:
                if listener is not None:
                    self._wait_notify(listener, interval.current)
                wm = self.current_position()
                moved = wm is not None and wm.value != last_wm
                metrics.WATERMARK_CHECKS.labels(source=self.name, outcome="moved" if moved else "idle").inc()
                if not moved:
                    if listener is None:
                        stop_event.wait(interval.idle())
                    continue
                last_wm = wm.value
                events, commit_pos = self.read_changes(tables, position, wm, self.batch_limit)
                if not events:
                    if listener is None:
                        stop_event.wait(interval.idle())
                    continue
                interval.busy()
                for ev in events:
                    yield ev
                position = commit_pos
            except ConnectorError:
                raise
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                logger.error("PostgreSQL stream error: %s", exc)
                stop_event.wait(interval.error())

    def _open_listener(self):
        import psycopg
        conn = psycopg.connect(self.dsn(), autocommit=True)
        conn.execute(f"LISTEN {self.quote(self.notify_channel)}")
        return conn

    @staticmethod
    def _wait_notify(conn, timeout: float) -> bool:
        gen = conn.notifies(timeout=timeout)
        for _ in gen:
            return True
        return False

    def retention_status(self, _positions: Dict[str, Optional[Position]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"slot_lag_bytes": None, "slot_active": None, "capture_errors": 0, "position_before_min": False,
                               "retention_margin_minutes": None}
        try:
            rows = self._query("SELECT active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag, wal_status "
                               "FROM pg_replication_slots WHERE slot_name = %s", (self.slot_name,), kind="meta")
            if rows:
                out["slot_lag_bytes"] = int(rows[0]["lag"] or 0)
                out["slot_active"] = bool(rows[0]["active"])
                if rows[0].get("wal_status") in ("lost", "unreserved"):
                    out["position_before_min"] = rows[0]["wal_status"] == "lost"
            else:
                out["capture_errors"] = 1  # slot vanished
        except Exception:
            pass
        return out

    # ------------------------------------------------------------------
    def count_rows(self, schema, table, approximate=True):
        if approximate:
            rows = self._query("SELECT c.reltuples::bigint AS n FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                               "WHERE n.nspname=%s AND c.relname=%s", (schema, table), kind="meta")
            return max(int(rows[0]["n"]), 0) if rows else None
        rows = self._query(f"SELECT COUNT(*) AS n FROM {self.qualified(schema, table)}", kind="count")
        return int(rows[0]["n"]) if rows else None

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        cols = ", ".join(self.quote(c) for c in columns)
        order = ", ".join(self.quote(c) for c in key_columns)
        conditions, params = [], []
        if after_key is not None:
            pred, _ = keyset_predicate(key_columns, self.quote, placeholder="%s", row_values=True)
            conditions.append(pred)
            params.extend(keyset_params(after_key, row_values=True))
        if where:
            conditions.append(f"({where})")
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT {cols} FROM {self.qualified(schema, table)}{where_sql} ORDER BY {order} LIMIT {int(chunk_size)}"
        return self._query(sql, params, kind="snapshot_chunk")

    def key_bounds(self, schema, table, key_columns):
        k = self.quote(key_columns[0])
        rows = self._query(f"SELECT MIN({k}) AS lo, MAX({k}) AS hi FROM {self.qualified(schema, table)}", kind="meta")
        return ((rows[0]["lo"],), (rows[0]["hi"],)) if rows else (None, None)


def _parse_pg_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        v = value.strip()
        # "2024-01-01 10:00:00.123456+00" -> fromisoformat needs +00:00
        v = re.sub(r"([+-]\d{2})$", r"\1:00", v)
        return datetime.fromisoformat(v)
    except ValueError:
        return None
