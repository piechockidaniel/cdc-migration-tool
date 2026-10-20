"""MariaDB / MySQL connector - row-based binary log replication stream.

The connector registers as a replica (``python-mysql-replication``) and
receives row events *pushed* by the server exactly like a real replica does -
no polling. Positions are binlog coordinates ``{file, pos}`` (GTID is recorded
as extra information when available).

Prerequisites validated: ``log_bin``, ``binlog_format=ROW``,
``binlog_row_image=FULL``, retention (``binlog_expire_logs_seconds`` /
``expire_logs_days``), replication privileges, unique ``server_id``,
``binlog_row_metadata=FULL`` (MySQL 8) and a PK per table.
"""
from __future__ import annotations

import logging
import queue
import re
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, ConnectorError, Operation, Position,
    PrerequisiteCheck, SourceConnector, TableRef, keyset_params, keyset_predicate,
)
from backend.core import metrics

logger = logging.getLogger(__name__)

POSITION_KIND = "binlog"


def binlog_key(value: Dict[str, Any]) -> Tuple[int, int]:
    """Sortable key for a ``{file, pos}`` position."""
    m = re.search(r"\.(\d+)$", str(value.get("file", "")))
    return int(m.group(1)) if m else 0, int(value.get("pos", 0))


class MariaDBConnector(SourceConnector):
    engine = "mariadb"
    capabilities = frozenset({
        Capability.NATIVE_CDC, Capability.BEFORE_IMAGE, Capability.KEYSET_SNAPSHOT,
        Capability.PUSH_EVENTS, Capability.TRANSACTION_BOUNDARIES,
    })

    def __init__(self, config: Dict[str, Any], connection_factory=None, stream_factory=None):
        super().__init__(config)
        self._conn = None
        self._connection_factory = connection_factory
        self._stream_factory = stream_factory
        self.server_id = int(config.get("server_id", 4379))
        self.batch_limit = int(config.get("batch_limit", 5000))

    # ------------------------------------------------------------------
    def _conn_params(self) -> Dict[str, Any]:
        c = self.config
        return {
            "host": c.get("server", c.get("host", "localhost")),
            "port": int(c.get("port") or 3306),
            "user": c.get("username", ""),
            "passwd": c.get("password", ""),
        }

    def connect(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            try:
                if self._connection_factory is not None:
                    self._conn = self._connection_factory(self)
                else:
                    import pymysql
                    p = self._conn_params()
                    self._conn = pymysql.connect(
                        host=p["host"], port=p["port"], user=p["user"], password=p["passwd"],
                        database=self.config.get("database"), autocommit=True,
                        connect_timeout=int(self.config.get("login_timeout", 15)),
                        cursorclass=pymysql.cursors.DictCursor,
                    )
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                raise ConnectorError(f"MariaDB connect failed: {exc}") from exc

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

    def _query(self, sql: str, params: Sequence[Any] = (), kind: str = "query") -> List[Dict[str, Any]]:
        with self._lock:
            if self._conn is None:
                self.connect()
            with metrics.timed_query(self.name, kind):
                cur = self._conn.cursor()
                try:
                    cur.execute(sql, tuple(params) if params else None)
                    if cur.description is None:
                        return []
                    rows = cur.fetchall()
                    if rows and not isinstance(rows[0], dict):
                        cols = [d[0] for d in cur.description]
                        rows = [dict(zip(cols, r)) for r in rows]
                    return list(rows)
                finally:
                    cur.close()

    def server_version(self) -> str:
        rows = self._query("SELECT VERSION() AS v", kind="meta")
        return rows[0]["v"] if rows else "unknown"

    def quote(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    # ------------------------------------------------------------------
    def list_tables(self) -> List[TableRef]:
        rows = self._query(
            "SELECT table_schema, table_name, table_rows, data_length + index_length AS size_bytes FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') "
            + ("AND table_schema = %s " if self.config.get("database") else "") + "ORDER BY 1, 2",
            (self.config["database"],) if self.config.get("database") else (), kind="meta")
        return [TableRef(r["table_schema"], r["table_name"], True, int(r["table_rows"] or 0), int(r["size_bytes"] or 0)) for r in rows]

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        rows = self._query(
            "SELECT column_name, column_type, is_nullable, ordinal_position, column_key, character_maximum_length "
            "FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table), kind="meta")
        return [ColumnRef(r["column_name"], r["column_type"], r["is_nullable"] == "YES", r["column_key"] == "PRI",
                          int(r["ordinal_position"]), r.get("character_maximum_length")) for r in rows]

    # ------------------------------------------------------------------
    def _variables(self, names: Sequence[str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for n in names:
            try:
                rows = self._query(f"SHOW GLOBAL VARIABLES LIKE '{n}'", kind="meta")
                for r in rows:
                    out[str(r.get("Variable_name", n))] = str(r.get("Value", ""))
            except Exception:
                pass
        return out

    def check_prerequisites(self, tables=None) -> List[PrerequisiteCheck]:
        checks: List[PrerequisiteCheck] = []

        def add(name, passed, detail="", fix="", severity="error"):
            checks.append(PrerequisiteCheck(name, bool(passed), detail, fix, severity))

        try:
            self.connect()
            version = self.server_version()
            add("connection", True, version)
        except Exception as exc:
            add("connection", False, str(exc))
            return checks
        v = self._variables(["log_bin", "binlog_format", "binlog_row_image", "binlog_row_metadata", "server_id",
                             "binlog_expire_logs_seconds", "expire_logs_days", "gtid_mode", "gtid_strict_mode", "log_slave_updates"])
        add("log_bin", v.get("log_bin", "").upper() in ("ON", "1"), f"log_bin={v.get('log_bin')}",
            "Set log_bin in my.cnf (restart required)")
        add("binlog_format_row", v.get("binlog_format", "").upper() == "ROW", f"binlog_format={v.get('binlog_format')}",
            "SET GLOBAL binlog_format = 'ROW' (and in my.cnf)")
        add("binlog_row_image_full", v.get("binlog_row_image", "FULL").upper() == "FULL", f"binlog_row_image={v.get('binlog_row_image')}",
            "SET GLOBAL binlog_row_image = 'FULL' (needed for before images)", "warning")
        if "binlog_row_metadata" in v:
            add("binlog_row_metadata_full", v["binlog_row_metadata"].upper() == "FULL", f"binlog_row_metadata={v['binlog_row_metadata']}",
                "SET GLOBAL binlog_row_metadata = 'FULL' (column names in binlog, MySQL 8.0.1+)", "warning")
        add("server_id_unique", str(v.get("server_id", "0")) not in ("0", str(self.server_id)),
            f"server server_id={v.get('server_id')}, connector server_id={self.server_id}",
            "The connector's server_id must differ from the server and every replica")
        retention_s = int(v.get("binlog_expire_logs_seconds") or 0) or int(v.get("expire_logs_days") or 0) * 86400
        add("binlog_retention", retention_s == 0 or retention_s >= 86400,
            f"retention={retention_s}s" if retention_s else "retention not set (logs kept until purged manually)",
            "Retention must exceed the longest outage of the synchroniser", "warning")
        add("gtid", True, f"gtid_mode={v.get('gtid_mode')} gtid_strict_mode={v.get('gtid_strict_mode')}",
            "GTID positions survive fail-over; enable when possible", "info")
        try:
            grants = " ".join(str(list(r.values())[0]) for r in self._query("SHOW GRANTS", kind="meta"))
            ok = any(k in grants.upper() for k in ("REPLICATION SLAVE", "ALL PRIVILEGES", "BINLOG MONITOR", "REPLICA"))
            add("replication_privileges", ok, grants[:300],
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO <user>")
        except Exception as exc:
            add("replication_privileges", False, str(exc), severity="warning")
        try:
            self.current_position()
            add("binlog_status_readable", True, "SHOW MASTER/BINARY LOG STATUS ok")
        except Exception as exc:
            add("binlog_status_readable", False, str(exc), "GRANT REPLICATION CLIENT / BINLOG MONITOR")
        for schema, table in tables or []:
            try:
                cols = self.get_columns(schema, table)
                if not cols:
                    add(f"table_exists:{schema}.{table}", False, "table not found")
                    continue
                pk = [c.name for c in cols if c.is_primary_key]
                add(f"primary_key:{schema}.{table}", bool(pk), f"pk={pk}",
                    "Without a PK define key columns in the mapping (row images are full, so it is possible but slower)")
            except Exception as exc:
                add(f"table:{schema}.{table}", False, str(exc))
        return checks

    def capture_status(self) -> Dict[str, Any]:
        pos = self.current_position()
        v = self._variables(["log_bin", "binlog_format", "binlog_row_image"])
        return {"database": self.config.get("database"), "cdc_enabled": v.get("log_bin", "").upper() in ("ON", "1") and v.get("binlog_format", "").upper() == "ROW",
                "variables": v, "position": pos.to_json() if pos else None}

    def enable_capture_database(self) -> Tuple[bool, str]:
        v = self._variables(["log_bin", "binlog_format", "binlog_row_image"])
        if v.get("log_bin", "").upper() not in ("ON", "1"):
            return False, "log_bin is OFF - enable it in my.cnf and restart (cannot be changed at runtime)"
        msgs = []
        try:
            if v.get("binlog_format", "").upper() != "ROW":
                self._query("SET GLOBAL binlog_format = 'ROW'", kind="ddl")
                msgs.append("binlog_format=ROW set globally (persist it in my.cnf)")
            if v.get("binlog_row_image", "FULL").upper() != "FULL":
                self._query("SET GLOBAL binlog_row_image = 'FULL'", kind="ddl")
                msgs.append("binlog_row_image=FULL set globally")
        except Exception as exc:
            return False, f"cannot change binlog settings (needs SUPER): {exc}"
        return True, "; ".join(msgs) or "binary log already configured for row-based capture"

    def enable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        return True, f"{schema}.{table}: row events are captured for every table once the binlog is ROW based"

    def disable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        return True, f"{schema}.{table} removed from the filter (client side)"

    # ------------------------------------------------------------------
    def current_position(self) -> Optional[Position]:
        rows: List[Dict[str, Any]] = []
        for stmt in ("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"):
            try:
                rows = self._query(stmt, kind="watermark")
                break
            except Exception:
                continue
        if not rows:
            return None
        r = rows[0]
        gtid = r.get("Executed_Gtid_Set") or r.get("Gtid_executed")
        return Position(POSITION_KIND, {"file": r.get("File"), "pos": int(r.get("Position", 0))}, {"gtid": gtid} if gtid else {})

    def _open_stream(self, tables, from_position, blocking: bool):
        if self._stream_factory is not None:
            return self._stream_factory(self, tables, from_position, blocking)
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
        kwargs: Dict[str, Any] = dict(
            connection_settings=self._conn_params(), server_id=self.server_id, blocking=blocking, resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            only_schemas=sorted({s for s, _ in tables}) or None, only_tables=sorted({t for _, t in tables}) or None,
        )
        if from_position and from_position.value:
            kwargs["log_file"] = from_position.value["file"]
            kwargs["log_pos"] = int(from_position.value["pos"])
        return BinLogStreamReader(**kwargs)

    @staticmethod
    def _to_events(connector: "MariaDBConnector", event, stream, wanted: set) -> List[ChangeEvent]:
        from_cls = type(event).__name__
        schema, table = event.schema, event.table
        if wanted and (schema, table) not in wanted:
            return []
        ts = datetime.fromtimestamp(event.timestamp) if getattr(event, "timestamp", None) else None
        pos = Position(POSITION_KIND, {"file": stream.log_file, "pos": int(stream.log_pos)})
        pk = getattr(event, "primary_key", None)
        pk_cols = list(pk) if isinstance(pk, (list, tuple)) else ([pk] if pk else [])
        out = []
        for row in event.rows:
            if from_cls == "WriteRowsEvent":
                op, before, after = Operation.INSERT, None, dict(row["values"])
            elif from_cls == "UpdateRowsEvent":
                op, before, after = Operation.UPDATE, dict(row["before_values"]), dict(row["after_values"])
            elif from_cls == "DeleteRowsEvent":
                op, before, after = Operation.DELETE, dict(row["values"]), None
            else:
                continue
            src = after if after is not None else before
            key = {k: src.get(k) for k in pk_cols} if pk_cols else {}
            ev = ChangeEvent(source=connector.name, schema=schema, table=table, operation=op, position=pos, key=key,
                             after=after, before=before, source_ts=ts, event_id=str(uuid.uuid4()))
            metrics.EVENTS_READ.labels(source=connector.name, table=ev.table_key, op=op.value).inc()
            out.append(ev)
        return out

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        wanted = {(s, t) for s, t in tables}
        stream = self._open_stream(tables, from_position, blocking=False)
        events: List[ChangeEvent] = []
        last_pos = from_position
        try:
            for binlog_event in stream:
                for ev in self._to_events(self, binlog_event, stream, wanted):
                    events.append(ev)
                    last_pos = ev.position
                if len(events) >= limit:
                    break
            if len(events) < limit and stream.log_file:
                last_pos = Position(POSITION_KIND, {"file": stream.log_file, "pos": int(stream.log_pos)})
        finally:
            try:
                stream.close()
            except Exception:  # pragma: no cover
                pass
        metrics.POLL_COUNT.labels(source=self.name, outcome="ok").inc()
        return events, last_pos

    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        """Blocking replica stream in a helper thread; the server pushes events."""
        wanted = {(s, t) for s, t in tables}
        q: "queue.Queue[Any]" = queue.Queue(maxsize=self.batch_limit)
        stream = self._open_stream(tables, from_position, blocking=True)

        def reader():
            try:
                for binlog_event in stream:
                    if stop_event.is_set():
                        break
                    for ev in self._to_events(self, binlog_event, stream, wanted):
                        q.put(ev)
            except Exception as exc:
                if not stop_event.is_set():
                    q.put(exc)
            finally:
                q.put(None)

        t = threading.Thread(target=reader, name=f"binlog-{self.name}", daemon=True)
        t.start()
        try:
            while not stop_event.is_set():
                try:
                    item = q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:
                    break
                if isinstance(item, Exception):
                    metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                    raise ConnectorError(f"binlog stream failed: {item}") from item
                yield item
        finally:
            stop_event.set()
            try:
                stream.close()
            except Exception:  # pragma: no cover
                pass

    def retention_status(self, positions: Dict[str, Optional[Position]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"position_before_min": False, "capture_errors": 0, "retention_margin_minutes": None}
        pos = positions.get("__source__")
        try:
            logs = self._query("SHOW BINARY LOGS", kind="meta")
            names = [r.get("Log_name") for r in logs]
            if pos and pos.value and names and pos.value.get("file") not in names:
                out["position_before_min"] = binlog_key(pos.value) < binlog_key({"file": names[0], "pos": 0})
            v = self._variables(["binlog_expire_logs_seconds", "expire_logs_days"])
            retention_s = int(v.get("binlog_expire_logs_seconds") or 0) or int(v.get("expire_logs_days") or 0) * 86400
            if retention_s:
                out["retention_minutes"] = retention_s / 60.0
        except Exception:
            pass
        return out

    # ------------------------------------------------------------------
    def count_rows(self, schema, table, approximate=True):
        if approximate:
            rows = self._query("SELECT table_rows AS n FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                               (schema, table), kind="meta")
            return int(rows[0]["n"] or 0) if rows else None
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
