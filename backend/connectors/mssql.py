"""Microsoft SQL Server connector (Change Data Capture).

Highlights compared with the original ``cdc_monitor``/``cdc_operations``:

* **Correct LSN semantics** - reads start at ``fn_cdc_increment_lsn(last)``,
  positions carry ``(start_lsn, seqval)`` so a batch can be cut at any row and
  resumed without duplicates.
* **Cheap watermark** - ``current_position`` is a single
  ``sys.fn_cdc_get_max_lsn()`` call for the *whole database*; per-table work
  only happens when the watermark moved and the table actually has rows
  (``changed_capture_instances`` probes all capture tables in ONE round trip).
* **Push mode** - optional Service Broker queue (``WAITFOR (RECEIVE ...)``)
  so the application blocks server-side instead of polling over the network.
  See ``install_service_broker_sql()`` for the (small) T-SQL footprint.
* **Prerequisite validation** - edition, Agent status, permissions, capture /
  cleanup jobs, retention vs. committed position (the classic silent data
  loss), PK presence, RCSI for snapshots, log-scan health.
"""
from __future__ import annotations

import logging
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

POSITION_KIND = "mssql_lsn"
_OP_MAP = {1: Operation.DELETE, 2: Operation.INSERT, 3: Operation.UPDATE, 4: Operation.UPDATE}


def lsn_to_hex(lsn: Optional[bytes]) -> Optional[str]:
    return lsn.hex() if lsn else None


def hex_to_lsn(value: Optional[str]) -> Optional[bytes]:
    return bytes.fromhex(value) if value else None


def capture_instance_name(schema: str, table: str, override: Optional[str] = None) -> str:
    return override or f"{schema}_{table}"


class MSSQLConnector(SourceConnector):
    engine = "mssql"
    capabilities = frozenset({
        Capability.NATIVE_CDC, Capability.BEFORE_IMAGE, Capability.KEYSET_SNAPSHOT,
        Capability.TRANSACTION_BOUNDARIES, Capability.CONSISTENT_SNAPSHOT, Capability.PUSH_EVENTS,
    })

    def __init__(self, config: Dict[str, Any], connection_factory=None):
        super().__init__(config)
        self._conn = None
        self._connection_factory = connection_factory  # injectable for tests
        self.capture_instances: Dict[str, str] = dict(config.get("capture_instances") or {})
        self.row_filter = config.get("row_filter", "all")  # 'all' | 'all update old'
        self.query_timeout = int(config.get("query_timeout", 60))
        self.service_broker_queue = config.get("service_broker_queue")  # e.g. "dbo.cdcsync_notify_queue"
        self.snapshot_hint = config.get("snapshot_hint", "")  # e.g. "WITH (NOLOCK)" (not recommended) or ""

    # ------------------------------------------------------------------
    # connection handling
    # ------------------------------------------------------------------
    def connection_string(self, redact: bool = False) -> str:
        c = self.config
        server = c.get("server", "localhost")
        if c.get("port"):
            server = f"{server},{c['port']}"
        parts = [
            f"DRIVER={{{c.get('driver', 'ODBC Driver 18 for SQL Server')}}}",
            f"SERVER={server}",
            f"DATABASE={c.get('database', '')}",
            f"Encrypt={'yes' if c.get('encrypt', True) else 'no'}",
            f"TrustServerCertificate={'yes' if c.get('trust_server_certificate', True) else 'no'}",
            f"APP={c.get('application_name', 'cdcsync')}",
            f"Connection Timeout={int(c.get('login_timeout', 15))}",
        ]
        if c.get("use_windows_auth", False):
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={c.get('username', '')}")
            parts.append("PWD=***" if redact else f"PWD={c.get('password', '')}")
        if c.get("read_only_intent"):
            parts.append("ApplicationIntent=ReadOnly")
        return ";".join(parts)

    def connect(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            try:
                if self._connection_factory is not None:
                    self._conn = self._connection_factory(self)
                else:
                    import pyodbc
                    self._conn = pyodbc.connect(self.connection_string(), autocommit=False, timeout=self.query_timeout)
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                raise ConnectorError(f"MSSQL connect failed ({self.connection_string(redact=True)}): {exc}") from exc
            logger.info("Connected to MSSQL %s/%s", self.config.get("server"), self.config.get("database"))

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
                self._query("SELECT 1 AS ok", kind="ping")
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
                cur = conn.cursor()
                try:
                    cur.execute(sql, tuple(params)) if params else cur.execute(sql)
                    if cur.description is None:
                        conn.commit()
                        return []
                    cols = [d[0] for d in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                    conn.commit()
                    return rows
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:  # pragma: no cover
                        pass
                    raise
                finally:
                    cur.close()

    def _execute(self, sql: str, params: Sequence[Any] = (), kind: str = "exec") -> int:
        with self._lock:
            conn = self._ensure()
            with metrics.timed_query(self.name, kind):
                cur = conn.cursor()
                try:
                    cur.execute(sql, tuple(params)) if params else cur.execute(sql)
                    count = cur.rowcount
                    conn.commit()
                    return count
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:  # pragma: no cover
                        pass
                    raise
                finally:
                    cur.close()

    def server_version(self) -> str:
        rows = self._query("SELECT @@VERSION AS v, SERVERPROPERTY('Edition') AS edition, "
                           "SERVERPROPERTY('ProductVersion') AS pv", kind="meta")
        if not rows:
            return "unknown"
        return f"{rows[0]['pv']} ({rows[0]['edition']})"

    def quote(self, identifier: str) -> str:
        return "[" + identifier.replace("]", "]]") + "]"

    # ------------------------------------------------------------------
    # metadata
    # ------------------------------------------------------------------
    def list_tables(self) -> List[TableRef]:
        rows = self._query(
            """
            SELECT s.name AS schema_name, t.name AS table_name, t.is_tracked_by_cdc,
                   SUM(CASE WHEN ps.index_id IN (0,1) THEN ps.row_count ELSE 0 END) AS row_estimate,
                   SUM(ps.used_page_count) * 8 * 1024 AS size_bytes
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.dm_db_partition_stats ps ON ps.object_id = t.object_id
            WHERE t.is_ms_shipped = 0 AND s.name <> 'cdc'
            GROUP BY s.name, t.name, t.is_tracked_by_cdc
            ORDER BY s.name, t.name
            """,
            kind="meta",
        )
        return [
            TableRef(r["schema_name"], r["table_name"], bool(r["is_tracked_by_cdc"]),
                     int(r["row_estimate"] or 0), int(r["size_bytes"] or 0))
            for r in rows
        ]

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        rows = self._query(
            """
            SELECT c.name, ty.name AS data_type, c.is_nullable, c.max_length, c.column_id,
                   CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            JOIN sys.tables t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
            LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id
            WHERE s.name = ? AND t.name = ?
            ORDER BY c.column_id
            """,
            (schema, table),
            kind="meta",
        )
        return [
            ColumnRef(r["name"], r["data_type"], bool(r["is_nullable"]), bool(r["is_pk"]), int(r["column_id"]), r["max_length"])
            for r in rows
        ]

    # ------------------------------------------------------------------
    # prerequisites
    # ------------------------------------------------------------------
    def check_prerequisites(self, tables: Optional[Sequence[Tuple[str, str]]] = None) -> List[PrerequisiteCheck]:
        checks: List[PrerequisiteCheck] = []

        def add(name, passed, detail="", fix="", severity="error"):
            checks.append(PrerequisiteCheck(name, bool(passed), detail, fix, severity))

        try:
            self.connect()
            add("connection", True, self.server_version())
        except Exception as exc:
            add("connection", False, str(exc), "Check server/port/credentials/ODBC driver")
            return checks

        # Edition: CDC needs Enterprise, Developer, or Standard >= 2016 SP1
        try:
            r = self._query("SELECT SERVERPROPERTY('EngineEdition') AS ee, SERVERPROPERTY('Edition') AS ed, "
                            "SERVERPROPERTY('ProductMajorVersion') AS major", kind="meta")[0]
            ee, major = int(r["ee"] or 0), int(r["major"] or 0)
            ok = ee in (3, 2, 5, 8) and major >= 13 or ee == 3
            add("edition_supports_cdc", ok, f"{r['ed']} (EngineEdition={ee}, major={major})",
                "CDC requires Enterprise/Developer, or Standard edition 2016 SP1+")
        except Exception as exc:
            add("edition_supports_cdc", False, str(exc), severity="warning")

        # Agent running (capture & cleanup jobs are Agent jobs, not needed on Azure SQL MI/DB)
        try:
            rows = self._query("SELECT servicename, status_desc FROM sys.dm_server_services "
                               "WHERE servicename LIKE 'SQL Server Agent%'", kind="meta")
            if rows:
                running = rows[0]["status_desc"].lower() == "running"
                add("sql_agent_running", running, rows[0]["status_desc"],
                    "Start SQL Server Agent; CDC capture/cleanup jobs do not run without it")
            else:
                add("sql_agent_running", True, "No Agent service visible (Azure SQL?)", severity="info")
        except Exception as exc:
            add("sql_agent_running", True, f"cannot read sys.dm_server_services ({exc}); needs VIEW SERVER STATE",
                severity="warning")

        # permissions
        try:
            r = self._query("SELECT IS_SRVROLEMEMBER('sysadmin') AS sa, IS_MEMBER('db_owner') AS dbo", kind="meta")[0]
            add("permission_sysadmin", bool(r["sa"]), "required to enable CDC on the database",
                "Run sp_cdc_enable_db with a sysadmin login once; the runtime login only needs db_owner", "warning")
            add("permission_db_owner", bool(r["dbo"]) or bool(r["sa"]), "required to enable CDC on tables",
                "Grant db_owner or run sp_cdc_enable_table as a db_owner")
        except Exception as exc:
            add("permissions", False, str(exc), severity="warning")

        # database CDC state, recovery model, RCSI
        try:
            r = self._query("SELECT is_cdc_enabled, recovery_model_desc, is_read_committed_snapshot_on, snapshot_isolation_state_desc "
                            "FROM sys.databases WHERE name = DB_NAME()", kind="meta")[0]
            add("database_cdc_enabled", bool(r["is_cdc_enabled"]), "sys.databases.is_cdc_enabled",
                "EXEC sys.sp_cdc_enable_db (needs sysadmin)")
            add("recovery_model", True, r["recovery_model_desc"],
                "With SIMPLE recovery the log cannot truncate until the capture job has read it - watch log growth",
                "info" if r["recovery_model_desc"] == "FULL" else "warning")
            add("read_committed_snapshot", bool(r["is_read_committed_snapshot_on"]),
                f"RCSI={bool(r['is_read_committed_snapshot_on'])}, snapshot_isolation={r['snapshot_isolation_state_desc']}",
                "Enable RCSI (ALTER DATABASE ... SET READ_COMMITTED_SNAPSHOT ON) or snapshot from a readable secondary "
                "so the initial snapshot does not take shared locks on production", "warning")
            cdc_enabled = bool(r["is_cdc_enabled"])
        except Exception as exc:
            add("database_state", False, str(exc))
            cdc_enabled = False

        if cdc_enabled:
            # jobs + retention
            try:
                jobs = self._query("SELECT job_type, retention, threshold, pollinginterval, maxscans, maxtrans "
                                   "FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID()", kind="meta")
                kinds = {j["job_type"].strip() for j in jobs}
                add("cdc_capture_job", "capture" in kinds, f"jobs: {sorted(kinds)}", "EXEC sys.sp_cdc_add_job @job_type = N'capture'")
                add("cdc_cleanup_job", "cleanup" in kinds, f"jobs: {sorted(kinds)}", "EXEC sys.sp_cdc_add_job @job_type = N'cleanup'", "warning")
                for j in jobs:
                    if j["job_type"].strip() == "cleanup":
                        minutes = int(j["retention"] or 0)
                        add("cdc_retention", minutes >= 24 * 60, f"retention = {minutes} min ({minutes/60:.1f} h)",
                            "Retention must exceed the longest expected outage of this synchroniser "
                            "(sp_cdc_change_job @job_type='cleanup', @retention=<minutes>)",
                            "warning")
            except Exception as exc:
                add("cdc_jobs", False, f"cannot read msdb.dbo.cdc_jobs: {exc}", severity="warning")
            # log scan health
            try:
                rows = self._query("SELECT TOP 1 end_time, latency, error_count, empty_scan_count, failed_sessions_count "
                                   "FROM sys.dm_cdc_log_scan_sessions ORDER BY session_id DESC", kind="meta")
                if rows:
                    r = rows[0]
                    add("cdc_log_scan", int(r["error_count"] or 0) == 0,
                        f"last scan {r['end_time']}, latency={r['latency']}s, errors={r['error_count']}",
                        "Inspect sys.dm_cdc_errors", "warning")
            except Exception:
                add("cdc_log_scan", True, "sys.dm_cdc_log_scan_sessions not readable", severity="info")
            try:
                errs = self._query("SELECT COUNT(*) AS n FROM sys.dm_cdc_errors", kind="meta")
                n = int(errs[0]["n"]) if errs else 0
                add("cdc_errors", n == 0, f"{n} entries in sys.dm_cdc_errors", "Review sys.dm_cdc_errors", "warning")
            except Exception:
                pass

        # per-table checks
        for schema, table in tables or []:
            ci = capture_instance_name(schema, table, self.capture_instances.get(f"{schema}.{table}"))
            try:
                cols = self.get_columns(schema, table)
                if not cols:
                    add(f"table_exists:{schema}.{table}", False, "table not found")
                    continue
                pk = [c.name for c in cols if c.is_primary_key]
                add(f"primary_key:{schema}.{table}", bool(pk), f"pk={pk}",
                    "Without a PK, UPDATE/DELETE cannot be routed safely; define a PK or a unique key column set in the mapping")
                tracked = self._query("SELECT t.is_tracked_by_cdc FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id "
                                      "WHERE s.name=? AND t.name=?", (schema, table), kind="meta")
                is_tracked = bool(tracked and tracked[0]["is_tracked_by_cdc"])
                add(f"table_cdc_enabled:{schema}.{table}", is_tracked, f"capture instance '{ci}'",
                    f"EXEC sys.sp_cdc_enable_table @source_schema=N'{schema}', @source_name=N'{table}', @role_name=NULL, @capture_instance=N'{ci}'")
                if is_tracked:
                    ct = self._query("SELECT capture_instance, supports_net_changes, start_lsn, create_date FROM cdc.change_tables "
                                     "WHERE source_object_id = OBJECT_ID(?)", (f"[{schema}].[{table}]",), kind="meta")
                    if ct and ct[0]["capture_instance"] != ci:
                        add(f"capture_instance_name:{schema}.{table}", False,
                            f"actual capture instance is '{ct[0]['capture_instance']}', expected '{ci}'",
                            "Set capture_instances override in the connection config")
                    big = [c for c in cols if c.data_type in ("xml", "image", "text", "ntext", "geography", "geometry", "hierarchyid")]
                    if big:
                        add(f"lob_columns:{schema}.{table}", True, f"LOB/CLR columns captured: {[c.name for c in big]}",
                            "Large columns inflate the change tables; consider @captured_column_list", "info")
            except Exception as exc:
                add(f"table:{schema}.{table}", False, str(exc))
        return checks

    # ------------------------------------------------------------------
    # capture management
    # ------------------------------------------------------------------
    def capture_status(self) -> Dict[str, Any]:
        db = self._query("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()", kind="meta")
        enabled = bool(db and db[0]["is_cdc_enabled"])
        tables: List[Dict[str, Any]] = []
        if enabled:
            tables = self._query(
                "SELECT source_schema, source_table, capture_instance, supports_net_changes, create_date, start_lsn "
                "FROM cdc.change_tables ORDER BY source_schema, source_table", kind="meta")
            for t in tables:
                t["start_lsn"] = lsn_to_hex(t.get("start_lsn"))
        max_lsn = self.current_position() if enabled else None
        return {"database": self.config.get("database"), "cdc_enabled": enabled, "tables": tables,
                "max_lsn": str(max_lsn.value) if max_lsn else None}

    def enable_capture_database(self) -> Tuple[bool, str]:
        try:
            db = self._query("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()", kind="meta")
            if db and db[0]["is_cdc_enabled"]:
                return True, "CDC already enabled on database"
            self._execute("EXEC sys.sp_cdc_enable_db", kind="ddl")
            return True, "CDC enabled on database"
        except Exception as exc:
            return False, f"Error enabling CDC on database: {exc}"

    def enable_capture_table(self, schema: str, table: str, **options: Any) -> Tuple[bool, str]:
        ci = capture_instance_name(schema, table, options.get("capture_instance") or self.capture_instances.get(f"{schema}.{table}"))
        try:
            tracked = self._query("SELECT t.is_tracked_by_cdc FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id "
                                  "WHERE s.name=? AND t.name=?", (schema, table), kind="meta")
            if tracked and tracked[0]["is_tracked_by_cdc"]:
                return True, f"CDC already enabled on {schema}.{table}"
            sql = ("EXEC sys.sp_cdc_enable_table @source_schema=?, @source_name=?, @role_name=?, @capture_instance=?, "
                   "@supports_net_changes=?")
            params = [schema, table, options.get("role_name"), ci, 1 if options.get("supports_net_changes", False) else 0]
            if options.get("captured_column_list"):
                sql += ", @captured_column_list=?"
                params.append(options["captured_column_list"])
            if options.get("filegroup_name"):
                sql += ", @filegroup_name=?"
                params.append(options["filegroup_name"])
            self._execute(sql, params, kind="ddl")
            self.capture_instances[f"{schema}.{table}"] = ci
            return True, f"CDC enabled on {schema}.{table} (capture instance {ci})"
        except Exception as exc:
            return False, f"Error enabling CDC on {schema}.{table}: {exc}"

    def disable_capture_table(self, schema: str, table: str, **options: Any) -> Tuple[bool, str]:
        ci = capture_instance_name(schema, table, options.get("capture_instance") or self.capture_instances.get(f"{schema}.{table}"))
        try:
            self._execute("EXEC sys.sp_cdc_disable_table @source_schema=?, @source_name=?, @capture_instance=?",
                          (schema, table, ci), kind="ddl")
            return True, f"CDC disabled on {schema}.{table}"
        except Exception as exc:
            return False, f"Error disabling CDC on {schema}.{table}: {exc}"

    # ------------------------------------------------------------------
    # positions / change reading
    # ------------------------------------------------------------------
    def current_position(self) -> Optional[Position]:
        rows = self._query("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn", kind="watermark")
        lsn = rows[0]["max_lsn"] if rows else None
        return Position(POSITION_KIND, lsn_to_hex(lsn)) if lsn else None

    def min_position(self, schema: str, table: str) -> Optional[Position]:
        ci = capture_instance_name(schema, table, self.capture_instances.get(f"{schema}.{table}"))
        rows = self._query("SELECT sys.fn_cdc_get_min_lsn(?) AS min_lsn", (ci,), kind="meta")
        lsn = rows[0]["min_lsn"] if rows else None
        return Position(POSITION_KIND, lsn_to_hex(lsn)) if lsn else None

    def increment(self, position: Position) -> Position:
        rows = self._query("SELECT sys.fn_cdc_increment_lsn(?) AS nxt", (hex_to_lsn(position.value),), kind="meta")
        return Position(POSITION_KIND, lsn_to_hex(rows[0]["nxt"]))

    def changed_capture_instances(self, tables: Sequence[Tuple[str, str]], after: Dict[str, Optional[Position]]) -> Dict[str, int]:
        """ONE round trip: which capture tables have rows after their committed position.

        Returns ``{"schema.table": pending_row_estimate}`` for tables that changed.
        """
        if not tables:
            return {}
        unions, params = [], []
        for schema, table in tables:
            key = f"{schema}.{table}"
            ci = capture_instance_name(schema, table, self.capture_instances.get(key))
            pos = after.get(key)
            # EXISTS + TOP-less seek on the clustered (__$start_lsn, __$seqval) index: O(1) per table
            if pos and pos.value:
                unions.append(f"SELECT '{key}' AS tbl, CASE WHEN EXISTS (SELECT 1 FROM cdc.{self.quote(ci + '_CT')} WITH (NOLOCK) "
                              f"WHERE __$start_lsn > ?) THEN 1 ELSE 0 END AS n")
                params.append(hex_to_lsn(pos.value))
            else:
                unions.append(f"SELECT '{key}' AS tbl, CASE WHEN EXISTS (SELECT 1 FROM cdc.{self.quote(ci + '_CT')} WITH (NOLOCK)) "
                              f"THEN 1 ELSE 0 END AS n")
        rows = self._query(" UNION ALL ".join(unions), params, kind="probe")
        return {r["tbl"]: int(r["n"]) for r in rows if int(r["n"] or 0) > 0}

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        if to_position is None:
            to_position = self.current_position()
        if to_position is None:
            return [], None
        to_lsn = hex_to_lsn(to_position.value)
        events: List[ChangeEvent] = []
        last_pos = from_position
        for schema, table in tables:
            if len(events) >= limit:
                break
            key = f"{schema}.{table}"
            ci = capture_instance_name(schema, table, self.capture_instances.get(key))
            if from_position is None or not from_position.value:
                min_pos = self.min_position(schema, table)
                if min_pos is None:
                    continue
                start_lsn = hex_to_lsn(min_pos.value)
                seq_filter, seq_params = "", []
            else:
                start_lsn = hex_to_lsn(from_position.value)
                min_pos = self.min_position(schema, table)
                if min_pos and min_pos.value and start_lsn < hex_to_lsn(min_pos.value):
                    # The cleanup job already purged changes after our committed position. Do NOT skip
                    # silently (that is how a bank loses rows): stop this table and let the
                    # data-loss-guard rule fire. Operator action: reset positions + new snapshot.
                    metrics.POLL_COUNT.labels(source=self.name, outcome="position_purged").inc()
                    raise ConnectorError(
                        f"{key}: committed LSN {from_position.value} is older than the oldest retained change "
                        f"{min_pos.value} (CDC cleanup retention exceeded) - changes were lost; reset the position "
                        f"and run a new snapshot")
                seqval = from_position.extra.get("seqval") if from_position.extra else None
                if seqval:
                    # resume inside the committed LSN: skip rows already applied
                    seq_filter = " WHERE NOT (ct.__$start_lsn = ? AND ct.__$seqval <= ?)"
                    seq_params = [start_lsn, hex_to_lsn(seqval)]
                else:
                    start_lsn = hex_to_lsn(self.increment(from_position).value)
                    seq_filter, seq_params = "", []
            if start_lsn > to_lsn:
                continue
            remaining = limit - len(events)
            sql = (
                f"SELECT TOP ({remaining}) ct.*, lt.tran_end_time "
                f"FROM cdc.{self.quote('fn_cdc_get_all_changes_' + ci)}(?, ?, ?) ct "
                f"LEFT JOIN cdc.lsn_time_mapping lt ON lt.start_lsn = ct.__$start_lsn"
                f"{seq_filter} ORDER BY ct.__$start_lsn, ct.__$seqval, ct.__$operation"
            )
            try:
                rows = self._query(sql, [start_lsn, to_lsn, self.row_filter] + seq_params, kind="cdc_read")
            except Exception as exc:
                metrics.POLL_COUNT.labels(source=self.name, outcome="error").inc()
                raise ConnectorError(f"reading CDC changes for {key}: {exc}") from exc
            pending_before: Optional[Dict[str, Any]] = None
            for row in rows:
                op_code = int(row.get("__$operation", 2))
                payload = {k: v for k, v in row.items() if not k.startswith("__$") and k != "tran_end_time"}
                pos = Position(POSITION_KIND, lsn_to_hex(row["__$start_lsn"]), {"seqval": lsn_to_hex(row["__$seqval"])})
                if op_code == 3:  # before image of an update (only with 'all update old')
                    pending_before = payload
                    continue
                op = _OP_MAP.get(op_code, Operation.INSERT)
                ev = ChangeEvent(
                    source=self.name, schema=schema, table=table, operation=op, position=pos,
                    after=payload if op != Operation.DELETE else None,
                    before=payload if op == Operation.DELETE else pending_before,
                    source_ts=row.get("tran_end_time"), tx_id=lsn_to_hex(row["__$start_lsn"]),
                    event_id=str(uuid.uuid4()),
                    metadata={"capture_instance": ci, "update_mask": lsn_to_hex(row.get("__$update_mask"))},
                )
                pending_before = None
                events.append(ev)
                metrics.EVENTS_READ.labels(source=self.name, table=key, op=op.value).inc()
                last_pos = pos
        metrics.POLL_COUNT.labels(source=self.name, outcome="ok").inc()
        # if we did not hit the limit, everything up to to_position is consumed
        if len(events) < limit:
            return events, to_position
        return events, last_pos

    # ------------------------------------------------------------------
    # push mode (Service Broker) + adaptive streaming
    # ------------------------------------------------------------------
    @staticmethod
    def install_service_broker_sql(queue_name: str = "cdcsync_notify_queue", poll_ms: int = 500) -> str:
        """T-SQL to install the (optional) server-side notifier.

        Footprint: one message type, contract, queue, service and ONE stored
        procedure that loops *inside* the server checking ``fn_cdc_get_max_lsn``
        and SENDs a message when it moved. No triggers on user tables.
        Activate it with a SQL Agent job step ``EXEC dbo.cdcsync_notifier``.
        """
        return f"""
-- enable Service Broker once (requires exclusive access, usually already on)
-- ALTER DATABASE CURRENT SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = 'cdcsync_lsn_advanced')
    CREATE MESSAGE TYPE cdcsync_lsn_advanced VALIDATION = NONE;
IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = 'cdcsync_contract')
    CREATE CONTRACT cdcsync_contract (cdcsync_lsn_advanced SENT BY INITIATOR);
IF NOT EXISTS (SELECT 1 FROM sys.service_queues WHERE name = '{queue_name}')
    CREATE QUEUE dbo.{queue_name} WITH STATUS = ON;
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = 'cdcsync_target')
    CREATE SERVICE cdcsync_target ON QUEUE dbo.{queue_name} (cdcsync_contract);
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = 'cdcsync_initiator')
    CREATE SERVICE cdcsync_initiator ON QUEUE dbo.{queue_name} (cdcsync_contract);
GO
CREATE OR ALTER PROCEDURE dbo.cdcsync_notifier @poll_ms INT = {poll_ms}
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @last BINARY(10) = sys.fn_cdc_get_max_lsn(), @cur BINARY(10), @h UNIQUEIDENTIFIER,
            @delay CHAR(12) = '00:00:00.' + RIGHT('000' + CAST(@poll_ms AS VARCHAR(3)), 3);
    WHILE 1 = 1
    BEGIN
        WAITFOR DELAY @delay;
        SET @cur = sys.fn_cdc_get_max_lsn();
        IF @cur <> @last
        BEGIN
            BEGIN DIALOG CONVERSATION @h FROM SERVICE cdcsync_initiator TO SERVICE 'cdcsync_target'
                ON CONTRACT cdcsync_contract WITH ENCRYPTION = OFF;
            SEND ON CONVERSATION @h MESSAGE TYPE cdcsync_lsn_advanced (CONVERT(VARBINARY(10), @cur));
            END CONVERSATION @h;
            SET @last = @cur;
        END
    END
END
GO
"""

    def wait_for_notification(self, timeout_ms: int = 30_000) -> Optional[Position]:
        """Block server-side until the notifier sends a new LSN (or timeout)."""
        if not self.service_broker_queue:
            raise ConnectorError("service_broker_queue not configured")
        sql = (f"WAITFOR (RECEIVE TOP (1) message_body, message_type_name FROM {self.service_broker_queue}), "
               f"TIMEOUT {int(timeout_ms)}")
        rows = self._query(sql, kind="broker_wait")
        # drain anything else that piled up (coalesce bursts)
        try:
            self._query(f"RECEIVE message_body FROM {self.service_broker_queue}", kind="broker_drain")
        except Exception:  # pragma: no cover
            pass
        for r in rows:
            if r.get("message_type_name") == "cdcsync_lsn_advanced" and r.get("message_body"):
                return Position(POSITION_KIND, bytes(r["message_body"]).hex())
        return None

    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        """Adaptive watermark loop: 1 tiny query per cycle when idle, backing off up to ``max_idle_s``."""
        from backend.connectors.watermark import AdaptiveInterval
        interval = AdaptiveInterval(
            min_s=float(self.config.get("poll_min_seconds", 0.25)),
            max_s=float(self.config.get("poll_max_seconds", 30.0)),
        )
        if isinstance(from_position, dict):
            positions: Dict[str, Optional[Position]] = {f"{s}.{t}": from_position.get(f"{s}.{t}") for s, t in tables}
        else:
            positions = {f"{s}.{t}": from_position for s, t in tables}
        last_watermark = None
        while not stop_event.is_set():
            try:
                if self.service_broker_queue:
                    pos = self.wait_for_notification(timeout_ms=int(interval.max_s * 1000))
                    watermark = pos or self.current_position()
                else:
                    watermark = self.current_position()
                metrics.WATERMARK_CHECKS.labels(source=self.name, outcome="moved" if watermark != last_watermark else "idle").inc()
                if watermark is None or watermark == last_watermark:
                    stop_event.wait(interval.idle())
                    continue
                last_watermark = watermark
                changed = self.changed_capture_instances(tables, positions)
                if not changed:
                    stop_event.wait(interval.idle())
                    continue
                interval.busy()
                for key, _n in changed.items():
                    schema, table = key.split(".", 1)
                    events, commit_pos = self.read_changes([(schema, table)], positions[key], watermark, limit=self.config.get("batch_limit", 5000))
                    for ev in events:
                        yield ev
                    positions[key] = commit_pos
            except ConnectorError:
                raise
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                logger.error("MSSQL stream error: %s", exc)
                stop_event.wait(interval.error())

    # ------------------------------------------------------------------
    # health facts for the alert rules
    # ------------------------------------------------------------------
    def retention_status(self, positions: Dict[str, Optional[Position]]) -> Dict[str, Any]:
        """Compare committed positions with what CDC still retains (data-loss guard)."""
        out: Dict[str, Any] = {"retention_minutes": None, "position_before_min": False, "capture_errors": 0,
                               "retention_margin_minutes": None, "prerequisites_failed": 0}
        try:
            jobs = self._query("SELECT job_type, retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID()", kind="meta")
            for j in jobs:
                if j["job_type"].strip() == "cleanup":
                    out["retention_minutes"] = int(j["retention"] or 0)
        except Exception:
            pass
        try:
            out["capture_errors"] = int(self._query("SELECT COUNT(*) AS n FROM sys.dm_cdc_errors", kind="meta")[0]["n"])
        except Exception:
            pass
        oldest_commit_age_min: Optional[float] = None
        for key, pos in positions.items():
            if not pos or not pos.value or key == "__source__":
                continue
            schema, table = key.split(".", 1)
            try:
                mn = self.min_position(schema, table)
                if mn and mn.value and hex_to_lsn(pos.value) < hex_to_lsn(mn.value):
                    out["position_before_min"] = True
                rows = self._query("SELECT sys.fn_cdc_map_lsn_to_time(?) AS t", (hex_to_lsn(pos.value),), kind="meta")
                t = rows[0]["t"] if rows else None
                if t:
                    age = (datetime.now() - t).total_seconds() / 60.0
                    oldest_commit_age_min = max(oldest_commit_age_min or 0, age)
            except Exception:
                continue
        if out["retention_minutes"] and oldest_commit_age_min is not None:
            out["retention_margin_minutes"] = round(out["retention_minutes"] - oldest_commit_age_min, 1)
        return out

    # ------------------------------------------------------------------
    # snapshot support
    # ------------------------------------------------------------------
    def count_rows(self, schema: str, table: str, approximate: bool = True) -> Optional[int]:
        if approximate:
            rows = self._query(
                "SELECT SUM(row_count) AS n FROM sys.dm_db_partition_stats "
                "WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1)", (f"[{schema}].[{table}]",), kind="meta")
            return int(rows[0]["n"] or 0) if rows else None
        rows = self._query(f"SELECT COUNT_BIG(*) AS n FROM {self.qualified(schema, table)}", kind="count")
        return int(rows[0]["n"]) if rows else None

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        cols = ", ".join(self.quote(c) for c in columns)
        order = ", ".join(self.quote(c) for c in key_columns)
        conditions, params = [], []
        if after_key is not None:
            pred, _ = keyset_predicate(key_columns, self.quote, row_values=False)
            conditions.append(pred)
            params.extend(keyset_params(after_key))
        if where:
            conditions.append(f"({where})")
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        hint = f" {self.snapshot_hint}" if self.snapshot_hint else ""
        sql = f"SELECT TOP ({int(chunk_size)}) {cols} FROM {self.qualified(schema, table)}{hint}{where_sql} ORDER BY {order}"
        return self._query(sql, params, kind="snapshot_chunk")

    def key_bounds(self, schema, table, key_columns):
        k = self.quote(key_columns[0])
        rows = self._query(f"SELECT MIN({k}) AS lo, MAX({k}) AS hi FROM {self.qualified(schema, table)}", kind="meta")
        if not rows:
            return None, None
        return (rows[0]["lo"],), (rows[0]["hi"],)
