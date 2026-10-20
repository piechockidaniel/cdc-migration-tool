"""Oracle connector - LogMiner based change capture (thin mode, no Oracle client needed).

How it works
------------
1. Prerequisites: ARCHIVELOG mode, minimal + table level supplemental logging,
   ``LOGMINING`` / ``SELECT ANY TRANSACTION`` privileges, access to
   ``V$LOGMNR_CONTENTS``/``V$LOG``/``V$ARCHIVED_LOG``/``V$TRANSACTION``,
   multitenant placement (LogMiner must run in CDB$ROOT), archive retention vs.
   committed SCN.
2. ``read_changes``: adds the online/archived log files covering the SCN range
   (``CONTINUOUS_MINE`` is desupported since 19c), starts LogMiner with
   ``COMMITTED_DATA_ONLY + DICT_FROM_ONLINE_CATALOG + NO_ROWID_IN_STMT``,
   selects DML for the wanted tables and parses ``SQL_REDO`` back into row
   images (``oracle_redo.py``).
3. Positions are commit SCNs. To never miss rows of long-running transactions
   the mining window starts at ``min(open transaction START_SCN, last_commit+1)``
   and already-applied transactions are filtered with ``COMMIT_SCN > last``.
4. Batches are cut only at transaction boundaries.

Oracle Continuous Query Notification (``DBMS_CHANGE_NOTIFICATION``) would be a
true push channel but needs thick mode + a callback port reachable from the
DB server; it is deliberately not used here. The watermark
(``SELECT current_scn FROM v$database``) is a single index-free lookup.
"""
from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, ConnectorError, Operation, Position,
    PrerequisiteCheck, SourceConnector, TableRef, keyset_params, keyset_predicate,
)
from backend.connectors.oracle_redo import RedoParseError, parse_redo
from backend.connectors.watermark import AdaptiveInterval
from backend.core import metrics

logger = logging.getLogger(__name__)

POSITION_KIND = "oracle_scn"
_OPCODES = {1: Operation.INSERT, 2: Operation.DELETE, 3: Operation.UPDATE}


class OracleConnector(SourceConnector):
    engine = "oracle"
    capabilities = frozenset({
        Capability.NATIVE_CDC, Capability.BEFORE_IMAGE, Capability.KEYSET_SNAPSHOT,
        Capability.TRANSACTION_BOUNDARIES, Capability.CONSISTENT_SNAPSHOT,
    })

    def __init__(self, config: Dict[str, Any], connection_factory=None):
        super().__init__(config)
        self._conn = None
        self._connection_factory = connection_factory
        self.batch_limit = int(config.get("batch_limit", 5000))
        self.max_scn_window = int(config.get("max_scn_window", 5_000_000))  # cap mining range per call
        self.supplemental_mode = config.get("supplemental_mode", "ALL")      # ALL | PRIMARY KEY

    # ------------------------------------------------------------------
    def dsn(self) -> str:
        c = self.config
        if c.get("dsn"):
            return c["dsn"]
        host = c.get("server", c.get("host", "localhost"))
        port = c.get("port") or 1521
        svc = c.get("service_name") or c.get("database", "")
        return f"{host}:{port}/{svc}"

    def connect(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            try:
                if self._connection_factory is not None:
                    self._conn = self._connection_factory(self)
                else:
                    import oracledb
                    kwargs: Dict[str, Any] = dict(user=self.config.get("username"), password=self.config.get("password"),
                                                  dsn=self.dsn(), tcp_connect_timeout=float(self.config.get("login_timeout", 15)))
                    if str(self.config.get("mode", "")).upper() == "SYSDBA":
                        kwargs["mode"] = oracledb.AUTH_MODE_SYSDBA
                    self._conn = oracledb.connect(**kwargs)
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                raise ConnectorError(f"Oracle connect failed ({self.dsn()}): {exc}") from exc

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
                self._query("SELECT 1 FROM dual", kind="ping")
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
                    cur.execute(sql, list(params) if params else [])
                    if cur.description is None:
                        return []
                    cols = [d[0] for d in cur.description]
                    rows = []
                    for r in cur.fetchall():
                        row = []
                        for v in r:
                            if hasattr(v, "read") and callable(v.read):  # LOB
                                v = v.read()
                            row.append(v)
                        rows.append(dict(zip(cols, row)))
                    return rows
                finally:
                    cur.close()

    def _execute(self, sql: str, params: Sequence[Any] = (), kind: str = "exec") -> None:
        with self._lock:
            if self._conn is None:
                self.connect()
            with metrics.timed_query(self.name, kind):
                cur = self._conn.cursor()
                try:
                    cur.execute(sql, list(params) if params else [])
                    self._conn.commit()
                finally:
                    cur.close()

    def server_version(self) -> str:
        rows = self._query("SELECT banner FROM v$version WHERE ROWNUM = 1", kind="meta")
        return rows[0]["BANNER"] if rows else "unknown"

    def quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    # ------------------------------------------------------------------
    def _owner(self) -> str:
        return (self.config.get("schema") or self.config.get("username") or "").upper()

    def list_tables(self) -> List[TableRef]:
        owner = self._owner()
        rows = self._query(
            "SELECT t.owner, t.table_name, t.num_rows, "
            "(SELECT SUM(bytes) FROM dba_segments s WHERE s.owner = t.owner AND s.segment_name = t.table_name) AS size_bytes "
            "FROM all_tables t WHERE t.owner = :1 ORDER BY t.table_name", (owner,), kind="meta")
        tracked = {r["TABLE_NAME"] for r in self._query(
            "SELECT DISTINCT table_name FROM all_log_groups WHERE owner = :1", (owner,), kind="meta")}
        return [TableRef(r["OWNER"], r["TABLE_NAME"], r["TABLE_NAME"] in tracked, int(r["NUM_ROWS"] or 0), int(r["SIZE_BYTES"] or 0))
                for r in rows]

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        rows = self._query(
            """
            SELECT c.column_name, c.data_type, c.nullable, c.column_id, c.data_length,
                   CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_pk
            FROM all_tab_columns c
            LEFT JOIN (
                SELECT cc.column_name FROM all_constraints k JOIN all_cons_columns cc
                  ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
                WHERE k.owner = :1 AND k.table_name = :2 AND k.constraint_type = 'P'
            ) pk ON pk.column_name = c.column_name
            WHERE c.owner = :3 AND c.table_name = :4 ORDER BY c.column_id
            """, (schema.upper(), table.upper(), schema.upper(), table.upper()), kind="meta")
        return [ColumnRef(r["COLUMN_NAME"], r["DATA_TYPE"], r["NULLABLE"] == "Y", bool(r["IS_PK"]), int(r["COLUMN_ID"]), r["DATA_LENGTH"])
                for r in rows]

    # ------------------------------------------------------------------
    def check_prerequisites(self, tables=None) -> List[PrerequisiteCheck]:
        checks: List[PrerequisiteCheck] = []

        def add(name, passed, detail="", fix="", severity="error"):
            checks.append(PrerequisiteCheck(name, bool(passed), detail, fix, severity))

        try:
            self.connect()
            add("connection", True, self.server_version())
        except Exception as exc:
            add("connection", False, str(exc))
            return checks
        try:
            db = self._query("SELECT log_mode, supplemental_log_data_min, supplemental_log_data_pk, supplemental_log_data_all, "
                             "cdb, current_scn FROM v$database", kind="meta")[0]
            add("archivelog_mode", db["LOG_MODE"] == "ARCHIVELOG", f"log_mode={db['LOG_MODE']}",
                "SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN;")
            add("supplemental_log_min", db["SUPPLEMENTAL_LOG_DATA_MIN"] in ("YES", "IMPLICIT"),
                f"minimal={db['SUPPLEMENTAL_LOG_DATA_MIN']}, pk={db['SUPPLEMENTAL_LOG_DATA_PK']}, all={db['SUPPLEMENTAL_LOG_DATA_ALL']}",
                "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")
            if db.get("CDB") == "YES":
                con = self._query("SELECT SYS_CONTEXT('USERENV','CON_NAME') AS con FROM dual", kind="meta")[0]["CON"]
                add("multitenant_container", con == "CDB$ROOT", f"connected to container {con}",
                    "LogMiner must run from CDB$ROOT (common user C##..., and the PDB tables referenced by owner)",
                    "warning")
        except Exception as exc:
            add("database_state", False, str(exc), "GRANT SELECT ON V_$DATABASE TO <user>")
        try:
            privs = {r["PRIVILEGE"] for r in self._query("SELECT privilege FROM session_privs", kind="meta")}
            add("priv_logmining", "LOGMINING" in privs or "SYSDBA" in privs, f"session_privs has LOGMINING={'LOGMINING' in privs}",
                "GRANT LOGMINING TO <user>; GRANT EXECUTE ON DBMS_LOGMNR TO <user>;")
            add("priv_select_any_transaction", "SELECT ANY TRANSACTION" in privs or "SYSDBA" in privs, "",
                "GRANT SELECT ANY TRANSACTION TO <user>;")
        except Exception as exc:
            add("privileges", False, str(exc))
        for view in ("v$log", "v$logfile", "v$archived_log", "v$transaction", "v$logmnr_logs"):
            try:
                self._query(f"SELECT COUNT(*) AS n FROM {view} WHERE ROWNUM = 1", kind="meta")
                add(f"access_{view}", True)
            except Exception as exc:
                add(f"access_{view}", False, str(exc)[:120], f"GRANT SELECT ON {view.replace('v$', 'V_$').upper()} TO <user>",
                    "warning" if view == "v$transaction" else "error")
        try:
            arch = self._query("SELECT MIN(first_change#) AS lo, COUNT(*) AS n FROM v$archived_log WHERE deleted = 'NO' AND status = 'A'",
                               kind="meta")
            if arch and arch[0]["N"]:
                add("archive_retention", True, f"{arch[0]['N']} archived logs available, oldest SCN {arch[0]['LO']}",
                    "Retention must exceed the longest outage of the synchroniser (RMAN retention policy)", "info")
        except Exception:
            pass
        for schema, table in tables or []:
            try:
                cols = self.get_columns(schema, table)
                if not cols:
                    add(f"table_exists:{schema}.{table}", False, "table not found")
                    continue
                pk = [c.name for c in cols if c.is_primary_key]
                add(f"primary_key:{schema}.{table}", bool(pk), f"pk={pk}",
                    "Without a PK define key columns in the mapping and use supplemental logging (ALL) COLUMNS")
                groups = {r["LOG_GROUP_TYPE"] for r in self._query(
                    "SELECT log_group_type FROM all_log_groups WHERE owner = :1 AND table_name = :2",
                    (schema.upper(), table.upper()), kind="meta")}
                ok = bool(groups & {"ALL COLUMN LOGGING", "PRIMARY KEY LOGGING"})
                add(f"supplemental_log_table:{schema}.{table}", ok, f"log groups={sorted(groups)}",
                    f"ALTER TABLE {self.qualified(schema, table)} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;")
                lobs = [c.name for c in cols if c.data_type in ("CLOB", "BLOB", "NCLOB", "LONG", "LONG RAW", "XMLTYPE")]
                if lobs:
                    add(f"lob_columns:{schema}.{table}", True, f"LOB columns {lobs} are not reconstructed by LogMiner redo SQL",
                        "Re-read LOB values from the source on apply (mapping option 'reselect_columns')", "warning")
            except Exception as exc:
                add(f"table:{schema}.{table}", False, str(exc))
        return checks

    def capture_status(self) -> Dict[str, Any]:
        try:
            db = self._query("SELECT log_mode, supplemental_log_data_min, current_scn FROM v$database", kind="meta")[0]
        except Exception as exc:
            return {"database": self.config.get("database"), "cdc_enabled": False, "error": str(exc)}
        tables = self._query("SELECT owner, table_name, log_group_type FROM all_log_groups WHERE owner = :1", (self._owner(),), kind="meta")
        return {"database": self.config.get("database"), "cdc_enabled": db["LOG_MODE"] == "ARCHIVELOG" and db["SUPPLEMENTAL_LOG_DATA_MIN"] != "NO",
                "current_scn": int(db["CURRENT_SCN"]), "tables": tables}

    def enable_capture_database(self) -> Tuple[bool, str]:
        try:
            db = self._query("SELECT log_mode, supplemental_log_data_min FROM v$database", kind="meta")[0]
            if db["LOG_MODE"] != "ARCHIVELOG":
                return False, "database is not in ARCHIVELOG mode (requires a restart in MOUNT state by the DBA)"
            if db["SUPPLEMENTAL_LOG_DATA_MIN"] == "NO":
                self._execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA", kind="ddl")
                return True, "minimal supplemental logging enabled"
            return True, "database already prepared for LogMiner"
        except Exception as exc:
            return False, f"cannot enable supplemental logging (needs ALTER DATABASE): {exc}"

    def enable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        mode = (options.get("supplemental_mode") or self.supplemental_mode).upper()
        clause = "(ALL) COLUMNS" if mode == "ALL" else "(PRIMARY KEY) COLUMNS"
        try:
            self._execute(f"ALTER TABLE {self.qualified(schema, table)} ADD SUPPLEMENTAL LOG DATA {clause}", kind="ddl")
            return True, f"supplemental logging {clause} enabled on {schema}.{table}"
        except Exception as exc:
            if "ORA-32588" in str(exc):  # already exists
                return True, f"supplemental logging already enabled on {schema}.{table}"
            return False, f"cannot enable supplemental logging on {schema}.{table}: {exc}"

    def disable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        mode = (options.get("supplemental_mode") or self.supplemental_mode).upper()
        clause = "(ALL) COLUMNS" if mode == "ALL" else "(PRIMARY KEY) COLUMNS"
        try:
            self._execute(f"ALTER TABLE {self.qualified(schema, table)} DROP SUPPLEMENTAL LOG DATA {clause}", kind="ddl")
            return True, f"supplemental logging dropped on {schema}.{table}"
        except Exception as exc:
            return False, str(exc)

    # ------------------------------------------------------------------
    def current_position(self) -> Optional[Position]:
        rows = self._query("SELECT current_scn FROM v$database", kind="watermark")
        return Position(POSITION_KIND, int(rows[0]["CURRENT_SCN"])) if rows else None

    def _oldest_open_scn(self) -> Optional[int]:
        try:
            rows = self._query("SELECT MIN(start_scn) AS scn FROM v$transaction", kind="meta")
            return int(rows[0]["SCN"]) if rows and rows[0]["SCN"] is not None else None
        except Exception:
            return None

    def _add_logfiles(self, start_scn: int, end_scn: int) -> int:
        files = self._query(
            """
            SELECT f.member AS name, l.first_change#, l.next_change# FROM v$log l JOIN v$logfile f ON f.group# = l.group#
            WHERE l.next_change# >= :1 AND l.first_change# <= :2
            UNION ALL
            SELECT a.name, a.first_change#, a.next_change# FROM v$archived_log a
            WHERE a.deleted = 'NO' AND a.status = 'A' AND a.next_change# >= :3 AND a.first_change# <= :4
            """, (start_scn, end_scn, start_scn, end_scn), kind="meta")
        seen = set()
        count = 0
        for i, f in enumerate(files):
            if f["NAME"] in seen:
                continue
            seen.add(f["NAME"])
            opt = "DBMS_LOGMNR.NEW" if count == 0 else "DBMS_LOGMNR.ADDFILE"
            self._execute(f"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => {opt}); END;", (f["NAME"],), kind="logmnr")
            count += 1
        return count

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        wanted = {(s.upper(), t.upper()) for s, t in tables}
        if to_position is None:
            to_position = self.current_position()
        end_scn = int(to_position.value)
        last_commit = int(from_position.value) if from_position and from_position.value else None
        if last_commit is None:
            last_commit = end_scn  # first start: capture from "now"
            return [], Position(POSITION_KIND, last_commit)
        start_scn = last_commit + 1
        oldest = self._oldest_open_scn()
        if oldest is not None and oldest < start_scn:
            start_scn = oldest
        end_scn = min(end_scn, start_scn + self.max_scn_window)
        if end_scn <= last_commit:
            return [], from_position
        if self._add_logfiles(start_scn, end_scn) == 0:
            raise ConnectorError(f"no redo/archive logs cover SCN {start_scn}-{end_scn}; archive retention too short?")
        rows: List[Dict[str, Any]] = []
        self._execute(
            "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, ENDSCN => :2, OPTIONS => "
            "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.NO_ROWID_IN_STMT); END;",
            (start_scn, end_scn), kind="logmnr")
        try:
            owners = sorted({s for s, _ in wanted})
            names = sorted({t for _, t in wanted})
            own_ph = ", ".join(f":o{i}" for i in range(len(owners)))
            tab_ph = ", ".join(f":t{i}" for i in range(len(names)))
            rows = self._query(
                f"SELECT scn, commit_scn, timestamp, commit_timestamp, operation_code, seg_owner, table_name, sql_redo, "
                f"xid, rs_id, ssn, csf FROM v$logmnr_contents WHERE operation_code IN (1,2,3) "
                f"AND seg_owner IN ({own_ph}) AND table_name IN ({tab_ph}) AND commit_scn > :last",
                owners + names + [last_commit], kind="cdc_read")
        finally:
            try:
                self._execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", kind="logmnr")
            except Exception:  # pragma: no cover
                pass
        events: List[ChangeEvent] = []
        last_full_commit = last_commit
        pending_sql = ""
        current_commit: Optional[int] = None
        cut = False
        for r in rows:
            commit_scn = int(r["COMMIT_SCN"])
            if current_commit is not None and commit_scn != current_commit:
                last_full_commit = current_commit
                if len(events) >= limit:
                    cut = True
                    break
            current_commit = commit_scn
            pending_sql += r["SQL_REDO"] or ""
            if int(r.get("CSF") or 0) == 1:
                continue  # continuation row follows
            sql, pending_sql = pending_sql, ""
            try:
                parsed = parse_redo(sql)
            except RedoParseError as exc:
                logger.warning("cannot parse redo for %s.%s at SCN %s: %s", r["SEG_OWNER"], r["TABLE_NAME"], r["SCN"], exc)
                metrics.EVENTS_FAILED.labels(table=f"{r['SEG_OWNER']}.{r['TABLE_NAME']}", op="PARSE").inc()
                continue
            if (parsed.owner.upper(), parsed.table.upper()) not in wanted:
                continue
            op = _OPCODES.get(int(r["OPERATION_CODE"]), Operation.INSERT)
            if op == Operation.INSERT:
                before, after = None, parsed.values
            elif op == Operation.DELETE:
                before, after = parsed.where, None
            else:
                before = parsed.where
                after = dict(parsed.where)
                after.update(parsed.values)
            ev = ChangeEvent(
                source=self.name, schema=parsed.owner, table=parsed.table, operation=op,
                position=Position(POSITION_KIND, commit_scn, {"rs_id": str(r.get("RS_ID")).strip(), "ssn": int(r.get("SSN") or 0)}),
                before=before, after=after, source_ts=r.get("COMMIT_TIMESTAMP") or r.get("TIMESTAMP"),
                tx_id=bytes(r["XID"]).hex() if isinstance(r.get("XID"), (bytes, bytearray)) else str(r.get("XID")),
                event_id=str(uuid.uuid4()),
            )
            events.append(ev)
            metrics.EVENTS_READ.labels(source=self.name, table=ev.table_key, op=op.value).inc()
        if not cut and current_commit is not None:
            last_full_commit = current_commit
        metrics.POLL_COUNT.labels(source=self.name, outcome="ok").inc()
        commit_pos = Position(POSITION_KIND, max(last_commit, end_scn) if not cut else last_full_commit)
        return events, commit_pos

    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        interval = AdaptiveInterval(float(self.config.get("poll_min_seconds", 1.0)), float(self.config.get("poll_max_seconds", 60.0)))
        position = from_position
        last_wm: Optional[int] = None
        while not stop_event.is_set():
            try:
                wm = self.current_position()
                moved = wm is not None and wm.value != last_wm
                metrics.WATERMARK_CHECKS.labels(source=self.name, outcome="moved" if moved else "idle").inc()
                if not moved:
                    stop_event.wait(interval.idle())
                    continue
                last_wm = wm.value
                events, commit = self.read_changes(tables, position, wm, self.batch_limit)
                if events:
                    interval.busy()
                    for ev in events:
                        yield ev
                else:
                    stop_event.wait(interval.idle())
                position = commit
            except ConnectorError:
                raise
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                logger.error("Oracle stream error: %s", exc)
                stop_event.wait(interval.error())

    def retention_status(self, positions: Dict[str, Optional[Position]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"position_before_min": False, "capture_errors": 0, "retention_margin_minutes": None}
        pos = positions.get("__source__")
        try:
            rows = self._query("SELECT MIN(first_change#) AS lo FROM v$archived_log WHERE deleted = 'NO' AND status = 'A'", kind="meta")
            online = self._query("SELECT MIN(first_change#) AS lo FROM v$log", kind="meta")
            candidates = [int(r["LO"]) for r in rows + online if r.get("LO") is not None]
            if pos and pos.value and candidates:
                out["position_before_min"] = int(pos.value) < min(candidates)
        except Exception:
            pass
        return out

    # ------------------------------------------------------------------
    def count_rows(self, schema, table, approximate=True):
        if approximate:
            rows = self._query("SELECT num_rows FROM all_tables WHERE owner = :1 AND table_name = :2",
                               (schema.upper(), table.upper()), kind="meta")
            return int(rows[0]["NUM_ROWS"] or 0) if rows else None
        rows = self._query(f"SELECT COUNT(*) AS n FROM {self.qualified(schema, table)}", kind="count")
        return int(rows[0]["N"]) if rows else None

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        cols = ", ".join(self.quote(c) for c in columns)
        order = ", ".join(self.quote(c) for c in key_columns)
        conditions, params = [], []
        if after_key is not None:
            # Oracle has no row-value comparison -> expanded predicate with positional binds
            pred, _ = keyset_predicate(key_columns, self.quote, placeholder="?", row_values=False)
            params.extend(keyset_params(after_key))
            conditions.append(pred)
        if where:
            conditions.append(f"({where})")
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT {cols} FROM {self.qualified(schema, table)}{where_sql} ORDER BY {order} FETCH FIRST {int(chunk_size)} ROWS ONLY"
        # convert '?' placeholders to :n binds
        n = 0
        while "?" in sql:
            n += 1
            sql = sql.replace("?", f":{n}", 1)
        return self._query(sql, params, kind="snapshot_chunk")

    def key_bounds(self, schema, table, key_columns):
        k = self.quote(key_columns[0])
        rows = self._query(f"SELECT MIN({k}) AS lo, MAX({k}) AS hi FROM {self.qualified(schema, table)}", kind="meta")
        return ((rows[0]["LO"],), (rows[0]["HI"],)) if rows else (None, None)
