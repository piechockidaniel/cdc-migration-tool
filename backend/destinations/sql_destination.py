"""Generic DB-API SQL destination with per-dialect upsert generation.

Dialects: ``mssql`` (pyodbc), ``postgresql`` (psycopg), ``mysql``/``mariadb``
(pymysql), ``oracle`` (oracledb) and ``sqlite`` (tests / local sinks).

All statements are parameterised - no value is ever interpolated into SQL
(the original multi-source engine did ``f"{col} = '{value}'"``).
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.connectors.base import ColumnRef
from backend.core import metrics

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dialect:
    name: str
    paramstyle: str            # qmark | format | numeric
    quote_open: str
    quote_close: str

    def quote(self, ident: str) -> str:
        return f"{self.quote_open}{ident.replace(self.quote_close, self.quote_close * 2)}{self.quote_close}"

    def qualified(self, schema: Optional[str], table: str) -> str:
        return f"{self.quote(schema)}.{self.quote(table)}" if schema else self.quote(table)

    def placeholder(self, index: int) -> str:
        if self.paramstyle == "qmark":
            return "?"
        if self.paramstyle == "format":
            return "%s"
        return f":{index}"

    def placeholders(self, n: int, start: int = 1) -> List[str]:
        return [self.placeholder(i) for i in range(start, start + n)]


DIALECTS: Dict[str, Dialect] = {
    "mssql": Dialect("mssql", "qmark", "[", "]"),
    "sqlserver": Dialect("mssql", "qmark", "[", "]"),
    "postgresql": Dialect("postgresql", "format", '"', '"'),
    "postgres": Dialect("postgresql", "format", '"', '"'),
    "mysql": Dialect("mysql", "format", "`", "`"),
    "mariadb": Dialect("mysql", "format", "`", "`"),
    "oracle": Dialect("oracle", "numeric", '"', '"'),
    "sqlite": Dialect("sqlite", "qmark", '"', '"'),
}


class SqlDestination:
    """Thread-safe wrapper over one DB-API connection."""

    def __init__(self, dialect: str, connection_factory: Callable[[], Any], name: str = "destination",
                 default_schema: Optional[str] = None):
        self.dialect = DIALECTS[dialect.lower()]
        self._factory = connection_factory
        self.name = name
        self.default_schema = default_schema
        self._conn = None
        self._lock = threading.RLock()
        self._pk_cache: Dict[str, List[str]] = {}
        self._col_cache: Dict[str, List[ColumnRef]] = {}

    # -- lifecycle ---------------------------------------------------------
    def connect(self) -> None:
        with self._lock:
            if self._conn is None:
                try:
                    self._conn = self._factory()
                except Exception:
                    metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                    raise

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
                self.query("SELECT 1" + (" FROM dual" if self.dialect.name == "oracle" else ""), kind="ping")
                return True
            except Exception:
                return False

    def _cursor(self):
        self.connect()
        return self._conn.cursor()

    def _params(self, params: Sequence[Any]) -> Any:
        return list(params) if self.dialect.paramstyle == "numeric" else tuple(params)

    # -- primitives --------------------------------------------------------
    def query(self, sql: str, params: Sequence[Any] = (), kind: str = "query") -> List[Dict[str, Any]]:
        with self._lock:
            with metrics.timed_query(self.name, kind):
                cur = self._cursor()
                try:
                    cur.execute(sql, self._params(params)) if params else cur.execute(sql)
                    if cur.description is None:
                        self._conn.commit()
                        return []
                    cols = [d[0] for d in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                    self._conn.commit()
                    return rows
                except Exception:
                    self._safe_rollback()
                    raise
                finally:
                    cur.close()

    def execute(self, sql: str, params: Sequence[Any] = (), kind: str = "exec", commit: bool = True) -> int:
        with self._lock:
            with metrics.timed_query(self.name, kind):
                cur = self._cursor()
                try:
                    cur.execute(sql, self._params(params)) if params else cur.execute(sql)
                    n = cur.rowcount
                    if commit:
                        self._conn.commit()
                    return n if n is not None else -1
                except Exception:
                    if commit:
                        self._safe_rollback()
                    raise
                finally:
                    cur.close()

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]], kind: str = "batch", commit: bool = True) -> int:
        if not rows:
            return 0
        with self._lock:
            with metrics.timed_query(self.name, kind):
                cur = self._cursor()
                try:
                    # pyodbc's fast_executemany relies on SQLDescribeParam, which SQL Server cannot do for
                    # parameters inside MERGE ... USING (SELECT ?) - only enable it for plain INSERTs.
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = sql.lstrip().upper().startswith("INSERT")
                    cur.executemany(sql, [self._params(r) for r in rows])
                    n = cur.rowcount
                    if commit:
                        self._conn.commit()
                    return n if n is not None and n >= 0 else len(rows)
                except Exception:
                    if commit:
                        self._safe_rollback()
                    raise
                finally:
                    cur.close()

    def begin(self) -> None:
        self.connect()
        self._lock.acquire()

    def commit(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._lock.release()

    def rollback(self) -> None:
        try:
            self._safe_rollback()
        finally:
            self._lock.release()

    def _safe_rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception:  # pragma: no cover
            pass

    # -- metadata ----------------------------------------------------------
    def get_columns(self, schema: Optional[str], table: str) -> List[ColumnRef]:
        key = f"{schema}.{table}"
        if key in self._col_cache:
            return self._col_cache[key]
        d = self.dialect.name
        cols: List[ColumnRef] = []
        if d == "sqlite":
            rows = self.query(f"PRAGMA table_info({self.dialect.quote(table)})", kind="meta")
            cols = [ColumnRef(r["name"], r["type"], not r["notnull"], bool(r["pk"]), int(r["cid"])) for r in rows]
        elif d == "mssql":
            rows = self.query(
                "SELECT c.name, ty.name AS data_type, c.is_nullable, c.column_id, "
                "CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk "
                "FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id "
                "JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id "
                "LEFT JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1 "
                "LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id "
                "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id", (schema or "dbo", table), kind="meta")
            cols = [ColumnRef(r["name"], r["data_type"], bool(r["is_nullable"]), bool(r["is_pk"]), int(r["column_id"])) for r in rows]
        elif d == "postgresql":
            rows = self.query(
                "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS data_type, NOT a.attnotnull AS nullable, a.attnum, "
                "EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY(i.indkey)) AS is_pk "
                "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                (schema or "public", table), kind="meta")
            cols = [ColumnRef(r["name"], r["data_type"], bool(r["nullable"]), bool(r["is_pk"]), int(r["attnum"])) for r in rows]
        elif d == "mysql":
            rows = self.query(
                "SELECT column_name, column_type, is_nullable, ordinal_position, column_key FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table), kind="meta")
            cols = [ColumnRef(r["column_name"], r["column_type"], r["is_nullable"] == "YES", r["column_key"] == "PRI", int(r["ordinal_position"])) for r in rows]
        elif d == "oracle":
            rows = self.query(
                "SELECT c.column_name, c.data_type, c.nullable, c.column_id, "
                "CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_pk FROM all_tab_columns c "
                "LEFT JOIN (SELECT cc.column_name FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name "
                "WHERE k.owner = :1 AND k.table_name = :2 AND k.constraint_type = 'P') pk ON pk.column_name = c.column_name "
                "WHERE c.owner = :3 AND c.table_name = :4 ORDER BY c.column_id",
                ((schema or "").upper(), table.upper(), (schema or "").upper(), table.upper()), kind="meta")
            cols = [ColumnRef(r["COLUMN_NAME"], r["DATA_TYPE"], r["NULLABLE"] == "Y", bool(r["IS_PK"]), int(r["COLUMN_ID"])) for r in rows]
        self._col_cache[key] = cols
        return cols

    def get_primary_key(self, schema: Optional[str], table: str) -> List[str]:
        key = f"{schema}.{table}"
        if key not in self._pk_cache:
            self._pk_cache[key] = [c.name for c in self.get_columns(schema, table) if c.is_primary_key]
        return self._pk_cache[key]

    # -- SQL generation ----------------------------------------------------
    def insert_sql(self, schema, table, columns: Sequence[str]) -> str:
        q = self.dialect
        return (f"INSERT INTO {q.qualified(schema, table)} ({', '.join(q.quote(c) for c in columns)}) "
                f"VALUES ({', '.join(q.placeholders(len(columns)))})")

    def delete_sql(self, schema, table, key_columns: Sequence[str]) -> str:
        q = self.dialect
        where = [f"{q.quote(k)} = {q.placeholder(i + 1)}" for i, k in enumerate(key_columns)]
        return f"DELETE FROM {q.qualified(schema, table)} WHERE {' AND '.join(where)}"

    def upsert_sql(self, schema, table, columns: Sequence[str], key_columns: Sequence[str]) -> Tuple[str, Callable[[Dict[str, Any]], List[Any]]]:
        """Return (sql, param_builder). ``param_builder(row)`` produces the positional parameters."""
        q = self.dialect
        cols = list(columns)
        non_keys = [c for c in cols if c not in key_columns]
        target = q.qualified(schema, table)
        d = q.name
        if d in ("postgresql", "sqlite"):
            sets = ", ".join(f"{q.quote(c)} = excluded.{q.quote(c)}" for c in non_keys) or f"{q.quote(key_columns[0])} = excluded.{q.quote(key_columns[0])}"
            sql = (f"INSERT INTO {target} ({', '.join(q.quote(c) for c in cols)}) VALUES ({', '.join(q.placeholders(len(cols)))}) "
                   f"ON CONFLICT ({', '.join(q.quote(k) for k in key_columns)}) DO UPDATE SET {sets}")
            return sql, lambda row: [row.get(c) for c in cols]
        if d == "mysql":
            sets = ", ".join(f"{q.quote(c)} = VALUES({q.quote(c)})" for c in non_keys) or f"{q.quote(key_columns[0])} = {q.quote(key_columns[0])}"
            sql = (f"INSERT INTO {target} ({', '.join(q.quote(c) for c in cols)}) VALUES ({', '.join(q.placeholders(len(cols)))}) "
                   f"ON DUPLICATE KEY UPDATE {sets}")
            return sql, lambda row: [row.get(c) for c in cols]
        # MERGE based dialects
        src_cols = ", ".join(f"{ph} AS {q.quote(c)}" for ph, c in zip(q.placeholders(len(cols)), cols))
        src = f"(SELECT {src_cols}{' FROM dual' if d == 'oracle' else ''}) src"
        on = " AND ".join(f"tgt.{q.quote(k)} = src.{q.quote(k)}" for k in key_columns)
        update = ", ".join(f"tgt.{q.quote(c)} = src.{q.quote(c)}" for c in non_keys) if d == "mssql" else \
            ", ".join(f"{q.quote(c)} = src.{q.quote(c)}" for c in non_keys)
        insert_cols = ", ".join(q.quote(c) for c in cols)
        insert_vals = ", ".join(f"src.{q.quote(c)}" for c in cols)
        hold = " WITH (HOLDLOCK)" if d == "mssql" else ""
        matched = f" WHEN MATCHED THEN UPDATE SET {update}" if non_keys else ""
        sql = (f"MERGE INTO {target}{hold} tgt USING {src} ON ({on}){matched} "
               f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")
        if d == "mssql":
            sql += ";"
        return sql, lambda row: [row.get(c) for c in cols]

    # -- row operations ----------------------------------------------------
    def insert_rows(self, schema, table, rows: Sequence[Dict[str, Any]], commit: bool = True) -> int:
        if not rows:
            return 0
        columns = list(rows[0].keys())
        sql = self.insert_sql(schema, table, columns)
        return self.executemany(sql, [[r.get(c) for c in columns] for r in rows], kind="insert", commit=commit)

    def upsert_rows(self, schema, table, rows: Sequence[Dict[str, Any]], key_columns: Sequence[str], commit: bool = True) -> int:
        if not rows:
            return 0
        columns = list(rows[0].keys())
        if self.dialect.name == "mssql" and len(rows) >= self.mssql_bulk_threshold:
            return self._mssql_bulk_upsert(schema, table, rows, columns, key_columns, commit)
        sql, builder = self.upsert_sql(schema, table, columns, key_columns)
        return self.executemany(sql, [builder(r) for r in rows], kind="upsert", commit=commit)

    mssql_bulk_threshold = 200

    def _mssql_type_strings(self, schema, table) -> Dict[str, str]:
        """Column -> full type declaration (varchar(50), decimal(18,2), datetime2(7) ...) for temp tables."""
        key = f"types:{schema}.{table}"
        cached = self._col_cache.get(key)
        if cached is not None:
            return cached  # type: ignore[return-value]
        rows = self.query(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema or "dbo", table), kind="meta")
        out: Dict[str, str] = {}
        for r in rows:
            t = r["DATA_TYPE"].lower()
            if t in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary"):
                n = r["CHARACTER_MAXIMUM_LENGTH"]
                t = f"{t}({'max' if n in (-1, None) else n})"
            elif t in ("decimal", "numeric"):
                t = f"{t}({r['NUMERIC_PRECISION']},{r['NUMERIC_SCALE']})"
            elif t in ("datetime2", "datetimeoffset", "time") and r["DATETIME_PRECISION"] is not None:
                t = f"{t}({r['DATETIME_PRECISION']})"
            out[r["COLUMN_NAME"]] = t
        self._col_cache[key] = out  # type: ignore[assignment]
        return out

    def _mssql_bulk_upsert(self, schema, table, rows, columns, key_columns, commit) -> int:
        """Large batches: fast_executemany INSERT into a session temp table, then ONE MERGE."""
        q = self.dialect
        types = self._mssql_type_strings(schema, table)
        missing = [c for c in columns if c not in types]
        if missing:
            raise RuntimeError(f"columns {missing} do not exist in {schema}.{table}")
        tmp = f"#cdcsync_{table}"[:116]
        ddl = f"CREATE TABLE {tmp} ({', '.join(f'{q.quote(c)} {types[c]} NULL' for c in columns)})"
        non_keys = [c for c in columns if c not in key_columns]
        on = " AND ".join(f"tgt.{q.quote(k)} = src.{q.quote(k)}" for k in key_columns)
        matched = f" WHEN MATCHED THEN UPDATE SET {', '.join(f'tgt.{q.quote(c)} = src.{q.quote(c)}' for c in non_keys)}" if non_keys else ""
        merge = (f"MERGE INTO {q.qualified(schema, table)} WITH (HOLDLOCK) tgt USING {tmp} src ON ({on}){matched} "
                 f"WHEN NOT MATCHED THEN INSERT ({', '.join(q.quote(c) for c in columns)}) "
                 f"VALUES ({', '.join('src.' + q.quote(c) for c in columns)});")
        with self._lock:
            self.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}", kind="upsert", commit=False)
            self.execute(ddl, kind="upsert", commit=False)
            try:
                self.executemany(self.insert_sql(None, tmp.lstrip("#"), columns).replace(q.quote(tmp.lstrip("#")), tmp, 1),
                                 [[r.get(c) for c in columns] for r in rows], kind="upsert", commit=False)
                n = self.execute(merge, kind="upsert", commit=False)
                self.execute(f"DROP TABLE {tmp}", kind="upsert", commit=False)
                if commit:
                    self._conn.commit()
                return n if n and n > 0 else len(rows)
            except Exception:
                if commit:
                    self._safe_rollback()
                raise

    def delete_rows(self, schema, table, keys: Sequence[Dict[str, Any]], key_columns: Sequence[str], commit: bool = True) -> int:
        if not keys:
            return 0
        sql = self.delete_sql(schema, table, key_columns)
        return self.executemany(sql, [[k.get(c) for c in key_columns] for k in keys], kind="delete", commit=commit)

    def row_count(self, schema, table) -> int:
        rows = self.query(f"SELECT COUNT(*) AS n FROM {self.dialect.qualified(schema, table)}", kind="count")
        return int(list(rows[0].values())[0]) if rows else 0

def create_destination(config: Dict[str, Any], name: str = "destination") -> SqlDestination:
    """Build a destination from a connection document (``engine`` selects the driver)."""
    from backend.storage.secrets import resolve_secrets
    cfg = resolve_secrets(config)
    engine = str(cfg.get("engine") or "mssql").lower()
    if engine in ("mssql", "sqlserver"):
        from backend.connectors.mssql import MSSQLConnector

        def factory():
            import pyodbc
            return pyodbc.connect(MSSQLConnector(cfg).connection_string(), autocommit=False)
        return SqlDestination("mssql", factory, name, cfg.get("schema", "dbo"))
    if engine in ("postgresql", "postgres"):
        from backend.connectors.postgresql import PostgresConnector

        def factory():
            import psycopg
            return psycopg.connect(PostgresConnector(cfg).dsn(), autocommit=False)
        return SqlDestination("postgresql", factory, name, cfg.get("schema", "public"))
    if engine in ("mysql", "mariadb"):
        def factory():
            import pymysql
            return pymysql.connect(host=cfg.get("server", cfg.get("host", "localhost")), port=int(cfg.get("port") or 3306),
                                   user=cfg.get("username", ""), password=cfg.get("password", ""), database=cfg.get("database"),
                                   autocommit=False)
        return SqlDestination("mysql", factory, name, cfg.get("database"))
    if engine == "oracle":
        from backend.connectors.oracle import OracleConnector

        def factory():
            import oracledb
            return oracledb.connect(user=cfg.get("username"), password=cfg.get("password"), dsn=OracleConnector(cfg).dsn())
        return SqlDestination("oracle", factory, name, cfg.get("schema", cfg.get("username")))
    if engine == "sqlite":
        def factory():
            import sqlite3
            return sqlite3.connect(cfg.get("path", ":memory:"), check_same_thread=False)
        return SqlDestination("sqlite", factory, name, None)
    raise ValueError(f"unsupported destination engine {engine!r}")
