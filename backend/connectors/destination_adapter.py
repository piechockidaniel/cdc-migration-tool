"""Read-only metadata adapter so destination-only engines (SQLite) and any SQL
destination can be browsed (tables / columns) through the connector API."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.connectors.base import ColumnRef, PrerequisiteCheck, TableRef
from backend.destinations.sql_destination import create_destination

_TABLE_QUERIES = {
    "sqlite": "SELECT 'main' AS s, name AS t FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    "mssql": "SELECT s.name AS s, t.name AS t FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 ORDER BY 1, 2",
    "postgresql": "SELECT schemaname AS s, tablename AS t FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1, 2",
    "mysql": "SELECT table_schema AS s, table_name AS t FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') ORDER BY 1, 2",
    "oracle": "SELECT owner AS s, table_name AS t FROM all_tables WHERE owner NOT IN ('SYS','SYSTEM') ORDER BY 1, 2",
}


class DestinationAdapter:
    engine = "destination"

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = str(config.get("id") or config.get("name") or "destination")
        self.dest = create_destination(config, name=self.name)
        self.engine = f"{self.dest.dialect.name} (destination)"

    def connect(self):
        self.dest.connect()

    def disconnect(self):
        self.dest.disconnect()

    def test_connection(self) -> Tuple[bool, str]:
        try:
            self.dest.connect()
            return True, f"Connected ({self.dest.dialect.name} destination)"
        except Exception as exc:
            return False, str(exc)

    def server_version(self) -> str:
        return self.dest.dialect.name

    def describe(self) -> Dict[str, Any]:
        return {"engine": self.engine, "name": self.name, "capabilities": ["destination"]}

    def list_tables(self) -> List[TableRef]:
        rows = self.dest.query(_TABLE_QUERIES[self.dest.dialect.name], kind="meta")
        out = []
        for r in rows:
            vals = list(r.values())
            out.append(TableRef(str(vals[0]), str(vals[1]), False))
        return out

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        return self.dest.get_columns(schema or None, table)

    def get_primary_key(self, schema: str, table: str) -> List[str]:
        return self.dest.get_primary_key(schema or None, table)

    def check_prerequisites(self, tables: Optional[Sequence[Tuple[str, str]]] = None) -> List[PrerequisiteCheck]:
        ok, msg = self.test_connection()
        checks = [PrerequisiteCheck("connection", ok, msg)]
        for s, t in tables or []:
            try:
                pk = self.get_primary_key(s, t)
                checks.append(PrerequisiteCheck(f"primary_key:{s}.{t}", bool(pk), f"pk={pk}", "Add a PK or set key_columns on the mapping (needed for upserts)"))
            except Exception as exc:
                checks.append(PrerequisiteCheck(f"table:{s}.{t}", False, str(exc)))
        return checks

    def capture_status(self):
        return {"cdc_enabled": False, "note": "destination connection - no change capture"}

    def enable_capture_database(self):
        return False, "destinations have no change capture"

    def enable_capture_table(self, _schema, _table, **_options):
        return False, "destinations have no change capture"

    def disable_capture_table(self, _schema, _table, **_options):
        return False, "destinations have no change capture"

    def current_position(self):
        return None
