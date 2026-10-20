"""Connector registry.

``create_connector({"engine": "postgresql", ...})`` returns the right
implementation. The ``engine`` key defaults to ``mssql`` so existing
connection documents (which never had one) keep working.
"""
from __future__ import annotations

from typing import Any, Dict, Type

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, ConnectorError, Operation, Position,
    PrerequisiteCheck, SourceConnector, TableRef,
)
from backend.connectors.demo import DemoConnector
from backend.connectors.mariadb import MariaDBConnector
from backend.connectors.mongodb import MongoConnector
from backend.connectors.mssql import MSSQLConnector
from backend.connectors.oracle import OracleConnector
from backend.connectors.postgresql import PostgresConnector

ENGINES: Dict[str, Type[SourceConnector]] = {
    "mssql": MSSQLConnector,
    "sqlserver": MSSQLConnector,
    "postgresql": PostgresConnector,
    "postgres": PostgresConnector,
    "mariadb": MariaDBConnector,
    "mysql": MariaDBConnector,
    "mongodb": MongoConnector,
    "mongo": MongoConnector,
    "oracle": OracleConnector,
    "demo": DemoConnector,       # synthetic change generator (UI / smoke tests)
}


def create_connector(config: Dict[str, Any], **kwargs: Any) -> SourceConnector:
    from backend.storage.secrets import resolve_secrets
    engine = str(config.get("engine") or "mssql").lower()
    cls = ENGINES.get(engine)
    if cls is None:
        raise ConnectorError(f"unsupported engine {engine!r}; known: {sorted(set(ENGINES))}")
    return cls(resolve_secrets(config), **kwargs)


__all__ = [
    "Capability", "ChangeEvent", "ColumnRef", "ConnectorError", "Operation", "Position", "PrerequisiteCheck",
    "SourceConnector", "TableRef", "ENGINES", "create_connector",
    "MSSQLConnector", "PostgresConnector", "MariaDBConnector", "MongoConnector", "OracleConnector",
]
