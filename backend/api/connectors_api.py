"""Connector API: engines, prerequisite validation, capture management, metadata.

All endpoints accept either a saved ``connection_id`` or an inline
``connection`` document (so the UI can validate before saving).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.connectors import ENGINES, ConnectorError, create_connector
from backend.connectors.mssql import MSSQLConnector
from backend.core import runtime
from backend.storage.secrets import redact

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectorRequest(BaseModel):
    connection_id: Optional[str] = None
    connection: Optional[Dict[str, Any]] = None
    tables: List[str] = Field(default_factory=list, description='["schema.table", ...]')
    options: Dict[str, Any] = Field(default_factory=dict)


def _connector(req: ConnectorRequest):
    """Source connector, or a read-only metadata adapter for destination-only engines / ``side=destination``."""
    from backend.connectors.destination_adapter import DestinationAdapter
    if req.connection_id:
        try:
            cfg = runtime.connector_config_from(runtime.connection_document(req.connection_id))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    elif req.connection:
        cfg = runtime.connector_config_from(req.connection)
    else:
        raise HTTPException(status_code=400, detail="connection_id or connection is required")
    engine = str(cfg.get("engine") or "mssql").lower()
    if engine == "sqlite" or req.options.get("side") == "destination":
        try:
            return DestinationAdapter(cfg)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    try:
        return create_connector(cfg)
    except ConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _tables(req: ConnectorRequest) -> List[Tuple[str, str]]:
    out = []
    for t in req.tables:
        if "." not in t:
            raise HTTPException(status_code=400, detail=f"table {t!r} must be schema.table")
        s, n = t.split(".", 1)
        out.append((s, n))
    return out


async def _run(fn, *args):
    """Blocking driver calls never run on the event loop."""
    return await asyncio.to_thread(fn, *args)


@router.get("/engines")
async def list_engines():
    seen = {}
    for name, cls in ENGINES.items():
        if cls.engine not in seen:
            seen[cls.engine] = {"engine": cls.engine, "aliases": [], "capabilities": sorted(c.value for c in cls.capabilities)}
        seen[cls.engine]["aliases"].append(name)
    return list(seen.values())


@router.post("/test")
async def test_connector(req: ConnectorRequest):
    c = _connector(req)
    try:
        ok, message = await _run(c.test_connection)
        return {"success": ok, "message": message, "engine": c.engine, "capabilities": c.describe()["capabilities"],
                "config": redact(c.config)}
    finally:
        await _run(c.disconnect)


@router.post("/prerequisites")
async def check_prerequisites(req: ConnectorRequest):
    c = _connector(req)
    try:
        checks = await _run(c.check_prerequisites, _tables(req))
        failed = [ch for ch in checks if not ch.ok and ch.severity == "error"]
        warnings = [ch for ch in checks if not ch.ok and ch.severity == "warning"]
        result = {"engine": c.engine, "ready": not failed, "failed": len(failed), "warnings": len(warnings),
                  "checks": [ch.to_json() for ch in checks]}
        runtime.audit.record("prerequisite", "connectors", f"{c.engine} prerequisites: {len(failed)} failed, {len(warnings)} warnings",
                             severity="error" if failed else ("warning" if warnings else "info"), source=c.name, payload=result)
        return result
    finally:
        await _run(c.disconnect)


@router.post("/capture/status")
async def capture_status(req: ConnectorRequest):
    c = _connector(req)
    try:
        return await _run(c.capture_status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await _run(c.disconnect)


@router.post("/capture/enable-database")
async def enable_database(req: ConnectorRequest):
    c = _connector(req)
    try:
        ok, message = await _run(c.enable_capture_database)
        runtime.audit.record("config", "connectors", f"enable capture on database: {message}", severity="info" if ok else "error", source=c.name)
        if not ok:
            raise HTTPException(status_code=400, detail=message)
        return {"success": True, "message": message}
    finally:
        await _run(c.disconnect)


@router.post("/capture/enable-tables")
async def enable_tables(req: ConnectorRequest):
    c = _connector(req)
    results = []
    try:
        for schema, table in _tables(req):
            ok, message = await _run(lambda: c.enable_capture_table(schema, table, **req.options))
            runtime.audit.record("config", "connectors", f"enable capture {schema}.{table}: {message}",
                                 severity="info" if ok else "error", source=c.name, table=f"{schema}.{table}")
            results.append({"table": f"{schema}.{table}", "success": ok, "message": message})
        return {"success": all(r["success"] for r in results), "results": results}
    finally:
        await _run(c.disconnect)


@router.post("/capture/disable-tables")
async def disable_tables(req: ConnectorRequest):
    c = _connector(req)
    results = []
    try:
        for schema, table in _tables(req):
            ok, message = await _run(lambda: c.disable_capture_table(schema, table, **req.options))
            runtime.audit.record("config", "connectors", f"disable capture {schema}.{table}: {message}",
                                 severity="info" if ok else "error", source=c.name, table=f"{schema}.{table}")
            results.append({"table": f"{schema}.{table}", "success": ok, "message": message})
        return {"success": all(r["success"] for r in results), "results": results}
    finally:
        await _run(c.disconnect)


@router.post("/tables")
async def list_tables(req: ConnectorRequest):
    c = _connector(req)
    try:
        tables = await _run(c.list_tables)
        return [{"schema": t.schema, "table": t.table, "key": t.key, "cdc_enabled": t.cdc_enabled,
                 "row_estimate": t.row_estimate, "size_bytes": t.size_bytes, **t.extra} for t in tables]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await _run(c.disconnect)


@router.post("/columns")
async def list_columns(req: ConnectorRequest):
    c = _connector(req)
    out: Dict[str, Any] = {}
    try:
        for schema, table in _tables(req):
            cols = await _run(c.get_columns, schema, table)
            out[f"{schema}.{table}"] = [col.__dict__ for col in cols]
        return out
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await _run(c.disconnect)


@router.post("/position")
async def current_position(req: ConnectorRequest):
    c = _connector(req)
    try:
        pos = await _run(c.current_position)
        return {"position": pos.to_json() if pos else None}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await _run(c.disconnect)


@router.get("/mssql/service-broker-script")
async def service_broker_script(queue_name: str = "cdcsync_notify_queue", poll_ms: int = 500):
    """T-SQL for the optional server-side notifier (push mode for SQL Server)."""
    return {"sql": MSSQLConnector.install_service_broker_sql(queue_name, poll_ms),
            "usage": "Run once in the source database, then set connection.options.service_broker_queue = 'dbo.<queue_name>' "
                     "and schedule EXEC dbo.cdcsync_notifier as a SQL Agent job step."}
