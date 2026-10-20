"""FastAPI application setup."""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Startup / shutdown (replaces the deprecated ``on_event`` hooks)."""
    from backend.core import runtime
    from backend.api.monitoring import attach_bus_broadcaster

    logger.info("Application started")
    attach_bus_broadcaster()
    runtime.start_background()
    try:
        yield
    finally:
        logger.info("Application shutdown")
        runtime.shutdown()
        from backend.db.mssql_manager import connection_pool
        connection_pool.close_all()
        from backend.core.duckdb_processor import duckdb_processor
        duckdb_processor.close()


# Create FastAPI app
app = FastAPI(
    title="CDC Database Synchronizer",
    description="Strangler-fig data migration: change data capture from SQL Server, PostgreSQL, MariaDB/MySQL, "
                "MongoDB and Oracle with chunked snapshots, auditing and alerting",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS: '*' with credentials is rejected by browsers and is a foot-gun; restrict via env var.
_origins = [o.strip() for o in os.environ.get("CDCSYNC_CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files: /static = shared vendored assets (bootstrap, icons) + legacy pages, /app = the SPA
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
app.mount("/app", StaticFiles(directory="frontend/app"), name="app")
if Path("docs").is_dir():  # the markdown manuals, linked from the in-app help menu
    app.mount("/app-docs", StaticFiles(directory="docs"), name="app-docs")

# Setup templates
templates = Jinja2Templates(directory="frontend/templates")

# Import and include routers
from backend.api import admin, operations, monitoring  # noqa: E402
from backend.api import connectors_api, pipelines_api, audit_api  # noqa: E402

app.include_router(admin.router, prefix="/api/admin", tags=["Administration"])
app.include_router(operations.router, prefix="/api/operations", tags=["Operations"])
app.include_router(monitoring.router, prefix="/api/monitoring", tags=["Monitoring"])
app.include_router(connectors_api.router, prefix="/api/connectors", tags=["Connectors"])
app.include_router(pipelines_api.router, prefix="/api/pipelines", tags=["Pipelines"])
app.include_router(pipelines_api.snapshot_router, prefix="/api/snapshots", tags=["Snapshots"])
app.include_router(audit_api.audit_router, prefix="/api/audit", tags=["Audit"])
app.include_router(audit_api.alerts_router, prefix="/api/alerts", tags=["Alerts"])
app.include_router(audit_api.metrics_router, tags=["Metrics"])
app.include_router(audit_api.settings_router, prefix="/api/settings", tags=["Settings"])

# Import web routes
from backend.api import web_routes  # noqa: E402

app.include_router(web_routes.router)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    from backend.core import runtime
    active = [p.status() for p in runtime.pipelines.all()]
    return {
        "status": "healthy",
        "service": "cdc-synchronizer",
        "pipelines": {p["id"]: p["state"] for p in active},
        "alerts_active": len(runtime.alert_manager.active),
        "config_backend": runtime.config_manager.store.backend_name,
    }
