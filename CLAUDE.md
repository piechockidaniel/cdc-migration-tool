# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a CDC Database Synchronizer for strangler-fig migrations - a Python application that captures changes from SQL Server (CDC), PostgreSQL (logical slots), MariaDB/MySQL (binlog), MongoDB (change streams) and Oracle (LogMiner) and applies them to SQL destinations, with chunked resumable snapshots, an audit trail / dead-letter queue, rule-based alerting and Prometheus metrics. FastAPI backend + vanilla JS UI.

Documentation is split by audience: `docs/user-guide/` (operators), `docs/developer-guide/` (architecture, connectors, storage, audit internals, frontend, testing, history incl. the 1.x code review), `docs/reference/configuration.md`. Start at `docs/README.md`.

## Development Commands

### Running the Application

```bash
# Start the application (main entry point)
python main.py
```

The application starts on http://localhost:8000 with:
- Web UI: http://localhost:8000
- Swagger API docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Setup and Dependencies

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify setup (checks Python version, dependencies, ODBC driver)
python verify_setup.py
```

### Code Quality

```bash
# Format code (if black is installed)
black backend/

# Lint code (if pylint is installed)
pylint backend/
```

```bash
# Tests (see docs/developer-guide/06-testing.md)
pytest                 # unit + API tests, no databases required
pytest -m perf -s      # throughput benchmarks
pytest -m live         # live connector tests against docker/docker-compose.integration.yml
```

## Architecture Overview

### High-Level Architecture

2.0 components (preferred for new work):

```
backend/connectors/      SourceConnector ABC + mssql / postgresql / mariadb / mongodb / oracle (+ oracle_redo parser, watermark back-off)
backend/destinations/    SqlDestination (dialect-aware upserts for mssql / postgresql / mysql / oracle / sqlite)
backend/core/pipeline.py ChangePipeline: connector.stream_changes -> EventBus -> RowApplier -> commit positions (at-least-once)
backend/core/applier.py  mapping runtime, key resolution, column controls, DuckDB expressions, poison-row isolation, DLQ
backend/core/snapshot_manager.py  chunked keyset snapshots (resumable, throttled, windows, position hand-off)
backend/core/audit/      AuditStore (SQLite), failure pattern mining, rule engine, AlertManager + channels
backend/core/metrics.py  performance counters (Prometheus + JSON)
backend/core/event_bus.py in-process pub/sub, Kafka transport
backend/core/runtime.py  wiring of the singletons; start_background()/shutdown()
backend/storage/         ConfigStore (sqlite | json), secrets, legacy migration
backend/api/connectors_api.py, pipelines_api.py, audit_api.py   new routers (/api/connectors, /api/pipelines, /api/snapshots, /api/audit, /api/alerts, /metrics)
```

Legacy (1.x) layering, still present and bug-fixed, used by the Administration UI:

```
FastAPI Application (backend/api/app.py)
├── API Layer (backend/api/)
│   ├── admin.py - Connection, CDC, mapping, workflow management
│   ├── operations.py - Start/stop/pause/resume sync operations
│   ├── monitoring.py - Live events, statistics, history via WebSocket
│   └── web_routes.py - HTML template routes
├── Core Business Logic (backend/core/)
│   ├── CDC Processing: pipeline.py, applier.py, snapshot_manager.py (2.0)
│   ├── Multi-Source: multi_source_*.py (3 files)
│   ├── Data Processing: duckdb_processor.py, data_ingestion_engine.py
│   ├── Management: config_manager.py, mapping_manager.py, workflow_manager.py
│   └── Monitoring: audit/, metrics.py, history_manager.py (legacy), data_quality.py
├── Database Layer (backend/db/)
│   ├── mssql_manager.py - Connection pooling and MSSQL operations
│   └── cdc_operations.py - CDC-specific SQL operations
└── Models (backend/models/)
    ├── schemas.py - Core Pydantic models (connections, mappings, CDC events)
    └── workflow_schemas.py - Visual workflow models
```

### Critical Concepts

#### 1. Mapping Types

The system supports two mapping types (defined in `backend/models/schemas.py`):

- **TABLE Mapping**: Traditional table-to-table CDC synchronization with column mappings
- **SQL Mapping**: Query-based mappings that can combine multiple sources

#### 2. Multi-Source Synchronization

The application supports advanced multi-source patterns:

- **Multi-source mappings** can combine data from multiple tables using JOIN, UNION, INTERSECT operations
- **Merge operations** are defined in `MergeOperation` model with support for complex SQL patterns
- **Conflict resolution** strategies (LATEST_WINS, SOURCE_PRIORITY, CUSTOM_RULE)
- Implementation split across:
  - `multi_source_cdc_monitor.py` - Monitors multiple CDC sources
  - `multi_source_executor.py` - Executes merge patterns
  - `multi_source_sync_engine.py` - Applies merged changes to destination

#### 3. DuckDB In-Memory Processing

DuckDB is used for in-memory data transformations during sync:

- Managed by `backend/core/duckdb_processor.py`
- Scripts stored in `duckdb_scripts/` directory with templates for common patterns
- Supports custom SQL transformations via `duckdb_script_manager.py`
- Can be used for filtering, aggregation, enrichment, pivoting, and joins
- See `duckdb_scripts/README.md` for detailed transformation capabilities

#### 4. Visual Workflow Designer

The system includes a visual workflow designer for building data flows:

- **Workflow Manager** (`backend/core/workflow_manager.py`) - CRUD operations for workflows
- **Workflow Converter** (`backend/core/workflow_converter.py`) - Compiles visual workflows to mappings
- **Frontend UI** (`frontend/static/workflow-designer.js`, `visual-merge-builder.js`)
- Workflows stored in `workflows/visual/` with compiled versions in `workflows/compiled/`

#### 5. Data Ingestion & Pattern Detection

Advanced pattern detection for data quality issues:

- **Pattern types**: Vertical shift (column swaps), diacritic mismatches, case mismatches
- **Pattern detectors** implemented in `backend/core/data_ingestion_engine.py`
- **UI builder** at `frontend/static/data-ingestion.js`
- Used to detect and suggest fixes for common data quality problems

#### 6. Connection Library

Connections are managed centrally and reused across mappings:

- Stored in `config/connections.json` via `config_manager.py`
- Full CRUD API at `/api/admin/connection/*`
- See `docs/user-guide/03-connections.md`
- Frontend at `frontend/static/connections.js`

### Important Implementation Details

#### CDC Flow (2.0)

1. `POST /api/operations/start` (working set) or `POST /api/pipelines/{id}/start` builds a `ChangePipeline`
2. Reader thread: `connector.stream_changes()` (watermark back-off / Service Broker / binlog / change stream) publishes `ChangeEvent`s on the bus
3. Applier thread: batches -> `RowApplier.apply()` (upserts, DLQ for failures) -> `commit_positions()` (config store + `confirm_position`, e.g. slot advance)
4. Audit batch summaries, metrics, legacy history/WebSocket listeners
5. `AlertManager` evaluates rule sets every 30 s against `runtime.health_contexts()`

The 1.x polling monitor / sync engine were removed in 2.0.

#### Column Mapping Features

`ColumnMapping` model supports:
- **Simple 1:1 mapping**: source_column → destination_column
- **Many-to-one**: source_columns (list) → destination_column with transformation
- **Auto-generation**: AUTO_GENERATE modes (NONE, ON_INSERT, ON_INIT)
- **Transformation engines**: SQL (SQL Server native) or DUCKDB (in-memory)
- **Column control**: ignore_changes, default_value
- See `backend/models/schemas.py:93-114` for full definition

#### Primary Key Detection

`RowApplier.resolve_keys()`: `mapping.key_columns` -> destination PK from the catalog -> source PK mapped through the column mapping -> name heuristic (flagged as unsafe in the audit trail and by the `unsafe_key_heuristic` alert rule). Always set `key_columns` on mappings whose destination has no PK.

#### Configuration Persistence

`backend/storage`: transactional `ConfigStore` (SQLite WAL by default, `config/state.sqlite3`; `CDCSYNC_CONFIG_BACKEND=json` for the hardened file backend). Legacy `config/*.json` files are migrated on first start. `config_manager.py` keeps its old public API. Positions are generic (`get_position`/`set_position`). Secrets can be `${env:..}`/`${file:..}` references.

#### Frontend Architecture

2.0: single-page application in `frontend/app/` (Vue 3 global build + Bootstrap 5 + Chart.js, all vendored, ES modules, hash router, no build step). `js/api.js` (endpoints), `js/store.js` (global reactive state, toasts, confirm), `js/ui.js` (shared components), `js/pages/<page>.js` (one component per page, templates as strings). Served at `/`; static under `/app`. Browser tests in `tests/ui` (puppeteer-core + @sparticuz/chromium). See `docs/developer-guide/05-frontend.md`.

1.x pages (Jinja2 templates in `frontend/templates/`, vanilla JS in `frontend/static/`, jsPlumb workflow designer) are still served under `/legacy/*`.

### Code Modification Guidelines

#### When Working with Mappings

- Always check `mapping_type` (TABLE vs SQL) before processing
- For multi-source mappings, check if `sources` list exists (>1 source)
- Column mappings support both `source_column` (single) and `source_columns` (multiple)
- Test transformation expressions in DuckDB before deploying

#### When Working with CDC

- LSN state must be persisted via `config_manager.set_last_lsn()`
- CDC operation codes: 1=DELETE, 2=INSERT, 3/4=UPDATE (before/after)
- Always handle pause state - check `is_paused` before processing
- Event listeners are async - use `asyncio.iscoroutinefunction()` check

#### When Adding New Features

- API endpoints go in appropriate router: `admin.py`, `operations.py`, or `monitoring.py`
- Add Pydantic models to `schemas.py` or `workflow_schemas.py`
- Core business logic goes in `backend/core/`
- Frontend JavaScript follows pattern: one file per major feature
- Update `app.py` if adding new routers or startup/shutdown hooks

#### Database Connection Management

- Use `connection_pool` from `backend/db/mssql_manager.py`
- Don't create raw connections - use `connection_pool.get_source()` or `get_destination()`
- Connections support both Windows Auth and SQL Auth
- Always close connections in shutdown handlers (see `app.py:56-62`)

### Key Files to Understand

For CDC synchronization:
- `backend/core/pipeline.py` - reader/applier threads, position commits
- `backend/core/applier.py` - mapping runtime, key resolution, DuckDB transformations
- `backend/connectors/*.py` - per-engine change capture

For multi-source:
- `backend/core/multi_source_sync_engine.py` - Entry point
- `backend/core/multi_source_executor.py` - Merge pattern execution
- `backend/models/schemas.py:116-165` - Multi-source data models

For transformations:
- `backend/core/duckdb_processor.py` - DuckDB integration
- `backend/core/duckdb_script_manager.py` - Script management
- `duckdb_scripts/README.md` - Transformation documentation

For workflows:
- `backend/core/workflow_manager.py` - Workflow CRUD
- `backend/core/workflow_converter.py` - Visual → Mapping conversion
- `frontend/static/workflow-designer.js` - Visual designer UI

### Prerequisites & Environment

- **Python**: 3.9+ (verify_setup.py checks for 3.13+)
- **SQL Server**: 2016+ with CDC enabled
- **ODBC Driver**: Microsoft ODBC Driver 17+ for SQL Server
- **SQL Server Agent**: Must be running for CDC
- **Permissions**: Sysadmin privileges required to enable CDC

### Known Limitations

From README.md:439-445:
1. Primary key detection assumes columns contain "id" in name
2. Limited transformation support (SQL expressions in mappings)
3. Last-write-wins strategy for update conflicts
4. No automatic schema change handling
5. Web UI has no authentication (security concern)

### Security Considerations

- Store credentials as `${env:NAME}` / `${file:/path}` references; API responses are redacted
- No authentication on web UI (front it with a reverse proxy + SSO)
- CORS origins come from `CDCSYNC_CORS_ORIGINS` (default localhost only)
- Consider using Windows Auth over SQL Auth when possible
- Use SSL/TLS for database connections in production
