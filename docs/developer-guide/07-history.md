# 7. History

## 2.0 change log

* **Connectors** for PostgreSQL (logical slots), MariaDB/MySQL (binlog stream), MongoDB (change
  streams), Oracle (LogMiner) next to SQL Server; prerequisite checks with fix hints; demo engine.
* **Event-driven SQL Server reading**: single watermark query with adaptive back-off, one-shot
  probe of changed capture tables, optional Service Broker push; correct LSN semantics.
* **Pipelines** replace the 1.x monitor/sync engine: at-least-once position commits after apply,
  upserts, batch apply with poison-row isolation, real key resolution, column controls and DuckDB
  scripts actually executed.
* **Snapshot manager**: chunked keyset loads, resumable, throttled, maintenance windows,
  position hand-off, poison rows to the DLQ.
* **Audit store + dead-letter queue**, failure-pattern mining with one-click fixes, rule engine
  with five rule sets, webhook/e-mail/WebSocket channels, Prometheus metrics and sampled history.
* **Storage**: transactional SQLite store with versioning and history; hardened JSON backend;
  secret references; automatic migration of `config/*.json`.
* **UI**: new single-page application; 1.x pages kept under `/legacy`. Context help on every
  page and dialog, guided first-run tour (`help.js`), help menu with links to the manuals.
* **Tests**: 156 unit/API tests (86 % line coverage of `backend/`), live connector tests
  (docker-compose), perf benchmarks, browser tests (smoke, e2e scenario, tour).
* Removed dead 1.x code: polling monitor, sync engine, latency table writer, blocking snapshot
  service, unused helper module and a dozen unused methods (vulture-driven sweep).
* Fixed 1.x defects listed in the code review below (duplicate CDC reads, LSN committed before
  apply, guessed primary keys, blocking snapshots, non-atomic config files, shadowed
  `ColumnMapping`, broken workflow compiler, multi-source runtime could not start, password
  printed to stdout, …).

### Defects found while raising coverage to 86 %

Writing tests for the legacy services and API surfaced bugs that the 1.x pages had been hitting
silently:

* `/api/operations/workset/active` was shadowed by `/workset/{workset_id}` (always 404 "workset
  'active' not found") – route order fixed.
* The mapping validator rejected every multi-source mapping (no `source_table`) – it now
  validates the merge pattern instead.
* Multi-source start read `mapping.destination_connection`, an attribute that does not exist –
  the destination now comes from the `destination_connection_id` query parameter or the active
  workset.
* The visual workflow compiler produced a mapping without column mappings (rejected on save);
  it now emits direct mappings for the source columns and honours the Column Mapper node, and
  an explicit mapper entry is kept even when the same column also feeds a transformation.
* `DataIngestionEngine` passed a raw document where a `ConnectionConfig` was expected –
  shared `connection_config_from_document` helper.
* `MongoConnector.stream_changes` let a failure to *open* the change stream escape as a bare
  driver exception; it is now a `ConnectorError` like every other connector failure, so the
  pipeline marks the state correctly.
* Swap/diacritic detectors in the data-quality checks produced false positives on short values.

## 1.x baseline (1.0.0, 2025-11-04)

Single-engine (SQL Server → SQL Server) synchroniser with polling CDC monitor, JSON-file
configuration, working sets, column mappings with DuckDB/SQL transformations (stored, partly
executed), visual workflow designer, multi-source merge builder, data-ingestion pattern detectors.

## Code review of the 1.x code base (baseline commit `ac6e62f`)

Scope: the complete Python backend (`backend/`), entry points, persistence and the
runtime behaviour that matters for a strangler-fig migration off a 16 TB,
undocumented SQL Server instance. Frontend JS was reviewed for API contract
compatibility only.

Severity legend: **S1** data loss / corruption / security · **S2** outage or
unbounded resource usage under production volume · **S3** correctness bug with a
workaround · **S4** quality / maintainability.

Each finding states what was done in this iteration (✅ fixed, ♻️ superseded by a
new component, ⚠️ mitigated, 📝 documented only).

---

### 1. CDC correctness (S1)

| #    | File                                                          | Finding                                                                                                                                                                                                                                                                                                                                                                                                                                             | Status                                                                                                                                                                   |
|------|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.1  | `core/cdc_monitor.py:_poll_table_changes`                     | `from_lsn` is the **last processed** LSN and `cdc.fn_cdc_get_all_changes_*` is *inclusive* of `from_lsn`. Every poll re-reads and re-applies the last transaction (duplicate inserts → PK violations, or duplicated rows when no PK). `sys.fn_cdc_increment_lsn` exists in `cdc_operations.py` but was never called.                                                                                                                                | ✅ fixed in legacy monitor; ♻️ `MSSQLConnector.read_changes` uses `increment_lsn` and (LSN, seqval) positions                                                             |
| 1.2  | `core/cdc_monitor.py:_save_lsn_state`                         | LSN is persisted **before** the events are applied (right after enqueueing). A crash/restart between enqueue and apply silently loses every queued event: *at-most-once*.                                                                                                                                                                                                                                                                           | ♻️ `ChangePipeline.commit_positions` commits only after apply/DLQ (*at-least-once* + idempotent upserts)                                                                 |
| 1.3  | `core/cdc_monitor.py`, `multi_source_cdc_monitor.py`          | `to_lsn <= from_lsn` check and `fn_cdc_get_max_lsn` called per table per poll; with N tables that is 2N+ round-trips every 5 s regardless of activity – exactly the "hammering" the architect wants to avoid.                                                                                                                                                                                                                                       | ♻️ single watermark query + one-shot probe (`changed_capture_instances`), adaptive back-off, optional Service Broker push                                                |
| 1.4  | `core/sync_engine.py:_get_primary_key_columns`                | Key columns are guessed with `'id' in column_name.lower()` → `Paid`, `Valid`, `Width`, `Provider`, `Video` all become "keys". UPDATE/DELETE then hit the wrong rows or nothing. Same heuristic in `latency_monitor.py` and `data_quality.py`.                                                                                                                                                                                                       | ♻️ `RowApplier.resolve_keys`: mapping `key_columns` → destination PK (catalog) → source PK → heuristic **flagged as unsafe** (audit + alert rule `unsafe_key_heuristic`) |
| 1.5  | `core/sync_engine.py:_apply_change`                           | CDC op codes 3 and 4 both map to UPDATE; with `row_filter='all update old'` the *before* image would be applied as an update. Works only because the filter is hard-coded to `'all'`.                                                                                                                                                                                                                                                               | ✅ connector pairs op 3 (before) with op 4 (after) explicitly                                                                                                             |
| 1.6  | `core/sync_engine.py`                                         | No idempotency: an INSERT replay (after restart, after 1.1) fails with PK violation and the row is lost; no dead-letter queue, failures only counted.                                                                                                                                                                                                                                                                                               | ♻️ upsert semantics (`MERGE`/`ON CONFLICT`/`ON DUPLICATE KEY`), DLQ with replay                                                                                          |
| 1.7  | `core/snapshot_service.py`                                    | `TRUNCATE` destination then `SELECT *` with a single server-side cursor: the destination is empty for the whole load, no resumability, no throttling, no consistency point with CDC (changes during the snapshot are lost or duplicated depending on when the monitor starts). On 16 TB this is a multi-day shared-lock scan. Also `_transform_batch` types every column as `VARCHAR/DOUBLE` – numerics become floats, `decimal` precision is lost. | ♻️ `SnapshotManager` (keyset chunks, upserts, resumable, throttled, windows, position hand-off); typed DuckDB staging (`infer_duckdb_type`)                              |
| 1.8  | `core/multi_source_sync_engine.py:_get_primary_key_condition` | SQL built by string interpolation `f"{col} = '{value}'"` – SQL injection via source data, breaks on quotes, dates, binary.                                                                                                                                                                                                                                                                                                                          | ✅ parameterised                                                                                                                                                          |
| 1.9  | `core/multi_source_sync_engine.py:_process_event_batch`       | Join key hard-coded to `customer_id`.                                                                                                                                                                                                                                                                                                                                                                                                               | ✅ uses `mapping.key_columns` / first output column                                                                                                                       |
| 1.10 | `core/multi_source_sync_engine.py:_apply_merged_change`       | Re-executes the **full merge query over all rows** for every single event (O(N) per event, O(N·M) per batch).                                                                                                                                                                                                                                                                                                                                       | 📝 documented; the multi-source path needs a redesign (key-filtered merge query) – out of scope for this iteration                                                       |
| 1.11 | `core/multi_source_cdc_monitor.py:_create_connections`        | `MSSQLConnection(conn_config)` receives a **dict** from the connection library; `MSSQLConnection` expects `ConnectionConfig` → `AttributeError: 'dict' has no attribute 'driver'`. `connection.close()` does not exist (`disconnect()`). The whole multi-source runtime cannot start.                                                                                                                                                               | ✅ fixed                                                                                                                                                                  |
| 1.12 | `core/multi_source_sync_engine.py:_record_latency`            | Calls `latency_monitor.record_latency(source_table=..., destination_table=..., latency_ms=...)` – a signature that does not exist; raised `TypeError` on every merged row (swallowed by `except`).                                                                                                                                                                                                                                                  | ✅ replaced by the `replication_lag_seconds` gauge                                                                                                                        |
| 1.13 | `models/schemas.py`                                           | `ColumnMapping` is defined **twice**; the second (ingestion) definition shadows the first for every `from backend.models.schemas import ColumnMapping`, so `MappingManager.auto_map_columns` and `WorkflowConverter._node_to_column_mapping` raise `ValidationError: name Field required`. The visual workflow compiler was broken.                                                                                                                 | ✅ renamed to `IngestionColumnMapping`                                                                                                                                    |
| 1.14 | `models/schemas.py`                                           | `ignore_changes`, `default_value`, `auto_generate*` are validated in `MappingManager` but **never applied** anywhere in the runtime.                                                                                                                                                                                                                                                                                                                | ✅ implemented in `RowApplier.transform_rows`                                                                                                                             |

### 2. Blocking I/O on the event loop (S2)

| #   | File                                         | Finding                                                                                                                                                                                                         | Status                                                                                                    |
|-----|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| 2.1 | `api/operations.py:start_synchronization`    | `snapshot_service.perform_snapshot` (hours for a large table) runs synchronously inside an `async def` route – the *entire* FastAPI process (UI, health checks, every other request) freezes until it finishes. | ♻️ snapshot jobs run in worker threads; the route returns immediately with job ids                        |
| 2.2 | `core/cdc_monitor.py`, `core/sync_engine.py` | All pyodbc calls run directly on the asyncio loop (`await asyncio.sleep` between them does not help). Every poll and every destination write blocks the API.                                                    | ♻️ pipeline reader/applier are threads; new API routes use `asyncio.to_thread`                            |
| 2.3 | `db/mssql_manager.py`                        | `Connection Timeout=0` = wait forever; a hung server hangs the caller indefinitely.                                                                                                                             | ✅ 15 s default, configurable (`login_timeout`)                                                            |
| 2.4 | `db/mssql_manager.py`                        | One shared `pyodbc.Connection` per role used from multiple coroutines/threads without a lock (pyodbc connections are not thread-safe for concurrent cursors).                                                   | ⚠️ connectors/destinations serialise access with an `RLock`; legacy class unchanged (single-threaded use) |

### 3. Persistence & concurrency (S1/S2) – see `03-storage.md`

| #   | File                                      | Finding                                                                                                                                                                                                                                | Status                                                                                                  |
|-----|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| 3.1 | `core/config_manager.py`                  | `open(path, 'w'); json.dump(...)` – non-atomic. A crash or a full disk mid-write truncates `mappings.json`/`worksets.json` to garbage; on next start `load_*` swallows the error and the app comes up **with an empty configuration**. | ♻️ SQLite (WAL) store; JSON backend rewritten with temp-file + fsync + rename                           |
| 3.2 | `core/config_manager.py`                  | No locking: two API calls (or two processes with `uvicorn --workers`) do read-modify-write on the in-memory dict and the last writer wins silently.                                                                                    | ♻️ transactional store + optimistic versioning (`ConflictError`)                                        |
| 3.3 | `core/config_manager.py:set_last_lsn`     | Rewrites `lsn_state.json` on every poll cycle for every table (fsync storm; on network filesystems a real outage risk).                                                                                                                | ♻️ positions are single-row updates in SQLite                                                           |
| 3.4 | `core/history_manager.py:flush_buffer`    | Loads the whole day's JSON array, appends, rewrites → O(n²) per day, multi-GB memory at bank volumes, and `query_events` loads everything to return 100 rows.                                                                          | ✅ append-only JSON Lines; ♻️ `AuditStore` (SQLite, indexed)                                             |
| 3.5 | `core/config_manager.py:_needs_migration` | Reads `mappings.json` twice, decides "unified" from the *first* document only; a bare `except: pass` hides parse errors.                                                                                                               | ♻️ replaced by the store migration (`storage/factory.py`)                                               |
| 3.6 | `models/schemas.py:WorkingSet`            | Embeds full `ConnectionConfig` **including passwords** in `worksets.json` in addition to `connections.json`.                                                                                                                           | ⚠️ secret references (`${env:..}`, `${file:..}`) + redaction added; schema kept for UI compatibility 📝 |

### 4. Security (S1)

| #   | File                                           | Finding                                                                                                                                                                                                                                    | Status                                                                                      |
|-----|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| 4.1 | `db/mssql_manager.py:_build_connection_string` | `print(result)` writes the full connection string **including the password** to stdout on every connection.                                                                                                                                | ✅ removed                                                                                   |
| 4.2 | `api/app.py`                                   | CORS `allow_origins=["*"]` together with `allow_credentials=True`.                                                                                                                                                                         | ✅ origins from `CDCSYNC_CORS_ORIGINS`                                                       |
| 4.3 | multiple                                       | Identifiers (`schema.table`, column names) interpolated unquoted into SQL everywhere (`DELETE FROM {schema}.{table}`, `TRUNCATE TABLE {schema}.{table}`). Any object name with a space/bracket breaks; combined with 1.8 it is injectable. | ♻️ dialect-aware quoting in connectors/destinations; legacy call sites quoted where touched |
| 4.4 | UI                                             | No authentication (known limitation).                                                                                                                                                                                                      | 📝 unchanged – put behind a reverse proxy with SSO                                          |

### 5. Metadata correctness (S3)

| #   | File                                       | Finding                                                                                                                                                                                                                                                                                                                         | Status                                                                                   |
|-----|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| 5.1 | `db/mssql_manager.py:get_tables`           | Joins `sys.change_tracking_tables` to compute `cdc_enabled`. That view is **Change Tracking**, a different feature. Tables with CDC show as not enabled and vice-versa. `sys.tables.is_tracked_by_cdc` is the right column. Also `LEFT JOIN sys.partitions` without aggregation duplicates rows for partitioned/indexed tables. | ✅ fixed                                                                                  |
| 5.2 | `db/cdc_operations.py:get_cdc_changes`     | Capture instance always `schema_table`; custom capture instances (common when CDC was re-enabled after DDL) are ignored.                                                                                                                                                                                                        | ✅ `capture_instances` override in the connector                                          |
| 5.3 | `api/admin.py:test_connection`             | Calls `get_server_version()` *after* `test_connection()` disconnected → a second connect/disconnect per test.                                                                                                                                                                                                                   | 📝 minor                                                                                 |
| 5.4 | `core/data_quality.py:_get_random_samples` | Comment says TABLESAMPLE, code does `ORDER BY NEWID()` – a full scan + sort of the whole table for 100 samples.                                                                                                                                                                                                                 | ✅ TABLESAMPLE with TOP fallback                                                          |
| 5.5 | `core/latency_monitor.py`                  | Creates a table in the **destination** database and inserts one row per replicated change (doubles write load, touches a schema we do not own).                                                                                                                                                                                 | ✅ removed in 2.0; `replication_lag_seconds` / `apply_latency_seconds` metrics replace it |
| 5.6 | `core/duckdb_processor.py`                 | Identifiers unquoted → any column named like a DuckDB keyword (`order`, `group`, `user`) breaks staging. Not thread-safe.                                                                                                                                                                                                       | ✅ quoted, `RLock`                                                                        |

### 6. Observability (S2)

* No performance counters at all; `get_statistics()` derives events/s from uptime. → `backend/core/metrics.py` (counters, gauges, histograms with p50/p95/p99, Prometheus + JSON exposition), instrumented across connectors, applier, snapshots, store, alerting.
* Failures only incremented a counter – no payload, no pattern. → DLQ + pattern mining.
* No alerting. → rule engine + rule sets + channels.

### 7. Quality (S4)

* `api/app.py` uses deprecated `@app.on_event` → lifespan.
* `verify_setup.py` required Python 3.13 while README says 3.9+ → 3.10.
* Mixed CRLF/LF line endings → normalised, `.gitattributes` added.
* `requirements.txt` pins `fastapi==0.124.0` while a stray `requests` line sits at the top; drivers for the new engines added.
* `SourceConfig.schema` shadows `BaseModel.schema` (pydantic warning on every import) → warning filtered (field name is part of the UI contract).
* Tests: none existed. → 100 unit/API tests + live connector tests + perf benchmarks (`06-testing.md`).

### 8. Things that are fine

* The unified `Mapping` model and the migration from legacy mapping files is reasonable.
* DuckDB for expression evaluation is a good fit (vectorised, SQL, no extra service).
* The visual workflow compiler and ingestion pattern detectors are self-contained and reusable.

### 9. Not addressed (explicitly)

* Multi-source merge engine performance (1.10) – needs key-filtered merge queries.
* UI authentication (4.4).
* Destination connectors other than SQL (graph / document sinks) – the `SqlDestination` interface is the extension point.

### Addendum (found while wiring the UI)

| #   | File                                             | Finding                                                                                                                                                                                                                                         | Status                                                                        |
|-----|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| A.1 | `api/admin.py:save_connection/update_connection` | `ConnectionConfig` had no `id` field, so the `connection_data.get('id', <slug>)` fallback *always* used the name slug and a custom id entered in the UI was silently ignored (and updates could target a different key than the one displayed). | ✅ `id` added to the model                                                     |
| A.2 | `api/admin.py:list_connections/get_connection`   | Passwords returned verbatim to the browser.                                                                                                                                                                                                     | ✅ redacted; the edit form sends `***` back and the stored secret is preserved |
| A.3 | `api/monitoring.py:get_statistics`               | Read `sync_engine` counters which are always zero once the new pipeline is used.                                                                                                                                                                | ✅ aggregates running pipelines                                                |


## IDE inspection triage (5,898 items)

Source: JetBrains "Inspection Results" export (`problems.html`), taken before commit `20aac29`.
Items were parsed per inspection and handled as follows.

### Real defects – fixed

| Inspection                         | Finding                                                                                                                                                             | Fix                                                                                                                                  |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| Incorrect docstring / non-callable | `WorkflowConverter.compile_workflow(self, workflow, Mappping=None)` then `Mappping(...)` → every visual-workflow compile raised `'NoneType' object is not callable` | stray parameter removed, `Mapping` used; the CONCAT node now also emits a real `concat_ws` transformation instead of an unused local |
| Annotator (HTML)                   | two stray `</div>` in `frontend/templates/admin.html` (browsers tolerated it)                                                                                       | removed; all four legacy templates now parse balanced                                                                                |
| Unbound local variable             | `multi_source_executor.execute_merge_preview` referenced `query` in the error branch via `locals()`                                                                 | `query` initialised before the `try`                                                                                                 |
| Unbound local variable             | `oracle.read_changes`: `rows` only assigned inside `try`                                                                                                            | initialised before the block                                                                                                         |
| Deprecated                         | `pydantic .dict()` (5 call sites in `admin.py`)                                                                                                                     | `model_dump()`                                                                                                                       |
| Annotator (Markdown)               | JSON code fences containing `{ ... }` placeholders, a shell command inside a ```python fence                                                                        | fences retyped (`jsonc` / `bash`)                                                                                                    |
| Unresolved file references         | relative links in `docs/CHANGELOG.md`, `docs/QUICKSTART.md` pointed at the wrong directory                                                                          | fixed                                                                                                                                |

### Code quality – fixed

* **Unused imports / locals** in backend and tests (all listed ones); `pyflakes backend tests` is now clean.
* **Shadowing**: `lifespan(app)`, `Subscription(bus)`, inner `ok` in the prerequisite helpers, `exc` in nested handlers, `labels` in the Prometheus exporter – renamed.
* **Protected member access** from other modules: `MetricsRegistry.iter_series` and `rules.FUNCTIONS` are now public.
* **`__dataclass_fields__`** → `dataclasses.fields()` (alerting, rules, snapshot jobs).
* **Inconsistent return statements / Optional types** in `AuditStore`: hand-written context manager replaced by `contextlib.nullcontext`, `lastrowid`/`rowcount` coerced.
* **Type hints**: `List[callable]` → `List[Callable]`, `parse_lsn -> Optional[bytes]`, `_update_row(key_condition: Tuple[str, list])`, typed `_BIN_OPS`/`_CMP_OPS`, `build_channels() -> List[AlertChannel]`, `Dict[str, Any]` for `to_dict`, `Iterator[...]` return types on `@contextmanager` methods (also removes the "parameter unfilled" false positives).
* **Instance attributes outside `__init__`** (`_health_conn`, `_stream_positions`) – declared / removed.
* `Field(None, …)` → `Field(default=None, …)` in `schemas.py` so IDE plugins recognise the defaults (the "parameter unfilled" warnings on `Mapping(...)` in tests).
* `Operation[op_name]` → `Operation(op_name)`, redundant parentheses, static helpers (`_pipeline`, `_table_filter`, `_mark_mapping_completed`), BSON imports moved to module level with snake-case names, f-strings without placeholders, unused `global` statements.
* **Accessibility** ("Missing associated label", 117): `aria-label` added to 96 unlabeled inputs/selects in the legacy templates (derived from placeholder / title / id).
* **Markdown tables** ("not correctly formatted"): tables in the 2.0 docs re-aligned.

### Not applicable / environment

| Inspection                                                            | Why it is not a code problem                                                                                                                                                                                                                |
|-----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Unsatisfied package requirements / Requirement is not satisfied (109) | the IDE interpreter has no packages installed – run `pip install -r requirements.txt` in the project venv and point the IDE at it                                                                                                           |
| Unknown attribute / Unknown tag in `frontend/app/index.html` (37)     | Vue directives (`v-if`, `:class`, `@click`, `<component>`) – needs the Vue plugin (bundled with PyCharm Professional / WebStorm)                                                                                                            |
| Unresolved file in a link (182)                                       | `/static/...` and `/legacy/...` are FastAPI mounts/routes, not files relative to the template; mark `frontend/static` as a Resource Root in the IDE if desired                                                                              |
| Spelling (5,080)                                                      | 4,705 are in vendored Bootstrap / jsPlumb / flowcanvas. The remaining ones are domain terms (`lsn`, `seqval`, `binlog`, `wal2json`, `LogMiner`, `cdcsync` …) – a project dictionary is now committed in `.idea/dictionaries/project.xml`    |
| Style / Grammar (52)                                                  | British vs. American spelling in comments and docs; left as is                                                                                                                                                                              |
| Too broad exception clause (52)                                       | deliberate at driver boundaries (connectors, destinations, pipeline loops): driver exception hierarchies differ per engine and the loops must survive any failure; every such clause logs, counts (`connection_errors_total`) and backs off |
| Method may be static (40)                                             | interface implementations (`SourceConnector`, `DestinationAdapter`) and legacy 1.x classes; the ones that are genuinely free functions were converted                                                                                       |
| Obsolete attribute / Unused schema declaration                        | vendored Bootstrap READMEs and the icon sprite                                                                                                                                                                                              |
| Incorrectly numbered list item (16)                                   | 1.x documentation (`COLUMN_CONTROLS_SUMMARY.md`, `CONNECTION_LIBRARY_COMPLETE.md`, …) – cosmetic                                                                                                                                            |

Re-running the inspection after these changes should leave only the environment and vendored categories.
