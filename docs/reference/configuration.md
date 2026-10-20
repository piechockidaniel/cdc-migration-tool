# Configuration reference

## Environment variables

| Variable                                           | Default                                       | Purpose                                                         |
|----------------------------------------------------|-----------------------------------------------|-----------------------------------------------------------------|
| `CDCSYNC_CONFIG_DIR`                               | `config`                                      | directory of the configuration store (`state.sqlite3`)          |
| `CDCSYNC_CONFIG_BACKEND`                           | `sqlite`                                      | `sqlite` or `json` (hardened file backend, see developer guide) |
| `CDCSYNC_HISTORY_DIR`                              | `history`                                     | audit store (`audit.sqlite3`) and 1.x history files             |
| `CDCSYNC_AUDIT_LEVEL`                              | `summary`                                     | `all` writes one audit row per change event                     |
| `CDCSYNC_ALERT_INTERVAL`                           | `30`                                          | seconds between rule evaluations                                |
| `CDCSYNC_ALERT_WEBHOOK`                            | –                                             | webhook URL for alerts (can also be set in the UI)              |
| `CDCSYNC_MAX_PARALLEL_SNAPSHOTS`                   | `2`                                           | snapshot jobs running concurrently                              |
| `CDCSYNC_KAFKA_BOOTSTRAP` / `CDCSYNC_KAFKA_PREFIX` | – / `cdcsync.`                                | mirror change events to Kafka (needs `confluent-kafka`)         |
| `CDCSYNC_CORS_ORIGINS`                             | `http://localhost:8000,http://127.0.0.1:8000` | allowed browser origins                                         |
| `CDCSYNC_DISABLE_BACKGROUND`                       | –                                             | `1` keeps the alert loop / auto-start off (tests)               |

## Connection documents

Common fields: `id`, `name`, `engine`, `server`, `port`, `database`, `username`, `password`
(literal or `${env:NAME}` / `${file:/path}` / `${vault:ref}`), `use_windows_auth` (SQL Server),
`driver` (ODBC driver name), `login_timeout` (s), `read_only_intent`, `options` (engine specific):

| Engine              | `options` keys                                                                                                                                                                                                                                                             |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mssql`             | `capture_instances` `{"schema.table": "instance"}`, `service_broker_queue`, `row_filter` (`all` \| `all update old`), `poll_min_seconds` (0.25), `poll_max_seconds` (30), `batch_limit` (5000), `snapshot_hint`, `encrypt`, `trust_server_certificate`, `application_name` |
| `postgresql`        | `slot_name` (`cdcsync_<id>`), `plugin` (`wal2json` \| `test_decoding`), `notify_channel`, `sslmode`, `batch_limit`, `poll_min_seconds`, `poll_max_seconds`                                                                                                                 |
| `mariadb` / `mysql` | `server_id` (4379), `batch_limit`                                                                                                                                                                                                                                          |
| `mongodb`           | `uri`, `auth_database`, `options` (connection-string options), `full_document` (`updateLookup`), `full_document_before_change` (`whenAvailable`), `max_await_ms`, `schema_sample`                                                                                          |
| `oracle`            | `service_name` or `dsn`, `schema` (owner), `supplemental_mode` (`ALL` \| `PRIMARY KEY`), `mode` (`SYSDBA`), `max_scn_window`, `batch_limit`, `poll_min_seconds`, `poll_max_seconds`                                                                                        |
| `sqlite`            | `path`                                                                                                                                                                                                                                                                     |
| `demo`              | `rate_per_second` (5), `initial_rows` (5000), `poison_ratio` (0), `seed`                                                                                                                                                                                                   |

## Mapping

```json
{"id": "crm_customer", "name": "CRM customers", "mapping_type": "table", "enabled": true,
 "source_schema": "dbo", "source_table": "Customer",
 "destination_schema": "public", "destination_table": "customer",
 "column_mappings": [
   {"source_column": "CustomerID", "destination_column": "id"},
   {"source_column": "Name", "destination_column": "name", "default_value": "unknown"},
   {"source_columns": ["FirstName", "LastName"], "destination_column": "full_name",
    "transformation": "concat_ws(' ', \"FirstName\", \"LastName\")"},
   {"destination_column": "loaded_at", "auto_generate": "on_insert", "auto_generate_expression": "now()"},
   {"source_column": "Notes", "destination_column": "notes", "ignore_changes": true}],
 "key_columns": ["id"], "sync_inserts": true, "sync_updates": true, "sync_deletes": true,
 "sync_mode": "upsert", "batch_size": 1000, "perform_initial_snapshot": false,
 "use_duckdb_transformation": false, "duckdb_script_name": null, "duckdb_script_content": null}
```

SQL mappings: `mapping_type: "sql"`, `source_query`, `key_columns`, `sync_mode` (`full` \|
`incremental` \| `upsert`), `sync_schedule` (cron, informational). Multi-source mappings add
`is_multi_source`, `sources[]` (`id`, `connection_id`, `alias`, `schema`, `table`, `where_clause`),
`merge_pattern` (`operations[]`, `output_columns[]`), `conflict_resolution`, `source_priority`.

## Pipeline definition

`POST /api/pipelines` – `id`, `source_connection_id`, `destination_connection_id`, `mapping_ids[]`
or `workset_id`, `auto_start`, `options`: `initial_position` (`earliest` \| `latest`),
`insert_mode` (`upsert` \| `insert`), `batch_size` (500), `batch_wait_s` (0.5), `expect_traffic`.

## Snapshot job

`POST /api/snapshots` – `mapping_id`, `source_connection_id`, `destination_connection_id`,
`pipeline_id`, `key_columns[]`, `where`, `chunk_size` (5000), `min_chunk_size` (500),
`max_chunk_size` (50000), `max_chunk_seconds` (20), `max_rows_per_second`,
`pause_between_chunks_ms` (0), `max_retries` (5), `windows[]` (`{start, end, days[0-6]}`), `start`.
States: `pending → running ⇄ paused → completed | failed | cancelled`.

## Alert rule

`PUT /api/alerts/rules` – `id`, `name`, `condition`, `severity` (`info` \| `warning` \| `error` \|
`critical`), `scope` (`global` \| `per_pipeline` \| `per_snapshot`), `rule_set`,
`cooldown_seconds` (900), `for_seconds` (0), `enabled`, `message` (template with `{pipeline[lag_seconds]}`
style placeholders), `description`. Conditions support comparisons, `and/or/not`, arithmetic,
`in`, and the functions `max min abs len sum any all round float int str`.

### Alert facts

| Namespace                                  | Facts                                                                                                                                                                   |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `pipeline.*` (per pipeline)                | `id running paused expected_running lag_seconds queue_depth idle_seconds expect_traffic connection_errors_5m events_failed events_applied unsafe_key_tables last_error` |
| `source.*` (per pipeline, engine specific) | `retention_minutes retention_margin_minutes position_before_min capture_errors slot_lag_bytes slot_active prerequisites_failed`                                         |
| `failures.*` (global)                      | `total_1h rate_1h total_5m new_signatures_1h systemic_patterns unresolved`                                                                                              |
| `snapshot.*` (per job)                     | `id state table rows_copied chunk_errors avg_chunk_seconds stalled_seconds progress waiting_for_window`                                                                 |
| `pipelines`, `bus` (global)                | number of pipelines, event-bus statistics                                                                                                                               |

## API endpoint map

Interactive documentation with schemas: `/docs`.

| Area                  | Endpoints                                                                                                                                                                                                                                                       |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Connections (library) | `GET/POST/PUT/DELETE /api/admin/connection/…`, `POST /api/admin/connect/test`                                                                                                                                                                                   |
| Connectors            | `GET /api/connectors/engines`; `POST /api/connectors/test \| prerequisites \| capture/status \| capture/enable-database \| capture/enable-tables \| capture/disable-tables \| tables \| columns \| position`; `GET /api/connectors/mssql/service-broker-script` |
| Mappings              | `POST /api/admin/mapping/create`, `GET …/list`, `GET/PUT/DELETE …/{id}`, `GET …/{id}/history`, `POST …/{id}/test`, `POST …/test-query`, `POST …/preview-merge`, `POST …/validate-merge`                                                                         |
| Working sets (1.x)    | `POST …/workset/create`, `PUT …/workset/update`, `GET …/workset/list`, `PUT …/workset/activate/{id}`, `DELETE …/workset/{id}`; `POST /api/operations/start \| stop \| pause \| resume`, `GET /api/operations/status \| statistics`                              |
| Pipelines             | `GET/POST /api/pipelines`, `POST /api/pipelines/{id}/start \| stop \| pause \| resume \| positions/reset`, `GET /api/pipelines/{id}/status \| health`, `DELETE /api/pipelines/{id}`, `GET /api/pipelines/bus/stats`                                             |
| Snapshots             | `GET/POST /api/snapshots`, `GET/DELETE /api/snapshots/{id}`, `POST /api/snapshots/{id}/start \| pause \| resume \| cancel \| reset`                                                                                                                             |
| Audit & DLQ           | `GET /api/audit/events \| stats \| failures \| failures/{id} \| patterns`, `POST /api/audit/failures/resolve \| failures/replay \| patterns/apply \| purge`                                                                                                     |
| Alerts                | `GET /api/alerts`, `POST /api/alerts/evaluate \| test`, `GET /api/alerts/context \| rule-sets \| rules`, `PUT /api/alerts/rule-sets \| rules \| channels`, `POST /api/alerts/rules/validate`, `DELETE /api/alerts/rules/{id}`                                   |
| Metrics               | `GET /metrics` (Prometheus), `GET /api/metrics`, `GET /api/metrics/history`                                                                                                                                                                                     |
| Settings              | `GET /api/settings/info \| export`, `POST /api/settings/import`                                                                                                                                                                                                 |
| Monitoring (1.x)      | `WS /api/monitoring/events`, `GET /api/monitoring/statistics \| errors \| history \| health`, `POST /api/monitoring/verify`                                                                                                                                     |
| Legacy features       | DuckDB script library `/api/admin/duckdb/scripts/…`, visual workflows `/api/admin/workflow/…`, ingestion patterns `/api/admin/ingestion/…`, multi-source `/api/operations/multi-source/…`                                                                       |
