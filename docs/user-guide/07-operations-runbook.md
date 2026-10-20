# 7. Operations runbook

## Deployment

* One process (`python main.py`, or `uvicorn backend.api.app:app --host 0.0.0.0 --port 8000`).
  Put it behind a reverse proxy that adds authentication – the UI has none.
* State: `config/state.sqlite3` (configuration, positions, alert state) and
  `history/audit.sqlite3` (audit trail, dead-letter queue). Both are SQLite in WAL mode and
  safe to back up with normal file tooling at any time. Redirect them with
  `CDCSYNC_CONFIG_DIR` / `CDCSYNC_HISTORY_DIR`.
* Secrets: reference them (`${env:NAME}`, `${file:/path}`) instead of storing them.
* Optional: `CDCSYNC_KAFKA_BOOTSTRAP` mirrors every change event to Kafka/Confluent
  (`cdcsync.changes.<pipeline>`, keyed by table) so downstream consumers of the legacy system can
  be moved one by one.
* All environment variables: [configuration reference](../reference/configuration.md#environment-variables).

## Backup and change management

* **Settings → Export** produces one JSON document with connections (secrets redacted unless
  asked), mappings, working sets, pipelines, positions and custom rules – commit it to git for
  review. **Import** restores it (`overwrite` decides whether existing documents are replaced).
* Every mapping change is versioned with timestamp and author (Mappings → History). The
  `config` category of the audit trail records connection, pipeline, rule and DLQ actions.

## Sizing retention at the source – the single most important setting

The synchroniser can only resume from where it stopped if the source still has those changes:

| Engine          | Setting                                                                           | Guidance                                                                                        |
|-----------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| SQL Server      | cleanup job retention (`sp_cdc_change_job @job_type='cleanup', @retention=<min>`) | ≥ the longest outage you will ever accept + snapshot duration; default 3 days is a sane minimum |
| PostgreSQL      | the slot retains WAL until consumed – disk is the limit                           | keep pipelines running; `drop_slot` when decommissioning; watch `slot_wal_retention`            |
| MariaDB / MySQL | `binlog_expire_logs_seconds`                                                      | same rule as SQL Server                                                                         |
| MongoDB         | oplog size (`replSetResizeOplog`)                                                 | oplog window ≥ longest outage                                                                   |
| Oracle          | RMAN archive-log retention                                                        | archives must cover the SCN window since the last commit                                        |

The `data-loss-guard` rule set alerts *before* the limit is reached
(`retention_margin_low`) and loudly when it has been crossed (`position_before_min`).

## Incidents

| Symptom                                                                                 | Diagnosis                                                                  | Action                                                                                                                                                  |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pipeline in `error`, last error "committed LSN … older than the oldest retained change" | retention exceeded during an outage; changes are gone                      | *Reset positions* on the pipeline, run a new snapshot for the affected tables with `pipeline_id` set, start the pipeline with `initial_position=latest` |
| Lag keeps growing, queue depth high                                                     | destination slower than the source; check `apply_latency_seconds` p95      | bigger `batch_size`, fewer DuckDB expressions, indexes on destination keys; split mappings over two pipelines                                           |
| Lag high but queue empty                                                                | source reader slow (watermark back-off, capture job latency on SQL Server) | check `source_poll_seconds`, CDC capture job latency (`sys.dm_cdc_log_scan_sessions`), lower `poll_max_seconds`, or enable Service Broker push          |
| `unsafe_key_heuristic` alert                                                            | a mapping without key columns writes to a table without PK                 | set `key_columns` on the mapping                                                                                                                        |
| Failure burst after a deployment on the source                                          | schema drift (`Invalid column name`, `relation … does not exist`)          | Failures → pattern *schema_drift*: re-scan, update the mapping, replay the DLQ                                                                          |
| Snapshot stalled                                                                        | waiting for its maintenance window, or the source is throttled             | check `waiting_for_window`, chunk timings; pause during business hours instead of cancelling                                                            |
| Process restarted                                                                       | –                                                                          | pipelines with `auto_start` resume from their committed positions; running snapshots are loaded as *paused* – resume them                               |
| Disk growing on a PostgreSQL source                                                     | an unconsumed slot                                                         | start the pipeline or drop the slot (`PostgresConnector.drop_slot`)                                                                                     |

## Housekeeping

* **Settings → Retention → Purge**: audit rows older than N days, resolved failures older than
  M days (`POST /api/audit/purge`).
* `CDCSYNC_AUDIT_LEVEL=all` writes one audit row per change event – useful for a short
  investigation, too much for bank volumes (default `summary`: batch summaries + every failure).
* The metrics history keeps 4 hours at 5-second resolution in memory; use Prometheus for
  anything longer.

## Upgrading from 1.x

On first start the old `config/*.json` files are imported into the SQLite store and renamed to
`*.migrated-<timestamp>`; 1.x LSN positions are adopted by pipelines named after the working
set (`ws_<id>`). The 1.x pages remain at `/legacy/…`; old bookmarks redirect.
