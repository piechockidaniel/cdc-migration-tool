# 4. Audit, rules & alerting internals

User-facing behaviour is described in the [user guide](../user-guide/06-monitoring-and-alerting.md); this page covers the implementation.

## 1. Audit store (`backend/core/audit/store.py`)

SQLite (WAL) database `history/audit.sqlite3` with two tables:

* **`audit`** – pipeline life-cycle, per-batch summaries (applied/failed/duration
  per table+operation), snapshot events, configuration changes (connections,
  pipelines, rules, DLQ actions), prerequisite results, alerts, warnings
  (e.g. "guessed key columns").
  `CDCSYNC_AUDIT_LEVEL=all` additionally writes one row per change event; the
  default `summary` keeps the store small at bank volumes (failures are *always*
  recorded individually).
* **`failures`** – the dead-letter queue: every change that could not be applied
  with the full event payload (replayable), the error, a normalised
  **signature**, attempts, resolution.

Writes are buffered (flushed every second / 500 records / immediately for
errors); reads are indexed by time, category, table and signature.
Retention: `POST /api/audit/purge?audit_days=30&failures_days=90`.

## 2. Failure pattern mining (`patterns.py`)

`error_signature()` strips numbers, quoted literals, GUIDs and hex from error
messages so that *"truncated in column 'name', value 'abc'"* and the same error
for another row collapse into one signature. Groups (signature + table +
operation) are classified:

| category       | meaning                                      | typical suggestion / proposed change                                                          |
|----------------|----------------------------------------------|-----------------------------------------------------------------------------------------------|
| `fixable`      | a mapping or destination change solves it    | widen column / `LEFT(col,n)`, `default_value`, `sync_mode=upsert`, cast, grant, `key_columns` |
| `schema_drift` | column referenced by the mapping disappeared | re-scan destination, regenerate column mapping                                                |
| `deprecated`   | the object does not exist (anymore)          | remove the mapping from the working set (candidate listed in `deprecated_candidates`)         |
| `transient`    | deadlock / timeout / connection reset        | replay the DLQ                                                                                |
| `ordering`     | FK parent missing                            | order mappings parent→child, replay                                                           |
| `unknown`      | –                                            | human                                                                                         |

A pattern is **systemic** when it recurs for ≥ 6 h with ≥ 3 occurrences – i.e. it
will not go away by itself. `GET /api/audit/patterns` returns the patterns with
`share`, `suggestion` and a machine-readable `proposed_change`; the Control
Center shows them with one-click *replay* / *resolve*.

This is the strangler-fig feedback loop the architect described:
observe failures → classify → adjust mapping (or drop a deprecated one) →
replay the DLQ through the running pipeline (`POST /api/audit/failures/replay`).

`POST /api/audit/patterns/apply {signature, action, mapping_id?, params}` applies
the proposed change to the mapping directly (`disable_mapping`, `set_sync_mode`,
`set_default_value` – the column is parsed from the error message –,
`add_transformation`, `set_key_columns`), marks the pattern's failures resolved
and tells you which running pipelines must be restarted to pick it up. The
Control Center exposes this as an *apply* button next to each pattern.

## 3. Rules (`rules.py`)

A rule = id, name, **condition**, severity, `rule_set`, `scope`
(`global` | `per_pipeline` | `per_snapshot`), `cooldown_seconds` (no re-notify
storm), `for_seconds` (flap protection), `enabled`.

Conditions are plain expressions over a facts namespace, evaluated by a
whitelisting AST evaluator (no `eval`):

```
pipeline.lag_seconds > 300
failures.total_1h >= 10 and failures.rate_1h > 0.01
source.retention_margin_minutes != None and source.retention_margin_minutes < 120
snapshot.state == 'running' and snapshot.stalled_seconds > 600
```

Available facts (`GET /api/alerts/context`):

* `pipeline.*` – `running, paused, expected_running, lag_seconds, queue_depth, idle_seconds, expect_traffic, connection_errors_5m, events_failed, events_applied, unsafe_key_tables, last_error`
* `source.*` (per pipeline, engine specific) – `retention_minutes, retention_margin_minutes, position_before_min, capture_errors, slot_lag_bytes, slot_active, prerequisites_failed`
* `failures.*` (global) – `total_1h, rate_1h, total_5m, new_signatures_1h, systemic_patterns, unresolved`
* `snapshot.*` – `state, rows_copied, chunk_errors, avg_chunk_seconds, stalled_seconds, progress, waiting_for_window`

### Built-in rule sets

| set                | rules                                                                                                                                    |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `pipeline-health`  | stopped unexpectedly, lag > 5 min / > 30 min, queue backlog, silent source, repeated connection errors                                   |
| `failure-patterns` | failure rate > 1 %, burst (50 in 5 min), new signature, systemic pattern, DLQ > 1000                                                     |
| `data-loss-guard`  | **retention margin < 2 h**, **committed position already purged**, capture errors (CDC job / slot / LogMiner), slot retaining > 5 GB WAL |
| `snapshot`         | failed, stalled 10 min, slow chunks, chunk errors                                                                                        |
| `configuration`    | guessed key columns in use, prerequisite failures                                                                                        |

Rule sets are switched on/off as a whole (`PUT /api/alerts/rule-sets`), single
rules are added/overridden with `PUT /api/alerts/rules` (stored, survive restarts,
override built-ins with the same id) and removed with `DELETE`.

## 4. Alert manager & channels (`alerting.py`)

Life-cycle per rule + scope: `pending` (while `for_seconds` not reached) →
`firing` (notified, repeats suppressed by `cooldown_seconds`) → `resolved`
(notified once when the condition clears). State is persisted in the config
store so a restart neither loses nor re-fires active alerts.

Channels: `log`, `audit` (always), `bus` (→ WebSocket toast in the UI and Kafka
when mirrored), `webhook` (generic JSON POST; Slack / Teams / Alertmanager /
n8n compatible – `PUT /api/alerts/channels {"webhook_url": ...}` or
`CDCSYNC_ALERT_WEBHOOK`), `email` (SMTP, `{"email": {host, port, sender, recipients[], username, password, use_tls}}`).
`POST /api/alerts/test` sends a test notification through every channel.

Evaluation runs every `CDCSYNC_ALERT_INTERVAL` seconds (30) in a background
thread; `POST /api/alerts/evaluate` runs it on demand and returns the facts.

## 5. Performance counters (`backend/core/metrics.py`)

Prometheus exposition at `GET /metrics`, JSON at `GET /api/metrics`, table in the
Control Center. Key series:

* `cdc_events_read_total{source,table,op}`, `cdc_events_applied_total{table,op}`, `cdc_events_failed_total`, `cdc_events_skipped_total`
* `apply_latency_seconds{table}` (histogram, p50/p95/p99), `apply_batch_size{table}`
* `replication_lag_seconds{table}` (source commit → destination apply)
* `source_poll_seconds{source}`, `source_polls_total{source,outcome}`, `watermark_checks_total{source,outcome}`, `source_roundtrips_total{source,kind}`, `db_query_seconds{connection,kind}`
* `pipeline_queue_depth{pipeline}`, `connection_errors_total{connection}`
* `snapshot_rows_total{job}`, `snapshot_chunks_total{job,outcome}`, `snapshot_chunk_seconds{job}`, `snapshot_progress_ratio{job}`, `snapshot_jobs_active`
* `alerts_fired_total{rule,severity}`, `audit_records_total{category}`
* `config_store_writes_total{collection}`, `config_store_write_seconds{backend}`
