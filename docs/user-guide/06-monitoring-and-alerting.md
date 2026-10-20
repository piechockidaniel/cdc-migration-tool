# 6. Monitoring, failures & alerting

## Where to look

| Question                                      | Page / endpoint                                                                                                |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| Is everything running, how far behind are we? | **Dashboard** – pipelines, worst lag, throughput and lag charts (history kept on the server, survives reloads) |
| What exactly failed and why?                  | **Failures** – patterns and the dead-letter queue                                                              |
| What happened at 03:12 last night?            | **Audit trail** – every start/stop, batch summary, config change, prerequisite run, alert; CSV export          |
| Numbers for Grafana / Prometheus              | `GET /metrics` (Prometheus exposition), `GET /api/metrics` (JSON), `GET /api/metrics/history` (sampled series) |
| Why is a rule (not) firing?                   | **Alerts & Rules → Show facts**, or the pipeline's ⓘ *health facts*                                            |

Key metrics: `cdc_events_applied_total` / `cdc_events_failed_total`, `replication_lag_seconds`,
`apply_latency_seconds` (p50/p95/p99), `pipeline_queue_depth`, `source_roundtrips_total` and
`watermark_checks_total` (proof that an idle source is *not* being hammered),
`snapshot_rows_total`, `snapshot_progress_ratio`, `alerts_fired_total`.

## Failures: dead-letter queue and patterns

Every change that cannot be applied is stored with its full payload, the error and a
normalised *signature* (numbers, literals and GUIDs removed, so the same problem on different
rows is one pattern). **Failures → Patterns** groups them and classifies each pattern:

| Category       | Meaning                                                                                                                                                          | What the UI offers                                                                            |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `fixable`      | a mapping or destination change solves it (NOT NULL → default value, truncation → widen/`LEFT()`, PK violation → upsert, conversion → cast, missing key columns) | **Apply** – changes the mapping directly and resolves the pattern                             |
| `schema_drift` | a column referenced by the mapping no longer exists                                                                                                              | re-scan destination, regenerate the mapping                                                   |
| `deprecated`   | the source/destination object does not exist anymore                                                                                                             | **Apply: disable mapping** – the candidate list is the input for the next strangler iteration |
| `transient`    | deadlock, timeout, connection reset                                                                                                                              | **Replay** through a running pipeline                                                         |
| `ordering`     | foreign-key parent missing                                                                                                                                       | order parent → child mappings, replay                                                         |
| `unknown`      | –                                                                                                                                                                | inspect the payload                                                                           |

A pattern that keeps recurring for ≥ 6 hours is marked **systemic**. Rows can also be replayed
or resolved individually from the *Dead-letter queue* tab (bulk selection supported).
Replaying needs a running pipeline whose destination/applier is used.

## Alert rules and rule sets

Rules are boolean conditions over *facts* the system collects every `CDCSYNC_ALERT_INTERVAL`
seconds (30). They are grouped in **rule sets** that you switch on as a whole:

| Rule set           | Covers                                                                                                                                |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `pipeline-health`  | pipeline stopped unexpectedly, lag > 5 min / > 30 min, queue backlog, silent source, repeated connection errors                       |
| `data-loss-guard`  | **retention margin < 2 h**, **committed position already purged**, capture errors at the source, PostgreSQL slot retaining > 5 GB WAL |
| `failure-patterns` | failure rate > 1 %, failure burst, new signature, systemic pattern, DLQ > 1000                                                        |
| `snapshot`         | job failed, stalled 10 min, slow chunks, chunk errors                                                                                 |
| `configuration`    | guessed key columns in use, prerequisite failures                                                                                     |

Writing your own (Alerts & Rules → Rules → + Rule):

```
pipeline.lag_seconds > 120 and pipeline.running
failures.total_1h >= 10 and failures.rate_1h > 0.01
source.retention_margin_minutes != None and source.retention_margin_minutes < 240
snapshot.state == 'running' and snapshot.avg_chunk_seconds > 30
```

Scope `per_pipeline` / `per_snapshot` evaluates the rule once per pipeline/job; `for_seconds`
suppresses flapping; `cooldown_seconds` limits repeat notifications. **Validate** checks the
syntax and shows whether the condition would fire right now for every scope. The facts
namespace (`pipeline.*`, `source.*`, `failures.*`, `snapshot.*`) is listed in the
[configuration reference](../reference/configuration.md#alert-facts).

## Notification channels

Log, audit trail and the in-app WebSocket toast are always on. Add a **webhook** (Slack/Teams/
Alertmanager/n8n – any JSON POST) and/or **e-mail** (SMTP) under *Alerts & Rules → Channels*,
then *Send test*. `CDCSYNC_ALERT_WEBHOOK` configures the webhook by environment.

An alert is *firing* until its condition clears, then a single *resolved* notification is sent.
Alert state survives restarts.
