# 1. Architecture

## Components

```
backend/
  api/            FastAPI routers: admin (connections, mappings, worksets, workflows – 1.x contract),
                  operations (1.x start/stop, now driving pipelines), monitoring (WebSocket, history),
                  connectors_api, pipelines_api (+ snapshots), audit_api (+ alerts, metrics, settings), web_routes
  connectors/     SourceConnector ABC + mssql / postgresql / mariadb / mongodb / oracle (+ oracle_redo parser),
                  demo (synthetic), destination_adapter (metadata for sinks), watermark (adaptive back-off)
  destinations/   SqlDestination: dialect-aware upserts for mssql / postgresql / mysql / oracle / sqlite
  core/
    pipeline.py         ChangePipeline: reader thread -> EventBus -> applier thread -> position commit
    applier.py          mapping runtime, key resolution, column controls, DuckDB expressions & scripts, poison-row isolation
    snapshot_manager.py chunked keyset snapshots (resumable, throttled, windows, position hand-off)
    audit/              AuditStore (SQLite) + DLQ, pattern mining, rules + SafeEvaluator, AlertManager + channels
    metrics.py          counters/gauges/histograms, Prometheus + JSON; metrics_history.py samples them
    event_bus.py        in-process pub/sub with bounded subscriptions, optional Kafka transport
    runtime.py          wiring of the singletons, health-context provider, background services
    config_manager.py   typed facade over the ConfigStore (1.x public API kept)
    duckdb_processor.py in-memory DuckDB used by the applier
    multi_source_*.py, history_manager.py, data_quality.py, mapping_executor.py, sql_mapping_service.py   1.x services still used by the legacy API
  storage/        ConfigStore interface, SqliteConfigStore (default), JsonFileConfigStore, secrets, legacy migration
  models/         pydantic schemas (API contract)
frontend/app/     the SPA (see 05-frontend.md); frontend/templates + static: 1.x pages under /legacy
```

## Data flow of a pipeline

```
 source DB ──stream_changes()──▶ reader thread ──publish──▶ EventBus topic changes.<pipeline>
                                                                 │ (bounded queue = back-pressure)
                                                                 ▼
                                   applier thread: get_batch() ─▶ RowApplier.apply()
                                      ├─ group consecutive events by (table, op)
                                      ├─ transform (renames, DuckDB expressions/script, defaults, ignore, auto-generate)
                                      ├─ executemany upsert / delete; on failure retry row by row -> DLQ
                                      └─ commit positions (ConfigStore) + connector.confirm_position()
                                                                 │
              audit batch summaries, metrics, legacy history/WebSocket listeners, alert facts
```

Threads, not asyncio: every driver is blocking; the FastAPI event loop only starts/stops
threads and reads status (`asyncio.to_thread` for the few endpoints that call a driver).

### Delivery semantics

* **At-least-once.** Positions are committed after the batch is applied or parked in the DLQ;
  a crash replays the tail. Upserts make replays idempotent.
* **Ordering** per table is preserved (one reader, one applier per pipeline; Kafka mirror keyed by table).
* **Positions**: SQL Server keeps one `(start_lsn, seqval)` per capture instance
  (`position_scope = "table"`); PostgreSQL (`pg_lsn`), MariaDB (`{file, pos}`), MongoDB (resume
  token) and Oracle (commit SCN) keep one cursor per source. `Position(kind, value, extra)` is
  opaque to the pipeline and serialised into the config store.
* **Data-loss guard**: connectors expose `retention_status()`; the SQL Server connector refuses
  to read past a purged position (pipeline state `error`, alert `position_before_min`).
* **Snapshot/CDC consistency**: a snapshot records the source position before its first chunk
  and hands it to the pipeline; both sides write upserts, so the overlap window is harmless.

## Design decisions (and why)

| Decision                                                                              | Reason                                                                                                  |
|---------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| Polling with one watermark query + adaptive back-off, push where the engine offers it | "do not hammer the source": idle cost is independent of the number of tables; see below                 |
| In-process bus with bounded queues                                                    | slow destination must block the reader, not grow memory; fan-out to Kafka without touching the pipeline |
| Upserts everywhere                                                                    | at-least-once without duplicates; snapshot overlap; DLQ replay                                          |
| Real key resolution (mapping → destination PK → source PK → flagged heuristic)        | the 1.x `'id' in name` guess updated wrong rows                                                         |
| DuckDB for expressions, per batch                                                     | vectorised, one round trip, SQL users can read it; no extra service                                     |
| Chunked keyset snapshots written through the applier                                  | resumable, throttled, poison rows isolated, same transformation path as CDC                             |
| SQLite (WAL) for state, JSON only as export format                                    | see [03-storage.md](03-storage.md)                                                                      |
| SQLite audit store with signature-grouped DLQ                                         | O(1) appends at bank volumes; patterns are the feedback loop of the strangler iteration                 |
| Rule engine with a whitelisting AST evaluator                                         | operators write conditions, no `eval`, missing facts never fire                                         |
| No-build SPA with vendored libraries                                                  | on-prem, air-gapped, readable source                                                                    |

## Event-driven processing for SQL Server CDC

### The honest starting point

SQL Server CDC is a *log reader that materialises changes into tables*
(`cdc.<capture_instance>_CT`) via the capture job. There is **no built-in
notification** when those tables receive rows: no `LISTEN/NOTIFY`, no change
stream. Every CDC consumer on the market (Debezium, Kafka Connect, Qlik, …)
ultimately polls `sys.fn_cdc_get_max_lsn()`. What can be engineered is *how
cheap* that polling is and *where* it happens.

The original implementation polled every table every 5 seconds with at least
two queries per table (`get_max_lsn` + `get_min_lsn`/`get_all_changes`) – with
200 mapped tables that is ~80 queries/s against the source, most of them
returning nothing.

### Level 1 – single watermark, adaptive cadence (default)

```
loop:
    wm = SELECT sys.fn_cdc_get_max_lsn()                 -- 1 trivial query for the WHOLE database
    if wm == last: sleep(backoff)   # 0.25 s → 0.5 → 1 → … → 30 s while idle
    else:
        changed = SELECT 'dbo.A', COUNT_BIG(*) FROM cdc.dbo_A_CT WITH (NOLOCK) WHERE __$start_lsn > ?
                  UNION ALL SELECT 'dbo.B', … UNION ALL …          -- 1 query, index seek per table
        for table in changed:  read_changes(table)                  -- only the tables that moved
        backoff = min
```

Idle source: one 1-row query every 30 s. Busy source: drained at 250 ms
cadence. Cost is independent of the number of mapped tables while idle.
`watermark_checks_total{outcome=idle|moved}` and `source_roundtrips_total`
show the effect.

### Level 2 – Service Broker push (optional, `options.service_broker_queue`)

`GET /api/connectors/mssql/service-broker-script` returns a small T-SQL script:
a message type, contract, queue, two services and one stored procedure
`dbo.cdcsync_notifier` that loops **inside the server** checking
`fn_cdc_get_max_lsn()` every 500 ms and `SEND`s a message when it changed
(scheduled as a SQL Agent job step; no triggers on any user table, nothing
touches the 16 TB schema).

The connector then blocks in `WAITFOR (RECEIVE TOP(1) … FROM dbo.cdcsync_notify_queue), TIMEOUT 30000`
– a true server-side push: zero network round trips while idle, sub-second
wake-up when the log moves, bursts coalesced by draining the queue.

### Level 3 – fan-out to Confluent / Kafka

Every `ChangeEvent` is published on the in-process bus; with
`CDCSYNC_KAFKA_BOOTSTRAP` set it is mirrored to `cdcsync.changes.<pipeline>`
(key = `schema.table`, idempotent producer). Downstream consumers of the
legacy instance can be strangled in one by one by subscribing to the topic
instead of the old DB – which is the demo setup the architect built on
Confluent Platform.

### Internal pub/sub (`backend/core/event_bus.py`)

```
connector.stream_changes ──publish──▶ topic changes.<pipeline> ──▶ Subscription (bounded queue) ──▶ applier thread
                                                │
                                                ├─▶ observers (WebSocket broadcaster, legacy history)
                                                └─▶ ExternalTransport (Kafka)
alerts ───────────────────────────────────────▶ topic alerts ──▶ WebSocket, audit
```

Bounded subscriptions give back-pressure: if the destination is slow the
reader blocks on `publish` instead of buffering millions of events in RAM.

### LSN bookkeeping

* Positions are `(start_lsn, seqval)`; reads begin at `fn_cdc_increment_lsn(last)`
  or, when resuming mid-LSN, filter `NOT (__$start_lsn = last AND __$seqval <= seq)`.
* The position is committed **after** the batch is applied (or parked in the DLQ)
  → at-least-once. Writes are upserts → replays are idempotent.
* `retention_status()` compares committed positions with `fn_cdc_get_min_lsn`
  and the cleanup job's retention; the `data-loss-guard` rule set fires before
  the cleanup job can purge unread changes.

### Same pattern for the other engines

| Engine        | Idle cost                                                      | Mechanism         |
|---------------|----------------------------------------------------------------|-------------------|
| PostgreSQL    | 1 `pg_current_wal_lsn()` per back-off step, or 0 with `LISTEN` | slot peek/advance |
| MariaDB/MySQL | 0 – replica stream, server pushes                              | binlog            |
| MongoDB       | 0 – `try_next` with `maxAwaitTimeMS` blocks server-side        | change stream     |
| Oracle        | 1 `SELECT current_scn FROM v$database` per back-off step       | LogMiner window   |


## Multi-source mappings (1.x engine, known limitation)

`multi_source_cdc_monitor.py` / `multi_source_sync_engine.py` monitor several SQL Server sources
and re-execute the merge pattern (`MergePatternExecutor.build_merge_query`: CTE per source, a
JOIN/UNION/INTERSECT/EXCEPT pipeline, output column selection) when any source changes. The
runtime re-runs the **full** merge for every event and looks up existing rows by the first
output column – correct for small reference data, O(N) per event otherwise. The planned fix is
a key-filtered merge query (`WHERE <join key> = ?`) driven by the join key extracted from the
event, which the executor already parses. The 2.0 pipeline does not run multi-source mappings;
they are started through `/api/operations/multi-source/*`.
