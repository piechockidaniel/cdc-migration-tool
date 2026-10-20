# 6. Testing

```
pip install -r requirements.txt
pytest                      # unit + API tests (no databases needed, ~10 s)
pytest -m perf -s           # throughput benchmarks, prints numbers
pytest -m live              # connector tests against docker-compose databases (skipped otherwise)
```

Linux note: `pyodbc` needs `libodbc.so.2` (`apt install unixodbc`) even when no
SQL Server is used, because the legacy modules import it.

## Layout

```
tests/
  conftest.py               redirects config/history dirs to a temp folder BEFORE backend is imported
  fakes.py                  FakeDbApiConnection (scriptable cursor), FakeConnector (in-memory source with
                            scripted change stream + keyset snapshot), sqlite_destination(), make_mapping()
  unit/
    test_metrics.py         counters/gauges/histograms, label handling, Prometheus/JSON export, thread safety
    test_storage.py         both store backends: CRUD, optimistic concurrency, threads, **multiple OS processes**,
                            history, atomic writes, external modification, legacy migration, secrets
    test_connectors_more.py connector edge cases on fakes: Mongo metadata/capture/stream errors, PostgreSQL slot
                            creation failures and wake-ups, MariaDB/Oracle error paths, ConnectorError mapping
    test_pipeline_recovery.py reader failure -> error state -> recovery, stream-closed reconnect, applier crash
                            re-queues the batch without committing, position-commit failure reporting,
                            table-scoped positions (SQL Server) + legacy LSN adoption, DuckDB processor,
                            workflow -> mapping compiler node types
    test_legacy_services.py 1.x services kept for the legacy UI: mapping manager/executor, multi-source
                            executor + sync engine, data-ingestion engine, workflow manager, DuckDB scripts,
                            JSONL history, config manager helpers
    test_connectors.py      MSSQL (LSN increment / seqval resume / limit cut / probe query / prerequisites /
                            enable / snapshot SQL / watermark stream / retention), PostgreSQL (wal2json +
                            test_decoding decoding, slot advance, prerequisites), MariaDB (binlog event mapping,
                            poll + push stream, prerequisites), MongoDB (change documents, stream), Oracle
                            (redo parser edge cases, LogMiner flow, transaction-boundary cut, prerequisites)
    test_applier_and_pipeline.py  key resolution, composite keys, column controls + DuckDB expressions,
                            poison-row isolation + DLQ, event bus back-pressure, Kafka transport, pipeline
                            end-to-end (commit-after-apply, resume from position, pause/resume, failures
                            not blocking, registry)
    test_snapshot_manager.py  chunking, position hand-off, pause/resume/restart resumability, idempotent
                            re-run, key requirement, retries/failure, maintenance windows, adaptive chunk size
    test_audit_alerting.py  audit store, DLQ grouping/resolution/purge, classifier catalogue, pattern
                            mining, safe evaluator (incl. injection attempts), built-in rules validity,
                            alert life-cycle/cooldown/resolution/persistence, for_seconds, rule sets,
                            channels (bus/log/webhook/email), background loop
  integration/
    test_legacy_api.py      the 1.x API surface (/api/admin, /api/operations, /api/monitoring) that the legacy
                            pages still call: worksets, mappings, workflows, multi-source, ingestion, latency
    test_api.py             FastAPI TestClient against the whole stack with a fake engine + SQLite destination:
                            connector endpoints, pipeline + snapshot life-cycle, DLQ replay/resolve, alert
                            rule management, legacy /api/operations endpoints
    test_live_connectors.py real databases (docker/docker-compose.integration.yml), one contract test per engine
  perf/
    test_throughput.py      applier upsert throughput, pipeline end-to-end events/s, audit write throughput
```

Coverage (`pytest --cov=backend`): **86 %** of `backend/` (156 tests, ~30 s); the uncovered
remainder is driver-bound code that only runs against a real database (exercised by the live
tests) and defensive `except` branches.

Reference numbers on the review machine (SQLite destination, fake source):
applier ≈ 200 k rows/s, pipeline end-to-end ≈ 30 k events/s, audit writes ≈ 45 k/s.

## Browser (UI) tests

```
cd tests/ui && npm install                         # puppeteer-core + chromium shipped via npm (no CDN)
CDCSYNC_URL=http://localhost:8000 npm run smoke    # every page renders without console errors
DEST_PATH=/tmp/dest.sqlite3 npm run e2e            # demo-engine scenario: connection -> mapping -> pipeline -> snapshot -> DLQ
npm run tour                                       # guided tour walks all steps, hint dismiss/reset, restart from the help menu
```
The harness pre-sets `help.tourDone` so the first-run tour does not block clicks (`TOUR=1` to keep it).
`tests/integration/test_spa_assets.py` (pytest) checks that the SPA and its modules are served and parse.

## Live environment

```
docker compose -f docker/docker-compose.integration.yml up -d
# SQL Server needs the init script once the container is healthy:
docker exec -i $(docker ps -qf name=mssql) /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'CdcSync!2024' -i /init/mssql.sql
CDCSYNC_LIVE_TESTS=1 pytest tests/integration -m live -v
```

`PG_PLUGIN=wal2json` selects the JSON plugin when the Postgres image ships it
(e.g. `debezium/postgres:16`); the stock image runs with `test_decoding`.

## Writing tests for a new connector

1. Subclass `SourceConnector`, inject a `connection_factory`/`stream_factory` so
   the driver can be replaced by `FakeDbApiConnection` (regex → result rows).
2. Assert the *SQL/parameters* sent for positions (increment / resume / limit) –
   that is where CDC bugs hide.
3. Add a live contract test mirroring the existing ones.
