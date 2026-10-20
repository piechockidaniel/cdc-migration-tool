# 2. Connectors

All sources implement `backend/connectors/base.py::SourceConnector`:

| Method                                                                                                  | Purpose                                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `check_prerequisites(tables)`                                                                           | validates everything CDC needs **before touching production**; every check carries `ok`, `severity`, `detail` and a `fix_hint` (the exact DDL / setting) |
| `enable_capture_database()` / `enable_capture_table()` / `disable_capture_table()` / `capture_status()` | capture management at DB and table level                                                                                                                 |
| `current_position()`                                                                                    | the cheap "watermark" (one index-free lookup)                                                                                                            |
| `read_changes(tables, from, to, limit)`                                                                 | pull changes strictly after a position; returns the position to commit                                                                                   |
| `stream_changes(tables, from, stop)`                                                                    | push-style iterator (change streams, binlog stream, Service Broker, adaptive watermark loop)                                                             |
| `read_snapshot_chunk(...)` / `count_rows()` / `key_bounds()`                                            | keyset-paginated snapshot support                                                                                                                        |
| `retention_status(positions)`                                                                           | facts for the data-loss-guard alert rules                                                                                                                |

Events are `ChangeEvent(source, schema, table, operation, position, key, before, after, source_ts, tx_id)`;
positions are opaque `Position(kind, value, extra)` objects serialised into the config store.

Connection documents are the same `ConnectionConfig` the UI already uses plus
`engine` and engine specific keys under `options` (flattened for the connector).
Secrets may be `${env:NAME}` / `${file:/path}` references.

```json
{"id": "crm-pg", "engine": "postgresql", "server": "pg.bank.local", "port": 5432, "database": "crm",
 "username": "cdc", "password": "${env:CRM_PG_PASSWORD}", "options": {"slot_name": "cdcsync_crm", "plugin": "wal2json"}}
```

---

## Microsoft SQL Server (`engine: mssql`) – `connectors/mssql.py`

* **Mechanism**: native CDC (`cdc.fn_cdc_get_all_changes_<ci>`), positions are
  `(start_lsn, seqval)` so a batch can be cut anywhere and resumed without
  duplicates; reads start at `fn_cdc_increment_lsn(last)`.
* **Event-driven reading**: see [the architecture page](01-architecture.md#event-driven-processing-for-sql-server-cdc) – one `fn_cdc_get_max_lsn()`
  per cycle (adaptive 0.25 s → 30 s back-off), a single `UNION ALL` probe over the
  `cdc.<ci>_CT` tables to learn *which* tables changed, reads only those. Optional
  Service Broker push (`options.service_broker_queue`).
* **Prerequisites checked**: edition, SQL Agent running, `sysadmin`/`db_owner`,
  `is_cdc_enabled`, recovery model, RCSI (for snapshots), capture & cleanup jobs,
  **retention vs. committed position**, `sys.dm_cdc_errors`, log-scan latency,
  per table: existence, PK, tracked, capture-instance name mismatch, LOB columns.
* **Options**: `capture_instances` (`{"dbo.Customers": "dbo_Customers_v2"}`),
  `row_filter` (`all` | `all update old` for before images), `read_only_intent`
  (AG readable secondary for snapshots), `snapshot_hint`, `poll_min_seconds`,
  `poll_max_seconds`, `batch_limit`, `service_broker_queue`.
* **Limitations**: DDL is not captured (SQL Server creates a new capture instance;
  use `capture_instances`), `text/ntext/image` need `@captured_column_list`.

## PostgreSQL (`engine: postgresql`) – `connectors/postgresql.py`

* **Mechanism**: logical replication slot read through SQL functions
  (`pg_logical_slot_peek_changes` + `pg_replication_slot_advance` after the
  position is committed) – at-least-once, works with plain `psycopg` and through
  PgBouncer (session mode). Output plugin `wal2json` (format 2, filtered
  server-side with `add-tables`) or the built-in `test_decoding` (parsed here).
* **Watermark**: `pg_current_wal_lsn()`; optional `LISTEN` wake-up
  (`options.notify_channel`) if the customer is willing to `NOTIFY` from a trigger
  or the application – the connector never installs triggers.
* **Prerequisites**: version ≥ 10, `wal_level=logical`, free replication slots,
  `REPLICATION` role, plugin availability, slot lag (disk-full guard), per table:
  PK, `REPLICA IDENTITY` (FULL needed for before images / tables without PK).
* **Options**: `slot_name`, `plugin`, `notify_channel`, `sslmode`, `batch_limit`.
* **Caveat**: a slot that is not consumed retains WAL forever → the
  `slot_wal_retention` rule alerts; `drop_slot()` when a pipeline is retired.

## MariaDB / MySQL (`engine: mariadb` | `mysql`) – `connectors/mariadb.py`

* **Mechanism**: the connector registers as a replica (`python-mysql-replication`)
  and receives row events pushed by the server – true push, zero polling.
  Positions are binlog coordinates `{file, pos}` (+ GTID when available).
* **Prerequisites**: `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`,
  `binlog_row_metadata=FULL` (MySQL 8), unique `server_id`, retention
  (`binlog_expire_logs_seconds` / `expire_logs_days`), `REPLICATION SLAVE/CLIENT`
  (or `BINLOG MONITOR`) grants, `SHOW MASTER|BINARY LOG STATUS` readable, PK per table.
* `enable_capture_database()` switches `binlog_format`/`binlog_row_image` globally
  when it has the privilege (persist in `my.cnf`!).
* **Options**: `server_id` (default 4379), `batch_limit`.

## MongoDB (`engine: mongodb`) – `connectors/mongodb.py`

* **Mechanism**: change streams on the database (`db.watch`) filtered by collection;
  resume tokens are the positions; `fullDocument=updateLookup` and
  `fullDocumentBeforeChange=whenAvailable` (6.0+ with `changeStreamPreAndPostImages`).
* `schema` = database, `table` = collection, key = `documentKey` (`_id`). Nested
  documents are serialised to JSON strings when the destination is relational.
* **Prerequisites**: replica set / sharded cluster, version ≥ 4 (6 for before
  images), oplog window vs. expected outage, pre/post images per collection.
* `enable_capture_table()` runs `collMod … changeStreamPreAndPostImages`.
* **Options**: `uri` (overrides host/port), `auth_database`, `options`
  (connection-string options, e.g. `directConnection=true`), `full_document`,
  `full_document_before_change`, `max_await_ms`, `schema_sample`.

## Oracle (`engine: oracle`) – `connectors/oracle.py`

* **Mechanism**: LogMiner in thin mode (no Oracle client). Per read: add the
  online/archived log files covering the SCN window (`CONTINUOUS_MINE` is
  desupported), `START_LOGMNR(COMMITTED_DATA_ONLY + DICT_FROM_ONLINE_CATALOG +
  NO_ROWID_IN_STMT)`, select DML for the wanted tables, parse `SQL_REDO`
  (`oracle_redo.py`: quoted identifiers, `''` escapes, `NULL`/`IS NULL`,
  `TO_DATE/TO_TIMESTAMP`, `HEXTORAW`, `UNISTR`, `EMPTY_CLOB`, CSF continuation rows).
* **Long transactions**: the mining window starts at
  `min(open transaction START_SCN, last_commit + 1)` and applied transactions are
  excluded with `COMMIT_SCN > last` – a transaction that started before the last
  commit and commits later is never missed. Batches are cut only at
  transaction boundaries.
* **Prerequisites**: ARCHIVELOG, minimal + table-level supplemental logging,
  `LOGMINING`, `SELECT ANY TRANSACTION`, access to `V$LOG*`, `V$ARCHIVED_LOG`,
  `V$TRANSACTION`, CDB$ROOT placement in multitenant, archive availability, PK,
  LOB columns (not reconstructed by redo SQL – re-select on apply if needed).
* **Options**: `service_name` / `dsn`, `schema` (owner, defaults to the user),
  `supplemental_mode` (`ALL` | `PRIMARY KEY`), `max_scn_window`, `batch_limit`,
  `mode: SYSDBA`.
* **Not used on purpose**: Continuous Query Notification (`DBMS_CHANGE_NOTIFICATION`)
  would be a push channel but needs thick mode and a callback port open from the
  DB server to the synchroniser – usually not acceptable in a bank network.

---

## Destinations

`backend/destinations/sql_destination.py` writes to SQL Server, PostgreSQL,
MariaDB/MySQL, Oracle and SQLite with dialect-aware quoting and **upserts**
(`MERGE … WITH (HOLDLOCK)`, `INSERT … ON CONFLICT`, `INSERT … ON DUPLICATE KEY`,
`MERGE INTO … USING dual`). Snapshot chunks are written in one transaction each.
On SQL Server batches of ≥ 200 rows go through a session temp table
(`fast_executemany` INSERT, typed from `INFORMATION_SCHEMA.COLUMNS`) followed by a
single `MERGE` – pyodbc cannot describe parameters inside `MERGE … USING (SELECT ?)`,
so small batches use a plain (non-fast) `executemany`.
Other sinks (graph / document stores, Kafka topics) plug in behind the same
interface; change events are additionally mirrored to Kafka when
`CDCSYNC_KAFKA_BOOTSTRAP` is set, which is often the simplest way to feed a
document or graph store through its own sink connector.

## Adding an engine

1. Subclass `SourceConnector` in `backend/connectors/<engine>.py`; implement the abstract methods
   (metadata, prerequisites, capture management, `current_position`, `read_changes`, snapshot chunking)
   and, when the engine can push, `stream_changes`. Accept an injectable `connection_factory`
   so tests can script the driver with `tests/fakes.py::FakeDbApiConnection`.
2. Declare `capabilities` and a `Position.kind`; if the engine keeps one cursor per source (not per
   table) nothing else is needed – the pipeline chooses the scope from `engine == "mssql"`.
3. Implement `retention_status()` so the `data-loss-guard` rules get facts.
4. Register the class in `backend/connectors/__init__.py::ENGINES`; add the engine form fields in
   `frontend/app/js/pages/connections.js::ENGINES`.
5. Tests: unit tests asserting the SQL/positions the connector sends (see `tests/unit/test_connectors.py`),
   a live contract test in `tests/integration/test_live_connectors.py` and a service in
   `docker/docker-compose.integration.yml`.

## API

* `GET  /api/connectors/engines`
* `POST /api/connectors/test | prerequisites | capture/status | capture/enable-database | capture/enable-tables | capture/disable-tables | tables | columns | position`
  – body `{"connection_id": "..."}` or `{"connection": {...}}`, plus `"tables": ["schema.table"]`
* `GET  /api/connectors/mssql/service-broker-script`
