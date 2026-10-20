# 3. Connections & CDC prerequisites

A *connection* is a document in the connection library (Connections page or
`POST /api/admin/connection/save`). It is referenced by id from mappings, pipelines, snapshot
jobs and working sets, so credentials live in exactly one place.

```json
{"id": "crm", "name": "CRM (legacy)", "engine": "mssql", "server": "sql01.bank.local", "port": 1433,
 "database": "CRM", "username": "cdcsync", "password": "${env:CRM_PASSWORD}",
 "options": {"read_only_intent": false, "poll_max_seconds": 30}}
```

* `engine`: `mssql` (default) · `postgresql` · `mariadb` / `mysql` · `mongodb` · `oracle` ·
  `sqlite` (destination only) · `demo` (synthetic source).
* `password` can be a literal, `${env:NAME}` or `${file:/run/secrets/name}`; references are
  resolved when connecting and the API always returns `***` for secrets. Editing a connection
  and leaving `***` keeps the stored secret.
* `options` carries engine-specific settings – the UI shows the right fields per engine; the
  complete list is in the [configuration reference](../reference/configuration.md#connection-documents).

## Validate before you touch production: **CDC prerequisites**

`Connections → ☑` (or `POST /api/connectors/prerequisites`) runs the engine's checklist and
returns, per check, `ok`, a severity, the measured detail and the exact *fix hint* (DDL or
setting). Nothing is changed by the check. Typical findings:

| Engine          | Checks                                                                                                                                                                                                                                                                                                                                    |
|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SQL Server      | edition supports CDC, SQL Agent running, `sysadmin`/`db_owner`, database CDC flag, recovery model, **RCSI** (so snapshots do not take shared locks), capture & cleanup jobs, **cleanup retention vs. expected outage**, `sys.dm_cdc_errors`, log-scan latency; per table: PK, tracked by CDC, capture-instance name mismatch, LOB columns |
| PostgreSQL      | version ≥ 10, `wal_level=logical`, free replication slots, `REPLICATION` role, plugin, slot lag (disk-full guard); per table: PK, `REPLICA IDENTITY` (FULL needed for before-images / tables without PK)                                                                                                                                  |
| MariaDB / MySQL | `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (MySQL 8), unique `server_id`, binlog retention, replication grants, binlog status readable; per table: PK                                                                                                                                            |
| MongoDB         | replica set / sharded, version ≥ 4 (6 for before images), oplog window vs. outage, roles; per collection: pre/post images                                                                                                                                                                                                                 |
| Oracle          | ARCHIVELOG, minimal + table supplemental logging, `LOGMINING` / `SELECT ANY TRANSACTION`, access to `V$LOG*`, `V$ARCHIVED_LOG`, `V$TRANSACTION`, multitenant placement (CDB$ROOT), archive availability; per table: PK, LOB columns                                                                                                       |

Treat *error* severities as blockers and *warnings* as things to decide consciously (e.g. a
retention of 60 minutes on the SQL Server cleanup job is technically valid but will lose data
during the first longer outage).

## Enabling change capture

**Enable capture** on the prerequisites dialog (or `POST /api/connectors/capture/enable-database`
and `/enable-tables`) performs the engine-specific action:

| Engine          | Database level                                                                                   | Table level                                                                                               |
|-----------------|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| SQL Server      | `sys.sp_cdc_enable_db` (sysadmin)                                                                | `sys.sp_cdc_enable_table` with capture instance `schema_table` (override via `options.capture_instances`) |
| PostgreSQL      | creates the logical replication slot (`options.slot_name`, plugin `wal2json` or `test_decoding`) | optional `REPLICA IDENTITY FULL`                                                                          |
| MariaDB / MySQL | sets `binlog_format=ROW` / `binlog_row_image=FULL` globally if allowed (persist in `my.cnf`)     | nothing to enable per table                                                                               |
| MongoDB         | needs a replica set (`rs.initiate()`) – reported, not changed                                    | `collMod … changeStreamPreAndPostImages`                                                                  |
| Oracle          | `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`                                                       | `ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`                                                   |

All of these are privileged operations; the audit trail records who enabled what and when.

## Browsing tables and columns

The table browser (📋 on a connection) lists tables with the capture flag, row estimate and
size, and shows the columns with primary-key markers. It works for destinations too
(SQLite and any SQL destination), which the mapping editor uses for auto-mapping.

## SQL Server push mode (optional)

By default SQL Server is polled with one cheap watermark query per cycle (adaptive back-off up
to 30 s while idle). For sub-second latency without any polling over the network, the
**Service Broker** notifier can be installed: *Connections → SQL Server push-mode script* returns
the T-SQL (one message type, contract, queue, service and a stored procedure that loops inside
the server and sends a message whenever the CDC watermark moves). Schedule
`EXEC dbo.cdcsync_notifier` as a SQL Agent job and set `options.service_broker_queue` to
`dbo.cdcsync_notify_queue`. No triggers are installed on user tables.

## Destinations

Destinations are connections too: SQL Server, PostgreSQL, MariaDB/MySQL, Oracle and SQLite.
Writes are upserts (`MERGE`, `INSERT … ON CONFLICT`, `INSERT … ON DUPLICATE KEY`), so replays and
snapshot overlaps are harmless. Destination tables must exist; the mapping editor reads their
columns and primary keys.
