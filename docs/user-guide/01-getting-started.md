# 1. Getting started

## What the tool does

CDC Synchronizer captures row-level changes from a source database (SQL Server, PostgreSQL,
MariaDB/MySQL, MongoDB, Oracle), reshapes them according to *mappings* and applies them to a
destination database – continuously (pipelines) and for the initial load (snapshots). It is built
for strangler-fig migrations: the legacy system keeps running, the new one is filled next to it,
and everything is audited, measured and alerted from day one.

## Install

Requirements: Python 3.10+, and the client libraries of the engines you use
(SQL Server: Microsoft ODBC Driver 17/18 + `unixODBC` on Linux; the others are pure pip packages).

```bash
python -m venv venv && source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
python verify_setup.py                               # checks Python, packages, ODBC drivers
python main.py                                       # http://localhost:8000
```

The web UI is at `/`, the API documentation at `/docs`. State is kept in `config/state.sqlite3`
(configuration) and `history/audit.sqlite3` (audit trail + dead-letter queue); both are created on
first start. Environment variables are listed in the [configuration reference](../reference/configuration.md).

## Ten-minute tour with the demo engine (no database needed)

The **demo** engine is a synthetic source that generates inserts/updates/deletes for a
`demo.Customers` table. Combined with an SQLite destination you can exercise the whole chain.

1. Create the destination file:
   ```bash
   sqlite3 /tmp/demo-dest.sqlite3 'CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT, balance REAL);'
   ```
2. **Connections → New connection**, engine *Demo*, id `demo_src`, option *Poison ratio* `0.05`
   (5 % of inserts will carry a NULL name so you can see the dead-letter queue). *Test*, *Save*.
3. **Connections → New connection**, engine *SQLite*, id `sqlite_dst`, engine option
   *File path* `/tmp/demo-dest.sqlite3`. *Save*.
4. **Connections → ☑ CDC prerequisites** on `demo_src`, tables `demo.Customers` – this is the
   checklist you will run against a real SQL Server before touching it.
5. **Mappings → New mapping**: browse source `demo_src` → `demo.Customers`, destination
   `sqlite_dst` → `main.customers`, click **Auto-map** (names are matched, the primary key
   `CustomerID` is mapped to `id`), **Save**.
6. **Pipelines → Define pipeline**: id `demo`, source `demo_src`, destination `sqlite_dst`, tick
   the mapping, **Save**, then ▶ **Start**. Counters start moving within seconds.
7. **Snapshots → New snapshot job**: mapping `demo_customers`, pipeline `demo`, chunk size 500,
   **Create**. Watch the progress bar; the job hands its start position to the pipeline so
   changes made during the load are not lost.
8. **Failures**: the poisoned rows appear as one *fixable* pattern with the suggestion
   *set a default value*. Click **Apply: set_default_value**, enter `unknown` – the mapping is
   changed and the pattern resolved. Restart the pipeline to pick the mapping up.
9. **Dashboard / Metrics / Audit trail** show throughput, lag, alerts and the full history.

## Your first real pipeline

1. Save the source and destination connections (use `${env:NAME}` or `${file:/path}` for passwords –
   they are resolved at connect time and never written to disk).
2. Run **CDC prerequisites** on the source for every table you want to capture. Each failed check
   comes with the exact statement/setting to fix. Use **Enable capture** once the database-level
   checks pass (needs the privileges listed in [Connections](03-connections.md)).
3. Create one mapping per table ([Mappings](04-mappings.md)). Set *key columns* explicitly if the
   destination has no primary key.
4. Define and start the pipeline ([Pipelines](05-pipelines-and-snapshots.md)); it starts from the
   earliest retained change unless you choose *latest*.
5. Create snapshot jobs for the tables that need an initial load – with a rate cap and a
   maintenance window on production systems.
6. Enable the alert rule sets you want and a notification channel
   ([Monitoring](06-monitoring-and-alerting.md)), then read the [runbook](07-operations-runbook.md)
   before going live.
