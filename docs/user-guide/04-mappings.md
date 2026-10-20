# 4. Mappings & transformations

A *mapping* describes how one source table (or query) becomes one destination table. Mappings
are created in **Mappings** or with `POST /api/admin/mapping/create`; the full schema is in the
[configuration reference](../reference/configuration.md#mapping).

## Two kinds of mapping

| `mapping_type` | Driven by                                                                                           | Use for                                                              |
|----------------|-----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `table`        | change data capture – a pipeline streams every INSERT/UPDATE/DELETE of `source_schema.source_table` | the normal case: continuous replication with reshaping               |
| `sql`          | a `source_query` executed on demand (`POST /api/operations/mapping/{id}/execute`) or on a schedule  | periodic aggregates, reference data, anything that is not row-by-row |

## Table mappings

### Column mappings

Each entry of `column_mappings` produces one destination column:

| Field                                        | Meaning                                                                                                                                                                                                                                                                                  |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `source_column`                              | copied 1:1 (renamed)                                                                                                                                                                                                                                                                     |
| `source_columns` + `transformation`          | several source columns combined by an expression                                                                                                                                                                                                                                         |
| `transformation`                             | a **DuckDB SQL expression** over the source columns, e.g. `upper("City")`, `concat_ws(' ', "FirstName", "LastName")`, `json_object('first', "FirstName", 'last', "LastName")`, `"Price" * (1 + "TaxRate")`, `try_cast("Amount" AS DECIMAL(18,2))`. Quote identifiers with double quotes. |
| `default_value`                              | used when the (transformed) value is NULL – the standard remedy for `NOT NULL` destinations                                                                                                                                                                                              |
| `ignore_changes`                             | the column is never written (preserve destination-only or manually maintained data)                                                                                                                                                                                                      |
| `auto_generate` + `auto_generate_expression` | value generated on the destination side: `on_insert` (every write) or `on_init` (only when the row is first inserted). Expressions are DuckDB: `uuid()`, `now()`, `current_date`, `'batch-' \|\| strftime(now(), '%Y%m%d')`                                                              |

> **Changed in 2.0:** all expressions are evaluated in **DuckDB**, on the whole batch, before the
> rows are written. SQL-Server-specific functions from the 1.x documentation (`NEWID()`,
> `GETDATE()`, `JSON_OBJECT`) must be written as `uuid()`, `now()`, `json_object(...)`.
> `transformation_engine = "sql"` is accepted for backwards compatibility but not executed.

**Auto-map** in the editor matches columns by name (case/underscore insensitive) and maps the
source primary key to the destination primary key even when the names differ
(`CustomerID` → `id`). Unmatched source columns are listed with an empty destination – fill in or
remove them.

### Key columns

UPDATE and DELETE must find the right destination row. The key is resolved in this order:

1. `key_columns` on the mapping (destination names) – **set this explicitly whenever you can**;
2. the destination table's primary key (read from the catalog);
3. the source primary key mapped through the column renames;
4. a name heuristic (`id`, `*_id`) – flagged in the audit trail and by the
   `unsafe_key_heuristic` alert rule.

### Sync behaviour

| Field                                            | Default  | Notes                                                                                                                                    |
|--------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------|
| `sync_inserts` / `sync_updates` / `sync_deletes` | true     | disable deletes to keep a history table, etc.                                                                                            |
| `sync_mode`                                      | `upsert` | `upsert` makes inserts idempotent (replays, snapshot overlap); `incremental` = plain inserts; `full` is only meaningful for SQL mappings |
| `perform_initial_snapshot`                       | false    | when the working set is started, a snapshot job is created automatically; you can always create one by hand                              |
| `batch_size`                                     | 1000     | rows per destination batch                                                                                                               |
| `enabled`                                        | true     | disabled mappings are skipped by pipelines – the *deprecated* failure pattern proposes this                                              |

### Batch-level DuckDB scripts

For transformations that are not expressible per column (filtering, de-duplication,
aggregation, pivots) set `use_duckdb_transformation = true` and either
`duckdb_script_content` (inline) or `duckdb_script_name` (a file in `duckdb_scripts/`). The
batch is loaded into `{source_table}`; the script's **last SELECT** defines the destination rows.
Output columns are renamed through the column mappings, so they may keep source names:

```sql
-- keep only customers with an e-mail address, latest version per id
CREATE OR REPLACE TEMP TABLE t AS
SELECT "CustomerID", "Name", "Email", upper("City") AS "City"
FROM {source_table} WHERE "Email" <> '';
SELECT * FROM t;
```

Templates (`basic_filter`, `aggregation`, `deduplication`, `data_enrichment`, `pivot_unpivot`,
`join_enrich`) and the placeholder list are described in
[`duckdb_scripts/README.md`](../../duckdb_scripts/README.md). Scripts run for INSERT/UPDATE batches
and snapshot chunks; DELETEs use the key columns only.

## SQL mappings

`source_query` is executed on the working set's source connection, the result written to the
destination according to `sync_mode`: `full` (truncate + insert), `incremental` (insert),
`upsert` (needs `key_columns`). `POST /api/operations/mapping/execute-all-sql` runs every
enabled SQL mapping, grouped per destination table in one transaction.

## Validate, test, verify

* **Validate / test** (`POST /api/admin/mapping/{id}/test`) – configuration check for table
  mappings, a 10-row probe for SQL mappings.
* **Verify** (`POST /api/monitoring/verify`) – row counts plus random samples compared between
  source and destination (uses `TABLESAMPLE`, never a full sort of the source).
* **History** – every saved version with timestamp and author (SQLite store).

## Multi-source mappings and the visual designer (1.x, legacy UI)

Two 1.x features remain available under `/legacy/admin` and through the API:

* **Multi-source mappings** (`is_multi_source`, `sources[]`, `merge_pattern`) join/union several
  source tables (`JOIN`, `UNION`, `INTERSECT`, `EXCEPT`, custom SQL) into one destination with a
  conflict strategy. Operationally usable for previews and low-volume merges; the runtime
  re-executes the merge for every change (see the developer architecture notes before using it
  for large tables).
* **Visual workflow designer** – drag-and-drop nodes (source database/table, column selector,
  JSON aggregation, concatenation, custom SQL, DuckDB transform, filter, de-duplicate,
  destination) compiled into an ordinary table mapping (`POST /api/admin/workflow/compile/{id}`).
  The compiled mapping then behaves exactly like a hand-written one.
