# 5. Pipelines & snapshots

## Pipelines (continuous replication)

A pipeline = one source connection → one destination connection + a set of table mappings.
Define it in **Pipelines → Define pipeline** or `POST /api/pipelines`:

```json
{"id": "customers", "source_connection_id": "crm", "destination_connection_id": "core",
 "mapping_ids": ["crm_customer", "crm_address"], "auto_start": true,
 "options": {"initial_position": "earliest", "insert_mode": "upsert", "batch_size": 500,
             "batch_wait_s": 0.5, "expect_traffic": true}}
```

| Option                        | Meaning                                                                                                          |
|-------------------------------|------------------------------------------------------------------------------------------------------------------|
| `initial_position`            | `earliest` = from the oldest change the source still retains; `latest` = from now (use together with a snapshot) |
| `insert_mode`                 | `upsert` (idempotent, recommended) or `insert`                                                                   |
| `batch_size` / `batch_wait_s` | how many events are applied per destination round trip / how long to wait to fill a batch                        |
| `expect_traffic`              | lets the *silent source* alert rule fire when a normally busy source goes quiet                                  |
| `auto_start`                  | start with the application                                                                                       |

**Start / Pause / Resume / Stop** are immediate. *Pause* holds events (nothing is applied,
positions do not advance, the source reader blocks on back-pressure); *Stop* commits the last
position and releases the source. A stopped pipeline resumes exactly where it left off.

What the card shows: read / applied / failed (→ dead-letter queue) / queue depth / lag
(source commit → destination apply) / uptime, the committed positions per table (SQL Server)
or per source (other engines), and the *health facts* – the numbers the alert rules see
(retention margin, capture errors, connection errors, idle time…).

### Delivery guarantees

* A position is committed **after** the batch is applied or parked in the dead-letter queue.
  A crash replays the tail; upserts make the replay harmless (at-least-once, idempotent).
* Events of one table are applied in source order. Failed rows never block the stream: a
  failing batch is retried row by row, the bad rows go to the DLQ with the full payload.
* If the source has already purged changes after the committed position (retention exceeded),
  the SQL Server connector **refuses to continue** and the pipeline enters `error` – the
  `position_before_min` alert fires. The remedy is *reset positions* + a new snapshot, never a
  silent skip.

### Working sets (1.x compatibility)

The *Working sets* tab keeps the 1.x concept: a named source/destination pair with a list of
mappings, one of which is *active*; **Start sync** builds a pipeline named `ws_<id>` and creates
snapshot jobs for mappings with *initial snapshot* enabled. New deployments should define
pipelines directly – they are more explicit and several can run at once.

## Snapshots (initial load)

A snapshot job copies a table in **keyset-paginated chunks** (`WHERE key > last ORDER BY key LIMIT n`):
every chunk is an independent index seek, progress is persisted after each chunk (a crash or a
pause resumes at the last key), rows are written as upserts in one transaction per chunk and
the destination stays usable throughout. Poison rows go to the dead-letter queue instead of
blocking the job.

Create jobs in **Snapshots → New snapshot job** or `POST /api/snapshots`:

| Field                                                                 | Notes                                                                                                                                                         |
|-----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mapping_id`, `source_connection_id`, `destination_connection_id`     | the source may be a different connection than the pipeline's – e.g. a readable secondary                                                                      |
| `pipeline_id`                                                         | the job records the source position before the first chunk and hands it to this pipeline if it has none yet → changes during the load are replayed afterwards |
| `key_columns`                                                         | chunk key (source names); defaults to the primary key; must be unique and indexed                                                                             |
| `where`                                                               | range filter – split a huge table into several jobs (`CustomerID BETWEEN 1 AND 50000000`)                                                                     |
| `chunk_size`, `min_chunk_size`, `max_chunk_size`, `max_chunk_seconds` | adaptive: halves when a chunk is slower than `max_chunk_seconds`, grows ×1.5 when fast                                                                        |
| `max_rows_per_second`, `pause_between_chunks_ms`                      | hard throttles                                                                                                                                                |
| `windows`                                                             | `[{"start": "22:00", "end": "06:00", "days": [0,1,2,3,4]}]` – outside the window the job waits                                                                |
| `max_retries`                                                         | per chunk, with exponential back-off                                                                                                                          |

Controls: start, pause, resume, cancel, reset (start over; already copied rows are simply
upserted again), delete. The card shows rows/chunks/rate/average chunk time/ETA, the last key
and the source position captured at start.

### Playbook for very large tables (the 16 TB case)

1. **Never snapshot from the production primary.** Point the job's source connection at an
   Availability-Group readable secondary (`options.read_only_intent`) or a restored copy –
   positions are the same LSNs, the hand-off still works. If only the primary exists, enable
   RCSI first (the prerequisite check tells you) so chunks read versioned rows.
2. **Pick the chunk key**: the clustered primary key. For heaps or non-clustered keys choose an
   indexed monotonically increasing column as `key_columns`.
3. **Start small**: chunk 5 000, `max_chunk_seconds` 5; the job adapts.
4. **Parallelise by range**: several jobs on the same mapping with disjoint `where` filters
   (by id range, partition or year). `CDCSYNC_MAX_PARALLEL_SNAPSHOTS` caps concurrency.
5. **Protect business hours** with `windows` and a `max_rows_per_second` ceiling; pause from the
   UI when the DBA asks.
6. **Start the pipeline first**, then the snapshot with `pipeline_id` set. Ordering is then
   irrelevant: both write upserts.
7. **Verify per key range** (Mappings → Verify, row counts) – not with `COUNT(*)` over 16 TB.
8. **Expect weeks, and make it visible**: the progress estimate uses the key range (`key_lo`/`key_hi`),
   the rule set *snapshot* alerts on stalled or failing jobs.
