# 3. Storage – are files a good option for the instance data?

**Short answer: not as they were implemented, and not as the primary store for a
production instance. JSON files are fine as an *interchange / GitOps format*;
the live state needs a transactional store. The default is now SQLite in WAL
mode; the JSON backend remains available but hardened.**

## 1. What the original implementation did

```
config/connections.json   config/mappings.json   config/worksets.json   config/lsn_state.json
history/cdc_events_YYYYMMDD.json
```

Every mutation: mutate an in-memory dict → `open(path, 'w')` → `json.dump`.
Every read: from the in-memory dict of *this process*.

## 2. Problems under parallel / concurrent access

| Problem                                                         | Consequence                                                                                                                                                                                                        | Reproduced by                                                        |
|-----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| **Non-atomic write** (`open('w')` truncates first, then writes) | A crash, `kill -9`, OOM, or a full disk between truncate and write leaves a 0-byte or half-written file. On restart `load_*` catches the exception and starts **with an empty configuration** – all mappings gone. | `test_atomic_write_leaves_no_partial_file` shows the fixed behaviour |
| **No locking between threads**                                  | FastAPI handles requests concurrently; two `PUT /mapping/update` calls interleave `dict → dump` and the last writer wins silently (lost update).                                                                   | `test_threads_do_not_lose_updates`                                   |
| **No locking between processes**                                | `uvicorn --workers 2`, a CLI/cron job, or an operator editing the file while the app runs: each process has its own stale dict and overwrites the others' changes.                                                 | `test_multiprocess_safety`                                           |
| **No cache invalidation**                                       | A process never notices that another one changed the file.                                                                                                                                                         | `test_json_store_sees_external_modification`                         |
| **Write amplification**                                         | `lsn_state.json` is rewritten on every poll for every table (every 5 s × N tables). On NFS/CIFS this alone can take the app down.                                                                                  | –                                                                    |
| **O(n²) history**                                               | each flush re-reads and rewrites the whole day file.                                                                                                                                                               | –                                                                    |
| **No versioning / audit trail**                                 | impossible to answer "who changed this mapping and when", which the migration programme needs for transparency.                                                                                                    | –                                                                    |
| **Secrets in plaintext**, duplicated in `worksets.json`.        | –                                                                                                                                                                                                                  | –                                                                    |

None of these is theoretical: a bank-scale pipeline with hundreds of tables
polls often enough that a partial write *will* eventually coincide with a
restart.

## 3. Options considered

| Option                                        | Concurrency                                                                                                                             | Durability           | Ops burden        | Multi-node                           | Verdict                                                      |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------------------|-------------------|--------------------------------------|--------------------------------------------------------------|
| JSON files (as-is)                            | none                                                                                                                                    | none                 | none              | no                                   | ❌                                                            |
| JSON files + atomic rename + flock + versions | per-collection global lock, whole-file rewrite per change                                                                               | good                 | none              | no (shared FS only)                  | ✅ acceptable for dev / GitOps export                         |
| **SQLite, WAL mode**                          | readers never block writers; single writer serialised by SQLite with busy timeout; safe across threads *and* processes on the same host | transactional, fsync | none (one file)   | no                                   | ✅ **default**                                                |
| PostgreSQL (same schema)                      | full                                                                                                                                    | full                 | a database to run | yes (HA, several synchroniser nodes) | ✅ for multi-node; `ConfigStore` interface is designed for it |
| etcd / Consul                                 | full                                                                                                                                    | full                 | cluster           | yes                                  | over-engineered for this                                     |

## 4. What was implemented

`backend/storage/`:

* `ConfigStore` interface – `collection → key → JSON document`, every document
  carries a `version`; `put(expected_version=n)` raises `ConflictError` on a stale
  write (optimistic concurrency, the API can expose `If-Match` semantics).
* `SqliteConfigStore` – WAL, `busy_timeout`, `BEGIN IMMEDIATE` write
  transactions, per-thread connections, `put_many` is atomic (e.g. "exactly one
  active working set"), `document_history` table (who/when/what) – the audit
  trail for configuration changes.
* `JsonFileConfigStore` – kept for compatibility: temp-file + `fsync` + `os.replace`
  (+ directory fsync), cross-process advisory lock (`flock` / `msvcrt.locking`),
  mtime-based invalidation, `_meta_version`.
* `factory.open_store()` – backend selection (`CDCSYNC_CONFIG_BACKEND=sqlite|json`),
  one-time migration of the legacy files (originals renamed to `*.migrated-<ts>`).
* `secrets.py` – `${env:VAR}`, `${file:/path}`, `${vault:...}` references resolved
  at connect time; `redact()` for logs/API.
* `ConfigManager` keeps its public API, so nothing in the UI changed.
* Import/export stays JSON (`export_config` / `import_config`) – that is the
  right use of files: reviewable, diffable, versionable in git.

The same approach is used for the runtime data that grows: the audit trail and
dead-letter queue are an SQLite database (`history/audit.sqlite3`, WAL), not
JSON arrays.

## 5. Recommendation for the bank deployment

1. Single synchroniser node (the common case): **SQLite** (default). Back up the
   `config/state.sqlite3` file with the normal file backup (it is consistent at any
   point in time thanks to WAL) – or export to JSON nightly and commit to git.
2. Several synchroniser nodes / HA: implement `ConfigStore` on PostgreSQL (the
   schema is two plain tables; ~150 lines) and point all nodes at it. Positions
   then become visible cluster-wide, which also enables fail-over of a pipeline
   to another node.
3. Never store passwords in the store: use `${env:...}` / `${file:...}` and
   mount secrets from the platform's secret manager.
