# meshdb — Progress Report

**Updated:** 2026-07-19 · **Steps complete:** 5 of 12 · **Status:** single-node engine runs; **VFS spike PASSED**

---

## What meshdb is meant to be

A high-availability, multi-write SQLite server. Users issue arbitrary SQL and define their
own tables. Runs from a 0.5 GB / 1 CPU instance (three containers, ~150 MB each) and scales
to a few hundred GB. **No modifications to SQLite source** — loadable extensions and custom
VFSes are allowed, forks are not.

"Multi-write" is delivered at the cluster level: many shards, each single-writer, with
primaries spread across nodes. SQLite serializes writes at the file level in the pager and
no unpatched design escapes that, so concurrency for writers comes from sharding.

---

## Where things stand

| # | Step | Status | Evidence |
|---|---|---|---|
| 0 | VFS capture spike | **done — PASSED** | `tests/vfs_capture.rs` (7) + `src/vfs/wal.rs` (7) |
| 1 | PRAGMA profiles, connection lifecycle | **done** | `tests/storage_open.rs` (8) |
| 2 | Execution + batching writer | **done** | `tests/writer.rs` (6) |
| 3 | Reader pool | **done** | `tests/reader_pool.rs` (5) |
| 4 | WAL checkpointing | **done** | `tests/checkpoint.rs` (5) |
| 5 | Shard manager (LRU, thread affinity) | not started | |
| 6 | Benchmarks + cgroup memory test | not started | |
| 7 | VFS capture productionized | not started | **unblocked** |
| 8 | Replication + per-shard bootstrap | not started | **unblocked** |
| 9 | Per-shard merkle verification | not started | |
| 10 | Cluster: election, fencing, failover | not started | |
| 11 | Shard placement + move | not started | |
| 12 | Read consistency levels | not started | |

**38 Rust tests + 26 CLI assertions. Clippy clean, fmt clean.**

---

## What is built

### Connection layer (`src/storage/pragma.rs`, `open.rs`)
PRAGMA settings live in one place as data. `apply()` sets them, `verify()` reads every one
back and errors on mismatch — several PRAGMAs silently no-op rather than failing, and this
layer's failure mode is silent divergence between nodes.

Writer-before-reader ordering is enforced by a `WriterOpened` token that only `open_writer`
can mint, not by a comment. A read-only connection cannot create the `-wal`/`-shm` sidecars.

### Writer (`src/storage/writer.rs`)
One thread owns one connection for its lifetime; callers submit through cloneable handles
and never see `SQLITE_BUSY`. Group commit with **no linger timer** — requests that queue
while `COMMIT` is fsyncing get drained into the next transaction, so batching is
self-tuning: size 1 when idle, growing exactly as much as fsync cost justifies under load.

Per-request `SAVEPOINT` isolation means one caller's constraint violation does not roll back
its batch peers. Errors are classified as deterministic logic failures (savepoint rolled
back, batch continues) or machine faults such as `DiskFull` (whole transaction aborted).

### Reader pool (`src/storage/reader.rs`)
N threads, each owning a read-only connection, racing to receive from one shared queue.
Bounded queue with `try_send` — a full queue is **rejected**, not queued, so overload
surfaces as a fast error rather than unbounded memory. Per-query deadline enforced through
SQLite's progress handler, which can stop a runaway query that never returns a row.

### Checkpointer (`src/storage/checkpoint.rs`)
Runs on the writer thread **between** transactions. `wal_autocheckpoint` is disabled
because SQLite would otherwise do this work inside `COMMIT`, stalling a batch callers are
blocked on. Escalation ladder: below soft limit do nothing → `PASSIVE` → after repeated
stalls above the hard limit, a blocking `TRUNCATE` with a loud warning.

### WAL capture VFS (`src/vfs/passthrough.rs`, `wal.rs`)
A pass-through SQLite VFS delegating every call to the default VFS, so the database is an
ordinary on-disk file with ordinary durability. Successful writes to the `-wal` file are
also fed to a parser that reconstructs committed transactions as page frames. Followers
apply those pages directly and never execute SQL — which is why non-deterministic functions
and per-machine errors cannot make a replica diverge.

### CLI (`src/main.rs`, `src/db.rs`)
```
meshdb <db-path>              interactive shell
meshdb <db-path> -c "SQL"     one statement
meshdb <db-path> -f <file>    statements from a file
```
Shell commands: `.help` `.stats` `.tables` `.quit`. Statements route by
`Statement::readonly()` — SQLite's own classification, not text matching.

---

## What is tested

| Property | Test |
|---|---|
| WAL mode actually active; settings took effect | `writer_opens_in_wal_mode`, `writer_pragmas_actually_take_effect` |
| `verify()` catches drift (not vacuously passing) | `verify_detects_a_setting_that_drifted` |
| Reader cannot write; cannot open a fresh DB alone | `reader_cannot_write`, `reader_alone_cannot_open_a_fresh_database` |
| One failed request does not roll back batch peers | `a_failed_request_does_not_roll_back_its_batch_peers` |
| Bad SQL is a result, not a crash | `a_rejected_statement_is_not_an_error` |
| 16 concurrent writers, no busy errors, no lost writes | `concurrent_writers_all_succeed_without_busy_errors` |
| **Group commit actually batches** | `group_commit_actually_batches_under_load` |
| 12 threads read concurrently | `many_threads_read_concurrently` |
| Reads and writes proceed together | `readers_do_not_block_the_writer` |
| Full queue sheds load | `a_full_queue_sheds_load_instead_of_growing` |
| Runaway query cancelled promptly; connection reusable | `a_runaway_query_is_cancelled_at_the_deadline` |
| WAL bounded under sustained writes | `wal_stays_bounded_under_sustained_writes` |
| **Long reader stalls checkpointer; ladder escalates** | `a_long_lived_reader_stalls_the_checkpointer_and_forces_escalation` |
| Checkpointing loses no data across reopen | `checkpointing_does_not_lose_or_corrupt_data` |
| VFS is transparent; DB readable with no custom VFS | `ordinary_sqlite_works_through_the_vfs` |
| Commit frames detected | `commit_frames_are_detected` |
| **Follower reconstructed byte-identically** | `a_follower_is_reconstructed_byte_identically` |
| **Negative control: a dropped txn IS detected** | `dropping_one_transaction_is_detected_as_divergence` |
| **Holds under page reuse, 4.6 MB, freelist churn** | `reconstruction_holds_under_page_reuse_and_churn` |
| Survives checkpoints and salt rotation | `capture_survives_checkpoints_and_wal_resets` |
| Survives concurrent readers + TRUNCATE | `capture_survives_concurrent_readers_and_truncate` |
| **In-place header rewrite honoured** | `a_rewritten_header_carrying_the_commit_marker_is_honoured` |
| Reused WAL slot takes the newer page | `a_reused_slot_takes_the_newer_page` |

### Measured, not assumed

- **Group commit: 801 requests committed in 63 transactions** (16 threads × 50 inserts),
  mean batch 12.71, max 15 — ~12.7× fewer fsyncs than one-per-request. A single serial CLI
  client shows `mean_batch=1.00`, which is correct: batching appears only when there is
  contention to absorb.
- **`journal_mode = WAL` alone does not create the `-wal`/`-shm` files.** Verified on SQLite
  3.51.3 and 3.53.2. An empty `IMMEDIATE` transaction materializes them, which is why
  `materialize_wal` exists.
- **`BEGIN` classifies as read-only** under `Statement::readonly()`. Without an explicit
  guard it would route to a reader, appear to succeed, and do nothing.
- **A stalled `PASSIVE` checkpoint still costs O(WAL size)** to discover it cannot advance.
  Found when a test took >60s; fixed with exponential stall backoff.
- **SQLite rewrites WAL frame headers in place.** Measured on 3.53.2: a churn workload wrote
  4059 frame headers, of which **1641 were in-place rewrites** over a page already written
  (`walRewriteChecksums` fixing checksums and stamping the commit marker). The WAL is an
  array of *slots*, not an append log. An append-based parser loses the commit marker and
  strands the transaction — which is exactly what the first implementation did, passing
  every light test and failing only under page-reuse churn.
- **The VFS write pattern itself is clean**: header then page data, no odd-sized writes, in
  the workloads traced. `WalCapture::take_trace()` reproduces this analysis when the bundled
  SQLite version is bumped.

---

## Outstanding

### Step 0 result: **PASSED**

All six criteria met, on SQLite 3.53.2:

1. ordinary SQLite works unchanged through the VFS, and the resulting file is readable by a
   plain SQLite with no custom VFS at all
2. WAL header and frame headers parse
3. commit frames are detected
4. a follower is reconstructed **byte-identically**, `PRAGMA integrity_check` = ok
5. survives checkpoints, WAL resets, and salt rotation
6. survives concurrent readers and `TRUNCATE` checkpoints

Verified as non-vacuous: `dropping_one_transaction_is_detected_as_divergence` confirms the
byte-comparison can fail, and the churn test builds a 4.6 MB database with freelist reuse
and index rebuilds.

**The physical-replication architecture is viable.** The fallbacks — logical replication
with its determinism and error-classification divergence, or shared-storage failover with no
read replicas — are not needed.

**Residual risk:** SQLite's WAL *format* is documented, but its *write pattern* is not an API
contract. Contained by pinning the bundled SQLite version exactly. When bumping it, re-run
the trace comparison (`take_trace()`) before trusting capture.

### Known gaps in what exists

| Gap | Impact |
|---|---|
| No structured logging — `eprintln!` only | Not operable as a service |
| No `Value` params; SQL is string-formatted | Injection-unsafe for untrusted callers; needed for determinism rewriting |
| `first_keyword` guard cannot see past a leading comment | `-- x\nBEGIN` slips through. Real guard is the authorizer (step 8) |
| No `VACUUM` path | Rejected outright; needs an out-of-transaction maintenance mode |
| Checkpoint test suite takes ~20s | Real fsyncs; acceptable but the slowest thing in CI |
| Writer has no backpressure | Unbounded `mpsc`; readers shed load but writers do not |
| Capture holds an uncommitted txn fully in memory | A huge single transaction is unbounded; needs a spill or cap |
| `capture_for` must be called before open | Registering later silently misses frames; not enforced by types |
| No graceful shutdown for in-flight work | `Drop` joins, but a long batch delays exit |

### Design decisions not yet made

- **Shard count default** (64 proposed, 16–256). Immutable after cluster creation and caps
  ultimate scale — the one number a user cannot revise.
- **Single-node 3-container mode:** buys rolling upgrades, not availability. Does not
  survive host failure, disk failure, or OOM. At a few hundred GB it also costs 3× disk and
  3× write I/O. Needs an explicit decision on whether it is supported at scale.
- **Shard rebalancing:** moving a live shard means stop writes, drain, copy, hand off the
  fencing token, resume. Undesigned.

### Permanent limitations (by design)

- **No cross-shard transactions, ever.** WAL mode gives no atomic commit across ATTACHed
  databases. This shapes every schema users write and must be visible in the API.
- **No parallel writes within one shard.** SQLite's file-level write serialization.
- **No client-held transactions** across round trips — would pin the writer and defeat
  batching.
- **SQLite version lockstep across the cluster**, because replication is byte-level.

---

## Running it

```bash
cargo build
cargo test                 # 24 tests, ~26s (checkpoint tests dominate)
./scripts/smoke.sh         # 26 end-to-end CLI assertions
./target/debug/meshdb /tmp/demo.db
```

## Layout

```
src/
  config.rs               PragmaProfile, ReaderPoolConfig, CheckpointConfig
  error.rs                every failure mode, with actionable messages
  db.rs                   routing facade: classify, guard, dispatch
  main.rs                 CLI
  storage/
    pragma.rs             apply() / verify()
    open.rs               open_writer / open_reader / WriterOpened token
    exec.rs               execution, Value, error classification
    writer.rs             writer thread, group commit, savepoints
    reader.rs             reader pool, backpressure, deadlines
    checkpoint.rs         escalation ladder, stall backoff
tests/                    storage_open, writer, reader_pool, checkpoint
scripts/smoke.sh          CLI end-to-end
```
