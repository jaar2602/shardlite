# meshdb — Progress Report

**Updated:** 2026-07-19 · **Steps complete:** 8 of 12 · **Status:** sharded single-node engine with WAL capture wired to a sink; floor profile measured

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
| 5 | Shard manager (LRU, thread affinity) | **done** | `tests/shard.rs` (9) + `src/shard/` (6) |
| 6 | Benchmarks + cgroup memory test | **done** | `benches/write_throughput.rs`, `src/bin/memcheck.rs`, `scripts/bench.sh` |
| 7 | VFS capture productionized | **done** | `tests/capture_wiring.rs` (9) |
| 8 | Replication + per-shard bootstrap | not started | **unblocked** |
| 9 | Per-shard merkle verification | not started | |
| 10 | Cluster: election, fencing, failover | not started | |
| 11 | Shard placement + move | not started | |
| 12 | Read consistency levels | not started | |

**70 Rust tests + 31 CLI assertions. Clippy clean, fmt clean.**

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

### Frame capture and sinks (`src/replication/`, wired in `shard/writer_fleet.rs`)
Shard writer connections can be routed through the capture VFS, with committed frames
drained to a [`FrameSink`] **on the writer thread after every batch**. That placement is the
whole design: retained memory is bounded by one batch rather than by the write rate, and a
slow sink slows the writer, fills the bounded write queue, and surfaces to callers as
`WriterBusy` — backpressure through the mechanism that already exists.

**Capture defaults to off.** With no replication target it buys nothing and costs both
throughput and memory. Frames are never dropped to stay under the retention cap; exceeding
it raises `CaptureOverflow` and refuses further writes, because a silently truncated stream
is the divergence physical replication exists to prevent.

### Data-directory manifest (`src/shard/manifest.rs`)
`shard_count` is immutable — changing it re-routes every key — so it is recorded at
creation and any later disagreement is refused at open, naming both values. A boring
line-based text file: readable with `cat`, no serialization dependency.

### Shard manager (`src/shard/`)
Virtual shards fixed at creation (default 64, range 1–256), never split — rebalancing moves
whole shards, so data is never rehashed. Routing is FNV-1a, implemented here rather than
taken from `DefaultHasher`, which is explicitly not stable across Rust releases; a hash
change would silently re-route every key.

Shards map to writer threads by `id % writer_threads`, so every shard keeps exactly one
writer while the thread count stays bounded. Each thread holds an LRU of open connections,
decoupling shard count from resident memory. Readers share one queue and keep their own
per-thread LRU.

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
| **64 shards stay within the LRU's connection ceiling** | `sixty_four_shards_are_served_by_a_bounded_number_of_connections` |
| **A never-written shard is still queryable** | `a_never_written_shard_can_still_be_queried` |
| Data survives heavy eviction churn | `a_reopened_shard_still_serves_readers` |
| Keys route to the same shard for read and write | `writes_and_reads_route_to_the_same_shard` |
| Routing hash is pinned against change | `routing_is_stable_and_spread` |
| LRU never exceeds capacity; failed opens uncached | `never_exceeds_capacity`, `a_failed_open_is_not_cached` |
| Untouched shards create no files | `only_touched_shards_create_files` |
| **Manifest refuses a changed shard count** | `refuses_a_different_shard_count` |
| **Parameters bind, never interpolate** | `parameters_are_bound_not_interpolated` |
| Every `Value` kind round-trips | `every_value_kind_round_trips_through_binding` |
| **Write queue sheds load when full** | `a_full_write_queue_sheds_load_instead_of_growing` |
| **Follower rebuilt from sink output, byte-identical** | `a_follower_is_reconstructed_from_what_the_sink_received` |
| Capture is off by default | `capture_is_off_by_default_and_costs_nothing` |
| Capture survives LRU eviction and reopen | `capture_survives_lru_eviction_and_reopen` |
| **Retention bounded by one batch** | `retention_stays_bounded_because_the_writer_drains_every_batch` |
| Overflow fails writes, never drops frames | `overflow_fails_writes_rather_than_dropping_frames` |
| Capture does not change the on-disk file | `captured_and_uncaptured_databases_are_identical_on_disk` |

### Benchmarks — 1 CPU / 1 GB container, real fsync (~1.5 ms)

Run with `./scripts/bench.sh`. 4000 writes per configuration.

**`synchronous = FULL`** (durable):

| C | A-contend | B-batched | B-nobatch | B/B-nb | mean batch |
|---|---|---|---|---|---|
| 1 | 631/s | 708/s | 725/s | 0.98x | 1.00 |
| 4 | 609/s | 1,645/s | 727/s | 2.26x | 2.91 |
| 16 | 674/s **(1000 errors)** | 7,353/s | 696/s | 10.6x | 14.8 |
| 64 | 640/s **(2108 errors)** | **17,537/s** | 733/s | 23.9x | 60.1 |

- **B-batched scales 24.8x** from C=1 to C=64. **B-nobatch is flat within 5%** (725 → 733).
  Identical serialization, only batching differs — so the win is attributable to batching,
  which is the entire reason the B-nobatch variant exists.
- **A-contend does not merely degrade, it fails.** At C=64, 2108 of 3968 writes error out,
  and p999 reaches **1755 ms** against 24 ms for B.

**Peak RSS**, 64 shards, hard 150 MB cgroup cap (OOM-kill on breach):

| On disk | Capture | Peak RSS |
|---|---|---|
| 344 MB | off | 46 MB |
| 1371 MB | off | **47 MB** |
| 1371 MB | **on** (1577 MB streamed) | **47 MB** |

Data grew 4x; RSS moved 1 MB. Streaming 1577 MB through capture added **nothing**, because
the writer drains every batch. `open_now` sat at exactly 16 — the LRU ceiling.

Capture's CPU cost, measured separately by `cargo run --example vfs_overhead`: **2.7%**
(33,832 → 32,921 writes/s at 1 CPU with real fsync).

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
- **A read-only connection cannot create a database file**, which is what makes the
  writer's `ensure_open` load-bearing — a shard never written to has no file for a reader
  to open. Note the reasoning first written here was **wrong**: a *clean* close checkpoints
  the WAL into the main file and removes the sidecars, and a read-only connection opens
  that file happily, so ordinary LRU eviction does not break readers.
- **An LRU multiplies per-connection cache by its capacity.** 16 open writer connections at
  the single-database 8 MiB would be 128 MB — the whole container budget. Sharded profiles
  use 1 MiB writers and 512 KiB readers for this reason alone.
- **A blocking request/reply API caps queue depth at the caller count.** The write queue
  bound was initially untestable for this reason: 32 threads cannot fill a 1024-deep queue
  when each blocks until its batch commits. The first version of the backpressure test
  passed while shedding **zero** writes — it would have passed identically against an
  unbounded channel. Caught only by printing the number rather than asserting on it.
- **This host's `/tmp` completes an fsync in 0.3 us** — it is not flushing to durable
  media. A real fsync is 100 us to 10 ms. The first benchmark run was therefore measuring
  CPU and lock overhead only, and the group-commit result was meaningless: FULL and NORMAL
  were indistinguishable, and B/B-nobatch was 2.4x where the model predicted ~57x. The
  container's overlay filesystem does a real ~1.5 ms fsync, which is why `scripts/bench.sh`
  runs there. **Benchmark the storage, not just the code.**
- **The B/B-nobatch ratio tracks mean batch size only insofar as fsync dominates.** At
  `FULL`, C=64: ratio 23.9 against a mean batch of 60. At `NORMAL`, same batch size, ratio
  only 6.75 — because batching amortizes the *fsync*, not the per-statement CPU (prepare,
  bind, savepoint, b-tree insert). The original prediction of `ratio ~ mean_batch` was
  wrong; this is the better model.
- **The queue costs ~11x at a single client on fast storage.** `NORMAL`, C=1: raw SQLite
  41,593/s versus 3,859/s through the writer thread — the channel round trip dominates when
  there is no fsync to hide behind. The design is a *loss* for one client on cheap-sync
  storage and a 24x win for concurrent clients on durable storage.
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
| `first_keyword` guard cannot see past a leading comment | `-- x\nBEGIN` slips through. Real guard is the authorizer (step 8) |
| No `VACUUM` path | Rejected outright; needs an out-of-transaction maintenance mode |
| Checkpoint test suite takes ~20s | Real fsyncs; acceptable but the slowest thing in CI |
| `busy_timeout` ordering fix is unproven | Moving it before lock-taking statements is correct on its own merits, but neither test discriminates it — both pass with the bug reintroduced. The 1-CPU `SQLITE_BUSY`-at-open failure was never root-caused. |
| `capture_for` must be called before open | Registering later silently misses frames; not enforced by types |
| Overflow is per-shard and sticky | Once raised the shard refuses writes until the process restarts; no recovery path yet |
| No sink ships anywhere | `NullSink` discards and `MemorySink` is for tests; a network sink is step 8 |
| Cross-shard queries fan out in caller code | No query planner; `execute_all_shards` is the only helper, and it is not atomic |
| No graceful shutdown for in-flight work | `Drop` joins, but a long batch delays exit |

### Debt deliberately deferred

These do not get harder with time, so they were left rather than done now: structured
logging (still `eprintln!`), a `VACUUM` maintenance path, a cross-shard query planner, and
the ~20s checkpoint test suite.

### Design decisions not yet made

- **Shard count default.** Now enforced by the manifest, but the *default* is still open:
  the CLI uses 1 for usability, `ShardConfig::floor()` uses 64. A user who accepts the CLI
  default and later needs to scale must migrate.
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
