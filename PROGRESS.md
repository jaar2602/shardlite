# shardlite — Progress Report

**Updated:** 2026-07-21 · **Steps complete:** 9 of 12 · **Status:** replication converges; native TCP, HTTP, and JSON/TCP edges live; drivers in four languages (Phase 2); standalone web console built and verified (Phase 3)

---

## What shardlite is meant to be

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
| 8 | Replication + per-shard bootstrap | **done** | `tests/replication.rs` (11) + `src/replication/` (3) |
| 8b | Frame retention + networked follower | **done** | `tests/replica_net.rs` (8) + `src/replication/log.rs` (5) |
| 9 | Divergence detection | **done (content hash; merkle deferred)** | `tests/verify.rs` (4) + `src/storage/verify.rs` (12) |
| 10 | Cluster: election, fencing, failover | **done** | `tests/cluster.rs` (10) + `tests/quorum.rs` (5) + `tests/promotion.rs` (5) + `src/cluster/` (44) |
| 11 | Shard placement + move | **done** | `tests/cluster.rs` (16) + `src/cluster/placement.rs` (7) + `src/net/forward.rs` |
| 12 | Read consistency levels | **done** | `tests/promotion.rs` (11) + `tests/cluster.rs` (18) |

**253 Rust tests + 41 CLI assertions. Clippy clean, fmt clean. No known flaky tests.**

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

### Network transport (`src/net/`)
A length-prefixed bincode protocol, a thread-per-connection server, and a blocking client.

**Threads rather than async, deliberately.** Every request ends in the blocking writer or
reader fleets, so an async server would add a `spawn_blocking` hop per request in exchange
for scaling to many *idle* connections — not the shape of a node targeting 1 CPU and 150 MB.

The **connection cap is where load shedding actually binds**: with one in-flight request per
connection, the bounded write queue behind it cannot fill from connections alone. Refusals
are counted (`ServerStats::refused_at_capacity`) and logged, not silently dropped.

Distinctions the protocol keeps rather than flattening: a *rejected* statement (constraint
violation, bad SQL) is a result and reaches the client as one, while an *error* carries a
`retryable` flag so backpressure is distinguishable from a permanent fault. Length prefixes
are checked against a cap before allocating, so four bytes cannot make the server reserve
gigabytes.

Clients ask the server to route keys rather than reimplementing the hash — a client-side
copy that drifted would silently misroute every key.

### Cross-shard query planner (`src/query/`)
Reads fan out across shards and are merged. The governing rule is that **anything which
cannot be answered correctly is refused, never approximated** — a plausible-looking wrong
number is worse than an error, because nothing downstream can tell the difference.

Combinable: plain `SELECT` (concatenate), `ORDER BY` with optional `LIMIT` (per-shard sort,
merge, truncate), and `COUNT` / `SUM` / `MIN` / `MAX` — exactly the aggregates that are
associative, so combining partial results reproduces the global answer.

Refused with a reason: `JOIN` (rows that must meet live on different shards), `GROUP BY` and
`HAVING` (a group can span shards), `DISTINCT` (a value can appear on several), `AVG` (a mean
of means is not the mean — ask for `SUM` and `COUNT`), `OFFSET` (the rows skipped per shard
are not the rows to skip globally), set operations, CTEs, and any aggregate mixed with plain
columns.

**A fan-out is not a consistent snapshot.** Each shard is read at its own moment, so it can
observe a write on one shard and miss a concurrent one on another. There is no cross-shard
atomicity in this design, so that is a property of the answer rather than a gap to close.

### Logging (`tracing`)
The library emits through the `tracing` facade and installs **no** subscriber, so an
embedding consumer picks its own destination — or none, in which case the macros compile to
near-nothing. Only the binary installs one, behind the `cli` feature, writing to stderr so
logs never mix with query results on stdout. `SHARDLITE_LOG=debug` turns up detail; the default
is warnings and above.

Instrumented where an operator would actually be debugging: checkpoint stalls and TRUNCATE
escalation, shard open/evict, capture overflow and recovery, sink refusals, snapshot holds
being broken, replication gaps, epoch bumps, and WAL-conversion contention.

**Retries are counted, not just survived.** Retrying the WAL conversion fixes the failure
but would otherwise hide the contention that caused it. `wal_conversion_stats()` exposes
retries, contended opens, failures and the longest wait — surfaced in the shell's `.stats`
— and a conversion needing 8+ attempts or 250 ms+ logs at `warn` rather than `debug`,
because at that point it is saying something about the deployment rather than reporting
ordinary concurrency.

### Replication (`src/replication/`)
A follower applies **pages, never SQL** — the reason physical replication was chosen, since
nothing it does can be non-deterministic.

Every transaction carries a position `(epoch, lsn)`. `lsn` is dense within an epoch, and
density is what makes loss detectable: a jump means frames are missing, and the follower
refuses rather than applying across the hole. `epoch` covers the case density cannot — a
primary that restarts without knowing where it stopped bumps it, forcing re-bootstrap,
because guessing at continuity risks undetectable corruption. A **clean** shutdown persists
the position so the common restart continues the same epoch and no data is re-copied.

Follower crash safety rests on idempotence: pages are fsynced, *then* the position is
recorded. A crash between them replays transactions, which is harmless because writing the
same page twice gives the same page. The reverse order would silently skip them.

Bootstrap transfers are **chunked and resumable** (`src/replication/bootstrap.rs`): progress
is persisted per chunk, so an interruption continues rather than restarting — at a few
hundred GB, restarting is not a retry but a second outage. Each transfer carries a
`SnapshotId` (shard, epoch, LSN, size), and a partial whose identity does not match is
discarded rather than spliced: the primary's freeze can break, and grafting new bytes onto a
prefix of the old file gives a database corrupt in a way nothing detects, because every page
in it is individually valid.

Bootstrap splits into a fast writer-thread part and a slow caller-thread part. The writer
ships pending frames, checkpoints, records the position, and **freezes** the shard —
suspending checkpointing, which in WAL mode means committed pages stay in the WAL and the
main file cannot change. The copy then runs on the caller's thread. Measured on a 23.6 MB
shard: `begin_snapshot` 2.3 ms against a 22.9 ms copy, with concurrent writes to another
shard on the same writer thread unaffected. The writer-thread cost is proportional to *WAL*
size (capped at 16 MB) rather than database size, so it does not grow with the data.

The freeze cannot be held indefinitely — with checkpointing suspended the WAL grows without
bound, and filling the disk is worse than a failed snapshot. Past `snapshot_hold_max_wal`
the hold is broken and the snapshot invalidated, so the caller retakes it rather than
shipping a file that changed underneath.

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
shardlite <db-path>              interactive shell
shardlite <db-path> -c "SQL"     one statement
shardlite <db-path> -f <file>    statements from a file
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
| **A refused batch is requeued, not lost** | `frames_refused_by_a_sink_are_not_lost` (verified by removing the fix: sink saw `[1, 27]` instead of `[1..=27]`) |
| Overflow is reported when the backlog cannot shift | `overflow_fails_writes_while_the_backlog_cannot_be_shifted` |
| **Overflow clears once the backlog drains** | `overflow_clears_once_the_backlog_is_consumed` |
| Rolled-back stream positions are reused | `rolling_back_returns_positions_for_reuse` |
| VACUUM reclaims space and keeps the data | `vacuum_reclaims_space_and_keeps_the_data` |
| VACUUM is refused under a snapshot hold | `vacuum_is_refused_while_a_snapshot_is_held` |
| **Follower converges byte-identically with its primary** | `a_follower_converges_with_its_primary` |
| **Snapshot does not block the writer thread** | `snapshotting_a_large_shard_does_not_block_the_writer_thread` |
| The frozen file does not change while held | `the_frozen_file_does_not_change_while_a_snapshot_is_held` |
| **A gap is refused, not applied across** | `a_gap_is_refused_rather_than_applied_across` |
| Bootstrap then stream, resuming at exactly `lsn+1` | `a_follower_bootstraps_from_a_snapshot_and_then_streams` |
| **A transfer resumes where it stopped** | `a_snapshot_transfer_resumes_where_it_stopped` |
| A partial from another snapshot is discarded | `a_partial_transfer_of_a_different_snapshot_is_discarded` |
| An incomplete transfer cannot be installed | `a_transfer_cannot_be_installed_half_finished` |
| **Client writes and reads back over TCP** | `a_client_can_write_and_read_back` |
| Reads fan out across shards over the wire | `reads_fan_out_across_shards_over_the_wire` |
| Refusals survive the transport with their reason | `an_uncombinable_query_is_refused_over_the_wire_too` |
| A rejection is a result, not a broken connection | `a_rejected_statement_is_a_result_not_a_transport_failure` |
| 12 concurrent clients lose no writes | `many_clients_work_concurrently` |
| **The connection cap sheds and counts it** | `the_connection_cap_sheds_load_and_counts_it` |
| An oversized length prefix is refused before allocating | `an_oversized_length_prefix_is_refused_before_allocating` |
| **Fan-out answers match a single-shard ground truth** | `aggregates_match_a_single_shard`, `ordered_queries_match_a_single_shard` |
| **A contended open waits instead of failing** | `opening_while_another_connection_holds_the_write_lock_succeeds` (verified by disabling the retry) |
| 24 concurrent opens of a fresh database all succeed | `many_concurrent_opens_of_a_fresh_database_all_succeed` |
| **Contention is counted, not silently absorbed** | `wal_conversion_contention_is_counted_and_logged` (verified by disabling the counter) |
| **The shard count does not change the answer** (1, 4, 16, 64) | `the_shard_count_does_not_change_the_answer` |
| Uncombinable shapes are refused, with a reason | `queries_that_cannot_be_combined_are_refused` |
| Empty shards contribute nothing | `an_empty_shard_contributes_nothing` |
| A clean restart continues the stream | `a_clean_restart_continues_the_stream_without_rebootstrap` |
| An unclean restart bumps the epoch | `an_unclean_shutdown_bumps_the_epoch_and_forces_rebootstrap` |
| An empty follower may join at LSN 1, but not mid-stream | `an_empty_follower_may_join_a_stream_at_its_beginning`, `an_empty_follower_cannot_join_mid_stream` |
| Every shard is an independent stream | `every_shard_converges_independently` |

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
- **Draining a capture and then failing to ship it loses frames outright.** Found by a test:
  `drain_committed` hands ownership to the caller, so a sink that then refused a batch left
  those frames delivered nowhere, retained nowhere, and their stream positions already
  consumed — the exact silent truncation the design forbids. A refusal now requeues the
  frames and rolls back the positions.
- **`VACUUM` on a database whose data is still in the WAL grows the main file.** The first
  version of that test compared a 4 KiB main file against a 471 KiB one and concluded vacuum
  had made things worse; the data simply had not been checkpointed yet.
- **Fanning a *write* into the query planner silently drops it.** The planner refuses
  non-SELECT statements, so routing an `INSERT` through it reported "unsupported" and the
  write never ran — a cross-shard `count(*)` returned 0 after three successful-looking
  inserts. The CLI now checks `Db::is_read` before fanning out. Caught only because the
  manual check printed the count; the smoke test had sent the inserts to `/dev/null`.
- **`PRAGMA journal_mode = WAL` does not honour `busy_timeout`.** Measured on 3.53.2: with a
  5000 ms timeout set and another connection holding the write lock, it gives up after
  **23 microseconds**, while an ordinary `INSERT` on the same connection waits the full
  5.01 seconds. SQLite does not invoke the busy handler for a journal-mode change.
  `open_writer` now retries the conversion explicitly with backoff, which terminates because
  whoever holds the lock is either converting it themselves — after which the pragma is a
  no-op returning `wal` — or committing a write, after which the lock frees. Reproduction:
  1 to 3 failures per 720 concurrent opens without the retry, 0 in 2160 with it.
- **An earlier diagnosis of that bug was wrong.** Moving `busy_timeout` ahead of
  `journal_mode` was based on the theory that the timeout was unset. The ordering is still
  right, but it was never the cause: the timeout was set and SQLite ignored it. The tests
  written for that fix passed either way, which was the signal that the diagnosis was
  unproven rather than merely untested.
- **A stale binary made the fix look broken.** The reproduction harness linked the library
  as an rlib, so rebuilding the library alone left it exercising the old code — showing
  failures that were already fixed. Recompiling the harness with the library is part of the
  check, not an afterthought.
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
| `first_keyword` guard cannot see past a leading comment | `-- x\nBEGIN` slips through. Real guard is the authorizer (step 8) |
| `busy_timeout` ordering fix is unproven | Moving it before lock-taking statements is correct on its own merits, but neither test discriminates it — both pass with the bug reintroduced. The 1-CPU `SQLITE_BUSY`-at-open failure was never root-caused. |
| `capture_for` must be called before open | Registering later silently misses frames; not enforced by types |
| No graceful shutdown for in-flight work | `Drop` joins, but a long batch delays exit |

### Remaining work, in one place

**Not started**

| Item | Why it matters |
|---|---|
| **Data-plane promotion** | Step 10 elects a leader and gates writes, but a promoted follower does not yet reopen its shard files read-write and start serving. That mechanism is entangled with placement and lands with step 11. Until then leadership is decided and enforced, not yet *acted on*. |
| Read consistency levels | `Stale` / `AtLeastLsn` / `Linearizable` — step 12. |
| Shard placement and movement | Step 11. Also the unresolved rebalancing policy. |
| Per-shard merkle verification | Step 9. Divergence detection is currently a whole-database hash. |
| authn/authz | Nothing at all. |

**Known gaps that are accepted, not planned**

| Gap | Note |
|---|---|
| `capture_for` must precede open | Registering later silently misses frames; enforced by convention, not types. |
| No cross-shard atomicity, ever | WAL gives no atomic commit across ATTACHed databases. Permanent, and it shapes the user-facing API. |
| Interactive read-then-write transactions | A buffered transaction cannot read its own uncommitted writes (reads inside it are refused), so client-side logic that reads mid-transaction and branches is unsupported. The atomic write-batch case is supported; see below. |

### Step 8b result: frame retention + networked replication

A follower now runs against a real primary over TCP. `FrameLog` retains recent frames within
a **total** budget divided by shard count — the same trap the connection LRU had, where a
per-unit number silently multiplies by 64. `Replica` pulls rather than being pushed at, so
the follower's own position is the single source of truth about its progress.

Three bugs were found by writing the tests, each verified non-vacuous by removing the fix:

| Bug | Consequence had it shipped |
|---|---|
| The replica asked the primary which epoch to claim, then claimed it | The primary's epoch check compared its own answer against itself and could never fail. A follower holding a copy from an older generation would have been fed a newer generation's frames as if they continued its own — silent corruption, not a detected gap. |
| A fresh follower recorded the epoch it *asked* with, not the one the frames came from | Its copy would be stamped with a generation it does not belong to, making every later subscription look stale. |
| A snapshot freeze was never released if the connection holding it went away | A follower crashing mid-bootstrap suspends the primary's checkpointing **forever**; the WAL then grows without bound. A crashed follower would slowly take the primary down with it. Now released on connection teardown, counted as `abandoned_freezes`, and warned. |

Bootstrap is treated as a normal outcome, not a failure: retention is bounded, so a follower
far enough behind is told `NeedsBootstrap` and takes a snapshot. It is counted, so a follower
bootstrapping *repeatedly* — meaning retention is too small for the write rate — is visible
rather than merely slow.

### Gaps 1 and 2 are now closed

**Quorum-ack (was Gap 1) — done.** A write is not acknowledged until a majority holds it.
`replication/ack.rs` tracks each follower's durable position; the writer waits after draining
frames and before replying. A follower's next subscription *is* its acknowledgement — asking
from `from_lsn` proves it holds everything below — so there is no separate ack message to
lose or reorder. One wait per batch, so group commit amortises the round trip.

**Measured, by removing the fix:** without quorum-ack, **35 of 60 acknowledged writes were
lost** when the leader died. With it, all 60 survive on the follower.

A timeout is reported as `NotReplicated`, which says the write **is** committed locally and
must not be retried — distinct from a failure. A bug found while testing: `clone_shallow`
collapsed it into `BatchAborted`, whose text is *"no writes in this batch were applied"* —
contradicting the inner warning and inviting exactly the double-apply it cautions against.
The kind is now preserved so a caller can branch on it rather than parse text.

**Gate wiring (was Gap 2) — done.** `shard::WriteGate` is checked before the transaction
opens, not after it commits: a deposed leader that finds out afterwards has already written
to a file another node may own. `Fence` implements it, and the trait lives in `shard` so the
dependency points one way. Verified by removing the check — both cluster tests then fail.

### Gap 3 — data-plane promotion: closed, along with the corruption risk it exposed

A follower's `Follower` and `ShardManager` now address the **same directory**, so promotion
is a handover rather than a copy. Making that safe required naming an invariant the design
had never stated:

> A shard is either **led** by this node — its `ShardManager` owns the file and SQLite has
> exclusive charge of it — or **followed** — the replication path owns the file and no SQLite
> connection may exist. Never both.

It is necessary because the follower bypasses SQLite entirely. `apply` writes raw pages;
`install_snapshot` **renames a whole new file over the old one**. A connection open across
either is broken in a way that never announces itself: after page writes it serves rows
assembled from a stale cache, and after a rename it holds the **deleted inode** and reads a
database frozen at the moment of replacement, forever. `PRAGMA integrity_check` passes in
both cases, because the *file* is fine — it is the handle that is wrong.

`shard::mode::ShardModes` enforces it where connections are opened, in both fleets, and
defaults to `Led` so a standalone deployment pays nothing. `Promotion` owns the ordering,
which is the whole correctness argument: demotion closes the write gate **first**, then hands
over; promotion **waits for the pull loop to come to rest** and refuses if it does not,
because a failed promotion is an outage while an optimistic one is corruption.

**A bug found by testing this.** `ShardManager::follow` quiesced only the *writer* fleet, so
cached **reader** connections survived the handover. The mode check hid it — a followed shard
refuses opens, so nothing used them — until the shard was promoted again, at which point that
stale handle became live and served the pre-rename inode. Fixed by quiescing both fleets.
The test that should have caught it did not: it asserted the shard was refused, which the
mode check alone satisfies. It now asserts on `closed_on_handover`, a count of connections
actually closed, and fails at "closed 1 of them" when readers are skipped.

**The cost, stated plainly:** a follower **cannot serve reads**. The original plan wanted
follower reads for scale-out. Making that safe means applying frames through SQLite's own WAL
so readers use ordinary WAL locking, rather than writing pages behind its back. That is a
real change to the apply path, not a flag, and it is not done.

### Earlier: step 10 was reported complete when it was not

Recorded plainly because an earlier version of this document, and the commit message for
`59151f3`, claimed more than was built.

**Gap 1 — no quorum-ack. Acknowledged writes can be lost on failover.**

The plan says: *"Primary waits for quorum ack before acknowledging the client."* That does not
exist. The write path drains frames to the **local** `FrameLog` before replying, which is
honest about local durability, but followers **pull** asynchronously. A leader that commits,
acknowledges the client, and dies before any follower polls has lost that write. The election
restriction cannot recover it — the new leader legitimately never received it.

This contradicts the project's governing requirement that nothing is lost. As it stands the
system is **correct and stable under failover, but not lossless**: failover selects the
most-advanced *survivor*, which is not the same as selecting a node that holds everything that
was acknowledged.

**Gap 2 — the write gate is not wired to anything.**

`Fence::check_may_write` exists, is tested, and is called from nowhere in `src/shard/` or
`src/storage/`. The gate opens and closes correctly and the cluster tests assert on
`is_open()`, but a deposed leader handed a write through `ShardManager` would still execute it,
because no code on the write path consults the fence. The mechanism is real; the wiring is
absent. The claim that "a deposed leader refuses its own writes" was premature.

**Gap 3 — data-plane promotion.** Known and previously recorded: a promoted follower does not
reopen its shards read-write and begin serving.

Order of work agreed: **quorum-ack, then gate-wiring, then placement.** Quorum-ack comes first
because it is the named non-negotiable property, and because it changes the direction of
replication — building placement on the current pull-only model would mean reworking it.

### Step 10 so far: the leadership plane

**Decision changed from the plan.** The plan specified openraft. The codebase has no async
runtime anywhere — thread-per-connection, synchronous throughout — and the plan had already
decided the Raft log carries only membership and leadership. openraft would have meant a tokio
runtime plus `RaftStorage`/`RaftNetwork`/state-machine/snapshot implementations, to use roughly
its election half. Raft's *election* algorithm is implemented directly instead: terms, votes, a
heartbeat lease, and the election restriction. No new dependency.

Skipping the log moves the hard part rather than removing it. Raft's log-matching property is
what normally guarantees a new leader holds every committed entry; `cluster/durability.rs`
re-establishes that over frame positions, and is the highest-risk file in the module.

| Piece | Guarantee |
|---|---|
| `term.rs` | A node votes at most once per term, durably. The one place paying temp-write→fsync→rename: a forgotten vote means two leaders in one term. |
| `durability.rs` | A candidate must be at least as advanced on **every** shard. Aggregates would let a node far ahead on a busy shard win while behind on a quiet one. Cross-epoch positions are **refused, not ordered** — `(epoch, lsn)` ordering silently elects a node with a higher epoch and less data. |
| `election.rs` | Follower/candidate/leader, jittered timeouts, and the lease: a leader that cannot reach a quorum steps itself down. |
| `fence.rs` | Two halves — the **token** (followers refuse a deposed leader's messages) and the **gate** (a deposed leader refuses its own writes). Either alone is insufficient. **The gate is built and tested but NOT wired to the write path — see the gap below.** |
| `node.rs` | Drives the state machine over TCP; the state machine itself performs no I/O and so is testable without a network. |

**Measured:** failover in **463–483 ms** against a 5 s budget, on a three-node cluster over
real TCP.

Three bugs found by testing, each verified non-vacuous by removing the fix:

| Bug | Consequence had it shipped |
|---|---|
| A hung peer froze the election loop | Cluster RPCs inherited the client's 30 s read timeout. A peer that is *hung* rather than crashed — backlogged, paused, half-partitioned — accepts the connection and never answers, blocking the leader in a socket read for 30 s. A leader frozen that long never evaluates its lease, never steps down, and **keeps its write gate open the whole time** — the exact split-brain the lease exists to prevent. Peer round trips are now bounded at a third of the election timeout, on both connect and I/O. |
| Adopting a higher term reset the election timer | A candidate that can never win, because the election restriction refuses it, would suppress the whole cluster forever: each time it stood it bumped the term, every peer reset its timer, and no qualified node ever got to stand. A leaderless cluster with no visible cause. |
| A stopping node kept answering heartbeats | `stop()` ended a node's own loop but its server kept acknowledging leadership on connections already open, so a leader that had genuinely lost its cluster kept being told it still had one. Departure has to be visible to peers, not only to the departing node. |

### Step 11: multi-write, and what it broke

**The headline goal is delivered.** Shards are led by different nodes, so several nodes accept
writes at once. Measured on a three-node cluster: `{shard_0: node_1, shard_1: node_2}`.

Chosen shape: **one coordinator, not one Raft group per shard.** Per-shard groups would mean
64 election state machines and 64x the heartbeat traffic on a third of a CPU. The single
elected leader computes the map and publishes it on the heartbeat, so the map arrives with
its authority already proven. The coordinator is **not in the write path** — clients reach a
shard's primary directly, and if the coordinator dies existing primaries keep accepting
writes while only *changes* to the map stall.

The map is **derived, not stored**: recomputed from (live members, shard count, term) rather
than replicated. That keeps the plan's promise that the Raft log carries only membership and
leadership, and means a new coordinator computes a map rather than recovering one. The cost
is that assignments move when the live set changes — recorded honestly in
`losing_a_member_reassigns_its_shards_and_only_its_shards`, which measures the churn rather
than asserting a stability the modulo scheme does not provide.

The write gate became **per shard**. A node-wide flag cannot express "leads some, follows
others", and would have let a node that led any shard write every shard.

**Schema changes: rolling, with a version guard.** Chosen over pausing the cluster (a write
outage that grows with shard count) and over plain rolling (which pushes a half-migrated
schema onto every reader).

Each shard carries a schema version in `PRAGMA user_version`, which lives in the **database
header page** — so under physical replication it travels with the data automatically and
cannot drift from the schema it describes. A follower's version is correct the instant the
frames land, with no bookkeeping of ours to get wrong.

`apply_ddl` rolls shard by shard. No pausing machinery was needed: writes are already
serialized per shard, so the change simply takes its turn in that shard's queue — everything
ahead completes, the change applies, everything behind follows. The DDL and the version bump
share **one transaction**; separately, a crash between them leaves a shard whose version lies
about what it holds, and the version is what every cross-shard read trusts.

While a roll is in progress `query_all_shards` **refuses**, naming both versions and a shard
at each end. Single-shard reads and writes continue untouched — that is the point of rolling
rather than pausing. The guard clears by itself when the roll finishes; a latching guard would
turn a transient disagreement into a permanent outage.

**Routing: the server forwards, the client stays simple.** The last piece, and the plan
already named it — `server/forward.rs`, *"forward to primary"*. A node that receives work for
a shard it does not own hands it to the owner. A client connects to any node and its writes
reach the right one; without this, multi-write existed at the storage layer and no client
could use it.

The alternative — clients holding the placement map and their own connections — pushes
cluster topology into every client, and a client with a stale map writes to the wrong node.

**Forwarding cannot loop.** Two nodes whose maps disagree for a moment would otherwise pass a
request back and forth forever, which is a hang: the worst way for this to fail. A forwarded
request is wrapped in `Request::Direct`, meaning *handle this here or refuse it*.

Cluster-wide DDL is built on the same routing: `ExecuteAll` walks every shard and sends each
change to that shard's owner, reporting per-shard outcomes because there is no atomicity
across shards and collapsing them would hide a partial application.

**Superseded:** `cross_shard_ddl_is_broken_by_placement_and_this_records_it` was written to
fail the moment DDL routing landed. It did, and has been replaced by
`ddl_reaches_every_shard_from_any_node`.

**A guard that took three attempts to make real.** Removing the `Direct` wrapper kept passing:
the first test sent `Direct` from a client, which never exercises `Router::forward` at all.
The property is *what goes on the wire*, so the test now stands up a stub peer and inspects
the request it receives. Only then did removing the wrapper fail. Worth recording as a
pattern: a guard aimed at the wrong side of a boundary looks exactly like a passing test. `execute_all_shards` is a *local* operation —
it applies a statement to every shard **this node holds**. Once shards are spread, no node
holds them all, so schema changes have no working path. Measured: all three nodes fail.
`cross_shard_ddl_is_broken_by_placement_and_this_records_it` asserts the breakage so it
cannot be forgotten, and is written to fail the moment DDL routing lands.

`apply_ddl` rolls only over shards **this node leads**, which is correct but partial: a
cluster-wide schema change still needs fan-out to each shard's owner. That needs the placement
map at the routing layer — the same missing piece that stops a client reaching the right node
for a write. One problem, not two, and the last thing between here and step 11 being done.

**Placement now drives promotion.** `Promotion::apply(lead, term)` moves shards one at a
time, and `ClusterNode` calls it whenever a map arrives. The ordering is the correctness
argument and it is not the obvious one:

1. Close gates for shards being **taken away**, first — everything after takes time, and for
   all of it those files are about to belong to another node.
2. Hand those files over: mark followed, then quiesce.
3. **Bring the pull loop to rest** before touching anything being gained. A loop inside
   `apply` is writing raw pages.
4. Take ownership of gained shards and stop following them.
5. **Only now** open their gates.

Doing 5 before 3 is the mistake that looks harmless and is not. A failed handover leaves
gates as they were and is counted as `handover_failed`: leading a shard whose file is still
being rewritten is worse than leading it late.

`Replica` gained a dynamic follow set and `pause`/`wait_idle`, so a shard can be taken from
it without tearing the loop down and rebuilding it.

**Two more tests were found to predate placement**, both passing only by racing the first
placement round: `only_the_leader_can_write` (removed — a non-coordinator now legitimately
leads shards) and `a_candidate_that_is_behind_cannot_win` (rewritten to write through a
shard's actual owner). A third, `a_deposed_leader_is_fenced_and_stops_writing`, was racing
something real: `is_leader()` flips when the election is won, but the fence bar rises when
placement is applied. That window is inherent — a bar cannot rise before the election that
justifies it concludes — so the test now waits for the state it asserts about.

### A correction

Earlier revisions of this document, and commit `eedb82e`, said **207 tests**. The real figure
at that commit was **202** — an `awk` miscount on my part, not a loss of tests. Counts here
are now taken from summing `test result` lines directly.

### Step 9: divergence detection

**The failure this catches.** Replication is physical — a follower writes raw pages it never
interprets. If the capturing VFS mis-reads a frame, or `apply` writes them out of order, the
follower ends up holding *valid pages that are the wrong pages*. `PRAGMA integrity_check`
passes, reads succeed, and they return wrong rows. The VFS is the highest-risk code in this
project and this is its only detector.

Demonstrated, not asserted: `a_follower_that_missed_frames_is_detected` shows the diverged
follower passing `integrity_check` while its content hash differs.

**Hashed logically, never as bytes.** Two nodes holding identical data legitimately differ in
freelist layout, vacuum history, and checkpoint timing. Byte comparison would report
divergence constantly and be ignored within a week — worse than no detector, because it would
train everyone to dismiss the one real alarm.

Verified by reverting to a byte hash: three of four tests fail, **including
`a_correctly_replicated_shard_matches_its_primary`**. Primary and follower files are not
byte-identical in practice, so the logical hash was necessary rather than merely tidier. It
also means the older `assert_converged` byte comparison in `tests/replica_net.rs` only works
under quiesced conditions and is not a model for production verification.

Encoding is type-tagged and length-prefixed, so the integer `1` cannot hash like the text
`'1'`, and `('ab','c')` cannot hash like `('a','bc')`.

**Merkle deferred, deliberately.** The plan specified a per-shard merkle tree over row
ranges. `content_hash` is what catches the bug and what serves as a real test oracle; the
merkle is a *scale* device for when a full hash stops finishing. Building the incremental
tree now would be speculative machinery for a scale nothing here can yet exercise. It should
land when shard sizes make full hashing too slow to run — and the trigger is measurable, not
a guess.

### Step 11 also invalidated three older tests

Worth recording as a pattern rather than three incidents. `only_the_leader_can_write`,
`a_candidate_that_is_behind_cannot_win` and `a_deposed_leader_stops_writing` all assumed the
coordinator owns every shard. Once placement spread them that stopped being true, and each
kept passing **only by racing the first placement round** — so they surfaced as intermittent
failures under parallel load, not as honest breakage. When an invariant changes, the tests
that quietly still pass are the ones to go looking at.

### Step 12: follower reads, then consistency levels

**Follower reads came first, and the approach was chosen on a measurement.** Applying frames
through SQLite's own WAL is the eventual answer and the riskiest change available — it means
rewriting the code Stage 0 was gated on. The alternative, invalidate-on-apply, was measured
first: reopening a read connection costs **~8 us**, and a query through a fresh connection
23 us against 2 us on a persistent one. An apply costs an fsync — about 1.5 ms on container
storage — so the reopen is under 1% of the work it follows, and only the first read after
each apply pays it. That settled it.

`ShardAccess` supplies the two things SQLite is not getting for itself: **exclusion** (applies
take the lock, reads share it, so no read is in flight while pages are rewritten or a file
renamed) and a **generation** (bumped by every apply, so a connection from an older one is
closed rather than reused — which clears the stale page cache and, after a snapshot install,
the handle to the deleted inode).

**Levels.** `Stale`, `AtLeastLsn(n)`, `Linearizable`, defaulting to `Linearizable`: a caller
that says nothing about freshness gets the strongest guarantee, not the fastest answer.

**Three bugs found while testing, all the same shape** — claiming a guarantee that could not
be met:

| Bug | Consequence |
|---|---|
| `coordinate_with` was never wired | Applies neither excluded readers nor invalidated them, so a follower's readers were pinned to a page cache frozen at first connect. The test caught it only because I read the output: `final count Some(1)` after 60 rows. Monotonicity alone passed — a frozen reader returns the same number forever, which is perfectly monotonic and completely wrong. |
| "No router" meant "I satisfy every level" | True for a standalone node, where every shard is `Led`; false for a replica, which then answered `Linearizable` from a copy that was behind. Ownership now comes from the shard's mode, not from whether a router is configured. |
| "Not leading" was treated as "replicating" | A node can be neither. Such a node has an empty file and would answer `no such table`, or zero rows for a table that exists elsewhere. A weaker read is now served locally only with evidence a copy exists. |

When a level cannot be honoured and there is nowhere to forward to, the answer is
`TooStale { have, need }` — refusing rather than returning rows that quietly break the
guarantee.

**Placement now also keeps shard modes in step**, not just the write gate. Previously a node
that owned nothing still reported mode `Led`; the fence and the mode disagreed, which is what
let a node with no copy answer reads for shards it did not hold.

### HTTP/JSON gateway (Phase 1) — `--features http`

An optional HTTP edge over the same core the TCP server drives, so any language with an HTTP
client — and a browser — can talk to shardlite. Off by default: a native-only deployment compiles
none of it. Sync (`tiny_http`, thread-per-connection), matching the TCP server and keeping the
core tokio-free; the async variant is a documented future drop-in behind the same `handle()`
boundary.

**Large results stream — the robustness requirement, met and measured.** `exec::run_stream`
plus a reader-fleet `Job::Stream` push rows onto a **bounded** channel; `POST /v1/query` on a
locally-held shard emits them as newline-delimited JSON straight from the cursor. Nothing
materialises. Proven: a **300,000-row query streamed while the server held ~11.5 MB RSS**, and
`tests/streaming.rs` streams 200k rows through a 64-row buffer with backpressure, stopping the
reader early when the consumer drops. The bounded channel *is* the backpressure — a slow
client throttles the reader rather than filling memory. (This also gave the core a streaming
read it never had; even the native path materialised and was capped at the 16 MB frame limit.)

Endpoints (all of Phase 1): `/v1/info`, `/v1/query` (streaming), `/v1/query_all`,
`/v1/execute`, `/v1/tx` (atomic+durable), `/v1/execute_all` (rolling DDL), `/v1/route`,
`/v1/schema/{shard}`, `/v1/stats`, `/v1/cluster` (topology + placement), `/v1/frames/{shard}`
(the WAL inspector as JSON), and `/v1/users` (GET/POST/DELETE, admin). Each maps to the
existing `Request` and reuses routing, forwarding, the reader fleet, and the user store
unchanged. Native `Response` maps to a faithful HTTP status (a rejected statement is 400, not
200 — caught by a test).

**Security posture enforced at startup, not documented and hoped for:** HTTP Basic →
the same challenge-response verification the native handshake uses (secret → keyed proof
against a fresh nonce; byte-identical). Roles apply unchanged (`Read`/`Write`/`Admin`). And the
gateway **refuses to start with auth enabled but no transport security** unless `--http-insecure`
is passed — because a credential over plaintext leaks. Both the posture and the status mapping
are revert-verified. Two credential schemes are accepted, the caller's choice: `Basic`
(browser-friendly) and `Bearer` (same `base64(name:secret)` payload, no browser login prompt —
what programmatic clients prefer).

CLI: `shardlite serve <dir> --http ADDR [--http-insecure]` runs the gateway alongside the native
TCP server on one core. Remaining for a later phase: the standalone console (Phase 3).

### JSON-over-TCP gateway (Phase 2) — `--features json-tcp`

A second cross-language edge for clients that hold a connection open and issue many small
calls, where HTTP's per-request header and connection overhead is pure waste. Same core, same
`Request` mapping, same reader fleet and streaming path as HTTP — only the framing differs.
Off by default, like HTTP; a native-only deployment compiles neither.

**The wire.** Each frame is `[4-byte big-endian length][JSON]`, the length checked against a
16 MB cap before allocating (a hostile header cannot make the server reserve gigabytes). A
request is a JSON object with an `op`; bounded ops answer with one `{"result": …}` (or
`{"error": …, "status": N}`) frame; `query` streams `{"columns":[…]}`, then a `{"row":[…]}`
per row straight off the cursor, then `{"end":true}`. The stream is the same bounded-channel,
nothing-materialises path HTTP uses — **100k rows streamed through the persistent socket** in
the integration suite.

**One socket, authenticated once.** Unlike HTTP's per-request `Authorization`, a JSON-TCP
connection sends `{"op":"auth","name","secret"}` as its first frame and stays authenticated
for its lifetime; a doorman refuses every other op until then. The same challenge-response
verification and the same `Read`/`Write`/`Admin` roles apply. And the same startup refusal:
auth enabled without transport security is rejected unless `--json-tcp-insecure` is passed,
because the secret crosses the wire in clear.

CLI: `shardlite serve <dir> --json-tcp ADDR [--json-tcp-insecure]`, alongside `--http` and the
native server on one core. Verified by `tests/json_tcp.rs` (6): 100k-row streaming, a
persistent connection carrying many ops, auth gating, role enforcement, and the insecure-refusal
posture.

### Cross-language drivers (Phase 2) — `drivers/`

Clients in Python, JavaScript, Go, and Rust, each **dependency-free** (standard library; the
Rust crate uses `ureq`) and each **streaming** — `query` yields one row at a time, so a
million-row result costs the driver almost nothing, matching the gateway.

Three transports, two of them cross-language. **HTTP/JSON** is the stable stateless edge every
driver speaks. **JSON/TCP** is the persistent-socket edge — Python, JS, and Go each ship a
`TcpClient` over it (manual `[len][JSON]` framing, auth-on-connect, streaming query). **Native
bincode over TCP** is Rust-only and version-locked (a bincode bump breaks non-Rust clients), so
the Rust crate re-exports `shardlite::net::Client` under `--features native` and the other
languages deliberately use JSON/TCP instead — the stable contract without the fragility.

Verified live by `scripts/driver_test.sh`, which runs each present driver against a gateway
serving **both** HTTP and JSON/TCP and asserts a full 20k-row stream over each: Python and
JavaScript pass over HTTP **and** JSON/TCP; Rust over HTTP; Go is exercised where a toolchain
is present (its HTTP and TCP clients are written to the same contract but unrun in this
environment, stated rather than implied).

### Standalone web console (Phase 3) — `console/`

A separate app — its own binary, its own login, its own state — for managing and observing
clusters, the way a database client manages many connections. Deliberately **not** part of the
database binary: keeping it out of the 150 MB / 0.33 CPU database avoids a browser-facing surface
and request load there, and a standalone backend owns its own login with no CORS. Design and
confirmed scope in `docs/console-plan.md`; it adds **no shardlite core changes** — every feature is
composition over endpoints the gateway already exposes.

**Backend** (`console/server/`, its own Rust crate outside the workspace). Reaches clusters over
the stable HTTP `/v1` edge, so it is decoupled from the exact shardlite build. Multi-user console
login (Argon2id passwords, in-memory sessions, `admin`/`user` roles — admin gates user and
connection management). A connection registry whose stored shardlite secrets are **sealed at rest**
with ChaCha20-Poly1305 under a master passphrase (the file alone grants no cluster access; a wrong
passphrase fails loudly, never silently as "no auth"). A **uniform streaming proxy**: whatever the
browser asks of a connection goes to `/v1` with the same method and body, and the reply streams
straight back — so the gateway's "1 row to 1 million rows" robustness carries end to end
(**60,000 rows streamed through the console** in the smoke test, nothing materialised). A stats
sampler polls each connection's `/v1/stats` into a bounded in-memory ring — the one bit of history
the stateless database does not keep. The built SPA is embedded into the binary, so the console
ships as one self-contained executable.

**Frontend** (`console/web/`, React + TypeScript + Tailwind). The IBM Carbon look **mocked with
Tailwind**, not the heavy `@carbon/react` library — a small hand-built primitive set (DataTable,
Button, Tabs, SideNav, sparklines) carrying Carbon's g100 palette, blue-60, and IBM Plex. Views:
Connections and Console-users (management), and per-connection SQL editor (streaming grid),
Schema (via `sqlite_schema`), Cluster (topology + placement), Shards & frames (the WAL inspector),
Stats (live sparklines), and shardlite Users.

Verified end to end by `scripts/console_smoke.sh` (13 checks): the embedded SPA and its client-side
routing fallback, login and session, multi-user role enforcement (a `user` may read connections
but not administer), secrets absent from the on-disk file, the streaming proxy over a 60k-row
result, atomic transactions, the frames report, and unauthenticated calls refused. 12 backend unit
tests cover the crypto, user store, registry sealing, and sessions (including the revert-checked
negatives: wrong passphrase fails closed, the last admin cannot be removed).

### Frame inspection: `shardlite frames`

Physical replication ships WAL frames, not SQL — the honest cost of deterministic
replication is an opaque stream. This is the observability answer: `vfs::inspect_wal` decodes
a WAL file offline into a `WalReport` (header, per-frame page/commit/salt, transaction
grouping), and `shardlite frames <dir> --shard N` (or `--file PATH`, `--all`) renders it.

Read-only and offline by construction: it reads a file at rest and never touches the live
capture path, so inspecting a shard can never disturb replication. It reports what is
*physically* present — a frame past the last commit is shown, not hidden — and flags frames
whose salt does not match the header as **leftover** from before the last checkpoint, which
SQLite ignores. Honestly labelled: `current` is a salt match, not a full checksum-validity
proof, which is stated rather than implied.

Verified against real shard WALs (`tests/frames.rs`, 5): a CREATE plus five INSERTs read back
as six committed transactions; commit frames carry a non-zero db-size marker; a non-WAL file
has no header; and a rotated header salt makes every frame read as leftover and count zero
transactions — revert-checked (removing the salt comparison fails that test). Three CLI
assertions in the smoke script.

### Client transactions: BEGIN/COMMIT, buffered and durable

`BEGIN` used to be refused, with a documented reason: a client-held transaction pins the
writer thread across a client round trip and defeats group commit. That reasoning was sound
for a *held-open* transaction — and it is exactly what buffering avoids.

The server now **buffers** a connection's statements between `BEGIN` and `COMMIT` and applies
them as one atomic batch at `COMMIT`. The writer is engaged only at `COMMIT`, for one batch,
so it is never pinned during think-time and everyone else's writes keep flowing — verified by
`the_writer_is_not_pinned_during_a_transaction`. `COMMIT` returns the **durable** ack: the
batch rides the same routing + quorum path as any write, so the reply arrives only once a
quorum holds it.

`COMMIT` is **all-or-nothing**, which required a real fix rather than a wrapper. `apply::batch`
isolates each statement in its own savepoint — correct for independent group-commit requests
(one caller's rejection must not roll back another's), wrong for a transaction. `batch_with`
adds a group-level savepoint for atomic groups: one failure rolls the whole transaction back
and reports a single rejection. Atomic and independent groups still commit together, so a
transaction still rides group commit. Verified non-vacuous at both layers — reverting either
the atomic-flush or the atomic-apply makes the failed-transaction test leave rows behind.

**Honest limits, enforced not hidden:** a transaction is one shard (no cross-shard atomicity,
ever — a cross-shard statement is refused, not half-applied); reads inside a transaction are
refused (the buffer is not applied yet, so a read could not see it — refusing beats a stale
lie); the buffer is capped (100k statements / 64 MiB) so a runaway cannot exhaust server
memory; an abandoned transaction (dropped connection) vanishes, having applied nothing.

The client surface is an RAII `Transaction`: `client.begin(shard)?`, `tx.execute(sql)?` (per
call returns the queued count), `tx.commit()? -> (rows, rowid)` (the durable ack), `tx.rollback()?`
or drop (best-effort rollback). This is the network path; the local single-node CLI shell has
no session or quorum and still applies statements individually.

### User management: a live store, a CLI, and runtime creation

The auth layer's users were fixed in code — no way to add one without recompiling. Now they
live in a file the server persists to, are managed by a CLI, and can be created **at runtime**
against a running server.

**A live, persisted store.** `AuthConfig` became an `RwLock` over the user map plus an
optional file path. `AuthConfig::open(path)` loads a users file and persists every later
change back to it durably (temp → fsync → rename, the term store's discipline) — a
half-written user database is a lockout, or worse an admission. The file stores the *derived
key* as hex, never the secret: reading it grants nothing a network capture would not.

**Runtime verbs.** `CreateUser` / `DropUser` / `ListUsers`, all `Admin`-only. Two rules make
them safe: an admin **cannot mint a `Cluster` credential** over the wire — that would tunnel
through the wall between clients and cluster members, so cluster users are a deploy-time,
file-only act; and the wire carries the derived key, not the secret, so the plaintext never
leaves the operator's machine. The key still grants access, so runtime management belongs over
TLS — documented at every entry point.

**The CLI.** `shardlite serve <dir> --listen ADDR [--users FILE] [--tls-cert/-key]` runs a
server; `shardlite user add|drop|list` manages users either offline (`--users FILE`, how the
first admin is bootstrapped before any server exists) or at runtime (`--server ADDR --as ADMIN
--admin-secret S`). Both paths converge on the same file. Existing invocations are untouched —
`serve`/`user` are new leading subcommands, everything else still runs local SQL.

Verified end to end (`scripts/auth_cli.sh`, 8 assertions): offline provisioning, secret absent
from the file, runtime creation over the wire persisting back to the file, and the two
refusals (runtime cluster grant, non-admin management). Both security rules revert-checked:
removing the cluster wall or lowering the required role fails its test. A restart test proves
a runtime-created user survives via the file.

### TLS, optional and behind a feature

The auth layer authenticated without encrypting — stated as its own limit. TLS closes it,
under the `tls` feature so a trusted-network deployment compiles none of rustls and carries
none of its size.

**rustls with the `ring` provider**, not the aws-lc-rs default, which wants a C toolchain and
cmake; ring keeps the build self-contained, matching the bundled-SQLite discipline. Tests
generate self-signed certs in-process with `rcgen`, so nothing is checked in to expire.

**One structural change made TLS possible: the connection is no longer split.** The plaintext
code `try_clone`d the socket into independent reader and writer halves — a TLS connection
cannot be split that way, its record state is one object. It never needed to be: the protocol
is strict request-then-response, so a single `transport::Stream` carries both directions.
`write_message` now frames each message into one write, so dropping the write buffer costs
nothing. This touched the most-tested code in the project; all 267 non-TLS tests pass
unchanged.

**Configuring it is one call on each side.** `Server::with_tls(cert)` turns encryption on;
omit it and the server is plaintext exactly as before. A client uses `Client::connect_tls`
with either `TlsClientConfig::with_ca_pem` (verifies the server — real MITM protection) or
`dangerous_accept_any_cert` (encrypts against a passive eavesdropper only, warns at every
call, for development). The accept loop wraps each socket through a closure and never learns
which transport it got.

**Verified, including the security-critical negatives:** a plaintext client cannot handshake
a TLS server and vice versa (no silent downgrade); a verifying client rejects a wrong
certificate (revert-checked — swapping it to accept-any fails the test); TLS and auth
compose, encryption for the channel and credentials for the identity, orthogonal and stacked.

**Scope line held:** this is the encryption the auth module said to add. It does not do
client-certificate authentication — identity is still the challenge–response layer, and TLS
is `with_no_client_auth` deliberately, so the two concerns stay separate.

### Authentication and authorization

The largest recorded gap — the server accepted any connection — is closed. Protocol version
bumped to 2.

**Challenge–response, because there is no TLS.** A password in `Hello` would cross plain TCP
in clear. Instead the server sends a fresh 32-byte nonce from `/dev/urandom` (fail-closed: no
entropy, no connection) and the client answers with `blake3::keyed_hash(key, nonce)` — the
secret never crosses the wire, and a captured handshake replays as nothing. blake3 was
already a dependency; its `Hash` equality is constant-time, so the comparison does not leak
matching-prefix timing. The server stores `blake3(secret)`, never the secret.

**Stated limit, in the module docs and here:** this stops unauthorized access. It does not
encrypt — an eavesdropper still sees queries and rows, and an active MITM can hijack a
connection after its handshake. On a hostile network, run inside a tunnel (WireGuard, SSH).
Pulling a TLS stack into the 0.5 GB footprint is an operator's decision, not this crate's.

**Roles.** `Read` < `Write` < `Admin` (DDL), and `Cluster` deliberately off that ladder:
Subscribe and snapshots hand out entire shards, votes and heartbeats steer the cluster — a
stolen admin credential must not include the exfiltration path, and a peer node's credential
must not run ad-hoc queries. The requirement map is exhaustive over `Request`, so a new verb
must choose its requirement or fail to compile. Client authz is enforced at the entry node;
forwarded requests arrive as `Direct`, a Cluster verb, trusting the peer's own enforcement.

**No users configured = open, loudly.** Existing deployments and all 260 prior tests run
unchanged; the server warns at bind that it is accepting anything. Failed authentications
close the connection (each guess costs a fresh connection and nonce) and are counted, as are
authorization refusals. Wrong-secret and unknown-name refusals are byte-identical, so the
handshake cannot enumerate names.

Guards verified by revert: disabling the role check fails the role tests, a constant nonce
fails the replay test, removing the doorman fails the unauthenticated-client test.

### The races are now model-checked, not hammered

`RUSTFLAGS="--cfg loom" cargo test --lib loom_` runs five loom models over the two
synchronization cores the audit found races in — the fence and the read/apply coordination.
Loom explores **every** interleaving of the modelled threads, so a passing model is a proof
over those operations, where the hammer test could only say "not seen in 500 tries".

The difference is not academic. The check-outside-the-lock bug survived the 500-iteration
hammer in most runs — it failed once in eighteen full suites. Loom finds it **every time, in
milliseconds**, when the fix is reverted. All three fixes were revert-verified under loom:

| Fix reverted | Loom's verdict |
|---|---|
| Staleness check moved back outside the gate mutex | fails: "an interleaving exists where a stale placement reopens a deposed leader's gate" |
| Step-down closes without raising the bar | fails: same interleaving |
| Generation bump moved outside the write lock | fails: "a matching generation served a stale cache" |

The models also check what the hammer never could: the read/apply exclusion is verified by
modelling the database file as a `loom::cell::UnsafeCell`, which loom itself polices — any
schedule where a read overlaps a write fails the model outright, and a reader slipping
between two writes of one apply would observe a value that never existed.

**Scope, stated honestly.** Loom proves the modelled operations only. The fence and
`ShardAccess` are small enough to model faithfully; `ClusterNode`, the writer fleet and the
network server are not, and their concurrency is covered by the stress harness and the
hammer tests, which sample schedules rather than enumerate them. The `std::sync` /
`loom::sync` swap is confined to the two modelled modules (`src/sync.rs`), so a normal build
compiles exactly what it always did.

### The concurrency audit: three product races, one of them in the fix itself

Prompted by the observation that the intermittent failures were concurrency-shaped, the
concurrent call graph was audited directly rather than fixing tests around it. Heartbeats are
handled on **every connection thread**, and `apply_placement` runs there — that is the fact
the earlier code had not internalised.

**Race 1 — a deposed leader could reopen its own write gate.** `handle_heartbeat` checks the
term, then applies placement and opens gates. A step-down (higher term, another connection
thread) could land between the check and the open: `fence.close()` fires, then the stale
placement's `open_for` reopens the gates the step-down just closed. Two writers on one shard,
enabled by thread interleaving. Fixed at the fence, the serialization point everything above
can race around: `step_down(term)` raises the bar as it closes, and `open_for` refuses any
term below the bar.

**Race 1b — the first version of that fix had the same bug one level down.** The staleness
check ran before taking the gate mutex, so a step-down could run *between* the check and the
gate update. The stress harness caught it — the 500-iteration hammer test failed once in
eighteen suite runs. The check now happens inside the gate mutex, making check and mutation
one step. A guard against check-then-act, written as check-then-act.

**Race 2 — placement map and gates could cross.** Two connection threads applying different
placements could record P1 then P2 into the map but apply gates in the order P2 then P1.
Application is now serialized under a dedicated mutex, with `try_lock`-and-skip rather than
queueing: a handover legitimately takes seconds, and heartbeat threads queued behind it would
stall their replies — the hung-peer failure mode, self-inflicted. A skipped placement is not
recorded, so the next heartbeat retries it.

**Race 3 — the router held its lock across network I/O.** One hung shard owner — accepting
connections, answering nothing — blocked every forward on the node for the full timeout,
whatever shard or owner they were bound for. The same disease that once froze the election
loop, sitting in the write path. Connections are now taken out of the map before use, so the
lock never spans a round trip. Measured before the fix: a forward to a healthy owner waited
**1.8 s** behind a hung one; after, it completes independently.

All three verified by faithful revert: the stale-open refusal, the raise-on-step-down, and
the lock-across-I/O each fail their tests when reverted. One revert attempt was unfaithful —
it introduced a deadlock the original never had — and was redone against the exact pre-fix
code before its result was trusted.

A **fifth** placement-unaware test also surfaced (`three_nodes_elect_exactly_one_leader`,
racing gate-opening at election and asserting followers never hold gates). Five is a pattern
about the cost of changing an invariant late, not five accidents.

Verified: **48 suite runs across concurrent stress rounds, no failures.**

### The unreproducible failure, found

A single failing run had been seen and lost — the loop that spotted it printed "FAILED" and
discarded the output. `scripts/stress.sh` exists to make that impossible: it runs the suite
repeatedly, optionally concurrently, and keeps the full log of every failure.

It earned its keep immediately, finding **two** real problems in its first thirty runs:

**1. A fourth placement-unaware test.** `a_deposed_leader_is_fenced_and_stops_writing`
asserted `fence().is_open(ShardId(0))` on the elected leader. Under placement the coordinator
leads a *subset*, and which subset depends on who won the election — so the assertion held or
failed by luck. This is the fourth test in this file to assume the coordinator owns every
shard; the pattern is now unmistakable.

**2. A port race in the test harness, and a real gap in the library.** Setting up cluster
membership is circular: a node needs its peers' addresses, and an address only exists once
something is bound. The harness bound port 0, read the port, dropped the listener, and
rebound — leaving a window where another process takes the port. Under three concurrent
suites that produced `Address already in use` once in twenty-four runs.

Fixed properly rather than retried around: `Server::from_listener` lets a caller bind once
and hand the listener over, closing the window entirely. It is a legitimate API in its own
right — pre-binding is what socket activation needs too.

Verified after both fixes: **thirty suite runs across three concurrent suites, no failures.**

The lesson is the harness, not the bugs. Two intermittent faults had been present for some
time and were invisible because nothing was looking for them repeatedly and keeping the
evidence.

### Tests wait on conditions, not on the clock

The step 12 tests originally waited for replication with fixed sleeps — 300 to 400 ms. That
is a guess about how fast the machine is, and under load it is the wrong guess. The failure
it produces is an unreproducible one-off, which is worse than a consistent one because it
gets dismissed rather than investigated.

They now poll until the follower's applied position reaches the primary's committed position,
with a generous deadline and a diagnostic if it is not met. Verified under **six full suites
running concurrently**: 253 passed in every one, no failures, no panics.

This was prompted by a single failing run I could **not** reproduce and could not diagnose,
because the check that spotted it printed only "FAILED" and discarded the output. The
diagnostic flaw is worth recording alongside the fix: a check that detects a problem without
capturing it converts a bug into a rumour.

### The flaky checkpoint test: root-caused and fixed

It failed **17 runs in 40** once reproduced properly. The cause was not timing noise — the
test asserted things SQLite does not do. Four wrong beliefs, each found by measuring:

1. **A successful PASSIVE checkpoint shrinks the WAL file.** It does not. Measured on 3.53.2:
   `(busy=0, log=2213, checkpointed=2213)` — everything copied — leaves the file at exactly
   its previous length. Only TRUNCATE shrinks it.
2. **After the reader releases, the ladder escalates and reclaims.** It correctly does the
   opposite: passive checkpoints start succeeding, the stall counter resets, and escalation
   never fires. The old test passed only when a stray stall happened to trigger a TRUNCATE
   after release.
3. **With no reader, the WAL stops growing.** It keeps growing while writes outpace the
   checkpoint interval, stalls or no stalls.
4. **Dropping the reader releases its snapshot immediately.** Ending the *transaction* does
   not — the connection must go too — and even then one more stall can follow, because the
   read mark is not cleared instantly.

The product was correct throughout; the test was wrong four times over. It now asserts what
the name promises — a pinned snapshot stalls checkpoints, the WAL passes its hard limit, and
the ladder escalates to TRUNCATE **while the reader still holds** — and measures recovery
over a settled window rather than on the instant after release.

**45 consecutive clean runs**, and the checkpoint suite dropped from ~20s to 5s.

Worth keeping as a lesson: a flaky test is often not a timing problem but a false belief that
happens to be true most of the time.

### Convergence checks now use the content hash

`tests/replica_net.rs` compared whole files byte for byte, which worked only because those
tests are small and quiesced. Primary and follower files are not byte-identical in general.
They now compare `storage::verify::content_hash`, with `integrity_check` kept alongside and
labelled as insufficient — a follower holding valid-but-wrong pages passes it happily.

### Debt deliberately deferred

These do not get harder with time, so they were left rather than done now: structured
logging (still `eprintln!`), a `VACUUM` maintenance path, a cross-shard query planner, and
the ~20s checkpoint test suite.

Also deferred, and worth naming: `Replica` reconnects per `sync_once`, which is fine at a
poll interval but wasteful under continuous catch-up; and an abandoned freeze is only
detected when the connection's read times out, so `idle_timeout` bounds how long a dead
follower can pin a primary's WAL.

From step 10: `ClusterNode` holds one cached connection per peer and campaigns to peers
sequentially, so one slow peer delays the next vote request by up to `peer_timeout`. Bounded
and correct, but a parallel fan-out would shorten elections in a larger cluster. Cluster
membership is also static — configured at startup, with no join/leave protocol.

### Design decisions not yet made

- ~~Shard count default~~ — **resolved.** There is no default. The CLI requires `--shards N`
  when creating a directory and refuses it for an existing one, because a value that cannot
  be revised should not be one you got by accident.
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
./target/debug/shardlite /tmp/demo.db
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
