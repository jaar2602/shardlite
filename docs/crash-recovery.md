# Dynamic topology crash recovery qualification

Shardlite's dynamic topology state machines repair themselves after an unclean process exit. The
leader's periodic reconciliation performs four cluster-level repairs before advancing operations:

1. recover a locally durable catalog value that reached prepare but not commit;
2. republish the latest committed catalog so a member that missed commit catches up;
3. complete an interrupted learner join from its durable catalog position;
4. finalize a durably committed joint voter transition.

Transfer and split workers then resume their durable operation records. Before ownership or routing
commit, an operation may safely restart or abort. After the affected shard is fenced, recovery only
rolls forward. If the source process restarted while fenced, the coordinator records the new stream
epoch and final LSN, invalidates evidence from the old stream, installs a replacement snapshot under
the still-closed write gate, and continues cutover. The old owner demotes its local shard before it
allows cleanup to be recorded.

## Running the deterministic suite

The harness is excluded from the normal fast test suite because it repeatedly launches and kills
real servers:

```sh
cargo test --features failpoints --test dynamic_crash -- --ignored --nocapture --test-threads=1
```

Run one named boundary while debugging:

```sh
SHARDLITE_QUALIFY_FAILPOINT=transfer.source.after_fence \
  cargo test --features failpoints --test dynamic_crash \
  every_transfer_boundary_repairs_itself_after_restart -- --ignored --nocapture
```

Set `SHARDLITE_QUALIFY_LOG=1` to inherit child stdout/stderr.

The `failpoints` feature is deliberately not enabled by default. With it disabled,
`shardlite::failpoint::hit` is a no-op and the binary does not inspect failpoint environment
variables. With it enabled, a selected point writes and fsyncs a one-shot marker, then exits with
status 86 through `process::exit`; panic unwinding and destructors do not make the recovery path
artificially easier.

## What the harness proves

Each scenario restarts the same node directories, waits for the operation to converge without a
manual catalog or file edit, verifies the terminal ownership/routing/member state, and compares the
complete expected primary-key/value set.

Coverage includes:

- all named durable online-split transitions and source capture/snapshot/backfill/replay/install/
  cleanup boundaries;
- whole-shard planning, destination snapshot and catch-up files, prepare, fence, final LSN,
  ownership commit, old-source demotion, and cleanup;
- three consecutive coordinator crashes while replacing invalidated transfer evidence after a
  fenced source restart;
- catalog crash after local prepare and after local commit;
- learner registration and join completion;
- joint voter commit and final stable voter commit.

## What it does not yet prove

Passing this suite is necessary, but is not a production zero-downtime claim. Production
qualification still needs:

- network partitions before and after ownership/routing commit, including asymmetric partitions;
- disk-full, short-write, fsync, checksum/corruption, and snapshot-invalidation faults;
- a continuous concurrent workload with acknowledged-write tracking and an independent SQLite
  answer oracle during moves, splits, leader changes, and repeated crashes;
- stale owner and stale router attempts across generation/epoch changes;
- replica loss, drain while degraded, capture-log pressure, and large resumable backfills;
- long-running soak tests and packaged deployment tests on the supported operating systems.
