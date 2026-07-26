# Schema migration — fenced DDL, cluster-wide detection, and opt-in repair

> **Status: Phases 0 and 1 complete. Defects 3 and 4 remain — and Defect 4 gates the value of
> everything already done, because the primary client path never advances the version the guard
> compares.** This plan closes the gap between "DDL is applied shard by shard" and "the cluster
> can tell you whether that finished, and finish it for you."

## The problem

DDL is not atomic across shards, and that is permanent — it follows from having no atomic
commit across shards, which is the same reason cross-shard transactions are unavailable.
[`storage/schema.rs`](../src/storage/schema.rs) already chose the right response: roll shard
by shard, and refuse cross-shard reads while versions disagree, because a half-migrated answer
is not a slower answer, it is a wrong one.

That guard is correct in design and incomplete in three specific ways.

### Defect 1 — DDL bypassed the write gate (fixed, Phase 0)

`check_may_write` is consulted in exactly one place,
[`writer_fleet.rs:684`](../src/shard/writer_fleet.rs#L684), inside `apply_shard_batch`.
`Job::Schema` dispatches to `schema_op`, a sibling path that never consults it.

`Promotion::fence_for_transfer` closes a shard's gate and drains its queue precisely so that
`last_lsn` is final — that position is what a transfer destination catches up to before taking
over. DDL committing after that barrier is a change the destination never receives, and the
two copies diverge with no error on either side.

Pinned by `tests/schema_roll.rs::a_fenced_shard_refuses_ddl_like_any_other_write`, which failed
on first run with `DDL past the fence must be refused like any other write, got Ok(2)` — the DDL
did not merely slip through, it advanced `user_version` from 1 to 2 on a shard whose gate was
closed. The control assertion in that test, an ordinary `INSERT` past the same barrier, was
refused, so the gate was wired and the failure was specific to DDL.

Fixed by consulting the gate in `schema_op`, conditional on `ddl.is_some()`. See Phase 0.

### Defect 2 — agreement was checked only over shards this node leads (fixed, Phase 1)

[`ShardManager::schema_agreement`](../src/shard/mod.rs#L990) skips any shard whose mode is not
`Led`. On a single node or a full replica every shard is `Led` and the check is complete. In a
cluster each node leads a subset, and the `ShardBatch` handler on the remote side runs its part
of a fan-out with no agreement check of its own. So a DDL that failed on another node's shards
is not caught before the fan-out merges across disagreeing schemas — the exact case the guard
exists to prevent.

Reproduced by `cross_node.rs::cross_node_schema_skew_is_detected_before_a_fan_out_read`, and the
result was worse than this description. The fan-out did not merely merge across two schemas, it
returned a **ragged** result — two declared columns with a three-value row in it:

```
columns: ["id", "name"]
rows:    [Text("a"), Text("on A")]
         [Text("b"), Text("on B"), Null]
```

A result set whose rows disagree about their own arity, which a driver either mis-decodes or
chokes on. Fixed in Phase 1.

### Defect 4 — the routed DDL path never advances the version (open, and it gates the others)

There are two DDL paths, and only one is versioned:

| Path | Reaches | Versioned? |
|---|---|---|
| `Request::ExecuteAll` → `apply_ddl_to` per shard | every shard, local and forwarded | **yes** |
| `Request::Run` → `run_routed` → `execute_all_shards` → `fan_out_shards(write)` | every shard | **no** |

`Request::Run` is the "just connect and run SQL" path the README leads with — what the CLI and
every driver use. DDL issued that way runs as an ordinary write, so `user_version` stays at 0 on
every shard forever.

The consequence is that the skew guard is **inert for the primary client path**, and that includes
everything Phases 0 and 1 just built. Every shard reports 0, `Agreement::of` returns `Agreed(0)`,
and `check()` passes. A `Run`-issued roll that fails half way through returns an error to the
caller, leaves the earlier shards changed, and still lets the cross-shard read through — which is
exactly the failure Defect 2 exists to prevent, reached by a different door.

Pinned by `schema_roll.rs::ddl_through_the_routed_path_advances_the_schema_version`, currently
failing with `left: 0, right: 1`.

**Not a one-line fix.** The obvious swap — have `run_routed` call `apply_ddl` — is wrong twice
over: `apply_ddl` only touches shards this node *leads*, so it would silently skip every remote
shard in a cluster, and `execute_all_shards` also performs `adopt_shard_key_from_ddl`, which
routing depends on. The correct shape is the one `Request::ExecuteAll`'s handler already has:
per shard, `apply_ddl_to` locally and a forwarded `SchemaApply` remotely. That logic lives in the
server handler and has no equivalent inside `ShardManager`, so it needs lifting rather than
copying.

### Defect 3 — uniform lag is invisible

[`Agreement::of`](../src/storage/schema.rs#L80) compares shards **to each other**. A roll that
failed on *every* shard leaves them all at version 3, returns `Agreed(3)`, and passes `check()`
cleanly. The cluster is uniformly wrong and the guard reports health.

Pairwise comparison cannot detect this — there is nothing to compare against. It needs a
declared target, which is what Phase 2 adds. This is the strongest argument for putting the
target in the catalog rather than inferring it from `max(versions)`.

## Design

### DDL is a write, not a topology operation

The tempting model is a `CatalogOperation` of kind `SchemaChange`, reusing the phased lifecycle
that join, drain and transfer use. It is the wrong model and it manufactures a problem that does
not otherwise exist.

`start_operation` enforces one non-terminal operation per shard because two things moving one
shard's *ownership* at once is two writers on one file. A schema change is not ownership: it
does not move the shard, change its primary, or bump the placement generation. Modelling it as
one forces a choice between N operations per roll (64 by default) and a new lock scope, and
either way an in-flight transfer blocks the roll on that shard.

DDL is a write, and the transfer protocol already handles writes arriving mid-transfer — that is
what `Fencing` → `record_final_lsn` → `record_fenced_catch_up` is for. `schema_op` already
commits pages and calls `drain_capture` with the comment *"The DDL produced frames like any
other write; they must reach the stream."* So a DDL applied to a transfer source propagates to
the destination through the frames it is already catching up on, and arrives before cutover. No
new machinery, no change to `start_operation`, no stall.

### Intent lives in the catalog; progress lives in the shards

```
Catalog {
    ...
    target_schema_version: u64,
    schema_log: [(version, ddl_text)],   // ordered, truncatable
}
```

Progress is **not** catalog state. Each shard's `user_version` lives in its SQLite header page,
so under physical replication it travels with the data automatically and cannot drift from the
schema it describes. Duplicating it into the catalog would create a second source of truth for
"where is this shard", which is the drift this design spends its effort avoiding.

Repair is therefore a reconcile loop, not a retry: for each shard, read its actual version,
replay the log entries between it and the target against whoever is *currently* primary.
Ordered because the log is ordered, idempotent because replay starts from a known position, and
correct across a primary change mid-roll because each pass re-reads placement.

### Followed shards need nothing, and cannot be replayed against

A node that receives a shard by snapshot and frame catch-up gets the schema with the pages. It
must **not** replay the log for that shard — the change is already installed, and replaying it
would double-apply. Replay applies only to shards a node *leads*, because a leader is the source
of frames, not a sink, so nothing will ever push a schema change to it.

This is what keeps the log small: it is not a schema-distribution mechanism, it is a way to
drive current primaries to a declared target.

**This is already enforced structurally, and it is not a rule the reconcile loop has to
remember.** `schema_op` begins with `ensure_shard`, which calls
[`check_may_open`](../src/shard/mode.rs#L195) *before anything is opened*; a `Followed` shard
returns `Error::ShardFollowed`. So a replay against a new node's shard dies at connection-open
time, before any SQL runs, whether or not the caller thought to check.

Verified by `tests/schema_roll.rs::a_followed_shard_refuses_ddl_so_replay_cannot_reach_a_new_node`,
which passes today: it follows a shard, attempts the replay, and confirms the version is
untouched after the shard is taken back.

Worth noting the asymmetry this exposes, because it is exactly Defect 1 seen from the other
side. Both guards protect the same class of hazard, but only one is wired to the DDL path:

| Guard | Question it answers | Checked at | Covers DDL? |
|---|---|---|---|
| `ShardModes::check_may_open` | may this node open the file at all? | `ensure_shard` | **yes** — `schema_op` calls it |
| `WriteGate::check_may_write` | may this node write this shard? | `apply_shard_batch` | **no** — `schema_op` bypasses it |

Phase 0 closes the second row. The first needs nothing.

## Phases

Each phase states its own acceptance gate. A phase is done when its gate passes, not when the
code looks right.

### Phase 0 — close the fence gap ✅

Done. The gate check sits in `schema_op`, conditional on `ddl.is_some()`. `schema_op(shard,
None)` is a pure version read that `schema_agreement` depends on; gating it would break skew
detection during a transfer, which is the opposite of the goal. The check goes before the
transaction opens, matching the existing rule that leadership is checked before the transaction
rather than after it commits.

**Gate — all met:**
- `a_fenced_shard_refuses_ddl_like_any_other_write` passes.
- `promotion.rs::a_schema_change_during_catch_up_reaches_the_destination_before_cutover` passes:
  a DDL applied to the source while the destination is following arrives through the frames, and
  after cutover the destination holds both the column and the matching `user_version`, with no
  DDL ever applied to it directly. This is the other half of the same window.
- Suite green — `schema_agreement` still reads versions on fenced shards, and every replication,
  cluster, quorum and promotion test passes.

Known unrelated flake, not introduced here:
`replication.rs::snapshotting_a_large_shard_does_not_block_the_writer_thread` asserts
`begin_took < copy_took`. On a machine where the source file is in page cache the copy finishes
in ~10 ms against a ~13 ms `begin_snapshot`, so the comparison is a coin flip. Verified failing 2
of 3 runs on unmodified source. Worth re-basing on a ratio against shard size, or measuring the
writer-thread block directly rather than racing it against a `std::fs::copy`.

### Phase 1 — cluster-wide agreement ✅ (one item deferred)

Done. `schema_agreement` now gathers versions from every shard's owner, not only locally-led
shards. The seam is `PeerRouter`, which is already "the bridge that lets `ShardManager` fan-outs
reach shards owned by other nodes" — so the check batches by owner exactly as `fan_out` does, at
one round trip per node rather than per shard.

`PeerRouter::schema_versions` is a **required** trait method, not defaulted. A default would have
to either invent a version — making cross-node skew look like agreement, the exact failure this
exists to catch — or error, breaking every fan-out for an implementor that forgot it. A compile
error is better than both.

The wire verb is `Request::SchemaVersions { shards }` → `Response::SchemaVersions { versions }`,
read-only and classified `Requirement::Cluster` alongside `ShardBatch`, which it mirrors in shape.
A new variant rather than making `SchemaApply`'s `ddl` optional: appending leaves every existing
variant's encoding untouched. `PROTOCOL_VERSION` went 6 → 7, because the handshake tests strict
equality and a new node asking an old one must fail there rather than as a confusing decode error.

**Gate:**
- ✅ Defect 2 reproduced first, and the reproduction was worse than the description — a ragged
  result set, not merely a half-migrated one.
- ✅ Both nodes detect the skew, not only the straggler. Either can be the node a client connects
  to, so a check that only the behind node failed would still let the other serve the bad result.
- ✅ Detection is always on. It completes an existing guard rather than adding a feature, and
  shipping a knowingly-incomplete correctness check behind a flag means the default build is the
  wrong one.
- ⏳ **Deferred:** the error names the shard but not its **owning node**. `PeerRouter` exposes
  `is_local` and no owner accessor; `net::forward::Router` knows the owner but the trait does not.
  Adding `owner(ShardId) -> Option<NodeId>` to the trait would close it. Worth doing — "shard_3
  is behind" sends an operator to the wrong host half the time — but it widens the trait, so it is
  its own change rather than a rider on this one.

### Phase 2 — declared target in the catalog

Add `target_schema_version` and the ordered `schema_log`. Set the target before the roll begins,
so intent is durable before any shard moves.

`Agreement` grows a third state: agreed-but-behind-target. Cross-shard reads are refused for it
on the same grounds as skew — every shard being uniformly wrong is not more correct than some
shards being wrong.

**Gate:**
- A roll that fails on every shard is refused by `check()`. Impossible to express today.
- The error names the gap and the remedy: *"shard_1 is at 3, cluster target is 5"*, not
  *"shard_1 is at 3, shard_0 is at 4"*.
- A cluster with no target set behaves exactly as today, so an existing deployment does not
  become unreadable on upgrade.

### Phase 3 — reconcile, opt-in

The repair loop: for each shard, compare actual to target, replay the gap against the current
primary via the ordinary `apply_ddl_to` path.

Opt-in, unlike detection. Auto-applying a schema change to live data at a moment the operator did
not choose is a real write with real blast radius, and an operator mid-incident may want the
cluster to stay refused rather than self-heal underneath them. The manual path stays available
either way.

**Gate:**
- A shard left behind by an interrupted roll converges without operator action, with the flag on.
- With the flag off, it stays behind and stays loudly refused. No silent repair.
- Replay is idempotent: running reconcile twice over a converged cluster is a no-op and does not
  advance any version.
- A shard whose primary changed mid-roll converges — the loop re-reads placement each pass.
- A followed shard is never replayed against. Already held and pinned; the gate here is that the
  reconcile loop **skips** followed shards rather than attempting and erroring on each pass, so a
  new node does not generate a refusal per shard per pass while it catches up.

### Phase 4 — truncation

The log cannot grow forever. Truncate entries once every shard is at-or-past them. A shard
further behind than the log covers is re-bootstrapped physically, which is already the answer for
a far-behind follower.

This mirrors [`FrameLog`](../src/replication/log.rs) exactly — bounded retention, and bootstrap as
the honest fallback rather than hoarding history against a shard that may never come back. Same
tradeoff, same shape, no new concept.

**Gate:**
- Truncation never drops an entry some shard still needs.
- A shard past the retention edge bootstraps rather than silently skipping versions.
- Evictions are counted, so "why does this shard keep bootstrapping?" has an answer.

## Invariants

1. A shard's `user_version` and its schema move together or not at all. Already held by
   `schema_op`'s single transaction; every change here must preserve it.
2. A shard whose write gate is closed accepts no schema change. **Held** as of Phase 0, pinned by
   `a_fenced_shard_refuses_ddl_like_any_other_write`.
3. Progress is read from shards, never stored in the catalog. One source of truth.
4. A followed shard receives schema only through frames, never through replay. **Held today**,
   enforced at `ensure_shard` rather than by convention, and pinned by
   `a_followed_shard_refuses_ddl_so_replay_cannot_reach_a_new_node`.
5. A cross-shard read is refused whenever versions disagree *or* any shard is behind the declared
   target.
6. Repair never runs without opt-in; detection always runs.

## Failure matrix

| Failure | Behaviour after this plan |
|---|---|
| DDL fails on one shard mid-roll | Skew detected; cross-shard reads refused; reconcile converges it (opt-in) or an operator does |
| DDL fails on every shard | Detected against the target — invisible today |
| DDL arrives during a transfer, before fencing | Carried to the destination by the frames it is already catching up on |
| DDL arrives after the fencing barrier | Refused by the gate; reconcile applies it to the new primary on the next pass |
| Coordinator dies mid-roll | Target is durable in the catalog; the next coordinator reconciles from it |
| Node down during a roll, rejoins as follower | Catches up physically; no replay needed, and a replay attempt is refused at `ensure_shard` |
| New node joins and receives shards by snapshot | Schema arrives in the pages; replay structurally impossible while the shard is followed |
| Node down during a roll, still leads its shards | Reconcile replays the gap when it returns |
| Shard further behind than the log retains | Bootstrapped physically; eviction counted |

## Non-goals

- **Making DDL atomic across shards.** It cannot be, without atomic cross-shard commit. This plan
  makes the non-atomic window detectable and recoverable, not absent.
- **Two-phase commit for schema.** Rejected for the same reason it is rejected for data: the
  coordinator would enter the write path and a coordinator failure would stall every shard.
- **Application-level schema versioning or migration authoring.** The log records the DDL that was
  issued; it does not validate, plan, or roll back application migrations.
- **Repairing data divergence.** Only schema. A shard whose *rows* diverged is a replication
  problem, handled by bootstrap.

## Open questions

- Should the target be per-table rather than per-cluster? Per-cluster is simpler and matches how
  the version is stored today. Per-table would let unrelated migrations proceed concurrently, at
  the cost of a much larger catalog and a harder agreement check. Start per-cluster.
- Does the reconcile loop belong on the coordinator or on each node? Coordinator is simpler to
  reason about; per-node avoids putting anything new in the coordinator's path. Leaning per-node,
  reading the catalog it already receives.
- The protocol change in Phase 1 needs a `PROTOCOL_VERSION` bump. Worth batching with any other
  pending wire change rather than spending a version on it alone.
