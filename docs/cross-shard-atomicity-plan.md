# Cross-shard write atomicity — two-phase commit for every multi-shard write

> **Status: Phases 1–2 implemented and verified. Phase 3 (distributed participants) outstanding.**
>
> D6 is fixed for writes whose participants are all on one node: a multi-row `INSERT`, a broadcast
> `UPDATE`/`DELETE`, and a DDL broadcast are each all-or-nothing at every shard count, with
> recovery for an unclean exit. Guarded by
> [`tests/cross_shard_atomicity.rs`](../tests/cross_shard_atomicity.rs), including the two criteria
> this plan called out as the real risks: that a parked shard does not stall its siblings, and that
> concurrent cross-shard writes do not deadlock.
>
> **What landed**
>
> - `storage/apply.rs` — `prepare_group` / `finish_prepared` / `has_applied`, using raw
>   `BEGIN IMMEDIATE` so no Rust borrow is held across calls.
> - `shard/writer_fleet.rs` — `Job::Prepare` / `Job::Decide`, with per-thread parking: a prepared
>   shard holds its own queued writes while the thread keeps serving its other shards.
> - `shard/txn.rs` — the durable coordinator log, fsynced between the phases, and recovery.
> - `shard/mod.rs` — `run_atomic`, behind `Route::Split`, `Route::All` and the DDL broadcast.
>
> **What did not**
>
> Phase 3: remote participants. `prepare` parks a transaction on *this* node's writer fleet, so a
> shard owned by another node cannot take part without the Prepare/Commit/Abort RPCs in Tier 2
> below. Until they exist, a fan-out that spans nodes keeps the per-shard path it always had —
> non-atomic, and explicitly marked in `run_atomic`.
>
> **Two implementation notes worth keeping.**
>
> `apply::batch_with` uses `rusqlite::Transaction`, which borrows the connection — so a prepared
> transaction cannot be held across writer-thread batches without a self-referential borrow. Raw
> `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` keeps the state in SQLite instead of in a Rust lifetime.
> This is what makes Tier 1 tractable, and it is not obvious until you try it.
>
> Blocking the *writer thread* on the coordinator's decision looks simpler than parking the shard,
> and is wrong: shards map to threads by `id % writer_threads`, so two participants sharing a
> thread would deadlock — the second prepare could never run. Parking the shard rather than the
> thread is not an optimisation, it is the only correct shape.

## Uniform semantics, not uniform mechanism

The governing rule is one behaviour, never a conditional one. Applied here it means:

**Every write that touches more than one shard is atomic, by the same mechanism, with the same
observable contract — whether the router split it, broadcast it, or it was DDL.**

The rule constrains semantics, not implementation. A single-participant write needs no coordinator
log, because SQLite's own transaction already provides the guarantee; skipping provably unnecessary
work is not a second behaviour. What must never vary is the answer the client gets.

### Every multi-shard write path is in scope

All three have the identical defect, and giving atomicity to only one would itself be a
conditional behaviour:

| Path | Statement | Current failure |
|---|---|---|
| `Route::Split` | multi-row `INSERT` | D6 — loop over `execute_one` with `?` |
| `Route::All` | keyless `UPDATE` / `DELETE` | `fan_out_shards` then `outcome?` — same shape |
| DDL broadcast | `CREATE` / `DROP` / `ALTER` | per-shard loop with `?` in `execute_all_shards` |

The DDL case is measured, not inferred. Pre-creating table `x` on shard 2, then broadcasting
`CREATE TABLE x` across 4 shards:

```
broadcast   → Err("table x already exists")
shard 0 has x: yes    shard 2 has x: yes
shard 1 has x: yes    shard 3 has x: no
cross-shard read → refused: "schema versions disagree … a schema change is part-applied"
```

The schema-agreement check *detects* it — which is the right instinct and stays as a safety net —
but the database is left half-migrated with no automatic repair. Under two-phase commit that
refusal becomes an assertion that should never fire, rather than a routine operational state.

## The scope decision that bounds everything

**This plan adds atomicity for writes the *server* fans out across shards. It does not add
interactive cross-shard transactions.**

The distinction is the difference between a bounded project and an open-ended one:

| | Server-fanned write (in scope) | Interactive `BEGIN`…`COMMIT` (not in scope) |
|---|---|---|
| Statements known up front | yes — the server computed the fan-out | no |
| Hold-open duration | server-side work only, ~10 ms | client think-time, unbounded |
| Reads inside the transaction | none | must see uncommitted writes |
| Session state | none | per-connection, per-shard |
| Protocol surface | internal peer RPCs | client-facing, all four surfaces |

Because a server-fanned write is non-interactive, the write locks are held only for the server's own
prepare → decide → commit sequence. That is the single fact that makes the writer-thread blocking
cost tolerable rather than fatal. An interactive cross-shard transaction would hold shard write
locks across client round trips, and no amount of engineering makes that safe on a two-writer-thread
fleet.

The existing session behaviour is unchanged: a client `BEGIN` that touches a second shard still
aborts, with its existing message.

## Why SQLite forces redo, not prepare

SQLite has no `PREPARE TRANSACTION`. There is no durable prepared state; a transaction open when the
process dies is rolled back unconditionally. Textbook 2PC — where recovery *commits* an
already-prepared transaction — cannot be implemented directly.

So the durable record has to be the **coordinator log holding the statements**, and recovery
**re-applies** rather than commits. That forces two design consequences:

1. **Re-apply must be idempotent.** Each shard carries a dedup table written inside the same local
   transaction as the data, so "did shard 3 apply txn 91?" is answerable after any crash.
2. **The decision fsync must strictly precede the first shard commit.** That single ordering
   invariant is what makes "no decision found → abort" safe during recovery, because no shard can
   have committed before the decision was durable.

Three alternatives were considered and rejected:

- **Log first, apply eagerly, undo on failure.** Requires generating undo, and cannot handle the
  constraint violation that D6 actually exhibits — earlier shards have already committed.
- **Dry-run on each shard, then apply.** Races: another writer can insert a conflicting row between
  validation and apply, so phase 3 still fails. Unsound.
- **Hold open without a durable log.** That is C1. It resolves constraint violations but leaves the
  coordinator unable to distinguish "committed, ack lost" from "never committed" on a timeout —
  the dominant failure in a multi-node deployment.

Holding the write lock across validation and commit is genuinely required; no cheaper trick
substitutes for it.

## The protocol

```
1. route       router splits the statement into per-shard sub-statements
2. order       sort participants by ascending shard id            ← deadlock avoidance
3. prepare     each participant: BEGIN IMMEDIATE, apply rows, INSERT dedup row, DO NOT COMMIT
                 any rejection  → abort all, reply Rejected (nothing applied — D6 fixed)
                 any transport failure → abort all
4. decide      write {txn_id, participants, statements, COMMIT} to the coordinator log, fsync
                                                                  ← the ordering invariant
5. commit      each participant: COMMIT
                 a failure here is recoverable, not fatal: the decision is durable
6. resolve     mark the log entry resolved
```

**Recovery**, on startup and on the leader's periodic reconciliation:

- Entry with no durable decision → abort. No shard can have committed.
- Entry decided `COMMIT`, unresolved → for each participant, check the dedup table; re-apply where
  the txn id is absent; then resolve.
- Entry decided `COMMIT` on a shard this node no longer owns → hand to the current owner, or leave
  unresolved and surface it. Never silently drop.

**Timeout ambiguity is resolved by the dedup table.** "Did you apply txn 91?" is a question with a
durable answer. That is precisely the capability C1 lacks and the reason C2 was chosen.

**Group the decision fsync.** The coordinator log should batch decisions across concurrent
transactions into one fsync, exactly as the writer fleet already batches shard commits. Without
this, every multi-shard write pays a full fsync (~1.5 ms on the 1 CPU / 1 GB target) on the
critical path with locks held.

## Blast radius

### Tier 1 — Core write path (the hard part)

| File | Change | Risk |
|---|---|---|
| `src/storage/apply.rs` | `batch_with` opens, applies and commits in one call. Split into prepare / commit / abort. | high — this is the innermost write primitive |
| `src/shard/writer_fleet.rs` | `Job`/`Pending` gain in-doubt states; the thread loop must park a prepared shard without blocking its siblings; `OpenShard` holds a live transaction | **highest** — concurrency restructure, not new logic |
| `src/shard/mod.rs` | one coordinator serving `Route::Split`, `Route::All` and `execute_all_shards` | medium |
| new `src/shard/txn.rs` | coordinator log, dedup table, recovery | medium — pattern exists to follow |

The writer fleet is the item to worry about. Shards map to threads by `id % writer_threads` (2 by
default), and `apply_shard_batch` runs them in a loop on one thread. A naively parked transaction
freezes every sibling shard on that thread — 32 of them at 64 shards. Making prepared shards
parkable without stalling siblings is the real engineering content of this plan.

### Tier 2 — Protocol (wide, shallow, mechanical)

Eight files match `Request::`. New `Prepare` / `Commit` / `Abort` / `TxnStatus` variants touch:
`protocol.rs`, `server.rs`, `forward.rs`, `client.rs`, `auth.rs` (Requirement mapping),
`json_tcp.rs`, `http_gateway.rs`. Adding variants to a bincode enum is wire-visible, but pre-1.0
there is no external compatibility burden: a mixed-version *cluster* still needs `cluster::Compatibility`
gating so two nodes cannot disagree mid-transaction about whether prepare exists, while old clients
are simply not a constraint.

`forward.rs` is the good news: remote execution funnels through one seam (`execute_one` →
`peer.execute_remote`), which its own doc comment calls out. The distributed path follows from the
local one rather than being written twice.

### Tier 3 — Safety interactions (least code, most risk)

These are where the cost actually lives, and none of them are line counts:

- **`src/cluster/fence.rs` — leadership lost while prepared.** `check_may_write` runs *before* the
  transaction opens, with an explicit comment that a deposed leader discovering its state afterward
  has already written to a file another node owns. A held transaction reopens exactly that window.
  Needs a re-check at commit and a defined "demoted while prepared" path.
- **`src/cluster/durability.rs` — election safety.** Its doc states every other safety property is
  downstream of its comparison being right. A prepared-but-uncommitted write is not acknowledged to
  any client, so it should not affect the comparison — but that argument must be written down and
  tested, not assumed.
- **Split and transfer.** The supervisor fences shards mid-operation. A prepared transaction on a
  shard entering a split must abort, and a recovery entry naming a shard that has since moved must
  find its new owner. This interacts directly with the machinery that just shipped.
- **Replication.** `drain_capture` runs after commit and before reply, with the invariant that an
  acknowledged write has reached the stream. Two-phase commit produces frames on K shards at
  slightly different moments, so a replica sees a window with the statement half-applied. That is
  consistent with the existing no-cross-shard-snapshot property, but it means **atomicity is bought
  on the primary only** and must be documented as such.

### Tier 4 — Surfaces and docs

Console view for in-doubt transactions; metrics for in-doubt count and resolution outcome;
`docs/crash-recovery.md` extended; and the README's "no cross-shard transactions" claim becomes
"no interactive cross-shard transactions; router-split writes are atomic."

### What already exists and should be reused

This is what makes C2 tractable rather than a rewrite:

- **`catalog.rs`'s durable prepare/commit**: `.prepared` file, magic header, recovery-on-open,
  `CatalogProposal`. The coordinator log should be the same shape, in the same style.
- **Failpoints and the crash harness**: `crate::failpoint::hit`, `--features failpoints`, and
  `tests/dynamic_crash.rs`. C2's phase boundaries get the same treatment — `txn.after_prepare_all`,
  `txn.after_decision_fsync`, `txn.after_first_commit`, `txn.before_resolve`.
- **`execute_atomic` and savepoints**: the all-or-nothing group machinery exists; it simply commits
  immediately today.
- **`cluster::Compatibility`**: version gating for the new protocol variants.

## Plan

### Phase 1 · Local atomicity, no network

Coordinator log, dedup table, prepare/commit/abort in `apply.rs`, writer-fleet parking, and a single
coordinator behind all three fan-out paths — **standalone only**, with remote participants refused
explicitly.

Deliberately excludes the protocol work so the concurrency restructure lands alone and can be
reverted alone.

**Done means:** the D6 scenario returns `Rejected` with zero rows applied at N=1, 3, 4 and 16; a
partially-failing broadcast `UPDATE` and a partially-failing DDL likewise leave no trace, with
`schema_agreement()` never firing; the alignment suite's D6 case flips from fail to pass; a
concurrent-writer test shows no deadlock with transactions acquiring overlapping shard sets; and a
benchmark shows single-shard write throughput unchanged while a multi-shard transaction is in
flight — this is the acceptance criterion for the sibling-stall risk, and it will not appear in a
functional test.

### Phase 2 · Crash safety

Failpoints at each phase boundary, recovery on startup and reconciliation, and the deterministic
crash suite extended.

**Done means:** killing the process at each of the four failpoints leaves the database consistent —
either fully applied or fully absent — verified by the existing harness style; a decided-but-
unresolved entry is re-applied exactly once, proven by asserting the dedup table prevents a second
apply; and an undecided entry is aborted.

### Phase 3 · Distributed participants

`Prepare` / `Commit` / `Abort` / `TxnStatus` across the protocol surfaces, prepared-state handling
in the fencing gate, resolution against shards that have changed owner, and the compatibility gate.

**Done means:** a multi-node cluster survives killing a *participant* (not just the coordinator) at
each phase boundary; an injected network timeout during commit resolves correctly via `TxnStatus`
against the dedup table; a shard transferred between prepare and recovery is resolved at its new
owner; and a deposed leader holding a prepared transaction aborts rather than committing.

### Phase 4 · Operability

In-doubt transaction metrics and console view, an operator command to inspect and force-resolve,
and the documentation changes.

**Done means:** an unresolved transaction is visible with its participants and age, and is counted
rather than only logged.

## Risks I would not discount

**The concurrency restructure, not the protocol.** Tier 1's writer-fleet work is where this plan
fails if it fails. It is the kind of change that passes every functional test and degrades under
concurrent load. Phase 1's throughput acceptance criterion exists specifically for this.

**Reopening a settled invariant.** "Cross-shard atomicity does not exist in this design" appears in
the README, planner docs, `run_routed`, and session code. Each site needs revisiting so the codebase
does not carry contradictory statements about its own guarantees.

**The fsync on the critical path.** Grouping decisions mitigates it; it does not remove it. Expect
multi-shard writes to be meaningfully slower than today, because today they are fast by being
wrong.

**Interaction with dynamic scaling.** Split and transfer are recent and were qualified without
prepared transactions in the picture. The crash matrix grows multiplicatively, not additively.

## Decisions taken under the one-behaviour rule

These were open questions; the rule settles all three.

**1 · `Route::All` and DDL get the same treatment.** Covered above. One mechanism for every
multi-shard write. Giving atomicity to `INSERT` but not to broadcast `UPDATE` would leave a user
guessing which statements are safe.

**2 · A write is acknowledged only when every participant has committed.** No provisional success,
no error for a transaction that will in fact apply.

The rejected alternative — reply success and let reconciliation finish — creates a window where a
client is told "committed" and an immediate read misses the data. That is conditional behaviour in
time rather than in shard count, and it is the same confusion as reporting failure for a
partly-applied write. Equally rejected: returning an error when the decision is already durable,
which tells the client "failed" about something that will succeed.

So the coordinator blocks until all participants commit. If a participant is unreachable it keeps
retrying; the decision is durable, so retrying is always safe and always terminates in the same
outcome. If the coordinator dies while blocked, the client's connection drops and recovery
completes the commit — the client can then resolve the outcome via `TxnStatus` against the dedup
table. **"Acknowledged" means "durably applied everywhere", with no exceptions and no timeouts that
change the answer.**

The cost is honest and should be stated: a multi-shard write is as slow as its slowest participant.
That is the price of the guarantee, and it is preferable to a fast answer that is sometimes wrong.

**3 · The dedup table is pruned on a watermark, always.** Entries below the oldest unresolved
transaction id can never be needed, since recovery only consults ids at or above it. Pruning runs
on the same periodic reconciliation that resolves in-doubt entries — not on a size trigger, not on
a configuration knob. One rule, always on, so retention never depends on deployment.

**4 · Between Phase 1 and Phase 3, a node that does not own every participant refuses the write.**

Phase 1 is standalone-only by construction. The alternative — keeping today's non-atomic path for
partially-owning nodes in the interim — is a second behaviour, and the project is pre-1.0 with no
compatibility burden, so there is nothing to weigh against the rule. A transient refusal is
recoverable; a transient silent partial-apply is not.

The refusal names the reason and the phase that lifts it, rather than reporting a generic
unsupported error.

## Interaction with strict mode

[Strict mode](shard-alignment-plan.md#strict-mode--no-implicit-choices) refuses statements where
shardlite would otherwise choose for the user. Two-phase commit adds one such choice worth
surfacing: **a multi-shard write is now correct but materially slower**, because it is as slow as
its slowest participant plus a decision fsync.

Under strict, a write whose fan-out spans shards is refused unless the client opts in explicitly,
so a developer discovers at development time that their write pattern does not co-locate. Under
default it simply works, atomically, at the higher cost.

This is the same shape as the `Plan::Central` row in the strict table: the default is correct, and
strict tells you when correctness was expensive.
