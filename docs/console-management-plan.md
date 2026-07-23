# Console → full shardlite management — completion plan

Goal: make the console able to **manage the entire shardlite**, not just observe it and run SQL.

This plan builds on [`console-v2-plan.md`](console-v2-plan.md). That plan's observability phases are
largely **done**; this one completes the **control plane** it deferred ("Later operator endpoints…
not part of the first v2 milestone"), plus the half-built S3 feature.

## Where we are (grounded in the current code)

Already shipped in the console (`console/…`) and drivable over HTTP `/v1/*`:

- **Observe**: Fleet dashboard, Topology/Cluster (members, leader, term, roles, placement),
  per-node Health (storage/consensus/placement checks), Shard Inventory (owner, epoch, LSN,
  replicas, lag, state), Stats (writer/reader/checkpoint/http counters), Schema explorer + drift.
- **Operate (data & schema)**: SQL workbench (streaming reads, auto-routed writes, EXPLAIN),
  durable **schema-rollout Operations** (preflight → submit → per-shard rollout → cancel).
- **Administer**: shardlite user CRUD (`/v1/users`), console users, audit ledger, sealed connection
  secrets, per-connection **S3 settings stored & sealed**.

The read/observe surface is essentially complete. The gap is **actions that change cluster or
storage state** — and most of them have **no shardlite network endpoint yet**, so the UI cannot drive
them regardless of front-end work.

## The one hard constraint that shapes everything

**Almost every management action needs a shardlite endpoint first.** Today these are CLI/startup-only
with no network path: vacuum, manual checkpoint, shard-key declaration, schema-agreement report,
force-election/step-down/drain/move-shard, S3 config (startup flag only), on-demand S3
snapshot/flush, failover trigger, runtime config. The console literally stores S3 config it **cannot
apply**, because shardlite only accepts S3 as `serve` flags (`registry.rs` even has a dead-code TODO
saying so).

So the unit of work is a **vertical slice**, in this order:

1. **shardlite**: a typed, versioned, authorized, **tested** endpoint — idempotent, fenced, with
   explicit failure semantics (honoring shardlite's "refuse over approximate" rule).
2. **console backend**: extend the `proxy_permission` allowlist (or add a server-aggregated route),
   enforce the role, **audit** the mutation.
3. **console frontend**: a view/action, with a confirmation modal for anything destructive.

Reuse the **durable job contract that already exists** on the console side (`/api/operations`, built
for schema rollout) for every long-running/destructive action — extend it with new operation types
rather than inventing per-action flows.

## Phases (each item is one vertical slice)

### Phase A — Observability completeness (low risk, read-only, no core surgery)

Cheap wins that make the control plane safe to act on later (you can't manage what you can't see).

- **A1 · WAL-conversion stats over HTTP** — surface `storage::wal_conversion_stats`
  (retries/contended/failed opens) in `/v1/stats` (currently CLI `.stats` only) → Stats view.
- **A2 · Schema-agreement report** — expose `ShardManager::schema_agreement` as
  `GET /v1/schema/agreement` (today only per-shard `schema_version` is exposed) → Schema view drift
  panel gains a real cluster-wide verdict.
- **A3 · Replication position / lag** — expose `AckTracker` / follower positions as
  `GET /v1/replication` (per shard: primary LSN, per-replica applied LSN, ack quorum, lag) → a new
  **Replication** panel and richer Shard Inventory.

### Phase B — S3 replication lifecycle (finish the half-built feature; highest user value)

The console already has the sealed config UI — it just has nowhere to send it.

- **B1 · Runtime S3 config endpoint** — `POST /v1/s3/config` (Admin) to attach/detach an S3 sink on
  a running node (today S3 is `serve`-flags-only, so a running cluster can't be told to replicate).
  Wire the console's stored, sealed S3 settings to push here on apply/connect. **This is the slice
  that turns dead-code config into a working feature.**
- **B2 · S3 status** — `GET /v1/s3/status` (Operator): sink health, per-shard last snapshot
  `(epoch, lsn)`, change-log lag, last upload error → a new **Replication / S3** view.
- **B3 · On-demand snapshot / flush** — `/v1/operations {type: s3_snapshot|s3_flush}` (Operator) via
  the job contract → button in the S3 view.
- **B4 · Failover visibility & guarded trigger** — surface `latest_snapshot` freshness and a
  heavily-gated `open_from_s3` trigger (Operator + confirm + audit) for a shard whose owner is down.

### Phase C — Shard / storage operations (via the job contract)

- **C1 · Vacuum a shard** — `/v1/operations {type: vacuum, shard}` wrapping `ShardManager::vacuum`
  (fenced, cancel-safe) → action in Shard Inventory.
- **C2 · Manual checkpoint** — `/v1/operations {type: checkpoint, shard}` (checkpointing is
  automatic today; expose a manual trigger for operators) → Shard Inventory.
- **C3 · Shard-key declaration** — `POST /v1/shardkey` (Admin) wrapping `declare_shard_key`, with
  guardrails: declare-only when the table has no rows on any shard (changing an existing key means
  data movement — refuse, don't silently mis-place) → Schema view. (Note: this composes with the
  cross-node adoption fix — the endpoint must broadcast like DDL does, not land on one node.)

### Phase D — Cluster control plane (highest risk; needs real core design)

Placement is currently **derived from liveness only** — there is no operator override. These require
genuine shardlite work (fencing, handover, safety proofs), so they come last and each ships behind
confirmation + audit + Operator role.

- **D1 · Drain a node** — wire `ClusterNode::stop` (exists, unwired) to `POST /v1/cluster/drain`
  (graceful departure → placement re-derives) → Topology action.
- **D2 · Step down / trigger election** — `POST /v1/cluster/step-down` (leader relinquishes) →
  Topology.
- **D3 · Move a shard / placement override** — the hardest: an operator-pinned placement with fenced
  handover. Design the core mechanism first (pinned overrides layered over `Placement::balanced`),
  then the endpoint, then a drag-in-Topology UI. Ship last.

### Phase E — Runtime configuration

- **E1 · Safely-mutable config** — `PATCH /v1/config` for the few runtime-safe knobs (max-conn, S3
  snapshot cadence) → a Settings panel.
- **E2 · Honest immutability** — surface the deploy-time-immutable values (shard count, peers, TLS,
  data dir) as **read-only with a reason**, rather than pretending they're editable.

## Cross-cutting requirements

- **Authorization, twice**: every mutating endpoint enforces a shardlite `Requirement` *and* the console
  `proxy_permission` allowlist. Add/confirm the **Operator** role (console-v2-plan proposes it;
  shardlite already has read/write/admin/cluster). Never use SQL-keyword parsing as a boundary.
- **Audit everything**: extend the append-only ledger to every new operation (it already records
  writes/user/connection/operation changes).
- **Idempotency + refuse-over-approximate**: operations take an idempotency key; a partial or
  uncertain result is an **error with a reason**, never a plausible-looking success.
- **Confirmation UX**: destructive/cluster-affecting actions (drain, move-shard, failover, vacuum)
  require a typed confirmation and show blast radius.
- **Contract tests**: per `console-v2-plan.md`'s "contract ownership" — checked JSON fixtures so the
  console and shardlite API versions can't silently drift.

## Suggested first slice

**Phase B1 + B2 (S3 config apply + status).** It is the one feature that is visibly half-done (config
sealed on disk but never sent to any node), it is high user value, and it exercises the entire stack
— a new shardlite runtime endpoint, console proxy/policy + un-sealing the stored secret to push it, and
a new Replication/S3 view. Delivering it proves the vertical-slice pattern the rest of the plan
repeats.

## Effort / risk at a glance

| Phase | Core (shardlite) work | Risk | Value |
|---|---|---|---|
| A — observability gaps | small (read endpoints) | low | medium |
| B — S3 lifecycle | medium (runtime sink attach) | medium | **high** |
| C — shard/storage ops | medium (wrap existing internals) | medium | high |
| D — cluster control plane | **large** (placement override, fencing) | **high** | high |
| E — runtime config | small | low | medium |
