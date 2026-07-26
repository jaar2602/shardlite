# shardlite backlog

Deferred work, with enough context to pick up cold.

## Dynamic scaling — membership, live movement, and linear shard splits

**In progress (experimental end-to-end path implemented; production hardening remains).** Let a cluster start
with one shard, admit storage nodes
without restarting existing members, move whole shards through snapshot/WAL catch-up and fenced
cutover, and create additional shards incrementally with linear hashing when existing shards cannot
use the new capacity. This replaces the permanent creation-time capacity choice without adding an
arbitrary token-range map.

The work is deliberately staged: cluster catalog and dynamic membership → exact replica bootstrap →
whole-shard transfer/rebalance → linear routing → durable logical row capture → two-shadow online
split and crash recovery. Full architecture, invariants, failure matrix, compatibility rules, and
acceptance gates are in [dynamic-scaling-plan.md](dynamic-scaling-plan.md).

Implemented:

- versioned legacy modulo and linear-hash routing, with exhaustive growth invariants;
- checksummed/fsynced cluster catalog, identity and compatibility validation;
- learner/storage/voter lifecycle, durable join/drain/transfer operation records;
- quorum prepare/commit wire primitives and catalog-aware election eligibility;
- exact snapshot/catch-up/final-LSN transfer guards and per-shard fence/drain barrier;
- resumable destination transfer worker, late-member catalog repair, and automatic stable
  one-shard-at-a-time rebalance;
- active linear routing in SQL/fan-out, routing-epoch forwarding checks, lazy shard activation, and
  global shard-key update refusal;
- transactional dirty-key capture, two-shadow backfill/replay, affected-shard fence, crash-resumable
  install, routing commit, and cleanup for the first supported split schema;
- seed `init`/`join`, joint voter consensus, quorum-backed lifecycle HTTP mutations, and console
  status/actions;
- deterministic process-exit failpoints and restart qualification across catalog/join, voter,
  whole-shard transfer, fenced transfer repair, and online split boundaries, with automatic
  prepared-value, membership, voter-transition, and operation recovery.

Hardening completed in this slice:

- deterministic network/disk fault hooks, stale-owner/router checks, and continuous concurrent
  read/write differential workload qualification (including repeated process restarts and replica
  loss; the long-running tests are intentionally `#[ignore]` and require a network-enabled runner);
- bounded split-log backpressure with a retryable sentinel and resumable, fsynced large-table scan
  checkpoints (including orphan/corrupt checkpoint refusal);
- per-replica split shadow/install and cleanup acknowledgements with image digest verification;
- differential support for keyless rowid, composite-key, and BLOB-key tables; virtual and
  `WITHOUT ROWID` tables remain fail-closed pending module/tuple-identity proof;
- capacity weights, failure-domain-aware placement, split/transfer resource budgets, and mutable
  policy controls;
- console display/edit controls for those policies and a read-only packaged-stack verification
  script at `deploy/stack/verify.sh`.

Still required before calling dynamic mode production-ready:

- an authenticated, single-use join-token issuance and console provisioning workflow (the current
  console can operate already-joined members, while `shardlite join` remains the explicit
  self-hosted bootstrap path);
- production fault injection for true network partitions, disk-full/short-write/fsync corruption,
  and replica loss under every placement policy, plus deployment verification in a network-enabled
  CI environment.

Capacity weights and failure-domain-aware replica placement are implemented. The catalog now carries
operator resource budgets for split/transfer source files and exposes policy/member controls through
the dynamic HTTP catalog API and console. `deploy/stack/verify.sh` provides a read-only packaged-stack
smoke check. Join-token issuance/provisioning remains gated on the authenticated token protocol and
provider orchestration; the Docker demo deliberately stays fixed-shard.

## Console cluster provisioning — Kubernetes (enterprise, licensed)

**Future upgrade, not scheduled.** Let the console *create and manage* shardlite clusters (StatefulSet +
headless Service + per-pod PVC), not just connect to existing ones — a **licensed enterprise**
capability. Individual/self-host users stay on the Docker path (`deploy/stack/shardlite-stack`). Full
design, blockers, and slice plan in [console-cluster-provisioning-plan.md](console-cluster-provisioning-plan.md).

**Hard dependency:** full-lifecycle *scaling* builds on the dynamic membership, bootstrap, transfer,
and rebalance slices in the [dynamic scaling plan](dynamic-scaling-plan.md). Those core paths now
exist; the remaining provisioning work is provider orchestration, join-token delivery, resource
preflight, audit/approval UX, and production failure qualification.

## Owner-aware S3 archive (correct stale-local-file recovery)

**Why deferred:** automatic recovery of a *reclaimed* shard whose local file exists but is behind
S3 cannot be done safely today. `StreamPositions` is **per-node** — each owner archives with its own
fresh LSN counter and its own stream epoch (bumped only on *that node's* unclean restart) — so the S3
keys `…/snapshot/{epoch}_{lsn}` are **not comparable across owners** (epochs can collide, LSNs reset
on each new owner). A naive local-vs-S3 comparison therefore risks either serving stale data or, worse,
**overwriting un-archived acknowledged writes**. It also means the current recovery is only guaranteed
correct for the common *single-previous-owner* failover; a shard that changed owners several times has
a latent selection ambiguity.

**Current safe behaviour (shipped):** a reclaimed shard with a local file **keeps the local file**
(never loses acknowledged writes); `POST /v1/shards/{n}/recover-from-s3` lets an operator force S3's
version when they know it is authoritative.

**Correct fix:** make the archive **owner-aware** — a per-shard *ownership generation* (incremented
when the shard's owner changes, NOT the Raft term, which bumps on every election) threaded through the
archive keys, so versions are globally ordered. Then:
- recovery selects the highest-generation snapshot + its change-log (no cross-owner mixing);
- the auto-recovery decision compares the S3 generation to a locally-persisted "owned-at generation",
  recovering only when S3 is strictly newer — provably without losing un-archived local writes.

This touches the archival/replication path (the highest-risk code after the VFS), so it needs its own
design + the failover/partition test harness. Tests must cover: the data-loss-prevention case (local
ahead of S3 → keep local) and the multi-owner-history case.
