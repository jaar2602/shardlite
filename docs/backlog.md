# shardlite backlog

Deferred work, with enough context to pick up cold.

## Console cluster provisioning — Kubernetes (enterprise, licensed)

**Future upgrade, not scheduled.** Let the console *create and manage* shardlite clusters (StatefulSet +
headless Service + per-pod PVC), not just connect to existing ones — a **licensed enterprise**
capability. Individual/self-host users stay on the Docker path (`deploy/stack/shardlite-stack`). Full
design, blockers, and slice plan in [console-cluster-provisioning-plan.md](console-cluster-provisioning-plan.md).

**Hard dependency:** full-lifecycle *scaling* needs live shard-move (shardlite plan step 11), which is
unbuilt — create/start/stop/destroy are deliverable without it, rebalance is not.

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
