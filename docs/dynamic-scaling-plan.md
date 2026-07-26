# Dynamic scaling — membership, live shard movement, and linear splits

> **Status: experimental vertical slice implemented; acceptance hardening remains.** Catalog
> membership, `init`/`join`, exact whole-shard transfer, automatic rebalance, linear SQL routing,
> joint voter consensus, first-schema logical split, HTTP mutations, and console controls are
> wired. Replica-aware split install, bounded capture backpressure, and
> partition/disk-fault and continuous-workload qualification remain before a production claim.
> Capacity-weighted placement, failure-domain labels, and split/transfer source-size budgets are
> now durable catalog policy and editable through the HTTP/console control surface.
> Deterministic process-crash/restart qualification at the durable phase boundaries is implemented;
> see [crash-recovery.md](crash-recovery.md).

## Product contract

The intended experience is:

```sh
# Create the first node with one active shard.
shardlite init ./data --listen 0.0.0.0:4600

# Add capacity later. The cluster supplies identity, routing, and placement.
shardlite join ./data --seed node1:4600 --listen 0.0.0.0:4600
```

The first node begins with one shard unless an initial parallelism hint is supplied. Adding a node
does not give that node a different shard count. The node joins the same logical keyspace, initially
owns nothing, receives replicas, and becomes eligible for placement only after its data is ready.
The current implementation still provisions a local allocation ceiling (default/max 256) in each
manifest, while activating logical shards incrementally; removing that ceiling is separate format
work and is not hidden by the dynamic routing contract.

The cluster grows in two ways:

1. **Move** an existing whole shard when enough shards already exist.
2. **Split** one shard into two when more write lanes or smaller files are needed.

Whole-shard movement remains the preferred operation because it uses shardlite's existing physical
snapshot and WAL replication. Splitting is used only when placement cannot produce enough balance
from the shards that already exist.

### What “zero downtime” means here

- There is no cluster-wide maintenance window.
- Unaffected shards continue serving reads and writes throughout a move or split.
- During the final cutover, requests for the one affected shard may queue briefly or receive a
  retryable response; they must never receive a plausible but stale or duplicated answer.
- Every acknowledged write remains present after success, retry, process crash, or coordinator
  failover.
- A cross-shard read never includes both the old and new copy of a row.

This is availability with a bounded per-shard cutover, not a promise that every request keeps its
usual latency during that cutover.

## Why linear hashing

The current route is:

```text
shard = fnv1a(key) % fixed_shard_count
```

Changing the divisor reroutes most keys. A general token-range map fixes that, but adds a large,
frequently coordinated metadata structure. Linear hashing needs only a level and a split pointer:

```text
base = 2^level
shard = hash(key) % base
if shard < split_pointer:
    shard = hash(key) % (2 * base)
```

The number of active shards is `base + split_pointer`. Splitting shard `split_pointer` creates
shard `split_pointer + base`; only keys in that source shard can move. Then the pointer advances.
When it reaches `base`, the level increments and the pointer returns to zero.

Examples:

| Active shards | `level` | `split_pointer` | Next source → destination |
|---:|---:|---:|---|
| 1 | 0 | 0 | 0 → 1 |
| 2 | 1 | 0 | 0 → 2 |
| 3 | 1 | 1 | 1 → 3 |
| 4 | 2 | 0 | 0 → 4 |
| 5 | 2 | 1 | 1 → 5 |

Properties that tests must pin:

- Routing is deterministic on every node and after restart.
- A growth step changes routes only for keys formerly in the one source shard.
- A key that moves goes to the newly created shard, never a third shard.
- All shard IDs below `active_shards` are reachable.
- Distribution remains approximately even over a representative high-cardinality key set.

Extract the stable FNV-1a calculation from today's `shard_of`; its bytes-to-hash behavior remains a
storage-format contract. Add a routing scheme/version so a future hash change is necessarily an
explicit migration.

## Hard invariants

These are implementation constraints, not aspirations:

1. **One committed routing state.** Every routed request carries or observes a routing epoch.
   Servers using an older epoch forward, refresh, or return a retryable error; they never guess.
2. **One writable owner.** An active shard has exactly one primary for one ownership generation.
   The previous owner's write gate closes before the next owner's opens.
3. **Data before ownership.** A node cannot become primary until its copy is verified through an
   exact `(stream_epoch, LSN)` barrier for the ownership generation being transferred.
4. **Replicas before movement.** Placement cannot reduce a shard below the configured replica
   policy merely to make rebalancing progress.
5. **Durable operation state.** Join, transfer, split, drain, and removal are persisted as
   idempotent operations. Restart resumes or rolls them forward from durable phase markers.
6. **No duplicate visibility.** A split destination is hidden from normal reads until cutover.
   Cutover does not publish the new routing epoch until clean source and destination images are
   prepared; affected requests wait while those images are installed.
7. **No silent rollback after commit.** Before a routing/ownership commit, an operation may abort
   and discard its destination. After commit, recovery rolls forward; it never resurrects the old
   route or owner.
8. **Shard keys are immutable.** Updating a row's shard key is refused. Moving a row between
   SQLite files cannot be one SQLite transaction, so pretending it is an ordinary `UPDATE` would
   violate atomicity.
9. **Schema and topology operations do not overlap on one shard.** A schema roll waits for a split
   to finish (or a not-yet-committed split aborts). A split starts only from schema agreement.
10. **Keyless data has an explicit home.** Tables without a shard key remain pinned to anchor shard
    0 and are reported as non-scalable. When shard 0 splits, both shadows get the schema but only
    the source half retains keyless rows.

## Cluster catalog

Dynamic routing and membership cannot be derived independently from static command-line peers.
Introduce a small quorum-replicated `ClusterCatalog`; it is metadata consensus, not replication of
user SQL.

Conceptually:

```rust
struct ClusterCatalog {
    cluster_id: ClusterId,
    catalog_version: u64,
    compatibility: Compatibility,
    routing: RoutingState,
    members: BTreeMap<NodeId, Member>,
    placements: BTreeMap<ShardId, ReplicaSet>,
    operations: BTreeMap<OperationId, Operation>,
}

struct Compatibility {
    directory_format: u32,
    sqlite_version: String,
    page_size: u32,
    routing_scheme: String, // "modulo-v1" or "linear-v1"
    hash_scheme: String,    // "fnv1a-v1"
}

struct RoutingState {
    epoch: u64,
    level: u8,
    split_pointer: u32,
}

struct ReplicaSet {
    generation: u64,
    primary: NodeId,
    replicas: Vec<NodeId>,
}
```

The exact serialization may use the project's existing serde/bincode stack. The durable record
needs a header, checksum, term/index, fsync-before-ack discipline, and snapshot/compaction; a torn or
unknown record is refused rather than treated as an empty cluster.

### Membership roles

Do not make every storage node an election voter. That causes election quorum and heartbeat work to
grow with storage capacity.

- **Voters**: a small odd set (normally 3 or 5) holding the catalog/election quorum.
- **Storage members**: host shard primaries and replicas; may be non-voting.
- **Learners**: joined and visible, but ineligible for ownership while validating/bootstrap is in
  progress.
- **Cordoned members**: keep serving current replicas and may vote, but receive no new placement.
- **Draining members**: actively transfer all primaries and replicas away before removal.

Changing the voter set needs joint-consensus semantics. Adding ordinary storage capacity does not:
the catalog quorum records the new learner/member and its address. A cluster initialized on one
node may automatically promote caught-up learners until it reaches its configured 3- or 5-voter
target; nodes beyond that target remain non-voting storage members unless an explicit voter
replacement is requested.

### Join validation

A join request sends node identity, advertised address, build/protocol versions, SQLite version,
page size, supported directory/routing/hash formats, and any existing local `cluster_id`.

The leader refuses:

- a data directory belonging to a different cluster;
- a SQLite/page format incompatible with physical replication;
- an unknown routing or hash scheme;
- a node ID already bound to another incarnation/address without an explicit replacement;
- non-empty shard files whose identities/generations are not present in the catalog.

A successful join writes the cluster identity and compatibility contract to the local manifest,
adds the node as a learner, and distributes its address through the catalog. `--peers` becomes a
legacy bootstrap mechanism; a seed discovers current membership but is not permanent configuration.

## Whole-shard transfer state machine

Build and ship this before online splitting. It makes adding nodes useful for every cluster that
already has at least as many shards as desired write lanes.

```text
Planned
  → Snapshotting
  → CatchingUp
  → Prepared
  → Fencing
  → Committed
  → Cleaning
  → Complete
```

1. **Planned** — catalog records source, destination, shard, old generation, and operation ID.
   Placement still names the source primary.
2. **Snapshotting** — use existing resumable bootstrap to install an identified
   `(shard, stream_epoch, LSN, size)` image on the destination.
3. **CatchingUp** — stream physical WAL frames. Destination reports durable applied LSN, never only
   an in-memory receive position.
4. **Prepared** — destination is within the configured lag bound and has verified snapshot
   identity, schema, and integrity. It still cannot answer as primary.
5. **Fencing** — close the source write gate and drain its writer queue. Record the final LSN and
   wait until the destination durably applies it.
6. **Committed** — one quorum catalog entry increments the ownership generation and names the
   destination primary. Only after observing that entry may the destination open its write gate.
7. **Cleaning** — source becomes a replica or removes its copy only after replica policy is
   satisfied elsewhere.

Failure rule: phases before `Committed` abort to the source owner; phases at or after `Committed`
roll forward to the destination. Every network command includes operation ID and expected phase so
duplicates are harmless and stale commands are refused.

## Placement and gradual rebalance

Replace the current stateless round-robin recomputation with a placement planner that minimizes
movement from the committed map.

Inputs:

- eligible members and cordon/drain state;
- configured capacity weights;
- shard primary and replica locations;
- shard bytes, write rate, read rate, and bootstrap cost;
- one active operation per shard and bounded operations per node.

Rules:

- Never move a healthy shard merely because member iteration order changed.
- Prefer promoting an already caught-up replica over bootstrapping another copy.
- Move one shard at a time by default; configurable concurrency must preserve per-node disk and
  network bounds.
- Place replicas on distinct failure domains when labels are available.
- Do not place a primary on a learner, cordoned node, or replica below its required LSN.
- A drain is complete only when the node owns neither primaries nor required replicas.

Adding a node first rebalances whole shards. Only if the planner has too few shards to use the new
capacity, or a shard exceeds a size/load threshold, does it request a split.

## Online split design

### Why physical WAL is insufficient

Physical replication works while two files have the same page layout. A split produces two files
with different subsets of rows, so source WAL pages cannot be filtered and applied safely: one page
can contain rows whose hashes belong to both halves.

Statement dual-writing is also insufficient. `random()`, time functions, triggers, and machine-local
errors can produce different resulting rows. The split stream must contain the resulting logical row
changes, not SQL to execute again.

### Durable logical split log

Add a split-only logical change log on the source shard. It must commit atomically with the user
transaction and contain resulting typed values:

```text
sequence
table identity
operation: insert/upsert/delete
shard-key bytes
encoded old/new row values as required
source transaction identity
```

One viable implementation is generated temporary SQLite triggers plus a private internal log table:
triggers see final `OLD`/`NEW` values after nondeterministic expressions and write the log in the
same transaction. A registered packing function can encode SQLite `NULL`, integer, real, text, and
BLOB values without JSON coercion. The implementation spike must prove triggers interact correctly
with STRICT tables, WITHOUT ROWID tables, user triggers, BLOBs, rollback/savepoints, and batched
writer transactions before this mechanism is accepted.

If that spike cannot meet the invariants, stop and evaluate SQLite Session changesets or a durable
execution-layer row-change capture. Do not fall back to replaying SQL.

The split log is bounded by backpressure: if the destination cannot consume it and the configured
disk/log limit is reached, affected writes become retryable failures rather than allowing an
unbounded log or dropping changes.

### Two clean shadows

To avoid duplicates in fan-out reads, do not publish a destination while the live source still
contains its rows. Build two hidden shadow files:

- **left shadow** — rows that remain in the source shard;
- **right shadow** — rows that move to the new destination shard.

Both receive schema and a consistent backfill from the source, partitioned using the *next* linear
routing state. Logical changes after the backfill barrier are replayed to the appropriate shadow.
The original source remains the only query-visible copy during this work.

Split phases:

```text
Planned
  → Logging
  → Backfilling
  → Replaying
  → Prepared
  → Fencing
  → Installing
  → RoutingCommitted
  → Cleaning
  → Complete
```

1. **Planned** — catalog names source, deterministic destination, old/new routing states, operation
   ID, schema version, and required free-space budget.
2. **Logging** — install durable logical capture before choosing the backfill barrier.
3. **Backfilling** — create both shadows with identical schema; stream every keyed row to exactly
   one shadow. Copy keyless rows only to the shard-0 side.
4. **Replaying** — apply logical records in source transaction order to the proper shadow.
5. **Prepared** — both shadows are fsynced, pass integrity/schema checks, and have replayed through
   a durable logical sequence.
6. **Fencing** — close the source gate, drain its queue, and replay through the final committed
   sequence. Requests for this shard wait or fail retryably; unrelated shards continue.
7. **Installing** — persist a prepared-cutover catalog phase, quiesce source readers/writers, install
   the clean left source image and clean right destination image, and fsync files/directories.
8. **RoutingCommitted** — quorum-commit the next routing epoch. The active shard set now includes the
   destination. Gates open only after nodes observe this state and the installed file identities.
9. **Cleaning** — retain old files/logs under the operation ID until the committed state is verified
   and replica policy is restored, then remove them recoverably.

There is deliberately a short affected-shard gate between `Fencing` and `RoutingCommitted`. Serving
through a half-installed multi-file cutover would risk missing or duplicated rows; bounded waiting is
the honest zero-downtime behavior.

### Split limitations in the first release

- One split at a time cluster-wide; concurrency can be relaxed only after failure testing.
- Shard-key updates are refused globally, not merely during a split.
- Every scalable non-empty table must have a declared single-column shard key with routable SQLite
  storage classes. Keyless tables remain on anchor shard 0.
- Virtual tables and schema objects the backfill/capture spike cannot reproduce are reported as
  blockers before logging starts.
- DDL and split are mutually exclusive.
- Merge/scale-down is deferred. Removing nodes moves their shards elsewhere; it does not reduce the
  logical shard count.

## Automatic split policy

Splitting is driven by mutable policy, not a capacity number burned into the data directory:

- target and maximum shard bytes;
- sustained writer queue pressure / write rate;
- minimum free space for two shadows plus retained logical changes;
- minimum time between splits;
- maximum concurrent movement bytes per node;
- desired minimum shards per eligible node.

Size/load metrics propose a split; the catalog leader decides and persists it. Thresholds can be
changed at runtime. Automatic work pauses when the cluster lacks replica health, disk headroom,
schema agreement, or catalog quorum.

A single hot key remains unsplittable. Status must distinguish “hot shard, splittable” from “hot
key, splitting will not help” where sampling can establish that honestly.

## Compatibility and migration

Do not reinterpret existing manifests silently.

- Existing directories remain `modulo-v1` and keep their immutable count.
- New clusters use `linear-v1`.
- A `modulo-v1` cluster whose shard count is a power of two has routing equivalent to
  `linear-v1 { level: log2(count), split_pointer: 0 }`; conversion still requires an explicit,
  quorum-recorded format operation so every node changes scheme together.
- A non-power-of-two modulo cluster requires a normal online redistribution into linear routing;
  until implemented, it can use dynamic membership and whole-shard movement but cannot split.
- Local manifests stop being the authority for mutable routing. They record cluster identity,
  compatibility, and last applied catalog version. The quorum catalog owns current routing and
  placement.
- Keep accepting `--shards` for legacy create/open during a deprecation window. New `init` treats
  `--initial-shards` as a mutable-performance starting point, not a permanent maximum.

## Observability and operator API

Expose enough state to answer “is it safe to add/remove this node?” without reading logs:

- catalog term/version and routing epoch;
- member role, incarnation, liveness, cordon/drain state, capacity, and compatibility;
- per-shard primary, replicas, ownership generation, bytes, LSNs, and lag;
- active operation phase, source/destination, bytes copied, logical sequence lag, retries, and last
  error;
- split eligibility and explicit blockers;
- movement/split throughput and estimated temporary disk requirement.

Minimum operations:

```text
node join
node cordon / uncordon
node drain
node remove
operation status
operation retry / abort       # abort only before commit
rebalance status / pause / resume
split shard / auto
```

All state-changing operations are authenticated, authorized, audited, and idempotent.

## Console management contract

The web console is the primary operator experience for scaling, but it is never the owner of a
scaling state machine. Join, transfer, split, drain, and recovery live durably in the cluster
catalog and continue if the browser closes or the console restarts. The console submits desired
state, displays cluster-owned progress, and exposes only phase-appropriate controls.

### Two provisioning modes

**Managed infrastructure (initially Kubernetes):**

1. User selects **Add node** or changes desired capacity.
2. Console runs a cluster preflight: catalog quorum, replica health, compatibility, free-space
   budget, and absence of conflicting operations.
3. Console creates the pod/PVC.
4. The new process joins as a learner using a short-lived, scoped join token.
5. Cluster bootstrap/rebalance runs and reports progress from its durable operation record.
6. Console marks the scale-out complete only when the node is eligible and the requested balance
   has been reached.

Scale-in reverses the safety order: cordon and drain in shardlite first, verify the node owns no
required primary or replica, remove it from the catalog, and only then delete its pod/PVC according
to the selected retention policy.

**Self-hosted infrastructure:**

The console must not gain arbitrary host, SSH, or Docker-socket access. **Prepare node** creates a
single-use join token and shows the exact `shardlite join --seed ... --token ...` command. The user
starts the process wherever they choose; from that point the same validation, bootstrap, rebalance,
drain, and removal UI applies.

### Console surfaces

- **Cluster / Capacity** — desired and observed nodes, voters/storage members, shard count, routing
  epoch, replica policy, usable/free bytes, and split policy.
- **Add node / Prepare node** — managed provisioning or self-hosted join-token flow.
- **Node detail** — learner/member/voter role, compatibility, primaries, replicas, lag, cordon,
  drain, voter promotion/replacement, and safe removal.
- **Rebalance** — proposed moves/splits, resource estimate, start/pause/resume, concurrency limits,
  and explicit blockers.
- **Scaling operations** — durable phase timeline, byte/row progress, LSN/log lag, retry count,
  last error, and whether abort is still safe.
- **Shard detail** — size/load history, placement, replicas, split eligibility, and an advanced
  manual split action. Normal scale-out should choose moves/splits automatically.

The UI may offer **Cancel** only before the catalog operation's commit boundary. Afterwards it says
**Complete recovery** or **Retry** because the only safe behavior is roll-forward. Scale-in,
removal, replica-policy reduction, forced voter replacement, and manual split require typed
confirmation; routine scale-out does not.

### API and security boundary

- Shardlite exposes cluster-owned operation resources, for example
  `POST /v1/cluster/operations` and `GET /v1/cluster/operations/{id}`. Exact paths can follow the
  existing HTTP conventions, but status must come from the catalog rather than console memory.
- Console proxies those resources into its existing Operations experience and adds its own
  infrastructure-provisioning stages where applicable.
- Submission carries an idempotency key. Repeating a timed-out request returns the same operation.
- Join tokens are single-use, short-lived, cluster-scoped, and authorize only learner admission—not
  SQL, catalog mutation, or immediate voter/primary status.
- Read-only users can inspect topology and progress. Cluster operators can scale out, rebalance,
  cordon, and drain. Only administrators can reduce replicas, force membership replacement, or
  destroy retained data.
- Every preflight, approval, operation transition requested by an actor, and destructive decision
  is appended to the audit ledger.

The console must display an honest distinction between **infrastructure ready** (the pod/process is
running), **data ready** (required replicas caught up), and **capacity active** (placement actually
uses the node). A green pod alone is not a successful database scale-out.

## Build order and acceptance gates

Each slice must leave the existing fixed-shard path working and be independently testable.

### 0 — Routing model and failure-test harness

- Extract stable `hash_key`.
- Implement pure `ModuloV1` and `LinearV1` routers plus exhaustive/property-style tests.
- Add deterministic failpoints at operation phase boundaries and a restart harness.

**Done when:** growth from 1 through at least 1,024 shards never moves a key outside the selected
source/destination pair, and the harness can kill/restart a process at named phases.

**Status:** done. The ignored `dynamic_crash` integration target launches real server processes,
exits with no destructor unwinding at named boundaries, restarts the same directories, and verifies
terminal catalog state plus complete key/value rows.

### 1 — Cluster identity, compatibility, and catalog

- Durable quorum catalog with term/index/checksum/fsync.
- Cluster ID and join compatibility handshake.
- Routing/ownership epochs on internal requests.
- Legacy fixed peer configuration continues to work.

**Done when:** incompatible nodes and foreign data directories are refused before receiving data or
opening a write gate; a stale catalog/routing epoch cannot perform a write.

### 2A — Dynamic storage membership

- Seed-based learner join and catalog-distributed addresses.
- Separate storage membership from the small voter set.
- Cordon, drain intent, and removal lifecycle.

**Done when:** a node absent at cluster creation joins without restarting existing nodes, remains
write-ineligible, and every existing node learns how to reach it.

### 2B — Dynamic voter membership

- Replicate the catalog to a learner before it can vote.
- Add/remove voters through joint consensus; never switch directly between two quorum sets.
- Automatically grow a one-voter development cluster to its configured 3/5-voter target as
  compatible learners catch up; require explicit replacement beyond that target.

**Done when:** a cluster initialized on one node can add two nodes, commit a three-voter
configuration without restart, survive any one voter loss, and failpoint/partition tests cannot
construct two catalog quorums for one term/index.

### 3 — Replica inventory and exact bootstrap

- Persist replica identity `(cluster, shard, ownership_generation, stream_epoch, LSN)`.
- Wire existing resumable snapshot/WAL primitives into join operations.
- Enforce replica count and placement constraints.

**Done when:** a learner receives a byte-correct shard, resumes interrupted bootstrap, catches up to
an exact LSN, and still owns no writes.

### 4 — Fenced whole-shard cutover

- Durable transfer state machine.
- Source drain/fence, final-LSN barrier, catalog ownership commit, destination gate.
- Pre-commit abort and post-commit roll-forward recovery.

**Done when:** a continuous keyed workload sees no lost acknowledged writes while a shard changes
owners, and kill injection at every phase converges to exactly one writable owner.

### 5 — Gradual automatic rebalance

- Stable, movement-minimizing placement.
- Capacity weights, failure-domain replica rules, concurrency/disk/network budgets.
- Drain and removal complete through the same transfer state machine.

**Done when:** adding a node rebalances existing whole shards one at a time without a global outage;
cordon and drain never reduce healthy replica count.

### 6 — Linear routing for new clusters

- Mutable routing state in the catalog and active-shard enumeration throughout fan-out, placement,
  metrics, and replication.
- New `init`/`join` UX and legacy manifest behavior.
- Reject shard-key updates.

**Done when:** a new one-shard cluster can commit routing metadata for its next deterministic shard,
while legacy modulo directories retain byte-for-byte routing behavior.

### 7 — Logical split-log spike

- Prove atomic typed row capture and replay through commit, rollback, savepoint failure, triggers,
  STRICT/WITHOUT ROWID tables, and process crash.
- Bound and backpressure retained logical changes.

**Done when:** destination reconstruction is differential-tested against the source's logical
contents under nondeterministic SQL and concurrent writes. If this gate fails, online split work
stops; SQL replay is not accepted as a substitute.

### 8 — Shadow backfill and verification

- Build left/right hidden files from one source.
- Resume interrupted table scans without duplicating rows.
- Replay the logical log and verify schema, integrity, row placement, and content.

**Done when:** the union of shadows equals the source, their keyed row sets are disjoint, every row
matches `LinearV1` next-state routing, and keyless rows exist only on the anchor side.

### 9 — Online split cutover and crash recovery

- Durable split state machine, prepared file installation, routing epoch commit, cleanup.
- Queue/retry affected-shard requests and keep unrelated shards live.

**Done when:** continuous point and fan-out differential workloads observe neither missing nor
duplicated rows, and failpoint restart from every phase converges without manual file edits.

### 10A — Policy-driven growth and add-node integration

- Size/load thresholds and resource budgets.
- Rebalance chooses move first, split only when needed.
- Progress/status/operator controls.

**Done when:** starting from one shard, repeatedly adding nodes under load causes automatic
split/bootstrap/placement and uses the new capacity without a permanent creation-time setting.

### 10B — Console scaling workflow

- Cluster-owned scaling operation API and console proxy/types.
- Capacity, join, node lifecycle, rebalance, split, and operation-progress UI.
- Kubernetes desired-capacity orchestration and self-hosted one-time join-token flow.
- Role checks, preflight, typed confirmations, idempotency, and audit records.

**Done when:** a user can add a node from the managed console—or prepare and start one
self-hosted—watch it pass from infrastructure ready to data ready to capacity active, then drain and
remove it without CLI database operations. Closing/restarting the console during every phase does
not interrupt or lose the cluster operation.

### 11 — Merge and placement refinements (separate follow-ups)

- Optional adjacent linear-bucket merge for reducing file count.
- Concurrent independent moves/splits after the one-at-a-time paths are proven.
- More advanced hot-range placement if linear split order becomes a demonstrated limitation.

None blocks the core start-small and add-node promise.

## Failure and correctness matrix

The integration suite must cover at least:

- source, destination, coordinator, and voter crash/restart at every durable transfer and split
  boundary (**deterministic process-exit matrix implemented**);
- network partition before and after ownership/routing commit;
- stale owner attempting writes after a newer ownership generation;
- stale router issuing requests across a routing epoch change;
- destination disk full, logical-log cap, snapshot invalidation, and checksum mismatch;
- DDL racing with a requested split;
- node drain while another node is unavailable;
- replica loss during transfer;
- repeated operation messages and replay after restart;
- inserts, updates, deletes, constraint failures, user triggers, BLOB/NULL/numeric/text keys;
- point reads and global aggregates throughout movement;
- non-power-of-two active shard counts;
- legacy modulo cluster join and refusal of incompatible conversion.

Use SQLite on an unsharded copy as the logical-answer oracle where possible. For every test that
claims no duplicates or loss, make the workload non-vacuous and compare complete primary-key sets
and values, not only row counts.

## Explicitly deferred

- Cross-shard transactions and globally atomic secondary constraints.
- Globally consistent cross-shard snapshots.
- Splitting one hot key.
- Arbitrary token-range placement.
- Concurrent splits before one-at-a-time recovery is proven.
- Transparent merge/scale-down in the first release.
- Replaying SQL as replication.

These are outside the lightweight scaling contract. The plan removes the permanent shard ceiling;
it does not turn SQLite into a fully general distributed SQL engine.
