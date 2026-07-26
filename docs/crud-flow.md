# How a CRUD statement flows through shardlite

The request-path and dynamic-scaling diagrams below describe the current code. New directories can
use the catalog-backed `init`/`join` path; legacy directories retain fixed modulo routing.

Key entry points:

- routing decision — [src/query/route.rs](../src/query/route.rs)
- hash → shard — [src/shard/routing.rs](../src/shard/routing.rs)
- the "just run SQL" path — [src/shard/mod.rs](../src/shard/mod.rs)
- ownership / forwarding — [src/net/forward.rs](../src/net/forward.rs), [src/cluster/placement.rs](../src/cluster/placement.rs)
- commit + replication — [src/shard/writer_fleet.rs](../src/shard/writer_fleet.rs), [src/replication/](../src/replication/)
- durable cluster catalog — [src/cluster/catalog.rs](../src/cluster/catalog.rs)
- catalog quorum and election safety — [src/cluster/catalog_quorum.rs](../src/cluster/catalog_quorum.rs), [src/cluster/durability.rs](../src/cluster/durability.rs)
- stable rebalance planning and fenced handoff — [src/cluster/rebalance.rs](../src/cluster/rebalance.rs), [src/cluster/promotion.rs](../src/cluster/promotion.rs)
- transfer and online split reconcilers — [src/cluster/transfer.rs](../src/cluster/transfer.rs), [src/cluster/split.rs](../src/cluster/split.rs)

---

## 1. The whole picture

```mermaid
flowchart TB
    subgraph CLIENTS["Clients"]
        CLI["shardlite CLI / shell"]
        DRV["Drivers: py / js / go / rust"]
    end

    subgraph EDGE["Any node — edges are optional build features"]
        HTTP["HTTP gateway /v1/run, /v1/query, /v1/execute"]
        JTCP["JSON-TCP gateway"]
        BIN["Native TCP protocol — Request/Response"]
    end

    subgraph NODE["Node that received the request — the coordinator for this statement"]
        AUTH["Auth + session — BEGIN/COMMIT buffered here"]
        CLASS["Classify: DDL? read-only? unsupported?"]
        ROUTE["Router: parse SQL, find shard key, hash it"]
        PLAN["Cross-shard planner — reads only"]
        OWN{"Do I own the target shard?"}
        FWD["Forward to owner, carrying routing epoch"]
        WF["Writer fleet — one writer per shard"]
        RF["Reader fleet — shared pool"]
        MERGE["Merge partials: concat / k-way / aggregate / group"]
    end

    subgraph STORE["Local shard files"]
        S0[("shard_0.db + WAL")]
        S1[("shard_1.db + WAL")]
        SN[("shard_N.db + WAL")]
    end

    subgraph REPL["Replication"]
        CAP["WAL capture VFS"]
        SINK["FrameSink — in-memory FrameLog and/or S3"]
        ACK["AckTracker — optional quorum wait"]
        FOL["Followers pull frames"]
    end

    CLI --> BIN
    DRV --> HTTP
    DRV --> JTCP
    HTTP --> AUTH
    JTCP --> AUTH
    BIN --> AUTH
    AUTH --> CLASS --> ROUTE --> OWN
    OWN -- "no" --> FWD
    OWN -- "yes, write" --> WF
    OWN -- "yes, read" --> RF
    ROUTE -- "unpinned read" --> PLAN --> RF
    RF --> MERGE
    WF --> S0 & S1 & SN
    RF --> S0 & S1 & SN
    WF --> CAP --> SINK --> FOL
    SINK --> ACK
    ACK -.-> WF
```

The rule that ties it together: **a client never names a shard**. It connects to *any*
node and runs plain SQL; the server does the hashing, the ownership lookup, the forwarding
and the merging. That choice is documented in `net/forward.rs` — pushing the placement map
into clients means a client with a stale map writes to the wrong node.

---

## 2. Sharding — how a row finds its file and its node

There are two independent mappings: **key → logical shard** and **logical shard → physical
copies**. Legacy directories use fixed modulo routing. Dynamic directories read `LinearV1` and its
epoch from the committed catalog; the same routing object drives INSERT, point CRUD, and fan-out
enumeration. Either the legacy placement calculator or the durable catalog controls the second.

```mermaid
flowchart LR
    K["Shard-key value<br/>e.g. users.id = 42"] --> B["Key bytes<br/>TEXT → UTF-8<br/>INTEGER → little-endian"]
    B --> H["FNV-1a 64-bit<br/>hash_key — frozen forever"]
    H --> R{"Routing scheme"}
    R -- "ModuloV1 — legacy directory" --> M["hash % fixed shard_count"]
    R -- "LinearV1 — dynamic directory" --> L["base = 2^level<br/>active = base + split_pointer"]
    L --> LH["first = hash % base<br/>if first < split_pointer:<br/>hash % 2×base"]
    M --> SID["logical ShardId"]
    LH --> SID
    SID --> PM{"Placement source"}
    PM -- "legacy static mode" --> LEG["derive round-robin from<br/>--peers + live membership"]
    PM -- "catalog-backed mode" --> CAT["committed ReplicaSet<br/>generation, primary, replicas"]
    LEG --> DB["owner opens shard_N.db + WAL"]
    CAT --> DB
```

Facts that matter when reading the diagram:

- The hash is a hand-written FNV-1a, *not* `DefaultHasher`, because a hash change would
  silently re-route every key — indistinguishable from data loss.
- `ModuloV1` is the shipped CLI behavior: `shard_count` is fixed at creation, recorded in
  the manifest, and a mismatch is refused.
- `LinearV1` stores `(level, split_pointer)` and grows by one logical shard at a time. A
  growth step splits only `split_pointer` into that shard and `base + split_pointer`; keys
  in other source shards retain their logical shard. SQL forwarding carries the routing epoch,
  so a stale coordinator is refused across cutover rather than writing to the old route.
- The **shard key** is either declared explicitly, or adopted automatically from a
  single-column `PRIMARY KEY` when the `CREATE TABLE` is applied — and every node adopts it
  from the same DDL text, so all nodes compute the same route.
- Shard-key updates are refused even when a caller explicitly names a physical shard; moving a row
  between files cannot be made atomic by bypassing the router.
- Legacy placement is derived from static peers and liveness. Catalog-backed placement is
  durable replicated state: each shard has an ownership `generation`, one `primary`, and
  zero or more `replicas`. `ClusterNode` can read this committed map instead of recomputing
  assignments. `shardlite init` and `shardlite join` select this dynamic path; opening an old
  manifest continues to use legacy behavior.
- Heartbeats distribute the current view; they do not decide a catalog change in the CRUD
  write path.
- Within a node, shard → writer thread is `shard_id % writer_threads`, so every shard still
  has exactly one writer while thread count stays bounded.

---

## 3. The routing decision — the same one for C, U and D

```mermaid
flowchart TD
    SQL["One client statement"] --> UNS{"Transaction control,<br/>ATTACH, VACUUM,<br/>foreign ALTER form?"}
    UNS -- "yes" --> REF["Refuse with the real reason"]
    UNS -- "no" --> DDL{"CREATE / DROP / ALTER?"}
    DDL -- "yes" --> ALL["Apply to EVERY shard<br/>no cross-shard atomicity —<br/>per-shard outcomes returned"]
    DDL -- "no" --> KEY{"Table has a shard key?"}
    KEY -- "no" --> PASS["Passthrough:<br/>write → shard 0 deterministically<br/>read → fan out"]
    KEY -- "yes" --> KIND{"Statement kind"}

    KIND -- "INSERT … VALUES" --> INS{"Key column listed<br/>and literal in every row?"}
    INS -- "no" --> REF2["Refuse — never guess a shard"]
    INS -- "all rows same shard" --> ONE["Route::One — send unchanged"]
    INS -- "rows span shards" --> SPLIT["Route::Split — one rewritten INSERT per shard"]

    KIND -- "UPDATE / DELETE" --> PIN{"WHERE pins key = literal<br/>top-level = or AND chain?"}
    PIN -- "yes" --> ONE
    PIN -- "no — or OR / join / UPDATE…FROM" --> ALLW["Route::All — touch every shard<br/>so no row is missed"]

    KIND -- "SELECT" --> PINR{"WHERE pins the key?"}
    PINR -- "yes" --> ONE
    PINR -- "no" --> FAN["Fan out + planner merge"]
```

`OR` is deliberately not descended: it can match rows on more than one shard. An `INSERT`
whose shard key is missing or non-literal is **refused** rather than sent somewhere
plausible — otherwise every keyless write lands on shard 0 and the cluster behaves like a
single database.

---

## 4. CREATE / INSERT — the write path end to end

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as Node A "coordinator"
    participant B as Node B "owns shard 7"
    participant W as Writer thread for shard 7
    participant F as shard_7.db + WAL
    participant K as WAL capture → FrameSink
    participant R as Follower replica

    C->>A: INSERT INTO users(id,name) VALUES (42,'ada')
    A->>A: reject_unsupported → classify → route_statement
    A->>A: committed Routing maps 42 LE to shard 7
    A->>A: placement says node B owns shard 7
    A->>B: Request::Direct(Execute{shard 7}) — Direct so it cannot bounce again
    B->>W: queue on thread 7 % writer_threads
    W->>W: WriteGate.check_may_write(shard 7) — fence, per shard
    W->>W: absorb everything queued behind → group commit
    W->>F: BEGIN … statements … COMMIT + fsync
    F-->>K: committed WAL frames captured
    W->>K: drain_capture BEFORE replying, stamp (epoch, lsn)
    K->>R: frames available for pull
    R-->>K: next Subscribe(from_lsn) IS the acknowledgement
    opt AckTracker configured
        W->>W: AckTracker.wait_for_quorum(shard 7, lsn)
    end
    W-->>B: rows_affected, last_insert_rowid
    B-->>A: Response::Changed
    A-->>C: ok
```

Four ordering decisions in that sequence are load-bearing:

1. **Gate before the transaction, not after.** A deposed leader must not commit at all.
2. **Drain to the sink before replying.** A caller told "ok" must never hold a write the
   sink never received.
3. **When quorum acknowledgement is configured, wait before replying.** Otherwise a leader
   that dies in that window has acknowledged a write no future leader holds. A timeout is
   reported as exactly what it is — *committed locally, replication unconfirmed* — never
   as a plain failure, because a client that retried would double-apply. Without an
   `AckTracker`, the durability boundary is the local commit plus sink drain.
4. **One wait per batch, not per write.** Group commit amortises the round trip, so the
   per-write quorum cost *falls* as load rises.

`CREATE TABLE` differs only in fan-out: it goes to every shard, and each node that applies
it adopts the table's single-column primary key as that table's shard key.

---

## 5. READ — point read vs. cross-shard fan-out

```mermaid
flowchart TB
    Q["SELECT …"] --> PIN{"Shard key pinned?"}
    PIN -- "yes" --> SINGLE["One shard"]
    SINGLE --> CONS{"ReadConsistency"}
    CONS -- "Linearizable (default)" --> LEAD["Only the shard's leader may answer"]
    CONS -- "Stale" --> ANY["Any copy that has applied something"]
    CONS -- "AtLeastLsn(n)" --> AT["A copy whose applied_lsn ≥ n<br/>— read your own writes"]
    LEAD --> RD["Reader fleet → stream rows"]
    ANY --> RD
    AT --> RD
    CONS -- "cannot honour, nowhere to forward" --> STALE["Response::TooStale — never answer<br/>from a copy that breaks the level"]

    PIN -- "no" --> AGREE{"Do all shards agree on schema?"}
    AGREE -- "no" --> RJ["Refuse — rows merged across two schemas<br/>are wrong, not merely slow"]
    AGREE -- "yes" --> PLAN["plan_with(sql, shard_keys)"]
    PLAN --> P1["SingleShard"]
    PLAN --> P2["Concat + limit"]
    PLAN --> P3["Merge — k-way on ORDER BY keys"]
    PLAN --> P4["Aggregate — combine one column"]
    PLAN --> P5["Grouped — rewritten partial aggregation, re-aggregate"]
    PLAN --> P6["PostProcess — DISTINCT / OFFSET"]
    PLAN --> P7["SetOp — each branch its own fan-out"]
    PLAN --> P8["Subqueries — evaluate globally, substitute, re-plan"]
    PLAN --> P9["Central — materialise sources on coordinator (heavy)"]
    P1 & P2 & P3 & P4 & P5 & P6 & P7 & P8 & P9 --> FANOUT["fan_out_shards"]
    FANOUT --> GRP["Group remote shards by owner node<br/>→ one ShardBatch request per node"]
    GRP --> MERGE["Merge partials in shard order"] --> ROWS["Streamed result — never materialised end to end"]
```

Note what a fan-out costs: **one request per node**, not one per shard. And a cross-shard
read is explicitly *not* a consistent snapshot — each shard's partial is its own latest
committed state.

---

## 6. Replication — what a follower does with the frames

```mermaid
flowchart LR
    subgraph LEADER["Leader of shard N"]
        WR["Writer commits batch"] --> CAP["WAL capture VFS"]
        CAP --> POS["Stamp position: (epoch, lsn)<br/>lsn dense within an epoch"]
        POS --> LOG["FrameLog — bounded total retention,<br/>evictions counted"]
        POS --> S3["S3Sink — async change-log upload<br/>+ on-demand snapshots"]
    end

    subgraph FOLLOWER["Follower of shard N"]
        SUB["Subscribe: node, shard, epoch, from_lsn"]
        APPLY["Write raw pages — never executes SQL"]
        FSY["fsync pages, THEN persist position<br/>temp → fsync → rename → parent fsync"]
        BOOT["Bootstrap: snapshot copy"]
    end

    LOG -- "frames from from_lsn" --> APPLY
    SUB --> LOG
    APPLY --> FSY --> SUB
    LOG -- "requested lsn already evicted" --> BOOT
    BOOT --> APPLY
    SUB -. "the request itself is the ack" .-> ACKT["AckTracker — majority per shard"]
```

```mermaid
stateDiagram-v2
    [*] --> Following
    Following --> Following: frames arrive contiguously → apply, fsync, advance
    Following --> Bootstrapping: gap in lsn, or epoch mismatch, or frames evicted
    Bootstrapping --> Following: snapshot installed, resume from its position
    Following --> Refusing: FenceToken term < highest seen → deposed leader ignored
    Refusing --> Following: new leader's term accepted
```

Why physical replication, and why this ordering:

- A follower **writes pages, never SQL** — non-deterministic functions and per-machine
  errors cannot make it diverge, because it evaluates nothing.
- **Pages first, position second.** A crash in between replays some transactions, which is
  harmless: writing the same page twice yields the same page. Position-first would silently
  skip transactions — undetectable corruption. The position itself is written to a temporary
  file, fsynced, renamed, and followed by a parent-directory fsync.
- **Epoch exists because density cannot survive an unclean restart.** LSN 900 in epoch 4 is
  not "ahead of" LSN 5 in epoch 5; they are not on the same ruler, so a cross-epoch
  comparison is `Incomparable` and a vote is refused rather than approximated.
- A follower's next `Subscribe(from_lsn)` *is* its acknowledgement — there is no separate
  ack message to lose or reorder.
- Bootstrap freezes the main file by **suspending checkpointing**, so the bytes being copied
  cannot change underneath the copy.

---

## 7. Safety rails around all of it

```mermaid
flowchart TB
    subgraph GATE["Three independent stale-writer checks"]
        T["FenceToken — 'should I accept this message?'<br/>checked by followers, receiving side"]
        G["WriteGate — 'may I write at all?'<br/>checked by this node's writers, before commit"]
        OG["Ownership generation — 'is this the placement I planned against?'<br/>checked at catalog cutover"]
    end
    subgraph MODE["Per-shard exclusivity"]
        LED["Led: ShardManager owns the file,<br/>SQLite has exclusive charge"]
        FOLW["Followed: replication owns the file,<br/>reads only via ShardAccess"]
        LED <--> FOLW
    end
    GATE --> MODE
    MODE --> INV["Never both, never neither —<br/>enforced where connections are opened"]
```

A node **leads some shards and follows others** — that is what multi-write means. So both
the gate and the shard mode are per shard; a node-wide flag would let a node that leads any
shard write every shard. On a followed shard, cached read connections carry a generation
that every apply bumps, so a stale page cache or a handle to a replaced inode is closed and
reopened rather than silently serving frozen rows.

---

## 8. Dynamic scaling control plane

This control plane is deliberately outside the request path. Dynamic CRUD reads the committed
catalog routing and placement, while slow durable reconcilers move or split one shard at a time.
The local manifest is an allocation ceiling (currently at most 256 files), not the number of active
logical shards; inactive files are created lazily.

### Catalog membership and agreement

```mermaid
flowchart LR
    JOIN["Unknown node requests join"] --> CHECK["Validate cluster id,<br/>protocol, format, hash,<br/>routing, page size"]
    CHECK --> LEARN["Learner<br/>cannot receive writes"]
    LEARN --> CATCH["Durably catch up catalog"]
    CATCH --> STORE["Storage member<br/>eligible for placement,<br/>not an election voter"]
    STORE --> JOINT["Joint old + new voter sets"]
    JOINT --> VOTER["Voter"]

    LEADER["Current catalog leader"] --> PROP["Propose exact next<br/>version + digest"]
    PROP --> PREP["Strict majority fsyncs<br/>the prepared value"]
    PREP --> COMMIT["Publish committed catalog"]
    COMMIT --> VIEW["Catalog consumers observe<br/>the new committed state"]
```

Catalog state contains:

- a stable cluster ID and compatibility contract;
- versioned `Routing`;
- members with `Learner`, `Storage`, or `Voter` roles and `Active`, `Cordoned`, or
  `Draining` states;
- per-shard replica sets with an ownership generation;
- operation records that can represent `Join`, `Transfer`, `Split`, `Drain`, and `Remove`.

A catalog value is chosen only after a strict majority has fsynced the exact proposal. During a
voter change, both the old and new sets must independently have a majority. Prepared values survive
a timeout and can be retried or recovered by a new leader. Election durability includes the latest
catalog version and digest, so a candidate behind the catalog—or claiming a different value at the
same version—cannot win and forget a chosen topology.

### One fenced primary transfer

```mermaid
stateDiagram-v2
    [*] --> Planned
    Planned --> Snapshotting
    Snapshotting --> CatchingUp: install snapshot at exact epoch / LSN
    CatchingUp --> Prepared: destination reports durable progress
    Prepared --> Fencing
    Fencing --> Fencing: source gate closed; queue drained; final LSN recorded
    Fencing --> Committed: destination durable through final LSN; generation + 1
    Committed --> Cleaning
    Cleaning --> Complete
    Planned --> Aborted
    Snapshotting --> Aborted
    CatchingUp --> Aborted
    Prepared --> Aborted
```

The cutover boundary is precise:

1. Bootstrap the destination with a snapshot and continue WAL catch-up in the same stream epoch.
2. Close only the source shard's write gate and drain its writer queue.
3. Record the source's exact final `(epoch, LSN)`.
4. Require the destination to report that LSN **durable**, not merely received.
5. Atomically change the committed primary and increment its ownership generation. The old
   primary remains a replica through this boundary.
6. Clean up and complete. Before commit the operation may abort; after commit recovery must
   roll forward.

The rebalance planner preserves existing assignments, proposes at most one transfer at a time,
evacuates cordoned or draining nodes first, and prefers a destination that is already a replica.
If there are fewer active shards than eligible storage nodes, it returns `NeedsSplit`: moving an
existing shard would only change which node is idle, not add a write lane. The leader starts a split,
then resumes whole-shard balancing after it completes. A late storage node receives the latest
catalog snapshot before its transfer worker acts.

### One online logical split

```mermaid
stateDiagram-v2
    [*] --> Planned
    Planned --> Snapshotting: install transactional dirty-key capture
    Snapshotting --> CatchingUp: snapshot source
    CatchingUp --> Prepared: partition two shadows + replay
    Prepared --> Fencing
    Fencing --> Installing: close source gate + final replay
    Installing --> Committed: install both files + commit routing epoch
    Committed --> Cleaning
    Cleaning --> Complete
```

The source stays query-visible while two hidden copies are partitioned. Per-table triggers write
typed dirty primary keys in the same user transaction. Replay reads the resulting row from the
source instead of re-running SQL, so `random()`, time functions, and user-trigger effects are
preserved. User triggers are disabled while shadows are reconstructed and restored before install.
Only the source shard is fenced for final convergence; unrelated shards remain writable. The two
images are installed before the new routing epoch is quorum-committed, and staging markers make an
interrupted install/cleanup roll forward.

The current split preflight intentionally accepts only tables whose declared shard key is their
single-column `PRIMARY KEY`, with text or integer values. It refuses virtual/keyless/composite-key
tables and refuses a source placement with replicas until every replica can acknowledge both shadow
installs. Phase-by-phase process-exit/restart qualification is implemented and verifies automatic
roll-forward with complete row comparisons. Capture-log bounds/backpressure, partitions, disk
faults, and continuous concurrent workload qualification remain production-hardening work; see
[crash-recovery.md](crash-recovery.md) and
[dynamic-scaling-plan.md](dynamic-scaling-plan.md).

### Operator surfaces

- `shardlite init` creates a small linear-routing cluster; `shardlite join --seed ...` durably joins
  a learner and completes it as non-voting storage.
- The leader automatically performs one split/transfer at a time after capacity joins.
- Quorum-backed HTTP mutations cover rebalance, member cordon/drain/removal, and begin/finalize
  voter changes.
- The console proxies those mutations, shows active operations, routing epoch, logical shard count,
  local allocation capacity, and joint voter state.

---

## What this design does not do

Straight from the README and the module docs, so the diagrams aren't read as promising more
than they deliver:

- **No cross-shard transactions.** A transaction is scoped to one shard.
- **No consistent cross-shard snapshot.** A fan-out read gathers per-shard latest states.
- **DDL is not atomic across shards.** Per-shard outcomes are returned rather than collapsed
  precisely because a partial failure leaves shards disagreeing.
- **A hot single row is a hot single shard.** Sharding does not help a workload concentrated
  on one key.
- **Legacy shard count stays immutable.** Existing modulo directories cannot be converted in place.
- **Dynamic growth currently stops at the local 256-file allocation ceiling.** Logical activation
  starts small, but raising that implementation ceiling still requires a format/runtime change.
- **HA logical split is not enabled yet.** Whole-shard moves retain replica-count semantics, but a
  split source with replicas is refused until per-replica shadow install acknowledgement exists.
- **A split has schema limits.** Every table must use a declared single text/integer primary shard
  key; virtual, keyless, and composite-key tables block it.
