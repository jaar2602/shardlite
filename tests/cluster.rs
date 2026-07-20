//! Election, fencing and failover across three real nodes over TCP.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use meshdb::cluster::{
    ClusterNode, Durability, DurabilitySource, Election, ElectionConfig, Fence, FenceToken, NodeId,
    TermStore,
};
use meshdb::net::{Server, ServerConfig};
use meshdb::replication::{FrameLog, FrameLogConfig};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use tempfile::TempDir;

/// Fast enough that a test finishes quickly, still far enough apart that a healthy leader is
/// not timed out by scheduling noise.
fn quick(node: NodeId, peers: Vec<NodeId>) -> ElectionConfig {
    ElectionConfig {
        node,
        peers,
        election_timeout: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(100),
    }
}

struct Node {
    id: NodeId,
    cluster: Arc<ClusterNode>,
    server: Arc<Server>,
    manager: Arc<ShardManager>,
    /// Kept so a test can talk to this node directly over the wire.
    #[allow(dead_code)]
    addr: String,
    _dir: TempDir,
}

impl Node {
    fn stop(&self) {
        self.cluster.stop();
        self.server
            .shutdown_handle()
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A node part-built: its listener is bound so its address is known, but its cluster
/// membership cannot be attached until every peer's address exists.
struct Pending {
    id: NodeId,
    dir: TempDir,
    manager: Arc<ShardManager>,
    frames: Arc<FrameLog>,
    fence: Arc<Fence>,
    /// Held from the moment the port is known until the real server takes it over.
    listener: std::net::TcpListener,
    addr: String,
}

/// Bring up `n` nodes that all know each other, and start their election loops.
fn cluster(n: usize) -> Vec<Arc<Node>> {
    // Bind every listener first so each node can be told the others' addresses.
    let mut built: Vec<Pending> = Vec::new();

    for i in 0..n {
        let id = (i + 1) as NodeId;
        let dir = TempDir::new().unwrap();
        let frames = Arc::new(FrameLog::new(FrameLogConfig {
            total_bytes: 4 * 1024 * 1024,
            shard_count: 2,
        }));
        // The fence is built first so it can be handed to the manager as the write gate.
        // Without that the gate is a mechanism nothing consults, and a deposed leader keeps
        // writing.
        let fence = Arc::new(Fence::new((i + 1) as NodeId, 0));
        let manager = Arc::new(
            ShardManager::open_clustered(
                dir.path(),
                ShardConfig {
                    shard_count: 2,
                    capture: true,
                    ..ShardConfig::floor()
                },
                Some(frames.clone()),
                None,
                Some(Arc::clone(&fence) as Arc<dyn meshdb::shard::WriteGate>),
            )
            .unwrap(),
        );
        // Bind once and *hold* the listener. Setting up cluster membership is circular — a
        // node needs its peers' addresses, which only exist once something is bound — and the
        // obvious way round it, bind/read-port/drop/rebind, leaves a window where another
        // process takes the port. That is an `Address already in use` failure that appeared
        // once in twenty-four concurrent suite runs.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        built.push(Pending {
            id,
            dir,
            manager,
            frames,
            fence,
            listener,
            addr,
        });
    }

    let addrs: BTreeMap<NodeId, String> = built.iter().map(|p| (p.id, p.addr.clone())).collect();

    // Rebind each server with its cluster node attached. The first bind was only to learn the
    // port; dropping it frees the port for the real listener.
    let mut nodes = Vec::new();
    for Pending {
        id,
        dir,
        manager,
        frames,
        fence,
        listener,
        addr,
    } in built
    {
        let peers: BTreeMap<NodeId, String> = addrs
            .iter()
            .filter(|(p, _)| **p != id)
            .map(|(&p, a)| (p, a.clone()))
            .collect();

        let terms = TermStore::open(&dir.path().join("cluster")).unwrap();
        let election = Election::new(
            quick(id, peers.keys().copied().collect()),
            terms,
            Instant::now(),
        )
        .unwrap();
        let cluster = Arc::new(
            ClusterNode::new(
                id,
                election,
                Arc::clone(&fence),
                peers,
                Arc::clone(&manager) as Arc<dyn DurabilitySource>,
                (0..2).map(ShardId).collect(),
            )
            // These nodes hold no replicated copies, so marking ownership is all that is
            // needed — there is no file to hand over, only a claim to drop. Without this the
            // fence and the mode disagree, and a node with no copy answers reads for shards
            // it does not own.
            .with_modes(Arc::clone(manager.modes())),
        );

        let router = Arc::new(meshdb::net::Router::new(Arc::clone(&cluster)));
        let server = Arc::new(
            Server::from_listener(
                listener,
                Arc::clone(&manager),
                meshdb::net::NodeServices {
                    frames: Some(frames),
                    cluster: Some(Arc::clone(&cluster)),
                    router: Some(router),
                    ..Default::default()
                },
                ServerConfig {
                    addr: addr.clone(),
                    ..ServerConfig::default()
                },
            )
            .unwrap(),
        );

        let s = Arc::clone(&server);
        std::thread::spawn(move || {
            let _ = s.serve();
        });
        nodes.push(Arc::new(Node {
            id,
            cluster,
            server,
            manager,
            addr,
            _dir: dir,
        }));
    }

    // Only start ticking once every listener is up, or the first campaign races the binds.
    std::thread::sleep(Duration::from_millis(80));
    for n in &nodes {
        let c = Arc::clone(&n.cluster);
        std::thread::spawn(move || c.run(Duration::from_millis(40)));
    }
    nodes
}

/// Wait until exactly one node among `live` claims leadership, or time out.
fn await_leader(live: &[Arc<Node>], within: Duration) -> Option<Arc<Node>> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let leaders: Vec<&Arc<Node>> = live.iter().filter(|n| n.cluster.is_leader()).collect();
        if leaders.len() == 1 {
            return Some(Arc::clone(leaders[0]));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// Wait until placement has spread shards over more than one node.
fn await_spread(live: &[Arc<Node>], within: Duration) -> Option<meshdb::cluster::Placement> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        for n in live {
            let p = n.cluster.placement();
            if p.leaders().len() > 1 {
                return Some(p);
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    // The fifth test in this file found asserting pre-placement behaviour. It checked the
    // gate for ShardId(0) the instant `is_leader()` flipped — racing the placement that
    // opens gates — and asserted followers never hold gates, which stops being true the
    // moment placement spreads shards to them. Both assumptions produced intermittent
    // failures under the stress harness, not honest ones.
    let nodes = cluster(3);
    let leader = await_leader(&nodes, Duration::from_secs(5)).expect("no leader was elected");

    // The others must agree, and must not think they lead the *election*.
    for f in nodes.iter().filter(|n| n.id != leader.id) {
        assert!(!f.cluster.is_leader(), "node {} also thinks it leads", f.id);
    }

    // Winning the election and being assigned shards are separate steps; wait for the
    // second rather than racing it.
    let deadline = Instant::now() + Duration::from_secs(5);
    while leader.cluster.led_shards().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let led = leader.cluster.led_shards();
    assert!(!led.is_empty(), "the leader was never assigned a shard");
    assert!(
        leader.cluster.fence().is_open(led[0]),
        "the leader's gate must be open for a shard it leads"
    );

    // Which shards followers may write is placement's business, covered by
    // `a_node_writes_only_the_shards_placement_gave_it` — not asserted here, where the
    // spread may or may not have happened yet.

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn killing_the_leader_elects_a_new_one_within_the_budget() {
    // Step 10's headline requirement: a new primary inside 5 seconds.
    let nodes = cluster(3);
    let first = await_leader(&nodes, Duration::from_secs(5)).expect("no initial leader");
    let first_term = first.cluster.term();

    first.stop();
    let survivors: Vec<Arc<Node>> = nodes
        .iter()
        .filter(|n| n.id != first.id)
        .map(Arc::clone)
        .collect();

    let started = Instant::now();
    let second = await_leader(&survivors, Duration::from_secs(5))
        .expect("no new leader after the old one was killed");
    let took = started.elapsed();

    assert_ne!(second.id, first.id);
    assert!(
        took < Duration::from_secs(5),
        "failover took {took:?}, over the 5s budget"
    );
    assert!(
        second.cluster.term() > first_term,
        "a new leader must hold a later term: {} vs {first_term}",
        second.cluster.term()
    );
    assert!(second.cluster.fence().is_open(ShardId(0)));
    println!(
        "failover in {took:?}, term {first_term} -> {}",
        second.cluster.term()
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_deposed_leader_is_fenced_and_stops_writing() {
    // Both halves of fencing, on real nodes: the deposed leader's own gate closes, and its
    // token no longer clears the bar its successor raised.
    // Placement-aware, and it has to be: the coordinator leads a *subset* of shards, and
    // which subset depends on who won the election. Asserting on shard 0 specifically was
    // right before placement and became an intermittent failure after it — the fourth test
    // in this file to make that assumption.
    let nodes = cluster(3);
    let first = await_leader(&nodes, Duration::from_secs(5)).expect("no initial leader");

    // Winning an election and being assigned shards are separate steps, so wait for the
    // second rather than racing it.
    let deadline = Instant::now() + Duration::from_secs(5);
    while first.cluster.led_shards().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let led = first.cluster.led_shards();
    assert!(
        !led.is_empty(),
        "the leader should have been assigned at least one shard"
    );
    let old_token = FenceToken::new(first.cluster.term(), first.id);
    assert!(
        first.cluster.fence().is_open(led[0]),
        "the leader's gate must be open for a shard it actually leads"
    );

    first.stop();
    let survivors: Vec<Arc<Node>> = nodes
        .iter()
        .filter(|n| n.id != first.id)
        .map(Arc::clone)
        .collect();
    let second = await_leader(&survivors, Duration::from_secs(5)).expect("no new leader");

    // Winning the election and raising the fence are two steps, and `is_leader` flips at the
    // first. The window between them is inherent — a bar cannot rise before the election that
    // justifies it concludes — so wait for the state this test is actually about rather than
    // racing it.
    let deadline = Instant::now() + Duration::from_secs(5);
    while second.cluster.fence().highest_seen() < second.cluster.term() && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        second.cluster.fence().highest_seen() >= second.cluster.term(),
        "the successor never raised its fence to its own term"
    );

    // The successor, and every node that heard from it, now refuses the old token.
    let err = second
        .cluster
        .fence()
        .validate(old_token)
        .expect_err("the deposed leader's token must be refused");
    assert!(err.to_string().contains("fenced"), "{err}");
    assert!(
        second.cluster.fence().stats().rejected > 0,
        "the rejection must be counted, not silently absorbed"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_node_that_cannot_reach_a_quorum_does_not_lead() {
    // A single node of a three-node cluster is a minority. It must never conclude it leads,
    // or a partition produces two writers.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5));

    // Cut the other two off.
    let alone = Arc::clone(&nodes[0]);
    for n in nodes.iter().skip(1) {
        n.stop();
    }

    // Give it well past several election timeouts to convince itself.
    std::thread::sleep(Duration::from_secs(3));

    assert!(
        !alone.cluster.is_leader(),
        "a minority of one must not lead a three-node cluster"
    );
    assert!(
        !alone.cluster.fence().is_open(ShardId(0)),
        "and its write gate must be shut"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_leader_keeps_leading_while_the_cluster_is_healthy() {
    // Leadership must be stable. A cluster that re-elects every few seconds does no useful
    // work, and the usual cause — a heartbeat too close to the timeout — is silent otherwise.
    let nodes = cluster(3);
    let leader = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let term = leader.cluster.term();

    std::thread::sleep(Duration::from_secs(3));

    assert!(
        leader.cluster.is_leader(),
        "the leader must not have flapped"
    );
    assert_eq!(
        leader.cluster.term(),
        term,
        "a healthy cluster must not change terms; it re-elected {} times",
        leader.cluster.stats().elections_started
    );
    assert_eq!(
        leader.cluster.stats().stepped_down,
        0,
        "a healthy leader must never step down"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_candidate_that_is_behind_cannot_win() {
    // The election restriction over real durability sources, not hand-built structs. A node
    // holding fewer writes must not be able to take leadership and lose them.
    //
    // Placement-aware by necessity: no node leads every shard any more, so the writes have
    // to go to a shard's actual owner. Writing through the coordinator and hoping would race
    // the first placement round.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    let shard = ShardId(0);
    let owner_id = placement.owner(shard).expect("shard 0 has no owner");
    let owner = nodes.iter().find(|n| n.id == owner_id).expect("owner node");
    let other = nodes
        .iter()
        .find(|n| n.id != owner_id)
        .expect("another node");

    // DDL goes to the shard's owner, on that shard only.
    owner
        .manager
        .execute_one(
            shard,
            meshdb::storage::exec::Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"),
        )
        .unwrap();
    for i in 1..=20 {
        owner
            .manager
            .execute_one(
                shard,
                meshdb::storage::exec::Statement::new(format!("INSERT INTO t VALUES ({i})")),
            )
            .unwrap();
    }

    let ahead = owner.manager.durability();
    let behind = other.manager.durability();

    assert!(
        ahead.lsn(shard) > behind.lsn(shard),
        "the owner should be ahead on {shard}: {ahead:?} vs {behind:?}"
    );
    assert!(
        !behind.may_lead(&ahead),
        "a node that is behind must not be electable: {behind:?} cannot lead over {ahead:?}"
    );
    assert!(ahead.may_lead(&behind), "and the owner must still qualify");

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_standalone_node_says_it_is_not_a_cluster_member() {
    // Answering a vote request with silence would look exactly like an unreachable peer, and
    // a node misconfigured out of its cluster would present as a network fault forever.
    use meshdb::net::protocol::Request;

    let dir = TempDir::new().unwrap();
    let manager = Arc::new(
        ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 1,
                ..ShardConfig::floor()
            },
        )
        .unwrap(),
    );
    let server = Arc::new(
        Server::bind(
            manager,
            ServerConfig {
                addr: "127.0.0.1:0".into(),
                ..ServerConfig::default()
            },
        )
        .unwrap(),
    );
    let addr = server.local_addr().unwrap().to_string();
    let s = Arc::clone(&server);
    std::thread::spawn(move || {
        let _ = s.serve();
    });
    std::thread::sleep(Duration::from_millis(50));

    let mut c = meshdb::net::Client::connect(&addr).unwrap();
    let req = meshdb::cluster::VoteRequest {
        term: 1,
        candidate: 9,
        durability: Durability::new(0),
    };
    // The client surfaces a server-side `Response::Error` as an `Err`, so the refusal arrives
    // as a failed request carrying the explanation.
    let err = c
        .request(Request::Vote(req))
        .expect_err("a standalone node must refuse, not answer");
    assert!(err.to_string().contains("not a cluster member"), "{err}");
}

#[test]
fn a_hung_peer_cannot_freeze_the_election_loop() {
    // The bug this guards against is subtle and severe. A peer that is *hung* rather than
    // crashed accepts the TCP connection and then never answers. With a client-sized read
    // timeout the leader blocks in a socket read for far longer than its own election
    // timeout, so it never evaluates its lease, never steps down, and keeps its write gate
    // open the entire time — the exact split-brain the lease exists to prevent.
    //
    // Every peer round trip must therefore fail well inside the election timeout.
    use std::net::TcpListener;

    // A listener that accepts and then does nothing at all.
    let black_hole = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = black_hole.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in black_hole.incoming() {
            // Hold the connection open, read nothing, write nothing.
            held.push(stream);
        }
    });

    let election_timeout = Duration::from_millis(400);
    let bound = election_timeout / 3;

    let started = Instant::now();
    let result = meshdb::net::Client::connect_bounded(&addr, bound, bound);
    let took = started.elapsed();

    assert!(
        result.is_err(),
        "a peer that never answers must not look like a healthy one"
    );
    assert!(
        took < election_timeout,
        "a hung peer blocked the caller for {took:?}, past the {election_timeout:?} election \
         timeout; a leader would still be inside this read when it should be stepping down"
    );
    println!("hung peer gave up after {took:?}");
}

#[test]
fn a_deposed_leader_stops_writing() {
    // The self-fencing half. A deposed leader that keeps committing locally diverges its own
    // copy even if every follower refuses its frames — so the gate has to shut the writer
    // down, not merely stop anyone listening.
    //
    // Placement-aware: the coordinator owns only some shards, so the test works through one
    // it actually leads rather than assuming shard 0.
    let nodes = cluster(3);
    let leader = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let _ = await_spread(&nodes, Duration::from_secs(5));
    std::thread::sleep(Duration::from_millis(400));

    let owned = leader.cluster.led_shards();
    let Some(&shard) = owned.first() else {
        // A coordinator leading nothing has nothing to fence; not the case under test.
        for n in &nodes {
            n.stop();
        }
        return;
    };

    leader
        .manager
        .execute_one(
            shard,
            meshdb::storage::exec::Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"),
        )
        .unwrap();
    assert!(leader.cluster.fence().is_open(shard));

    // Cut it off from the cluster; its lease expires and it must fence itself.
    for n in nodes.iter().filter(|n| n.id != leader.id) {
        n.stop();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while leader.cluster.fence().is_open(shard) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !leader.cluster.fence().is_open(shard),
        "a leader that lost its quorum must shut its own write gate"
    );

    let err = leader
        .manager
        .execute_one(
            shard,
            meshdb::storage::exec::Statement::new("INSERT INTO t VALUES (99)"),
        )
        .expect_err("a deposed leader must refuse to write, even locally");
    assert!(err.to_string().contains("not the leader"), "{err}");

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn shards_are_led_by_different_nodes() {
    // The headline goal. Until now one leader owned every shard, so writes still serialised
    // onto a single node however many nodes were running.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5))
        .expect("placement never spread shards across nodes");

    assert!(
        placement.leaders().len() > 1,
        "shards must be led by more than one node: {placement:?}"
    );
    for s in 0..2u32 {
        assert!(
            placement.owner(ShardId(s)).is_some(),
            "shard {s} has no leader"
        );
    }
    println!("{placement:?}");

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_node_writes_only_the_shards_placement_gave_it() {
    // Two nodes both writing the same shard is the corruption everything else guards
    // against, so the gate has to follow the map exactly.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");

    // Let every node settle on the same map.
    std::thread::sleep(Duration::from_millis(400));

    for n in &nodes {
        let mine = placement.shards_for(n.id);
        for s in 0..2u32 {
            let shard = ShardId(s);
            let should_lead = mine.contains(&shard);
            assert_eq!(
                n.cluster.fence().is_open(shard),
                should_lead,
                "node {} gate for {shard} is wrong: placement says owner is {:?}",
                n.id,
                placement.owner(shard)
            );
        }
    }

    // And exactly one node accepts a write to any given shard.
    for s in 0..2u32 {
        let shard = ShardId(s);
        let accepted = nodes
            .iter()
            .filter(|n| {
                n.manager
                    .execute_one(
                        shard,
                        meshdb::storage::exec::Statement::new(
                            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)",
                        ),
                    )
                    .is_ok()
            })
            .count();
        assert_eq!(
            accepted, 1,
            "exactly one node must accept a write to {shard}, {accepted} did"
        );
    }

    // Refused either by the write gate or by the ownership mark, depending on which guard
    // saw it first. Both are real refusals and both are counted; what matters is that the
    // refusal is visible rather than silently absorbed.
    let refused: u64 = nodes
        .iter()
        .map(|n| n.cluster.fence().stats().gated_writes + n.manager.modes().refused_opens())
        .sum();
    assert!(
        refused > 0,
        "refusing a write to an unowned shard must be counted, not silently absorbed"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn losing_a_node_reassigns_its_shards() {
    // A shard whose owner is gone must be given to someone. Otherwise placement has traded a
    // single point of failure for several.
    let nodes = cluster(3);
    let leader = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let _ = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    // Kill a node that is not the coordinator, so the coordinator survives to react.
    let victim = nodes
        .iter()
        .find(|n| n.id != leader.id && !n.cluster.led_shards().is_empty())
        .cloned();
    let Some(victim) = victim else {
        for n in &nodes {
            n.stop();
        }
        return;
    };
    let orphaned = victim.cluster.led_shards();
    victim.stop();

    // The coordinator should notice and reassign.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut covered = false;
    while Instant::now() < deadline && !covered {
        let p = leader.cluster.placement();
        covered = orphaned
            .iter()
            .all(|s| p.owner(*s).is_some_and(|o| o != victim.id));
        if !covered {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    assert!(
        covered,
        "shards {orphaned:?} were left with a dead owner: {:?}",
        leader.cluster.placement()
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn ddl_reaches_every_shard_from_any_node() {
    // The piece that makes multi-write usable. No node holds every shard, so a schema change
    // has to reach each shard's owner. This replaces
    // `cross_shard_ddl_is_broken_by_placement_and_this_records_it`, which existed to prompt
    // exactly this.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        placement.leaders().len() > 1,
        "the point is that shards live on different nodes"
    );

    // Ask a node that does NOT own every shard — under placement, none does.
    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    let outcomes = c
        .execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    for (shard, o) in &outcomes {
        assert_eq!(
            *o,
            meshdb::net::ShardOutcome::Ok,
            "shard {shard} did not get the schema change: {o:?}"
        );
    }

    // Every shard really has the table, checked at its owner.
    for s in 0..2u32 {
        let shard = ShardId(s);
        let owner_id = placement.owner(shard).expect("owner");
        let owner = nodes.iter().find(|n| n.id == owner_id).unwrap();
        owner
            .manager
            .execute_one(
                shard,
                meshdb::storage::exec::Statement::new(format!("INSERT INTO t VALUES ({s})")),
            )
            .unwrap_or_else(|e| panic!("{shard} on node {owner_id} has no table: {e}"));
        assert_eq!(
            owner.manager.schema_version(shard).unwrap(),
            1,
            "{shard} should have advanced exactly one schema version"
        );
    }

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_write_is_forwarded_to_the_node_that_owns_the_shard() {
    // A client connects to any node and its write reaches the right one. Without this,
    // multi-write exists at the storage layer but no client can use it.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // Find a shard node 0 does not own, and write it through node 0 anyway.
    let elsewhere = (0..2u32)
        .map(ShardId)
        .find(|s| placement.owner(*s) != Some(nodes[0].id))
        .expect("some shard should live on another node");
    let owner_id = placement.owner(elsewhere).unwrap();
    assert_ne!(owner_id, nodes[0].id);

    c.execute(elsewhere.0, "INSERT INTO t VALUES (77)")
        .unwrap_or_else(|e| panic!("write to {elsewhere} via node {} failed: {e}", nodes[0].id));

    // It landed on the owner, not on the node that took the request.
    let owner = nodes.iter().find(|n| n.id == owner_id).unwrap();
    let n = owner
        .manager
        .query(
            elsewhere,
            meshdb::storage::exec::Statement::new("SELECT count(*) FROM t WHERE id = 77"),
        )
        .unwrap();
    match n {
        meshdb::storage::exec::Outcome::Ok(meshdb::storage::exec::Executed::Rows(r)) => {
            assert_eq!(r.rows[0][0], meshdb::storage::Value::Integer(1));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_forwarded_request_is_answered_or_refused_but_never_forwarded_again() {
    // Loop prevention, tested directly rather than by trying to engineer two disagreeing
    // maps. Two nodes whose placement differs for a moment would otherwise pass a request
    // back and forth forever — a hang, which is the worst way for this to fail.
    //
    // A `Direct` request means "handle this here or refuse it", so sending one to a node that
    // does not own the shard must come back as a refusal, promptly.
    use meshdb::net::protocol::Request;

    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    // A shard node 0 does not own.
    let elsewhere = (0..2u32)
        .map(ShardId)
        .find(|s| placement.owner(*s) != Some(nodes[0].id))
        .expect("some shard should live on another node");

    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    let started = Instant::now();
    let result = c.request(Request::Direct(Box::new(Request::Execute {
        shard: elsewhere.0,
        statements: vec![meshdb::storage::exec::Statement::new(
            "CREATE TABLE loop_guard (a INTEGER)",
        )],
    })));
    let took = started.elapsed();

    assert!(
        result.is_err(),
        "a directed request to a node that does not own the shard must be refused, got {result:?}"
    );
    assert!(
        took < Duration::from_secs(2),
        "it must refuse promptly rather than bouncing; took {took:?}"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("not the leader") || msg.contains("followed"),
        "the refusal should say why this node cannot serve the shard: {msg}"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn what_the_router_puts_on_the_wire_is_direct_wrapped() {
    // The other half of loop prevention, and the half a client-side test cannot reach:
    // `Router::forward` must wrap what it sends. Verified by watching the wire, because the
    // failure it prevents — two nodes bouncing a request forever — only shows up when their
    // placement maps disagree, which is precisely the state that is hard to arrange on
    // purpose and certain to happen eventually.
    use meshdb::net::protocol::{PROTOCOL_VERSION, Request, Response, read_message, write_message};
    use std::net::TcpListener;
    use std::sync::mpsc;

    // A stub peer that completes the handshake, records one request, and answers.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let stub_addr = listener.local_addr().unwrap().to_string();
    let (seen_tx, seen_rx) = mpsc::channel::<Request>();
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            let mut r = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut w = std::io::BufWriter::new(stream);
            let _hello: Request = read_message(&mut r).unwrap();
            write_message(
                &mut w,
                &Response::Welcome {
                    version: PROTOCOL_VERSION,
                    shard_count: 2,
                    epoch: Some(1),
                },
            )
            .unwrap();
            let req: Request = read_message(&mut r).unwrap();
            let _ = seen_tx.send(req);
            write_message(&mut w, &Response::Ok).unwrap();
        }
    });

    // A node that believes shard 0 belongs to the stub.
    let dir = TempDir::new().unwrap();
    let terms = TermStore::open(dir.path()).unwrap();
    let fence = Arc::new(Fence::new(1, 0));
    let election = Election::new(quick(1, vec![2]), terms, Instant::now()).unwrap();
    let manager = Arc::new(
        ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                ..ShardConfig::floor()
            },
        )
        .unwrap(),
    );
    let node = Arc::new(ClusterNode::new(
        1,
        election,
        fence,
        [(2u64, stub_addr)].into_iter().collect(),
        manager as Arc<dyn DurabilitySource>,
        vec![ShardId(0), ShardId(1)],
    ));

    // Tell it, as a heartbeat would, that node 2 owns shard 0.
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(ShardId(0), 2u64);
    assignments.insert(ShardId(1), 1u64);
    node.handle_heartbeat(&meshdb::cluster::Heartbeat {
        term: 1,
        leader: 2,
        placement: meshdb::cluster::Placement {
            term: 1,
            assignments,
        },
    })
    .unwrap();

    let router = meshdb::net::Router::new(Arc::clone(&node));
    assert!(
        !router.is_mine(ShardId(0)),
        "shard 0 should belong to node 2"
    );

    let sent = Request::Execute {
        shard: 0,
        statements: vec![meshdb::storage::exec::Statement::new("SELECT 1")],
    };
    router.forward(ShardId(0), sent.clone()).unwrap();

    let observed = seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    match observed {
        Request::Direct(inner) => assert_eq!(*inner, sent, "the inner request must be unchanged"),
        other => panic!(
            "a forwarded request must go on the wire wrapped in Direct, or two nodes with \
             disagreeing maps will bounce it forever; got {other:?}"
        ),
    }
    assert_eq!(router.stats().forwarded, 1);
}

/// A node with a follower attached, so weaker reads have a local copy to be answered from.
#[test]
fn read_consistency_levels_decide_where_a_read_is_answered() {
    // The point of step 12: a caller trades freshness for not going to the leader. The levels
    // are only meaningful if they actually route differently, so this checks the routing, not
    // just that each returns rows.
    use meshdb::net::ReadConsistency;

    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // A shard node 0 does not own, so the level actually decides something.
    let elsewhere = (0..2u32)
        .map(ShardId)
        .find(|s| placement.owner(*s) != Some(nodes[0].id))
        .expect("some shard should live elsewhere");
    c.execute(elsewhere.0, "INSERT INTO t VALUES (1)").unwrap();

    // Linearizable is forwarded to the owner and sees the write.
    let strong = c
        .query_with(
            elsewhere.0,
            "SELECT count(*) FROM t",
            ReadConsistency::Linearizable,
        )
        .unwrap();
    assert_eq!(strong.rows[0][0], meshdb::storage::Value::Integer(1));

    // Stale is answerable by whichever node took the request. It must still be answered —
    // the level is about freshness, not about failing.
    let stale = c
        .query_with(
            elsewhere.0,
            "SELECT count(*) FROM t",
            ReadConsistency::Stale,
        )
        .unwrap();
    assert!(
        stale.rows[0][0] == meshdb::storage::Value::Integer(0)
            || stale.rows[0][0] == meshdb::storage::Value::Integer(1),
        "a stale read may lag but must be a real answer: {:?}",
        stale.rows[0][0]
    );

    // AtLeastLsn beyond anything this node holds cannot be answered locally, so it is
    // forwarded to the owner — which does hold it.
    let bounded = c
        .query_with(
            elsewhere.0,
            "SELECT count(*) FROM t",
            ReadConsistency::AtLeastLsn(1),
        )
        .unwrap();
    assert_eq!(
        bounded.rows[0][0],
        meshdb::storage::Value::Integer(1),
        "a bounded-staleness read must not be answered by a copy that is behind it"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn the_default_read_level_is_the_strong_one() {
    // A caller that says nothing about freshness must get the strongest guarantee. The
    // opposite default would hand stale rows to code that never considered the question, and
    // it would do it silently.
    use meshdb::net::ReadConsistency;
    assert_eq!(ReadConsistency::default(), ReadConsistency::Linearizable);

    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    let elsewhere = (0..2u32)
        .map(ShardId)
        .find(|s| placement.owner(*s) != Some(nodes[0].id))
        .expect("some shard should live elsewhere");

    for i in 1..=5 {
        c.execute(elsewhere.0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    // Plain `query` must see every acknowledged write, which means it went to the owner.
    let rows = c.query(elsewhere.0, "SELECT count(*) FROM t").unwrap();
    assert_eq!(
        rows.rows[0][0],
        meshdb::storage::Value::Integer(5),
        "the default read must reflect every acknowledged write"
    );

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn a_hung_shard_owner_does_not_block_forwards_to_healthy_owners() {
    // The router once held its connection map's lock across the network round trip. One
    // owner that accepted connections and answered nothing would then block every forward on
    // the node — any shard, any owner — for the full timeout. Same disease that froze the
    // election loop, sitting in the write path.
    use meshdb::net::protocol::{PROTOCOL_VERSION, Request, Response, read_message, write_message};
    use std::net::TcpListener;

    // Owner of shard 0: accepts, says hello, then never answers anything again.
    let hung = TcpListener::bind("127.0.0.1:0").unwrap();
    let hung_addr = hung.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in hung.incoming().flatten() {
            {
                let mut r = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut w = std::io::BufWriter::new(stream.try_clone().unwrap());
                let _: Result<Request, _> = read_message(&mut r);
                let _ = write_message(
                    &mut w,
                    &Response::Welcome {
                        version: PROTOCOL_VERSION,
                        shard_count: 2,
                        epoch: None,
                    },
                );
                held.push(stream); // and now silence, forever
            }
        }
    });

    // Owner of shard 1: healthy, answers everything with Ok.
    let healthy = TcpListener::bind("127.0.0.1:0").unwrap();
    let healthy_addr = healthy.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in healthy.incoming().flatten() {
            {
                std::thread::spawn(move || {
                    let mut r = std::io::BufReader::new(stream.try_clone().unwrap());
                    let mut w = std::io::BufWriter::new(stream);
                    let _: Result<Request, _> = read_message(&mut r);
                    let _ = write_message(
                        &mut w,
                        &Response::Welcome {
                            version: PROTOCOL_VERSION,
                            shard_count: 2,
                            epoch: None,
                        },
                    );
                    while read_message::<Request, _>(&mut r).is_ok() {
                        let _ = write_message(&mut w, &Response::Ok);
                    }
                });
            }
        }
    });

    // A node that believes node 2 (hung) owns shard 0 and node 3 (healthy) owns shard 1.
    let dir = TempDir::new().unwrap();
    let terms = TermStore::open(dir.path()).unwrap();
    let fence = Arc::new(Fence::new(1, 0));
    let election = Election::new(quick(1, vec![2, 3]), terms, Instant::now()).unwrap();
    let manager = Arc::new(
        ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                ..ShardConfig::floor()
            },
        )
        .unwrap(),
    );
    let node = Arc::new(ClusterNode::new(
        1,
        election,
        fence,
        [(2u64, hung_addr), (3u64, healthy_addr)]
            .into_iter()
            .collect(),
        manager as Arc<dyn DurabilitySource>,
        vec![ShardId(0), ShardId(1)],
    ));
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(ShardId(0), 2u64);
    assignments.insert(ShardId(1), 3u64);
    node.handle_heartbeat(&meshdb::cluster::Heartbeat {
        term: 1,
        leader: 2,
        placement: meshdb::cluster::Placement {
            term: 1,
            assignments,
        },
    })
    .unwrap();

    let router =
        Arc::new(meshdb::net::Router::new(Arc::clone(&node)).with_timeout(Duration::from_secs(2)));

    // Thread A forwards into the hung owner and will sit there until its timeout.
    let ra = Arc::clone(&router);
    let a = std::thread::spawn(move || {
        let _ = ra.forward(
            ShardId(0),
            Request::Execute {
                shard: 0,
                statements: vec![meshdb::storage::exec::Statement::new("SELECT 1")],
            },
        );
    });
    // Give A time to be inside the round trip.
    std::thread::sleep(Duration::from_millis(200));

    // Thread B forwards to the healthy owner. It must not wait for A's timeout.
    let started = Instant::now();
    let result = router.forward(
        ShardId(1),
        Request::Execute {
            shard: 1,
            statements: vec![meshdb::storage::exec::Statement::new("SELECT 1")],
        },
    );
    let took = started.elapsed();

    assert!(
        result.is_ok(),
        "the healthy forward should succeed: {result:?}"
    );
    assert!(
        took < Duration::from_millis(800),
        "a forward to a healthy owner waited {took:?} behind a hung one; the router is \
         serialising forwards through a lock held across network I/O"
    );
    a.join().unwrap();
}

#[test]
fn a_transaction_commits_on_the_shard_owner_through_forwarding() {
    // A client-held transaction on a shard owned by another node: it must buffer on the entry
    // node and the atomic COMMIT must forward to the owner, landing there all-or-nothing.
    let nodes = cluster(3);
    let _ = await_leader(&nodes, Duration::from_secs(5)).expect("no leader");
    let placement = await_spread(&nodes, Duration::from_secs(5)).expect("no spread");
    std::thread::sleep(Duration::from_millis(400));

    let mut c = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();

    // A shard node 0 does NOT own, so BEGIN buffers on node 0 and COMMIT must forward.
    let elsewhere = (0..2u32)
        .map(ShardId)
        .find(|s| placement.owner(*s) != Some(nodes[0].id))
        .expect("some shard should live on another node");
    let owner_id = placement.owner(elsewhere).unwrap();
    assert_ne!(
        owner_id, nodes[0].id,
        "the transaction shard must be remote"
    );

    let mut tx = c.begin(elsewhere.0).unwrap();
    for i in 1..=15 {
        tx.execute(format!("INSERT INTO t VALUES ({i}, 'txn')"))
            .unwrap();
    }
    let (rows, _) = tx.commit().unwrap();
    assert_eq!(rows, 15, "the forwarded COMMIT applied the whole batch");

    // Present on the owner, atomically.
    let owner = nodes.iter().find(|n| n.id == owner_id).unwrap();
    let n = owner
        .manager
        .query(
            elsewhere,
            meshdb::storage::exec::Statement::new("SELECT count(*) FROM t"),
        )
        .unwrap();
    match n {
        meshdb::storage::exec::Outcome::Ok(meshdb::storage::exec::Executed::Rows(r)) => {
            assert_eq!(r.rows[0][0], meshdb::storage::Value::Integer(15));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A forwarded transaction that fails on the owner rolls back there too — atomicity
    // survives the hop.
    let mut c2 = meshdb::net::Client::connect(&nodes[0].addr).unwrap();
    let mut bad = c2.begin(elsewhere.0).unwrap();
    bad.execute("INSERT INTO t VALUES (500, 'ok')").unwrap();
    bad.execute("INSERT INTO t VALUES (1, 'dup')").unwrap(); // duplicate PK
    assert!(
        bad.commit().is_err(),
        "a forwarded failing transaction must be refused"
    );
    let after = owner
        .manager
        .query(
            elsewhere,
            meshdb::storage::exec::Statement::new("SELECT count(*) FROM t WHERE id = 500"),
        )
        .unwrap();
    match after {
        meshdb::storage::exec::Outcome::Ok(meshdb::storage::exec::Executed::Rows(r)) => {
            assert_eq!(
                r.rows[0][0],
                meshdb::storage::Value::Integer(0),
                "the failed forwarded transaction left nothing"
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    for n in &nodes {
        n.stop();
    }
}
