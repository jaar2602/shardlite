//! Dynamic catalog replication over the real node protocol.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shardlite::cluster::{
    Catalog, CatalogStore, ClusterId, ClusterNode, Compatibility, DurabilitySource, Election,
    ElectionConfig, Fence, Heartbeat, MemberRole, Placement, TermStore,
};
use shardlite::net::{Client, NodeServices, Server, ServerConfig};
use shardlite::shard::{LinearV1, Routing, ShardConfig, ShardId, ShardManager};
use tempfile::TempDir;

#[test]
fn a_voter_fsyncs_and_publishes_the_leaders_exact_catalog_value() {
    let leader_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let routing = Routing::LinearV1(LinearV1::ONE);
    let mut initial = Catalog::bootstrap(
        ClusterId([6; 16]),
        1,
        "node-1:4600".into(),
        routing,
        Compatibility::current(routing).unwrap(),
    )
    .unwrap();
    initial.add_learner(2, 1, "node-2:4600".into()).unwrap();
    initial.promote(2, MemberRole::Voter).unwrap();
    initial.initialize_placements(1).unwrap();

    let leader_store = CatalogStore::create(leader_dir.path(), initial.clone()).unwrap();
    let follower_store = Arc::new(CatalogStore::create(follower_dir.path(), initial).unwrap());
    let manager = Arc::new(
        ShardManager::open(
            follower_dir.path(),
            ShardConfig {
                shard_count: 1,
                ..ShardConfig::floor()
            },
        )
        .unwrap(),
    );
    let election = Election::new(
        ElectionConfig {
            node: 2,
            peers: vec![1],
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        },
        TermStore::open(&follower_dir.path().join("cluster")).unwrap(),
        Instant::now(),
    )
    .unwrap();
    let cluster = Arc::new(
        ClusterNode::new(
            2,
            election,
            Arc::new(Fence::new(2, 0)),
            BTreeMap::from([(1, "node-1:4600".into())]),
            Arc::clone(&manager) as Arc<dyn DurabilitySource>,
            vec![ShardId(0)],
        )
        .with_catalog(Arc::clone(&follower_store))
        .with_modes(Arc::clone(manager.modes())),
    );
    cluster
        .handle_heartbeat(&Heartbeat {
            term: 1,
            leader: 1,
            placement: Placement {
                term: 1,
                assignments: BTreeMap::from([(ShardId(0), 1)]),
            },
        })
        .unwrap();

    let server = Server::bind_with(
        manager,
        NodeServices {
            cluster: Some(cluster),
            catalog: Some(Arc::clone(&follower_store)),
            ..Default::default()
        },
        ServerConfig {
            addr: "127.0.0.1:0".into(),
            ..ServerConfig::default()
        },
    )
    .unwrap();
    let addr = server.local_addr().unwrap();
    let shutdown = server.shutdown_handle();
    let thread = std::thread::spawn(move || server.serve().unwrap());

    let (_, proposal) = leader_store
        .propose(1, 1, |catalog| {
            catalog.add_learner(3, 1, "node-3:4600".into())
        })
        .unwrap();
    let mut client = Client::connect(&addr.to_string()).unwrap();
    client.catalog_prepare(1, &proposal).unwrap();
    assert_eq!(
        follower_store
            .prepared_snapshot()
            .as_ref()
            .map(|prepared| prepared.catalog.version),
        Some(2)
    );
    assert!(!follower_store.snapshot().members.contains_key(&3));

    client.catalog_commit(1, 1, 2).unwrap();
    assert!(follower_store.snapshot().members.contains_key(&3));
    assert!(follower_store.prepared_snapshot().is_none());

    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = TcpStream::connect(addr);
    thread.join().unwrap();
}
