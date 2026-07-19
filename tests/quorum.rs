//! Quorum acknowledgement: a client's "ok" must mean a majority holds the write.
//!
//! The property under test is the project's governing requirement — nothing acknowledged is
//! ever lost. Everything here is arranged so that a regression shows up as data missing from
//! a survivor, not as a subtler symptom.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use meshdb::net::{Client, NodeServices, Replica, ReplicaConfig, Server, ServerConfig};
use meshdb::replication::{AckTracker, Follower, FrameLog, FrameLogConfig};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);
const FOLLOWER_NODE: u64 = 2;

struct Leader {
    manager: Arc<ShardManager>,
    acks: Arc<AckTracker>,
    server: Arc<Server>,
    addr: String,
    _dir: TempDir,
}

/// A leader that will not acknowledge a write until `voters` members hold it.
fn leader(voters: usize, patience: Duration) -> Leader {
    let dir = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: 8 * 1024 * 1024,
        shard_count: 1,
    }));
    let acks = Arc::new(AckTracker::new(voters, patience));
    let manager = Arc::new(
        ShardManager::open_clustered(
            dir.path(),
            ShardConfig {
                shard_count: 1,
                capture: true,
                ..ShardConfig::floor()
            },
            Some(frames.clone()),
            Some(Arc::clone(&acks)),
            None,
        )
        .unwrap(),
    );
    let server = Arc::new(
        Server::bind_with(
            Arc::clone(&manager),
            NodeServices {
                frames: Some(frames),
                acks: Some(Arc::clone(&acks)),
                ..Default::default()
            },
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
    Leader {
        manager,
        acks,
        server,
        addr,
        _dir: dir,
    }
}

fn follower_for(l: &Leader, dir: &TempDir, stage: &TempDir) -> Replica {
    Replica::new(
        ReplicaConfig {
            primary_addr: l.addr.clone(),
            shards: vec![S0],
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: FOLLOWER_NODE,
        },
        Arc::new(Follower::open(dir.path()).unwrap()),
    )
}

/// Start the follower's pull loop. Returns its stop switch.
///
/// Called *before* any DDL: under quorum-ack the schema statement is itself a write that
/// needs a majority, so a follower started afterwards would deadlock the setup.
fn start(r: &Arc<Replica>) -> Arc<std::sync::atomic::AtomicBool> {
    let stop = r.stop_handle();
    let r2 = Arc::clone(r);
    std::thread::spawn(move || {
        let _ = r2.run();
    });
    stop
}

fn insert(i: i64) -> Statement {
    Statement::with_params(
        "INSERT INTO t VALUES (?1, ?2)",
        vec![Value::Integer(i), Value::Text(format!("value-{i}"))],
    )
}

fn rows_in(path: &std::path::Path) -> Vec<i64> {
    let conn = meshdb::rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
    let ids: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    ids
}

#[test]
fn a_write_no_quorum_holds_is_not_acknowledged() {
    // The bug this whole file exists for. Before quorum-ack this returned Ok, the client
    // believed the write was durable, and a leader dying here lost it with no trace.
    //
    // The follower has to exist first: under quorum-ack the schema statement is itself a
    // write needing a majority. It is then stopped, which is what removes the quorum.
    let l = leader(2, Duration::from_millis(300));
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = Arc::new(follower_for(&l, &fdir, &stage));
    let stop = start(&r);

    let mut c = Client::connect(&l.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    l.manager.execute_one(S0, insert(1)).unwrap();

    // The only other copy stops. Everything after this must be refused, not acknowledged —
    // continuing to say "ok" while the survivor falls behind is exactly how acknowledged
    // data is lost, and from the client's side it looks perfectly healthy.
    stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(100));

    let err = l
        .manager
        .execute_one(S0, insert(2))
        .expect_err("a write no quorum holds must not be acknowledged");

    let msg = err.to_string();
    assert!(
        msg.contains("committed on this node"),
        "the error must say the write IS committed locally, or a client will retry and \
         double-apply: {msg}"
    );
    assert!(msg.contains("not confirmed"), "{msg}");
    assert!(
        !msg.contains("no writes in this batch were applied"),
        "the error must not also claim nothing was applied — a caller that believes that \
         retries, and applying a committed write twice is worse than the stall: {msg}"
    );
    assert!(
        matches!(err, meshdb::error::Error::NotReplicated { .. }),
        "the kind must survive so a caller can branch on it rather than parse text: {err:?}"
    );
    assert!(l.acks.stats().timed_out > 0, "the stall must be counted");
    println!("{msg}");
}

#[test]
fn a_write_a_follower_holds_is_acknowledged() {
    let l = leader(2, Duration::from_secs(5));
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = Arc::new(follower_for(&l, &fdir, &stage));
    let stop = start(&r);

    let mut c = Client::connect(&l.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();

    l.manager.execute_one(S0, insert(1)).unwrap();
    l.manager.execute_one(S0, insert(2)).unwrap();

    assert_eq!(l.acks.stats().timed_out, 0, "no write should have stalled");
    assert!(l.acks.stats().confirmed >= 2);
    stop.store(true, Ordering::Relaxed);
}

#[test]
fn every_acknowledged_write_survives_the_leaders_death() {
    // The property, end to end and stated as bluntly as it can be: whatever the leader said
    // "ok" to must be present on the survivor after the leader is gone.
    //
    // `voters = 2` makes the single follower load-bearing — every acknowledgement requires
    // it — so a regression cannot hide behind a quorum that did not include the survivor.
    let l = leader(2, Duration::from_secs(5));
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = Arc::new(follower_for(&l, &fdir, &stage));
    let stop = start(&r);

    let mut c = Client::connect(&l.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();

    // Record exactly what was acknowledged, and nothing else.
    let mut acknowledged = Vec::new();
    for i in 1..=60 {
        if l.manager.execute_one(S0, insert(i)).is_ok() {
            acknowledged.push(i);
        }
    }
    assert!(
        acknowledged.len() >= 55,
        "too many writes went unconfirmed to be a meaningful test: {} of 60",
        acknowledged.len()
    );

    // The leader dies. Nothing is flushed, drained, or handed over on the way out.
    l.server.shutdown_handle().store(true, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(100));

    let survivor = rows_in(&S0.path(r.follower().dir()));
    let missing: Vec<i64> = acknowledged
        .iter()
        .copied()
        .filter(|id| !survivor.contains(id))
        .collect();

    assert!(
        missing.is_empty(),
        "{} acknowledged writes were lost when the leader died: {missing:?}",
        missing.len()
    );
    println!(
        "{} acknowledged writes, all {} present on the survivor",
        acknowledged.len(),
        survivor.len()
    );
}

#[test]
fn a_standalone_node_does_not_wait_for_a_quorum_it_does_not_have() {
    // A deployment with no cluster must not block on reports that will never arrive. The
    // failure mode if this regresses is a server that simply stops accepting writes.
    let dir = TempDir::new().unwrap();
    let manager = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 1,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    manager
        .execute_all_shards(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT",
        ))
        .unwrap();

    let started = Instant::now();
    for i in 1..=20 {
        manager.execute_one(S0, insert(i)).unwrap();
    }
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(2),
        "a standalone node blocked for {took:?}; it should never wait for a quorum"
    );
}

#[test]
fn a_single_voter_cluster_is_its_own_quorum() {
    // `voters = 1` is a legitimate configuration — one node, replication configured for
    // later growth. It must not deadlock waiting for itself.
    let l = leader(1, Duration::from_millis(500));
    l.manager
        .execute_all_shards(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT",
        ))
        .unwrap();

    for i in 1..=10 {
        l.manager.execute_one(S0, insert(i)).unwrap();
    }
    assert_eq!(l.acks.stats().timed_out, 0);
}
