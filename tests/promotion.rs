//! Data-plane promotion: a follower's replicated files becoming the ones it writes to.
//!
//! The hazard being guarded is silent. A follower writes raw pages and, on bootstrap,
//! renames a whole new file over the old one — neither through SQLite. A connection open
//! across either serves stale rows, or reads a deleted inode forever, with no error and a
//! passing `integrity_check`. So these tests care as much about what is *refused* as about
//! what works.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use meshdb::cluster::{Fence, Promotion};
use meshdb::net::{Client, NodeServices, Replica, ReplicaConfig, Server, ServerConfig};
use meshdb::replication::{AckTracker, Follower, FrameLog, FrameLogConfig};
use meshdb::shard::mode::ShardMode;
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);
const FOLLOWER: u64 = 2;

/// The single scalar a query returned.
fn scalar(o: meshdb::storage::exec::Outcome) -> Value {
    match o {
        meshdb::storage::exec::Outcome::Ok(meshdb::storage::exec::Executed::Rows(r)) => {
            r.rows[0][0].clone()
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

fn insert(i: i64) -> Statement {
    Statement::with_params(
        "INSERT INTO t VALUES (?1, ?2)",
        vec![Value::Integer(i), Value::Text(format!("value-{i}"))],
    )
}

struct Primary {
    manager: Arc<ShardManager>,
    addr: String,
    _dir: TempDir,
    _server: Arc<Server>,
}

fn primary(voters: usize) -> Primary {
    let dir = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: 8 * 1024 * 1024,
        shard_count: 1,
    }));
    let acks = Arc::new(AckTracker::new(voters, Duration::from_secs(5)));
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
                acks: Some(acks),
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
    Primary {
        manager,
        addr,
        _dir: dir,
        _server: server,
    }
}

/// A replica node whose `Follower` and `ShardManager` address the **same directory** — which
/// is what makes promotion a handover rather than a copy, and what makes the mode invariant
/// necessary.
struct Replicant {
    manager: Arc<ShardManager>,
    replica: Arc<Replica>,
    fence: Arc<Fence>,
    promotion: Promotion,
    dir: TempDir,
    _stage: TempDir,
}

fn replicant(p: &Primary) -> Replicant {
    replicant_with_shards(p, 1)
}

fn replicant_with_shards(p: &Primary, shards: u32) -> Replicant {
    let dir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let fence = Arc::new(Fence::new(FOLLOWER, 0));
    let manager = Arc::new(
        ShardManager::open_clustered(
            dir.path(),
            ShardConfig {
                shard_count: shards,
                capture: true,
                ..ShardConfig::floor()
            },
            None,
            None,
            Some(Arc::clone(&fence) as Arc<dyn meshdb::shard::WriteGate>),
        )
        .unwrap(),
    );
    let replica = Arc::new(Replica::new(
        ReplicaConfig {
            primary_addr: p.addr.clone(),
            shards: (0..shards).map(ShardId).collect(),
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: FOLLOWER,
        },
        // The same directory the manager was opened on.
        Arc::new(Follower::open(dir.path()).unwrap()),
    ));
    let promotion = Promotion::new(
        Arc::clone(&manager),
        Arc::clone(&replica),
        Arc::clone(&fence),
        (0..shards).map(ShardId).collect(),
    )
    .with_settle_timeout(Duration::from_secs(5));

    Replicant {
        manager,
        replica,
        fence,
        promotion,
        dir,
        _stage: stage,
    }
}

#[test]
fn a_followed_shard_cannot_be_opened_by_sqlite() {
    // The invariant, enforced where connections are opened. Without it, a read here would
    // succeed and go on serving stale pages after the follower rewrote the file.
    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    assert_eq!(r.manager.mode(S0), ShardMode::Followed);

    let read = r
        .manager
        .query(S0, Statement::new("SELECT count(*) FROM sqlite_schema"));
    let err = read.expect_err("a read on a followed shard must be refused");
    assert!(err.to_string().contains("followed"), "{err}");

    let write = r
        .manager
        .execute_one(S0, Statement::new("CREATE TABLE x (a INTEGER)"));
    assert!(
        write.is_err(),
        "and a write on a followed shard must be refused too"
    );
    assert!(
        r.manager.modes().refused_opens() > 0,
        "refusals are counted"
    );
}

#[test]
fn a_promoted_follower_serves_writes_on_the_data_it_replicated() {
    // The point of the whole step: the replica's own files become the ones it writes to, with
    // no copy and no re-bootstrap.
    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    let stop = r.replica.stop_handle();
    let r2 = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = r2.run();
    });

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=40 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    assert!(
        r.replica.follower().position(S0).applied_lsn > 0,
        "the replica should have applied something"
    );

    // The primary is gone. This node takes over.
    r.promotion.promote(7).unwrap();
    assert_eq!(r.manager.mode(S0), ShardMode::Led);
    assert!(r.fence.is_open(S0));
    assert!(!r.replica.is_running(), "the pull loop must have stopped");
    let _ = stop;

    // Everything replicated is readable through SQLite, on the very same files.
    let rows = r
        .manager
        .query(S0, Statement::new("SELECT count(*) FROM t"))
        .unwrap();
    assert_eq!(scalar(rows), Value::Integer(40));

    // And it now accepts writes, which a follower could not.
    r.manager.execute_one(S0, insert(41)).unwrap();
    let rows = r
        .manager
        .query(S0, Statement::new("SELECT count(*) FROM t"))
        .unwrap();
    assert_eq!(scalar(rows), Value::Integer(41));

    let check = r
        .manager
        .query(S0, Statement::new("PRAGMA integrity_check"))
        .unwrap();
    assert_eq!(scalar(check), Value::Text("ok".into()));
}

#[test]
fn demotion_closes_connections_rather_than_merely_stopping_writes() {
    // A connection left open across the handover is the failure mode: after the follower
    // rewrites pages it serves stale rows, and after a snapshot rename it reads a deleted
    // inode forever. Neither reports an error, so the handover has to close handles.
    let p = primary(2);
    let r = replicant(&p);

    // Lead first, and actually open a connection by using it.
    r.promotion.promote(1).unwrap();
    r.manager
        .execute_one(
            S0,
            Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY)"),
        )
        .unwrap();
    r.manager
        .query(S0, Statement::new("SELECT count(*) FROM t"))
        .unwrap();

    // Hand the file over.
    r.promotion.follow().unwrap();

    // The handles must actually be closed, not merely unreachable. The mode check refuses
    // new opens, so a leaked connection is invisible from the outside — until the shard is
    // promoted again, when that cached handle becomes usable and serves the pre-rename
    // inode. Counting what was closed is the only way to see it.
    assert!(
        r.manager.modes().closed_on_handover() >= 2,
        "the handover must close both the writer's and the reader's cached connections, \
         closed {} of them",
        r.manager.modes().closed_on_handover()
    );

    assert!(
        r.manager
            .query(S0, Statement::new("SELECT count(*) FROM t"))
            .is_err(),
        "a reader must not keep serving a followed shard from a cached connection"
    );
    assert!(
        !r.fence.is_open(S0),
        "and writes must have been gated first"
    );
}

#[test]
fn promotion_refuses_rather_than_racing_a_running_pull_loop() {
    // Promoting while the pull loop is mid-apply would hand SQLite a file being rewritten
    // underneath it. Refusing is an outage; proceeding is silent corruption.
    //
    // The loop is made genuinely stuck rather than merely slow: its primary accepts the
    // connection and never answers, so `sync_once` sits in a socket read far longer than the
    // settle timeout. Racing a healthy loop would test the test, not the code.
    use std::net::TcpListener;

    let black_hole = TcpListener::bind("127.0.0.1:0").unwrap();
    let stuck_addr = black_hole.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in black_hole.incoming() {
            held.push(stream);
        }
    });

    let p = primary(2);
    let mut r = replicant(&p);
    // Point the replica at the black hole, so its loop cannot make progress or exit.
    r.replica = Arc::new(Replica::new(
        ReplicaConfig {
            primary_addr: stuck_addr,
            shards: vec![S0],
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: r._stage.path().to_path_buf(),
            node: FOLLOWER,
        },
        Arc::new(Follower::open(r.dir.path()).unwrap()),
    ));
    let promotion = Promotion::new(
        Arc::clone(&r.manager),
        Arc::clone(&r.replica),
        Arc::clone(&r.fence),
        vec![S0],
    )
    .with_settle_timeout(Duration::from_millis(200));
    promotion.follow().unwrap();

    let replica = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = replica.run();
    });
    // Let it get into the blocking read.
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        r.replica.is_running(),
        "the loop should be stuck, not finished"
    );

    let err = promotion
        .promote(3)
        .expect_err("promotion must refuse while the pull loop is still running");
    assert!(err.to_string().contains("did not come to rest"), "{err}");
    assert!(
        !r.fence.is_open(S0),
        "a refused promotion must not leave the write gate open"
    );
    assert_eq!(
        r.manager.mode(S0),
        ShardMode::Followed,
        "and must not have taken ownership of the file"
    );

    r.replica.stop_handle().store(true, Ordering::Relaxed);
}

#[test]
fn a_standalone_node_leads_everything_by_default() {
    // The invariant must cost a non-replicated deployment nothing at all.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 2,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    assert_eq!(m.mode(ShardId(0)), ShardMode::Led);
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    m.execute_one(ShardId(0), Statement::new("INSERT INTO t VALUES (1)"))
        .unwrap();
    assert_eq!(m.modes().refused_opens(), 0);
    // Silence the unused-field warning on `dir`, which the replicant fixture needs.
    let _ = &dir;
}

#[test]
fn placement_moves_shards_one_at_a_time() {
    // Placement hands out shards individually, so promotion has to be per shard rather than
    // all-or-nothing: a node routinely leads some and follows others.
    let p = primary(2);
    let r = replicant_with_shards(&p, 4);

    // Start following everything, then take two shards.
    r.promotion.follow().unwrap();
    for s in 0..4u32 {
        assert_eq!(r.manager.mode(ShardId(s)), ShardMode::Followed);
    }

    r.promotion.apply(&[ShardId(0), ShardId(2)], 3).unwrap();

    for s in [0u32, 2] {
        assert_eq!(r.manager.mode(ShardId(s)), ShardMode::Led, "shard {s}");
        assert!(r.fence.is_open(ShardId(s)), "shard {s} gate should be open");
    }
    for s in [1u32, 3] {
        assert_eq!(r.manager.mode(ShardId(s)), ShardMode::Followed, "shard {s}");
        assert!(
            !r.fence.is_open(ShardId(s)),
            "shard {s} gate should be shut"
        );
    }

    // The replica keeps following only what it does not lead.
    let following = r.replica.following();
    assert!(!following.contains(&ShardId(0)) && !following.contains(&ShardId(2)));
    assert!(following.contains(&ShardId(1)) && following.contains(&ShardId(3)));
}

#[test]
fn a_shard_taken_away_is_closed_before_its_file_is_handed_over() {
    // The ordering that matters. If the gate were closed after the handover, this node would
    // still be committing SQL into a file the replication path had begun rewriting.
    let p = primary(2);
    let r = replicant_with_shards(&p, 4);
    r.promotion.follow().unwrap();
    r.promotion.apply(&[ShardId(0), ShardId(1)], 3).unwrap();
    assert!(r.fence.is_open(ShardId(1)));

    // Placement takes shard 1 away.
    r.promotion.apply(&[ShardId(0)], 4).unwrap();

    assert!(
        !r.fence.is_open(ShardId(1)),
        "a shard taken away must not still be writable"
    );
    assert_eq!(r.manager.mode(ShardId(1)), ShardMode::Followed);
    assert!(r.replica.following().contains(&ShardId(1)));
    assert!(
        r.manager
            .execute_one(ShardId(1), Statement::new("CREATE TABLE z (a INTEGER)"))
            .is_err(),
        "and writing it must be refused"
    );

    // The retained shard is untouched by the change.
    assert!(r.fence.is_open(ShardId(0)));
    assert_eq!(r.manager.mode(ShardId(0)), ShardMode::Led);
}

#[test]
fn a_gained_shard_is_not_writable_until_its_file_is_taken_over() {
    // The gate must open last. Opening it before ownership moves would let this node write a
    // file the replication path still owns and is rewriting underneath it.
    let p = primary(2);
    let r = replicant_with_shards(&p, 2);
    r.promotion.follow().unwrap();

    // While following, the mode refuses opens and the gate is shut. Both, not either.
    assert!(!r.fence.is_open(ShardId(0)));
    assert_eq!(r.manager.mode(ShardId(0)), ShardMode::Followed);

    r.promotion.apply(&[ShardId(0)], 2).unwrap();

    // After promotion the two agree the other way.
    assert!(r.fence.is_open(ShardId(0)));
    assert_eq!(r.manager.mode(ShardId(0)), ShardMode::Led);
    r.manager
        .execute_one(ShardId(0), Statement::new("CREATE TABLE ok (a INTEGER)"))
        .unwrap();
}

#[test]
fn applying_the_same_placement_twice_changes_nothing() {
    // Placement is republished on every heartbeat. If each republication tore files down and
    // rebuilt them, a healthy cluster would spend its time handing shards to itself.
    let p = primary(2);
    let r = replicant_with_shards(&p, 4);
    r.promotion.follow().unwrap();
    r.promotion.apply(&[ShardId(0), ShardId(1)], 3).unwrap();

    let before = r.manager.modes().closed_on_handover();
    for _ in 0..5 {
        r.promotion.apply(&[ShardId(0), ShardId(1)], 3).unwrap();
    }
    assert_eq!(
        r.manager.modes().closed_on_handover(),
        before,
        "repeating an unchanged placement must not churn connections"
    );
    assert!(r.fence.is_open(ShardId(0)) && r.fence.is_open(ShardId(1)));
}
