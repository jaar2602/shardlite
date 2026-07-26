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

use shardlite::cluster::{Fence, Promotion};
use shardlite::net::{Client, NodeServices, Replica, ReplicaConfig, Server, ServerConfig};
use shardlite::replication::{AckTracker, Follower, FrameLog, FrameLogConfig};
use shardlite::shard::mode::ShardMode;
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::Value;
use shardlite::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);
const FOLLOWER: u64 = 2;

/// Wait for the replica to have applied everything the primary has committed for `shard`.
///
/// A fixed sleep is a guess about how fast a machine is. Under load it is the wrong guess,
/// and the failure it produces is a one-off nobody can reproduce — which is worse than a
/// consistent failure, because it gets dismissed.
fn await_caught_up(p: &Primary, r: &Replicant, shard: ShardId, within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        let target = p.manager.last_lsn(shard);
        let have = r.replica.follower().position(shard).applied_lsn;
        if target > 0 && have >= target {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("replica stuck at {have} of {target} for {shard}");
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The single scalar a query returned.
fn scalar(o: shardlite::storage::exec::Outcome) -> Value {
    match o {
        shardlite::storage::exec::Outcome::Ok(shardlite::storage::exec::Executed::Rows(r)) => {
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
            Some(Arc::clone(&fence) as Arc<dyn shardlite::shard::WriteGate>),
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
            credentials: None,
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
fn a_followed_shard_refuses_writes_but_serves_reads() {
    // The asymmetry that makes read scale-out possible. Writes on a followed shard are
    // refused outright — two writers on one file is the corruption everything else guards
    // against. Reads are allowed, because `ShardAccess` excludes them from applies and
    // invalidates their cached connections afterwards, which is what makes them safe.
    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    // Give the follower something to hold. A shard it has never received has no file at all,
    // and a read then fails for that reason rather than this one.
    let stop = r.replica.stop_handle();
    let r2 = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = r2.run();
    });
    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        p.manager
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }
    assert!(
        await_caught_up(&p, &r, S0, Duration::from_secs(10)),
        "the replica never caught up"
    );
    stop.store(true, Ordering::Relaxed);
    assert!(r.replica.wait_idle(Duration::from_secs(5)));

    assert_eq!(r.manager.mode(S0), ShardMode::Followed);

    let write = r
        .manager
        .execute_one(S0, Statement::new("INSERT INTO t VALUES (999)"));
    assert!(
        write.is_err(),
        "a write on a followed shard must be refused"
    );
    assert!(
        r.manager.modes().refused_opens() > 0,
        "the refused write must be counted"
    );

    // And the read is served — from the follower's own replicated copy.
    let read = r
        .manager
        .query(S0, Statement::new("SELECT count(*) FROM t"))
        .expect("a followed shard must serve reads");
    assert_eq!(scalar(read), Value::Integer(20));
}

#[test]
fn a_read_never_sees_a_follower_mid_apply() {
    // The protocol's real job. Without the lock a read can land while pages are being
    // rewritten; without the generation it can be served from a connection whose cache
    // predates them. Either way the rows returned describe a state that never existed.
    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    let stop = r.replica.stop_handle();
    let r2 = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = r2.run();
    });

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // Read continuously while the primary writes and the follower applies.
    let reader_mgr = Arc::clone(&r.manager);
    let reading = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag = Arc::clone(&reading);
    let checker = std::thread::spawn(move || {
        let mut seen = Vec::new();
        while flag.load(Ordering::Relaxed) {
            if let Ok(out) = reader_mgr.query(S0, Statement::new("SELECT count(*) FROM t"))
                && let Value::Integer(n) = scalar(out)
            {
                seen.push(n);
            }
        }
        seen
    });

    for i in 1..=60 {
        p.manager
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }
    assert!(
        await_caught_up(&p, &r, S0, Duration::from_secs(10)),
        "the replica never caught up"
    );
    // One more pass so the reading thread definitely observes the final state.
    std::thread::sleep(Duration::from_millis(50));
    reading.store(false, Ordering::Relaxed);
    let seen = checker.join().unwrap();
    stop.store(true, Ordering::Relaxed);

    assert!(!seen.is_empty(), "the reader should have seen something");
    // Counts must only ever go up. A read served from a stale cache, or taken across an
    // apply, would show the count going backwards — data appearing to vanish.
    for w in seen.windows(2) {
        assert!(
            w[1] >= w[0],
            "a follower read went backwards, {} then {}: the reader saw a state that never \
             existed. Sequence: {seen:?}",
            w[0],
            w[1]
        );
    }
    // Monotonicity alone is not enough: a reader frozen on a stale cached connection returns
    // the same number forever, which is perfectly monotonic and completely wrong. It must
    // actually catch up.
    assert_eq!(
        seen.last().copied(),
        Some(60),
        "the follower's readers never caught up — a reader pinned to a stale connection \
         reports a constant value, which passes a monotonicity check while being wrong. \
         Saw {} distinct values, last {:?}",
        {
            let mut d = seen.clone();
            d.dedup();
            d.len()
        },
        seen.last()
    );
    println!(
        "{} reads, values progressed to {:?}, generation={} reopens={}",
        seen.len(),
        seen.last(),
        r.manager.modes().access(S0).generation(),
        r.manager.modes().access(S0).reopens()
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

    // Reads still work — that is the point of the handover protocol, not a hole in it — but
    // they are served by a *fresh* connection, which the count above establishes. The old
    // handle is gone, and with it the stale page cache and the risk of a deleted inode.
    assert!(
        r.manager
            .query(S0, Statement::new("SELECT count(*) FROM t"))
            .is_ok(),
        "a followed shard should still serve reads after the handover"
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
            credentials: None,
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

#[test]
fn a_stale_read_is_answered_by_the_replica_and_a_strong_one_is_not() {
    // The levels only mean something if they route differently. Here the replica genuinely
    // holds the shard, so a weak read can be served from it while a strong one cannot.
    use shardlite::net::ReadConsistency;
    use shardlite::net::protocol::{Request, Response};

    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    // A server in front of the replica, told about its follower so it can report how far
    // behind its copy is.
    let server = Arc::new(
        Server::bind_with(
            Arc::clone(&r.manager),
            NodeServices {
                follower: Some(Arc::clone(r.replica.follower())),
                ..Default::default()
            },
            ServerConfig {
                addr: "127.0.0.1:0".into(),
                ..ServerConfig::default()
            },
        )
        .unwrap(),
    );
    let replica_addr = server.local_addr().unwrap().to_string();
    let s = Arc::clone(&server);
    std::thread::spawn(move || {
        let _ = s.serve();
    });
    std::thread::sleep(Duration::from_millis(50));

    let stop = r.replica.stop_handle();
    let r2 = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = r2.run();
    });

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=30 {
        p.manager
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }
    assert!(
        await_caught_up(&p, &r, S0, Duration::from_secs(10)),
        "the replica never caught up"
    );
    stop.store(true, Ordering::Relaxed);
    assert!(r.replica.wait_idle(Duration::from_secs(5)));

    let mut rc = Client::connect(&replica_addr).unwrap();

    // Stale: the replica answers from its own copy.
    let stale = rc
        .query_with(0, "SELECT count(*) FROM t", ReadConsistency::Stale)
        .expect("a replica must answer a stale read");
    assert_eq!(stale.rows[0][0], Value::Integer(30));

    // AtLeastLsn within what it holds: also answered locally.
    let applied = r.replica.follower().position(S0).applied_lsn;
    assert!(applied > 0, "the replica should have applied something");
    let bounded = rc
        .query_with(
            0,
            "SELECT count(*) FROM t",
            ReadConsistency::AtLeastLsn(applied),
        )
        .expect("a bounded read within what the replica holds must be answered");
    assert_eq!(bounded.rows[0][0], Value::Integer(30));

    // Beyond what it holds, and with no router to forward to, it must refuse rather than
    // answer from a copy that is behind the guarantee asked for.
    let too_new = rc.request(Request::Query {
        shard: 0,
        statement: Statement::new("SELECT count(*) FROM t"),
        consistency: ReadConsistency::AtLeastLsn(applied + 10_000),
    });
    match too_new {
        Err(e) => assert!(
            !e.to_string().is_empty(),
            "a read past what this copy holds must not be answered"
        ),
        Ok(Response::Rows { .. }) => {
            panic!("a copy behind the requested position answered anyway — the guarantee is a lie")
        }
        Ok(_) => {}
    }
}

#[test]
fn a_schema_change_during_catch_up_reaches_the_destination_before_cutover() {
    // The other half of the transfer window. `fence_for_transfer` stops the source writing so
    // that its `last_lsn` is final, and the destination catches up to exactly that position
    // before taking over. A DDL applied *before* that barrier must therefore arrive the way
    // every other write does — as frames — or the destination takes over holding an older
    // schema than the rows it just received, and `user_version` records that mismatch forever.
    //
    // Nothing replays DDL to the destination here, and nothing may: its shard is followed, so
    // `check_may_open` refuses a writer connection outright. The frames are the only path, and
    // this test is what says they are enough.
    let p = primary(2);
    let r = replicant(&p);
    r.promotion.follow().unwrap();

    let stop = r.replica.stop_handle();
    let r2 = Arc::clone(&r.replica);
    std::thread::spawn(move || {
        let _ = r2.run();
    });

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=10 {
        p.manager
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }
    assert!(
        await_caught_up(&p, &r, S0, Duration::from_secs(10)),
        "the replica never caught up to the first schema"
    );

    // The schema change lands on the source while the destination is still following — the
    // window a transfer runs in.
    p.manager
        .apply_ddl_to(S0, Statement::new("ALTER TABLE t ADD COLUMN note TEXT"))
        .unwrap();
    assert_eq!(p.manager.schema_version(S0).unwrap(), 2);
    p.manager
        .execute_one(
            S0,
            Statement::new("INSERT INTO t (id, note) VALUES (11, 'after')"),
        )
        .unwrap();

    assert!(
        await_caught_up(&p, &r, S0, Duration::from_secs(10)),
        "the replica never caught up past the schema change"
    );
    stop.store(true, Ordering::Relaxed);
    assert!(r.replica.wait_idle(Duration::from_secs(5)));

    // Cutover.
    r.promotion.promote(1).unwrap();

    // The destination took over at the source's schema version, holding a column no DDL was
    // ever applied to it for. The header page carried the version and the frames carried the
    // column, together, in the same stream as the rows.
    assert_eq!(
        r.manager.schema_version(S0).unwrap(),
        2,
        "the destination must take over at the source's schema version"
    );
    let read = r
        .manager
        .query(S0, Statement::new("SELECT note FROM t WHERE id = 11"))
        .expect("the destination must hold the post-change schema");
    assert_eq!(scalar(read), Value::Text("after".into()));
}
