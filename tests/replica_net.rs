//! Two nodes over TCP: a primary serving frames, a follower pulling them.

use std::sync::Arc;
use std::time::Duration;

use meshdb::net::{Client, Replica, ReplicaConfig, Server, ServerConfig};
use meshdb::replication::{Follower, FrameLog, FrameLogConfig};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

struct Primary {
    _dir: TempDir,
    manager: Arc<ShardManager>,
    frames: Arc<FrameLog>,
    addr: String,
    server: Arc<Server>,
}

/// A primary with capture on, its frames retained, and a server in front.
fn primary(shards: u32, retention_bytes: usize) -> Primary {
    let dir = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: retention_bytes,
        shard_count: shards,
    }));
    let manager = Arc::new(
        ShardManager::open_with_sink(
            dir.path(),
            ShardConfig {
                shard_count: shards,
                capture: true,
                ..ShardConfig::floor()
            },
            Some(frames.clone()),
        )
        .unwrap(),
    );
    let server = Arc::new(
        Server::bind_with_frames(
            Arc::clone(&manager),
            Some(Arc::clone(&frames)),
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
        _dir: dir,
        manager,
        frames,
        addr,
        server,
    }
}

fn replica(p: &Primary, dir: &TempDir, stage: &TempDir, shards: Vec<ShardId>) -> Replica {
    Replica::new(
        ReplicaConfig {
            primary_addr: p.addr.clone(),
            shards,
            poll_interval: Duration::from_millis(10),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: 2,
        },
        Arc::new(Follower::open(dir.path()).unwrap()),
    )
}

fn insert(i: i64) -> Statement {
    Statement::with_params(
        "INSERT INTO t VALUES (?1, ?2)",
        vec![Value::Integer(i), Value::Text(format!("value-{i}"))],
    )
}

/// Compare a shard on the primary against the follower's copy.
///
/// By **content**, not bytes. An earlier version compared whole files and happened to work
/// because these tests are small and quiesced — but primary and follower files are not
/// byte-identical in general: freelist layout, vacuum history and checkpoint timing all
/// differ legitimately. Measured while building `storage::verify`: switching that module to
/// a byte hash made `a_correctly_replicated_shard_matches_its_primary` fail outright.
fn assert_converged(p: &Primary, r: &Replica, shard: ShardId) {
    use meshdb::storage::verify::{hash_file, hex};

    let tmp = TempDir::new().unwrap();
    let snap = tmp.path().join("cmp.db");
    p.manager.snapshot(shard, &snap).unwrap();

    let a = hash_file(&snap).unwrap();
    let b = hash_file(&shard.path(r.follower().dir())).unwrap();
    assert_eq!(
        hex(&a),
        hex(&b),
        "{shard}: the follower's contents differ from the primary's"
    );

    // Still worth checking, and worth knowing it is not sufficient: a follower holding valid
    // pages that are the wrong pages passes this happily. The content hash above is what
    // actually catches that.
    let conn = meshdb::rusqlite::Connection::open(shard.path(r.follower().dir())).unwrap();
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}

#[test]
fn a_follower_replicates_over_the_network() {
    let p = primary(1, 8 * 1024 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = replica(&p, &fdir, &stage, vec![S0]);

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=100 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }

    r.sync_once().unwrap();
    assert_converged(&p, &r, S0);

    let conn = meshdb::rusqlite::Connection::open(S0.path(r.follower().dir())).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 100);

    let stats = r.stats();
    assert!(stats.applied_txns > 0, "nothing was applied: {stats:?}");
    println!("{stats:?}");
}

#[test]
fn a_follower_catches_up_after_a_disconnect() {
    // The point of retention: a follower that missed a window resumes from where it
    // stopped, without copying the whole shard again.
    let p = primary(1, 8 * 1024 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = replica(&p, &fdir, &stage, vec![S0]);

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=50 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    r.sync_once().unwrap();
    let after_first = r.stats();
    assert_eq!(
        after_first.bootstraps, 0,
        "the first sync should stream, not bootstrap"
    );

    // The follower is "away" while the primary keeps writing.
    for i in 51..=150 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }

    r.sync_once().unwrap();
    let after_second = r.stats();
    assert_eq!(
        after_second.bootstraps, 0,
        "catching up from retention must not require a bootstrap"
    );
    assert!(after_second.applied_txns > after_first.applied_txns);

    assert_converged(&p, &r, S0);
    let conn = meshdb::rusqlite::Connection::open(S0.path(r.follower().dir())).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 150);
}

#[test]
fn a_follower_too_far_behind_bootstraps_over_the_network() {
    // Retention is deliberately tiny, so the follower falls out of history and must be
    // seeded from a snapshot instead. That is the design working, not a failure.
    let p = primary(1, 64 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = replica(&p, &fdir, &stage, vec![S0]);

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();

    // Enough traffic that the early frames are certainly evicted.
    let pad = "x".repeat(2000);
    for i in 1..=400 {
        p.manager
            .execute_one(
                S0,
                Statement::with_params(
                    "INSERT INTO t VALUES (?1, ?2)",
                    vec![Value::Integer(i), Value::Text(pad.clone())],
                ),
            )
            .unwrap();
    }

    let log_stats = p.frames.stats();
    assert!(
        log_stats.evicted_txns > 0,
        "retention should have evicted history: {log_stats:?}"
    );

    r.sync_once().unwrap();
    let stats = r.stats();
    assert!(
        stats.bootstraps > 0,
        "a follower out of history must bootstrap: {stats:?}"
    );
    println!("{stats:?} / primary log {log_stats:?}");

    assert_converged(&p, &r, S0);
    let conn = meshdb::rusqlite::Connection::open(S0.path(r.follower().dir())).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 400);
}

#[test]
fn bootstrap_then_stream_continues_without_a_second_bootstrap() {
    // After being seeded, the follower must pick up the stream rather than re-copying.
    let p = primary(1, 8 * 1024 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = replica(&p, &fdir, &stage, vec![S0]);

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=80 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    r.sync_once().unwrap();
    let bootstraps = r.stats().bootstraps;

    for i in 81..=200 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    r.sync_once().unwrap();

    assert_eq!(
        r.stats().bootstraps,
        bootstraps,
        "streaming after a bootstrap must not trigger another one"
    );
    assert_converged(&p, &r, S0);
}

#[test]
fn every_shard_replicates_independently() {
    let p = primary(4, 8 * 1024 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let shards: Vec<ShardId> = (0..4).map(ShardId).collect();
    let r = replica(&p, &fdir, &stage, shards.clone());

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for s in &shards {
        for i in 1..=25 {
            p.manager.execute_one(*s, insert(i)).unwrap();
        }
    }

    r.sync_once().unwrap();
    for s in &shards {
        assert_converged(&p, &r, *s);
        assert!(
            r.follower().position(*s).applied_lsn > 0,
            "{s} did not advance"
        );
    }
}

#[test]
fn a_follower_from_another_generation_is_refused() {
    // The epoch guard only means something if the follower claims its *own* epoch. If it
    // asked the primary which epoch to claim and then claimed it, the check would compare
    // the primary's answer against itself and always pass — and a follower holding a copy
    // from an older generation would be fed a newer generation's frames as though they
    // continued its own, which is silent corruption rather than a detected gap.
    use meshdb::net::protocol::{Request, Response};

    let p = primary(1, 8 * 1024 * 1024);
    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=20 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }

    let current = p.manager.epoch().unwrap();
    match c
        .request(Request::Subscribe {
            node: 0,
            shard: 0,
            epoch: current + 1,
            from_lsn: 1,
            max_txns: 16,
        })
        .unwrap()
    {
        Response::NeedsBootstrap { reason, .. } => {
            assert!(reason.contains("epoch"), "{reason}");
        }
        other => panic!("a stale epoch must not be served frames, got {other:?}"),
    }

    // Epoch 0 claims no generation, so it is not a mismatch — a fresh follower is served
    // from retention rather than forced into a needless snapshot.
    match c
        .request(Request::Subscribe {
            node: 0,
            shard: 0,
            epoch: 0,
            from_lsn: 1,
            max_txns: 16,
        })
        .unwrap()
    {
        Response::Frames { txns, epoch, .. } => {
            assert!(!txns.is_empty());
            assert_eq!(epoch, current);
        }
        other => panic!("a fresh follower should be served, got {other:?}"),
    }
}

#[test]
fn a_follower_records_the_epoch_the_frames_came_from() {
    // A fresh follower subscribes with epoch 0. If it recorded the epoch it *asked* with
    // rather than the one the frames carried, its copy would be stamped with a generation
    // it does not belong to — and every later subscription would then look stale.
    let p = primary(1, 8 * 1024 * 1024);
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let r = replica(&p, &fdir, &stage, vec![S0]);

    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=30 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    r.sync_once().unwrap();

    assert_eq!(
        r.follower().position(S0).epoch,
        p.manager.epoch().unwrap(),
        "the follower recorded the wrong generation"
    );

    // And the next sync is therefore accepted as a continuation, not refused as stale.
    for i in 31..=60 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    r.sync_once().unwrap();
    assert_eq!(r.stats().bootstraps, 0);
    assert_converged(&p, &r, S0);
}

#[test]
fn a_freeze_abandoned_by_a_dead_follower_is_released() {
    // A follower that takes a snapshot freeze and then dies must not pin the primary. The
    // freeze suspends checkpointing, so one left held forever grows the WAL without bound —
    // a crashed follower would slowly take the primary down with it.
    use meshdb::net::protocol::{Request, Response};

    let p = primary(1, 8 * 1024 * 1024);
    let mut c = Client::connect(&p.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=50 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }

    // Freeze, then vanish without sending SnapshotEnd — what a crashed follower does.
    {
        let mut dying = Client::connect(&p.addr).unwrap();
        match dying.request(Request::SnapshotBegin { shard: 0 }).unwrap() {
            Response::SnapshotInfo { .. } => {}
            other => panic!("expected snapshot info, got {other:?}"),
        }
    }

    // Wait for the server to notice the connection went away and release what it held.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while p.server.stats().abandoned_freezes == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        p.server.stats().abandoned_freezes,
        1,
        "the abandoned freeze must be released, and counted rather than absorbed silently"
    );

    // The proof it was really released: a second freeze can be taken, and the primary still
    // writes and checkpoints.
    let mut c3 = Client::connect(&p.addr).unwrap();
    match c3.request(Request::SnapshotBegin { shard: 0 }).unwrap() {
        Response::SnapshotInfo { .. } => {}
        other => panic!("the shard is still frozen: {other:?}"),
    }
    match c3.request(Request::SnapshotEnd { shard: 0 }).unwrap() {
        Response::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }

    for i in 51..=100 {
        p.manager.execute_one(S0, insert(i)).unwrap();
    }
    let tmp = TempDir::new().unwrap();
    p.manager.snapshot(S0, &tmp.path().join("x.db")).unwrap();
    assert_eq!(p.server.stats().errors, 0);
}
