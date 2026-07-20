//! Divergence detection against a real replication stream.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use meshdb::net::{Client, NodeServices, Replica, ReplicaConfig, Server, ServerConfig};
use meshdb::replication::{Follower, FrameLog, FrameLogConfig};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::Statement;
use meshdb::storage::verify::{hash_file, hex};
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

struct Pair {
    primary: Arc<ShardManager>,
    replica: Arc<Replica>,
    _dirs: (TempDir, TempDir, TempDir),
    _server: Arc<Server>,
}

fn pair() -> Pair {
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: 8 * 1024 * 1024,
        shard_count: 1,
    }));
    let primary = Arc::new(
        ShardManager::open_with_sink(
            pdir.path(),
            ShardConfig {
                shard_count: 1,
                capture: true,
                ..ShardConfig::floor()
            },
            Some(frames.clone()),
        )
        .unwrap(),
    );
    let server = Arc::new(
        Server::bind_with(
            Arc::clone(&primary),
            NodeServices {
                frames: Some(frames),
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

    let replica = Arc::new(Replica::new(
        ReplicaConfig {
            primary_addr: addr,
            shards: vec![S0],
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: 2,
            credentials: None,
        },
        Arc::new(Follower::open(fdir.path()).unwrap()),
    ));
    Pair {
        primary,
        replica,
        _dirs: (pdir, fdir, stage),
        _server: server,
    }
}

/// Wait until the follower has applied everything the primary has committed.
///
/// A fixed sleep is a guess about how fast the machine is; under load it is the wrong guess,
/// and the failure it produces is an unreproducible one-off.
fn await_caught_up(p: &Pair, within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        let target = p.primary.last_lsn(S0);
        let have = p.replica.follower().position(S0).applied_lsn;
        if target > 0 && have >= target {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("replica stuck at {have} of {target}");
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Hash the follower safely: stand its pull loop down first, because a hash taken while it
/// is mid-apply describes a state that never existed.
fn follower_hash(p: &Pair) -> [u8; 32] {
    p.replica.pause();
    assert!(
        p.replica.wait_idle(Duration::from_secs(5)),
        "the pull loop never came to rest"
    );
    let h = hash_file(&S0.path(p.replica.follower().dir())).unwrap();
    p.replica.resume();
    h
}

#[test]
fn a_correctly_replicated_shard_matches_its_primary() {
    let p = pair();
    let stop = p.replica.stop_handle();
    let r = Arc::clone(&p.replica);
    std::thread::spawn(move || {
        let _ = r.run();
    });

    let mut c = Client::connect(&format!("{}", p._server.local_addr().unwrap())).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    for i in 1..=50 {
        p.primary
            .execute_one(
                S0,
                Statement::with_params(
                    "INSERT INTO t VALUES (?1, ?2)",
                    vec![Value::Integer(i), Value::Text(format!("v{i}"))],
                ),
            )
            .unwrap();
    }
    assert!(
        await_caught_up(&p, Duration::from_secs(10)),
        "never caught up"
    );

    let a = p.primary.content_hash(S0).unwrap();
    let b = follower_hash(&p);
    assert_eq!(
        hex(&a),
        hex(&b),
        "a correctly replicated shard must hash identically to its primary"
    );
    println!("both nodes at {}", hex(&a));
    stop.store(true, Ordering::Relaxed);
}

#[test]
fn a_follower_that_missed_frames_is_detected() {
    // The failure the whole file exists for. The follower's pages are individually valid, so
    // integrity_check passes and reads succeed — they just return the wrong rows.
    let p = pair();
    let stop = p.replica.stop_handle();
    let r = Arc::clone(&p.replica);
    std::thread::spawn(move || {
        let _ = r.run();
    });

    let mut c = Client::connect(&format!("{}", p._server.local_addr().unwrap())).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        p.primary
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }
    assert!(
        await_caught_up(&p, Duration::from_secs(10)),
        "never caught up"
    );
    assert_eq!(p.primary.content_hash(S0).unwrap(), follower_hash(&p));

    // Now let the primary move on while the follower is stopped — the shape of a follower
    // that silently stopped receiving.
    stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(100));
    for i in 21..=30 {
        p.primary
            .execute_one(S0, Statement::new(format!("INSERT INTO t VALUES ({i})")))
            .unwrap();
    }

    let a = p.primary.content_hash(S0).unwrap();
    let b = follower_hash(&p);
    assert_ne!(
        hex(&a),
        hex(&b),
        "a follower missing frames must be detected"
    );

    // And the follower still looks perfectly healthy to SQLite, which is the point.
    let conn = meshdb::rusqlite::Connection::open(S0.path(p.replica.follower().dir())).unwrap();
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        check, "ok",
        "the diverged follower passes integrity_check — which is why this detector exists"
    );
    println!("primary {} vs follower {}", hex(&a), hex(&b));
}

#[test]
fn a_checkpoint_does_not_look_like_divergence() {
    // Primary and follower checkpoint on their own schedules, so their files differ in
    // layout constantly. A detector that flagged that would be ignored within a week.
    let p = pair();
    let stop = p.replica.stop_handle();
    let r = Arc::clone(&p.replica);
    std::thread::spawn(move || {
        let _ = r.run();
    });

    let mut c = Client::connect(&format!("{}", p._server.local_addr().unwrap())).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT")
        .unwrap();
    let pad = "x".repeat(500);
    for i in 1..=40 {
        p.primary
            .execute_one(
                S0,
                Statement::with_params(
                    "INSERT INTO t VALUES (?1, ?2)",
                    vec![Value::Integer(i), Value::Text(pad.clone())],
                ),
            )
            .unwrap();
    }
    assert!(
        await_caught_up(&p, Duration::from_secs(10)),
        "never caught up"
    );

    let before = p.primary.content_hash(S0).unwrap();
    assert_eq!(before, follower_hash(&p));

    // Force the primary to checkpoint, rewriting its file layout entirely.
    p.primary.vacuum(S0).unwrap();

    assert_eq!(
        hex(&p.primary.content_hash(S0).unwrap()),
        hex(&before),
        "VACUUM changed layout, not data"
    );
    stop.store(true, Ordering::Relaxed);
}

#[test]
fn refusing_to_hash_a_followed_shard() {
    // A hash taken while the replication path is rewriting the file describes a state that
    // never existed, so it must be refused rather than quietly wrong.
    let p = pair();
    p.primary.follow(S0).unwrap();
    let err = p
        .primary
        .content_hash(S0)
        .expect_err("hashing a followed shard must be refused");
    assert!(err.to_string().contains("followed"), "{err}");
}
