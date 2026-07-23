//! Client-held BEGIN/COMMIT: buffered on the server, atomic and durable at COMMIT.

use std::sync::Arc;
use std::time::Duration;

use shardlite::net::{Client, NodeServices, Server, ServerConfig};
use shardlite::replication::{AckTracker, FrameLog, FrameLogConfig};
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::Value;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

fn serve() -> (String, TempDir, Arc<Server>) {
    let dir = TempDir::new().unwrap();
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
    let server = Arc::new(
        Server::bind_with(
            manager,
            NodeServices::default(),
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
    (addr, dir, server)
}

#[test]
fn begin_commit_applies_the_whole_batch() {
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();

    let mut tx = c.begin(0).unwrap();
    assert_eq!(tx.execute("INSERT INTO t VALUES (1, 'a')").unwrap(), 1);
    assert_eq!(tx.execute("INSERT INTO t VALUES (2, 'b')").unwrap(), 2);
    assert_eq!(tx.execute("INSERT INTO t VALUES (3, 'c')").unwrap(), 3);
    let (rows, _) = tx.commit().unwrap();
    assert_eq!(rows, 3, "COMMIT applies every staged write");

    let n = c.query(0, "SELECT count(*) FROM t").unwrap();
    assert_eq!(n.rows[0][0], Value::Integer(3));
}

#[test]
fn nothing_is_visible_before_commit() {
    // The buffer is not applied until COMMIT, so a concurrent reader sees nothing until then.
    let (addr, _dir, _s) = serve();
    let mut setup = Client::connect(&addr).unwrap();
    setup
        .execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let mut writer = Client::connect(&addr).unwrap();
    let mut tx = writer.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (1)").unwrap();
    tx.execute("INSERT INTO t VALUES (2)").unwrap();

    // A separate connection sees nothing yet.
    let mut reader = Client::connect(&addr).unwrap();
    assert_eq!(
        reader.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(0),
        "buffered writes must not be visible before COMMIT"
    );

    tx.commit().unwrap();
    assert_eq!(
        reader.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(2),
        "and all of them appear atomically at COMMIT"
    );
}

#[test]
fn rollback_discards_the_batch() {
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let mut tx = c.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (1)").unwrap();
    tx.execute("INSERT INTO t VALUES (2)").unwrap();
    tx.rollback().unwrap();

    assert_eq!(
        c.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(0),
        "a rolled-back transaction applies nothing"
    );
    // The connection is still usable afterwards.
    c.execute(0, "INSERT INTO t VALUES (9)").unwrap();
    assert_eq!(
        c.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1)
    );
}

#[test]
fn a_failing_statement_rolls_the_whole_transaction_back() {
    // Atomicity: one bad statement in the batch means none of it lands.
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    c.execute(0, "INSERT INTO t VALUES (1)").unwrap();

    let mut tx = c.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (2)").unwrap();
    tx.execute("INSERT INTO t VALUES (1)").unwrap(); // duplicate PK — will fail at COMMIT
    tx.execute("INSERT INTO t VALUES (3)").unwrap();
    let err = tx
        .commit()
        .expect_err("a constraint violation must fail the COMMIT");
    assert!(
        err.to_string().contains("UNIQUE") || err.to_string().contains("constraint"),
        "{err}"
    );

    // Only the pre-transaction row survives; 2 and 3 were rolled back with the failure.
    assert_eq!(
        c.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1),
        "a failed transaction must leave nothing behind"
    );
}

#[test]
fn a_dropped_transaction_rolls_back_and_does_not_wedge_the_connection() {
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    {
        let mut tx = c.begin(0).unwrap();
        tx.execute("INSERT INTO t VALUES (1)").unwrap();
        // tx dropped here without commit — Drop rolls it back.
    }
    // The connection works normally, and nothing was applied.
    c.execute(0, "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(
        c.query(0, "SELECT id FROM t").unwrap().rows,
        vec![vec![Value::Integer(2)]],
        "the abandoned transaction applied nothing; the connection stayed usable"
    );
}

#[test]
fn a_read_inside_a_transaction_is_refused_not_answered_wrongly() {
    // The buffered writes are not applied yet, so a read could not see them. Refusing is
    // honest; answering from stale state would be a silent lie.
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let mut tx = c.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (1)").unwrap();
    // Reach past the guard to issue a raw read on the same connection.
    use shardlite::net::protocol::{ReadConsistency, Request};
    let err = tx
        .raw(Request::Query {
            shard: 0,
            statement: shardlite::storage::exec::Statement::new("SELECT count(*) FROM t"),
            consistency: ReadConsistency::Linearizable,
        })
        .expect_err("a read inside a transaction must be refused");
    assert!(err.to_string().contains("inside a transaction"), "{err}");
}

#[test]
fn a_transaction_is_limited_to_one_shard() {
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let mut tx = c.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (1)").unwrap();
    // A write aimed at another shard within the same transaction must be refused.
    use shardlite::net::protocol::Request;
    let err = tx
        .raw(Request::Execute {
            shard: 1,
            statements: vec![shardlite::storage::exec::Statement::new(
                "INSERT INTO t VALUES (2)",
            )],
        })
        .expect_err("a cross-shard statement in a transaction must be refused");
    assert!(err.to_string().contains("one shard"), "{err}");
}

#[test]
fn the_writer_is_not_pinned_during_a_transaction() {
    // The whole reason for buffering rather than holding a real transaction open: another
    // connection's writes to the same shard must keep flowing while a transaction sits open.
    let (addr, _dir, _s) = serve();
    let mut setup = Client::connect(&addr).unwrap();
    setup
        .execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let mut holder = Client::connect(&addr).unwrap();
    let mut tx = holder.begin(0).unwrap();
    tx.execute("INSERT INTO t VALUES (1000)").unwrap();

    // With the transaction open, a different connection writes the same shard and it lands
    // immediately — the writer was never pinned.
    let mut other = Client::connect(&addr).unwrap();
    let started = std::time::Instant::now();
    other.execute(0, "INSERT INTO t VALUES (1)").unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "an independent write blocked behind an open transaction; the writer is pinned"
    );
    assert_eq!(
        other.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1),
        "the independent write is applied while the transaction is still open"
    );

    tx.commit().unwrap();
    assert_eq!(
        other.query(0, "SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(2)
    );
}

#[test]
fn commit_is_durable_reaching_quorum_before_returning() {
    // The durable ack: with a quorum requiring a follower, COMMIT must not return until that
    // follower holds the write. Without a follower, it stalls; the point is that COMMIT waits
    // on the same quorum path an ordinary write does.
    let dir = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: 4 * 1024 * 1024,
        shard_count: 1,
    }));
    // Quorum of 1 (this node alone) so COMMIT completes without a follower, but through the
    // real ack path.
    let acks = Arc::new(AckTracker::new(1, Duration::from_secs(5)));
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

    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let confirmed_before = acks.stats().confirmed;
    let mut tx = c.begin(0).unwrap();
    for i in 1..=10 {
        tx.execute(format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    tx.commit().unwrap();

    assert!(
        acks.stats().confirmed > confirmed_before,
        "COMMIT must go through the quorum-ack path, like any durable write"
    );
    assert_eq!(S0, ShardId(0));
}

#[test]
fn a_transaction_that_exceeds_the_buffer_cap_is_refused() {
    // The memory guard: a runaway transaction must be refused rather than accumulate
    // unboundedly on the server. Exercised via large blob params, whose real size counts
    // toward the cap (a bug once let them slip through as 16 bytes each).
    let (addr, _dir, _s) = serve();
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB) STRICT")
        .unwrap();

    let mut tx = c.begin(0).unwrap();
    // ~4 MiB per statement; the cap is 64 MiB, so this refuses within ~17 statements rather
    // than growing forever.
    let big = vec![0u8; 4 * 1024 * 1024];
    let mut refused = false;
    for i in 0..40 {
        let stmt = shardlite::storage::exec::Statement::with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            vec![Value::Integer(i), Value::Blob(big.clone())],
        );
        if tx.execute(stmt).is_err() {
            refused = true;
            break;
        }
    }
    assert!(
        refused,
        "a transaction of ~160 MiB must hit the 64 MiB cap and be refused"
    );
}
