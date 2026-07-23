//! The network transport, end to end.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use shardlite::error::Error;
use shardlite::net::{Client, Server, ServerConfig, ShardOutcome};
use shardlite::shard::{ShardConfig, ShardManager};
use shardlite::storage::Value;
use shardlite::storage::exec::Statement;
use tempfile::TempDir;

/// Start a server on an ephemeral port and return its address plus a shutdown handle.
fn serve(shards: u32, cfg: ServerConfig) -> (String, TempDir, Arc<Server>) {
    let dir = TempDir::new().unwrap();
    let manager = Arc::new(
        ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: shards,
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
                ..cfg
            },
        )
        .unwrap(),
    );
    let addr = server.local_addr().unwrap().to_string();

    let s = Arc::clone(&server);
    std::thread::spawn(move || {
        let _ = s.serve();
    });
    // Give the listener a moment to start accepting.
    std::thread::sleep(Duration::from_millis(50));
    (addr, dir, server)
}

#[test]
fn a_client_can_write_and_read_back() {
    let (addr, _dir, _s) = serve(4, ServerConfig::default());
    let mut c = Client::connect(&addr).unwrap();
    assert_eq!(c.shard_count(), 4);

    let outcomes = c
        .execute_all("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    assert_eq!(outcomes.len(), 4);
    assert!(outcomes.iter().all(|(_, o)| *o == ShardOutcome::Ok));

    // The server routes the key, so the client never reimplements the hash.
    let (affected, _) = c
        .put(
            b"alice",
            "INSERT INTO t VALUES (?1, ?2)",
            vec![Value::Text("alice".into()), Value::Integer(7)],
        )
        .unwrap();
    assert_eq!(affected, 1);

    let shard = c.route(b"alice").unwrap();
    let rows = c
        .query(
            shard,
            Statement::with_params(
                "SELECT n FROM t WHERE k = ?1",
                vec![Value::Text("alice".into())],
            ),
        )
        .unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Integer(7)]]);
}

#[test]
fn reads_fan_out_across_shards_over_the_wire() {
    let (addr, _dir, _s) = serve(8, ServerConfig::default());
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();

    for i in 0..40i64 {
        let key = format!("key-{i}");
        c.put(
            key.as_bytes(),
            "INSERT INTO t VALUES (?1, ?2)",
            vec![Value::Text(key.clone()), Value::Integer(i)],
        )
        .unwrap();
    }

    let count = c.query_all("SELECT count(*) FROM t").unwrap();
    assert_eq!(count.rows[0][0], Value::Integer(40));

    let sum = c.query_all("SELECT sum(n) FROM t").unwrap();
    assert_eq!(sum.rows[0][0], Value::Integer((0..40).sum()));

    let top = c
        .query_all("SELECT n FROM t ORDER BY n DESC LIMIT 3")
        .unwrap();
    assert_eq!(
        top.rows,
        vec![
            vec![Value::Integer(39)],
            vec![Value::Integer(38)],
            vec![Value::Integer(37)]
        ]
    );
}

#[test]
fn an_uncombinable_query_is_refused_over_the_wire_too() {
    // The refusal must survive the transport intact, complete with its explanation —
    // a client that received a bare "error" could not act on it.
    let (addr, _dir, _s) = serve(4, ServerConfig::default());
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();

    let err = c.query_all("SELECT group_concat(k) FROM t").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("GROUP_CONCAT"), "{msg}");
    assert!(msg.contains("order"), "the explanation is missing: {msg}");
}

#[test]
fn a_rejected_statement_is_a_result_not_a_transport_failure() {
    let (addr, _dir, _s) = serve(1, ServerConfig::default());
    let mut c = Client::connect(&addr).unwrap();
    c.execute_all("CREATE TABLE t (k TEXT PRIMARY KEY) STRICT")
        .unwrap();
    c.execute(0, "INSERT INTO t VALUES ('dup')").unwrap();

    // A constraint violation is deterministic — it must not look like the connection broke.
    let err = c.execute(0, "INSERT INTO t VALUES ('dup')").unwrap_err();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "a rejection should not surface as a protocol error: {err:?}"
    );
    assert!(err.to_string().contains("UNIQUE"), "{err}");

    // And the connection is still usable afterwards.
    let n = c.query_all("SELECT count(*) FROM t").unwrap();
    assert_eq!(n.rows[0][0], Value::Integer(1));
}

#[test]
fn many_clients_work_concurrently() {
    let (addr, _dir, server) = serve(8, ServerConfig::default());
    let mut setup = Client::connect(&addr).unwrap();
    setup
        .execute_all("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    drop(setup);

    const CLIENTS: usize = 12;
    const PER_CLIENT: i64 = 20;
    let failures = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for c in 0..CLIENTS {
            let addr = addr.clone();
            let failures = Arc::clone(&failures);
            s.spawn(move || {
                let mut client = match Client::connect(&addr) {
                    Ok(c) => c,
                    Err(_) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };
                for i in 0..PER_CLIENT {
                    let key = format!("c{c}-{i}");
                    if client
                        .put(
                            key.as_bytes(),
                            "INSERT INTO t VALUES (?1, ?2)",
                            vec![Value::Text(key.clone()), Value::Integer(i)],
                        )
                        .is_err()
                    {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(failures.load(Ordering::Relaxed), 0);

    let mut c = Client::connect(&addr).unwrap();
    let n = c.query_all("SELECT count(*) FROM t").unwrap();
    assert_eq!(
        n.rows[0][0],
        Value::Integer(CLIENTS as i64 * PER_CLIENT),
        "every write from every client must be present"
    );

    let stats = server.stats();
    assert!(stats.accepted >= CLIENTS as u64);
    assert_eq!(stats.errors, 0);
}

#[test]
fn the_connection_cap_sheds_load_and_counts_it() {
    // The cap is the load-shedding boundary that actually binds, so a refusal must be
    // countable rather than looking like a client that gave up.
    let (addr, _dir, server) = serve(
        1,
        ServerConfig {
            max_connections: 2,
            ..ServerConfig::default()
        },
    );

    // Hold the cap open.
    let _a = Client::connect(&addr).unwrap();
    let _b = Client::connect(&addr).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // The third is refused — either at connect or on its first request, depending on when
    // the server notices; both are the same shedding.
    let refused = match Client::connect(&addr) {
        Err(e) => e.to_string().contains("too many connections"),
        Ok(mut c) => c
            .query_all("SELECT 1")
            .err()
            .map(|e| e.to_string().contains("too many connections"))
            .unwrap_or(false),
    };
    assert!(refused, "the third connection should have been shed");
    assert!(
        server.stats().refused_at_capacity > 0,
        "shedding must be counted, not silent"
    );
}

#[test]
fn a_version_mismatch_says_so_plainly() {
    // A mismatched peer should be told exactly that, rather than failing later as a
    // confusing decode error.
    use shardlite::net::protocol::{Request, Response, read_message, write_message};
    use std::net::TcpStream;

    let (addr, _dir, _s) = serve(1, ServerConfig::default());
    let stream = TcpStream::connect(&addr).unwrap();
    let mut w = std::io::BufWriter::new(stream.try_clone().unwrap());
    let mut r = std::io::BufReader::new(stream);

    write_message(
        &mut w,
        &Request::Hello {
            version: 9999,
            client: "wrong-version".into(),
        },
    )
    .unwrap();

    match read_message::<Response, _>(&mut r).unwrap() {
        Response::Error { message, retryable } => {
            assert!(message.contains("9999"), "{message}");
            assert!(!retryable, "a version mismatch will not fix itself");
        }
        other => panic!("expected a version error, got {other:?}"),
    }
}

#[test]
fn subscribing_without_capture_says_why() {
    // Capture defaults to off, so a follower must be told that rather than receiving an
    // empty stream that looks like "nothing has happened yet".
    use shardlite::net::protocol::{Request, Response, read_message, write_message};
    use std::net::TcpStream;

    let (addr, _dir, _s) = serve(1, ServerConfig::default());
    let stream = TcpStream::connect(&addr).unwrap();
    let mut w = std::io::BufWriter::new(stream.try_clone().unwrap());
    let mut r = std::io::BufReader::new(stream);

    write_message(
        &mut w,
        &Request::Hello {
            version: shardlite::net::protocol::PROTOCOL_VERSION,
            client: "sub".into(),
        },
    )
    .unwrap();
    let _: Response = read_message(&mut r).unwrap();

    write_message(
        &mut w,
        &Request::Subscribe {
            node: 0,
            shard: 0,
            epoch: 1,
            from_lsn: 1,
            max_txns: 16,
        },
    )
    .unwrap();

    match read_message::<Response, _>(&mut r).unwrap() {
        Response::Error { message, .. } => {
            assert!(message.contains("capture is off"), "{message}");
        }
        other => panic!("expected an explanation, got {other:?}"),
    }
}
