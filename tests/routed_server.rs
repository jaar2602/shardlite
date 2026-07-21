//! The "just connect and run SQL" experience over the wire: a client uses only `Client::run` and
//! never names a shard. The server routes each statement — DDL to every shard, a keyed write to its
//! shard, a point read to one shard, any other read fanned out — so the cluster is invisible.

use std::sync::Arc;
use std::time::Duration;

use meshdb::net::{Client, NodeServices, RunResult, Server, ServerConfig};
use meshdb::shard::{ShardConfig, ShardManager};
use meshdb::storage::Value;
use tempfile::TempDir;

fn serve(shards: u32) -> (String, TempDir, Arc<Server>) {
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

fn changed(r: RunResult) -> u64 {
    match r {
        RunResult::Changed { rows_affected, .. } => rows_affected,
        other => panic!("expected a write result, got {other:?}"),
    }
}

fn rows(r: RunResult) -> Vec<Vec<Value>> {
    match r {
        RunResult::Rows(q) => q.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn a_client_runs_sql_without_knowing_about_shards() {
    // 16 shards behind the server; the client below never once mentions a shard number.
    let (addr, _dir, _s) = serve(16);
    let mut c = Client::connect(&addr).unwrap();

    // CREATE TABLE fans out to every shard, and its PRIMARY KEY becomes the shard key — no
    // `shardkey` step, no shard number.
    c.run("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT")
        .unwrap();

    // Plain INSERTs. Each routes to the shard its key hashes to.
    for i in 0..200 {
        assert_eq!(
            changed(
                c.run(&format!(
                    "INSERT INTO users (id, name) VALUES ('user-{i}', 'n{i}')"
                ))
                .unwrap()
            ),
            1
        );
    }

    // A fan-out read: count is correct across all shards.
    assert_eq!(
        rows(c.run("SELECT count(*) FROM users").unwrap()),
        vec![vec![Value::Integer(200)]]
    );

    // A point read finds its row — routed to the one shard that holds it.
    assert_eq!(
        rows(c.run("SELECT name FROM users WHERE id = 'user-7'").unwrap()),
        vec![vec![Value::Text("n7".into())]]
    );

    // A point UPDATE and DELETE reach the right shard.
    assert_eq!(
        changed(
            c.run("UPDATE users SET name = 'changed' WHERE id = 'user-7'")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        rows(c.run("SELECT name FROM users WHERE id = 'user-7'").unwrap()),
        vec![vec![Value::Text("changed".into())]]
    );
    assert_eq!(
        changed(c.run("DELETE FROM users WHERE id = 'user-42'").unwrap()),
        1
    );
    assert_eq!(
        rows(c.run("SELECT count(*) FROM users").unwrap()),
        vec![vec![Value::Integer(199)]]
    );
}

#[test]
fn a_multi_row_insert_is_split_and_every_row_lands() {
    let (addr, _dir, _s) = serve(16);
    let mut c = Client::connect(&addr).unwrap();
    c.run("CREATE TABLE t (id TEXT PRIMARY KEY, v INTEGER) STRICT")
        .unwrap();

    // One statement, rows destined for different shards — the server splits it.
    let affected = changed(
        c.run(
            "INSERT INTO t (id, v) VALUES \
             ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('f', 6)",
        )
        .unwrap(),
    );
    assert_eq!(affected, 6, "every row of the split insert must be applied");
    assert_eq!(
        rows(c.run("SELECT count(*) FROM t").unwrap()),
        vec![vec![Value::Integer(6)]]
    );
    // Each row is findable via its own point read.
    for id in ["a", "b", "c", "d", "e", "f"] {
        assert_eq!(
            rows(
                c.run(&format!("SELECT v FROM t WHERE id = '{id}'"))
                    .unwrap()
            )
            .len(),
            1,
            "row {id} went missing after the split"
        );
    }
}

#[test]
fn an_unpinned_delete_touches_every_shard() {
    let (addr, _dir, _s) = serve(16);
    let mut c = Client::connect(&addr).unwrap();
    c.run("CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT) STRICT")
        .unwrap();
    for i in 0..60 {
        c.run(&format!("INSERT INTO t (id, tag) VALUES ({i}, 'x')"))
            .unwrap();
    }
    // No shard-key predicate: the server must apply the DELETE on every shard, not just one.
    let deleted = changed(c.run("DELETE FROM t WHERE tag = 'x'").unwrap());
    assert_eq!(deleted, 60, "an unpinned delete must reach all shards");
    assert_eq!(
        rows(c.run("SELECT count(*) FROM t").unwrap()),
        vec![vec![Value::Integer(0)]]
    );
}
