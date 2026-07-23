//! The JSON-over-TCP protocol, end to end. Compiled only with `--features json-tcp`.

#![cfg(feature = "json-tcp")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use shardlite::net::{AuthConfig, JsonTcpConfig, JsonTcpServer, NodeServices, Role};
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::exec::Statement;
use tempfile::TempDir;

struct Server {
    addr: String,
    manager: Arc<ShardManager>,
    _dir: TempDir,
    _server: Arc<JsonTcpServer>,
}

fn serve(auth: Option<AuthConfig>, insecure: bool) -> Server {
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
        JsonTcpServer::bind(
            Arc::clone(&manager),
            NodeServices {
                auth: auth.map(Arc::new),
                ..Default::default()
            },
            JsonTcpConfig {
                addr: "127.0.0.1:0".into(),
                insecure,
            },
        )
        .unwrap(),
    );
    let addr = server.local_addr().unwrap().to_string();
    let s = Arc::clone(&server);
    std::thread::spawn(move || s.serve());
    std::thread::sleep(Duration::from_millis(50));
    Server {
        addr,
        manager,
        _dir: dir,
        _server: server,
    }
}

/// A minimal framed JSON client for the tests.
struct Conn(TcpStream);
impl Conn {
    fn connect(addr: &str) -> Self {
        Conn(TcpStream::connect(addr).unwrap())
    }
    fn send(&mut self, v: serde_json::Value) {
        let bytes = serde_json::to_vec(&v).unwrap();
        self.0
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .unwrap();
        self.0.write_all(&bytes).unwrap();
        self.0.flush().unwrap();
    }
    fn recv(&mut self) -> serde_json::Value {
        let mut len = [0u8; 4];
        self.0.read_exact(&mut len).unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        self.0.read_exact(&mut body).unwrap();
        serde_json::from_slice(&body).unwrap()
    }
}

fn ddl(s: &Server, sql: &str) {
    s.manager.execute_all_shards(Statement::new(sql)).unwrap();
}

#[test]
fn info_and_a_small_query() {
    let s = serve(None, false);
    ddl(&s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT");
    s.manager
        .execute_one(
            ShardId(0),
            Statement::new("INSERT INTO t VALUES (1,'a'),(2,'b')"),
        )
        .unwrap();

    let mut c = Conn::connect(&s.addr);
    c.send(serde_json::json!({ "op": "info" }));
    assert_eq!(c.recv()["result"]["shard_count"], 1);

    c.send(
        serde_json::json!({ "op": "query", "shard": 0, "sql": "SELECT id,v FROM t ORDER BY id" }),
    );
    assert_eq!(c.recv()["columns"], serde_json::json!(["id", "v"]));
    assert_eq!(c.recv()["row"], serde_json::json!([1, "a"]));
    assert_eq!(c.recv()["row"], serde_json::json!([2, "b"]));
    assert_eq!(c.recv()["end"], true);
}

#[test]
fn a_large_query_streams_frame_by_frame() {
    let s = serve(None, false);
    ddl(&s, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    s.manager
        .execute_one(
            ShardId(0),
            Statement::new(
                "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<100000) \
                 INSERT INTO t SELECT x FROM r",
            ),
        )
        .unwrap();

    let mut c = Conn::connect(&s.addr);
    c.send(serde_json::json!({ "op": "query", "shard": 0, "sql": "SELECT id FROM t ORDER BY id" }));
    assert_eq!(c.recv()["columns"], serde_json::json!(["id"]));
    let mut count = 0i64;
    let mut last = 0i64;
    loop {
        let f = c.recv();
        if f.get("end").is_some() {
            break;
        }
        let n = f["row"][0].as_i64().unwrap();
        assert_eq!(n, last + 1);
        last = n;
        count += 1;
    }
    assert_eq!(count, 100_000);
}

#[test]
fn execute_tx_and_a_persistent_connection() {
    // One connection, many operations — the point of a persistent socket.
    let s = serve(None, false);
    let mut c = Conn::connect(&s.addr);

    // DDL via the op, then writes, then a read — all on the same connection.
    s.manager
        .execute_all_shards(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
        ))
        .unwrap();

    c.send(serde_json::json!({ "op": "execute", "shard": 0, "sql": "INSERT INTO t VALUES (1)" }));
    assert_eq!(c.recv()["result"]["rows_affected"], 1);

    c.send(serde_json::json!({ "op": "tx", "shard": 0, "statements": [
        {"sql": "INSERT INTO t VALUES (2)"}, {"sql": "INSERT INTO t VALUES (3)"}] }));
    assert_eq!(c.recv()["result"]["rows_affected"], 2);

    c.send(serde_json::json!({ "op": "query", "shard": 0, "sql": "SELECT count(*) FROM t" }));
    let _cols = c.recv();
    assert_eq!(c.recv()["row"], serde_json::json!([3]));
    assert_eq!(c.recv()["end"], true);
}

#[test]
fn a_rejected_statement_carries_a_status() {
    let s = serve(None, false);
    ddl(&s, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    s.manager
        .execute_one(ShardId(0), Statement::new("INSERT INTO t VALUES (1)"))
        .unwrap();

    let mut c = Conn::connect(&s.addr);
    c.send(serde_json::json!({ "op": "execute", "shard": 0, "sql": "INSERT INTO t VALUES (1)" }));
    let r = c.recv();
    assert!(
        r.get("error").is_some(),
        "a duplicate key must be an error: {r}"
    );
    assert_eq!(r["status"], 400);
}

#[test]
fn auth_gates_the_connection_and_roles_are_enforced() {
    let auth =
        AuthConfig::new()
            .user("reader", "rpw", Role::Read)
            .user("writer", "wpw", Role::Write);
    let s = serve(Some(auth), true);
    ddl(&s, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");

    // Before auth, any op is refused.
    let mut c = Conn::connect(&s.addr);
    c.send(serde_json::json!({ "op": "info" }));
    assert_eq!(c.recv()["status"], 401);

    // Wrong secret closes the connection.
    c.send(serde_json::json!({ "op": "auth", "name": "reader", "secret": "nope" }));
    assert_eq!(c.recv()["status"], 401);

    // A fresh connection authenticates and reads.
    let mut c = Conn::connect(&s.addr);
    c.send(serde_json::json!({ "op": "auth", "name": "reader", "secret": "rpw" }));
    assert_eq!(c.recv()["result"]["ok"], true);
    c.send(serde_json::json!({ "op": "info" }));
    assert!(c.recv()["result"]["shard_count"].is_number());

    // Reader cannot write.
    c.send(serde_json::json!({ "op": "execute", "shard": 0, "sql": "INSERT INTO t VALUES (1)" }));
    assert_eq!(c.recv()["status"], 403);

    // Writer can.
    let mut w = Conn::connect(&s.addr);
    w.send(serde_json::json!({ "op": "auth", "name": "writer", "secret": "wpw" }));
    assert_eq!(w.recv()["result"]["ok"], true);
    w.send(serde_json::json!({ "op": "execute", "shard": 0, "sql": "INSERT INTO t VALUES (1)" }));
    assert_eq!(w.recv()["result"]["rows_affected"], 1);
}

#[test]
fn the_server_refuses_auth_without_transport_security() {
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
    let auth = AuthConfig::new().user("a", "b", Role::Admin);
    let err = JsonTcpServer::bind(
        manager,
        NodeServices {
            auth: Some(Arc::new(auth)),
            ..Default::default()
        },
        JsonTcpConfig {
            addr: "127.0.0.1:0".into(),
            insecure: false,
        },
    );
    assert!(err.is_err(), "auth without insecure must be refused");
    assert!(
        err.err()
            .unwrap()
            .to_string()
            .contains("transport security")
    );
}
