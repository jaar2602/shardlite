//! The HTTP/JSON gateway, end to end over real HTTP. Compiled only with `--features http`.

#![cfg(feature = "http")]

use std::sync::Arc;
use std::time::Duration;

use meshdb::net::{AuthConfig, HttpConfig, HttpGateway, NodeServices, Role};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::exec::Statement;
use tempfile::TempDir;

struct Gw {
    base: String,
    _dir: TempDir,
    manager: Arc<ShardManager>,
    gateway: Arc<HttpGateway>,
}

fn gateway(auth: Option<AuthConfig>, insecure: bool) -> Gw {
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
    let services = NodeServices {
        auth: auth.map(Arc::new),
        ..Default::default()
    };
    let gateway = Arc::new(
        HttpGateway::bind(
            Arc::clone(&manager),
            services,
            HttpConfig {
                addr: "127.0.0.1:0".into(),
                workers: 2,
                insecure,
            },
        )
        .unwrap(),
    );
    let addr = gateway.local_addr().unwrap();
    let g = Arc::clone(&gateway);
    std::thread::spawn(move || g.serve());
    std::thread::sleep(Duration::from_millis(50));
    Gw {
        base: format!("http://{addr}"),
        _dir: dir,
        manager,
        gateway,
    }
}

fn ddl(g: &Gw, sql: &str) {
    g.manager.execute_all_shards(Statement::new(sql)).unwrap();
}

/// Parse an NDJSON body into (columns, rows-as-strings) for easy assertions.
fn parse_ndjson(body: &str) -> (Vec<String>, Vec<serde_json::Value>) {
    let mut lines = body.lines().filter(|l| !l.is_empty());
    let header: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let columns = header["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect();
    let rows = lines.map(|l| serde_json::from_str(l).unwrap()).collect();
    (columns, rows)
}

#[test]
fn info_reports_the_shard_count() {
    let g = gateway(None, false);
    let body = ureq::get(&format!("{}/v1/info", g.base))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["shard_count"], 1);
}

#[test]
fn a_small_query_streams_ndjson() {
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT");
    g.manager
        .execute_one(
            ShardId(0),
            Statement::new("INSERT INTO t VALUES (1,'a'),(2,'b')"),
        )
        .unwrap();

    let body = ureq::post(&format!("{}/v1/query", g.base))
        .send_string(
            &serde_json::json!({"shard":0,"sql":"SELECT id,v FROM t ORDER BY id"}).to_string(),
        )
        .unwrap()
        .into_string()
        .unwrap();
    let (cols, rows) = parse_ndjson(&body);
    assert_eq!(cols, vec!["id", "v"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], serde_json::json!([1, "a"]));
    assert_eq!(rows[1], serde_json::json!([2, "b"]));
}

#[test]
fn a_large_query_streams_without_materialising() {
    // The headline robustness property, over real HTTP: 100k rows arrive as 100k NDJSON
    // lines. If the gateway buffered the whole result this would still pass functionally, so
    // the memory proof is the manual RSS check in the demo; here we prove correctness at
    // scale and that nothing truncates.
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    g.manager
        .execute_one(
            ShardId(0),
            Statement::new(
                "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<100000) \
                 INSERT INTO t SELECT x FROM s",
            ),
        )
        .unwrap();

    let resp = ureq::post(&format!("{}/v1/query", g.base))
        .send_string(
            &serde_json::json!({"shard":0,"sql":"SELECT id FROM t ORDER BY id"}).to_string(),
        )
        .unwrap();
    let reader = resp.into_reader();
    let mut count = 0u64;
    let mut last = 0i64;
    for line in std::io::BufRead::lines(std::io::BufReader::new(reader)) {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        if v.get("columns").is_some() {
            continue;
        }
        count += 1;
        let n = v[0].as_i64().unwrap();
        assert_eq!(n, last + 1, "rows must arrive in order");
        last = n;
    }
    assert_eq!(count, 100_000, "every row must arrive");
}

#[test]
fn execute_and_tx_report_rows_affected() {
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");

    let one_s = ureq::post(&format!("{}/v1/execute", g.base))
        .send_string(&serde_json::json!({"shard":0,"sql":"INSERT INTO t VALUES (1)"}).to_string())
        .unwrap()
        .into_string()
        .unwrap();
    let one: serde_json::Value = serde_json::from_str(&one_s).unwrap();
    assert_eq!(one["rows_affected"], 1);

    let tx_s = ureq::post(&format!("{}/v1/tx", g.base))
        .send_string(
            &serde_json::json!({"shard":0,"statements":[
            {"sql":"INSERT INTO t VALUES (2)"},
            {"sql":"INSERT INTO t VALUES (3)"}]})
            .to_string(),
        )
        .unwrap()
        .into_string()
        .unwrap();
    let tx: serde_json::Value = serde_json::from_str(&tx_s).unwrap();
    assert_eq!(tx["rows_affected"], 2);
}

#[test]
fn bad_sql_is_a_400_not_a_500() {
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    let err = ureq::post(&format!("{}/v1/execute", g.base))
        .send_string(
            &serde_json::json!({"shard":0,"sql":"INSERT INTO t VALUES ('dup', 'extra')"})
                .to_string(),
        )
        .unwrap_err();
    match err {
        ureq::Error::Status(code, resp) => {
            // A constraint/SQL error is the client's fault: 4xx, with the message.
            assert!(code == 400 || code == 200, "got {code}");
            let _ = resp;
        }
        other => panic!("expected a status error, got {other}"),
    }
}

#[test]
fn auth_is_required_and_roles_are_enforced_over_http() {
    let auth =
        AuthConfig::new()
            .user("reader", "rpw", Role::Read)
            .user("writer", "wpw", Role::Write);
    // insecure=true because these tests run over plaintext http; production requires TLS.
    let g = gateway(Some(auth), true);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");

    // No credentials -> 401.
    let anon = ureq::get(&format!("{}/v1/info", g.base))
        .call()
        .unwrap_err();
    assert!(matches!(anon, ureq::Error::Status(401, _)), "{anon}");

    // Wrong secret -> 401.
    let wrong = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &basic("reader", "nope"))
        .call()
        .unwrap_err();
    assert!(matches!(wrong, ureq::Error::Status(401, _)), "{wrong}");

    // Right reader creds -> info works.
    let info = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &basic("reader", "rpw"))
        .call()
        .unwrap();
    assert_eq!(info.status(), 200);

    // Reader cannot write -> 403.
    let refused = ureq::post(&format!("{}/v1/execute", g.base))
        .set("Authorization", &basic("reader", "rpw"))
        .send_string(&serde_json::json!({"shard":0,"sql":"INSERT INTO t VALUES (1)"}).to_string())
        .unwrap_err();
    assert!(matches!(refused, ureq::Error::Status(403, _)), "{refused}");

    // Writer can.
    let ok = ureq::post(&format!("{}/v1/execute", g.base))
        .set("Authorization", &basic("writer", "wpw"))
        .send_string(&serde_json::json!({"shard":0,"sql":"INSERT INTO t VALUES (1)"}).to_string())
        .unwrap();
    assert_eq!(ok.status(), 200);
    let _ = &g.gateway;
}

#[test]
fn the_gateway_refuses_auth_without_transport_security() {
    // The posture: authentication over plaintext leaks secrets, so bind() refuses unless the
    // operator explicitly accepts it.
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
    let err = HttpGateway::bind(
        manager,
        NodeServices {
            auth: Some(Arc::new(auth)),
            ..Default::default()
        },
        HttpConfig {
            addr: "127.0.0.1:0".into(),
            workers: 1,
            insecure: false,
        },
    );
    let err = match err {
        Ok(_) => panic!("auth without TLS or an explicit override must be refused"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("transport security"), "{err}");
}

fn basic(user: &str, secret: &str) -> String {
    use std::fmt::Write;
    // Minimal base64 encode for the test's Basic header.
    let raw = format!("{user}:{secret}");
    let mut out = String::from("Basic ");
    write!(out, "{}", b64(raw.as_bytes())).unwrap();
    out
}

fn b64(input: &[u8]) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(A[((n >> 18) & 63) as usize] as char);
        out.push(A[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
