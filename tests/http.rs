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
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["forwarding"], false);
}

#[test]
fn observability_contracts_are_versioned_and_truthful_for_standalone() {
    let g = gateway(None, false);
    for endpoint in ["meta", "health", "topology", "shards"] {
        let response = ureq::get(&format!("{}/v1/{endpoint}", g.base))
            .call()
            .unwrap();
        assert_eq!(response.status(), 200, "{endpoint}");
        let value: serde_json::Value =
            serde_json::from_str(&response.into_string().unwrap()).unwrap();
        match endpoint {
            "meta" => {
                assert_eq!(value["api_version"], 1);
                assert_eq!(value["clustered"], false);
                assert_eq!(value["capabilities"]["topology"], true);
            }
            "health" => {
                assert_eq!(value["status"], "healthy");
                assert_eq!(value["checks"]["consensus"]["status"], "not_applicable");
            }
            "topology" => {
                assert_eq!(value["api_version"], 1);
                assert_eq!(value["clustered"], false);
                assert!(value["observed_at_ms"].as_u64().unwrap() > 0);
            }
            "shards" => {
                assert_eq!(value["api_version"], 1);
                assert_eq!(value["shards"].as_array().unwrap().len(), 1);
                assert_eq!(value["shards"][0]["local_role"], "primary");
            }
            _ => unreachable!(),
        }
    }
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

#[test]
fn bearer_token_authenticates_the_same_as_basic() {
    // The alternative scheme: Bearer base64(name:secret). Same verification, no browser
    // login prompt — the option a programmatic client picks.
    let auth = AuthConfig::new().user("api", "tok", Role::Read);
    let g = gateway(Some(auth), true);

    let bearer = format!("Bearer {}", b64(b"api:tok"));
    let ok = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &bearer)
        .call()
        .unwrap();
    assert_eq!(ok.status(), 200);

    // A wrong token under Bearer is refused just the same.
    let bad = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &format!("Bearer {}", b64(b"api:wrong")))
        .call()
        .unwrap_err();
    assert!(matches!(bad, ureq::Error::Status(401, _)), "{bad}");
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

#[test]
fn stats_schema_route_and_execute_all_endpoints() {
    let g = gateway(None, false);
    // Create the table via the HTTP execute_all endpoint (the rolling, version-bumping DDL
    // path), so this also exercises /v1/execute_all end to end.
    let all: serde_json::Value = serde_json::from_str(
        &ureq::post(&format!("{}/v1/execute_all", g.base))
            .send_string(
                &serde_json::json!({"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"})
                    .to_string(),
            )
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert!(
        all["shards"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["ok"] == true)
    );

    // stats
    let stats: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/stats", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert!(stats["reader"]["threads"].as_u64().unwrap() >= 1);
    assert!(stats["http"]["requests"].as_u64().unwrap() >= 1);

    // schema version
    let sch: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/schema/0", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sch["shard"], 0);
    assert_eq!(
        sch["schema_version"].as_i64().unwrap(),
        1,
        "the execute_all DDL should have advanced the schema version"
    );

    // route: a key maps to some shard
    let route: serde_json::Value = serde_json::from_str(
        &ureq::post(&format!("{}/v1/route", g.base))
            .send_string(&serde_json::json!({"key":"alice"}).to_string())
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert!(route["shard"].is_number());

    // cluster: standalone reports clustered:false
    let cl: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/cluster", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cl["clustered"], false);
}

#[test]
fn phase_a_observability_endpoints() {
    let g = gateway(None, false);
    // The version-bumping DDL path (same as the execute_all test), so schema agreement is at v1.
    ureq::post(&format!("{}/v1/execute_all", g.base))
        .send_string(
            &serde_json::json!({"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"})
                .to_string(),
        )
        .unwrap();

    // A1 — /v1/stats now carries the WAL-conversion contention block and the writer open/eviction
    // counters the CLI `.stats` shows.
    let stats: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/stats", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert!(stats["wal_conversion"]["retries"].is_number());
    assert!(stats["wal_conversion"]["max_wait_ms"].is_number());
    assert!(stats["writer"]["shard_opens"].is_number());
    assert!(stats["writer"]["mean_batch"].is_number());

    // A2 — /v1/schema/agreement: one table applied to every shard agrees at version 1.
    let agree: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/schema/agreement", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(agree["status"], "agreed");
    assert_eq!(agree["version"].as_i64().unwrap(), 1);

    // A3 — /v1/replication: standalone has no ack tracker/follower, so replicated:false and every
    // shard is a primary reporting its LSN.
    let repl: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/replication", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(repl["replicated"], false);
    let shards = repl["shards"].as_array().unwrap();
    assert!(!shards.is_empty());
    assert!(shards.iter().all(|s| s["role"] == "primary"));
    assert!(shards[0]["primary_lsn"].is_number());
}

#[test]
fn phase_c_shard_operations() {
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    g.manager
        .execute_one(
            ShardId(0),
            Statement::new("INSERT INTO t VALUES (1),(2),(3)"),
        )
        .unwrap();

    // C1 — vacuum one shard.
    let vac: serde_json::Value = serde_json::from_str(
        &ureq::post(&format!("{}/v1/shards/0/vacuum", g.base))
            .send_string("")
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(vac["ok"], true);
    assert_eq!(vac["op"], "vacuum");

    // C2 — force a checkpoint; reports (busy, log_pages, checkpointed).
    let ckpt: serde_json::Value = serde_json::from_str(
        &ureq::post(&format!("{}/v1/shards/0/checkpoint", g.base))
            .send_string("")
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert!(ckpt["checkpointed"].is_number());
    assert!(ckpt["busy"].is_number());

    // C3 — declare a shard key on an EMPTY table: accepted.
    ddl(
        &g,
        "CREATE TABLE k (id INTEGER PRIMARY KEY, region TEXT) STRICT",
    );
    let sk: serde_json::Value = serde_json::from_str(
        &ureq::post(&format!("{}/v1/shardkey", g.base))
            .send_string(&serde_json::json!({"table":"k","column":"region"}).to_string())
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sk["ok"], true);
    assert_eq!(
        g.manager.shard_key("k").as_deref(),
        Some("region"),
        "the shard key should now be declared"
    );

    // C3 guard — declaring a key on a POPULATED table is refused with 409.
    g.manager
        .execute_one(ShardId(0), Statement::new("INSERT INTO t (id) VALUES (99)"))
        .unwrap();
    let err = ureq::post(&format!("{}/v1/shardkey", g.base))
        .send_string(&serde_json::json!({"table":"t","column":"id"}).to_string())
        .unwrap_err();
    match err {
        ureq::Error::Status(409, _) => {}
        other => panic!("expected a 409 on a populated table, got {other:?}"),
    }
}

#[test]
fn phase_e_config_reports_settings_and_mutability() {
    let g = gateway(None, false);
    let cfg: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/config", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    let settings = cfg["settings"].as_array().unwrap();
    let find = |key: &str| settings.iter().find(|s| s["key"] == key).unwrap();
    // shard_count is present and honestly marked immutable-with-a-reason.
    let sc = find("shard_count");
    assert_eq!(sc["value"], 1);
    assert_eq!(sc["mutable"], false);
    assert!(sc["note"].as_str().unwrap().contains("re-route"));
    // s3_archival is marked runtime-mutable and names the endpoint.
    let s3 = find("s3_archival");
    assert_eq!(s3["mutable"], true);
    assert!(s3["note"].as_str().unwrap().contains("/v1/s3/config"));
}

#[test]
fn phase_d_drain_refused_on_a_standalone_node() {
    // A standalone node is not a cluster member, so there is nothing to drain: 409, not a silent
    // no-op. (The clustered drain path exercises ClusterNode::stop, covered by the cluster tests.)
    let g = gateway(None, false);
    let err = ureq::post(&format!("{}/v1/cluster/drain", g.base))
        .send_string("")
        .unwrap_err();
    match err {
        ureq::Error::Status(409, _) => {}
        other => panic!("expected 409 on a standalone node, got {other:?}"),
    }
}

#[test]
fn frames_endpoint_decodes_the_wal() {
    let g = gateway(None, false);
    ddl(&g, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT");
    g.manager
        .execute_one(ShardId(0), Statement::new("INSERT INTO t VALUES (1)"))
        .unwrap();

    let f: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/frames/0", g.base))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(f["wal"], true);
    assert!(f["transactions"].as_u64().unwrap() >= 1);
    assert_eq!(f["page_size"], 4096);
}

#[test]
fn user_management_over_http() {
    let auth =
        AuthConfig::new()
            .user("boss", "bosspw", Role::Admin)
            .user("app", "apppw", Role::Write);
    let g = gateway(Some(auth), true);

    // Admin lists users.
    let list: serde_json::Value = serde_json::from_str(
        &ureq::get(&format!("{}/v1/users", g.base))
            .set("Authorization", &basic("boss", "bosspw"))
            .call()
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(list["users"].as_array().unwrap().len(), 2);

    // Admin creates a user, who can then authenticate.
    ureq::post(&format!("{}/v1/users", g.base))
        .set("Authorization", &basic("boss", "bosspw"))
        .send_string(&serde_json::json!({"name":"newbie","secret":"npw","role":"read"}).to_string())
        .unwrap();
    let info = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &basic("newbie", "npw"))
        .call()
        .unwrap();
    assert_eq!(info.status(), 200);

    // Admin cannot mint a cluster credential over the wire.
    let cluster_attempt = ureq::post(&format!("{}/v1/users", g.base))
        .set("Authorization", &basic("boss", "bosspw"))
        .send_string(&serde_json::json!({"name":"peer","secret":"x","role":"cluster"}).to_string())
        .unwrap_err();
    assert!(
        matches!(cluster_attempt, ureq::Error::Status(400, _)),
        "{cluster_attempt}"
    );

    // A non-admin cannot manage users.
    let refused = ureq::get(&format!("{}/v1/users", g.base))
        .set("Authorization", &basic("app", "apppw"))
        .call()
        .unwrap_err();
    assert!(matches!(refused, ureq::Error::Status(403, _)), "{refused}");

    // Admin drops the new user, who is then denied.
    ureq::delete(&format!("{}/v1/users/newbie", g.base))
        .set("Authorization", &basic("boss", "bosspw"))
        .call()
        .unwrap();
    let denied = ureq::get(&format!("{}/v1/info", g.base))
        .set("Authorization", &basic("newbie", "npw"))
        .call()
        .unwrap_err();
    assert!(matches!(denied, ureq::Error::Status(401, _)), "{denied}");
}
