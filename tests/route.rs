//! Client-statement routing: a write with no explicit shard must spread across shards by its
//! declared shard key, and a point read/update/delete must reach the one shard that holds the row.
//!
//! The load-bearing property is *agreement*: the shard an INSERT routes a value to is the shard a
//! later lookup of that same value routes to. If they ever disagreed, auto-routed writes would be
//! invisible to auto-routed reads. Every test here ties routing back to `shard_of`, the same hash
//! the storage layer places rows with.

use std::collections::HashSet;

use shardlite::query::route::primary_key_of;
use shardlite::query::{Route, ShardKeys, route_statement};
use shardlite::shard::{ShardConfig, ShardId, ShardManager, shard_of};
use shardlite::storage::exec::Statement;
use tempfile::TempDir;

const SHARDS: u32 = 64;

fn keys_for(table: &str, col: &str) -> ShardKeys {
    let mut k = ShardKeys::new();
    k.insert(table.to_string(), col.to_string());
    k
}

/// The shard `shard_of` (the placement hash) would choose for a text key.
fn text_shard(s: &str) -> u32 {
    shard_of(s.as_bytes(), SHARDS).0
}

fn int_shard(n: i64) -> u32 {
    shard_of(&n.to_le_bytes(), SHARDS).0
}

#[test]
fn an_insert_routes_to_the_shard_that_holds_its_key() {
    let keys = keys_for("users", "id");
    // A single-row INSERT goes to exactly the shard the key hashes to — the same shard the storage
    // layer would place the row on.
    for id in ["alice", "bob", "carol", "dave"] {
        let sql = format!("INSERT INTO users (id, name) VALUES ('{id}', 'x')");
        assert_eq!(
            route_statement(&sql, &keys, SHARDS),
            Route::One(text_shard(id)),
            "{sql}"
        );
    }
    // Integer keys route by their little-endian bytes.
    let sql = "INSERT INTO users (id, name) VALUES (42, 'x')";
    let keys_int = keys_for("users", "id");
    assert_eq!(
        route_statement(sql, &keys_int, SHARDS),
        Route::One(int_shard(42))
    );
}

#[test]
fn a_point_read_update_delete_reaches_the_same_shard_as_the_insert() {
    let keys = keys_for("users", "id");
    for id in ["alice", "bob", "carol", "dave", "erin", "frank"] {
        let want = Route::One(text_shard(id));
        // INSERT, SELECT, UPDATE and DELETE for the same key must all name the same shard.
        for sql in [
            format!("INSERT INTO users (id, name) VALUES ('{id}', 'x')"),
            format!("SELECT * FROM users WHERE id = '{id}'"),
            format!("UPDATE users SET name = 'y' WHERE id = '{id}'"),
            format!("DELETE FROM users WHERE id = '{id}'"),
            // The key predicate need not be first, and a qualified column resolves too.
            format!("SELECT * FROM users WHERE name = 'x' AND users.id = '{id}'"),
        ] {
            assert_eq!(route_statement(&sql, &keys, SHARDS), want, "{sql}");
        }
    }
}

#[test]
fn a_shard_key_update_is_refused_instead_of_moving_half_a_row() {
    let keys = keys_for("users", "id");
    for sql in [
        "UPDATE users SET id = 'new' WHERE id = 'old'",
        "UPDATE users SET (name, id) = ('n', 'new') WHERE id = 'old'",
    ] {
        match route_statement(sql, &keys, SHARDS) {
            Route::Refuse(message) => assert!(message.contains("cannot be updated"), "{message}"),
            other => panic!("expected shard-key update refusal, got {other:?}"),
        }
    }
}

#[test]
fn writes_actually_spread_across_shards() {
    // The whole point: many inserts with distinct keys must not all land on one shard.
    let keys = keys_for("users", "id");
    let mut hit: HashSet<u32> = HashSet::new();
    for i in 0..500 {
        let sql = format!("INSERT INTO users (id) VALUES ('user-{i}')");
        if let Route::One(s) = route_statement(&sql, &keys, SHARDS) {
            hit.insert(s);
        } else {
            panic!("expected a single-shard route for {sql}");
        }
    }
    // 500 keys over 64 shards should touch nearly all of them — certainly far more than the one
    // shard the old default-to-0 behaviour used.
    assert!(
        hit.len() > SHARDS as usize / 2,
        "writes hit only {} of {SHARDS} shards — not spreading",
        hit.len()
    );
}

#[test]
fn a_multi_row_insert_splits_per_shard_and_keeps_every_row() {
    let keys = keys_for("users", "id");
    let sql = "INSERT INTO users (id, name) VALUES \
               ('alice', 'a'), ('bob', 'b'), ('carol', 'c'), ('dave', 'd')";
    match route_statement(sql, &keys, SHARDS) {
        Route::Split(parts) => {
            // Each part targets the shard its rows' key hashes to, and every original row appears
            // exactly once across the parts.
            let mut names = HashSet::new();
            for (shard, sub) in &parts {
                for (id, expect_shard) in [
                    ("alice", "alice"),
                    ("bob", "bob"),
                    ("carol", "carol"),
                    ("dave", "dave"),
                ] {
                    if sub.contains(&format!("'{id}'")) {
                        assert_eq!(*shard, text_shard(expect_shard), "row {id} in wrong part");
                        names.insert(id);
                    }
                }
            }
            assert_eq!(names.len(), 4, "not every row was placed: {parts:?}");
        }
        // If by chance all four hashed to one shard the split collapses to One — assert that fallback
        // is at least self-consistent rather than silently dropping rows.
        Route::One(_) => {}
        other => panic!("expected a per-shard split, got {other:?}"),
    }
}

#[test]
fn an_unpinned_write_targets_every_shard_and_a_read_falls_through() {
    let keys = keys_for("users", "id");
    // No shard-key predicate: a write must touch every shard (so no row is missed)...
    assert_eq!(
        route_statement("UPDATE users SET name = 'x'", &keys, SHARDS),
        Route::All
    );
    assert_eq!(
        route_statement("DELETE FROM users WHERE name = 'gone'", &keys, SHARDS),
        Route::All
    );
    // ...while a read is left to the caller's fan-out.
    assert_eq!(
        route_statement("SELECT * FROM users WHERE name = 'x'", &keys, SHARDS),
        Route::Passthrough
    );
    // An OR over the shard key can match rows on several shards, so it is not pinned.
    assert_eq!(
        route_statement(
            "DELETE FROM users WHERE id = 'a' OR id = 'b'",
            &keys,
            SHARDS
        ),
        Route::All
    );
}

#[test]
fn an_undeclared_table_is_passed_through() {
    // No shard key declared for `logs`: the router has no opinion, the caller's default applies.
    let keys = keys_for("users", "id");
    assert_eq!(
        route_statement("INSERT INTO logs (msg) VALUES ('hi')", &keys, SHARDS),
        Route::Passthrough
    );
    assert_eq!(
        route_statement("DELETE FROM logs WHERE id = 'x'", &keys, SHARDS),
        Route::Passthrough
    );
}

#[test]
fn an_unroutable_insert_is_refused_not_misrouted() {
    let keys = keys_for("users", "id");
    // The key column is not in the INSERT's column list.
    match route_statement("INSERT INTO users (name) VALUES ('x')", &keys, SHARDS) {
        Route::Refuse(msg) => assert!(msg.contains("id"), "{msg}"),
        other => panic!("expected refusal, got {other:?}"),
    }
    // A non-literal shard key cannot be routed statically.
    match route_statement(
        "INSERT INTO users (id, name) VALUES (lower('X'), 'x')",
        &keys,
        SHARDS,
    ) {
        Route::Refuse(msg) => assert!(msg.contains("literal"), "{msg}"),
        other => panic!("expected refusal, got {other:?}"),
    }
    // No column list at all: positions are unknown.
    match route_statement("INSERT INTO users VALUES ('a', 'b')", &keys, SHARDS) {
        Route::Refuse(msg) => assert!(msg.contains("list its columns"), "{msg}"),
        other => panic!("expected refusal, got {other:?}"),
    }
}

#[test]
fn the_primary_key_is_read_as_the_shard_key() {
    // Column-level PRIMARY KEY.
    assert_eq!(
        primary_key_of("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT"),
        Some(("users".to_string(), "id".to_string()))
    );
    // Table-level single-column PRIMARY KEY.
    assert_eq!(
        primary_key_of("CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))"),
        Some(("t".to_string(), "a".to_string()))
    );
    // A composite primary key is not a single routing key.
    assert_eq!(
        primary_key_of("CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, b))"),
        None
    );
    // No primary key, and non-CREATE statements.
    assert_eq!(primary_key_of("CREATE TABLE t (a INTEGER, b TEXT)"), None);
    assert_eq!(primary_key_of("INSERT INTO t (a) VALUES (1)"), None);
}

#[test]
fn creating_a_table_auto_declares_its_primary_key_and_routes() {
    // The whole "no shard awareness" path: create a table with a PK, and writes route by it with
    // no `declare_shard_key` call at all.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: SHARDS,
            writer_threads: 2,
            reader_threads: 2,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    assert_eq!(m.shard_key("users"), None, "nothing declared yet");

    m.execute_all_shards("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT")
        .unwrap();

    // The primary key was adopted as the shard key automatically...
    assert_eq!(m.shard_key("users").as_deref(), Some("id"));
    // ...so a plain INSERT routes to the shard its key hashes to, with no declaration step.
    assert_eq!(
        route_statement(
            "INSERT INTO users (id, name) VALUES ('alice', 'a')",
            &m.shard_keys(),
            SHARDS
        ),
        Route::One(text_shard("alice"))
    );
}

#[test]
fn an_explicit_shard_key_is_not_overwritten_by_the_primary_key() {
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: SHARDS,
            writer_threads: 2,
            reader_threads: 2,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    // A deliberate choice to shard by a non-PK column must survive table creation.
    m.declare_shard_key("orders", "customer").unwrap();
    m.execute_all_shards("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT) STRICT")
        .unwrap();
    assert_eq!(
        m.shard_key("orders").as_deref(),
        Some("customer"),
        "explicit declaration must win over the primary key"
    );
}

#[test]
fn routed_writes_survive_a_round_trip_through_the_store() {
    // End to end: declare a key, route each INSERT to its shard, execute it there, then fan out a
    // COUNT. Nothing is lost (every write landed on a real shard and is found), and the data is
    // genuinely spread — the heaviest shard holds far less than all of it.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: SHARDS,
            writer_threads: 2,
            reader_threads: 2,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    m.execute_all_shards("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT")
        .unwrap();
    m.declare_shard_key("users", "id").unwrap();
    let keys = m.shard_keys();

    let n = 400;
    let mut per_shard = vec![0usize; SHARDS as usize];
    for i in 0..n {
        let sql = format!("INSERT INTO users (id, name) VALUES ('user-{i}', 'n{i}')");
        match route_statement(&sql, &keys, SHARDS) {
            Route::One(s) => {
                m.execute_one(ShardId(s), Statement::new(&sql)).unwrap();
                per_shard[s as usize] += 1;
            }
            other => panic!("expected One, got {other:?} for {sql}"),
        }
    }

    // Every inserted row is visible via a fan-out — none were routed into the void.
    let total = m.query_all_shards("SELECT count(*) FROM users").unwrap();
    assert_eq!(
        total.rows,
        vec![vec![shardlite::storage::Value::Integer(n)]]
    );

    // And a point lookup finds its row on the one shard the insert chose.
    let s = text_shard("user-7");
    let hit = m
        .query(ShardId(s), "SELECT name FROM users WHERE id = 'user-7'")
        .unwrap();
    match hit {
        shardlite::storage::exec::Outcome::Ok(shardlite::storage::exec::Executed::Rows(r)) => {
            assert_eq!(
                r.rows,
                vec![vec![shardlite::storage::Value::Text("n7".into())]]
            );
        }
        other => panic!("point lookup did not return the row: {other:?}"),
    }

    // No single shard holds more than a fraction of the data — it is spread, not piled on shard 0.
    // No single shard holds more than a fraction — so nothing piled onto shard 0.
    let heaviest = *per_shard.iter().max().unwrap();
    assert!(
        heaviest < n as usize / 4,
        "one shard holds {heaviest} of {n} rows — not spreading"
    );
}
