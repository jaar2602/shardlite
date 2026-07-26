//! Authentication and authorization, over the real wire.
//!
//! These tests care as much about what is refused as what works — an auth layer is defined
//! by its refusals. The handshake is also driven by hand where the property is about the
//! protocol itself (nonce freshness, replay) rather than the client's convenience wrapper.

use std::sync::Arc;
use std::time::Duration;

use shardlite::net::{AuthConfig, Client, NodeServices, Role, Server, ServerConfig};
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::Value;
use tempfile::TempDir;

struct Node {
    server: Arc<Server>,
    addr: String,
    _dir: TempDir,
}

fn serve(auth: Option<AuthConfig>) -> Node {
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
            NodeServices {
                auth: auth.map(Arc::new),
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
    Node {
        server,
        addr,
        _dir: dir,
    }
}

fn full_auth() -> AuthConfig {
    AuthConfig::new()
        .user("reader", "read-secret", Role::Read)
        .user("app", "write-secret", Role::Write)
        .user("ops", "admin-secret", Role::Admin)
        .user("node", "cluster-secret", Role::Cluster)
}

#[test]
fn an_unconfigured_server_is_open_exactly_as_before() {
    // Backwards compatibility is a security property here in reverse: enabling the feature
    // must be a choice, not a breakage. The open mode is announced by a warning at bind.
    let n = serve(None);
    let mut c = Client::connect(&n.addr).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    c.execute(0, "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(
        c.query_all("SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1)
    );
}

#[test]
fn an_unauthenticated_client_is_told_credentials_are_needed() {
    let n = serve(Some(full_auth()));
    let err = Client::connect(&n.addr).expect_err("connecting without credentials must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("requires authentication"),
        "the refusal must say what is missing, not just refuse: {msg}"
    );
}

#[test]
fn a_wrong_secret_is_refused_counted_and_the_connection_closed() {
    let n = serve(Some(full_auth()));
    let err = Client::connect_as(&n.addr, "app", "not-the-secret")
        .expect_err("a wrong secret must be refused");
    assert!(err.to_string().contains("authentication failed"), "{err}");
    assert_eq!(
        n.server.stats().auth_failures,
        1,
        "the failure must be counted, not silently absorbed"
    );

    // The refusal for an unknown *name* must be indistinguishable, or the handshake
    // enumerates valid names for an attacker.
    let err2 = Client::connect_as(&n.addr, "who-is-this", "whatever").unwrap_err();
    assert_eq!(
        err.to_string(),
        err2.to_string(),
        "wrong-secret and unknown-name refusals must be identical"
    );
}

#[test]
fn correct_credentials_work_and_roles_bound_what_they_may_do() {
    let n = serve(Some(full_auth()));

    // Admin sets up the schema — DDL is an operator action.
    let mut ops = Client::connect_as(&n.addr, "ops", "admin-secret").unwrap();
    ops.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // Write can insert and read, but not reshape the schema cluster-wide.
    let mut app = Client::connect_as(&n.addr, "app", "write-secret").unwrap();
    app.execute(0, "INSERT INTO t VALUES (1)").unwrap();
    let err = app
        .execute_all("CREATE TABLE nope (a INTEGER)")
        .expect_err("the write role must not run cluster-wide DDL");
    assert!(err.to_string().contains("write"), "{err}");
    assert!(err.to_string().contains("Admin"), "{err}");

    // Read can query and nothing else.
    let mut reader = Client::connect_as(&n.addr, "reader", "read-secret").unwrap();
    assert_eq!(
        reader.query_all("SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1)
    );
    assert!(
        reader.execute(0, "INSERT INTO t VALUES (2)").is_err(),
        "the read role must not write"
    );

    assert!(
        n.server.stats().authz_refused >= 2,
        "each role refusal must be counted"
    );
}

#[test]
fn client_roles_cannot_touch_the_replication_stream() {
    // The wall between the ladders. Subscribe and snapshots hand out whole shards; an
    // administrator's stolen credentials must not include the exfiltration path.
    use shardlite::net::protocol::Request;

    let n = serve(Some(full_auth()));
    let mut ops = Client::connect_as(&n.addr, "ops", "admin-secret").unwrap();

    let err = ops
        .request(Request::Subscribe {
            node: 9,
            shard: 0,
            epoch: 1,
            from_lsn: 1,
            max_txns: 16,
        })
        .expect_err("an admin must not be able to subscribe to the replication stream");
    assert!(err.to_string().contains("Cluster"), "{err}");

    let err = ops
        .request(Request::SnapshotBegin { shard: 0 })
        .expect_err("nor freeze and copy a shard wholesale");
    assert!(err.to_string().contains("Cluster"), "{err}");

    // And the cluster principal, conversely, is not a query account.
    let mut node = Client::connect_as(&n.addr, "node", "cluster-secret").unwrap();
    assert!(
        node.query_all("SELECT 1").is_err(),
        "the cluster role is for machines, not queries"
    );
}

#[test]
fn every_connection_gets_a_fresh_challenge_and_a_replayed_proof_fails() {
    // The property that makes challenge–response worth its round trip: a captured handshake
    // is useless against any other connection. Driven by hand, because the property is the
    // protocol's, not the client wrapper's.
    use shardlite::net::protocol::{
        PROTOCOL_VERSION, Request, Response, read_message, write_message,
    };
    use std::net::TcpStream;

    let n = serve(Some(full_auth()));

    let handshake = |auth_reply: Option<[u8; 32]>| -> ([u8; 32], Response) {
        let stream = TcpStream::connect(&n.addr).unwrap();
        let mut w = std::io::BufWriter::new(stream.try_clone().unwrap());
        let mut r = std::io::BufReader::new(stream);
        write_message(
            &mut w,
            &Request::Hello {
                version: PROTOCOL_VERSION,
                client: "replay-test".into(),
            },
        )
        .unwrap();
        let Response::Challenge { nonce } = read_message::<Response, _>(&mut r).unwrap() else {
            panic!("expected a challenge");
        };
        let proof = auth_reply.unwrap_or_else(|| {
            shardlite::net::auth::prove(&shardlite::net::auth::derive_key("write-secret"), &nonce)
        });
        write_message(
            &mut w,
            &Request::Auth {
                name: "app".into(),
                proof,
            },
        )
        .unwrap();
        (nonce, read_message::<Response, _>(&mut r).unwrap())
    };

    // A legitimate handshake, recording the proof an attacker would capture.
    let stream = TcpStream::connect(&n.addr).unwrap();
    let mut w = std::io::BufWriter::new(stream.try_clone().unwrap());
    let mut r = std::io::BufReader::new(stream);
    write_message(
        &mut w,
        &Request::Hello {
            version: PROTOCOL_VERSION,
            client: "victim".into(),
        },
    )
    .unwrap();
    let Response::Challenge { nonce: first_nonce } = read_message::<Response, _>(&mut r).unwrap()
    else {
        panic!("expected a challenge");
    };
    let captured_proof = shardlite::net::auth::prove(
        &shardlite::net::auth::derive_key("write-secret"),
        &first_nonce,
    );

    // A new connection gets a different nonce, and the captured proof fails against it.
    let (second_nonce, replay_outcome) = handshake(Some(captured_proof));
    assert_ne!(
        first_nonce, second_nonce,
        "challenges must be fresh per connection"
    );
    match replay_outcome {
        Response::Error { message, .. } => {
            assert!(message.contains("authentication failed"), "{message}")
        }
        other => panic!("a replayed proof must be refused, got {other:?}"),
    }

    // And a fresh, honest proof still works — the refusal above was the replay, not noise.
    let (_, honest) = handshake(None);
    assert!(
        matches!(honest, Response::Welcome { .. }),
        "an honest handshake must still succeed, got {honest:?}"
    );
}

#[test]
fn an_authenticated_replica_pulls_frames_from_an_authenticated_primary() {
    // The cluster principal end to end: subscription is a cluster verb, so a replica must
    // authenticate to follow — and with credentials configured, it simply works.
    use shardlite::net::{Replica, ReplicaConfig};
    use shardlite::replication::{Follower, FrameLog, FrameLogConfig};
    use shardlite::storage::exec::Statement;

    let dir = TempDir::new().unwrap();
    let frames = Arc::new(FrameLog::new(FrameLogConfig {
        total_bytes: 4 * 1024 * 1024,
        shard_count: 1,
    }));
    let manager = Arc::new(
        ShardManager::open_with_sink(
            dir.path(),
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
            Arc::clone(&manager),
            NodeServices {
                frames: Some(frames),
                auth: Some(Arc::new(full_auth())),
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

    let mut ops = Client::connect_as(&addr, "ops", "admin-secret").unwrap();
    ops.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        manager
            .execute_one(
                ShardId(0),
                Statement::new(format!("INSERT INTO t VALUES ({i})")),
            )
            .unwrap();
    }

    let fdir = TempDir::new().unwrap();
    let stage = TempDir::new().unwrap();
    let replica = Replica::new(
        ReplicaConfig {
            primary_addr: addr.clone(),
            shards: vec![ShardId(0)],
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: 2,
            credentials: Some(("node".into(), "cluster-secret".into())),
        },
        Arc::new(Follower::open(fdir.path()).unwrap()),
    );
    replica.sync_once().unwrap();
    assert!(
        replica.follower().position(ShardId(0)).applied_lsn > 0,
        "the authenticated replica should have applied frames"
    );

    // Without credentials the same replica is turned away at the door.
    let fdir2 = TempDir::new().unwrap();
    let anon = Replica::new(
        ReplicaConfig {
            primary_addr: addr,
            shards: vec![ShardId(0)],
            poll_interval: Duration::from_millis(5),
            batch: 64,
            snapshot_chunk: 64 * 1024,
            staging_dir: stage.path().to_path_buf(),
            node: 3,
            credentials: None,
        },
        Arc::new(Follower::open(fdir2.path()).unwrap()),
    );
    let err = anon
        .sync_once()
        .expect_err("an unauthenticated replica must be refused");
    assert!(err.to_string().contains("authentication"), "{err}");
}

#[test]
fn an_admin_creates_a_user_at_runtime_who_can_then_log_in() {
    // The point of runtime management: no restart, no config edit, and the new user works
    // immediately with exactly the role granted.
    let n = serve(Some(full_auth()));

    let mut ops = Client::connect_as(&n.addr, "ops", "admin-secret").unwrap();
    ops.create_user("newbie", "newbie-secret", Role::Write)
        .unwrap();

    // The new user logs in and does write-role things — reads and single-shard writes.
    let mut newbie = Client::connect_as(&n.addr, "newbie", "newbie-secret").unwrap();
    newbie
        .execute(0, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    newbie.execute(0, "INSERT INTO t VALUES (1)").unwrap();

    // ...but not admin things.
    assert!(
        newbie.execute_all("CREATE TABLE nope (a INTEGER)").is_err(),
        "a runtime-created write user must not have admin powers"
    );

    // The admin can see them listed.
    let users = ops.list_users().unwrap();
    assert!(
        users
            .iter()
            .any(|(name, role)| name == "newbie" && *role == Role::Write),
        "the new user should appear in the list: {users:?}"
    );
}

#[test]
fn an_admin_cannot_mint_a_cluster_credential_over_the_wire() {
    // The wall between clients and cluster members, enforced at the one place it could be
    // tunnelled: an admin creating users. A cluster credential is a deploy-time decision.
    let n = serve(Some(full_auth()));
    let mut ops = Client::connect_as(&n.addr, "ops", "admin-secret").unwrap();

    let err = ops
        .create_user("sneaky-node", "secret", Role::Cluster)
        .expect_err("an admin must not be able to create a cluster user at runtime");
    assert!(err.to_string().contains("cluster"), "{err}");

    // And the user was not created — a rejected grant must leave no trace.
    let users = ops.list_users().unwrap();
    assert!(!users.iter().any(|(name, _)| name == "sneaky-node"));
}

#[test]
fn a_non_admin_cannot_manage_users() {
    // User management is the most privileged client action; a write or read role must be
    // refused, and the refusal counted like any other authorization failure.
    let n = serve(Some(full_auth()));
    let mut app = Client::connect_as(&n.addr, "app", "write-secret").unwrap();

    assert!(
        app.create_user("x", "y", Role::Read).is_err(),
        "the write role must not create users"
    );
    assert!(app.drop_user("reader").is_err(), "nor drop them");
    assert!(app.list_users().is_err(), "nor list them");
    assert!(n.server.stats().authz_refused >= 3);
}

#[test]
fn dropping_a_user_denies_them_immediately() {
    let n = serve(Some(full_auth()));
    let mut ops = Client::connect_as(&n.addr, "ops", "admin-secret").unwrap();

    // The reader works, then is dropped, then cannot connect.
    assert!(Client::connect_as(&n.addr, "reader", "read-secret").is_ok());
    ops.drop_user("reader").unwrap();
    assert!(
        Client::connect_as(&n.addr, "reader", "read-secret").is_err(),
        "a dropped user must not be able to authenticate"
    );
}

#[test]
fn runtime_changes_survive_a_restart_through_the_users_file() {
    // Runtime management would be a foot-gun if it were only in memory: create a user, the
    // server restarts, the user is gone, and nobody knows why. The change must be durable.
    let dir = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let users_file = dir.path().join("users.db");

    // Bootstrap an admin offline, exactly as the CLI's `user add --users` does.
    {
        let auth = AuthConfig::open(&users_file).unwrap();
        auth.create(
            "boss",
            shardlite::net::auth::derive_key("boss-pw"),
            Role::Admin,
        )
        .unwrap();
    }

    let boot = |data_path: &std::path::Path| -> (String, Arc<Server>) {
        let manager = Arc::new(
            ShardManager::open(
                data_path,
                ShardConfig {
                    shard_count: 1,
                    ..ShardConfig::floor()
                },
            )
            .unwrap(),
        );
        let auth = Arc::new(AuthConfig::open(&users_file).unwrap());
        let server = Arc::new(
            Server::bind_with(
                manager,
                NodeServices {
                    auth: Some(auth),
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
        (addr, server)
    };

    // First run: the admin creates a user at runtime.
    {
        let (addr, server) = boot(data.path());
        let mut boss = Client::connect_as(&addr, "boss", "boss-pw").unwrap();
        boss.create_user("survivor", "surv-pw", Role::Write)
            .unwrap();
        server
            .shutdown_handle()
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // Second run: a fresh server, same file. The runtime-created user is still there.
    let data2 = TempDir::new().unwrap();
    let (addr, _server) = boot(data2.path());
    assert!(
        Client::connect_as(&addr, "survivor", "surv-pw").is_ok(),
        "a user created at runtime must survive a restart via the users file"
    );
}
