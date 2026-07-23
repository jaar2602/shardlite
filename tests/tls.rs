//! TLS transport, end to end. Compiled only with `--features tls`.
//!
//! Certificates are generated in-process with a far-future validity, so nothing is checked
//! in to expire and no test depends on a file on disk.

#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::Duration;

use shardlite::net::transport::{TlsClientConfig, TlsServerConfig};
use shardlite::net::{Client, NodeServices, Server, ServerConfig};
use shardlite::shard::{ShardConfig, ShardManager};
use shardlite::storage::Value;
use tempfile::TempDir;

/// A self-signed certificate for `127.0.0.1`, written to PEM files under `dir`.
///
/// Returns the cert and key paths. The cert names the loopback IP, so a client verifying
/// against it (the cert is its own CA, self-signed) accepts a connection to 127.0.0.1.
fn self_signed(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert_pem).unwrap();
    std::fs::write(&key_path, key_pem).unwrap();
    (cert_path, key_path)
}

struct TlsNode {
    addr: String,
    _dir: TempDir,
    _certs: TempDir,
    _server: Arc<Server>,
}

fn serve_tls() -> (TlsNode, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let certs = TempDir::new().unwrap();
    let (cert, key) = self_signed(certs.path());

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
        .unwrap()
        .with_tls(TlsServerConfig::from_pem_files(&cert, &key).unwrap()),
    );
    let addr = server.local_addr().unwrap().to_string();
    let s = Arc::clone(&server);
    std::thread::spawn(move || {
        let _ = s.serve();
    });
    std::thread::sleep(Duration::from_millis(50));
    (
        TlsNode {
            addr,
            _dir: dir,
            _certs: certs,
            _server: server,
        },
        cert,
    )
}

fn client_verifying(cert: &std::path::Path) -> TlsClientConfig {
    // The self-signed cert is its own CA — verifying against it means "only this exact
    // server". The name must match the cert's SAN, which is the loopback IP.
    TlsClientConfig::with_ca_pem(cert, "127.0.0.1").unwrap()
}

fn connect(addr: &str, tls: &TlsClientConfig) -> shardlite::error::Result<Client> {
    Client::connect_tls(
        addr,
        Duration::from_secs(5),
        Duration::from_secs(5),
        None,
        tls,
    )
}

#[test]
fn a_verified_tls_client_reads_and_writes() {
    let (node, cert) = serve_tls();
    let tls = client_verifying(&cert);
    let mut c = connect(&node.addr, &tls).unwrap();

    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    c.execute(0, "INSERT INTO t VALUES (1, 'over tls')")
        .unwrap();
    let rows = c.query_all("SELECT count(*) FROM t").unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}

#[test]
fn a_plaintext_client_cannot_talk_to_a_tls_server() {
    // The point of enabling TLS: an unencrypted connection is not silently downgraded, it
    // fails. A plaintext client sends a bincode Hello where the server expects a TLS
    // ClientHello, and the handshake cannot complete.
    let (node, _cert) = serve_tls();
    let result = Client::connect_with(&node.addr, Duration::from_secs(2));
    assert!(
        result.is_err(),
        "a plaintext client must not complete a handshake with a TLS server"
    );
}

#[test]
fn a_tls_client_cannot_talk_to_a_plaintext_server() {
    // The mirror: a client expecting TLS must not be tricked into speaking plaintext, or an
    // active attacker could strip the encryption.
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

    let tls = TlsClientConfig::dangerous_accept_any_cert("127.0.0.1");
    assert!(
        connect(&addr, &tls).is_err(),
        "a TLS client must not complete a handshake with a plaintext server"
    );
}

#[test]
fn a_wrong_certificate_is_rejected_by_a_verifying_client() {
    // The property that makes TLS worth more than plaintext-plus-auth: a man-in-the-middle
    // presenting a different certificate is refused, so the connection cannot be hijacked.
    let (node, _real_cert) = serve_tls();

    // A client that trusts a *different* self-signed cert — standing in for an attacker's,
    // or simply the wrong one — must refuse the real server.
    let other_certs = TempDir::new().unwrap();
    let (wrong_cert, _wrong_key) = self_signed(other_certs.path());
    let tls = client_verifying(&wrong_cert);

    let err = connect(&node.addr, &tls).expect_err("a client must reject an untrusted cert");
    // The failure is the TLS handshake, surfaced as a protocol error.
    assert!(!err.to_string().is_empty());
}

#[test]
fn accept_any_cert_still_encrypts_and_works() {
    // The development mode: no verification, but a working encrypted channel. It must
    // actually connect — the danger is that it accepts the wrong cert, not that it is
    // broken.
    let (node, _cert) = serve_tls();
    let tls = TlsClientConfig::dangerous_accept_any_cert("127.0.0.1");
    let mut c = connect(&node.addr, &tls).unwrap();
    c.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    c.execute(0, "INSERT INTO t VALUES (7)").unwrap();
    assert_eq!(
        c.query_all("SELECT count(*) FROM t").unwrap().rows[0][0],
        Value::Integer(1)
    );
}

#[test]
fn tls_and_authentication_compose() {
    // The two layers are orthogonal and stack: encryption for the channel, credentials for
    // the identity. A caller gets both by giving both.
    use shardlite::net::{AuthConfig, Role};

    let dir = TempDir::new().unwrap();
    let certs = TempDir::new().unwrap();
    let (cert, key) = self_signed(certs.path());
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
    let auth = AuthConfig::new().user("app", "secret", Role::Admin);
    let server = Arc::new(
        Server::bind_with(
            manager,
            NodeServices {
                auth: Some(Arc::new(auth)),
                ..Default::default()
            },
            ServerConfig {
                addr: "127.0.0.1:0".into(),
                ..ServerConfig::default()
            },
        )
        .unwrap()
        .with_tls(TlsServerConfig::from_pem_files(&cert, &key).unwrap()),
    );
    let addr = server.local_addr().unwrap().to_string();
    let s = Arc::clone(&server);
    std::thread::spawn(move || {
        let _ = s.serve();
    });
    std::thread::sleep(Duration::from_millis(50));

    let tls = client_verifying(&cert);

    // Right credentials over TLS: works.
    let mut ok = Client::connect_tls(
        &addr,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Some(("app".into(), shardlite::net::auth::derive_key("secret"))),
        &tls,
    )
    .unwrap();
    ok.execute_all("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // Encrypted but unauthenticated: refused by the auth layer, not the TLS layer.
    let anon = Client::connect_tls(
        &addr,
        Duration::from_secs(5),
        Duration::from_secs(5),
        None,
        &tls,
    );
    assert!(
        anon.is_err(),
        "TLS must not exempt a connection from authentication"
    );
}
