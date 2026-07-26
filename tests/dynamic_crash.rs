//! Process-level crash/restart qualification for dynamic topology operations.
//!
//! Run the full matrix explicitly:
//!
//! ```text
//! cargo test --features failpoints --test dynamic_crash -- --ignored --nocapture
//! ```
//!
//! Set `SHARDLITE_QUALIFY_FAILPOINT` to one exact name to run a single boundary while debugging.
//! These tests are ignored in the ordinary fast suite because they repeatedly start real servers,
//! force an unclean exit, restart them, and wait for election/reconciliation.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use shardlite::cluster::{Catalog, CatalogCommand, MemberRole, OperationKind, OperationPhase};
use shardlite::net::Client;
use shardlite::storage::Value;
use tempfile::TempDir;

const SPLIT_FAILPOINTS: &[&str] = &[
    "split.after_planned",
    "split.after_snapshotting",
    "split.owner.after_capture_install",
    "split.owner.after_snapshot_file",
    "split.after_snapshot_ready",
    "split.owner.after_backfill",
    "split.owner.after_replay",
    "split.after_replay",
    "split.after_prepared",
    "split.after_fencing",
    "split.owner.after_finalize",
    "split.after_fenced",
    "split.owner.after_install",
    "split.after_committed",
    "split.after_cleaning",
    "split.owner.after_cleanup",
    "split.after_complete",
];

const TRANSFER_FAILPOINTS: &[&str] = &[
    "transfer.after_planned",
    "transfer.after_snapshotting",
    "transfer.destination.after_snapshot_file",
    "transfer.destination.after_catchup_file",
    "transfer.after_snapshot_installed",
    "transfer.after_catchup",
    "transfer.after_prepared",
    "transfer.after_fencing",
    "transfer.source.after_fence",
    "transfer.after_final_lsn",
    "transfer.after_committed",
    "transfer.source.after_demotion",
    "transfer.after_cleaning",
    "transfer.after_complete",
];

struct ServerProcess {
    binary: PathBuf,
    dir: PathBuf,
    child: Option<Child>,
}

impl ServerProcess {
    fn start(binary: &Path, dir: &Path, failpoint: Option<&str>, marker: &Path) -> Self {
        let child = spawn_server(binary, dir, failpoint, marker);
        Self {
            binary: binary.to_path_buf(),
            dir: dir.to_path_buf(),
            child: Some(child),
        }
    }

    fn wait_for_injected_crash(&mut self, marker: &Path) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("server process")
                .try_wait()
                .expect("checking server process")
            {
                assert_eq!(
                    status.code(),
                    Some(shardlite::failpoint::EXIT_CODE),
                    "server exited without the deterministic failpoint"
                );
                assert!(
                    marker.exists(),
                    "server exited at the failpoint but did not persist {}",
                    marker.display()
                );
                self.child.take();
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server did not reach failpoint recorded by {}",
                marker.display()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn restart_without_failpoint(&mut self) {
        self.restart(None, Path::new("unused"));
    }

    fn restart_at(&mut self, failpoint: &str, marker: &Path) {
        self.restart(Some(failpoint), marker);
    }

    fn restart(&mut self, failpoint: Option<&str>, marker: &Path) {
        assert!(self.child.is_none(), "old server is still running");
        self.child = Some(spawn_server(&self.binary, &self.dir, failpoint, marker));
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
#[ignore = "process-crash qualification matrix; run explicitly with --ignored"]
fn every_split_boundary_repairs_itself_after_restart() {
    for failpoint in selected(SPLIT_FAILPOINTS) {
        eprintln!("qualifying {failpoint}");
        qualify_split(failpoint);
    }
}

#[test]
#[ignore = "process-crash qualification matrix; run explicitly with --ignored"]
fn every_transfer_boundary_repairs_itself_after_restart() {
    for failpoint in selected(TRANSFER_FAILPOINTS) {
        eprintln!("qualifying {failpoint}");
        qualify_transfer(failpoint);
    }
}

#[test]
#[ignore = "process-crash qualification matrix; run explicitly with --ignored"]
fn prepared_and_locally_committed_catalog_values_repair_after_restart() {
    for failpoint in [
        "catalog.after_local_prepare",
        "catalog.after_local_commit",
        "join.after_registered",
        "join.after_complete",
    ] {
        if !selected(&[failpoint]).is_empty() {
            eprintln!("qualifying {failpoint}");
            qualify_interrupted_join(failpoint);
        }
    }
}

#[test]
#[ignore = "process-crash qualification matrix; run explicitly with --ignored"]
fn fenced_epoch_repair_survives_repeated_coordinator_crashes() {
    let required = [
        "transfer.after_fencing",
        "transfer.after_fenced_stream_restart",
        "transfer.after_fenced_bootstrap",
    ];
    if std::env::var("SHARDLITE_QUALIFY_FAILPOINT")
        .is_ok_and(|selected| !required.contains(&selected.as_str()))
    {
        return;
    }

    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, 2);

    let first = fixture.path().join("fencing.marker");
    let mut node1 =
        ServerProcess::start(&binary, &node1_dir, Some("transfer.after_fencing"), &first);
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);
    seed_rows(&node1_addr);
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = ServerProcess::start(&binary, &node2_dir, None, Path::new("unused"));
    wait_ready(&node2_addr, &mut node2);
    node1.wait_for_injected_crash(&first);

    let second = fixture.path().join("stream-restart.marker");
    node1.restart_at("transfer.after_fenced_stream_restart", &second);
    wait_ready(&node1_addr, &mut node1);
    node1.wait_for_injected_crash(&second);

    let third = fixture.path().join("replacement-bootstrap.marker");
    node1.restart_at("transfer.after_fenced_bootstrap", &third);
    wait_ready(&node1_addr, &mut node1);
    node1.wait_for_injected_crash(&third);

    node1.restart_without_failpoint();
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);
    wait_catalog(&node1_addr, Duration::from_secs(45), |catalog| {
        operation_complete(catalog, OperationKind::Transfer)
    });
    assert_exact_rows(&node1_addr);
}

#[test]
#[ignore = "process-crash qualification matrix; run explicitly with --ignored"]
fn joint_voter_changes_finalize_automatically_after_restart() {
    for failpoint in ["voter.after_joint_commit", "voter.after_finalize_commit"] {
        if selected(&[failpoint]).is_empty() {
            continue;
        }
        eprintln!("qualifying {failpoint}");
        qualify_voter_change(failpoint);
    }
}

fn qualify_split(failpoint: &str) {
    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, 1);

    let marker = fixture.path().join("crash.marker");
    let mut node1 = ServerProcess::start(&binary, &node1_dir, Some(failpoint), &marker);
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);
    seed_rows(&node1_addr);
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = ServerProcess::start(&binary, &node2_dir, None, Path::new("unused"));
    wait_ready(&node2_addr, &mut node2);

    node1.wait_for_injected_crash(&marker);
    node1.restart_without_failpoint();
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);

    let catalog = wait_catalog(&node1_addr, Duration::from_secs(45), |catalog| {
        operation_complete(catalog, OperationKind::Split)
    });
    assert_eq!(catalog.routing.shard_count(), 2);
    assert_exact_rows(&node1_addr);
}

fn qualify_transfer(failpoint: &str) {
    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, 2);

    let marker = fixture.path().join("crash.marker");
    let destination_crash = failpoint.starts_with("transfer.destination.");
    let mut node1 = ServerProcess::start(
        &binary,
        &node1_dir,
        (!destination_crash).then_some(failpoint),
        &marker,
    );
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);
    seed_rows(&node1_addr);
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = ServerProcess::start(
        &binary,
        &node2_dir,
        destination_crash.then_some(failpoint),
        &marker,
    );
    wait_ready(&node2_addr, &mut node2);

    let crashed = if destination_crash {
        &mut node2
    } else {
        &mut node1
    };
    crashed.wait_for_injected_crash(&marker);
    crashed.restart_without_failpoint();
    if destination_crash {
        wait_ready(&node2_addr, &mut node2);
    } else {
        wait_ready(&node1_addr, &mut node1);
        wait_leader(&node1_addr);
    }

    let catalog = wait_catalog(&node1_addr, Duration::from_secs(45), |catalog| {
        operation_complete(catalog, OperationKind::Transfer)
    });
    assert!(
        catalog
            .placements
            .values()
            .any(|placement| placement.primary == 2),
        "completed rebalance did not activate node 2: {:?}",
        catalog.placements
    );
    assert_exact_rows(&node1_addr);
}

fn qualify_interrupted_join(failpoint: &str) {
    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, 1);

    let marker = fixture.path().join("crash.marker");
    let mut node1 = ServerProcess::start(&binary, &node1_dir, Some(failpoint), &marker);
    wait_ready(&node1_addr, &mut node1);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = String::new();
    while !marker.exists() {
        let failed = Command::new(&binary)
            .args([
                "join",
                node2_dir.to_str().unwrap(),
                "--seed",
                &node1_addr,
                "--node-id",
                "2",
                "--listen",
                &node2_addr,
                "--capacity",
                "4",
            ])
            .output()
            .unwrap();
        assert!(
            !failed.status.success(),
            "join unexpectedly succeeded across injected crash"
        );
        last_error = String::from_utf8_lossy(&failed.stderr).into_owned();
        assert!(
            Instant::now() < deadline,
            "join never reached {failpoint}; last error: {last_error}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("interrupted join stderr: {last_error}");
    node1.wait_for_injected_crash(&marker);
    node1.restart_without_failpoint();
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);

    // The leader first recovers the prepared/committed learner registration. Reissuing join is
    // idempotent and installs that exact durable catalog into the destination directory.
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = ServerProcess::start(&binary, &node2_dir, None, Path::new("unused"));
    wait_ready(&node2_addr, &mut node2);
    let catalog = wait_catalog(&node1_addr, Duration::from_secs(30), |catalog| {
        catalog
            .members
            .get(&2)
            .is_some_and(|member| member.role == MemberRole::Storage)
            && operation_complete(catalog, OperationKind::Join)
    });
    assert_eq!(catalog.members[&2].role, MemberRole::Storage);
}

fn qualify_voter_change(failpoint: &str) {
    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, 2);

    let marker = fixture.path().join("voter.marker");
    let mut node1 = ServerProcess::start(&binary, &node1_dir, Some(failpoint), &marker);
    wait_ready(&node1_addr, &mut node1);
    wait_leader(&node1_addr);
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = ServerProcess::start(&binary, &node2_dir, None, Path::new("unused"));
    wait_ready(&node2_addr, &mut node2);
    // Let the storage member observe the current leader heartbeat before it is asked to prepare
    // the entry that promotes it into the joint voter set.
    std::thread::sleep(Duration::from_secs(1));

    let deadline = Instant::now() + Duration::from_secs(15);
    while !marker.exists() {
        if let Ok(mut client) = Client::connect_with(&node1_addr, Duration::from_millis(500)) {
            let _ = client.catalog_command(CatalogCommand::BeginVoterChange {
                voters: [1, 2].into_iter().collect(),
            });
        }
        assert!(
            Instant::now() < deadline,
            "voter transition never reached {failpoint}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    node1.wait_for_injected_crash(&marker);
    let mut surviving = Client::connect_with(&node2_addr, Duration::from_secs(1)).unwrap();
    let (survivor_catalog, _) = surviving.catalog_state().unwrap();
    assert!(
        survivor_catalog.voter_transition.is_some() || failpoint == "voter.after_finalize_commit",
        "new voter did not durably receive the joint configuration before leader crash: \
         {survivor_catalog:?}"
    );
    node1.restart_without_failpoint();
    wait_ready(&node1_addr, &mut node1);

    let catalog = wait_catalog(&node1_addr, Duration::from_secs(30), |catalog| {
        catalog.voter_transition.is_none()
            && catalog
                .members
                .get(&1)
                .is_some_and(|member| member.role == MemberRole::Voter)
            && catalog
                .members
                .get(&2)
                .is_some_and(|member| member.role == MemberRole::Voter)
    });
    assert!(catalog.voter_transition.is_none());
}

fn selected<'a>(all: &'a [&'a str]) -> Vec<&'a str> {
    match std::env::var("SHARDLITE_QUALIFY_FAILPOINT") {
        Ok(one) => all
            .iter()
            .copied()
            .filter(|candidate| *candidate == one)
            .collect(),
        Err(_) => all.to_vec(),
    }
}

fn spawn_server(binary: &Path, dir: &Path, failpoint: Option<&str>, marker: &Path) -> Child {
    let mut command = Command::new(binary);
    command.args(["serve", dir.to_str().unwrap()]);
    if std::env::var_os("SHARDLITE_QUALIFY_LOG").is_some() {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }
    if let Some(failpoint) = failpoint {
        command
            .env("SHARDLITE_FAILPOINT", failpoint)
            .env("SHARDLITE_FAILPOINT_MARKER", marker);
    }
    command.spawn().expect("starting shardlite server")
}

fn init(binary: &Path, dir: &Path, address: &str, initial_shards: u32) {
    let output = Command::new(binary)
        .args([
            "init",
            dir.to_str().unwrap(),
            "--listen",
            address,
            "--initial-shards",
            &initial_shards.to_string(),
            "--capacity",
            "4",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn join(binary: &Path, dir: &Path, seed: &str, address: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = Command::new(binary)
            .args([
                "join",
                dir.to_str().unwrap(),
                "--seed",
                seed,
                "--node-id",
                "2",
                "--listen",
                address,
                "--capacity",
                "4",
            ])
            .output()
            .unwrap();
        if output.status.success() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "join did not converge: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn seed_rows(address: &str) {
    let mut client = Client::connect_with(address, Duration::from_secs(2)).unwrap();
    client
        .execute_all("CREATE TABLE users (id TEXT PRIMARY KEY, value INTEGER NOT NULL) STRICT")
        .unwrap();
    for (key, value) in [("alpha", 1), ("echo", 2), ("foxtrot", 3), ("hotel", 4)] {
        client
            .run(&format!(
                "INSERT INTO users (id, value) VALUES ('{key}', {value})"
            ))
            .unwrap();
    }
}

fn assert_exact_rows(address: &str) {
    let result = wait_query(address, Duration::from_secs(30));
    assert_eq!(
        result,
        vec![
            vec![Value::Text("alpha".into()), Value::Integer(1)],
            vec![Value::Text("echo".into()), Value::Integer(2)],
            vec![Value::Text("foxtrot".into()), Value::Integer(3)],
            vec![Value::Text("hotel".into()), Value::Integer(4)],
        ]
    );
}

fn wait_query(address: &str, within: Duration) -> Vec<Vec<Value>> {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(300))
            && let Ok(result) = client.query_all("SELECT id, value FROM users ORDER BY id")
        {
            return result.rows;
        }
        assert!(
            Instant::now() < deadline,
            "distributed query did not recover"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn operation_complete(catalog: &Catalog, kind: OperationKind) -> bool {
    catalog
        .operations
        .values()
        .any(|operation| operation.kind == kind && operation.phase == OperationPhase::Complete)
}

fn wait_catalog(address: &str, within: Duration, predicate: impl Fn(&Catalog) -> bool) -> Catalog {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(300))
            && let Ok((catalog, _)) = client.catalog_state()
            && predicate(&catalog)
        {
            return catalog;
        }
        assert!(
            Instant::now() < deadline,
            "catalog did not reach the required repaired state"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_ready(address: &str, process: &mut ServerProcess) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Client::connect_with(address, Duration::from_millis(200)).is_ok() {
            return;
        }
        if let Some(status) = process
            .child
            .as_mut()
            .expect("server process")
            .try_wait()
            .expect("checking server")
        {
            panic!("server exited before becoming ready: {status}");
        }
        assert!(Instant::now() < deadline, "server never became ready");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn wait_leader(address: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(300))
            && client
                .catalog_command(CatalogCommand::Cordon {
                    node: 1,
                    cordoned: false,
                })
                .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "node never became shard leader");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}
