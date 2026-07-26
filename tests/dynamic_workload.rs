//! Long-running differential workload qualification for dynamic topology changes.
//!
//! This is intentionally an ignored, process-level test.  It is the workload companion to
//! `dynamic_crash`: unlike the boundary matrix, it keeps clients active while a split/transfer is
//! in progress, repeatedly loses the destination replica, and restarts the coordinator.  Run it
//! explicitly with:
//!
//! ```text
//! cargo test --features failpoints --test dynamic_workload -- --ignored --nocapture
//! ```

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use shardlite::cluster::{CatalogCommand, OperationKind, OperationPhase};
use shardlite::net::{Client, RunResult};
use shardlite::storage::Value;
use tempfile::TempDir;

struct Node {
    binary: PathBuf,
    dir: PathBuf,
    child: Option<Child>,
}

impl Node {
    fn start(binary: &Path, dir: &Path) -> Self {
        let child = spawn(binary, dir);
        Self {
            binary: binary.to_path_buf(),
            dir: dir.to_path_buf(),
            child: Some(child),
        }
    }

    fn wait_ready(&mut self, address: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if Client::connect_with(address, Duration::from_millis(250)).is_ok() {
                return;
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("node process")
                .try_wait()
                .expect("checking node process")
            {
                panic!("node exited before becoming ready: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "node never became ready at {address}"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    /// Simulate loss of a replica/coordinator and restart the same durable node.
    fn crash_and_restart(&mut self, address: &str) {
        let mut child = self.child.take().expect("running node");
        child.kill().expect("killing node for replica-loss test");
        let _ = child.wait();
        self.child = Some(spawn(&self.binary, &self.dir));
        self.wait_ready(address);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Keep a write stream alive through split/transfer and process restarts.  Every acknowledged
/// statement is applied to an independent SQLite connection, which is returned as the exact
/// logical answer to compare after movement settles.
#[test]
#[ignore = "long-running process qualification; run explicitly with --ignored"]
fn concurrent_differential_workload_survives_move_failover_and_replica_loss() {
    let selected = std::env::var("SHARDLITE_QUALIFY_OPERATION").ok();
    if selected.as_deref().is_none_or(|value| value == "split") {
        qualify_workload(1);
    }
    if selected.as_deref().is_none_or(|value| value == "transfer") {
        qualify_workload(4);
    }
}

fn qualify_workload(initial_shards: u32) {
    let fixture = TempDir::new().unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_shardlite"));
    let node1_dir = fixture.path().join("node-1");
    let node2_dir = fixture.path().join("node-2");
    let node1_addr = free_addr();
    let node2_addr = free_addr();
    init(&binary, &node1_dir, &node1_addr, initial_shards);

    let mut node1 = Node::start(&binary, &node1_dir);
    node1.wait_ready(&node1_addr);
    wait_leader(&node1_addr);
    create_table(&node1_addr);

    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(AtomicUsize::new(0));
    let reader_errors = Arc::new(AtomicUsize::new(0));

    let writer_stop = Arc::clone(&stop);
    let writer_acknowledged = Arc::clone(&acknowledged);
    let writer_address = node1_addr.clone();
    let writer =
        thread::spawn(move || writer_oracle(&writer_address, writer_stop, writer_acknowledged));

    let reader_stop = Arc::clone(&stop);
    let reader_errors_clone = Arc::clone(&reader_errors);
    let reader_address = node1_addr.clone();
    let reader =
        thread::spawn(move || reader_loop(&reader_address, reader_stop, reader_errors_clone));

    // Joining a second member causes the planner to choose either a transfer (multiple active
    // shards) or an online split (a single active shard).  The workload is deliberately started
    // before the join so snapshot, backfill, replay, and fencing all race with real clients.
    join(&binary, &node2_dir, &node1_addr, &node2_addr);
    let mut node2 = Node::start(&binary, &node2_dir);
    node2.wait_ready(&node2_addr);
    let operation = wait_operation_active(&node1_addr, Duration::from_secs(30));

    // Lose the destination twice.  The first interruption is expected during bootstrap; the
    // second also covers the catch-up/replay path when the operation takes longer than one
    // restart.  If the operation completed between checks, this still exercises replica loss and
    // recovery of the completed placement.
    for _ in 0..2 {
        node2.crash_and_restart(&node2_addr);
        thread::sleep(Duration::from_millis(150));
    }
    wait_operation_complete(&node1_addr, operation, Duration::from_secs(60));

    // Restart the coordinator while clients are still active.  Requests may fail briefly while
    // the listener is down, but the writer retries and only advances the oracle after an explicit
    // acknowledgement.  This is the zero-downtime contract at the client boundary.
    node1.crash_and_restart(&node1_addr);
    wait_leader(&node1_addr);
    thread::sleep(Duration::from_millis(500));

    stop.store(true, Ordering::Relaxed);
    let expected = writer.join().expect("writer thread");
    reader.join().expect("reader thread");

    assert!(
        acknowledged.load(Ordering::Relaxed) >= 40,
        "workload was vacuous: only {} acknowledged writes",
        acknowledged.load(Ordering::Relaxed)
    );
    assert_eq!(
        reader_errors.load(Ordering::Relaxed),
        0,
        "successful reads returned malformed or duplicate rows"
    );
    assert_rows_equal(&node1_addr, &expected);
    assert_rows_equal(&node2_addr, &expected);
}

fn writer_oracle(
    address: &str,
    stop: Arc<AtomicBool>,
    acknowledged: Arc<AtomicUsize>,
) -> Vec<Vec<Value>> {
    let oracle = Connection::open_in_memory().expect("oracle sqlite");
    oracle
        .execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT")
        .expect("oracle schema");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut sequence = 0u64;
    while !stop.load(Ordering::Relaxed) || sequence < 60 {
        assert!(
            Instant::now() < deadline,
            "writer could not make progress through topology changes"
        );
        let id = format!("live-{sequence:08}");
        let insert = format!("INSERT INTO users (id, value) VALUES ('{id}', 'v-{sequence:08}')");
        apply_until_ack(address, &oracle, &insert);
        acknowledged.fetch_add(1, Ordering::Relaxed);

        if sequence > 2 && sequence.is_multiple_of(3) {
            let target = sequence - 1;
            let sql = format!(
                "UPDATE users SET value = 'updated-{sequence:08}' WHERE id = 'live-{target:08}'"
            );
            apply_until_ack(address, &oracle, &sql);
            acknowledged.fetch_add(1, Ordering::Relaxed);
        }
        if sequence > 5 && sequence.is_multiple_of(5) {
            let target = sequence - 2;
            let sql = format!("DELETE FROM users WHERE id = 'live-{target:08}'");
            apply_until_ack(address, &oracle, &sql);
            acknowledged.fetch_add(1, Ordering::Relaxed);
        }
        sequence += 1;
        thread::sleep(Duration::from_millis(3));
    }

    let mut rows = oracle
        .prepare("SELECT id, value FROM users ORDER BY id")
        .expect("oracle query");
    rows.query_map([], |row| {
        Ok(vec![
            Value::Text(row.get::<_, String>(0)?),
            Value::Text(row.get::<_, String>(1)?),
        ])
    })
    .expect("oracle rows")
    .collect::<rusqlite::Result<Vec<_>>>()
    .expect("oracle row conversion")
}

fn reader_loop(address: &str, stop: Arc<AtomicBool>, errors: Arc<AtomicUsize>) {
    while !stop.load(Ordering::Relaxed) {
        let Ok(mut client) = Client::connect_with(address, Duration::from_millis(500)) else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        let Ok(RunResult::Rows(rows)) = client.run("SELECT id, value FROM users ORDER BY id")
        else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        let mut ids = std::collections::BTreeSet::new();
        for row in rows.rows {
            if row.len() != 2 {
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
            let Value::Text(id) = &row[0] else {
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            };
            if !ids.insert(id.clone()) {
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
            let Value::Text(value) = &row[1] else {
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            };
            if !id.starts_with("live-")
                || !(value.starts_with("v-") || value.starts_with("updated-"))
            {
                errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
}

fn apply_until_ack(address: &str, oracle: &Connection, sql: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(500))
            && matches!(client.run(sql), Ok(RunResult::Changed { .. }))
        {
            oracle.execute_batch(sql).expect("oracle write");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "write never became acknowledgeable: {sql}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_rows_equal(address: &str, expected: &[Vec<Value>]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(500))
            && let Ok(RunResult::Rows(rows)) = client.run("SELECT id, value FROM users ORDER BY id")
            && rows.rows == expected
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{address} never matched the SQLite oracle"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_operation_active(address: &str, within: Duration) -> (u64, OperationKind) {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(500))
            && let Ok((catalog, _)) = client.catalog_state()
            && let Some(operation) = catalog.operations.values().find(|operation| {
                matches!(
                    operation.kind,
                    OperationKind::Transfer | OperationKind::Split
                ) && !operation.phase.terminal()
            })
        {
            return (operation.id, operation.kind);
        }
        assert!(
            Instant::now() < deadline,
            "move/split operation never became active"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_operation_complete(address: &str, (id, kind): (u64, OperationKind), within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(mut client) = Client::connect_with(address, Duration::from_millis(500))
            && let Ok((catalog, _)) = client.catalog_state()
            && catalog.operations.get(&id).is_some_and(|operation| {
                operation.kind == kind && operation.phase == OperationPhase::Complete
            })
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "move/split operation {id} never completed"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn create_table(address: &str) {
    let mut client = Client::connect_with(address, Duration::from_secs(2)).expect("table client");
    client
        .execute_all("CREATE TABLE users (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT")
        .expect("creating workload table");
}

fn init(binary: &Path, dir: &Path, address: &str, shards: u32) {
    let output = Command::new(binary)
        .args([
            "init",
            dir.to_str().unwrap(),
            "--listen",
            address,
            "--initial-shards",
            &shards.to_string(),
            "--capacity",
            "4",
        ])
        .output()
        .expect("init command");
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn join(binary: &Path, dir: &Path, seed: &str, address: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
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
            .expect("join command");
        if output.status.success() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "join did not converge: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn spawn(binary: &Path, dir: &Path) -> Child {
    Command::new(binary)
        .args(["serve", dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("starting shardlite server")
}

fn wait_leader(address: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
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
        assert!(
            Instant::now() < deadline,
            "node never became catalog leader"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
    listener.local_addr().expect("port address").to_string()
}
