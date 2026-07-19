//! What does routing through the capture VFS actually cost?
//!
//! Same workload twice: default VFS, then the capture VFS. Reports throughput and the
//! memory the capture accumulates.
use meshdb::rusqlite::{Connection, OpenFlags, params};
use meshdb::vfs;
use std::time::Instant;

const ROWS: usize = 20_000;
const BATCH: usize = 64;

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        })
        .unwrap_or(0)
}

fn setup(conn: &Connection) {
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT")
        .unwrap();
}

fn workload(conn: &Connection) -> f64 {
    let pad = vec![0u8; 512];
    let t0 = Instant::now();
    for chunk in (0..ROWS).step_by(BATCH) {
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        for i in chunk..(chunk + BATCH).min(ROWS) {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", params![i as i64, &pad])
                .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
    }
    ROWS as f64 / t0.elapsed().as_secs_f64()
}

fn main() {
    let dir = std::env::temp_dir().join(format!("vfsover{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // --- default VFS ---
    let p1 = dir.join("plain.db");
    let c1 = Connection::open(&p1).unwrap();
    setup(&c1);
    let plain = workload(&c1);
    let rss_after_plain = peak_rss_kb();
    drop(c1);

    // --- capture VFS ---
    let p2 = dir.join("captured.db");
    let cap = vfs::capture_for(&p2).unwrap();
    let c2 = Connection::open_with_flags_and_vfs(
        &p2,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs::VFS_NAME,
    )
    .unwrap();
    setup(&c2);
    let captured = workload(&c2);
    let rss_after_capture = peak_rss_kb();

    let st = cap.stats();
    let txns = cap.drain_committed();
    let frames: usize = txns.iter().map(|t| t.frames.len()).sum();
    let bytes: usize = txns
        .iter()
        .flat_map(|t| t.frames.iter())
        .map(|f| f.data.len())
        .sum();

    println!("rows={ROWS}  batch={BATCH}");
    println!("default VFS   {plain:>10.0} writes/s");
    println!(
        "capture VFS   {captured:>10.0} writes/s   ({:.1}% of default)",
        captured / plain * 100.0
    );
    println!();
    println!(
        "captured      {} txns, {frames} frames, {:.1} MB held in memory",
        txns.len(),
        bytes as f64 / 1024.0 / 1024.0
    );
    println!("header rewrites observed: {}", st.header_rewrites);
    println!(
        "RSS after plain {} MB -> after capture {} MB",
        rss_after_plain / 1024,
        rss_after_capture / 1024
    );
    std::fs::remove_dir_all(&dir).ok();
}
