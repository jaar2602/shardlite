//! Peak-RSS check: does resident memory stay bounded as shards and data grow?
//!
//! The claim under test is the one the whole floor profile rests on — that a 64-shard
//! database of arbitrary size runs inside a ~150 MB container, because only a bounded
//! number of connections are ever resident.
//!
//! Usage:
//!   memcheck <dir> [shards] [mb_per_shard] [rss_limit_mb] [capture:0|1]
//!
//! Reports peak RSS (`VmHWM`, the kernel's high-water mark — not a sample, so a transient
//! spike cannot be missed) and exits non-zero if it exceeds the limit.
//!
//! Run it under a real memory cap to make the failure mode an OOM kill rather than a
//! number nobody reads:
//!   systemd-run --user --scope -p MemoryMax=150M -- memcheck /tmp/x 64 4 150
//!   docker run --cpus=1 --memory=1g ...

use std::process::ExitCode;

use meshdb::replication::NullSink;
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::exec::{Statement, Value};

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

fn dir_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = std::path::PathBuf::from(args.first().cloned().unwrap_or("/tmp/meshdb-mem".into()));
    let shards: u32 = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(64);
    let mb_per_shard: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(4);
    let limit_mb: u64 = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(150);
    let capture: bool = args.get(4).map(|v| v == "1").unwrap_or(false);

    let _ = std::fs::remove_dir_all(&dir);

    let cfg = ShardConfig {
        shard_count: shards,
        capture,
        ..ShardConfig::floor()
    };
    println!(
        "shards={shards} target={}MB/shard ({}MB total) writers={}x{} readers={}x{} limit={limit_mb}MB",
        mb_per_shard,
        mb_per_shard * shards as usize,
        cfg.writer_threads,
        cfg.open_writers_per_thread,
        cfg.reader_threads,
        cfg.open_readers_per_thread,
    );
    println!("capture={capture}");

    let sink = std::sync::Arc::new(NullSink::new());
    let m = match ShardManager::open_with_sink(&dir, cfg, Some(sink.clone())) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = m.execute_all_shards("CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT")
    {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }

    // ~1 KiB rows, submitted in batches so the run is not dominated by round trips.
    let row = vec![0u8; 1024];
    let rows_per_shard = mb_per_shard * 1024;
    let start = std::time::Instant::now();

    for s in 0..shards {
        let shard = ShardId(s);
        for chunk_start in (0..rows_per_shard).step_by(64) {
            let stmts: Vec<Statement> = (chunk_start..(chunk_start + 64).min(rows_per_shard))
                .map(|i| {
                    Statement::with_params(
                        "INSERT INTO t VALUES (?1, ?2)",
                        vec![Value::Integer(i as i64), Value::Blob(row.clone())],
                    )
                })
                .collect();
            if let Err(e) = m.execute(shard, stmts) {
                eprintln!("error writing {shard}: {e}");
                return ExitCode::FAILURE;
            }
        }
        if s % 16 == 0 {
            println!(
                "  {shard}: rss={}MB on_disk={}MB",
                peak_rss_kb() / 1024,
                dir_bytes(&dir) / 1024 / 1024
            );
        }
    }

    // Reads touch every shard again, exercising the reader LRU as well.
    for s in 0..shards {
        if m.query(ShardId(s), "SELECT count(*) FROM t").is_err() {
            eprintln!("error reading shard {s}");
            return ExitCode::FAILURE;
        }
    }

    let rss_mb = peak_rss_kb() / 1024;
    let disk_mb = dir_bytes(&dir) / 1024 / 1024;
    let ws = m.writer_stats();
    let rs = m.reader_stats();

    println!("\n--- result ---");
    println!("elapsed        {:.1}s", start.elapsed().as_secs_f64());
    println!("on disk        {disk_mb} MB across {shards} shards");
    println!("peak RSS       {rss_mb} MB   (limit {limit_mb} MB)");
    println!(
        "writer shards  open_now={} opens={} evictions={}",
        ws.open_now, ws.shard_opens, ws.shard_evictions
    );
    println!(
        "reader shards  opens={} evictions={}",
        rs.shard_opens, rs.shard_evictions
    );
    if capture {
        let (txns, frames, bytes) = sink.counts();
        println!(
            "captured       {txns} txns, {frames} frames, {} MB shipped to sink",
            bytes / 1024 / 1024
        );
    }

    let _ = std::fs::remove_dir_all(&dir);

    if rss_mb > limit_mb {
        eprintln!("\nFAIL: peak RSS {rss_mb} MB exceeds the {limit_mb} MB limit");
        return ExitCode::FAILURE;
    }
    println!("\nPASS: peak RSS stayed within {limit_mb} MB");
    ExitCode::SUCCESS
}
