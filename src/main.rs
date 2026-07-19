//! meshdb CLI — open a data directory and run SQL against it.

use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use meshdb::db::Db;
use meshdb::shard::ShardId;
use meshdb::storage::exec::{Executed, Outcome, QueryResult};

const USAGE: &str = "\
meshdb — HA multi-write SQLite server (single-node)

usage:
  meshdb <data-dir> [--shards N]        interactive shell
  meshdb <data-dir> -c \"SQL\"            run one statement and exit
  meshdb <data-dir> -f <file>           run statements from a file, one per line

--shards N applies only when creating a new directory. It is recorded in the
manifest and IMMUTABLE: changing it re-routes every key.

shell commands:
  .help     show this message
  .info     manifest and shard layout
  .shard N  target shard N for subsequent statements (default 0)
  .stats    writer, reader and WAL statistics
  .tables   list tables on the current shard
  .quit     exit

DDL (CREATE / DROP / ALTER) is applied to every shard automatically. Other
statements go to the current shard; there is no cross-shard query planner yet.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        eprintln!("{USAGE}");
        return if args.is_empty() {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        };
    }

    let dir = std::path::PathBuf::from(&args[0]);
    let mut shards = 1u32;
    let mut rest: Vec<&str> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--shards" => match args.get(i + 1).and_then(|v| v.parse::<u32>().ok()) {
                Some(n) => {
                    shards = n;
                    i += 2;
                }
                None => {
                    eprintln!("error: --shards needs a number\n{USAGE}");
                    return ExitCode::FAILURE;
                }
            },
            other => {
                rest.push(other);
                i += 1;
            }
        }
    }

    let db = match Db::open(&dir, shards) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    match rest.first().copied() {
        Some("-c") => match rest.get(1) {
            Some(sql) => {
                if run_and_print(&db, ShardId::FIRST, sql) {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            None => {
                eprintln!("error: -c requires a SQL argument\n{USAGE}");
                ExitCode::FAILURE
            }
        },
        Some("-f") => match rest.get(1) {
            Some(f) => run_file(&db, f),
            None => {
                eprintln!("error: -f requires a file argument\n{USAGE}");
                ExitCode::FAILURE
            }
        },
        Some(other) => {
            eprintln!("error: unknown option {other}\n{USAGE}");
            ExitCode::FAILURE
        }
        None => repl(&db),
    }
}

fn run_file(db: &Db, file: &str) -> ExitCode {
    let content = match std::fs::read_to_string(file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: reading {file}: {e}");
            return ExitCode::FAILURE;
        }
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        if !run_and_print(db, ShardId::FIRST, line) {
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn repl(db: &Db) -> ExitCode {
    println!(
        "meshdb — {} ({} shard(s))",
        db.dir().display(),
        db.shard_count()
    );
    println!("type .help for commands, .quit to exit");

    let mut current = ShardId::FIRST;
    let stdin = io::stdin();
    loop {
        print!("meshdb:{current}> ");
        if io::stdout().flush().is_err() {
            return ExitCode::FAILURE;
        }

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                println!();
                return ExitCode::SUCCESS;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }

        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }

        match sql {
            ".quit" | ".exit" => return ExitCode::SUCCESS,
            ".help" => {
                println!("{USAGE}");
                continue;
            }
            ".info" => {
                let m = db.manifest();
                println!("dir           {}", db.dir().display());
                println!("format        {}", m.format_version);
                println!("shard_count   {} (immutable)", m.shard_count);
                println!("sqlite        {}", m.sqlite_version);
                continue;
            }
            ".stats" => {
                let w = db.writer_stats();
                let r = db.reader_stats();
                let c = db.shards().checkpoint_stats();
                println!(
                    "writer: threads={} batches={} requests={} max_batch={} mean_batch={:.2}",
                    w.threads,
                    w.batches,
                    w.requests,
                    w.max_batch,
                    w.mean_batch()
                );
                println!(
                    "shards: open_now={} opens={} evictions={}",
                    w.open_now, w.shard_opens, w.shard_evictions
                );
                println!(
                    "reader: threads={} queries={} rejected_busy={} timed_out={}",
                    r.threads, r.queries, r.rejected_busy, r.timed_out
                );
                println!(
                    "wal:    bytes={} passive={} truncated={} stalls={} failures={}",
                    c.wal_bytes, c.passive, c.truncated, c.stalls, c.failures
                );
                continue;
            }
            ".tables" => {
                run_and_print(
                    db,
                    current,
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                );
                continue;
            }
            _ => {}
        }

        if let Some(n) = sql.strip_prefix(".shard ") {
            match n.trim().parse::<u32>() {
                Ok(n) if n < db.shard_count() => {
                    current = ShardId(n);
                    println!("targeting {current}");
                }
                Ok(n) => eprintln!(
                    "error: shard {n} is outside the {} configured",
                    db.shard_count()
                ),
                Err(_) => eprintln!("error: .shard needs a number"),
            }
            continue;
        }
        if sql.starts_with('.') {
            eprintln!("error: unknown command {sql}");
            continue;
        }

        run_and_print(db, current, sql);
    }
}

/// Returns false if the statement failed.
fn run_and_print(db: &Db, shard: ShardId, sql: &str) -> bool {
    // Schema changes must reach every shard, so they are never routed to just one.
    if Db::is_ddl(sql) && db.shard_count() > 1 {
        return match db.run_all(sql) {
            Ok(results) => {
                let failed: Vec<_> = results
                    .iter()
                    .filter(|(_, o)| matches!(o, Outcome::Rejected(_)))
                    .collect();
                if failed.is_empty() {
                    println!("ok (applied to {} shards)", results.len());
                    true
                } else {
                    // Not atomic across shards: say exactly which ones diverged.
                    eprintln!(
                        "PARTIAL: {} of {} shards rejected the statement; schemas now differ",
                        failed.len(),
                        results.len()
                    );
                    for (id, o) in failed.iter().take(5) {
                        if let Outcome::Rejected(m) = o {
                            eprintln!("  {id}: {m}");
                        }
                    }
                    false
                }
            }
            Err(e) => {
                eprintln!("error: {e}");
                false
            }
        };
    }

    match db.run_on(shard, sql) {
        Ok(Outcome::Ok(Executed::Rows(result))) => {
            print_table(&result);
            true
        }
        Ok(Outcome::Ok(Executed::Changed(w))) => {
            println!(
                "ok ({} row{} affected, last_insert_rowid={})",
                w.rows_affected,
                if w.rows_affected == 1 { "" } else { "s" },
                w.last_insert_rowid
            );
            true
        }
        Ok(Outcome::Rejected(msg)) => {
            eprintln!("rejected: {msg}");
            false
        }
        Err(e) => {
            eprintln!("error: {e}");
            false
        }
    }
}

fn print_table(r: &QueryResult) {
    if r.columns.is_empty() {
        return;
    }
    let cells: Vec<Vec<String>> = r
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.render()).collect())
        .collect();
    let widths: Vec<usize> = r
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|row| row.get(i).map_or(0, |s| s.chars().count()))
                .chain(std::iter::once(c.chars().count()))
                .max()
                .unwrap_or(0)
        })
        .collect();

    let header: Vec<String> = r
        .columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!("{c:<w$}"))
        .collect();
    println!("{}", header.join("  "));
    println!(
        "{}",
        widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in &cells {
        let line: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!("{v:<w$}"))
            .collect();
        println!("{}", line.join("  "));
    }
    println!(
        "({} row{})",
        r.rows.len(),
        if r.rows.len() == 1 { "" } else { "s" }
    );
}
