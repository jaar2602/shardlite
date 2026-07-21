//! meshdb CLI — open a data directory and run SQL against it.

use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use meshdb::db::Db;
use meshdb::query::{Route, route_statement};
use meshdb::shard::ShardId;
use meshdb::shard::manifest::Manifest;
use meshdb::storage::exec::{Executed, Outcome, QueryResult};

const USAGE: &str = "\
meshdb — HA multi-write SQLite server (single-node)

usage:
  meshdb <data-dir> --shards N          create a new data directory
  meshdb <data-dir>                     interactive shell (existing directory)
  meshdb <data-dir> -c \"SQL\"            run one statement and exit
  meshdb <data-dir> -f <file>           run statements from a file, one per line

--shards N is REQUIRED when creating a new directory and is refused for an
existing one. It is recorded in the manifest and cannot be changed afterwards,
because changing it re-routes every key. There is deliberately no default: a
value you cannot revise should not be one you got by accident.

  1        single database; no cross-shard concerns, and no way to scale later
  16-64    typical; 64 covers roughly 100 MB to 1 TB
  256      maximum

A write is routed to the shard its declared shard key hashes to (see the
`shardkey` command), so writes spread across shards automatically; a multi-row
INSERT is split per shard. A point read/update/delete on the shard key reaches
just the shard holding the row. Without a declared shard key a write falls back
to the current shard (see .shard).

shell commands:
  .help     show this message
  .info     manifest and shard layout
  .shard N  target shard N for subsequent statements (default 0)
  .stats    writer, reader and WAL statistics
  .vacuum   rebuild the current shard, reclaiming free pages
  .tables   list tables on the current shard
  .quit     exit

DDL (CREATE / DROP / ALTER) is applied to every shard automatically.

Reads fan out across all shards and are merged. GROUP BY, DISTINCT, AVG, OFFSET,
UNION/INTERSECT/EXCEPT, subqueries and JOINs (co-located, or materialised
centrally) are all combined correctly. A shape that cannot be — GROUP_CONCAT, a
correlated subquery, a source over the materialisation cap — is refused rather
than answered wrongly; run those against one shard with .shard N.

Note that a fan-out is not a consistent snapshot: each shard is read at its own
moment, and there is no cross-shard atomicity in this design.";

fn main() -> ExitCode {
    init_tracing();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        eprintln!("{USAGE}");
        return if args.is_empty() {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        };
    }

    match args[0].as_str() {
        "serve" => return serve_cmd(&args),
        "user" => return user_cmd(&args),
        "frames" => return frames_cmd(&args),
        "shardkey" => return shardkey_cmd(&args),
        _ => {}
    }

    let dir = std::path::PathBuf::from(&args[0]);
    let mut requested: Option<u32> = None;
    let mut rest: Vec<&str> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--shards" => match args.get(i + 1).and_then(|v| v.parse::<u32>().ok()) {
                Some(n) => {
                    requested = Some(n);
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

    // Shard count is immutable once data exists, so it is never defaulted. Being asked once
    // costs nothing; getting it wrong by omission costs a migration.
    let manifest_path = Manifest::path(&dir);
    let on_disk = manifest_path
        .exists()
        .then(|| Manifest::read(&manifest_path))
        .transpose();
    let shards = match on_disk {
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
        Ok(Some(m)) => match requested {
            None => m.shard_count,
            Some(n) if n == m.shard_count => m.shard_count,
            Some(n) => {
                eprintln!(
                    "error: {} already holds data for {} shards; --shards {n} cannot change \
                     that.\nShard count is fixed at creation because changing it re-routes \
                     every key. Omit --shards to open it, or create a new directory.",
                    dir.display(),
                    m.shard_count
                );
                return ExitCode::FAILURE;
            }
        },
        Ok(None) => match requested {
            Some(n) => n,
            None => {
                eprintln!(
                    "error: {} does not exist yet, so --shards N is required.\n\n\
                     The shard count is recorded at creation and cannot be changed \
                     afterwards — changing it re-routes every key, making existing rows \
                     unreachable. There is no default because a value you cannot revise \
                     should not be one you got by accident.\n\n\
                     \x20 --shards 1     single database; nothing to scale later\n\
                     \x20 --shards 64    typical; covers roughly 100 MB to 1 TB\n\
                     \x20 --shards 256   maximum",
                    dir.display()
                );
                return ExitCode::FAILURE;
            }
        },
    };

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
                let wc = meshdb::storage::wal_conversion_stats();
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
                // Retrying the WAL conversion hides contention unless it is counted.
                println!(
                    "open:   wal_retries={} contended={} failed={} max_wait={}ms",
                    wc.retries, wc.contended_opens, wc.failed_opens, wc.max_wait_ms
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

        if sql == ".vacuum" {
            println!("rebuilding {current} (needs ~2x its size in free disk)...");
            match db.vacuum(current) {
                Ok(()) => println!("ok"),
                Err(e) => eprintln!("error: {e}"),
            }
            continue;
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

    // Route by the declared shard key so a write spreads across shards instead of all landing on
    // the current shard (0 by default), and a point read/update/delete reaches the one shard that
    // holds the row. A statement with no declared key, or a non-point read, falls through
    // (Passthrough) to the fan-out / current-shard path below.
    if db.shard_count() > 1 {
        match route_statement(sql, &db.shards().shard_keys(), db.shard_count()) {
            Route::One(s) => return run_on_shard(db, ShardId(s), sql),
            Route::Split(parts) => return run_split(db, &parts),
            Route::All => return run_on_all(db, sql),
            Route::Refuse(msg) => {
                eprintln!("cannot route: {msg}");
                eprintln!(
                    "(declare the table's shard key with `meshdb shardkey <table> <column>`)"
                );
                return false;
            }
            Route::Passthrough => {}
        }
    }

    // Reads fan out by default; a shape that cannot be merged is refused rather than
    // silently answered from one shard. Writes must NOT come through here — the planner
    // refuses non-SELECT statements, so a write routed into the fan-out would be reported
    // as unsupported and never actually run.
    let fan_out = db.shard_count() > 1 && !Db::is_ddl(sql) && db.is_read(sql).unwrap_or(false);
    if fan_out {
        match db.query_all(sql) {
            Ok(result) => {
                print_table(&result);
                return true;
            }
            Err(meshdb::Error::Unsupported(msg)) => {
                eprintln!("cannot fan out: {msg}");
                eprintln!("(use `.shard N` to target one shard)");
                return false;
            }
            Err(e) => {
                eprintln!("error: {e}");
                return false;
            }
        }
    }

    run_on_shard(db, shard, sql)
}

/// Run a statement on one shard and print its result. The shared tail of both the default path and
/// an auto-routed single-shard statement.
fn run_on_shard(db: &Db, shard: ShardId, sql: &str) -> bool {
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

/// Run a split multi-row INSERT — one rewritten statement per shard — reporting the combined effect.
fn run_split(db: &Db, parts: &[(u32, String)]) -> bool {
    let mut affected = 0u64;
    for (shard, sub) in parts {
        match db.run_on(ShardId(*shard), sub) {
            Ok(Outcome::Ok(Executed::Changed(w))) => affected += w.rows_affected,
            Ok(Outcome::Rejected(msg)) => {
                eprintln!("rejected on shard {shard}: {msg}");
                return false;
            }
            Err(e) => {
                eprintln!("error on shard {shard}: {e}");
                return false;
            }
            Ok(_) => {}
        }
    }
    println!(
        "ok ({affected} row{} affected across {} shards)",
        if affected == 1 { "" } else { "s" },
        parts.len()
    );
    true
}

/// Apply a write to every shard, reporting the summed effect. Used for a write whose WHERE does not
/// pin the shard key, so it must touch them all rather than silently miss rows.
fn run_on_all(db: &Db, sql: &str) -> bool {
    match db.run_all(sql) {
        Ok(results) => {
            let mut affected = 0u64;
            for (id, o) in &results {
                match o {
                    Outcome::Ok(Executed::Changed(w)) => affected += w.rows_affected,
                    Outcome::Rejected(msg) => {
                        eprintln!("rejected on {id}: {msg}");
                        return false;
                    }
                    _ => {}
                }
            }
            println!(
                "ok ({affected} row{} affected across {} shards)",
                if affected == 1 { "" } else { "s" },
                results.len()
            );
            true
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

/// Install a log subscriber for the CLI.
///
/// Only the binary does this. The library emits through the `tracing` facade and installs
/// nothing, so an embedding consumer picks its own destination — or none.
///
/// Defaults to warnings and above; `MESHDB_LOG=debug` (or any `RUST_LOG`-style filter)
/// turns up the detail.
/// The value after `--name`, if present.
fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

/// Positional arguments — everything that is neither a `--flag` nor a flag's value.
fn positionals(args: &[String]) -> Vec<&str> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a.starts_with("--") {
            // Assume every flag takes a value; the flags here all do.
            i += 2;
        } else {
            out.push(a.as_str());
            i += 1;
        }
    }
    out
}

const FRAMES_USAGE: &str = "\
usage:
  meshdb frames <data-dir> --shard N     inspect one shard's WAL
  meshdb frames --file <path-to-wal>     inspect a WAL file directly

  --all      show every frame, including uncommitted and leftover ones
             (default: a summary plus per-transaction commit frames)

Physical replication ships WAL frames, which are not human-readable SQL. This decodes the
frame stream a shard emits: the WAL header, each frame's page number and commit marker, and
how the frames group into transactions. Read-only.

A frame with a non-zero db-size field is a COMMIT (it ends a transaction). A frame whose salt
does not match the header is a leftover from before the last checkpoint, which SQLite ignores.";

/// Inspect a shard's WAL frame stream.
fn frames_cmd(args: &[String]) -> ExitCode {
    let path = match flag(args, "--file") {
        Some(f) => std::path::PathBuf::from(f),
        None => {
            let pos = positionals(&args[1..]);
            let (Some(dir), Some(shard)) = (
                pos.first(),
                flag(args, "--shard").and_then(|v| v.parse::<u32>().ok()),
            ) else {
                eprintln!("{FRAMES_USAGE}");
                return ExitCode::FAILURE;
            };
            let db = std::path::Path::new(dir).join(format!("shard_{shard}.db"));
            meshdb::storage::checkpoint::wal_path_for(&db)
        }
    };

    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "no WAL at {} - the shard has been checkpointed and holds no pending frames",
                path.display()
            );
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("error: reading {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    let report = meshdb::vfs::inspect_wal(&bytes);
    render_frames(&report, &path, args.iter().any(|a| a == "--all"));
    ExitCode::SUCCESS
}

fn render_frames(report: &meshdb::vfs::WalReport, path: &std::path::Path, all: bool) {
    println!("WAL: {}", path.display());
    println!("  file size:      {} bytes", report.file_bytes);

    let Some(header) = &report.header else {
        println!("  not a WAL file (no valid header)");
        return;
    };

    println!("  page size:      {} bytes", header.page_size);
    println!("  checkpoint seq: {}", header.checkpoint_seq);
    println!(
        "  salt:           {}",
        header
            .salt
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    println!("  frames:         {} total", report.frames.len());
    println!("  transactions:   {} committed", report.transactions());
    let uncommitted = report.uncommitted_frames();
    if uncommitted > 0 {
        println!("  uncommitted:    {uncommitted} frame(s) past the last commit");
    }
    let leftover = report.frames.iter().filter(|f| !f.current).count();
    if leftover > 0 {
        println!("  leftover:       {leftover} frame(s) from before the last checkpoint");
    }
    if report.trailing_bytes > 0 {
        println!(
            "  trailing:       {} bytes after the last whole frame (partial write?)",
            report.trailing_bytes
        );
    }

    if report.frames.is_empty() {
        return;
    }
    println!();

    if all {
        println!(
            "  {:>5}  {:>10}  {:>8}  {:<10}  {:<10}",
            "frame", "offset", "page", "commit", "status"
        );
        for f in &report.frames {
            println!(
                "  {:>5}  {:>10}  {:>8}  {:<10}  {:<10}",
                f.index,
                f.offset,
                f.page_no,
                if f.is_commit() {
                    format!("db={}", f.db_size_after_commit)
                } else {
                    "-".into()
                },
                if f.current { "current" } else { "leftover" },
            );
        }
    } else {
        println!("  transactions (commit frames):");
        println!(
            "  {:>4}  {:>8}  {:>9}  {:>10}",
            "txn", "frames", "db-pages", "at-offset"
        );
        let mut txn = 0u64;
        let mut since = 0u64;
        for f in report.current_frames() {
            since += 1;
            if f.is_commit() {
                txn += 1;
                println!(
                    "  {txn:>4}  {since:>8}  {:>9}  {:>10}",
                    f.db_size_after_commit, f.offset
                );
                since = 0;
            }
        }
        if since > 0 {
            println!("  (+{since} uncommitted frame(s) after the last transaction)");
        }
        println!("\n  run with --all to see every frame");
    }
}

const SERVE_USAGE: &str = "usage: meshdb serve <data-dir> [options]

  --listen ADDR      address to accept connections on (default 127.0.0.1:4600)
  --shards N         required when creating a new data directory
  --users FILE       enable authentication from this users file (see `meshdb user`)
  --max-conn N       connection cap (default 256)
  --tls-cert FILE    PEM certificate; enables TLS (requires the `tls` build feature)
  --tls-key FILE     PEM private key for the certificate

Without --users the server accepts any connection and warns that it is open.
Without --tls-cert connections are plaintext.";

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

#[cfg(feature = "http")]
fn start_http(
    server: &meshdb::net::Server,
    addr: &str,
    insecure: bool,
) -> std::result::Result<(), String> {
    // The gateway shares the same shards and services as the TCP server, so both speak to one
    // core. It runs on its own threads; the TCP server keeps the main thread.
    let gateway = meshdb::net::HttpGateway::bind(
        server.shards_arc(),
        server.services_clone(),
        meshdb::net::HttpConfig {
            addr: addr.to_string(),
            insecure,
            ..Default::default()
        },
    )
    .map_err(|e| e.to_string())?;
    eprintln!("meshdb HTTP gateway on {addr}");
    std::thread::spawn(move || gateway.serve());
    Ok(())
}

#[cfg(not(feature = "http"))]
fn start_http(
    _server: &meshdb::net::Server,
    _addr: &str,
    _insecure: bool,
) -> std::result::Result<(), String> {
    Err("this build has no HTTP support; rebuild with `--features http`".into())
}

#[cfg(feature = "json-tcp")]
fn start_json_tcp(
    server: &meshdb::net::Server,
    addr: &str,
    insecure: bool,
) -> std::result::Result<(), String> {
    let jt = meshdb::net::JsonTcpServer::bind(
        server.shards_arc(),
        server.services_clone(),
        meshdb::net::JsonTcpConfig {
            addr: addr.to_string(),
            insecure,
        },
    )
    .map_err(|e| e.to_string())?;
    eprintln!("meshdb JSON-TCP on {addr}");
    std::thread::spawn(move || jt.serve());
    Ok(())
}

#[cfg(not(feature = "json-tcp"))]
fn start_json_tcp(
    _server: &meshdb::net::Server,
    _addr: &str,
    _insecure: bool,
) -> std::result::Result<(), String> {
    Err("this build has no JSON-TCP support; rebuild with `--features json-tcp`".into())
}

fn serve_cmd(args: &[String]) -> ExitCode {
    // args[0] == "serve"; args[1] should be the data directory.
    let pos = positionals(&args[1..]);
    let Some(dir) = pos.first().map(std::path::PathBuf::from) else {
        eprintln!("{SERVE_USAGE}");
        return ExitCode::FAILURE;
    };
    let listen = flag(args, "--listen")
        .unwrap_or("127.0.0.1:4600")
        .to_string();
    let requested = flag(args, "--shards").and_then(|v| v.parse::<u32>().ok());

    let shards = match resolve_shards(&dir, requested) {
        Ok(n) => n,
        Err(code) => return code,
    };

    let manager = match meshdb::shard::ShardManager::open(
        &dir,
        meshdb::shard::ShardConfig {
            shard_count: shards,
            ..meshdb::shard::ShardConfig::floor()
        },
    ) {
        Ok(m) => std::sync::Arc::new(m),
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Authentication, if a users file was given.
    let auth = match flag(args, "--users") {
        Some(path) => match meshdb::net::AuthConfig::open(std::path::Path::new(path)) {
            Ok(a) => Some(std::sync::Arc::new(a)),
            Err(e) => {
                eprintln!("error: reading users file: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let mut cfg = meshdb::net::ServerConfig {
        addr: listen.clone(),
        ..meshdb::net::ServerConfig::default()
    };
    if let Some(n) = flag(args, "--max-conn").and_then(|v| v.parse::<usize>().ok()) {
        cfg.max_connections = n;
    }

    let services = meshdb::net::NodeServices {
        auth,
        ..Default::default()
    };
    let server = match meshdb::net::Server::bind_with(manager, services, cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // TLS, if a certificate was given. Feature-gated; a clear message if the binary lacks it.
    let server = match (flag(args, "--tls-cert"), flag(args, "--tls-key")) {
        (Some(cert), Some(key)) => match enable_tls(server, cert, key) {
            Ok(s) => s,
            Err(msg) => {
                eprintln!("error: {msg}");
                return ExitCode::FAILURE;
            }
        },
        (None, None) => server,
        _ => {
            eprintln!("error: --tls-cert and --tls-key must be given together");
            return ExitCode::FAILURE;
        }
    };

    // Optional HTTP gateway alongside the native TCP server.
    if let Some(http_addr) = flag(args, "--http") {
        match start_http(&server, http_addr, has_flag(args, "--http-insecure")) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    // Optional JSON-over-TCP server — a persistent-socket protocol for cross-language drivers.
    if let Some(jt_addr) = flag(args, "--json-tcp") {
        match start_json_tcp(&server, jt_addr, has_flag(args, "--json-tcp-insecure")) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    eprintln!("meshdb serving {shards} shards on {listen}");
    match server.serve() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "tls")]
fn enable_tls(
    server: meshdb::net::Server,
    cert: &str,
    key: &str,
) -> std::result::Result<meshdb::net::Server, String> {
    let tls = meshdb::net::transport::TlsServerConfig::from_pem_files(
        std::path::Path::new(cert),
        std::path::Path::new(key),
    )
    .map_err(|e| e.to_string())?;
    Ok(server.with_tls(tls))
}

#[cfg(not(feature = "tls"))]
fn enable_tls(
    _server: meshdb::net::Server,
    _cert: &str,
    _key: &str,
) -> std::result::Result<meshdb::net::Server, String> {
    Err("this build has no TLS support; rebuild with `--features tls`".into())
}

/// Resolve the shard count the same way the main path does: the manifest decides for an
/// existing directory, and `--shards` is required to create one.
const SHARDKEY_USAGE: &str = "usage:
  meshdb shardkey <dir> <table> <column>   declare a table's shard key (co-partitioning)
  meshdb shardkey <dir> --list             list declared shard keys

Two tables declared on their shard keys may be joined in a cross-shard read on those keys.
This asserts how the app routes those tables — meshdb trusts it, as it cannot verify placement.";

fn shardkey_cmd(args: &[String]) -> ExitCode {
    // args[0] == "shardkey"
    let pos = positionals(&args[1..]);
    let Some(dir) = pos.first().map(std::path::PathBuf::from) else {
        eprintln!("{SHARDKEY_USAGE}");
        return ExitCode::FAILURE;
    };
    let shards = match resolve_shards(&dir, None) {
        Ok(n) => n,
        Err(code) => return code,
    };
    let db = match Db::open(&dir, shards) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // List when asked, or when only the directory was given.
    if has_flag(args, "--list") || pos.len() < 3 {
        let keys = db.shards().shard_keys();
        if keys.is_empty() {
            println!("no shard keys declared");
        } else {
            let mut entries: Vec<_> = keys.iter().collect();
            entries.sort();
            for (table, column) in entries {
                println!("{table}\t{column}");
            }
        }
        return ExitCode::SUCCESS;
    }

    let (table, column) = (pos[1], pos[2]);
    match db.shards().declare_shard_key(table, column) {
        Ok(()) => {
            println!("declared shard key: {table} -> {column}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn resolve_shards(
    dir: &std::path::Path,
    requested: Option<u32>,
) -> std::result::Result<u32, ExitCode> {
    let manifest_path = Manifest::path(dir);
    if manifest_path.exists() {
        match Manifest::read(&manifest_path) {
            Ok(m) => Ok(m.shard_count),
            Err(e) => {
                eprintln!("error: {e}");
                Err(ExitCode::FAILURE)
            }
        }
    } else {
        match requested {
            Some(n) => Ok(n),
            None => {
                eprintln!(
                    "error: {} does not exist yet, so --shards N is required to create it.",
                    dir.display()
                );
                Err(ExitCode::FAILURE)
            }
        }
    }
}

const USER_USAGE: &str = "usage:
  meshdb user add  <name> <secret> --role <read|write|admin> [target]
  meshdb user drop <name>                                    [target]
  meshdb user list                                           [target]

target is one of:
  --users FILE                          edit the users file directly (offline)
  --server ADDR --as ADMIN --admin-secret S
                                        change it on a running server (runtime)

Offline is how you create the FIRST admin, before any server is running. Once a
server is up with --users, use the runtime form to manage everyone else.

The role `cluster` cannot be granted over the wire — cluster credentials are a
deploy-time decision, not a runtime one. Set them in the users file directly.

Runtime user management sends a derived key, not the secret, but that key still
grants access: run it over TLS or a trusted network.";

fn user_cmd(args: &[String]) -> ExitCode {
    // args[0] == "user"; args[1] is the action.
    let action = args.get(1).map(String::as_str).unwrap_or("");
    let pos = positionals(&args[2..]);

    // Offline (a file) or online (a running server)?
    enum Target {
        File(meshdb::net::AuthConfig),
        Server(meshdb::net::Client),
    }
    let target = match (flag(args, "--users"), flag(args, "--server")) {
        (Some(file), None) => match meshdb::net::AuthConfig::open(std::path::Path::new(file)) {
            Ok(a) => Target::File(a),
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        },
        (None, Some(addr)) => {
            let (Some(admin), Some(secret)) = (flag(args, "--as"), flag(args, "--admin-secret"))
            else {
                eprintln!("error: --server needs --as ADMIN and --admin-secret S");
                return ExitCode::FAILURE;
            };
            match meshdb::net::Client::connect_as(addr, admin, secret) {
                Ok(c) => Target::Server(c),
                Err(e) => {
                    eprintln!("error: connecting as admin: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        _ => {
            eprintln!("{USER_USAGE}");
            return ExitCode::FAILURE;
        }
    };

    match action {
        "add" => {
            let (Some(name), Some(secret)) = (pos.first(), pos.get(1)) else {
                eprintln!(
                    "error: `user add` needs <name> <secret>

{USER_USAGE}"
                );
                return ExitCode::FAILURE;
            };
            let role_str = flag(args, "--role").unwrap_or("");
            let role: meshdb::net::Role = match role_str.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let result = match target {
                Target::File(auth) => {
                    auth.create(name, meshdb::net::auth::derive_key(secret), role)
                }
                Target::Server(mut c) => c.create_user(name, secret, role),
            };
            match result {
                Ok(()) => {
                    println!("user '{name}' created with role {role}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        "drop" => {
            let Some(name) = pos.first() else {
                eprintln!("error: `user drop` needs <name>");
                return ExitCode::FAILURE;
            };
            let result = match target {
                Target::File(auth) => auth.drop_user(name).map(|existed| {
                    if !existed {
                        eprintln!("warning: no such user '{name}'");
                    }
                }),
                Target::Server(mut c) => c.drop_user(name),
            };
            match result {
                Ok(()) => {
                    println!("user '{name}' dropped");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        "list" => {
            let users = match target {
                Target::File(auth) => Ok(auth.list()),
                Target::Server(mut c) => c.list_users(),
            };
            match users {
                Ok(list) => {
                    if list.is_empty() {
                        println!("(no users)");
                    }
                    for (name, role) in list {
                        println!("{name}	{role}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{USER_USAGE}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("MESHDB_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Logs go to stderr so they never mix with query results on stdout.
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
