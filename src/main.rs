//! meshdb CLI — open a database and run SQL against it.
//!
//! Usage:
//!   meshdb <db-path>                 interactive shell
//!   meshdb <db-path> -c "SQL"        run one statement and exit
//!   meshdb <db-path> -f <file>       run statements from a file (one per line)

use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use meshdb::db::Db;
use meshdb::storage::exec::{Executed, Outcome, QueryResult};

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

    let path = std::path::PathBuf::from(&args[0]);
    let db = match Db::open(&path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    match args.get(1).map(String::as_str) {
        Some("-c") => match args.get(2) {
            Some(sql) => {
                if run_and_print(&db, sql) {
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
        Some("-f") => match args.get(2) {
            Some(file) => run_file(&db, file),
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

const USAGE: &str = "\
meshdb — HA multi-write SQLite server (single-node, step 2)

usage:
  meshdb <db-path>              interactive shell
  meshdb <db-path> -c \"SQL\"     run one statement and exit
  meshdb <db-path> -f <file>    run statements from a file, one per line

shell commands:
  .help     show this message
  .stats    writer batching statistics
  .tables   list tables
  .quit     exit";

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
        if !run_and_print(db, line) {
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn repl(db: &Db) -> ExitCode {
    println!("meshdb — {}", db.path().display());
    println!("type .help for commands, .quit to exit");

    let stdin = io::stdin();
    loop {
        print!("meshdb> ");
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
            ".stats" => {
                let w = db.writer_stats();
                let r = db.reader_stats();
                println!(
                    "writer: batches={} requests={} max_batch={} mean_batch={:.2}",
                    w.batches,
                    w.requests,
                    w.max_batch,
                    w.mean_batch()
                );
                println!(
                    "reader: threads={} queries={} rejected_busy={} timed_out={}",
                    r.threads, r.queries, r.rejected_busy, r.timed_out
                );
                continue;
            }
            ".tables" => {
                run_and_print(
                    db,
                    "SELECT name FROM sqlite_schema WHERE type='table' \
                     AND name NOT LIKE 'sqlite_%' ORDER BY name",
                );
                continue;
            }
            _ => {}
        }

        run_and_print(db, sql);
    }
}

/// Returns false if the statement failed.
fn run_and_print(db: &Db, sql: &str) -> bool {
    match db.run(sql) {
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
