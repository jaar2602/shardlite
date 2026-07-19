//! Centralized PRAGMA configuration.
//!
//! Every PRAGMA lives here as data rather than as scattered `execute` calls, and every
//! profile is read back by [`verify`] after being applied. That read-back is not
//! paranoia: several PRAGMAs silently no-op instead of erroring — a read-only connection
//! cannot change `journal_mode`, `auto_vacuum` cannot change once tables exist — and this
//! layer's failure mode is silent divergence between nodes rather than a loud crash.

use rusqlite::Connection;

use crate::config::{PragmaProfile, Role};
use crate::error::{Error, Result};

/// `temp_store = MEMORY` keeps temp b-trees (ORDER BY, CREATE INDEX) off disk. It is
/// bounded by `soft_heap_limit` on the writer profile; the two belong together.
const TEMP_STORE_MEMORY: i64 = 2;

/// Blocks schema-embedded function and virtual-table tricks.
const TRUSTED_SCHEMA_OFF: i64 = 0;

/// Apply `profile` to `conn`. `db_path` is used only for error context.
///
/// Does not verify — call [`verify`] afterwards. [`crate::storage::open`] does both.
pub fn apply(conn: &Connection, p: &PragmaProfile, db_path: &str) -> Result<()> {
    // First, so every later statement has it. Note that this does NOT protect the
    // journal-mode change below — see `set_wal_mode`.
    set(conn, "busy_timeout", &p.busy_timeout_ms.to_string())?;

    if p.role == Role::Writer {
        set_wal_mode(conn, p, db_path)?;

        // Persistent. Keeps unpredictable page churn out of the write and replication path.
        set(conn, "auto_vacuum", "NONE")?;

        // Autocheckpoint fires *inside* COMMIT, which would inject an unpredictable
        // multi-hundred-millisecond stall into the write path. Checkpoints are explicit,
        // run by the writer thread between transactions.
        set(conn, "wal_autocheckpoint", "0")?;

        if p.soft_heap_limit > 0 {
            set(conn, "soft_heap_limit", &p.soft_heap_limit.to_string())?;
        }
    }

    if p.role == Role::Reader {
        // Belt-and-braces on top of SQLITE_OPEN_READ_ONLY.
        set(conn, "query_only", "ON")?;
    }

    set(conn, "synchronous", p.synchronous.as_keyword())?;
    set(
        conn,
        "foreign_keys",
        if p.foreign_keys { "ON" } else { "OFF" },
    )?;
    set(conn, "cache_size", &p.cache_size.to_string())?;
    set(conn, "mmap_size", &p.mmap_size.to_string())?;
    set(conn, "temp_store", "MEMORY")?;
    set(conn, "trusted_schema", "OFF")?;

    Ok(())
}

/// Read every setting back and fail on any mismatch.
pub fn verify(conn: &Connection, p: &PragmaProfile) -> Result<()> {
    let role = p.role;

    // Checked for both roles: a reader seeing anything other than `wal` means it opened
    // a file the writer never configured.
    let journal: String = read_text(conn, "journal_mode")?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err(Error::PragmaMismatch {
            role,
            pragma: "journal_mode",
            expected: "wal".to_string(),
            actual: journal,
        });
    }

    expect(conn, role, "synchronous", p.synchronous.as_i64())?;
    expect(conn, role, "busy_timeout", p.busy_timeout_ms)?;
    expect(conn, role, "foreign_keys", i64::from(p.foreign_keys))?;
    expect(conn, role, "cache_size", p.cache_size)?;
    expect(conn, role, "mmap_size", p.mmap_size)?;
    expect(conn, role, "temp_store", TEMP_STORE_MEMORY)?;
    expect(conn, role, "trusted_schema", TRUSTED_SCHEMA_OFF)?;

    match role {
        Role::Writer => {
            expect(conn, role, "wal_autocheckpoint", 0)?;
            expect(conn, role, "auto_vacuum", 0)?;
            if p.soft_heap_limit > 0 {
                expect(conn, role, "soft_heap_limit", p.soft_heap_limit)?;
            }
        }
        Role::Reader => {
            expect(conn, role, "query_only", 1)?;
        }
    }

    Ok(())
}

/// Convert the database to WAL mode, retrying while another connection holds the lock.
///
/// **`busy_timeout` does not apply to a journal-mode change.** Measured on 3.53.2: with a
/// 5000 ms timeout set and another connection holding the write lock, this pragma gives up
/// after **23 microseconds**, while an ordinary `INSERT` on the same connection waits the
/// full 5.01 seconds. SQLite does not invoke the busy handler for this operation, so the
/// retry has to be explicit.
///
/// (An earlier attempt moved `busy_timeout` ahead of this statement on the theory that the
/// timeout was merely unset. That ordering is still right, but it was not the cause — the
/// timeout was set and SQLite ignored it.)
///
/// Retrying is safe and terminates for a good reason: whoever holds the lock is either
/// converting the database themselves, after which this pragma becomes a no-op that returns
/// `wal`, or committing a write, after which the lock is free.
fn set_wal_mode(conn: &Connection, p: &PragmaProfile, db_path: &str) -> Result<()> {
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_millis(p.busy_timeout_ms.max(0) as u64);
    let mut backoff = std::time::Duration::from_millis(1);

    loop {
        match conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0)) {
            Ok(got) if got.eq_ignore_ascii_case("wal") => return Ok(()),
            // A different mode came back, which is not contention — the filesystem cannot
            // support WAL. Retrying would never help.
            Ok(got) => {
                return Err(Error::WalUnavailable {
                    path: db_path.to_string(),
                    got,
                });
            }
            Err(e) if is_busy(&e) && std::time::Instant::now() < deadline => {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
            }
            Err(e) => return Err(Error::Sqlite(e)),
        }
    }
}

fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _)
            if err.code == rusqlite::ffi::ErrorCode::DatabaseBusy
                || err.code == rusqlite::ffi::ErrorCode::DatabaseLocked
    )
}

/// PRAGMA statements cannot take bound parameters, so they are formatted. Every caller
/// passes internal constants — no user input reaches this.
fn set(conn: &Connection, name: &str, value: &str) -> Result<()> {
    // `execute_batch` rather than `execute`: several PRAGMAs return a row when set, and
    // `execute` rejects statements that return rows.
    conn.execute_batch(&format!("PRAGMA {name} = {value};"))?;
    Ok(())
}

fn read_i64(conn: &Connection, name: &str) -> Result<i64> {
    Ok(conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))?)
}

fn read_text(conn: &Connection, name: &str) -> Result<String> {
    Ok(conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))?)
}

fn expect(conn: &Connection, role: Role, pragma: &'static str, want: i64) -> Result<()> {
    let got = read_i64(conn, pragma)?;
    if got != want {
        return Err(Error::PragmaMismatch {
            role,
            pragma,
            expected: want.to_string(),
            actual: got.to_string(),
        });
    }
    Ok(())
}
