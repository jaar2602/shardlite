//! Centralized PRAGMA configuration.
//!
//! Every PRAGMA lives here as data rather than as scattered `execute` calls, and every
//! profile is read back by [`verify`] after being applied. That read-back is not
//! paranoia: several PRAGMAs silently no-op instead of erroring — a read-only connection
//! cannot change `journal_mode`, `auto_vacuum` cannot change once tables exist — and this
//! layer's failure mode is silent divergence between nodes rather than a loud crash.

use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;

use crate::config::{PragmaProfile, Role};
use crate::error::{Error, Result};

/// Retrying the WAL conversion makes contention invisible if nothing counts it. A retry is
/// normal when several connections open the same shard at once; *many* retries, or a long
/// wait, says something about the storage or the access pattern that an operator would want
/// to know before it becomes a stall.
static WAL_RETRIES: AtomicU64 = AtomicU64::new(0);
static WAL_CONTENDED_OPENS: AtomicU64 = AtomicU64::new(0);
static WAL_FAILED_OPENS: AtomicU64 = AtomicU64::new(0);
static WAL_MAX_WAIT_MS: AtomicU64 = AtomicU64::new(0);

/// Contention observed while converting databases to WAL mode, process-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalConversionStats {
    /// Individual retry attempts.
    pub retries: u64,
    /// Opens that needed at least one retry.
    pub contended_opens: u64,
    /// Opens that exhausted the timeout and failed.
    pub failed_opens: u64,
    /// Longest a single conversion waited.
    pub max_wait_ms: u64,
}

/// Contention observed while converting databases to WAL mode.
///
/// Process-wide rather than per-database: the contention is between connections, and a
/// caller wanting to know "is this normal?" is asking about the process, not one file.
pub fn wal_conversion_stats() -> WalConversionStats {
    WalConversionStats {
        retries: WAL_RETRIES.load(Ordering::Relaxed),
        contended_opens: WAL_CONTENDED_OPENS.load(Ordering::Relaxed),
        failed_opens: WAL_FAILED_OPENS.load(Ordering::Relaxed),
        max_wait_ms: WAL_MAX_WAIT_MS.load(Ordering::Relaxed),
    }
}

/// A conversion that retried this many times, or waited this long, is logged at `warn`
/// rather than `debug`. Below these it is ordinary concurrency; above, it is worth seeing
/// without having to turn logging up.
const NOISY_ATTEMPTS: u32 = 8;
const NOISY_WAIT_MS: u128 = 250;

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
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_millis(p.busy_timeout_ms.max(0) as u64);
    let mut backoff = std::time::Duration::from_millis(1);
    let mut attempts: u32 = 0;

    loop {
        match conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0)) {
            Ok(got) if got.eq_ignore_ascii_case("wal") => {
                if attempts > 0 {
                    record_contention(attempts, started.elapsed(), db_path);
                }
                return Ok(());
            }
            // A different mode came back, which is not contention — the filesystem cannot
            // support WAL. Retrying would never help.
            Ok(got) => {
                return Err(Error::WalUnavailable {
                    path: db_path.to_string(),
                    got,
                });
            }
            Err(e) if is_busy(&e) && std::time::Instant::now() < deadline => {
                attempts += 1;
                WAL_RETRIES.fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    db = db_path,
                    attempt = attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    "WAL conversion is locked; retrying"
                );
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
            }
            Err(e) => {
                WAL_FAILED_OPENS.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    db = db_path,
                    attempts,
                    waited_ms = started.elapsed().as_millis() as u64,
                    error = %e,
                    "gave up converting the database to WAL mode. `PRAGMA journal_mode` does \
                     not honour busy_timeout, so this is retried explicitly — exhausting it \
                     means something held the write lock for the whole timeout"
                );
                return Err(Error::Sqlite(e));
            }
        }
    }
}

/// Record a conversion that had to wait, loudly enough to notice if it is not ordinary.
///
/// A retry or two is normal when several connections open the same shard at once. Many
/// retries, or a long wait, is a different message: too many writers converging on one
/// shard, or storage slower than the design assumes. The open still succeeded, so this must
/// not read as an error — but it must not be invisible either, which is what retrying
/// silently would have made it.
fn record_contention(attempts: u32, waited: std::time::Duration, db_path: &str) {
    let ms = waited.as_millis();
    WAL_CONTENDED_OPENS.fetch_add(1, Ordering::Relaxed);
    WAL_MAX_WAIT_MS.fetch_max(ms as u64, Ordering::Relaxed);

    if attempts >= NOISY_ATTEMPTS || ms >= NOISY_WAIT_MS {
        tracing::warn!(
            db = db_path,
            attempts,
            waited_ms = ms as u64,
            "converting this database to WAL mode took an unusual amount of contention. The \
             open succeeded, so nothing is lost, but repeated occurrences suggest many \
             writers opening the same shard at once, or storage slower than assumed"
        );
    } else {
        tracing::debug!(
            db = db_path,
            attempts,
            waited_ms = ms as u64,
            "WAL conversion waited for another connection"
        );
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
