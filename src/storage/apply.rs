//! Applying a batch of statements as one transaction.
//!
//! Shared by the single-database writer and the sharded writer fleet, so the transaction
//! and savepoint semantics exist in exactly one place.

use rusqlite::{Connection, TransactionBehavior};

use crate::storage::exec::{self, Outcome, SqlError, Statement};

/// Apply `groups` as a **single** transaction, isolating each group with a SAVEPOINT.
///
/// Each group is one caller's statements. A caller whose statement fails deterministically
/// (constraint violation, bad SQL) has only its own savepoint rolled back; the rest of the
/// batch still commits — that is what makes it safe to merge unrelated callers into one
/// transaction and amortize a single fsync across all of them.
///
/// Returns `Err(reason)` if the transaction aborted, in which case **nothing** in the batch
/// was applied. That happens on a machine fault — `DiskFull`, `IoErr`, `Corrupt` — where
/// continuing would commit a batch on a node that cannot be trusted.
pub fn batch(
    conn: &mut Connection,
    groups: &[Vec<Statement>],
) -> std::result::Result<Vec<Vec<Outcome>>, String> {
    batch_with(conn, groups, &[])
}

/// Apply groups, some of which may be **atomic**.
///
/// A group's index in `atomic` marks it all-or-nothing: if any statement in it fails, the
/// whole group is rolled back and reported as a single [`Outcome::Rejected`]. This is what a
/// client transaction needs — one bad statement voids the batch — and it is deliberately
/// different from the default per-statement isolation, which independent group-commit
/// requests need so that one caller's mistake cannot roll back another's write.
///
/// A missing or `false` entry keeps the per-statement behaviour. Atomic and non-atomic groups
/// commit together in the one outer transaction, so a transaction still rides group commit.
pub fn batch_with(
    conn: &mut Connection,
    groups: &[Vec<Statement>],
    atomic: &[bool],
) -> std::result::Result<Vec<Vec<Outcome>>, String> {
    // IMMEDIATE takes the write lock at BEGIN rather than on first write, so a problem
    // surfaces before any work is done instead of midway through the batch.
    let mut tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;

    let mut results: Vec<Vec<Outcome>> = Vec::with_capacity(groups.len());

    for (i, group) in groups.iter().enumerate() {
        if atomic.get(i).copied().unwrap_or(false) {
            results.push(apply_atomic_group(&mut tx, group)?);
        } else {
            results.push(apply_isolated_group(&mut tx, group)?);
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(results)
}

/// Independent statements: each isolated by its own savepoint, so one rejection leaves the
/// others standing. The original, unchanged behaviour.
fn apply_isolated_group(
    tx: &mut rusqlite::Transaction<'_>,
    group: &[Statement],
) -> std::result::Result<Vec<Outcome>, String> {
    let mut outcomes = Vec::with_capacity(group.len());
    for statement in group {
        let sp = tx.savepoint().map_err(|e| e.to_string())?;
        match exec::run(&sp, statement) {
            Ok(executed) => {
                sp.commit().map_err(|e| e.to_string())?;
                outcomes.push(Outcome::Ok(executed));
            }
            Err(SqlError::Logic(msg)) => {
                // Dropping the savepoint rolls it back, leaving earlier statements in this
                // transaction untouched.
                drop(sp);
                outcomes.push(Outcome::Rejected(msg));
            }
            Err(SqlError::Fatal(e)) => return Err(e.to_string()),
        }
    }
    Ok(outcomes)
}

/// A client transaction: all-or-nothing. Wrapped in one savepoint; the first failure rolls
/// the whole group back and reports it as a single rejection.
fn apply_atomic_group(
    tx: &mut rusqlite::Transaction<'_>,
    group: &[Statement],
) -> std::result::Result<Vec<Outcome>, String> {
    let sp = tx.savepoint().map_err(|e| e.to_string())?;
    let mut outcomes = Vec::with_capacity(group.len());
    for statement in group {
        match exec::run(&sp, statement) {
            Ok(executed) => outcomes.push(Outcome::Ok(executed)),
            Err(SqlError::Logic(msg)) => {
                // Roll the whole transaction back to its start — earlier statements included —
                // and report the one rejection that voided it.
                drop(sp);
                return Ok(vec![Outcome::Rejected(msg)]);
            }
            Err(SqlError::Fatal(e)) => return Err(e.to_string()),
        }
    }
    sp.commit().map_err(|e| e.to_string())?;
    Ok(outcomes)
}

/// The table recording which cross-shard transactions this shard has applied.
///
/// Written **inside** the same transaction as the data, which is what makes "did shard 3 apply
/// transaction 91?" answerable after any crash. Recovery needs that question answered, because
/// SQLite has no `PREPARE TRANSACTION`: a transaction open when the process dies is rolled back,
/// so recovery cannot commit a prepared transaction — it can only re-apply, and re-applying is
/// only safe if it can tell what already landed.
pub const TXN_TABLE: &str = "_shardlite_txn";

/// What a prepare produced. The transaction is left **open** on `conn` when this is `Prepared`.
pub enum Prepared {
    /// Every statement applied. The transaction is open and awaiting a decision.
    Prepared(Vec<Outcome>),
    /// A statement was rejected; the transaction has already been rolled back.
    Rejected(String),
}

/// Run `statements` inside a new transaction and leave it open, awaiting a decision.
///
/// Raw `BEGIN IMMEDIATE` rather than [`rusqlite::Transaction`] on purpose: a `Transaction` borrows
/// the connection, so one could not be held across writer-thread batches without a self-referential
/// borrow. Keeping the transaction state inside SQLite instead of inside a Rust lifetime is what
/// makes a prepared shard parkable at all.
///
/// A rejection rolls back before returning, so the caller never has to abort a prepare that failed.
pub fn prepare_group(
    conn: &Connection,
    txn_id: u64,
    statements: &[Statement],
    bump_schema_version: bool,
) -> std::result::Result<Prepared, String> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(|e| e.to_string())?;

    let rollback = |msg: String| -> std::result::Result<Prepared, String> {
        let _ = conn.execute_batch("ROLLBACK");
        Ok(Prepared::Rejected(msg))
    };

    if let Err(e) = conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {TXN_TABLE} (id INTEGER PRIMARY KEY)"
    )) {
        return rollback(e.to_string());
    }

    let mut outcomes = Vec::with_capacity(statements.len());
    for statement in statements {
        match exec::run(conn, statement) {
            Ok(executed) => outcomes.push(Outcome::Ok(executed)),
            Err(SqlError::Logic(msg)) => return rollback(msg),
            Err(SqlError::Fatal(e)) => return rollback(e.to_string()),
        }
    }

    if bump_schema_version {
        let next = match crate::storage::schema::version_of(conn) {
            Ok(v) => v + 1,
            Err(e) => return rollback(e.to_string()),
        };
        // Inside the transaction, so schema and version move together or not at all.
        if let Err(e) = conn.execute_batch(&format!("PRAGMA user_version = {next}")) {
            return rollback(e.to_string());
        }
    }

    // The marker lands with the data, so a crash cannot separate them.
    if let Err(e) = conn.execute(
        &format!("INSERT OR IGNORE INTO {TXN_TABLE} (id) VALUES (?1)"),
        [txn_id as i64],
    ) {
        return rollback(e.to_string());
    }

    Ok(Prepared::Prepared(outcomes))
}

/// Commit or roll back a transaction left open by [`prepare_group`].
pub fn finish_prepared(conn: &Connection, commit: bool) -> std::result::Result<(), String> {
    let sql = if commit { "COMMIT" } else { "ROLLBACK" };
    conn.execute_batch(sql).map_err(|e| e.to_string())
}

/// Whether this shard has already applied `txn_id` — the question recovery asks before re-applying.
pub fn has_applied(conn: &Connection, txn_id: u64) -> std::result::Result<bool, String> {
    let exists: std::result::Result<i64, _> = conn.query_row(
        &format!("SELECT COUNT(*) FROM {TXN_TABLE} WHERE id = ?1"),
        [txn_id as i64],
        |row| row.get(0),
    );
    match exists {
        Ok(n) => Ok(n > 0),
        // No marker table means nothing was ever applied through the two-phase path.
        Err(_) => Ok(false),
    }
}
