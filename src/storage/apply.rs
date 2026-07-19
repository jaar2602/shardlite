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
    // IMMEDIATE takes the write lock at BEGIN rather than on first write, so a problem
    // surfaces before any work is done instead of midway through the batch.
    let mut tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;

    let mut results: Vec<Vec<Outcome>> = Vec::with_capacity(groups.len());

    for group in groups {
        let mut outcomes = Vec::with_capacity(group.len());

        for statement in group {
            let sp = tx.savepoint().map_err(|e| e.to_string())?;

            match exec::run(&sp, statement) {
                Ok(executed) => {
                    sp.commit().map_err(|e| e.to_string())?;
                    outcomes.push(Outcome::Ok(executed));
                }
                Err(SqlError::Logic(msg)) => {
                    // Dropping the savepoint rolls it back, leaving earlier statements in
                    // this transaction untouched.
                    drop(sp);
                    outcomes.push(Outcome::Rejected(msg));
                }
                Err(SqlError::Fatal(e)) => return Err(e.to_string()),
            }
        }

        results.push(outcomes);
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(results)
}
