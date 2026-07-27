//! The durable record of a cross-shard write, and what recovery does with it.
//!
//! # Why a log, and why it holds the statements
//!
//! SQLite has no `PREPARE TRANSACTION`. A transaction open when the process dies is rolled back,
//! unconditionally — so recovery cannot commit a prepared transaction the way textbook two-phase
//! commit does. It can only **re-apply**, which means the durable record has to carry the
//! statements themselves, and re-applying has to be idempotent. That is what the per-shard marker
//! table (`crate::storage::apply::TXN_TABLE`) is for: it lands inside the same transaction as the
//! data, so "did shard 3 apply transaction 91?" has a durable answer after any crash.
//!
//! # The one ordering invariant
//!
//! **The decision is fsynced before any shard commits.** Everything else follows from it:
//!
//! - A record with no durable decision means no shard can have committed → abort is safe.
//! - A record decided `Commit` may have committed anywhere → re-apply where the marker is absent.
//!
//! Get that order wrong and recovery cannot tell the two apart, which is the whole point of the
//! log.
//!
//! # Shape on disk
//!
//! One file per in-flight transaction under `txn/`, removed once resolved. That follows the
//! prepare/commit pattern the catalog already uses (`catalog.prepared`), for the same reason: a
//! temp file renamed into place cannot be observed half-written.

use std::path::{Path, PathBuf};

use crate::shard::ShardId;
use crate::storage::exec::Statement;

/// The decision recorded for a transaction. Absent until phase two begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Decision {
    Commit,
}

/// A cross-shard write, durable enough to finish after a crash.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TxnRecord {
    pub id: u64,
    /// Participating shards with the statements each was given, in ascending shard order — the
    /// order prepares are taken in, which is what keeps two coordinators from deadlocking.
    pub participants: Vec<(u32, Vec<String>)>,
    /// Whether the statements are DDL, so a re-apply also advances the schema version.
    pub ddl: bool,
    /// `None` until the decision is durable. `None` on recovery means nothing committed.
    pub decision: Option<Decision>,
}

impl TxnRecord {
    pub fn statements_for(&self, shard: ShardId) -> Option<Vec<Statement>> {
        self.participants
            .iter()
            .find(|(s, _)| *s == shard.0)
            .map(|(_, sql)| sql.iter().map(|s| Statement::new(s.as_str())).collect())
    }

    pub fn shards(&self) -> Vec<ShardId> {
        self.participants.iter().map(|(s, _)| ShardId(*s)).collect()
    }
}

fn dir_of(root: &Path) -> PathBuf {
    root.join("txn")
}

fn record_path(root: &Path, id: u64) -> PathBuf {
    dir_of(root).join(format!("{id:020}.txn"))
}

/// Write a record durably, fsyncing before returning.
///
/// The fsync is the ordering invariant, not an optimisation: this call returning is what makes it
/// safe for the first shard to commit.
pub fn put(root: &Path, record: &TxnRecord) -> crate::Result<()> {
    let dir = dir_of(root);
    std::fs::create_dir_all(&dir)
        .map_err(|e| crate::Error::Protocol(format!("creating {}: {e}", dir.display())))?;
    let encoded = bincode::serde::encode_to_vec(record, bincode::config::standard())
        .map_err(|e| crate::Error::Protocol(format!("encoding transaction record: {e}")))?;

    let path = record_path(root, record.id);
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)
            .map_err(|e| crate::Error::Protocol(format!("writing transaction record: {e}")))?;
        file.write_all(&encoded)
            .map_err(|e| crate::Error::Protocol(format!("writing transaction record: {e}")))?;
        file.sync_all()
            .map_err(|e| crate::Error::Protocol(format!("syncing transaction record: {e}")))?;
    }
    std::fs::rename(&tmp, &path)
        .map_err(|e| crate::Error::Protocol(format!("replacing transaction record: {e}")))?;
    // The rename itself must be durable, or the record can vanish with the directory entry.
    if let Ok(handle) = std::fs::File::open(&dir) {
        let _ = handle.sync_all();
    }
    Ok(())
}

/// Forget a resolved transaction.
pub fn remove(root: &Path, id: u64) -> crate::Result<()> {
    let path = record_path(root, id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::Error::Protocol(format!(
            "removing transaction record: {e}"
        ))),
    }
}

/// Every unresolved record, oldest first.
pub fn pending(root: &Path) -> Vec<TxnRecord> {
    let Ok(entries) = std::fs::read_dir(dir_of(root)) else {
        return Vec::new();
    };
    let mut records: Vec<TxnRecord> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "txn"))
        .filter_map(|e| std::fs::read(e.path()).ok())
        .filter_map(|bytes| {
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .ok()
                .map(|(record, _)| record)
        })
        .collect();
    records.sort_by_key(|r: &TxnRecord| r.id);
    records
}

/// The next id to allocate, chosen above anything already on disk so a restart cannot reuse one.
///
/// Reuse would be a correctness bug rather than an untidiness: the per-shard marker table is keyed
/// by id, so a repeated id would make recovery believe a fresh transaction had already been
/// applied and skip it.
pub fn next_id_after_recovery(root: &Path) -> u64 {
    pending(root).iter().map(|r| r.id).max().unwrap_or(0) + 1
}
