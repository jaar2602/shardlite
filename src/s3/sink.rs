//! Streaming a shard's change-log (and snapshots) to S3 — the durable copy for HA (slice 2).
//!
//! [`S3Sink`] implements [`crate::replication::FrameSink`], so it sits on the same capture path the
//! in-memory frame log does: every committed transaction is handed to `accept`, serialised, and
//! uploaded to S3 as a **change-log** object. Uploads run on a background thread, so a commit is
//! never blocked on an S3 round trip (async archival, a small RPO window). But a *persistent*
//! upload failure is surfaced back through `accept` — a sink that silently stopped archiving would
//! let the local database drift ahead of its durable copy, exactly the divergence physical
//! replication exists to prevent.
//!
//! A full-shard **snapshot** is uploaded on demand by [`S3Sink::put_snapshot`]; on restore the
//! change-log is replayed over the most recent snapshot. Object keys sort by `(epoch, lsn)` so the
//! newest snapshot and the ordered change-log are both trivially found.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;

use crate::replication::{FrameSink, Lsn, StreamTxn};
use crate::shard::ShardId;

use super::{S3Client, S3Error};

/// Archives a shard's frames and snapshots to S3.
pub struct S3Sink {
    tx: Sender<Job>,
    client: Arc<S3Client>,
    prefix: String,
    /// Set once the background uploader gives up on a change-log object. `accept` then fails, so
    /// the writer stops rather than committing further data that never reached S3.
    failed: Arc<Mutex<Option<String>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

enum Job {
    Upload { key: String, bytes: Vec<u8> },
    Flush(Sender<()>),
    Stop,
}

impl S3Sink {
    pub fn new(client: Arc<S3Client>, prefix: impl Into<String>) -> Self {
        let (tx, rx) = channel::<Job>();
        let failed = Arc::new(Mutex::new(None));
        let worker = std::thread::Builder::new()
            .name("meshdb-s3sink".into())
            .spawn({
                let client = Arc::clone(&client);
                let failed = Arc::clone(&failed);
                move || run_uploader(rx, client, failed)
            })
            .expect("spawn s3 sink worker");
        Self {
            tx,
            client,
            prefix: prefix.into(),
            failed,
            worker: Mutex::new(Some(worker)),
        }
    }

    /// The key of the change-log object for `[first_lsn ..]` of `shard` in `epoch`.
    fn wal_key(&self, shard: ShardId, epoch: u64, first_lsn: Lsn) -> String {
        format!(
            "{}/shard_{}/wal/{:020}_{:020}",
            self.prefix, shard.0, epoch, first_lsn
        )
    }

    /// The key of a snapshot object taken at `(epoch, lsn)`.
    fn snapshot_key(&self, shard: ShardId, epoch: u64, lsn: Lsn) -> String {
        format!(
            "{}/shard_{}/snapshot/{:020}_{:020}",
            self.prefix, shard.0, epoch, lsn
        )
    }

    /// Upload a full-shard snapshot **synchronously** (snapshots are large and infrequent; the
    /// caller controls their timing and has usually flushed the change-log first).
    pub fn put_snapshot(
        &self,
        shard: ShardId,
        epoch: u64,
        lsn: Lsn,
        db: &[u8],
    ) -> crate::Result<()> {
        let key = self.snapshot_key(shard, epoch, lsn);
        self.client.put(&key, db).map_err(to_err)
    }

    /// Block until every queued change-log upload has completed, then report health. Call before a
    /// snapshot so the change-log up to the snapshot point is durable first.
    pub fn flush(&self) -> crate::Result<()> {
        let (done_tx, done_rx) = channel();
        if self.tx.send(Job::Flush(done_tx)).is_ok() {
            let _ = done_rx.recv();
        }
        self.health()
    }

    fn health(&self) -> crate::Result<()> {
        match self.failed.lock().unwrap().clone() {
            Some(e) => Err(crate::Error::Protocol(format!("s3 sink unhealthy: {e}"))),
            None => Ok(()),
        }
    }
}

/// Serialise a change-log batch for a S3 object. Public so the restore path (and tests) decode
/// with the exact same encoding the sink writes.
pub fn encode_change_log(txns: &[StreamTxn]) -> crate::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(txns, bincode::config::standard())
        .map_err(|e| crate::Error::Protocol(format!("encoding change-log: {e}")))
}

/// Deserialise a change-log object read back from S3.
pub fn decode_change_log(bytes: &[u8]) -> crate::Result<Vec<StreamTxn>> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(txns, _)| txns)
        .map_err(|e| crate::Error::Protocol(format!("decoding change-log: {e}")))
}

impl FrameSink for S3Sink {
    fn accept(&self, shard: ShardId, epoch: u64, txns: Vec<StreamTxn>) -> crate::Result<()> {
        if txns.is_empty() {
            return Ok(());
        }
        let first = txns.first().unwrap().lsn;
        let bytes = encode_change_log(&txns)?;
        let key = self.wal_key(shard, epoch, first);
        // Enqueue for async upload — the commit is not blocked on S3.
        let _ = self.tx.send(Job::Upload { key, bytes });
        // Surface a persistent *prior* failure so the writer stops before drifting further.
        self.health()
    }
}

impl Drop for S3Sink {
    fn drop(&mut self) {
        let _ = self.tx.send(Job::Stop);
        if let Some(h) = self.worker.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

fn run_uploader(rx: Receiver<Job>, client: Arc<S3Client>, failed: Arc<Mutex<Option<String>>>) {
    for job in rx {
        match job {
            Job::Upload { key, bytes } => {
                if let Err(e) = put_with_retry(&client, &key, &bytes) {
                    *failed.lock().unwrap() = Some(e.to_string());
                }
            }
            Job::Flush(done) => {
                let _ = done.send(());
            }
            Job::Stop => break,
        }
    }
}

fn put_with_retry(client: &S3Client, key: &str, bytes: &[u8]) -> std::result::Result<(), S3Error> {
    let mut last = None;
    for attempt in 0..3u64 {
        match client.put(key, bytes) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e);
                if attempt < 2 {
                    std::thread::sleep(std::time::Duration::from_millis(50 * (attempt + 1)));
                }
            }
        }
    }
    Err(last.expect("at least one attempt failed"))
}

fn to_err(e: S3Error) -> crate::Error {
    crate::Error::Protocol(format!("s3: {e}"))
}
