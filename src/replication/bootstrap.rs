//! Resumable snapshot transfer.
//!
//! Copying a shard whole means a failure at 99% starts again from zero, which at a few
//! hundred GB is not a retry so much as a second outage. Transfers here are chunked and the
//! progress is persisted, so an interruption resumes where it stopped.
//!
//! # Why a resume needs an identity
//!
//! Resuming is only safe against *the same* snapshot. The primary freezes a shard by
//! suspending checkpointing, and that freeze is not unconditional — if the WAL outgrows its
//! limit the hold breaks, checkpointing resumes, and the file changes. Splicing new bytes
//! onto a prefix of the old file would produce a database that is corrupt in a way nothing
//! detects, because every page in it is individually valid.
//!
//! So each transfer carries a [`SnapshotId`] — shard, epoch, LSN, and total size. A resume
//! whose identity does not match starts over. Restarting a large copy is expensive;
//! splicing two different snapshots together is unrecoverable.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::shard::ShardId;

use super::follower::Follower;
use super::stream::Lsn;

/// Default transfer chunk. Large enough that per-chunk overhead is noise, small enough that
/// an interruption loses little.
pub const DEFAULT_CHUNK: usize = 1024 * 1024;

/// Identifies one snapshot. Two transfers may only be spliced if these match exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotId {
    pub shard: ShardId,
    pub epoch: u64,
    pub lsn: Lsn,
    pub total_bytes: u64,
}

impl SnapshotId {
    fn encode(&self) -> String {
        format!(
            "shard={}\nepoch={}\nlsn={}\ntotal={}\n",
            self.shard.0, self.epoch, self.lsn, self.total_bytes
        )
    }

    fn decode(text: &str) -> Option<(Self, u64)> {
        let mut shard = None;
        let mut epoch = None;
        let mut lsn = None;
        let mut total = None;
        let mut written = None;
        for line in text.lines() {
            let (k, v) = line.split_once('=')?;
            match k {
                "shard" => shard = v.parse().ok().map(ShardId),
                "epoch" => epoch = v.parse().ok(),
                "lsn" => lsn = v.parse().ok(),
                "total" => total = v.parse().ok(),
                "written" => written = v.parse().ok(),
                _ => {}
            }
        }
        Some((
            Self {
                shard: shard?,
                epoch: epoch?,
                lsn: lsn?,
                total_bytes: total?,
            },
            written.unwrap_or(0),
        ))
    }
}

/// Reads a frozen shard file in chunks.
///
/// The file must be held frozen by the primary for the life of this reader — see
/// `WriterFleet::begin_snapshot`.
pub struct SnapshotSource {
    file: File,
    id: SnapshotId,
    offset: u64,
}

impl SnapshotSource {
    pub fn open(shard: ShardId, epoch: u64, lsn: Lsn, path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|e| {
            Error::Manifest(format!("opening snapshot source {}: {e}", path.display()))
        })?;
        let total_bytes = file
            .metadata()
            .map_err(|e| Error::Manifest(format!("sizing {}: {e}", path.display())))?
            .len();
        Ok(Self {
            file,
            id: SnapshotId {
                shard,
                epoch,
                lsn,
                total_bytes,
            },
            offset: 0,
        })
    }

    pub fn id(&self) -> SnapshotId {
        self.id
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Continue from `offset`, as reported by a resumed [`SnapshotTransfer`].
    pub fn seek_to(&mut self, offset: u64) -> Result<()> {
        if offset > self.id.total_bytes {
            return Err(Error::Manifest(format!(
                "cannot resume at {offset}; the snapshot is only {} bytes",
                self.id.total_bytes
            )));
        }
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::Manifest(format!("seeking snapshot to {offset}: {e}")))?;
        self.offset = offset;
        Ok(())
    }

    /// Read the next chunk. `Ok(0)` means the snapshot is fully read.
    pub fn read_chunk(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self
            .file
            .read(buf)
            .map_err(|e| Error::Manifest(format!("reading snapshot: {e}")))?;
        self.offset += n as u64;
        Ok(n)
    }
}

/// Receives a snapshot into a partial file, resumable across interruptions.
pub struct SnapshotTransfer {
    partial: PathBuf,
    meta: PathBuf,
    file: File,
    id: SnapshotId,
    written: u64,
}

impl SnapshotTransfer {
    /// Begin or resume a transfer of `id` into `dir`.
    ///
    /// If a partial transfer for a *different* snapshot is present it is discarded — see the
    /// module note on why splicing two snapshots is worse than recopying one.
    pub fn begin(dir: &Path, id: SnapshotId) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::Manifest(format!("creating {}: {e}", dir.display())))?;
        let partial = dir.join(format!("shard_{}.partial", id.shard.0));
        let meta = dir.join(format!("shard_{}.partial.meta", id.shard.0));

        let mut written = 0u64;
        let mut reuse = false;
        if let Ok(text) = std::fs::read_to_string(&meta)
            && let Some((found, w)) = SnapshotId::decode(&text)
        {
            if found == id {
                // Never trust the recorded offset over the file itself: a crash between the
                // chunk write and the meta update leaves the file longer than the meta says.
                // Taking the smaller of the two rewrites a chunk, which is harmless.
                let on_disk = std::fs::metadata(&partial).map(|m| m.len()).unwrap_or(0);
                written = w.min(on_disk);
                reuse = true;
                tracing::info!(
                    shard = %id.shard,
                    resume_at = written,
                    total = id.total_bytes,
                    "resuming snapshot transfer"
                );
            } else {
                tracing::warn!(
                    shard = %id.shard,
                    "a partial transfer exists for a different snapshot; discarding it \
                     rather than splicing two snapshots together"
                );
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(!reuse)
            .open(&partial)
            .map_err(|e| Error::Manifest(format!("opening {}: {e}", partial.display())))?;

        let mut me = Self {
            partial,
            meta,
            file,
            id,
            written,
        };
        me.file
            .seek(SeekFrom::Start(written))
            .map_err(|e| Error::Manifest(format!("seeking partial to {written}: {e}")))?;
        me.persist_meta()?;
        Ok(me)
    }

    /// Bytes already received; where the source should resume from.
    pub fn offset(&self) -> u64 {
        self.written
    }

    pub fn total(&self) -> u64 {
        self.id.total_bytes
    }

    pub fn is_complete(&self) -> bool {
        self.written >= self.id.total_bytes
    }

    /// Append a chunk read from the source.
    pub fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.file
            .write_all(data)
            .map_err(|e| Error::Manifest(format!("writing {}: {e}", self.partial.display())))?;
        // Durable before the offset moves. A crash between the two replays a chunk, which
        // is harmless; the reverse order would skip one, which is not.
        self.file
            .sync_data()
            .map_err(|e| Error::Manifest(format!("syncing {}: {e}", self.partial.display())))?;
        self.written += data.len() as u64;
        self.persist_meta()
    }

    /// Install the completed transfer into `follower` and clean up.
    pub fn finish(self, follower: &Follower) -> Result<()> {
        if !self.is_complete() {
            return Err(Error::Manifest(format!(
                "snapshot transfer for {} is incomplete: {} of {} bytes",
                self.id.shard, self.written, self.id.total_bytes
            )));
        }
        drop(self.file);
        follower.install_snapshot(self.id.shard, self.id.epoch, self.id.lsn, &self.partial)?;
        let _ = std::fs::remove_file(&self.partial);
        let _ = std::fs::remove_file(&self.meta);
        tracing::info!(shard = %self.id.shard, bytes = self.written, "snapshot installed");
        Ok(())
    }

    fn persist_meta(&self) -> Result<()> {
        let body = format!("{}written={}\n", self.id.encode(), self.written);
        std::fs::write(&self.meta, body)
            .map_err(|e| Error::Manifest(format!("writing {}: {e}", self.meta.display())))
    }
}

/// Copy whatever remains of `source` into `transfer`.
///
/// Call again after an interruption; both sides carry their own progress.
pub fn pump(
    source: &mut SnapshotSource,
    transfer: &mut SnapshotTransfer,
    chunk: usize,
) -> Result<()> {
    if source.id() != transfer.id {
        return Err(Error::Manifest(format!(
            "snapshot identity mismatch for {}: source is epoch {} lsn {}, transfer expects \
             epoch {} lsn {}",
            transfer.id.shard, source.id.epoch, source.id.lsn, transfer.id.epoch, transfer.id.lsn
        )));
    }
    source.seek_to(transfer.offset())?;

    let mut buf = vec![0u8; chunk.max(1)];
    loop {
        let n = source.read_chunk(&mut buf)?;
        if n == 0 {
            break;
        }
        transfer.write_chunk(&buf[..n])?;
    }
    Ok(())
}
