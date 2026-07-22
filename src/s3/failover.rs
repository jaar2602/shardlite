//! Serving a shard from S3 on failover (slice 4).
//!
//! When a node becomes the owner of a shard it has no local copy of, it opens the shard's **latest
//! snapshot** straight from S3 over the read-write overlay ([`super::wvfs`]): reads fault from S3,
//! writes go to a local WAL, and it serves immediately — no download, no restore step.
//!
//! This is the mechanism the cluster's placement change would call. Wiring it into `ClusterNode`'s
//! ownership handover (so it fires automatically when a shard moves) is the remaining integration
//! step; here it is a direct, testable entry point.
//!
//! Freshness: the overlay opens the newest snapshot the sink has uploaded, so a failed-over shard is
//! current as of that snapshot. Replaying the change-log since the snapshot (for a tighter RPO) is a
//! refinement layered on top — the change-log objects are already archived by [`super::S3Sink`].

use std::path::Path;
use std::sync::Arc;

use crate::shard::ShardId;

use super::{S3Client, wvfs};

/// The key of the most recent snapshot archived for `shard` under `prefix`, if any. Snapshot keys
/// sort by `(epoch, lsn)`, so the lexicographically-largest is the newest.
pub fn latest_snapshot(
    client: &S3Client,
    prefix: &str,
    shard: ShardId,
) -> crate::Result<Option<String>> {
    let p = format!("{prefix}/shard_{}/snapshot/", shard.0);
    let mut keys = client.list(&p).map_err(to_err)?;
    keys.sort();
    Ok(keys.pop())
}

/// Open `shard` from its latest S3 snapshot over the read-write overlay, so this node serves it
/// immediately (reads from S3, writes to a local WAL under `scratch_dir`, which must exist).
pub fn open_from_s3(
    client: Arc<S3Client>,
    prefix: &str,
    shard: ShardId,
    scratch_dir: &Path,
) -> crate::Result<rusqlite::Connection> {
    let key = latest_snapshot(&client, prefix, shard)?.ok_or_else(|| {
        crate::Error::Protocol(format!("no S3 snapshot archived for shard {}", shard.0))
    })?;
    wvfs::open_readwrite(client, &key, scratch_dir)
}

fn to_err(e: super::S3Error) -> crate::Error {
    crate::Error::Protocol(format!("s3: {e}"))
}
