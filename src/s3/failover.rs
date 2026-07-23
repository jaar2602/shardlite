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
//! Freshness: the overlay opens the newest snapshot the sink uploaded, then **replays the archived
//! change-log** for every transaction committed after it ([`build_overlay`]). So a failed-over shard
//! is current as of the last committed transaction, not merely the last snapshot — the RPO is the
//! change-log upload interval, not the snapshot interval. The replayed pages ride over the S3 base as
//! a [`super::PageOverlay`]; new writes still land in the local WAL.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::shard::ShardId;

use super::{PageOverlay, S3Client, wvfs};

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

/// Open `shard` from its latest S3 snapshot over the read-write overlay, **replaying the archived
/// change-log** on top so it is current as of the last committed transaction. Reads come from S3
/// (plus the replayed pages), writes go to a local WAL under `scratch_dir` (which must exist).
pub fn open_from_s3(
    client: Arc<S3Client>,
    prefix: &str,
    shard: ShardId,
    scratch_dir: &Path,
) -> crate::Result<rusqlite::Connection> {
    let key = latest_snapshot(&client, prefix, shard)?.ok_or_else(|| {
        crate::Error::Protocol(format!("no S3 snapshot archived for shard {}", shard.0))
    })?;
    let (epoch, lsn) = parse_epoch_lsn(&key)
        .ok_or_else(|| crate::Error::Protocol(format!("unparseable snapshot key: {key}")))?;
    let overlay = build_overlay(&client, prefix, shard, epoch, lsn)?;
    wvfs::open_readwrite_with_overlay(client, &key, scratch_dir, overlay)
}

/// Collapse the archived change-log for `shard` — every transaction committed after the snapshot at
/// `(snap_epoch, snap_lsn)` — into a page overlay. Returns `None` when nothing was committed since
/// the snapshot (the snapshot is already current).
///
/// Refuses over approximates: a gap in the log (a missing WAL object) or a change-log that continues
/// in a **later epoch** than the snapshot (the WAL was reset by an unclean restart) both mean the
/// overlay cannot be reconstructed faithfully, so it errors rather than serving a shard silently
/// missing writes. The operator's remedy is a fresh snapshot.
pub fn build_overlay(
    client: &S3Client,
    prefix: &str,
    shard: ShardId,
    snap_epoch: u64,
    snap_lsn: u64,
) -> crate::Result<Option<PageOverlay>> {
    let p = format!("{prefix}/shard_{}/wal/", shard.0);
    let mut keys = client.list(&p).map_err(to_err)?;
    keys.sort();

    // Reject a change-log that ran on past the snapshot's epoch: crossing a WAL reset means the
    // snapshot is stale for the current generation and must be retaken.
    if keys
        .iter()
        .filter_map(|k| parse_epoch_lsn(k))
        .any(|(e, _)| e > snap_epoch)
    {
        return Err(crate::Error::Protocol(format!(
            "shard {} change-log continues past the snapshot's epoch {snap_epoch}; \
             the archive spans a WAL reset and needs a fresh snapshot",
            shard.0
        )));
    }

    // Decode every object in this epoch and gather the transactions after the snapshot, in order.
    let mut txns = Vec::new();
    for k in keys {
        match parse_epoch_lsn(&k) {
            Some((e, _)) if e == snap_epoch => {}
            _ => continue,
        }
        let bytes = client.get(&k).map_err(to_err)?;
        for t in super::sink::decode_change_log(&bytes)? {
            if t.lsn > snap_lsn {
                txns.push(t);
            }
        }
    }
    if txns.is_empty() {
        return Ok(None);
    }
    txns.sort_by_key(|t| t.lsn);

    // The log must be contiguous from snap_lsn+1: a hole is a lost transaction we will not paper over.
    for (expect, t) in (snap_lsn + 1..).zip(txns.iter()) {
        if t.lsn != expect {
            return Err(crate::Error::Protocol(format!(
                "shard {} change-log has a gap: expected lsn {expect}, found {}",
                shard.0, t.lsn
            )));
        }
    }

    // Collapse to the final image: later transactions win for a page they rewrite; the last
    // transaction's db size is the database's size after replay.
    let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
    let page_size = txns.last().unwrap().txn.page_size;
    let db_size = txns.last().unwrap().txn.db_size_pages as u64 * page_size as u64;
    for t in &txns {
        for f in &t.txn.frames {
            pages.insert(f.page_no, f.data.clone());
        }
    }
    Ok(Some(PageOverlay {
        pages,
        page_size,
        db_size,
    }))
}

/// Parse the `{epoch:020}_{lsn:020}` suffix a snapshot or WAL key ends with.
fn parse_epoch_lsn(key: &str) -> Option<(u64, u64)> {
    let name = key.rsplit('/').next()?;
    let (e, l) = name.split_once('_')?;
    Some((e.parse().ok()?, l.parse().ok()?))
}

/// Materialise `shard`'s **complete current image** from S3 — the latest snapshot with the archived
/// change-log replayed on top — as db bytes, plus `(epoch, snapshot_lsn, pages_replayed)`. This is
/// the reconstruct-to-local recovery path (see [`crate::shard::ShardManager::recover_shard_from_s3`]):
/// unlike [`open_from_s3`], which serves live from S3 over an overlay, this returns a self-contained
/// database the node then installs and serves locally, so there is never a second live view of the
/// shard.
pub fn recover_bytes(
    client: &S3Client,
    prefix: &str,
    shard: ShardId,
) -> crate::Result<(Vec<u8>, u64, u64, usize)> {
    let key = latest_snapshot(client, prefix, shard)?.ok_or_else(|| {
        crate::Error::Protocol(format!("no S3 snapshot archived for shard {}", shard.0))
    })?;
    let (epoch, snap_lsn) = parse_epoch_lsn(&key)
        .ok_or_else(|| crate::Error::Protocol(format!("unparseable snapshot key: {key}")))?;
    let mut db = client.get(&key).map_err(to_err)?;
    let overlay = build_overlay(client, prefix, shard, epoch, snap_lsn)?;
    let pages = match &overlay {
        Some(o) => {
            apply_overlay(&mut db, o);
            o.pages.len()
        }
        None => 0,
    };
    Ok((db, epoch, snap_lsn, pages))
}

/// Splice a change-log [`PageOverlay`] into `db` in place: overwrite each replayed page at its
/// offset (growing the buffer for pages past the snapshot's size), then trim to the post-replay
/// database size. The result is a byte-exact current image.
fn apply_overlay(db: &mut Vec<u8>, o: &PageOverlay) {
    let ps = o.page_size as usize;
    if (o.db_size as usize) > db.len() {
        db.resize(o.db_size as usize, 0);
    }
    for (&page_no, data) in &o.pages {
        let off = (page_no as usize - 1) * ps;
        if off + data.len() > db.len() {
            db.resize(off + data.len(), 0);
        }
        db[off..off + data.len()].copy_from_slice(data);
    }
    db.truncate(o.db_size as usize);
}

fn to_err(e: super::S3Error) -> crate::Error {
    crate::Error::Protocol(format!("s3: {e}"))
}
