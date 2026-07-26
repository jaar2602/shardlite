//! Applying a replication stream to a local copy.
//!
//! A follower writes pages. It never executes SQL, which is the entire reason physical
//! replication was chosen: non-deterministic functions and per-machine errors cannot make
//! it diverge, because it does not evaluate anything.
//!
//! # Crash safety
//!
//! Pages are written and fsynced, and only then is the position recorded. A crash in
//! between replays some transactions on restart — which is harmless, because writing the
//! same page twice yields the same page. **Idempotence is what makes the cheap ordering
//! safe**; without it the position would have to be updated atomically with the data, which
//! raw page writes cannot do.
//!
//! Doing it the other way round — position first — would silently skip transactions after a
//! crash, which is undetectable corruption.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::shard::ShardId;

use super::stream::{Lsn, StreamTxn};

/// Where a follower has reached in one shard's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Position {
    pub epoch: u64,
    pub applied_lsn: Lsn,
}

/// What a follower needs before it can accept more frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// Ready for `lsn`.
    Next(Lsn),
    /// Cannot continue from frames alone — the shard must be re-seeded from a snapshot.
    Bootstrap { reason: String },
}

pub struct Follower {
    dir: PathBuf,
    positions: Mutex<BTreeMap<ShardId, Position>>,
    /// Lets readers on this node be excluded while pages are rewritten, and their cached
    /// connections invalidated afterwards. `None` when nothing on this node reads these
    /// files — a replica with no server in front of it.
    access: Mutex<Option<std::sync::Arc<crate::shard::mode::ShardModes>>>,
}

impl Follower {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::Manifest(format!("creating {}: {e}", dir.display())))?;
        let me = Self {
            dir: dir.to_path_buf(),
            positions: Mutex::new(BTreeMap::new()),
            access: Mutex::new(None),
        };
        me.load_positions()?;
        Ok(me)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Share the per-shard read/apply coordination with the node's readers.
    ///
    /// Without this a follower rewrites pages with no regard for connections that may be
    /// reading them. With it, applies exclude readers and invalidate their caches — which is
    /// what makes a followed shard readable at all.
    pub fn coordinate_with(&self, modes: std::sync::Arc<crate::shard::mode::ShardModes>) {
        *self.access.lock().expect("follower access mutex") = Some(modes);
    }

    /// Run `f` as an apply: readers excluded throughout, and their caches invalidated after.
    fn as_apply<T>(&self, shard: ShardId, f: impl FnOnce() -> T) -> T {
        let modes = self.access.lock().expect("follower access mutex").clone();
        match modes {
            Some(m) => m.access(shard).apply(f),
            None => f(),
        }
    }

    /// Every shard this follower holds a position for.
    ///
    /// A shard absent here is one this follower has nothing for, which an election must treat
    /// as position 0 rather than as agreement.
    pub fn positions(&self) -> BTreeMap<ShardId, Position> {
        self.positions.lock().expect("follower mutex").clone()
    }

    pub fn position(&self, shard: ShardId) -> Position {
        self.positions
            .lock()
            .expect("follower mutex")
            .get(&shard)
            .copied()
            .unwrap_or_default()
    }

    /// What this follower needs next for `shard` in `epoch`.
    /// This is deliberately **conservative**: a follower that has never seen the shard asks
    /// for a bootstrap, even though [`Self::apply`] would accept a stream starting at LSN 1.
    ///
    /// The two differ because they answer different questions. `apply` knows the frames in
    /// its hand and can prove nothing is missing. `need` is asked *before* any frames exist,
    /// and cannot know whether the primary still retains its history — an established
    /// primary has long since checkpointed LSN 1 away. Asking for a snapshot is always
    /// satisfiable; asking to replay from the beginning usually is not.
    ///
    /// A caller that knows the primary retains everything may stream from 1 anyway; `apply`
    /// will accept it.
    pub fn need(&self, shard: ShardId, epoch: u64) -> Need {
        let pos = self.position(shard);
        if pos.epoch == 0 && pos.applied_lsn == 0 {
            return Need::Bootstrap {
                reason: "this follower has no copy of the shard".into(),
            };
        }
        if pos.epoch != epoch {
            return Need::Bootstrap {
                reason: format!(
                    "follower is on epoch {} but the primary is on {epoch}; a new epoch is a \
                     new stream and its LSNs cannot be compared to the old ones",
                    pos.epoch
                ),
            };
        }
        Need::Next(pos.applied_lsn + 1)
    }

    /// Apply transactions for `shard`, which must be contiguous and start where this
    /// follower left off.
    ///
    /// Rejects gaps rather than applying across them. A page written without its
    /// predecessors produces a database that is silently wrong — corruption that
    /// `integrity_check` may well call fine, because the pages are individually valid.
    pub fn apply(&self, shard: ShardId, epoch: u64, txns: &[StreamTxn]) -> Result<()> {
        if txns.is_empty() {
            return Ok(());
        }

        let pos = self.position(shard);
        let fresh = pos.epoch == 0 && pos.applied_lsn == 0;
        if !fresh && pos.epoch != epoch {
            return Err(Error::ReplicationGap {
                shard: shard.to_string(),
                expected: pos.applied_lsn + 1,
                got: txns[0].lsn,
                detail: format!("epoch {} does not match the primary's {epoch}", pos.epoch),
            });
        }
        if fresh && txns[0].lsn != 1 {
            // Nothing held locally, but the stream does not start at its beginning — the
            // missing prefix can only come from a snapshot.
            return Err(Error::ReplicationGap {
                shard: shard.to_string(),
                expected: 1,
                got: txns[0].lsn,
                detail: "an uninitialised follower can only join a stream at LSN 1".into(),
            });
        }

        for (expect, t) in (pos.applied_lsn + 1..).zip(txns.iter()) {
            if t.lsn != expect {
                tracing::error!(
                    %shard,
                    epoch,
                    expected = expect,
                    got = t.lsn,
                    "replication gap; refusing to apply across it"
                );
                return Err(Error::ReplicationGap {
                    shard: shard.to_string(),
                    expected: expect,
                    got: t.lsn,
                    detail: "the stream is not contiguous".into(),
                });
            }
        }

        let path = shard.path(&self.dir);
        let raw: Vec<crate::vfs::CommittedTxn> = txns.iter().map(|t| t.txn.clone()).collect();
        self.as_apply(shard, || crate::vfs::apply_to_db_file(&path, &raw))
            .map_err(|e| Error::Open {
                path: path.display().to_string(),
                source: rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some(e.to_string()),
                ),
            })?;

        // Pages are durable before the position moves. A crash here replays them, which is
        // harmless; the reverse order would skip them, which is not.
        self.set_position(
            shard,
            Position {
                epoch,
                applied_lsn: txns.last().expect("non-empty").lsn,
            },
        )
    }

    /// Install a snapshot of `shard` taken at `(epoch, lsn)`, replacing whatever is there.
    pub fn install_snapshot(
        &self,
        shard: ShardId,
        epoch: u64,
        lsn: Lsn,
        snapshot: &Path,
    ) -> Result<()> {
        let dest = shard.path(&self.dir);
        // Copy to a temporary name and rename, so an interrupted install cannot leave a
        // half-written database that looks complete.
        let tmp = dest.with_extension("db.installing");
        std::fs::copy(snapshot, &tmp)
            .map_err(|e| Error::Manifest(format!("copying snapshot to {}: {e}", tmp.display())))?;
        std::fs::File::open(&tmp)
            .and_then(|file| file.sync_all())
            .map_err(|e| Error::Manifest(format!("fsyncing snapshot {}: {e}", tmp.display())))?;

        // Validate the private install before replacing the live file. A transport or storage
        // fault must leave the previous follower image and position untouched so the caller can
        // retry from a fresh snapshot.
        let integrity = crate::rusqlite::Connection::open(&tmp)
            .and_then(|connection| {
                connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            })
            .map_err(|error| {
                Error::Manifest(format!("checking snapshot {}: {error}", tmp.display()))
            })?;
        if integrity != "ok" {
            let _ = std::fs::remove_file(&tmp);
            return Err(Error::Manifest(format!(
                "snapshot {} failed integrity_check: {integrity}",
                tmp.display()
            )));
        }

        // The rename and the WAL removal are an apply: a reader holding a connection across
        // the rename keeps the *deleted inode* and serves a database frozen at this instant,
        // forever, with no error. Excluding readers and bumping the generation is the only
        // thing that prevents it.
        self.as_apply(shard, || -> Result<()> {
            std::fs::rename(&tmp, &dest)
                .map_err(|e| Error::Manifest(format!("installing {}: {e}", dest.display())))?;
            // Any WAL left from a previous life describes a database that no longer exists.
            let _ = std::fs::remove_file(crate::storage::checkpoint::wal_path_for(&dest));
            sync_parent(&dest)?;
            Ok(())
        })?;

        tracing::info!(%shard, epoch, lsn, "installed snapshot");
        self.set_position(
            shard,
            Position {
                epoch,
                applied_lsn: lsn,
            },
        )
    }

    fn position_path(&self, shard: ShardId) -> PathBuf {
        self.dir.join(format!("shard_{}.position", shard.0))
    }

    fn set_position(&self, shard: ShardId, pos: Position) -> Result<()> {
        let path = self.position_path(shard);
        let tmp = path.with_extension("position.tmp");
        {
            let mut file = std::fs::File::create(&tmp)
                .map_err(|e| Error::Manifest(format!("creating {}: {e}", tmp.display())))?;
            write!(file, "epoch={}\nlsn={}\n", pos.epoch, pos.applied_lsn)
                .and_then(|_| file.sync_all())
                .map_err(|e| Error::Manifest(format!("persisting {}: {e}", tmp.display())))?;
        }
        std::fs::rename(&tmp, &path)
            .map_err(|e| Error::Manifest(format!("installing {}: {e}", path.display())))?;
        sync_parent(&path)?;
        self.positions
            .lock()
            .expect("follower mutex")
            .insert(shard, pos);
        Ok(())
    }

    fn load_positions(&self) -> Result<()> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Ok(());
        };
        let mut map = BTreeMap::new();
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(id) = name
                .strip_prefix("shard_")
                .and_then(|r| r.strip_suffix(".position"))
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(text) = std::fs::read_to_string(e.path()) else {
                continue;
            };
            let mut pos = Position::default();
            for line in text.lines() {
                if let Some((k, v)) = line.split_once('=') {
                    match k {
                        "epoch" => pos.epoch = v.trim().parse().unwrap_or(0),
                        "lsn" => pos.applied_lsn = v.trim().parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
            map.insert(ShardId(id), pos);
        }
        *self.positions.lock().expect("follower mutex") = map;
        Ok(())
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().expect("replica path has parent");
    if let Ok(directory) = std::fs::File::open(parent) {
        directory
            .sync_all()
            .map_err(|e| Error::Manifest(format!("fsyncing {}: {e}", parent.display())))?;
    }
    Ok(())
}
