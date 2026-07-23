//! Stream positions: how a follower knows it has everything, in order.
//!
//! Frames alone are not enough. A follower must be able to answer "am I missing anything?"
//! — and applying page N+2 without N produces a database that is silently wrong rather than
//! obviously broken. So every committed transaction carries a position.
//!
//! # Epoch and LSN
//!
//! A position is `(epoch, lsn)`.
//!
//! `lsn` is dense within an epoch: 1, 2, 3, … . A follower that sees a jump knows frames
//! were lost, because density is the property that makes loss detectable at all.
//!
//! `epoch` exists because density cannot survive a primary that restarts without knowing
//! where it stopped. On a clean shutdown the position is persisted and the next run
//! continues the same epoch. On an unclean one it cannot be trusted, so the epoch is bumped
//! and every follower must re-bootstrap. Bumping is expensive and deliberate: the
//! alternative is guessing at continuity, and a wrong guess is undetectable corruption.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::shard::ShardId;
use crate::vfs::CommittedTxn;

/// Position within one shard's stream. Dense within an epoch.
pub type Lsn = u64;

/// A committed transaction with its position.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamTxn {
    pub lsn: Lsn,
    pub txn: CommittedTxn,
}

const FILE_NAME: &str = "shardlite.stream";
const HEADER: &str = "shardlite-stream 1";

/// The primary's position in every shard's stream.
///
/// Persisted on clean shutdown so a restart can continue the same epoch. Written as the
/// same boring line-based text as the manifest, for the same reason: it can be read with
/// `cat` when something has gone wrong.
#[derive(Debug)]
pub struct StreamPositions {
    path: PathBuf,
    inner: Mutex<Positions>,
}

#[derive(Debug, Default, Clone)]
struct Positions {
    epoch: u64,
    /// Next LSN to hand out, per shard.
    next: BTreeMap<ShardId, Lsn>,
    /// False until a clean shutdown writes it, so a crash is detectable on reload.
    clean: bool,
}

impl StreamPositions {
    /// Load positions for `dir`, bumping the epoch if the last run did not shut down
    /// cleanly.
    pub fn load_or_init(dir: &Path) -> Result<Self> {
        let path = dir.join(FILE_NAME);
        let mut p = if path.exists() {
            Self::read(&path)?
        } else {
            Positions {
                epoch: 0,
                ..Positions::default()
            }
        };

        if !p.clean {
            // Either the first run, or the previous one died before persisting. Either way
            // the LSN it reached is unknown, so continuing the epoch would let a follower
            // accept a stream with an invisible hole.
            let previous = p.epoch;
            p.epoch += 1;
            p.next.clear();
            if previous > 0 {
                tracing::warn!(
                    previous_epoch = previous,
                    epoch = p.epoch,
                    "the last run did not shut down cleanly, so its stream position cannot \
                     be trusted; bumping the epoch, which forces every follower to \
                     re-bootstrap"
                );
            }
        }
        p.clean = false;

        let me = Self {
            path,
            inner: Mutex::new(p),
        };
        // Mark dirty immediately: a crash from here on must bump the epoch next time.
        me.persist()?;
        Ok(me)
    }

    pub fn epoch(&self) -> u64 {
        self.inner.lock().expect("stream mutex").epoch
    }

    /// Hand out `count` consecutive positions for `shard`.
    pub fn allocate(&self, shard: ShardId, count: usize) -> Lsn {
        let mut g = self.inner.lock().expect("stream mutex");
        let start = *g.next.entry(shard).or_insert(1);
        g.next.insert(shard, start + count as Lsn);
        start
    }

    /// Hand back `count` positions allocated but never shipped.
    ///
    /// Safe only because allocation happens on the shard's single writer thread, so no
    /// other allocation can interleave. Without this a refused batch would leave a
    /// permanent hole in the stream, and the follower would refuse everything after it.
    pub fn rollback(&self, shard: ShardId, count: usize) {
        let mut g = self.inner.lock().expect("stream mutex");
        if let Some(next) = g.next.get_mut(&shard) {
            *next = next.saturating_sub(count as Lsn).max(1);
        }
    }

    /// Last position handed out for `shard`, or 0 if none.
    pub fn last_allocated(&self, shard: ShardId) -> Lsn {
        self.inner
            .lock()
            .expect("stream mutex")
            .next
            .get(&shard)
            .map(|n| n.saturating_sub(1))
            .unwrap_or(0)
    }

    /// Record a clean shutdown, so the next run continues this epoch.
    pub fn mark_clean(&self) -> Result<()> {
        self.inner.lock().expect("stream mutex").clean = true;
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let g = self.inner.lock().expect("stream mutex");
        let mut body = format!("{HEADER}\nepoch={}\nclean={}\n", g.epoch, g.clean);
        for (shard, next) in &g.next {
            body.push_str(&format!("next.{}={}\n", shard.0, next));
        }
        std::fs::write(&self.path, body)
            .map_err(|e| Error::Manifest(format!("writing {}: {e}", self.path.display())))
    }

    fn read(path: &Path) -> Result<Positions> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Manifest(format!("reading {}: {e}", path.display())))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != HEADER {
            return Err(Error::Manifest(format!(
                "{} has header `{header}`, expected `{HEADER}`",
                path.display()
            )));
        }

        let mut p = Positions::default();
        for line in lines {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k.trim() {
                "epoch" => p.epoch = v.trim().parse().unwrap_or(0),
                "clean" => p.clean = v.trim() == "true",
                other => {
                    if let Some(id) = other.strip_prefix("next.")
                        && let (Ok(id), Ok(next)) = (id.parse::<u32>(), v.trim().parse::<Lsn>())
                    {
                        p.next.insert(ShardId(id), next);
                    }
                }
            }
        }
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn allocates_dense_positions_per_shard() {
        let dir = TempDir::new().unwrap();
        let p = StreamPositions::load_or_init(dir.path()).unwrap();

        assert_eq!(p.allocate(ShardId(0), 3), 1);
        assert_eq!(p.allocate(ShardId(0), 2), 4);
        assert_eq!(p.last_allocated(ShardId(0)), 5);

        // Shards are independent streams.
        assert_eq!(p.allocate(ShardId(1), 1), 1);
    }

    #[test]
    fn rolling_back_returns_positions_for_reuse() {
        // A batch the sink refused must not consume its place in the stream, or the
        // follower sees a permanent hole and refuses everything after it.
        let dir = TempDir::new().unwrap();
        let p = StreamPositions::load_or_init(dir.path()).unwrap();

        assert_eq!(p.allocate(ShardId(0), 5), 1);
        p.rollback(ShardId(0), 5);
        assert_eq!(
            p.allocate(ShardId(0), 5),
            1,
            "rolled-back positions must be handed out again"
        );
    }

    #[test]
    fn a_clean_shutdown_continues_the_epoch() {
        let dir = TempDir::new().unwrap();
        let epoch = {
            let p = StreamPositions::load_or_init(dir.path()).unwrap();
            p.allocate(ShardId(0), 10);
            p.mark_clean().unwrap();
            p.epoch()
        };

        let p2 = StreamPositions::load_or_init(dir.path()).unwrap();
        assert_eq!(p2.epoch(), epoch, "a clean restart keeps the epoch");
        assert_eq!(
            p2.allocate(ShardId(0), 1),
            11,
            "and continues the LSN where it stopped"
        );
    }

    #[test]
    fn an_unclean_shutdown_bumps_the_epoch_and_forces_rebootstrap() {
        // The whole reason epoch exists. Without persisting a position, the LSN reached is
        // unknown, and continuing would let a follower accept a stream with an invisible
        // hole in it.
        let dir = TempDir::new().unwrap();
        let epoch = {
            let p = StreamPositions::load_or_init(dir.path()).unwrap();
            p.allocate(ShardId(0), 10);
            p.epoch() // dropped without mark_clean
        };

        let p2 = StreamPositions::load_or_init(dir.path()).unwrap();
        assert_eq!(
            p2.epoch(),
            epoch + 1,
            "an unclean restart must bump the epoch"
        );
        assert_eq!(
            p2.allocate(ShardId(0), 1),
            1,
            "and restart LSNs, since the new epoch is a fresh stream"
        );
    }
}
