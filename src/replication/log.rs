//! Bounded retention of recent frames, so a follower can catch up without a full bootstrap.
//!
//! # What retention is for, and what it is not
//!
//! Retention covers **blips**: a follower that missed a few seconds to a network hiccup or a
//! restart resumes from where it stopped. It does not cover long absences, and it is not
//! meant to — at a realistic write rate a few megabytes is a fraction of a second of
//! history. A follower that falls further behind bootstraps, which is bounded work with a
//! known cost, rather than the primary hoarding history against a follower that may never
//! return.
//!
//! # Memory is the binding constraint
//!
//! The budget is **total**, not per shard. A per-shard cap looks reasonable in isolation and
//! then multiplies by 64 — the same trap the connection LRU had, where an 8 MiB page cache
//! became 128 MB once the shard count was applied. Here the total is divided by the shard
//! count, so the number an operator configures is the number that gets used.
//!
//! # Evictions are counted
//!
//! Dropping the oldest frames is how the bound is kept, but it is also the moment a
//! follower's ability to catch up quietly disappears. Every eviction is counted and the
//! lowest retained position is exposed, so "why is this follower always bootstrapping?" has
//! an answer.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::shard::ShardId;

use super::FrameSink;
use super::stream::{Lsn, StreamTxn};

/// Sizing for retention.
#[derive(Debug, Clone)]
pub struct FrameLogConfig {
    /// Total bytes of frames to retain, across all shards.
    pub total_bytes: usize,
    /// Shards the budget is divided between.
    pub shard_count: u32,
}

impl FrameLogConfig {
    /// 16 MiB total. At the floor profile that is a real fraction of the memory budget, and
    /// still only a moment of history at speed — which is the honest shape of retention.
    pub fn floor(shard_count: u32) -> Self {
        Self {
            total_bytes: 16 * 1024 * 1024,
            shard_count: shard_count.max(1),
        }
    }

    fn per_shard(&self) -> usize {
        (self.total_bytes / self.shard_count.max(1) as usize).max(64 * 1024)
    }
}

#[derive(Debug, Default)]
struct ShardLog {
    txns: VecDeque<StreamTxn>,
    bytes: usize,
}

#[derive(Debug, Default)]
struct Counters {
    retained: AtomicU64,
    evicted: AtomicU64,
    served: AtomicU64,
    /// Requests that could not be served because the frames were already evicted.
    missed: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLogStats {
    pub retained_txns: u64,
    /// Transactions dropped to stay inside the budget. Each one is history a follower can
    /// no longer resume from.
    pub evicted_txns: u64,
    pub served_txns: u64,
    /// Subscriptions that had to be answered with a bootstrap because history was gone.
    pub missed_requests: u64,
    pub bytes: usize,
}

/// What a subscription request can be answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Served {
    /// Frames from the requested position onward.
    Frames(Vec<StreamTxn>),
    /// The follower is level with the primary; nothing to send.
    UpToDate,
    /// The requested position is no longer retained.
    TooOld { lowest_retained: Lsn },
}

/// Retains recent frames per shard, within a total budget.
///
/// Implements [`FrameSink`], so a primary uses it as its sink and followers pull from it.
#[derive(Debug)]
pub struct FrameLog {
    per_shard_bytes: usize,
    shards: Mutex<BTreeMap<ShardId, ShardLog>>,
    counters: Counters,
}

impl FrameLog {
    pub fn new(cfg: FrameLogConfig) -> Self {
        Self {
            per_shard_bytes: cfg.per_shard(),
            shards: Mutex::new(BTreeMap::new()),
            counters: Counters::default(),
        }
    }

    pub fn stats(&self) -> FrameLogStats {
        let g = self.shards.lock().expect("frame log mutex");
        FrameLogStats {
            retained_txns: self.counters.retained.load(Ordering::Relaxed),
            evicted_txns: self.counters.evicted.load(Ordering::Relaxed),
            served_txns: self.counters.served.load(Ordering::Relaxed),
            missed_requests: self.counters.missed.load(Ordering::Relaxed),
            bytes: g.values().map(|s| s.bytes).sum(),
        }
    }

    /// Lowest position still retained for `shard`, if any.
    pub fn lowest_retained(&self, shard: ShardId) -> Option<Lsn> {
        self.shards
            .lock()
            .expect("frame log mutex")
            .get(&shard)
            .and_then(|s| s.txns.front().map(|t| t.lsn))
    }

    /// Highest position retained for `shard`, if any.
    pub fn highest_retained(&self, shard: ShardId) -> Option<Lsn> {
        self.shards
            .lock()
            .expect("frame log mutex")
            .get(&shard)
            .and_then(|s| s.txns.back().map(|t| t.lsn))
    }

    /// Answer a subscription starting at `from_lsn`, up to `max_txns`.
    pub fn serve(&self, shard: ShardId, from_lsn: Lsn, max_txns: usize) -> Served {
        let g = self.shards.lock().expect("frame log mutex");
        let Some(log) = g.get(&shard) else {
            // Nothing retained at all. Only "up to date" if the follower is asking for the
            // very first position; otherwise history it needs has gone.
            return if from_lsn <= 1 {
                Served::UpToDate
            } else {
                self.counters.missed.fetch_add(1, Ordering::Relaxed);
                Served::TooOld { lowest_retained: 0 }
            };
        };

        let Some(first) = log.txns.front() else {
            return Served::UpToDate;
        };
        let highest = log.txns.back().map(|t| t.lsn).unwrap_or(0);

        if from_lsn > highest {
            return Served::UpToDate;
        }
        if from_lsn < first.lsn {
            self.counters.missed.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                %shard,
                requested = from_lsn,
                lowest_retained = first.lsn,
                "subscription cannot be served from retention; the follower must bootstrap"
            );
            return Served::TooOld {
                lowest_retained: first.lsn,
            };
        }

        let batch: Vec<StreamTxn> = log
            .txns
            .iter()
            .filter(|t| t.lsn >= from_lsn)
            .take(max_txns)
            .cloned()
            .collect();
        self.counters
            .served
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        Served::Frames(batch)
    }
}

impl FrameSink for FrameLog {
    fn accept(&self, shard: ShardId, _epoch: u64, txns: Vec<StreamTxn>) -> crate::Result<()> {
        let mut g = self.shards.lock().expect("frame log mutex");
        let log = g.entry(shard).or_default();

        for t in txns {
            let size: usize = t.txn.frames.iter().map(|f| f.data.len()).sum();
            log.bytes += size;
            log.txns.push_back(t);
            self.counters.retained.fetch_add(1, Ordering::Relaxed);
        }

        // Evict oldest first. This is exactly the moment a follower's ability to resume
        // disappears, so it is counted rather than done silently.
        let mut evicted = 0u64;
        while log.bytes > self.per_shard_bytes && log.txns.len() > 1 {
            if let Some(old) = log.txns.pop_front() {
                let size: usize = old.txn.frames.iter().map(|f| f.data.len()).sum();
                log.bytes = log.bytes.saturating_sub(size);
                evicted += 1;
            }
        }
        if evicted > 0 {
            self.counters.evicted.fetch_add(evicted, Ordering::Relaxed);
            tracing::trace!(%shard, evicted, retained_bytes = log.bytes, "evicted old frames");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::{CommittedTxn, Frame};

    fn txn(lsn: Lsn, bytes: usize) -> StreamTxn {
        StreamTxn {
            lsn,
            txn: CommittedTxn {
                db_size_pages: 1,
                page_size: 4096,
                generation: 1,
                frames: vec![Frame {
                    page_no: 1,
                    data: vec![0u8; bytes],
                }],
            },
        }
    }

    fn log_of(total: usize) -> FrameLog {
        FrameLog::new(FrameLogConfig {
            total_bytes: total,
            shard_count: 1,
        })
    }

    #[test]
    fn serves_from_a_retained_position() {
        let log = log_of(1024 * 1024);
        let batch: Vec<StreamTxn> = (1..=10).map(|i| txn(i, 100)).collect();
        log.accept(ShardId(0), 1, batch).unwrap();

        match log.serve(ShardId(0), 4, 100) {
            Served::Frames(f) => {
                assert_eq!(f.len(), 7);
                assert_eq!(f[0].lsn, 4);
                assert_eq!(f.last().unwrap().lsn, 10);
            }
            other => panic!("expected frames, got {other:?}"),
        }
    }

    #[test]
    fn a_follower_level_with_the_primary_is_up_to_date() {
        let log = log_of(1024 * 1024);
        log.accept(ShardId(0), 1, (1..=5).map(|i| txn(i, 100)).collect())
            .unwrap();
        assert_eq!(log.serve(ShardId(0), 6, 100), Served::UpToDate);
    }

    #[test]
    fn evicted_history_is_reported_as_too_old_not_as_a_gap() {
        // The important distinction. Serving a batch that starts after the follower's
        // position would leave a hole it could not detect; saying "too old" sends it to
        // bootstrap instead.
        let log = log_of(64 * 1024);
        // ~40 KiB each, so only a couple fit in the 64 KiB floor.
        log.accept(ShardId(0), 1, (1..=8).map(|i| txn(i, 40_000)).collect())
            .unwrap();

        let lowest = log.lowest_retained(ShardId(0)).unwrap();
        assert!(lowest > 1, "old frames should have been evicted");

        match log.serve(ShardId(0), 1, 100) {
            Served::TooOld { lowest_retained } => assert_eq!(lowest_retained, lowest),
            other => panic!("expected TooOld, got {other:?}"),
        }
        assert!(log.stats().missed_requests > 0, "the miss must be counted");
    }

    #[test]
    fn retention_stays_inside_its_budget_and_counts_evictions() {
        let log = log_of(64 * 1024);
        log.accept(ShardId(0), 1, (1..=50).map(|i| txn(i, 10_000)).collect())
            .unwrap();

        let stats = log.stats();
        assert!(
            stats.bytes <= 64 * 1024 + 10_000,
            "retention exceeded its budget: {} bytes",
            stats.bytes
        );
        assert!(stats.evicted_txns > 0, "evictions must be counted");
        assert_eq!(stats.retained_txns, 50);
    }

    #[test]
    fn the_budget_is_divided_between_shards_not_multiplied_by_them() {
        // A per-shard cap looks reasonable and then multiplies by the shard count. The
        // configured total must be the total.
        let cfg = FrameLogConfig {
            total_bytes: 8 * 1024 * 1024,
            shard_count: 64,
        };
        let log = FrameLog::new(cfg);
        for s in 0..64u32 {
            log.accept(ShardId(s), 1, (1..=40).map(|i| txn(i, 20_000)).collect())
                .unwrap();
        }
        let bytes = log.stats().bytes;
        assert!(
            bytes <= 16 * 1024 * 1024,
            "64 shards retained {bytes} bytes against an 8 MiB budget"
        );
    }
}
