//! The one piece of observability the database does not provide: a short *history* of stats for
//! charting. meshdb reports its counters as an instantaneous snapshot; it deliberately keeps no
//! time series (a 150 MB database is not going to also be a metrics store). So the console keeps
//! its own: a background thread samples each connection's `/v1/stats` on a timer into a bounded
//! in-memory ring. Bounded and in-memory — the console does not become a time-series database
//! either; it holds only a rolling recent window for sparklines.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::proxy;
use crate::registry::Registry;

/// One poll of a connection's stats, stamped with wall-clock time for the chart x-axis.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// Unix milliseconds.
    pub t: u64,
    pub stats: Value,
}

/// How often each connection is polled, and how many samples are retained. 5 s × 240 ≈ the last
/// 20 minutes — enough for a live view without unbounded growth.
const INTERVAL: Duration = Duration::from_secs(5);
const CAPACITY: usize = 240;

#[derive(Default)]
pub struct Metrics {
    rings: RwLock<HashMap<String, VecDeque<Sample>>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&self, name: &str, sample: Sample) {
        let mut rings = self.rings.write().unwrap();
        let ring = rings.entry(name.to_string()).or_default();
        if ring.len() == CAPACITY {
            ring.pop_front();
        }
        ring.push_back(sample);
    }

    /// The retained samples for a connection, oldest first.
    pub fn history(&self, name: &str) -> Vec<Sample> {
        self.rings
            .read()
            .unwrap()
            .get(name)
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Drop a connection's history when the connection is removed, so the map does not leak
    /// entries for connections that no longer exist.
    pub fn forget(&self, name: &str) {
        self.rings.write().unwrap().remove(name);
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Spawn the sampler. It walks every connection each interval, polls `/v1/stats`, and records the
/// result. A connection that is unreachable or unsealable is simply skipped this round — one bad
/// connection never stops sampling the others, and never crashes the console.
pub fn spawn(registry: Arc<Registry>, metrics: Arc<Metrics>) {
    std::thread::Builder::new()
        .name("console-metrics".into())
        .spawn(move || loop {
            for name in registry.names() {
                if let Ok(resolved) = registry.resolve(&name) {
                    if let Some(stats) = proxy::fetch_json(&resolved, "stats") {
                        metrics.push(
                            &name,
                            Sample {
                                t: now_millis(),
                                stats,
                            },
                        );
                    }
                }
            }
            std::thread::sleep(INTERVAL);
        })
        .expect("spawning the metrics sampler");
}
