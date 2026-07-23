//! Bounded multi-seed observability collection.
//!
//! A fixed worker pool polls every enabled profile without letting unreachable clusters create
//! unbounded threads or queues. Each seed is independent evidence: failures retain the last good
//! payload and its timestamp, and aggregation explicitly reports disagreement rather than merging
//! conflicting leaders/terms into a fictional single truth.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::proxy;
use crate::registry::{Registry, Resolved};

const INTERVAL: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const STALE_AFTER_MS: u64 = 15_000;
const CAPACITY: usize = 240;
const WORKERS: usize = 8;
const PER_CLUSTER_CONCURRENCY: usize = 4;
const QUEUE_CAPACITY: usize = 256;
const PERSIST_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub t: u64,
    #[serde(default)]
    pub source: String,
    pub stats: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeObservation {
    pub seed: String,
    pub last_attempt_ms: u64,
    pub last_success_ms: Option<u64>,
    pub latency_ms: Option<u128>,
    pub error: Option<String>,
    pub meta: Option<Value>,
    pub health: Option<Value>,
    pub topology: Option<Value>,
    pub shards: Option<Value>,
    pub stats: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterSummary {
    pub name: String,
    pub status: String,
    pub observed_at_ms: u64,
    pub last_success_ms: Option<u64>,
    pub stale: bool,
    pub seeds: usize,
    pub reachable_nodes: usize,
    pub node_count: usize,
    pub leader: Option<String>,
    pub versions: Vec<String>,
    pub preferred_seed: Option<String>,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterObservation {
    #[serde(flatten)]
    pub summary: ClusterSummary,
    pub nodes: Vec<NodeObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaPosition {
    pub node: String,
    pub epoch: u64,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardInventoryRow {
    pub id: u64,
    pub owner: Option<String>,
    pub primary_node: Option<String>,
    pub epoch: u64,
    pub primary_lsn: u64,
    pub replicas: Vec<ReplicaPosition>,
    pub max_lag: Option<u64>,
    pub state: String,
    pub evidence: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardInventory {
    pub observed_at_ms: u64,
    pub rows: Vec<ShardInventoryRow>,
}

pub struct Metrics {
    rings: RwLock<HashMap<String, VecDeque<Sample>>>,
    observations: RwLock<HashMap<String, HashMap<String, NodeObservation>>>,
    path: Option<PathBuf>,
    last_persist: Mutex<Instant>,
}

#[derive(Default, Serialize, Deserialize)]
struct DurableState {
    rings: HashMap<String, VecDeque<Sample>>,
    observations: HashMap<String, HashMap<String, NodeObservation>>,
}

impl Metrics {
    pub fn open(path: &Path) -> Result<Self, String> {
        let durable = if path.exists() {
            let bytes = std::fs::read(path)
                .map_err(|error| format!("reading {}: {error}", path.display()))?;
            serde_json::from_slice(&bytes)
                .map_err(|error| format!("parsing {}: {error}", path.display()))?
        } else {
            DurableState::default()
        };
        Ok(Self {
            rings: RwLock::new(durable.rings),
            observations: RwLock::new(durable.observations),
            path: Some(path.to_path_buf()),
            last_persist: Mutex::new(Instant::now()),
        })
    }

    fn record(&self, name: &str, result: PollResult) {
        let mut observations = self.observations.write().unwrap();
        let existing = observations
            .entry(name.to_string())
            .or_default()
            .entry(result.seed.clone())
            .or_insert_with(|| NodeObservation {
                seed: result.seed.clone(),
                last_attempt_ms: result.attempted_at_ms,
                last_success_ms: None,
                latency_ms: None,
                error: None,
                meta: None,
                health: None,
                topology: None,
                shards: None,
                stats: None,
            });
        existing.last_attempt_ms = result.attempted_at_ms;
        existing.error = result.error;
        if existing.error.is_none() {
            existing.last_success_ms = Some(result.attempted_at_ms);
            existing.latency_ms = Some(result.latency_ms);
            existing.meta = result.meta;
            existing.health = result.health;
            existing.topology = result.topology;
            existing.shards = result.shards;
            existing.stats = result.stats.clone();
        }
        drop(observations);

        if let Some(stats) = result.stats {
            let mut rings = self.rings.write().unwrap();
            let ring = rings.entry(name.to_string()).or_default();
            if ring.len() == CAPACITY {
                ring.pop_front();
            }
            ring.push_back(Sample {
                t: result.attempted_at_ms,
                source: result.seed.clone(),
                stats,
            });
        }
        self.persist(false);
    }

    pub fn history(&self, name: &str) -> Vec<Sample> {
        self.rings
            .read()
            .unwrap()
            .get(name)
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn forget(&self, name: &str) {
        self.rings.write().unwrap().remove(name);
        self.observations.write().unwrap().remove(name);
        self.persist(true);
    }

    pub fn fleet(&self, registry: &Registry) -> Vec<ClusterSummary> {
        let observations = self.observations.read().unwrap();
        let mut summaries: Vec<_> = registry
            .list()
            .into_iter()
            .map(|connection| {
                summarize(
                    &connection.name,
                    connection.seeds.len(),
                    observations.get(&connection.name),
                    registry.preferred_seed(&connection.name),
                )
            })
            .collect();
        summaries.sort_by(|left, right| left.name.cmp(&right.name));
        summaries
    }

    pub fn observation(&self, name: &str, registry: &Registry) -> Option<ClusterObservation> {
        let connection = registry.list().into_iter().find(|item| item.name == name)?;
        let observations = self.observations.read().unwrap();
        let nodes: Vec<_> = observations
            .get(name)
            .map(|by_seed| {
                let mut values: Vec<_> = by_seed.values().cloned().collect();
                // Overview/topology payloads stay O(nodes), not O(nodes × shards). The dedicated
                // inventory endpoint below collapses all positions to one row per shard.
                for value in &mut values {
                    value.shards = None;
                }
                values.sort_by(|left, right| left.seed.cmp(&right.seed));
                values
            })
            .unwrap_or_default();
        let summary = summarize(
            name,
            connection.seeds.len(),
            observations.get(name),
            registry.preferred_seed(name),
        );
        Some(ClusterObservation { summary, nodes })
    }

    pub fn shard_inventory(&self, name: &str, registry: &Registry) -> Option<ShardInventory> {
        registry.list().into_iter().find(|item| item.name == name)?;
        #[derive(Default)]
        struct Aggregate {
            primaries: Vec<ReplicaPosition>,
            owners: Vec<String>,
            replicas: Vec<ReplicaPosition>,
            evidence: usize,
        }
        let observations = self.observations.read().unwrap();
        let mut aggregate: HashMap<u64, Aggregate> = HashMap::new();
        for node in observations
            .get(name)
            .into_iter()
            .flat_map(|nodes| nodes.values())
        {
            let Some(contract) = node.shards.as_ref() else {
                continue;
            };
            let node_id = value_string(contract, "node")
                .or_else(|| {
                    node.meta
                        .as_ref()
                        .and_then(|meta| value_string(meta, "node"))
                })
                .unwrap_or_else(|| node.seed.clone());
            let Some(shards) = contract.get("shards").and_then(Value::as_array) else {
                continue;
            };
            for shard in shards {
                let Some(id) = shard.get("id").and_then(Value::as_u64) else {
                    continue;
                };
                let entry = aggregate.entry(id).or_default();
                entry.evidence += 1;
                let epoch = shard.get("epoch").and_then(Value::as_u64).unwrap_or(0);
                let lsn = shard.get("lsn").and_then(Value::as_u64).unwrap_or(0);
                let position = ReplicaPosition {
                    node: node_id.clone(),
                    epoch,
                    lsn,
                };
                match shard.get("local_role").and_then(Value::as_str) {
                    Some("primary") => {
                        entry.primaries.push(position);
                        if let Some(owner) = value_string(shard, "owner") {
                            entry.owners.push(owner);
                        }
                    }
                    Some("replica") => entry.replicas.push(position),
                    _ => {}
                }
            }
        }
        let mut rows: Vec<_> = aggregate
            .into_iter()
            .map(|(id, mut value)| {
                value
                    .primaries
                    .sort_by(|left, right| left.node.cmp(&right.node));
                value
                    .replicas
                    .sort_by(|left, right| left.node.cmp(&right.node));
                value.owners.sort();
                value.owners.dedup();
                let primary = value.primaries.first();
                let incomparable = primary.is_some_and(|primary| {
                    value
                        .replicas
                        .iter()
                        .any(|replica| replica.epoch != primary.epoch)
                });
                let max_lag = primary.and_then(|primary| {
                    let comparable: Vec<_> = value
                        .replicas
                        .iter()
                        .filter(|replica| replica.epoch == primary.epoch)
                        .collect();
                    (!comparable.is_empty()).then(|| {
                        comparable
                            .iter()
                            .map(|replica| primary.lsn.saturating_sub(replica.lsn))
                            .max()
                            .unwrap_or(0)
                    })
                });
                let state = if value.primaries.is_empty() {
                    "unavailable"
                } else if value.primaries.len() > 1 || value.owners.len() > 1 {
                    "conflict"
                } else if incomparable {
                    "incomparable"
                } else {
                    "available"
                };
                ShardInventoryRow {
                    id,
                    owner: value
                        .owners
                        .first()
                        .cloned()
                        .or_else(|| primary.map(|item| item.node.clone())),
                    primary_node: primary.map(|item| item.node.clone()),
                    epoch: primary.map(|item| item.epoch).unwrap_or(0),
                    primary_lsn: primary.map(|item| item.lsn).unwrap_or(0),
                    replicas: value.replicas,
                    max_lag,
                    state: state.into(),
                    evidence: value.evidence,
                }
            })
            .collect();
        rows.sort_by_key(|row| row.id);
        Some(ShardInventory {
            observed_at_ms: now_millis(),
            rows,
        })
    }

    fn persist(&self, force: bool) {
        let Some(path) = &self.path else { return };
        let mut last = self.last_persist.lock().unwrap();
        if !force && last.elapsed() < PERSIST_INTERVAL {
            return;
        }
        let mut persisted_observations = self.observations.read().unwrap().clone();
        for node in persisted_observations
            .values_mut()
            .flat_map(HashMap::values_mut)
        {
            node.shards = None;
        }
        let state = DurableState {
            rings: self.rings.read().unwrap().clone(),
            observations: persisted_observations,
        };
        match serde_json::to_vec(&state)
            .map_err(|error| error.to_string())
            .and_then(|bytes| crate::store::write_atomic(path, &bytes))
        {
            Ok(()) => *last = Instant::now(),
            Err(error) => eprintln!("shardlite-console: persisting observability state: {error}"),
        }
    }
}

fn summarize(
    name: &str,
    configured_seeds: usize,
    observations: Option<&HashMap<String, NodeObservation>>,
    preferred_seed: Option<String>,
) -> ClusterSummary {
    let now = now_millis();
    let nodes: Vec<&NodeObservation> = observations
        .map(|values| values.values().collect())
        .unwrap_or_default();
    let successful: Vec<_> = nodes
        .iter()
        .copied()
        .filter(|node| node.last_success_ms.is_some())
        .collect();
    let fresh: Vec<_> = successful
        .iter()
        .copied()
        .filter(|node| {
            node.error.is_none()
                && now.saturating_sub(node.last_success_ms.unwrap_or(0)) <= STALE_AFTER_MS
        })
        .collect();
    let last_success_ms = successful
        .iter()
        .filter_map(|node| node.last_success_ms)
        .max();
    let stale = last_success_ms.is_some_and(|time| now.saturating_sub(time) > STALE_AFTER_MS);
    let mut issues = Vec::new();
    let failures = nodes.iter().filter(|node| node.error.is_some()).count();
    if failures > 0 {
        issues.push(format!(
            "{failures} database endpoint{} unreachable",
            if failures == 1 { "" } else { "s" }
        ));
    }
    if stale {
        issues.push("last successful observation is stale".into());
    }

    let mut leaders: Vec<String> = fresh
        .iter()
        .filter_map(|node| value_string(node.topology.as_ref()?, "leader"))
        .collect();
    leaders.sort();
    leaders.dedup();
    if leaders.len() > 1 {
        issues.push(format!("leader disagreement: {}", leaders.join(", ")));
    }
    let mut terms: Vec<String> = fresh
        .iter()
        .filter_map(|node| value_string(node.topology.as_ref()?, "term"))
        .collect();
    terms.sort();
    terms.dedup();
    if terms.len() > 1 {
        issues.push(format!("term disagreement: {}", terms.join(", ")));
    }
    let mut versions: Vec<String> = successful
        .iter()
        .filter_map(|node| value_string(node.meta.as_ref()?, "version"))
        .collect();
    versions.sort();
    versions.dedup();
    if versions.len() > 1 {
        issues.push(format!("version skew: {}", versions.join(", ")));
    }
    for node in &fresh {
        let health = node
            .health
            .as_ref()
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if !matches!(health, "healthy" | "ok") {
            issues.push(format!("{} reports {health}", node.seed));
        }
        let topology_stats = node.topology.as_ref().and_then(|value| value.get("stats"));
        if topology_stats
            .and_then(|value| value.get("stepped_down"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
        {
            issues.push("leadership step-downs observed".into());
        }
        if topology_stats
            .and_then(|value| value.get("handover_failed"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
        {
            issues.push("data handover failures observed".into());
        }
        let checkpoint = node
            .stats
            .as_ref()
            .and_then(|value| value.get("checkpoint"));
        if checkpoint
            .and_then(|value| value.get("failures"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
        {
            issues.push("checkpoint failures observed".into());
        } else if checkpoint
            .and_then(|value| value.get("stalls"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
        {
            issues.push("checkpoint stalls observed".into());
        }
    }
    issues.sort();
    issues.dedup();

    let node_ids: HashSet<String> = successful
        .iter()
        .filter_map(|node| {
            node.meta
                .as_ref()
                .and_then(|meta| value_string(meta, "node"))
                .or_else(|| {
                    node.topology
                        .as_ref()
                        .and_then(|value| value_string(value, "node"))
                })
                .or_else(|| Some(node.seed.clone()))
        })
        .collect();
    let status = if fresh.is_empty() {
        "unavailable"
    } else if issues.is_empty() {
        "healthy"
    } else {
        "degraded"
    };
    ClusterSummary {
        name: name.to_string(),
        status: status.into(),
        observed_at_ms: now,
        last_success_ms,
        stale,
        seeds: configured_seeds,
        reachable_nodes: fresh.len(),
        node_count: node_ids.len(),
        leader: (leaders.len() == 1).then(|| leaders[0].clone()),
        versions,
        preferred_seed,
        issues,
    }
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    match value.get(key)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[derive(Debug)]
struct PollResult {
    seed: String,
    attempted_at_ms: u64,
    latency_ms: u128,
    error: Option<String>,
    meta: Option<Value>,
    health: Option<Value>,
    topology: Option<Value>,
    shards: Option<Value>,
    stats: Option<Value>,
}

fn poll(resolved: &Resolved) -> PollResult {
    let started = Instant::now();
    let attempted_at_ms = now_millis();
    let meta = fetch_compatible(resolved, "meta", "info");
    let topology = fetch_compatible(resolved, "topology", "cluster");
    let health = proxy::fetch_json_result(resolved, "health")
        .map(|(value, _)| value)
        .ok()
        .or_else(|| derive_health(topology.as_ref().ok()));
    let shards = proxy::fetch_json_result(resolved, "shards")
        .map(|(value, _)| value)
        .ok()
        .or_else(|| derive_shards(meta.as_ref().ok(), topology.as_ref().ok()));
    let stats = proxy::fetch_json_result(resolved, "stats")
        .map(|(value, _)| value)
        .ok();
    let error = match (&meta, &topology) {
        (Err(meta), Err(topology)) => Some(format!("metadata: {meta}; topology: {topology}")),
        (Err(meta), _) => Some(format!("metadata: {meta}")),
        (_, Err(topology)) => Some(format!("topology: {topology}")),
        _ => None,
    };
    PollResult {
        seed: resolved.url.clone(),
        attempted_at_ms,
        latency_ms: started.elapsed().as_millis(),
        error,
        meta: meta.ok(),
        health,
        topology: topology.ok(),
        shards,
        stats,
    }
}

fn fetch_compatible(resolved: &Resolved, preferred: &str, legacy: &str) -> Result<Value, String> {
    proxy::fetch_json_result(resolved, preferred)
        .or_else(|_| proxy::fetch_json_result(resolved, legacy))
        .map(|(value, _)| value)
}

fn derive_health(topology: Option<&Value>) -> Option<Value> {
    let topology = topology?;
    let clustered = topology
        .get("clustered")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let leader = topology.get("leader").filter(|leader| !leader.is_null());
    Some(json!({
        "status": if !clustered || leader.is_some() { "healthy" } else { "unavailable" },
        "observed_at_ms": now_millis(),
        "derived": true,
    }))
}

fn derive_shards(meta: Option<&Value>, topology: Option<&Value>) -> Option<Value> {
    let meta = meta?;
    let count = meta.get("shard_count")?.as_u64()?;
    let topology = topology;
    let observer = topology.and_then(|value| value.get("node")).cloned();
    let assignments = topology
        .and_then(|value| value.get("placement"))
        .and_then(|value| value.get("assignments"));
    let shards: Vec<_> = (0..count)
        .map(|id| {
            let owner = assignments
                .and_then(|values| values.get(id.to_string()))
                .cloned();
            let primary = owner.as_ref() == observer.as_ref() || topology.is_none();
            json!({
                "id": id,
                "owner": owner,
                "local_role": if primary { "primary" } else { "unknown" },
                "epoch": 0,
                "lsn": 0,
                "derived": true,
            })
        })
        .collect();
    Some(
        json!({ "api_version": 0, "observed_at_ms": now_millis(), "node": observer, "shards": shards }),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JobKey {
    cluster: String,
    seed: String,
}

struct Job {
    key: JobKey,
    resolved: Resolved,
}

struct Backoff {
    queued: bool,
    failures: u32,
    next: Instant,
}

pub fn spawn(registry: Arc<Registry>, metrics: Arc<Metrics>) {
    let (sender, receiver) = mpsc::sync_channel::<Job>(QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    let schedule: Arc<Mutex<HashMap<JobKey, Backoff>>> = Arc::new(Mutex::new(HashMap::new()));

    for index in 0..WORKERS {
        let receiver = Arc::clone(&receiver);
        let registry = Arc::clone(&registry);
        let metrics = Arc::clone(&metrics);
        let schedule = Arc::clone(&schedule);
        std::thread::Builder::new()
            .name(format!("console-observer-{index}"))
            .spawn(move || loop {
                let job = {
                    let receiver = receiver.lock().unwrap();
                    receiver.recv()
                };
                let Ok(job) = job else { break };
                let result = poll(&job.resolved);
                let success = result.error.is_none();
                if success {
                    registry.mark_preferred(&job.key.cluster, &job.key.seed);
                }
                metrics.record(&job.key.cluster, result);
                let mut schedule = schedule.lock().unwrap();
                if let Some(state) = schedule.get_mut(&job.key) {
                    state.queued = false;
                    state.failures = if success {
                        0
                    } else {
                        state.failures.saturating_add(1)
                    };
                    let exponent = state.failures.min(4);
                    let delay = (INTERVAL * (1u32 << exponent)).min(MAX_BACKOFF);
                    state.next = Instant::now() + delay + jitter(&job.key);
                }
            })
            .expect("spawning observability worker");
    }

    std::thread::Builder::new()
        .name("console-observer-scheduler".into())
        .spawn(move || loop {
            let now = Instant::now();
            let mut seen = HashSet::new();
            for name in registry.names() {
                let Ok(mut seeds) = registry.resolve_seeds(&name) else {
                    continue;
                };
                for resolved in &mut seeds {
                    resolved.timeout_ms = resolved.timeout_ms.min(5_000);
                }
                let mut queued_for_cluster = schedule
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(key, state)| key.cluster == name && state.queued)
                    .count();
                for resolved in seeds {
                    let key = JobKey {
                        cluster: name.clone(),
                        seed: resolved.url.clone(),
                    };
                    seen.insert(key.clone());
                    if queued_for_cluster >= PER_CLUSTER_CONCURRENCY {
                        break;
                    }
                    let mut states = schedule.lock().unwrap();
                    let state = states.entry(key.clone()).or_insert(Backoff {
                        queued: false,
                        failures: 0,
                        next: now,
                    });
                    if state.queued || now < state.next {
                        continue;
                    }
                    match sender.try_send(Job { key, resolved }) {
                        Ok(()) => {
                            state.queued = true;
                            queued_for_cluster += 1;
                        }
                        Err(mpsc::TrySendError::Full(_)) => break,
                        Err(mpsc::TrySendError::Disconnected(_)) => return,
                    }
                }
            }
            schedule.lock().unwrap().retain(|key, _| seen.contains(key));
            std::thread::sleep(Duration::from_millis(250));
        })
        .expect("spawning observability scheduler");
}

fn jitter(key: &JobKey) -> Duration {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    Duration::from_millis(hasher.finish() % 1_000)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicting_observers_are_reported_not_flattened() {
        let now = now_millis();
        let node = |seed: &str, leader: u64| NodeObservation {
            seed: seed.into(),
            last_attempt_ms: now,
            last_success_ms: Some(now),
            latency_ms: Some(1),
            error: None,
            meta: Some(json!({ "node": seed, "version": "1", "shard_count": 4 })),
            health: Some(json!({ "status": "healthy" })),
            topology: Some(json!({ "leader": leader, "term": 7 })),
            shards: None,
            stats: None,
        };
        let observations = HashMap::from([("a".into(), node("a", 1)), ("b".into(), node("b", 2))]);
        let summary = summarize("prod", 2, Some(&observations), None);
        assert_eq!(summary.status, "degraded");
        assert!(summary
            .issues
            .iter()
            .any(|issue| issue.contains("leader disagreement")));
        assert_eq!(summary.leader, None);
    }

    #[test]
    fn stale_evidence_never_looks_healthy() {
        let old = now_millis().saturating_sub(STALE_AFTER_MS + 1);
        let observations = HashMap::from([(
            "a".into(),
            NodeObservation {
                seed: "a".into(),
                last_attempt_ms: old,
                last_success_ms: Some(old),
                latency_ms: Some(1),
                error: None,
                meta: Some(json!({ "version": "1" })),
                health: Some(json!({ "status": "healthy" })),
                topology: Some(json!({ "leader": 1, "term": 1 })),
                shards: None,
                stats: None,
            },
        )]);
        let summary = summarize("prod", 1, Some(&observations), None);
        assert_eq!(summary.status, "unavailable");
        assert!(summary.stale);
    }

    #[test]
    fn a_current_failure_retains_evidence_but_is_not_counted_reachable() {
        let now = now_millis();
        let observations = HashMap::from([(
            "a".into(),
            NodeObservation {
                seed: "a".into(),
                last_attempt_ms: now,
                last_success_ms: Some(now),
                latency_ms: Some(1),
                error: Some("connection refused".into()),
                meta: Some(json!({ "node": 1, "version": "1" })),
                health: Some(json!({ "status": "healthy" })),
                topology: Some(json!({ "leader": 1, "term": 1 })),
                shards: None,
                stats: None,
            },
        )]);
        let summary = summarize("prod", 1, Some(&observations), None);
        assert_eq!(summary.reachable_nodes, 0);
        assert_eq!(summary.status, "unavailable");
        assert_eq!(summary.last_success_ms, Some(now));
    }

    #[test]
    fn observations_and_metric_history_survive_restart() {
        let path = std::env::temp_dir().join(format!(
            "shardlite-console-observability-{}.json",
            rand::random::<u64>()
        ));
        let metrics = Metrics::open(&path).unwrap();
        metrics.record(
            "prod",
            PollResult {
                seed: "https://one".into(),
                attempted_at_ms: now_millis(),
                latency_ms: 4,
                error: None,
                meta: Some(json!({ "version": "1" })),
                health: Some(json!({ "status": "healthy" })),
                topology: Some(json!({ "leader": 1 })),
                shards: Some(json!({ "shards": [] })),
                stats: Some(json!({ "reader": { "queries": 1 } })),
            },
        );
        metrics.persist(true);
        drop(metrics);

        let reopened = Metrics::open(&path).unwrap();
        assert_eq!(reopened.history("prod").len(), 1);
        assert_eq!(
            reopened
                .observations
                .read()
                .unwrap()
                .get("prod")
                .unwrap()
                .len(),
            1
        );
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn shard_evidence_collapses_to_one_row_with_epoch_safe_lag() {
        let suffix = rand::random::<u64>();
        let registry_path =
            std::env::temp_dir().join(format!("shardlite-console-registry-{suffix}.json"));
        let metrics_path =
            std::env::temp_dir().join(format!("shardlite-console-metrics-{suffix}.json"));
        let registry = Registry::open(
            &registry_path,
            crate::crypto::Sealer::from_passphrase("master"),
        )
        .unwrap();
        registry
            .put_config_seeds(
                "prod",
                vec!["http://node-1:4680".into(), "http://node-2:4680".into()],
                None,
                None,
                false,
                true,
                5_000,
                true,
                None,
                None,
                None,
            )
            .unwrap();
        let metrics = Metrics::open(&metrics_path).unwrap();
        let result = |seed: &str, node: u64, role: &str, lsn: u64| PollResult {
            seed: seed.into(),
            attempted_at_ms: now_millis(),
            latency_ms: 1,
            error: None,
            meta: Some(json!({ "node": node, "version": "1", "shard_count": 1 })),
            health: Some(json!({ "status": "healthy" })),
            topology: Some(json!({ "node": node, "leader": 1, "term": 1 })),
            shards: Some(json!({
                "node": node,
                "shards": [{ "id": 0, "owner": 1, "local_role": role, "epoch": 7, "lsn": lsn }]
            })),
            stats: None,
        };
        metrics.record("prod", result("http://node-1:4680", 1, "primary", 10));
        metrics.record("prod", result("http://node-2:4680", 2, "replica", 8));
        let inventory = metrics.shard_inventory("prod", &registry).unwrap();
        assert_eq!(inventory.rows.len(), 1);
        assert_eq!(inventory.rows[0].state, "available");
        assert_eq!(inventory.rows[0].max_lag, Some(2));
        assert_eq!(inventory.rows[0].replicas.len(), 1);
        std::fs::remove_file(registry_path).ok();
        std::fs::remove_file(metrics_path).ok();
    }
}
