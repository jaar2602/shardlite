//! JSON conversions shared by the HTTP gateway and the JSON-over-TCP protocol.
//!
//! Both edges translate between JSON and the same `Request`/`Response` the native protocol
//! uses; this is the one place that mapping lives, so the two cannot drift.

use crate::storage::exec::{Statement, Value};

use super::protocol::{ReadConsistency, Response};

/// One SQL value as clean JSON: null, number, string. A blob becomes an array of byte values —
/// lossless, if verbose; a base64 form could come later.
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => serde_json::json!(i),
        Value::Real(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::json!(s),
        Value::Blob(b) => serde_json::json!(b),
    }
}

/// JSON parameters → SQL values. Null, integers, floats, strings — the ordinary bound-parameter
/// kinds. Arrays and objects are refused with a message rather than guessed at.
pub fn json_params(vals: &[serde_json::Value]) -> std::result::Result<Vec<Value>, String> {
    vals.iter()
        .map(|v| match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Integer(*b as i64)),
            serde_json::Value::Number(n) if n.is_i64() => Ok(Value::Integer(n.as_i64().unwrap())),
            serde_json::Value::Number(n) => Ok(Value::Real(n.as_f64().unwrap())),
            serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
            other => Err(format!("unsupported parameter type: {other}")),
        })
        .collect()
}

/// Build a `Statement` from a JSON `{sql, params}`-shaped object.
pub fn statement_from(
    sql: &str,
    params: &[serde_json::Value],
) -> std::result::Result<Statement, String> {
    Ok(Statement::with_params(sql, json_params(params)?))
}

/// Parse a consistency value: `"stale"`, `{"at_least_lsn": N}`, or default linearizable.
pub fn consistency_from(v: Option<&serde_json::Value>) -> ReadConsistency {
    match v {
        Some(serde_json::Value::String(s)) if s == "stale" => ReadConsistency::Stale,
        Some(serde_json::Value::Object(o)) => o
            .get("at_least_lsn")
            .and_then(|v| v.as_u64())
            .map(ReadConsistency::AtLeastLsn)
            .unwrap_or_default(),
        _ => ReadConsistency::Linearizable,
    }
}

/// A native `Response` → its JSON body.
pub fn response_json_body(resp: Response) -> serde_json::Value {
    use Response as R;
    match resp {
        R::Rows { columns, rows } => serde_json::json!({
            "columns": columns,
            "rows": rows.iter().map(|r| r.iter().map(value_to_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }),
        R::Changed {
            rows_affected,
            last_insert_rowid,
        } => serde_json::json!({
            "rows_affected": rows_affected, "last_insert_rowid": last_insert_rowid,
        }),
        R::Ok => serde_json::json!({ "ok": true }),
        R::AllShards { outcomes } => serde_json::json!({
            "shards": outcomes.iter().map(|(s, o)| serde_json::json!({
                "shard": s,
                "ok": matches!(o, super::protocol::ShardOutcome::Ok),
                "error": match o { super::protocol::ShardOutcome::Rejected(m) => Some(m.clone()), _ => None },
            })).collect::<Vec<_>>(),
        }),
        R::Routed { shard } => serde_json::json!({ "shard": shard }),
        R::SchemaVersion { shard, version } => {
            serde_json::json!({ "shard": shard, "version": version })
        }
        R::TooStale { shard, have, need } => serde_json::json!({
            "error": "too stale", "shard": shard, "have": have, "need": need,
        }),
        R::Users { users } => serde_json::json!({
            "users": users.iter().map(|(n, r)| serde_json::json!({ "name": n, "role": r.to_string() })).collect::<Vec<_>>(),
        }),
        R::Rejected { message } => serde_json::json!({ "error": message, "rejected": true }),
        R::Error { message, retryable } => {
            serde_json::json!({ "error": message, "retryable": retryable })
        }
        other => serde_json::json!({ "error": format!("unexpected response: {other:?}") }),
    }
}

/// The HTTP-style status a `Response` maps to, so both edges agree on success vs fault.
pub fn response_status(resp: &Response) -> u16 {
    use Response as R;
    match resp {
        R::Rejected { .. } => 400,
        R::TooStale { .. } => 409,
        R::Error {
            retryable: true, ..
        } => 503,
        R::Error { message, .. } if message.contains("not the leader") => 409,
        R::Error { message, .. } if message.contains("authentication") => 401,
        R::Error { message, .. } if message.contains("not permitted") => 403,
        R::Error { .. } => 400,
        _ => 200,
    }
}

use crate::shard::{ShardId, ShardManager};

use super::server::NodeServices;

/// `/v1/info`-equivalent JSON.
pub fn info_json(shards: &ShardManager) -> serde_json::Value {
    serde_json::json!({ "shard_count": shards.shard_count(), "epoch": shards.epoch() })
}

/// Fleet counters as JSON, shared by both edges' stats views.
pub fn fleet_stats_json(shards: &ShardManager) -> serde_json::Value {
    let w = shards.writer_stats();
    let r = shards.reader_stats();
    serde_json::json!({
        "writer": { "batches": w.batches, "requests": w.requests, "open_now": w.open_now, "threads": w.threads },
        "reader": { "queries": r.queries, "rejected_busy": r.rejected_busy, "timed_out": r.timed_out, "threads": r.threads },
    })
}

/// Cluster topology JSON. `clustered:false` on a standalone node.
pub fn cluster_json(shards: &ShardManager, services: &NodeServices) -> serde_json::Value {
    match services.cluster.as_ref() {
        None => serde_json::json!({ "clustered": false, "shard_count": shards.shard_count() }),
        Some(c) => {
            let p = c.placement();
            let assignments: serde_json::Map<String, serde_json::Value> = p
                .assignments
                .iter()
                .map(|(s, n)| (s.0.to_string(), serde_json::json!(n)))
                .collect();
            let s = c.stats();
            serde_json::json!({
                "clustered": true,
                "node": c.id(),
                "term": c.term(),
                "role": format!("{:?}", c.role()).to_lowercase(),
                "leader": c.leader(),
                "led_shards": c.led_shards().iter().map(|s| s.0).collect::<Vec<_>>(),
                "placement": { "term": p.term, "assignments": assignments },
                "stats": {
                    "elections_started": s.elections_started, "became_leader": s.became_leader,
                    "stepped_down": s.stepped_down, "heartbeats_sent": s.heartbeats_sent,
                    "peer_unreachable": s.peer_unreachable, "handover_failed": s.handover_failed,
                },
            })
        }
    }
}

/// The WAL frame inspector as JSON for `shard`.
pub fn frames_json_for(shards: &ShardManager, shard: u32) -> serde_json::Value {
    let db = ShardId(shard).path(shards.dir());
    let wal = crate::storage::checkpoint::wal_path_for(&db);
    let bytes = std::fs::read(&wal).unwrap_or_default();
    let report = crate::vfs::inspect_wal(&bytes);
    match &report.header {
        None => serde_json::json!({ "wal": false, "file_bytes": report.file_bytes }),
        Some(h) => serde_json::json!({
            "wal": true, "file_bytes": report.file_bytes, "page_size": h.page_size,
            "salt": h.salt.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "frames": report.frames.len(), "transactions": report.transactions(),
            "uncommitted_frames": report.uncommitted_frames(),
            "leftover_frames": report.frames.iter().filter(|f| !f.current).count(),
        }),
    }
}
