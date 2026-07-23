//! Logical-database views assembled from existing ShardLite endpoints.
//!
//! Shard identifiers are deliberately kept inside this module. Browser-facing responses describe
//! one database, while operator-only placement diagnostics continue to use the raw endpoints.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{json, Value};

use crate::proxy;
use crate::registry::{Registry, Resolved};

const OBJECT_SQL: &str = "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaObject {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub table: String,
    pub sql: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SchemaCatalog {
    pub objects: Vec<SchemaObject>,
    pub tables: Vec<Value>,
    pub schema_version: Option<i64>,
    pub consistency: Value,
}

pub fn schema_catalog(registry: &Registry, connection: &str) -> Result<SchemaCatalog, String> {
    let resolved = registry
        .resolve(connection)
        .map_err(|error| error.to_string())?;
    let (info, _) = proxy::fetch_json_result(&resolved, "info")?;
    let count = info
        .get("shard_count")
        .and_then(Value::as_u64)
        .filter(|count| *count > 0 && *count <= 4096)
        .ok_or_else(|| "database returned invalid storage metadata".to_string())?
        as u32;

    let mut snapshots = Vec::new();
    let mut versions = Vec::new();
    let mut failures = 0usize;
    let mut reference_unit = None;
    for unit in 0..count {
        let snapshot = proxy::query_rows(&resolved, unit, OBJECT_SQL, &[])
            .and_then(|(_, rows)| rows.into_iter().map(schema_object).collect());
        let version = proxy::fetch_json_result(&resolved, &format!("schema/{unit}"))
            .ok()
            .and_then(|(value, _)| value.get("schema_version").and_then(Value::as_i64));
        match snapshot {
            Ok(objects) => {
                reference_unit.get_or_insert(unit);
                snapshots.push(objects);
                if let Some(version) = version {
                    versions.push(version);
                }
            }
            Err(_) => failures += 1,
        }
    }
    if snapshots.is_empty() {
        return Err("The database schema could not be read from any available node.".into());
    }
    let objects = logical_objects(&snapshots);
    let (status, summary) = catalog_consistency(&snapshots, failures);
    let schema_version = versions.first().copied().filter(|first| {
        failures == 0
            && versions.len() == count as usize
            && versions.iter().all(|value| value == first)
    });
    let tables = table_details(
        &resolved,
        reference_unit.expect("a successful snapshot has a unit"),
        &objects,
    )?;
    Ok(SchemaCatalog {
        objects,
        tables,
        schema_version,
        consistency: json!({
            "status": status,
            "coverage": if failures == 0 { "complete" } else { "partial" },
            "summary": summary,
        }),
    })
}

fn table_details(
    resolved: &Resolved,
    unit: u32,
    objects: &[SchemaObject],
) -> Result<Vec<Value>, String> {
    let mut output = Vec::new();
    for table in objects.iter().filter(|object| object.kind == "table") {
        let parameter = Value::String(table.name.clone());
        let (_, columns) = proxy::query_rows(
            resolved,
            unit,
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid",
            std::slice::from_ref(&parameter),
        )?;
        let (_, indexes) = proxy::query_rows(
            resolved,
            unit,
            "SELECT seq, name, \"unique\", origin, partial FROM pragma_index_list(?) ORDER BY seq",
            std::slice::from_ref(&parameter),
        )?;
        let (_, foreign_keys) = proxy::query_rows(
            resolved,
            unit,
            "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match FROM pragma_foreign_key_list(?) ORDER BY id, seq",
            std::slice::from_ref(&parameter),
        )?;
        output.push(json!({
            "name": table.name,
            "sql": table.sql,
            "columns": columns,
            "indexes": indexes,
            "foreign_keys": foreign_keys,
        }));
    }
    Ok(output)
}

fn schema_object(row: Vec<Value>) -> Result<SchemaObject, String> {
    if row.len() < 4 {
        return Err("database returned an incomplete schema record".into());
    }
    Ok(SchemaObject {
        kind: row[0].as_str().unwrap_or_default().to_string(),
        name: row[1].as_str().unwrap_or_default().to_string(),
        table: row[2].as_str().unwrap_or_default().to_string(),
        sql: row[3].as_str().map(str::to_string),
    })
}

fn catalog_consistency(
    snapshots: &[Vec<SchemaObject>],
    failures: usize,
) -> (&'static str, &'static str) {
    let drifted = snapshots
        .first()
        .is_some_and(|first| snapshots.iter().skip(1).any(|snapshot| snapshot != first));
    if drifted {
        ("drifted", "Definitions differ within the database. Review recent schema operations before making another change.")
    } else if failures > 0 {
        (
            "unknown",
            "Part of the database could not be checked. The visible catalog may be incomplete.",
        )
    } else {
        (
            "consistent",
            "The same schema definition is available throughout the database.",
        )
    }
}

fn logical_objects(snapshots: &[Vec<SchemaObject>]) -> Vec<SchemaObject> {
    let mut objects = BTreeMap::new();
    for object in snapshots.iter().flatten() {
        objects
            .entry((object.kind.clone(), object.name.clone()))
            .or_insert_with(|| object.clone());
    }
    objects.into_values().collect()
}

pub fn verify_node(registry: &Registry, connection: &str, endpoint: &str) -> Result<Value, String> {
    let current = registry
        .resolve(connection)
        .map_err(|error| error.to_string())?;
    let candidate = registry
        .resolve_candidate(connection, endpoint)
        .map_err(|error| error.to_string())?;
    let (current_meta, _) = fetch_compatible(&current, "meta", "info")?;
    let (current_topology, _) = fetch_compatible(&current, "topology", "cluster")?;
    let (candidate_meta, latency_ms) = fetch_compatible(&candidate, "meta", "info")?;
    let (candidate_topology, _) = fetch_compatible(&candidate, "topology", "cluster")?;
    let health = proxy::fetch_json_result(&candidate, "health")
        .ok()
        .map(|(value, _)| value);
    Ok(verify_contracts(
        &candidate.url,
        latency_ms,
        &current_meta,
        &current_topology,
        &candidate_meta,
        &candidate_topology,
        health.as_ref(),
    ))
}

fn fetch_compatible(
    resolved: &Resolved,
    preferred: &str,
    legacy: &str,
) -> Result<(Value, u128), String> {
    proxy::fetch_json_result(resolved, preferred)
        .or_else(|_| proxy::fetch_json_result(resolved, legacy))
}

fn verify_contracts(
    endpoint: &str,
    latency_ms: u128,
    current_meta: &Value,
    current_topology: &Value,
    candidate_meta: &Value,
    candidate_topology: &Value,
    health: Option<&Value>,
) -> Value {
    let current_api = current_meta.get("api_version").and_then(Value::as_u64);
    let candidate_api = candidate_meta.get("api_version").and_then(Value::as_u64);
    let current_version = current_meta.get("version").and_then(Value::as_str);
    let candidate_version = candidate_meta.get("version").and_then(Value::as_str);
    let compatible = current_api
        .zip(candidate_api)
        .is_none_or(|(left, right)| left == right)
        && current_version
            .zip(candidate_version)
            .is_none_or(|(left, right)| compatibility_line(left) == compatibility_line(right));
    let current_members = member_ids(current_topology);
    let candidate_members = member_ids(candidate_topology);
    let candidate_id =
        value_string(candidate_meta, "node").or_else(|| value_string(candidate_topology, "node"));
    let membership_overlap = !current_members.is_disjoint(&candidate_members);
    let member = candidate_id
        .as_ref()
        .is_some_and(|id| current_members.contains(id))
        || membership_overlap;
    let clustered = candidate_topology
        .get("clustered")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let current_reports_members = current_topology
        .get("members")
        .and_then(Value::as_array)
        .is_some_and(|members| !members.is_empty());
    let candidate_reports_members = candidate_topology
        .get("members")
        .and_then(Value::as_array)
        .is_some_and(|members| !members.is_empty());
    let wrong_database =
        clustered && current_reports_members && candidate_reports_members && !membership_overlap;
    let healthy = health
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .is_none_or(|status| matches!(status, "healthy" | "ok"));
    let distribution_stable = member
        && healthy
        && (!clustered
            || candidate_topology
                .get("leader")
                .is_some_and(|value| !value.is_null()));
    let status = if !compatible {
        "incompatible"
    } else if wrong_database {
        "wrong_database"
    } else if !member {
        "not_member"
    } else if !healthy {
        "unhealthy"
    } else if distribution_stable {
        "ready"
    } else {
        "stabilizing"
    };
    let guidance = match status {
        "incompatible" => vec!["Use a node version and API contract compatible with this database."],
        "wrong_database" => vec!["This endpoint reports membership in a different ShardLite database. Check the deployment configuration and endpoint."],
        "not_member" => vec!["Join the node using the existing ShardLite deployment process.", "Return here and verify the endpoint again."],
        "unhealthy" => vec!["Resolve the node health checks before adding it as a failover endpoint."],
        "stabilizing" => vec!["Wait for leadership and data distribution to stabilize, then verify again."],
        _ => vec!["The endpoint can be added to this connection as a failover path."],
    };
    json!({
        "endpoint": endpoint,
        "status": status,
        "reachable": true,
        "latency_ms": latency_ms,
        "node": candidate_id,
        "version": candidate_version,
        "api_version": candidate_api,
        "compatible": compatible,
        "member": member,
        "health": health.and_then(|value| value.get("status")).and_then(Value::as_str).unwrap_or("unknown"),
        "distribution_stable": distribution_stable,
        "guidance": guidance,
    })
}

fn compatibility_line(version: &str) -> String {
    let mut parts = version.split('.');
    let major = parts.next().unwrap_or(version);
    if major == "0" {
        format!("0.{}", parts.next().unwrap_or("0"))
    } else {
        major.to_string()
    }
}

fn member_ids(topology: &Value) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    if let Some(node) = value_string(topology, "node") {
        ids.insert(node);
    }
    if let Some(members) = topology.get("members").and_then(Value::as_array) {
        for member in members {
            if let Some(node) = value_string(member, "node") {
                ids.insert(node);
            }
        }
    }
    ids
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    match value.get(key)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(sql: &str) -> SchemaObject {
        SchemaObject {
            kind: "table".into(),
            name: "accounts".into(),
            table: "accounts".into(),
            sql: Some(sql.into()),
        }
    }

    #[test]
    fn catalog_reports_consistent_drifted_and_unknown_without_ids() {
        assert_eq!(
            catalog_consistency(
                &[
                    vec![object("CREATE TABLE accounts(id)")],
                    vec![object("CREATE TABLE accounts(id)")]
                ],
                0
            )
            .0,
            "consistent"
        );
        assert_eq!(
            catalog_consistency(
                &[
                    vec![object("CREATE TABLE accounts(id)")],
                    vec![object("CREATE TABLE accounts(id, name)")]
                ],
                0
            )
            .0,
            "drifted"
        );
        assert_eq!(
            catalog_consistency(&[vec![object("CREATE TABLE accounts(id)")]], 1).0,
            "unknown"
        );
    }

    #[test]
    fn logical_catalog_keeps_objects_seen_anywhere_without_duplicates() {
        let accounts = object("CREATE TABLE accounts(id)");
        let mut events = object("CREATE TABLE events(id)");
        events.name = "events".into();
        events.table = "events".into();
        let catalog = logical_objects(&[vec![accounts.clone()], vec![accounts, events]]);
        assert_eq!(
            catalog
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["accounts", "events"]
        );
    }

    #[test]
    fn node_contract_requires_compatible_visible_membership() {
        let current_meta = json!({"api_version": 1, "version": "0.2.0", "node": "a"});
        let current_topology = json!({"clustered": true, "node": "a", "leader": "a", "members": [{"node": "a"}, {"node": "b"}]});
        let candidate_meta = json!({"api_version": 1, "version": "0.2.3", "node": "b"});
        let candidate_topology = json!({"clustered": true, "node": "b", "leader": "a", "members": [{"node": "a"}, {"node": "b"}]});
        let result = verify_contracts(
            "https://b",
            4,
            &current_meta,
            &current_topology,
            &candidate_meta,
            &candidate_topology,
            Some(&json!({"status": "healthy"})),
        );
        assert_eq!(result["status"], "ready");
        assert_eq!(result["member"], true);

        let foreign =
            json!({"clustered": true, "node": "z", "leader": "z", "members": [{"node": "z"}]});
        let result = verify_contracts(
            "https://z",
            4,
            &current_meta,
            &current_topology,
            &json!({"api_version": 1, "version": "0.2.0", "node": "z"}),
            &foreign,
            None,
        );
        assert_eq!(result["status"], "wrong_database");

        let incompatible = verify_contracts(
            "https://b",
            4,
            &current_meta,
            &current_topology,
            &json!({"api_version": 1, "version": "0.3.0", "node": "b"}),
            &candidate_topology,
            Some(&json!({"status": "healthy"})),
        );
        assert_eq!(incompatible["status"], "incompatible");
    }
}
