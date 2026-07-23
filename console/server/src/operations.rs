//! Durable console-side coordination for operations ShardLite already exposes.
//!
//! This module deliberately does not touch ShardLite files or invent new server capabilities. A
//! schema rollout is a persisted wrapper around existing read-only preflight endpoints and
//! `POST /v1/execute_all`. One worker serializes operations, idempotency prevents duplicate
//! submission, and a process restart marks in-flight work interrupted rather than replaying DDL.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::audit::Audit;
use crate::registry::Registry;

const MAX_OPERATIONS: usize = 1000;
const MAX_SQL_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Running,
    Succeeded,
    Partial,
    Failed,
    Cancelled,
    Interrupted,
}

impl Status {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Partial | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardVersion {
    pub shard: u32,
    pub schema_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOutcome {
    pub shard: u32,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preflight {
    pub connection: String,
    pub sql_fingerprint: String,
    pub token: String,
    pub versions: Vec<ShardVersion>,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub kind: String,
    pub actor: String,
    pub connection: String,
    pub status: Status,
    pub stage: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub idempotency_key: String,
    pub sql: String,
    pub sql_fingerprint: String,
    pub preflight_token: String,
    pub expected_versions: Vec<ShardVersion>,
    #[serde(default)]
    pub observed_versions: Vec<ShardVersion>,
    #[serde(default)]
    pub outcomes: Vec<ShardOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub cancel_requested: bool,
}

#[derive(Debug)]
pub enum OperationError {
    NotFound,
    Conflict(String),
    Invalid(String),
    Io(String),
}

impl std::fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(formatter, "no such operation"),
            Self::Conflict(message) | Self::Invalid(message) | Self::Io(message) => {
                write!(formatter, "{message}")
            }
        }
    }
}

#[derive(Clone)]
pub struct Operations {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    records: Mutex<Vec<Operation>>,
    ready: Condvar,
}

struct ExecutionFailure {
    message: String,
    ambiguous: bool,
}

impl ExecutionFailure {
    fn safe(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ambiguous: false,
        }
    }

    fn ambiguous(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ambiguous: true,
        }
    }
}

impl Operations {
    pub fn open(path: &Path) -> Result<Self, String> {
        let mut records: Vec<Operation> = if path.exists() {
            let bytes = std::fs::read(path).map_err(|error| {
                format!("reading operation journal {}: {error}", path.display())
            })?;
            serde_json::from_slice(&bytes)
                .map_err(|error| format!("parsing operation journal {}: {error}", path.display()))?
        } else {
            Vec::new()
        };
        let now = unix_millis();
        let mut changed = false;
        for operation in &mut records {
            if operation.status == Status::Running {
                operation.status = Status::Interrupted;
                operation.stage = "manual_verification_required".into();
                operation.updated_at_ms = now;
                operation.error = Some(
                    "the console restarted while this operation was running; it was not replayed. Verify the database schema before deciding how to roll forward"
                        .into(),
                );
                changed = true;
            }
        }
        let operations = Self {
            inner: Arc::new(Inner {
                path: path.to_path_buf(),
                records: Mutex::new(records),
                ready: Condvar::new(),
            }),
        };
        if changed {
            let records = operations.inner.records.lock().unwrap();
            operations.persist(&records)?;
        }
        Ok(operations)
    }

    pub fn preflight(
        registry: &Registry,
        connection: &str,
        sql: &str,
    ) -> Result<Preflight, String> {
        validate_sql(sql).map_err(|error| error.to_string())?;
        let resolved = registry
            .resolve(connection)
            .map_err(|error| error.to_string())?;
        let (info, _) = crate::proxy::fetch_json_result(&resolved, "info")?;
        let count = info
            .get("shard_count")
            .and_then(Value::as_u64)
            .filter(|count| *count > 0 && *count <= 4096)
            .ok_or_else(|| "cluster returned an invalid shard_count".to_string())?;
        let mut versions = Vec::with_capacity(count as usize);
        for shard in 0..count as u32 {
            let (schema, _) =
                crate::proxy::fetch_json_result(&resolved, &format!("schema/{shard}"))?;
            let schema_version = schema
                .get("schema_version")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("shard {shard} returned no schema_version"))?;
            versions.push(ShardVersion {
                shard,
                schema_version,
            });
        }
        let sql_fingerprint = fingerprint(sql.as_bytes());
        Ok(Preflight {
            connection: connection.to_string(),
            token: preflight_token(connection, &sql_fingerprint, &versions),
            sql_fingerprint,
            versions,
            observed_at_ms: unix_millis(),
        })
    }

    pub fn submit(
        &self,
        actor: &str,
        connection: &str,
        sql: &str,
        idempotency_key: &str,
        preflight_token_value: &str,
        expected_versions: Vec<ShardVersion>,
    ) -> Result<Operation, OperationError> {
        validate_sql(sql)?;
        if !(8..=128).contains(&idempotency_key.len())
            || !idempotency_key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(OperationError::Invalid(
                "idempotency_key must be 8-128 letters, numbers, '-' or '_'".into(),
            ));
        }
        if expected_versions.is_empty() {
            return Err(OperationError::Invalid(
                "a completed schema preflight is required".into(),
            ));
        }
        let sql_fingerprint = fingerprint(sql.as_bytes());
        let expected_token = preflight_token(connection, &sql_fingerprint, &expected_versions);
        if expected_token != preflight_token_value {
            return Err(OperationError::Invalid(
                "preflight token does not match this connection, SQL, and version set".into(),
            ));
        }

        let mut records = self.inner.records.lock().unwrap();
        if let Some(existing) = records.iter().find(|operation| {
            operation.actor == actor
                && operation.connection == connection
                && operation.idempotency_key == idempotency_key
        }) {
            if existing.sql_fingerprint == sql_fingerprint
                && existing.preflight_token == preflight_token_value
            {
                return Ok(existing.clone());
            }
            return Err(OperationError::Conflict(
                "that idempotency key was already used for a different operation".into(),
            ));
        }
        while records.len() >= MAX_OPERATIONS {
            let Some(index) = records
                .iter()
                .position(|operation| operation.status.terminal())
            else {
                return Err(OperationError::Conflict(
                    "the operation journal is full of active work".into(),
                ));
            };
            records.remove(index);
        }
        let now = unix_millis();
        let operation = Operation {
            id: format!("op-{now:016x}-{:016x}", rand::random::<u64>()),
            kind: "schema_rollout".into(),
            actor: actor.to_string(),
            connection: connection.to_string(),
            status: Status::Queued,
            stage: "queued".into(),
            created_at_ms: now,
            updated_at_ms: now,
            idempotency_key: idempotency_key.to_string(),
            sql: sql.to_string(),
            sql_fingerprint,
            preflight_token: preflight_token_value.to_string(),
            expected_versions,
            observed_versions: Vec::new(),
            outcomes: Vec::new(),
            error: None,
            cancel_requested: false,
        };
        records.push(operation.clone());
        self.persist(&records).map_err(OperationError::Io)?;
        self.inner.ready.notify_one();
        Ok(operation)
    }

    pub fn list(&self, connection: Option<&str>, actor: Option<&str>) -> Vec<Operation> {
        let records = self.inner.records.lock().unwrap();
        let mut output: Vec<_> = records
            .iter()
            .filter(|operation| connection.is_none_or(|value| operation.connection == value))
            .filter(|operation| actor.is_none_or(|value| operation.actor == value))
            .cloned()
            .collect();
        output.sort_by_key(|operation| std::cmp::Reverse(operation.created_at_ms));
        output
    }

    pub fn get(&self, id: &str) -> Option<Operation> {
        self.inner
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|operation| operation.id == id)
            .cloned()
    }

    pub fn cancel(&self, id: &str) -> Result<Operation, OperationError> {
        let mut records = self.inner.records.lock().unwrap();
        let operation = records
            .iter_mut()
            .find(|operation| operation.id == id)
            .ok_or(OperationError::NotFound)?;
        match operation.status {
            Status::Queued => {
                operation.status = Status::Cancelled;
                operation.stage = "cancelled_before_execution".into();
                operation.updated_at_ms = unix_millis();
            }
            Status::Running if operation.stage == "revalidating_preflight" => {
                operation.cancel_requested = true;
                operation.updated_at_ms = unix_millis();
            }
            Status::Running => {
                return Err(OperationError::Conflict(
                    "the rollout has reached ShardLite and can no longer be cancelled safely".into(),
                ));
            }
            _ => {
                return Err(OperationError::Conflict(
                    "a completed operation cannot be cancelled".into(),
                ));
            }
        }
        let output = operation.clone();
        self.persist(&records).map_err(OperationError::Io)?;
        Ok(output)
    }

    pub fn spawn(&self, registry: Arc<Registry>, audit: Audit) {
        let operations = self.clone();
        std::thread::Builder::new()
            .name("console-operations".into())
            .spawn(move || operations.run(registry, audit))
            .expect("spawning console operation worker");
        self.inner.ready.notify_one();
    }

    fn run(&self, registry: Arc<Registry>, audit: Audit) {
        loop {
            let Some(operation) = self.take_next() else {
                let records = self.inner.records.lock().unwrap();
                drop(
                    self.inner
                        .ready
                        .wait_timeout(records, std::time::Duration::from_secs(1)),
                );
                continue;
            };
            let result = self.execute(&registry, &operation);
            let (status, outcomes, observed_versions, error) = match result {
                Ok(Some((outcomes, versions))) => {
                    let succeeded = outcomes.iter().filter(|outcome| outcome.ok).count();
                    let status = if succeeded == outcomes.len() {
                        Status::Succeeded
                    } else if succeeded == 0 {
                        Status::Failed
                    } else {
                        Status::Partial
                    };
                    let error = (status != Status::Succeeded).then(|| {
                        format!(
                            "{succeeded}/{} internal steps applied the statement; this operation was not rolled back",
                            outcomes.len()
                        )
                    });
                    (status, outcomes, versions, error)
                }
                Ok(None) => (Status::Cancelled, Vec::new(), Vec::new(), None),
                Err(error) => (
                    if error.ambiguous {
                        Status::Interrupted
                    } else {
                        Status::Failed
                    },
                    Vec::new(),
                    Vec::new(),
                    Some(error.message),
                ),
            };
            let final_operation =
                self.finish(&operation.id, status, outcomes, observed_versions, error);
            if let Some(final_operation) = final_operation {
                audit.record(
                    Some(&final_operation.actor),
                    "operation.schema_rollout.complete",
                    &format!("{}/{}", final_operation.connection, final_operation.id),
                    match final_operation.status {
                        Status::Succeeded => "ok",
                        Status::Partial => "partial",
                        Status::Cancelled => "cancelled",
                        Status::Interrupted => "interrupted",
                        _ => "failed",
                    },
                );
            }
        }
    }

    fn take_next(&self) -> Option<Operation> {
        let mut records = self.inner.records.lock().unwrap();
        let operation = records
            .iter_mut()
            .find(|operation| operation.status == Status::Queued)?;
        operation.status = Status::Running;
        operation.stage = "revalidating_preflight".into();
        operation.updated_at_ms = unix_millis();
        let output = operation.clone();
        if self.persist(&records).is_err() {
            return None;
        }
        Some(output)
    }

    fn execute(
        &self,
        registry: &Registry,
        operation: &Operation,
    ) -> Result<Option<(Vec<ShardOutcome>, Vec<ShardVersion>)>, ExecutionFailure> {
        let preflight = Self::preflight(registry, &operation.connection, &operation.sql)
            .map_err(ExecutionFailure::safe)?;
        if preflight.token != operation.preflight_token {
            return Err(ExecutionFailure::safe(
                "schema changed after approval; the rollout was not sent to ShardLite. Run a new preflight and review the new versions",
            ));
        }
        let resolved = registry
            .resolve(&operation.connection)
            .map_err(|error| ExecutionFailure::safe(error.to_string()))?;
        if !self
            .begin_execute(&operation.id, &preflight.versions)
            .map_err(ExecutionFailure::safe)?
        {
            return Ok(None);
        }
        let response = crate::proxy::post_json_result(
            &resolved,
            "execute_all",
            &json!({ "sql": operation.sql }),
        )
        .map_err(|error| {
            ExecutionFailure::ambiguous(format!(
                "the database-wide request was sent but its result is unknown: {error}. Verify the database schema before rolling forward"
            ))
        })?;
        let outcomes: Vec<ShardOutcome> =
            serde_json::from_value(response.get("shards").cloned().ok_or_else(|| {
                ExecutionFailure::ambiguous(
                    "ShardLite returned no database-wide rollout outcomes; verify the database schema",
                )
            })?)
            .map_err(|error| {
                ExecutionFailure::ambiguous(format!(
                    "invalid database-wide rollout response: {error}; verify the database schema"
                ))
            })?;
        if outcomes.is_empty() {
            return Err(ExecutionFailure::ambiguous(
                "ShardLite returned an empty database-wide rollout result; verify the database schema",
            ));
        }
        Ok(Some((outcomes, preflight.versions)))
    }

    /// Move from cancellable revalidation to the point of no return immediately before the
    /// existing ShardLite endpoint is called.
    fn begin_execute(&self, id: &str, versions: &[ShardVersion]) -> Result<bool, String> {
        let mut records = self.inner.records.lock().unwrap();
        let operation = records
            .iter_mut()
            .find(|operation| operation.id == id)
            .ok_or_else(|| "operation disappeared from its durable journal".to_string())?;
        if operation.cancel_requested {
            operation.status = Status::Cancelled;
            operation.stage = "cancelled_before_execution".into();
            operation.updated_at_ms = unix_millis();
            operation.observed_versions = versions.to_vec();
            self.persist(&records)?;
            return Ok(false);
        }
        operation.stage = "executing_on_shardlite".into();
        operation.updated_at_ms = unix_millis();
        operation.observed_versions = versions.to_vec();
        self.persist(&records)?;
        Ok(true)
    }

    fn finish(
        &self,
        id: &str,
        status: Status,
        outcomes: Vec<ShardOutcome>,
        observed_versions: Vec<ShardVersion>,
        error: Option<String>,
    ) -> Option<Operation> {
        let mut records = self.inner.records.lock().unwrap();
        let operation = records.iter_mut().find(|operation| operation.id == id)?;
        operation.status = status;
        operation.stage = match status {
            Status::Succeeded => "complete",
            Status::Partial => "partial_requires_roll_forward",
            Status::Cancelled => "cancelled_before_execution",
            Status::Failed => "failed",
            Status::Interrupted => "manual_verification_required",
            _ => "complete",
        }
        .into();
        operation.updated_at_ms = unix_millis();
        operation.outcomes = outcomes;
        operation.observed_versions = observed_versions;
        operation.error = error;
        let output = operation.clone();
        let _ = self.persist(&records);
        Some(output)
    }

    fn persist(&self, records: &[Operation]) -> Result<(), String> {
        let json = serde_json::to_vec_pretty(records).map_err(|error| error.to_string())?;
        crate::store::write_atomic(&self.inner.path, &json)
    }
}

fn validate_sql(sql: &str) -> Result<(), OperationError> {
    if sql.trim().is_empty() {
        return Err(OperationError::Invalid("SQL is required".into()));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(OperationError::Invalid(
            "SQL exceeds the 1 MiB operation limit".into(),
        ));
    }
    Ok(())
}

fn fingerprint(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn preflight_token(connection: &str, sql_fingerprint: &str, versions: &[ShardVersion]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(connection.as_bytes());
    hasher.update(&[0]);
    hasher.update(sql_fingerprint.as_bytes());
    for version in versions {
        hasher.update(&version.shard.to_le_bytes());
        hasher.update(&version.schema_version.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "shardlite-console-operations-{}.json",
            rand::random::<u64>()
        ))
    }

    fn preflight() -> (Vec<ShardVersion>, String) {
        let versions = vec![ShardVersion {
            shard: 0,
            schema_version: 7,
        }];
        let sql_fingerprint = fingerprint(b"CREATE TABLE t(id)");
        let token = preflight_token("prod", &sql_fingerprint, &versions);
        (versions, token)
    }

    #[test]
    fn submission_is_durable_and_idempotent() {
        let path = path();
        let operations = Operations::open(&path).unwrap();
        let (versions, token) = preflight();
        let first = operations
            .submit(
                "alice",
                "prod",
                "CREATE TABLE t(id)",
                "request-1234",
                &token,
                versions.clone(),
            )
            .unwrap();
        let duplicate = operations
            .submit(
                "alice",
                "prod",
                "CREATE TABLE t(id)",
                "request-1234",
                &token,
                versions,
            )
            .unwrap();
        assert_eq!(first.id, duplicate.id);
        assert_eq!(Operations::open(&path).unwrap().list(None, None).len(), 1);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn an_idempotency_key_cannot_be_reused_for_different_sql() {
        let path = path();
        let operations = Operations::open(&path).unwrap();
        let (versions, token) = preflight();
        operations
            .submit(
                "alice",
                "prod",
                "CREATE TABLE t(id)",
                "request-1234",
                &token,
                versions,
            )
            .unwrap();
        let other_versions = vec![ShardVersion {
            shard: 0,
            schema_version: 7,
        }];
        let other_fingerprint = fingerprint(b"CREATE TABLE u(id)");
        let other_token = preflight_token("prod", &other_fingerprint, &other_versions);
        assert!(matches!(
            operations.submit(
                "alice",
                "prod",
                "CREATE TABLE u(id)",
                "request-1234",
                &other_token,
                other_versions,
            ),
            Err(OperationError::Conflict(_))
        ));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn queued_work_can_be_cancelled_and_is_not_selected() {
        let path = path();
        let operations = Operations::open(&path).unwrap();
        let (versions, token) = preflight();
        let operation = operations
            .submit(
                "alice",
                "prod",
                "CREATE TABLE t(id)",
                "request-1234",
                &token,
                versions,
            )
            .unwrap();
        assert_eq!(
            operations.cancel(&operation.id).unwrap().status,
            Status::Cancelled
        );
        assert!(operations.take_next().is_none());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn restart_interrupts_running_work_instead_of_replaying_it() {
        let path = path();
        let operations = Operations::open(&path).unwrap();
        let (versions, token) = preflight();
        operations
            .submit(
                "alice",
                "prod",
                "CREATE TABLE t(id)",
                "request-1234",
                &token,
                versions,
            )
            .unwrap();
        let running = operations.take_next().unwrap();
        assert_eq!(running.status, Status::Running);
        drop(operations);
        let reopened = Operations::open(&path).unwrap();
        assert_eq!(reopened.list(None, None)[0].status, Status::Interrupted);
        assert!(reopened.take_next().is_none());
        std::fs::remove_file(path).ok();
    }
}
