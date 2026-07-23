//! The console HTTP API and the policy boundary in front of stored meshdb credentials.
//!
//! Authentication, CSRF, body limits, console permissions, and the proxy route/method matrix are
//! enforced here before a request can reach a managed cluster.

use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use tiny_http::Request;

use crate::audit::Audit;
use crate::auth::{self, LoginLimiter, Session, Sessions};
use crate::metrics::Metrics;
use crate::operations::{Operation, OperationError, Operations, ShardVersion};
use crate::registry::{Registry, RegistryError};
use crate::respond;
use crate::users::{Permission, Role, UserError, Users};

const MAX_BODY_BYTES: usize = 1024 * 1024;

pub struct AppState {
    pub users: Users,
    pub registry: Arc<Registry>,
    pub sessions: Sessions,
    pub metrics: Arc<Metrics>,
    pub operations: Operations,
    pub audit: Audit,
    pub login_limiter: LoginLimiter,
    pub secure_cookie: bool,
    pub streams: StreamSlots,
}

pub struct StreamSlots {
    active: AtomicUsize,
    max: usize,
}

impl StreamSlots {
    pub fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max: max.max(1),
        }
    }

    fn acquire(&self) -> Option<StreamGuard<'_>> {
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            if active >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(StreamGuard { slots: self }),
                Err(now) => active = now,
            }
        }
    }
}

struct StreamGuard<'a> {
    slots: &'a StreamSlots,
}

impl Drop for StreamGuard<'_> {
    fn drop(&mut self) {
        self.slots.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_body(request: &mut Request) -> Result<Vec<u8>, &'static str> {
    if request
        .body_length()
        .is_some_and(|length| length > MAX_BODY_BYTES)
    {
        return Err("request body exceeds the 1 MiB console limit");
    }
    let mut buf = Vec::new();
    request
        .as_reader()
        .take((MAX_BODY_BYTES + 1) as u64)
        .read_to_end(&mut buf)
        .map_err(|_| "could not read request body")?;
    if buf.len() > MAX_BODY_BYTES {
        return Err("request body exceeds the 1 MiB console limit");
    }
    Ok(buf)
}

fn json_body(request: &mut Request) -> Result<Value, &'static str> {
    let body = read_body(request)?;
    serde_json::from_slice(&body).map_err(|_| "invalid JSON body")
}

fn header<'a>(request: &'a Request, name: &'static str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv(name))
        .map(|h| h.value.as_str())
}

fn cookie_token(request: &Request) -> Option<String> {
    header(request, "Cookie").and_then(auth::token_from_cookie_header)
}

fn csrf_valid(request: &Request, session: &Session) -> bool {
    header(request, "X-CSRF-Token").is_some_and(|token| token == session.csrf)
}

fn login_identity(request: &Request, username: &str) -> String {
    let remote = request
        .remote_addr()
        .map(|address| address.ip().to_string())
        .unwrap_or_else(|| "unknown".into());
    format!("{username}@{remote}")
}

fn require(
    request: Request,
    state: &AppState,
    session: &Session,
    permission: Permission,
    action: &str,
    target: &str,
) -> std::io::Result<()> {
    state
        .audit
        .record(Some(&session.user), action, target, "denied");
    let message = match permission {
        Permission::ManageConnections
        | Permission::ManageConsoleUsers
        | Permission::ManageMeshUsers
        | Permission::ReadAudit => "admin permission required",
        Permission::Write => "developer or admin permission required",
        Permission::Operate => "operator or admin permission required",
        Permission::Observe | Permission::Query => "permission denied",
    };
    respond::error(request, 403, message)
}

fn parse_role(value: &Value) -> Option<Role> {
    value
        .get("role")
        .and_then(|role| serde_json::from_value(role.clone()).ok())
}

pub fn handle(mut request: Request, state: &AppState) -> std::io::Result<()> {
    let method = request.method().to_string();
    let raw_url = request.url().to_string();
    let path = raw_url.split('?').next().unwrap_or("").to_string();

    if method == "GET" && matches!(path.as_str(), "/healthz" | "/readyz") {
        return respond::respond_json(
            request,
            200,
            &json!({ "ok": true, "service": "meshdb-console" }),
        );
    }

    let segments: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();

    if segments.first().map(String::as_str) != Some("api") {
        return crate::assets::serve(request, &path);
    }

    let token = cookie_token(&request);
    let session = token
        .as_deref()
        .and_then(|token| state.sessions.lookup(token));
    let tail: Vec<&str> = segments.iter().skip(1).map(String::as_str).collect();

    match (method.as_str(), tail.as_slice()) {
        ("POST", ["login"]) => {
            let v = match json_body(&mut request) {
                Ok(v) => v,
                Err(message) => return respond::error(request, 400, message),
            };
            let username = v.get("username").and_then(Value::as_str).unwrap_or("");
            let password = v.get("password").and_then(Value::as_str).unwrap_or("");
            let identity = login_identity(&request, username);
            if let Some(retry_after) = state.login_limiter.retry_after(&identity) {
                state
                    .audit
                    .record(Some(username), "login", &identity, "throttled");
                return respond::error(
                    request,
                    429,
                    &format!("too many login attempts; retry in {retry_after} seconds"),
                );
            }
            return match state.users.verify(username, password) {
                Some(role) => {
                    state.login_limiter.succeeded(&identity);
                    if let Some(old) = token {
                        state.sessions.remove(&old);
                    }
                    let (token, csrf) = state.sessions.create(username.to_string(), role);
                    state.audit.record(Some(username), "login", &identity, "ok");
                    respond::respond_json_with_cookie(
                        request,
                        200,
                        &json!({ "user": username, "role": role, "csrf_token": csrf }),
                        &auth::set_cookie(&token, state.secure_cookie),
                    )
                }
                None => {
                    state.login_limiter.failed(&identity);
                    state
                        .audit
                        .record(Some(username), "login", &identity, "denied");
                    respond::error(request, 401, "invalid credentials")
                }
            };
        }
        ("POST", ["logout"]) => {
            if let Some(ref session) = session {
                if !csrf_valid(&request, session) {
                    return respond::error(request, 403, "invalid CSRF token");
                }
                state
                    .audit
                    .record(Some(&session.user), "logout", "session", "ok");
            }
            if let Some(token) = token {
                state.sessions.remove(&token);
            }
            return respond::respond_json_with_cookie(
                request,
                200,
                &json!({ "ok": true }),
                &auth::clear_cookie(),
            );
        }
        ("GET", ["me"]) => {
            return match &session {
                Some(session) => respond::respond_json(
                    request,
                    200,
                    &json!({
                        "user": session.user,
                        "role": session.role,
                        "csrf_token": session.csrf,
                    }),
                ),
                None => respond::error(request, 401, "not logged in"),
            };
        }
        _ => {}
    }

    let session = match session {
        Some(session) => session,
        None => return respond::error(request, 401, "not logged in"),
    };

    // A native form POST lets the browser stream a large NDJSON response directly to its
    // download manager. CSRF travels as a form field because navigation/form requests cannot
    // attach the custom header used by fetch().
    if method == "POST" {
        if let ["connections", name, "query-download"] = tail.as_slice() {
            if !session.role.permits(Permission::Query) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Query,
                    "query.download",
                    name,
                );
            }
            let _stream = match state.streams.acquire() {
                Some(slot) => slot,
                None => return respond::error(request, 429, "too many concurrent query streams"),
            };
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(message) => return respond::error(request, 413, message),
            };
            let fields: std::collections::HashMap<String, String> =
                url::form_urlencoded::parse(&body).into_owned().collect();
            if fields.get("csrf").map(String::as_str) != Some(session.csrf.as_str()) {
                return respond::error(request, 403, "invalid CSRF token");
            }
            let mut payload = match fields
                .get("payload")
                .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
            {
                Some(Value::Object(payload)) => payload,
                _ => return respond::error(request, 400, "invalid query download payload"),
            };
            let format = match payload
                .remove("format")
                .and_then(|value| value.as_str().map(str::to_string))
                .as_deref()
            {
                None | Some("ndjson") => crate::proxy::ExportFormat::Ndjson,
                Some("csv") => crate::proxy::ExportFormat::Csv,
                Some(_) => {
                    return respond::error(request, 400, "export format must be ndjson or csv")
                }
            };
            let max_rows = match payload.remove("max_rows") {
                None | Some(Value::Null) => None,
                Some(Value::Number(value)) => match value.as_u64() {
                    Some(value @ 1..=10_000_000) => Some(value as usize),
                    _ => {
                        return respond::error(
                            request,
                            400,
                            "max_rows must be between 1 and 10000000",
                        )
                    }
                },
                Some(_) => {
                    return respond::error(request, 400, "max_rows must be a number or null")
                }
            };
            let payload = match serde_json::to_vec(&Value::Object(payload)) {
                Ok(payload) => payload,
                Err(_) => return respond::error(request, 400, "invalid query download payload"),
            };
            let resolved = match state.registry.resolve(name) {
                Ok(resolved) => resolved,
                Err(RegistryError::NotFound) => {
                    return respond::error(request, 404, "no such connection")
                }
                Err(RegistryError::Disabled) => {
                    return respond::error(request, 409, "this connection is disabled")
                }
                Err(e) => return respond::error(request, 502, &e.to_string()),
            };
            let extension = match format {
                crate::proxy::ExportFormat::Ndjson => "ndjson",
                crate::proxy::ExportFormat::Csv => "csv",
            };
            let filename = format!("meshdb-{name}-query.{extension}");
            crate::proxy::forward_download(
                request, &resolved, "query", payload, &filename, format, max_rows,
            )?;
            return Ok(());
        }
    }

    if method != "GET" && !csrf_valid(&request, &session) {
        state
            .audit
            .record(Some(&session.user), "csrf", &path, "denied");
        return respond::error(request, 403, "invalid CSRF token");
    }

    match (method.as_str(), tail.as_slice()) {
        ("POST", ["logout-all"]) => {
            state.sessions.remove_user(&session.user);
            state
                .audit
                .record(Some(&session.user), "logout_all", "sessions", "ok");
            respond::respond_json_with_cookie(
                request,
                200,
                &json!({ "ok": true }),
                &auth::clear_cookie(),
            )
        }

        ("GET", ["audit"]) => {
            if !session.role.permits(Permission::ReadAudit) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ReadAudit,
                    "audit.list",
                    "audit",
                );
            }
            match state.audit.recent(500) {
                Ok(events) => respond::respond_json(request, 200, &json!(events)),
                Err(e) => respond::error(request, 500, &e),
            }
        }

        ("POST", ["operations", "preflight"]) => {
            if !session.role.permits(Permission::Write) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Write,
                    "operation.preflight",
                    "operations",
                );
            }
            let value = match json_body(&mut request) {
                Ok(value) => value,
                Err(message) => return respond::error(request, 400, message),
            };
            let connection = value
                .get("connection")
                .and_then(Value::as_str)
                .unwrap_or("");
            let sql = value.get("sql").and_then(Value::as_str).unwrap_or("");
            match Operations::preflight(&state.registry, connection, sql) {
                Ok(preflight) => respond::respond_json(request, 200, &json!(preflight)),
                Err(error) => respond::error(request, 409, &error),
            }
        }

        ("POST", ["operations"]) => {
            if !session.role.permits(Permission::Write) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Write,
                    "operation.submit",
                    "operations",
                );
            }
            let value = match json_body(&mut request) {
                Ok(value) => value,
                Err(message) => return respond::error(request, 400, message),
            };
            let connection = value
                .get("connection")
                .and_then(Value::as_str)
                .unwrap_or("");
            let sql = value.get("sql").and_then(Value::as_str).unwrap_or("");
            let idempotency_key = value
                .get("idempotency_key")
                .and_then(Value::as_str)
                .unwrap_or("");
            let preflight_token = value
                .get("preflight_token")
                .and_then(Value::as_str)
                .unwrap_or("");
            let expected_versions: Vec<ShardVersion> = match value
                .get("expected_versions")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
            {
                Ok(Some(versions)) => versions,
                Ok(None) => Vec::new(),
                Err(_) => return respond::error(request, 400, "invalid expected_versions"),
            };
            if let Err(error) = state.registry.resolve(connection) {
                return respond::error(request, 409, &error.to_string());
            }
            let result = state.operations.submit(
                &session.user,
                connection,
                sql,
                idempotency_key,
                preflight_token,
                expected_versions,
            );
            state.audit.record(
                Some(&session.user),
                "operation.schema_rollout.submit",
                connection,
                if result.is_ok() { "queued" } else { "failed" },
            );
            match result {
                Ok(operation) => respond::respond_json(request, 202, &json!(operation)),
                Err(error) => operation_error(request, error),
            }
        }

        ("GET", ["operations"]) => {
            if !session.role.permits(Permission::Write) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Write,
                    "operation.list",
                    "operations",
                );
            }
            let actor = (session.role != Role::Admin).then_some(session.user.as_str());
            respond::respond_json(request, 200, &json!(state.operations.list(None, actor)))
        }

        ("GET", ["operations", id]) => {
            if !session.role.permits(Permission::Write) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Write,
                    "operation.read",
                    id,
                );
            }
            match state.operations.get(id) {
                Some(operation) if operation_visible(&session, &operation) => {
                    respond::respond_json(request, 200, &json!(operation))
                }
                _ => respond::error(request, 404, "no such operation"),
            }
        }

        ("POST", ["operations", id, "cancel"]) => {
            if !session.role.permits(Permission::Write) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Write,
                    "operation.cancel",
                    id,
                );
            }
            let Some(operation) = state.operations.get(id) else {
                return respond::error(request, 404, "no such operation");
            };
            if !operation_visible(&session, &operation) {
                return respond::error(request, 404, "no such operation");
            }
            let result = state.operations.cancel(id);
            state.audit.record(
                Some(&session.user),
                "operation.schema_rollout.cancel",
                &format!("{}/{}", operation.connection, id),
                if result.is_ok() {
                    "accepted"
                } else {
                    "refused"
                },
            );
            match result {
                Ok(operation) => respond::respond_json(request, 200, &json!(operation)),
                Err(error) => operation_error(request, error),
            }
        }

        ("GET", ["console-users"]) => {
            if !session.role.permits(Permission::ManageConsoleUsers) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConsoleUsers,
                    "console_user.list",
                    "console-users",
                );
            }
            let users: Vec<Value> = state
                .users
                .list()
                .into_iter()
                .map(|(name, role)| json!({ "name": name, "role": role }))
                .collect();
            respond::respond_json(request, 200, &json!(users))
        }
        ("POST", ["console-users"]) => {
            if !session.role.permits(Permission::ManageConsoleUsers) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConsoleUsers,
                    "console_user.create",
                    "console-users",
                );
            }
            let v = match json_body(&mut request) {
                Ok(v) => v,
                Err(message) => return respond::error(request, 400, message),
            };
            let name = v.get("username").and_then(Value::as_str).unwrap_or("");
            let password = v.get("password").and_then(Value::as_str).unwrap_or("");
            let role = match parse_role(&v) {
                Some(role) => role,
                None => {
                    return respond::error(
                        request,
                        400,
                        "role must be viewer, developer, operator, or admin",
                    )
                }
            };
            if name.is_empty() || password.is_empty() {
                return respond::error(request, 400, "username and password are required");
            }
            let result = state.users.create(name, password, role);
            state.audit.record(
                Some(&session.user),
                "console_user.create",
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            user_result(request, result)
        }
        ("DELETE", ["console-users", name]) => {
            if !session.role.permits(Permission::ManageConsoleUsers) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConsoleUsers,
                    "console_user.delete",
                    name,
                );
            }
            let result = state.users.delete(name);
            if result.is_ok() {
                state.sessions.remove_user(name);
            }
            state.audit.record(
                Some(&session.user),
                "console_user.delete",
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            user_result(request, result)
        }

        ("GET", ["connections"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "connection.list",
                    "connections",
                );
            }
            respond::respond_json(request, 200, &json!(state.registry.list()))
        }
        ("GET", ["fleet"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "fleet.read",
                    "fleet",
                );
            }
            respond::respond_json(request, 200, &json!(state.metrics.fleet(&state.registry)))
        }
        ("POST", ["connections"]) => {
            if !session.role.permits(Permission::ManageConnections) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConnections,
                    "connection.save",
                    "connections",
                );
            }
            let v = match json_body(&mut request) {
                Ok(v) => v,
                Err(message) => return respond::error(request, 400, message),
            };
            let name = v.get("name").and_then(Value::as_str).unwrap_or("");
            let seeds: Vec<String> = v
                .get("seeds")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_else(|| {
                    v.get("url")
                        .and_then(Value::as_str)
                        .filter(|url| !url.is_empty())
                        .map(|url| vec![url.to_string()])
                        .unwrap_or_default()
                });
            if name.is_empty() || seeds.is_empty() {
                return respond::error(
                    request,
                    400,
                    "name and at least one database endpoint are required",
                );
            }
            let meshdb_user = v
                .get("meshdb_user")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let meshdb_secret = v
                .get("meshdb_secret")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let replace = v.get("replace").and_then(Value::as_bool).unwrap_or(false);
            let enabled = v.get("enabled").and_then(Value::as_bool).unwrap_or(true);
            let timeout_ms = v
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(60_000);
            let allow_insecure_http = v
                .get("allow_insecure_http")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let custom_ca_pem = v
                .get("custom_ca_pem")
                .and_then(Value::as_str)
                .map(str::to_string);
            // S3 replication settings. Present only when a bucket is given; the secret key is
            // sealed (blank on edit preserves the stored one), everything else is plain config.
            let s3_field = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
            let s3_bucket = s3_field("s3_bucket");
            let s3 = if s3_bucket.is_empty() {
                None
            } else {
                Some(crate::registry::S3Settings {
                    bucket: s3_bucket,
                    region: s3_field("s3_region"),
                    endpoint: s3_field("s3_endpoint"),
                    access_key: s3_field("s3_access_key"),
                    prefix: s3_field("s3_prefix"),
                    enabled: v.get("s3_enabled").and_then(Value::as_bool).unwrap_or(false),
                })
            };
            let s3_secret = v
                .get("s3_secret_key")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let result = state.registry.put_config_seeds(
                name,
                seeds,
                meshdb_user,
                meshdb_secret,
                replace,
                enabled,
                timeout_ms,
                allow_insecure_http,
                custom_ca_pem,
                s3,
                s3_secret,
            );
            state.audit.record(
                Some(&session.user),
                if replace {
                    "connection.update"
                } else {
                    "connection.create"
                },
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            registry_result(request, result)
        }
        ("POST", ["connections", name, "test"]) => {
            if !session.role.permits(Permission::ManageConnections) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConnections,
                    "connection.test",
                    name,
                );
            }
            let result = state
                .registry
                .resolve_all_any(name)
                .map_err(|e| e.to_string())
                .and_then(|seeds| {
                    let mut failures = Vec::new();
                    for resolved in seeds {
                        match crate::proxy::test_connection(&resolved) {
                            Ok((latency, info)) => {
                                state.registry.mark_preferred(name, &resolved.url);
                                return Ok((resolved.url, latency, info));
                            }
                            Err(error) => failures.push(format!("{}: {error}", resolved.url)),
                        }
                    }
                    Err(failures.join("; "))
                });
            state.audit.record(
                Some(&session.user),
                "connection.test",
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            match result {
                Ok((seed, latency_ms, info)) => respond::respond_json(
                    request,
                    200,
                    &json!({ "ok": true, "seed": seed, "latency_ms": latency_ms, "info": info }),
                ),
                Err(e) => respond::error(request, 502, &e),
            }
        }
        ("POST", ["connections", name, "verify-node"]) => {
            if !session.role.permits(Permission::ManageConnections) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConnections,
                    "connection.verify_node",
                    name,
                );
            }
            let value = match json_body(&mut request) {
                Ok(value) => value,
                Err(message) => return respond::error(request, 400, message),
            };
            let endpoint = value
                .get("endpoint")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if endpoint.is_empty() {
                return respond::error(request, 400, "a node endpoint is required");
            }
            let result = crate::database::verify_node(&state.registry, name, endpoint);
            state.audit.record(
                Some(&session.user),
                "connection.verify_node",
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            match result {
                Ok(report) => respond::respond_json(request, 200, &report),
                Err(error) => respond::error(request, 502, &error),
            }
        }
        ("DELETE", ["connections", name]) => {
            if !session.role.permits(Permission::ManageConnections) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::ManageConnections,
                    "connection.delete",
                    name,
                );
            }
            let result = state.registry.delete(name);
            if result.is_ok() {
                state.metrics.forget(name);
            }
            state.audit.record(
                Some(&session.user),
                "connection.delete",
                name,
                if result.is_ok() { "ok" } else { "failed" },
            );
            registry_result(request, result)
        }

        ("GET", ["connections", name, "metrics"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "metrics.read",
                    name,
                );
            }
            respond::respond_json(request, 200, &json!(state.metrics.history(name)))
        }
        ("GET", ["connections", name, "observation"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "observation.read",
                    name,
                );
            }
            match state.metrics.observation(name, &state.registry) {
                Some(observation) => respond::respond_json(request, 200, &json!(observation)),
                None => respond::error(request, 404, "no such connection"),
            }
        }
        ("GET", ["connections", name, "schema-catalog"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "schema_catalog.read",
                    name,
                );
            }
            match crate::database::schema_catalog(&state.registry, name) {
                Ok(catalog) => respond::respond_json(request, 200, &json!(catalog)),
                Err(error) => respond::error(request, 502, &error),
            }
        }
        ("GET", ["connections", name, "shard-inventory"]) => {
            if !session.role.permits(Permission::Observe) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Observe,
                    "shard_inventory.read",
                    name,
                );
            }
            match state.metrics.shard_inventory(name, &state.registry) {
                Some(inventory) => respond::respond_json(request, 200, &json!(inventory)),
                None => respond::error(request, 404, "no such connection"),
            }
        }

        // Push this connection's stored (sealed) S3 config to every node of the cluster, so an
        // operator turns replication on without re-entering the secret. S3 config is node-local
        // (each node archives the shards it owns), so it is applied to all seeds, not just one.
        ("POST", ["connections", name, "apply-s3"]) => {
            if !session.role.permits(Permission::Operate) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Operate,
                    "meshdb.s3.apply",
                    name,
                );
            }
            let seeds = match state.registry.resolve_seeds(name) {
                Ok(seeds) => seeds,
                Err(RegistryError::NotFound) => {
                    return respond::error(request, 404, "no such connection")
                }
                Err(RegistryError::Disabled) => {
                    return respond::error(request, 409, "this connection is disabled")
                }
                Err(e) => return respond::error(request, 502, &e.to_string()),
            };
            let first = &seeds[0];
            let Some(s3) = first.s3.clone() else {
                return respond::error(
                    request,
                    400,
                    "no S3 configuration is stored for this connection",
                );
            };
            let body = if !s3.enabled {
                serde_json::json!({ "enabled": false })
            } else {
                let Some(secret) = first.s3_secret_key.clone() else {
                    return respond::error(
                        request,
                        400,
                        "S3 is enabled for this connection but no secret key is stored",
                    );
                };
                let mut b = serde_json::json!({
                    "enabled": true,
                    "bucket": s3.bucket,
                    "region": s3.region,
                    "access_key": s3.access_key,
                    "secret_key": secret,
                    "prefix": s3.prefix,
                });
                if !s3.endpoint.is_empty() {
                    b["endpoint"] = serde_json::json!(s3.endpoint);
                }
                b
            };
            let mut applied = Vec::new();
            let mut failures = Vec::new();
            for resolved in &seeds {
                match crate::proxy::post_json_result(resolved, "s3/config", &body) {
                    Ok(_) => applied.push(resolved.url.clone()),
                    Err(e) => failures.push(format!("{}: {e}", resolved.url)),
                }
            }
            state.audit.record(
                Some(&session.user),
                "meshdb.s3.apply",
                name,
                if failures.is_empty() { "ok" } else { "failed" },
            );
            let status = if failures.is_empty() { 200 } else { 502 };
            respond::respond_json(
                request,
                status,
                &serde_json::json!({ "applied": applied, "failures": failures }),
            )
        }

        // Declare a table's shard key on every node — shard-key metadata is per-node, so it must
        // reach all seeds or routing disagrees between nodes (the meshdb endpoint guards each node
        // against declaring on a table that already holds rows there).
        ("POST", ["connections", name, "shardkey"]) => {
            if !session.role.permits(Permission::Operate) {
                return require(
                    request,
                    state,
                    &session,
                    Permission::Operate,
                    "meshdb.shardkey.declare",
                    name,
                );
            }
            let body = match json_body(&mut request) {
                Ok(v) => v,
                Err(message) => return respond::error(request, 400, message),
            };
            let seeds = match state.registry.resolve_seeds(name) {
                Ok(seeds) => seeds,
                Err(RegistryError::NotFound) => {
                    return respond::error(request, 404, "no such connection")
                }
                Err(RegistryError::Disabled) => {
                    return respond::error(request, 409, "this connection is disabled")
                }
                Err(e) => return respond::error(request, 502, &e.to_string()),
            };
            let mut applied = Vec::new();
            let mut failures = Vec::new();
            for resolved in &seeds {
                match crate::proxy::post_json_result(resolved, "shardkey", &body) {
                    Ok(_) => applied.push(resolved.url.clone()),
                    Err(e) => failures.push(format!("{}: {e}", resolved.url)),
                }
            }
            state.audit.record(
                Some(&session.user),
                "meshdb.shardkey.declare",
                name,
                if failures.is_empty() { "ok" } else { "failed" },
            );
            let status = if failures.is_empty() { 200 } else { 502 };
            respond::respond_json(
                request,
                status,
                &serde_json::json!({ "applied": applied, "failures": failures }),
            )
        }

        (_, ["connections", name, rest @ ..]) if !rest.is_empty() => {
            let Some(permission) = proxy_permission(&method, rest) else {
                return respond::error(request, 404, "no such endpoint or method");
            };
            let action = format!("meshdb.{}.{}", rest[0], method.to_ascii_lowercase());
            if !session.role.permits(permission) {
                return require(request, state, &session, permission, &action, name);
            }
            let resolved = match state.registry.resolve(name) {
                Ok(resolved) => resolved,
                Err(RegistryError::NotFound) => {
                    return respond::error(request, 404, "no such connection")
                }
                Err(RegistryError::Disabled) => {
                    return respond::error(request, 409, "this connection is disabled")
                }
                Err(e) => return respond::error(request, 502, &e.to_string()),
            };
            let _stream = if rest[0] == "query" {
                match state.streams.acquire() {
                    Some(slot) => Some(slot),
                    None => {
                        return respond::error(request, 429, "too many concurrent query streams")
                    }
                }
            } else {
                None
            };
            let body = if method == "GET" {
                Vec::new()
            } else {
                match read_body(&mut request) {
                    Ok(body) => body,
                    Err(message) => return respond::error(request, 413, message),
                }
            };
            let suffix = rest.join("/");
            let audited = matches!(permission, Permission::Write | Permission::ManageMeshUsers);
            let status = crate::proxy::forward(request, &resolved, &method, &suffix, body)?;
            if audited {
                state.audit.record(
                    Some(&session.user),
                    &action,
                    name,
                    if status < 400 { "ok" } else { "failed" },
                );
            }
            Ok(())
        }

        _ => respond::error(request, 404, "no such endpoint"),
    }
}

fn proxy_permission(method: &str, rest: &[&str]) -> Option<Permission> {
    match (method, rest) {
        ("GET", ["info" | "meta" | "health" | "cluster" | "topology" | "shards" | "stats"])
        | ("GET", ["replication"])
        | ("GET", ["s3", "status"])
        | ("GET", ["schema", _]) => Some(Permission::Observe),
        ("POST", ["query" | "query_all" | "route"]) => Some(Permission::Query),
        // /v1/run auto-routes and can write, so it needs write permission.
        ("POST", ["execute" | "tx" | "run"]) => Some(Permission::Write),
        // S3 archival config/snapshot/flush are operator actions (meshdb also requires Admin).
        ("POST", ["s3", "config" | "snapshot" | "flush"]) => Some(Permission::Operate),
        // Shard maintenance (vacuum/checkpoint) is an operator action.
        ("POST", ["shards", _, "vacuum" | "checkpoint"]) => Some(Permission::Operate),
        ("GET", ["frames", _]) => Some(Permission::Operate),
        ("GET" | "POST", ["users"]) | ("DELETE", ["users", _]) => Some(Permission::ManageMeshUsers),
        _ => None,
    }
}

fn user_result(request: Request, result: Result<(), UserError>) -> std::io::Result<()> {
    match result {
        Ok(()) => respond::respond_json(request, 200, &json!({ "ok": true })),
        Err(UserError::Exists) => respond::error(request, 409, &UserError::Exists.to_string()),
        Err(UserError::NotFound) => respond::error(request, 404, &UserError::NotFound.to_string()),
        Err(UserError::LastAdmin) => {
            respond::error(request, 409, &UserError::LastAdmin.to_string())
        }
        Err(e) => respond::error(request, 500, &e.to_string()),
    }
}

fn registry_result(request: Request, result: Result<(), RegistryError>) -> std::io::Result<()> {
    match result {
        Ok(()) => respond::respond_json(request, 200, &json!({ "ok": true })),
        Err(RegistryError::Exists) => {
            respond::error(request, 409, &RegistryError::Exists.to_string())
        }
        Err(RegistryError::NotFound) => {
            respond::error(request, 404, &RegistryError::NotFound.to_string())
        }
        Err(e @ (RegistryError::Disabled | RegistryError::Invalid(_))) => {
            respond::error(request, 400, &e.to_string())
        }
        Err(e) => respond::error(request, 500, &e.to_string()),
    }
}

fn operation_visible(session: &Session, operation: &Operation) -> bool {
    session.role == Role::Admin || operation.actor == session.user
}

fn operation_error(request: Request, error: OperationError) -> std::io::Result<()> {
    match error {
        OperationError::NotFound => respond::error(request, 404, &error.to_string()),
        OperationError::Conflict(_) => respond::error(request, 409, &error.to_string()),
        OperationError::Invalid(_) => respond::error(request, 400, &error.to_string()),
        OperationError::Io(_) => respond::error(request, 500, &error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_routes_have_an_explicit_method_and_permission() {
        assert_eq!(
            proxy_permission("GET", &["info"]),
            Some(Permission::Observe)
        );
        for endpoint in ["meta", "health", "topology", "shards"] {
            assert_eq!(
                proxy_permission("GET", &[endpoint]),
                Some(Permission::Observe)
            );
        }
        assert_eq!(
            proxy_permission("POST", &["query"]),
            Some(Permission::Query)
        );
        assert_eq!(
            proxy_permission("POST", &["execute"]),
            Some(Permission::Write)
        );
        assert_eq!(
            proxy_permission("POST", &["execute_all"]),
            None,
            "schema rollout must go through the durable operation coordinator"
        );
        assert_eq!(
            proxy_permission("GET", &["frames", "0"]),
            Some(Permission::Operate)
        );
        assert_eq!(
            proxy_permission("DELETE", &["users", "alice"]),
            Some(Permission::ManageMeshUsers)
        );
        assert_eq!(proxy_permission("DELETE", &["query"]), None);
        assert_eq!(proxy_permission("GET", &["unknown"]), None);
    }

    #[test]
    fn query_stream_slots_leave_capacity_for_control_requests() {
        let slots = StreamSlots::new(1);
        let first = slots.acquire().unwrap();
        assert!(slots.acquire().is_none());
        drop(first);
        assert!(slots.acquire().is_some());
    }
}
