//! The console's own HTTP API, and the auth gate in front of it.
//!
//! Everything under `/api` is the console talking to its own frontend; everything else is the
//! embedded SPA. Three endpoints are reachable without a session — `POST /api/login`,
//! `POST /api/logout`, `GET /api/me` — and everything else requires one. Console-user management
//! and connection-registry writes additionally require the `admin` console role. The
//! `/api/connections/{name}/…` family forwards to that connection's meshdb `/v1` edge.

use std::sync::Arc;

use serde_json::{json, Value};
use tiny_http::Request;

use crate::auth::{self, Sessions};
use crate::metrics::Metrics;
use crate::registry::{Registry, RegistryError};
use crate::respond;
use crate::users::{Role, UserError, Users};

/// Everything a request handler needs. `registry` and `metrics` are shared with the background
/// sampler thread, hence `Arc`.
pub struct AppState {
    pub users: Users,
    pub registry: Arc<Registry>,
    pub sessions: Sessions,
    pub metrics: Arc<Metrics>,
}

fn read_body(request: &mut Request) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = request.as_reader().read_to_end(&mut buf);
    buf
}

fn cookie_token(request: &Request) -> Option<String> {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Cookie"))
        .and_then(|h| auth::token_from_cookie_header(h.value.as_str()))
}

/// Endpoints under `/v1` a connection may be asked to forward to. A closed whitelist so the
/// proxy can never be pointed at an arbitrary upstream path.
const PROXY_ALLOWED: &[&str] = &[
    "query",
    "query_all",
    "execute",
    "tx",
    "execute_all",
    "route",
    "info",
    "cluster",
    "stats",
    "schema",
    "frames",
    "users",
];

pub fn handle(mut request: Request, state: &AppState) -> std::io::Result<()> {
    let method = request.method().to_string();
    let raw_url = request.url().to_string();
    let path = raw_url.split('?').next().unwrap_or("").to_string();
    let segments: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Anything that is not an /api call is the embedded SPA.
    if segments.first().map(String::as_str) != Some("api") {
        return crate::assets::serve(request, &path);
    }

    let session = cookie_token(&request).and_then(|t| state.sessions.lookup(&t));
    let tail: Vec<&str> = segments.iter().skip(1).map(String::as_str).collect();

    // --- endpoints reachable without a session ---
    match (method.as_str(), tail.as_slice()) {
        ("POST", ["login"]) => {
            let body = read_body(&mut request);
            let v: Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => return respond::error(request, 400, "invalid JSON body"),
            };
            let username = v.get("username").and_then(Value::as_str).unwrap_or("");
            let password = v.get("password").and_then(Value::as_str).unwrap_or("");
            return match state.users.verify(username, password) {
                Some(role) => {
                    let token = state.sessions.create(username.to_string(), role);
                    respond::respond_json_with_cookie(
                        request,
                        200,
                        &json!({ "user": username, "role": role }),
                        &auth::set_cookie(&token),
                    )
                }
                None => respond::error(request, 401, "invalid credentials"),
            };
        }
        ("POST", ["logout"]) => {
            if let Some(token) = cookie_token(&request) {
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
                Some(s) => {
                    respond::respond_json(request, 200, &json!({ "user": s.user, "role": s.role }))
                }
                None => respond::error(request, 401, "not logged in"),
            };
        }
        _ => {}
    }

    // --- everything below requires a session ---
    let session = match session {
        Some(s) => s,
        None => return respond::error(request, 401, "not logged in"),
    };
    let is_admin = session.role == Role::Admin;

    match (method.as_str(), tail.as_slice()) {
        // Console user management (admin only).
        ("GET", ["console-users"]) => {
            if !is_admin {
                return respond::error(request, 403, "admin only");
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
            if !is_admin {
                return respond::error(request, 403, "admin only");
            }
            let body = read_body(&mut request);
            let v: Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => return respond::error(request, 400, "invalid JSON body"),
            };
            let name = v.get("username").and_then(Value::as_str).unwrap_or("");
            let password = v.get("password").and_then(Value::as_str).unwrap_or("");
            let role: Role = match v.get("role").and_then(|r| serde_json::from_value(r.clone()).ok())
            {
                Some(r) => r,
                None => return respond::error(request, 400, "role must be \"admin\" or \"user\""),
            };
            if name.is_empty() || password.is_empty() {
                return respond::error(request, 400, "username and password are required");
            }
            user_result(request, state.users.create(name, password, role))
        }
        ("DELETE", ["console-users", name]) => {
            if !is_admin {
                return respond::error(request, 403, "admin only");
            }
            user_result(request, state.users.delete(name))
        }

        // Connection registry.
        ("GET", ["connections"]) => {
            respond::respond_json(request, 200, &json!(state.registry.list()))
        }
        ("POST", ["connections"]) => {
            if !is_admin {
                return respond::error(request, 403, "admin only");
            }
            let body = read_body(&mut request);
            let v: Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => return respond::error(request, 400, "invalid JSON body"),
            };
            let name = v.get("name").and_then(Value::as_str).unwrap_or("");
            let url = v.get("url").and_then(Value::as_str).unwrap_or("");
            if name.is_empty() || url.is_empty() {
                return respond::error(request, 400, "name and url are required");
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
            registry_result(
                request,
                state
                    .registry
                    .put(name, url, meshdb_user, meshdb_secret, replace),
            )
        }
        ("DELETE", ["connections", name]) => {
            if !is_admin {
                return respond::error(request, 403, "admin only");
            }
            let result = state.registry.delete(name);
            if result.is_ok() {
                state.metrics.forget(name);
            }
            registry_result(request, result)
        }

        // A connection's stats history (console-owned, not from the cluster).
        ("GET", ["connections", name, "metrics"]) => {
            respond::respond_json(request, 200, &json!(state.metrics.history(name)))
        }

        // Everything else under a connection forwards to its meshdb /v1 edge.
        (_, ["connections", name, rest @ ..]) if !rest.is_empty() => {
            if !PROXY_ALLOWED.contains(&rest[0]) {
                return respond::error(request, 404, "no such endpoint");
            }
            let resolved = match state.registry.resolve(name) {
                Ok(r) => r,
                Err(RegistryError::NotFound) => {
                    return respond::error(request, 404, "no such connection")
                }
                Err(e) => return respond::error(request, 502, &e.to_string()),
            };
            let body = read_body(&mut request);
            let suffix = rest.join("/");
            crate::proxy::forward(request, &resolved, &method, &suffix, body)
        }

        _ => respond::error(request, 404, "no such endpoint"),
    }
}

fn user_result(request: Request, result: Result<(), UserError>) -> std::io::Result<()> {
    match result {
        Ok(()) => respond::respond_json(request, 200, &json!({ "ok": true })),
        Err(UserError::Exists) => respond::error(request, 409, &UserError::Exists.to_string()),
        Err(UserError::NotFound) => respond::error(request, 404, &UserError::NotFound.to_string()),
        Err(UserError::LastAdmin) => respond::error(request, 409, &UserError::LastAdmin.to_string()),
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
        Err(e) => respond::error(request, 500, &e.to_string()),
    }
}
