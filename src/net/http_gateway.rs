//! An optional HTTP/JSON gateway over the same core the TCP server uses.
//!
//! # What this is, and is not
//!
//! A translation edge: HTTP request → the same [`super::server::handle`] or streaming read the
//! native protocol drives, → JSON. It adds no storage or cluster logic; it reuses the auth
//! doorman, shard routing, and the reader fleet unchanged. A deployment that only speaks the
//! native protocol compiles none of this (`http` feature off).
//!
//! # Synchronous, on purpose
//!
//! Thread-per-connection over `tiny_http`, matching the TCP server's model. The gateway's only
//! contact with the core is a plain function call, so a future async variant is an additive
//! feature swap here, not a rewrite of the core — see the plan in `docs/`.
//!
//! # Large results stream
//!
//! A query does not materialise its result. `POST /v1/query` on a locally-held shard streams
//! rows as newline-delimited JSON straight from the reader fleet's cursor, so a million-row
//! result costs the same memory as a ten-row one. The bounded channel between the reader and
//! the socket is the backpressure: a slow client throttles the reader rather than piling rows
//! into memory.

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tiny_http::{Header, Method, Request, Response, StatusCode};

use crate::error::{Error, Result};
use crate::shard::ShardManager;
use crate::shard::reader_fleet::StreamMsg;
use crate::storage::exec::{Statement, Value};

use super::auth::{self, Requirement, Role};
use super::json::{response_json_body, response_status, value_to_json};
use super::server::NodeServices;

/// How many rows the reader may run ahead of the socket. Small: memory stays tiny, the writer
/// always has work.
const STREAM_DEPTH: usize = 256;

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub addr: String,
    /// Worker threads serving HTTP. Each handles one request to completion, so a streaming
    /// query occupies one for its duration; a small pool is right for the floor profile.
    pub workers: usize,
    /// Permit auth over plaintext HTTP. Off by default: HTTP Basic sends the secret in clear,
    /// so a gateway with users but no transport security refuses to start unless this is set.
    pub insecure: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4680".into(),
            workers: 4,
            insecure: false,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    requests: AtomicU64,
    errors: AtomicU64,
    auth_failures: AtomicU64,
    authz_refused: AtomicU64,
}

pub struct HttpGateway {
    server: Arc<tiny_http::Server>,
    shards: Arc<ShardManager>,
    services: NodeServices,
    cfg: HttpConfig,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
}

impl HttpGateway {
    pub fn bind(
        shards: Arc<ShardManager>,
        services: NodeServices,
        cfg: HttpConfig,
    ) -> Result<Self> {
        // The security posture, enforced at startup, not documented and hoped for: a gateway
        // that authenticates over plaintext leaks every secret. Refuse it unless the operator
        // explicitly accepts the risk (a trusted network, a TLS-terminating proxy in front).
        let auth_on = services.auth.as_ref().is_some_and(|a| !a.is_empty());
        if auth_on && !cfg.insecure {
            return Err(Error::Protocol(
                "the HTTP gateway has authentication enabled but no transport security: HTTP \
                 Basic would send secrets in clear. Put a TLS-terminating proxy in front and \
                 pass --http-insecure to acknowledge, or disable auth on the gateway."
                    .into(),
            ));
        }
        if !auth_on {
            tracing::warn!(
                "HTTP gateway has no authentication: any client reaching {} has full access",
                cfg.addr
            );
        }

        let server = tiny_http::Server::http(&cfg.addr)
            .map_err(|e| Error::Protocol(format!("binding HTTP {}: {e}", cfg.addr)))?;
        tracing::info!(addr = %cfg.addr, workers = cfg.workers, "HTTP gateway listening");
        Ok(Self {
            server: Arc::new(server),
            shards,
            services,
            cfg,
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.server.server_addr().to_ip()
    }

    /// Serve until the shutdown handle is set. Blocks.
    pub fn serve(&self) {
        std::thread::scope(|scope| {
            for _ in 0..self.cfg.workers.max(1) {
                scope.spawn(|| {
                    while !self.shutdown.load(Ordering::Relaxed) {
                        // A short recv timeout lets a worker notice shutdown between requests.
                        match self
                            .server
                            .recv_timeout(std::time::Duration::from_millis(200))
                        {
                            Ok(Some(req)) => self.dispatch(req),
                            Ok(None) => {}
                            Err(_) => break,
                        }
                    }
                });
            }
        });
    }

    fn dispatch(&self, req: Request) {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let method = req.method().clone();
        let url = req.url().to_string();
        let path = url.split('?').next().unwrap_or("").to_string();

        // Authenticate once, up front. The role then gates each route.
        let role = match self.authenticate(&req) {
            Ok(r) => r,
            Err(resp) => {
                let _ = req.respond(resp);
                return;
            }
        };

        let outcome = match (&method, path.as_str()) {
            (Method::Get, "/v1/info") => {
                self.route(&req, role, Requirement::Read, || self.info_json())
            }
            (Method::Post, "/v1/query") => {
                // Streaming: handled specially, it takes ownership of the request to stream.
                return self.handle_query(req, role);
            }
            (Method::Post, "/v1/query_all") => {
                return self.handle_query_all(req, role);
            }
            (Method::Post, "/v1/run") => {
                return self.handle_run(req, role);
            }
            (Method::Post, "/v1/execute") => {
                return self.handle_execute(req, role);
            }
            (Method::Post, "/v1/tx") => {
                return self.handle_tx(req, role);
            }
            (Method::Post, "/v1/execute_all") => {
                return self.handle_execute_all(req, role);
            }
            (Method::Post, "/v1/route") => {
                return self.handle_route(req, role);
            }
            (Method::Get, "/v1/stats") => {
                self.route(&req, role, Requirement::Read, || self.stats_json())
            }
            (Method::Get, "/v1/cluster") => {
                self.route(&req, role, Requirement::Read, || self.cluster_json())
            }
            (Method::Get, p) if p.starts_with("/v1/schema/") => {
                self.schema_route(role, p.trim_start_matches("/v1/schema/"))
            }
            (Method::Get, p) if p.starts_with("/v1/frames/") => {
                self.frames_route(role, p.trim_start_matches("/v1/frames/"))
            }
            (Method::Get, "/v1/users") => {
                return self.handle_list_users(req, role);
            }
            (Method::Post, "/v1/users") => {
                return self.handle_create_user(req, role);
            }
            (Method::Delete, p) if p.starts_with("/v1/users/") => {
                let name = p.trim_start_matches("/v1/users/").to_string();
                return self.handle_drop_user(req, role, name);
            }
            _ => Err(HttpError::new(404, "no such endpoint")),
        };

        let resp = match outcome {
            Ok(body) => json_response(200, &body),
            Err(e) => {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                e.into_response()
            }
        };
        let _ = req.respond(resp);
    }

    /// Run a role-gated handler that produces a JSON body.
    fn route(
        &self,
        _req: &Request,
        role: Option<Role>,
        need: Requirement,
        f: impl FnOnce() -> serde_json::Value,
    ) -> std::result::Result<serde_json::Value, HttpError> {
        self.check(role, need)?;
        Ok(f())
    }

    /// Authorization: `None` role means auth is off (permitted). Otherwise the role must cover
    /// the requirement.
    fn check(&self, role: Option<Role>, need: Requirement) -> std::result::Result<(), HttpError> {
        let auth_on = self.services.auth.as_ref().is_some_and(|a| !a.is_empty());
        if !auth_on {
            return Ok(());
        }
        match role {
            Some(r) if r.permits(need) => Ok(()),
            Some(_) => {
                self.counters.authz_refused.fetch_add(1, Ordering::Relaxed);
                Err(HttpError::new(
                    403,
                    "the authenticated role does not permit this",
                ))
            }
            None => Err(HttpError::new(401, "authentication required")),
        }
    }

    /// Verify HTTP Basic credentials against the same challenge-response the TCP path uses.
    ///
    /// Returns `None` when auth is not configured (open), `Some(role)` on success, and an
    /// error response otherwise. The secret is turned into the keyed proof and checked against
    /// a fresh nonce, so the verification is byte-identical to the native handshake.
    fn authenticate(
        &self,
        req: &Request,
    ) -> std::result::Result<Option<Role>, Response<std::io::Cursor<Vec<u8>>>> {
        let Some(auth) = self.services.auth.as_ref().filter(|a| !a.is_empty()) else {
            return Ok(None);
        };
        let Some((name, secret)) = http_credentials(req) else {
            return Err(auth_challenge(
                "authentication required (Authorization: Basic or Bearer, base64 of name:secret)",
            ));
        };
        let nonce = auth::nonce()
            .map_err(|_| json_response(500, &serde_json::json!({ "error": "entropy failure" })))?;
        let proof = auth::prove(&auth::derive_key(&secret), &nonce);
        match auth.verify(&name, &nonce, &proof) {
            Some(role) => Ok(Some(role)),
            None => {
                self.counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                Err(auth_challenge("authentication failed"))
            }
        }
    }

    fn info_json(&self) -> serde_json::Value {
        serde_json::json!({
            "shard_count": self.shards.shard_count(),
            "epoch": self.shards.epoch(),
        })
    }

    /// Whether this node can serve `shard` from its own storage (owns it, or standalone).
    fn is_local(&self, shard: u32) -> bool {
        self.services
            .router
            .as_ref()
            .is_none_or(|r| r.is_mine(crate::shard::ShardId(shard)))
    }

    /// `POST /v1/query` — streams rows as NDJSON for a locally-held shard, or falls back to the
    /// materialised (routed) path for a remote one.
    fn handle_query(&self, mut req: Request, role: Option<Role>) {
        if let Err(e) = self.check(role, Requirement::Read) {
            let _ = req.respond(e.into_response());
            return;
        }
        let body = match read_body(&mut req) {
            Ok(b) => b,
            Err(e) => {
                let _ = req.respond(e.into_response());
                return;
            }
        };
        let q: QueryBody = match serde_json::from_slice(&body) {
            Ok(q) => q,
            Err(e) => {
                let _ = req.respond(HttpError::new(400, &format!("bad JSON: {e}")).into_response());
                return;
            }
        };
        let stmt = match q.to_statement() {
            Ok(s) => s,
            Err(e) => {
                let _ = req.respond(e.into_response());
                return;
            }
        };

        if self.is_local(q.shard) {
            // The streaming path. Start the reader, then respond with a body that pulls rows.
            match self
                .shards
                .query_stream(crate::shard::ShardId(q.shard), stmt, STREAM_DEPTH)
            {
                Ok(rx) => {
                    let headers = vec![content_type("application/x-ndjson")];
                    let resp =
                        Response::new(StatusCode(200), headers, NdjsonBody::new(rx), None, None);
                    let _ = req.respond(resp);
                }
                Err(e) => {
                    let _ = req.respond(error_to_http(&e).into_response());
                }
            }
        } else {
            // Remote shard: materialise through the routed path (subject to the native 16 MB
            // frame cap) and emit it as the same NDJSON shape, so the client sees one format.
            let resp = super::server::handle(
                super::protocol::Request::Query {
                    shard: q.shard,
                    statement: q.to_statement().unwrap_or_else(|_| Statement::new("")),
                    consistency: q.consistency(),
                },
                &self.shards,
                &self.services,
            );
            let _ = req.respond(materialised_query_to_ndjson(resp));
        }
    }

    fn handle_query_all(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Read).and_then(|()| {
            let body = read_body(&mut req)?;
            let q: SqlBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let resp = super::server::handle(
                super::protocol::Request::QueryAll {
                    statement: Statement::new(&q.sql),
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    /// `POST /v1/run` — auto-routed: the server picks the shard(s), so the client names none. A read
    /// returns rows, a write the count. The permission follows the statement's verb (as
    /// `auth::required` does for `Request::Run`), so it must be classified before the check.
    fn handle_run(&self, mut req: Request, role: Option<Role>) {
        let out = (|| {
            let body = read_body(&mut req)?;
            let q: SqlBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let need = match crate::db::first_keyword(&q.sql).as_str() {
                "CREATE" | "DROP" | "ALTER" => Requirement::Admin,
                "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => Requirement::Write,
                _ => Requirement::Read,
            };
            self.check(role, need)?;
            let resp = super::server::handle(
                super::protocol::Request::Run {
                    statement: Statement::new(&q.sql),
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        })();
        respond_json(req, out);
    }

    fn handle_execute(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Write).and_then(|()| {
            let body = read_body(&mut req)?;
            let q: QueryBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let stmt = q.to_statement()?;
            let resp = super::server::handle(
                super::protocol::Request::Execute {
                    shard: q.shard,
                    statements: vec![stmt],
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    fn handle_tx(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Write).and_then(|()| {
            let body = read_body(&mut req)?;
            let tx: TxBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let statements: std::result::Result<Vec<Statement>, HttpError> =
                tx.statements.iter().map(|s| s.to_statement()).collect();
            let resp = super::server::handle(
                super::protocol::Request::Transaction {
                    shard: tx.shard,
                    statements: statements?,
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    fn handle_execute_all(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Admin).and_then(|()| {
            let body = read_body(&mut req)?;
            let q: SqlBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let resp = super::server::handle(
                super::protocol::Request::ExecuteAll {
                    statement: Statement::new(&q.sql),
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    fn handle_route(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Read).and_then(|()| {
            let body = read_body(&mut req)?;
            let k: KeyBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let resp = super::server::handle(
                super::protocol::Request::Route {
                    key: k.key.into_bytes(),
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    fn handle_list_users(&self, req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Admin).map(|()| {
            response_to_http(super::server::handle(
                super::protocol::Request::ListUsers,
                &self.shards,
                &self.services,
            ))
        });
        respond_json(req, out);
    }

    fn handle_create_user(&self, mut req: Request, role: Option<Role>) {
        let out = self.check(role, Requirement::Admin).and_then(|()| {
            let body = read_body(&mut req)?;
            let u: CreateUserBody = serde_json::from_slice(&body)
                .map_err(|e| HttpError::new(400, &format!("bad JSON: {e}")))?;
            let parsed: Role = u
                .role
                .parse()
                .map_err(|e: Error| HttpError::new(400, &e.to_string()))?;
            // The key is derived here so the secret is never forwarded to the core; the
            // cluster-role refusal lives in handle(), the single place that rule is enforced.
            let resp = super::server::handle(
                super::protocol::Request::CreateUser {
                    name: u.name,
                    key: auth::derive_key(&u.secret),
                    role: parsed,
                },
                &self.shards,
                &self.services,
            );
            Ok(response_to_http(resp))
        });
        respond_json(req, out);
    }

    fn handle_drop_user(&self, req: Request, role: Option<Role>, name: String) {
        let out = self.check(role, Requirement::Admin).map(|()| {
            response_to_http(super::server::handle(
                super::protocol::Request::DropUser { name },
                &self.shards,
                &self.services,
            ))
        });
        respond_json(req, out);
    }

    fn schema_route(
        &self,
        role: Option<Role>,
        shard_str: &str,
    ) -> std::result::Result<serde_json::Value, HttpError> {
        self.check(role, Requirement::Read)?;
        let shard: u32 = shard_str
            .parse()
            .map_err(|_| HttpError::new(400, "shard must be a number"))?;
        let version = self
            .shards
            .schema_version(crate::shard::ShardId(shard))
            .map_err(|e| error_to_http(&e))?;
        Ok(serde_json::json!({ "shard": shard, "schema_version": version }))
    }

    fn frames_route(
        &self,
        role: Option<Role>,
        shard_str: &str,
    ) -> std::result::Result<serde_json::Value, HttpError> {
        self.check(role, Requirement::Admin)?;
        let shard: u32 = shard_str
            .parse()
            .map_err(|_| HttpError::new(400, "shard must be a number"))?;
        let db = crate::shard::ShardId(shard).path(self.shards.dir());
        let wal = crate::storage::checkpoint::wal_path_for(&db);
        let bytes = std::fs::read(&wal).unwrap_or_default();
        let report = crate::vfs::inspect_wal(&bytes);
        Ok(frames_json(&report))
    }

    fn stats_json(&self) -> serde_json::Value {
        let w = self.shards.writer_stats();
        let r = self.shards.reader_stats();
        serde_json::json!({
            "writer": {
                "batches": w.batches, "requests": w.requests, "max_batch": w.max_batch,
                "open_now": w.open_now, "threads": w.threads,
            },
            "reader": {
                "queries": r.queries, "rejected_busy": r.rejected_busy,
                "timed_out": r.timed_out, "threads": r.threads,
            },
            "http": {
                "requests": self.counters.requests.load(Ordering::Relaxed),
                "errors": self.counters.errors.load(Ordering::Relaxed),
                "auth_failures": self.counters.auth_failures.load(Ordering::Relaxed),
                "authz_refused": self.counters.authz_refused.load(Ordering::Relaxed),
            },
        })
    }

    fn cluster_json(&self) -> serde_json::Value {
        match self.services.cluster.as_ref() {
            None => {
                serde_json::json!({ "clustered": false, "shard_count": self.shards.shard_count() })
            }
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
}

// ---- request bodies ----

#[derive(serde::Deserialize)]
struct QueryBody {
    #[serde(default)]
    shard: u32,
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
    #[serde(default)]
    consistency: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct SqlBody {
    sql: String,
}

#[derive(serde::Deserialize)]
struct StmtBody {
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct KeyBody {
    key: String,
}

#[derive(serde::Deserialize)]
struct CreateUserBody {
    name: String,
    secret: String,
    role: String,
}

#[derive(serde::Deserialize)]
struct TxBody {
    #[serde(default)]
    shard: u32,
    statements: Vec<StmtBody>,
}

impl QueryBody {
    fn to_statement(&self) -> std::result::Result<Statement, HttpError> {
        Ok(Statement::with_params(
            &self.sql,
            json_params(&self.params)?,
        ))
    }
    fn consistency(&self) -> super::protocol::ReadConsistency {
        use super::protocol::ReadConsistency;
        match &self.consistency {
            Some(serde_json::Value::String(s)) if s == "stale" => ReadConsistency::Stale,
            Some(serde_json::Value::Object(o)) => o
                .get("at_least_lsn")
                .and_then(|v| v.as_u64())
                .map(ReadConsistency::AtLeastLsn)
                .unwrap_or_default(),
            _ => ReadConsistency::Linearizable,
        }
    }
}

impl StmtBody {
    fn to_statement(&self) -> std::result::Result<Statement, HttpError> {
        Ok(Statement::with_params(
            &self.sql,
            json_params(&self.params)?,
        ))
    }
}

/// Adapt the shared param parser's error into an HTTP 400.
fn json_params(vals: &[serde_json::Value]) -> std::result::Result<Vec<Value>, HttpError> {
    super::json::json_params(vals).map_err(|m| HttpError::new(400, &m))
}

// ---- value / response JSON ----

/// A WAL inspection report as JSON, matching the `meshdb frames` view.
fn frames_json(report: &crate::vfs::WalReport) -> serde_json::Value {
    match &report.header {
        None => serde_json::json!({ "wal": false, "file_bytes": report.file_bytes }),
        Some(h) => serde_json::json!({
            "wal": true,
            "file_bytes": report.file_bytes,
            "page_size": h.page_size,
            "salt": h.salt.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "frames": report.frames.len(),
            "transactions": report.transactions(),
            "uncommitted_frames": report.uncommitted_frames(),
            "leftover_frames": report.frames.iter().filter(|f| !f.current).count(),
        }),
    }
}

/// A native `Response` → (HTTP status, JSON) for the bounded (non-streaming) endpoints. The
/// status reflects the outcome: a rejected statement is a 400, not a 200 with an error body.
fn response_to_http(resp: super::protocol::Response) -> (u16, serde_json::Value) {
    (response_status(&resp), response_json_body(resp))
}

/// A materialised query `Response` rendered as the streaming NDJSON shape, so a remote query
/// looks the same to the client as a local streamed one.
fn materialised_query_to_ndjson(
    resp: super::protocol::Response,
) -> Response<std::io::Cursor<Vec<u8>>> {
    use super::protocol::Response as R;
    let mut body = Vec::new();
    match resp {
        R::Rows { columns, rows } => {
            let _ = writeln!(body, "{}", serde_json::json!({ "columns": columns }));
            for row in rows {
                let cells: Vec<serde_json::Value> = row.iter().map(value_to_json).collect();
                let _ = writeln!(body, "{}", serde_json::Value::Array(cells));
            }
        }
        other => {
            let _ = writeln!(body, "{}", response_json_body(other));
        }
    }
    Response::new(
        StatusCode(200),
        vec![content_type("application/x-ndjson")],
        std::io::Cursor::new(body.clone()),
        Some(body.len()),
        None,
    )
}

// ---- streaming body ----

/// A `Read` over the reader fleet's row channel, emitting one NDJSON line per message.
///
/// This is where memory stays bounded: it holds at most one serialised line plus whatever the
/// bounded channel buffers. `read` blocks on the channel, which blocks the reader thread when
/// the socket is slow — backpressure, end to end.
struct NdjsonBody {
    rx: std::sync::mpsc::Receiver<StreamMsg>,
    pending: std::io::Cursor<Vec<u8>>,
    finished: bool,
}

impl NdjsonBody {
    fn new(rx: std::sync::mpsc::Receiver<StreamMsg>) -> Self {
        Self {
            rx,
            pending: std::io::Cursor::new(Vec::new()),
            finished: false,
        }
    }

    fn next_line(&mut self) -> Option<Vec<u8>> {
        match self.rx.recv() {
            Ok(StreamMsg::Columns(c)) => {
                let mut line = serde_json::to_vec(&serde_json::json!({ "columns": c })).unwrap();
                line.push(b'\n');
                Some(line)
            }
            Ok(StreamMsg::Row(r)) => {
                let cells: Vec<serde_json::Value> = r.iter().map(value_to_json).collect();
                let mut line = serde_json::to_vec(&serde_json::Value::Array(cells)).unwrap();
                line.push(b'\n');
                Some(line)
            }
            // The status line was already 200 by the time streaming began, so a mid-stream
            // failure is reported as a trailing error object rather than a status code.
            Ok(StreamMsg::Failed(e)) => {
                let mut line = serde_json::to_vec(&serde_json::json!({ "error": e })).unwrap();
                line.push(b'\n');
                self.finished = true;
                Some(line)
            }
            Ok(StreamMsg::Done) | Err(_) => {
                self.finished = true;
                None
            }
        }
    }
}

impl Read for NdjsonBody {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let n = self.pending.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            if self.finished {
                return Ok(0);
            }
            match self.next_line() {
                Some(line) => self.pending = std::io::Cursor::new(line),
                None => return Ok(0),
            }
        }
    }
}

// ---- small helpers ----

struct HttpError {
    status: u16,
    message: String,
}

impl HttpError {
    fn new(status: u16, message: &str) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }
    fn into_response(self) -> Response<std::io::Cursor<Vec<u8>>> {
        json_response(self.status, &serde_json::json!({ "error": self.message }))
    }
}

/// Map a native `Error` to an HTTP error with a faithful status.
fn error_to_http(e: &Error) -> HttpError {
    let status = match e {
        Error::ReaderPoolBusy | Error::WriterBusy | Error::TooManyConnections { .. } => 503,
        Error::NotLeader { .. } => 409,
        Error::Unsupported(_) => 400,
        _ => 500,
    };
    HttpError::new(status, &e.to_string())
}

fn json_response(status: u16, body: &serde_json::Value) -> Response<std::io::Cursor<Vec<u8>>> {
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    Response::new(
        StatusCode(status),
        vec![content_type("application/json")],
        std::io::Cursor::new(bytes.clone()),
        Some(bytes.len()),
        None,
    )
}

/// A 401 with a JSON error body and the `WWW-Authenticate` header a browser needs to prompt
/// for credentials.
fn auth_challenge(message: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(401, &serde_json::json!({ "error": message }))
        .with_header(Header::from_bytes(&b"WWW-Authenticate"[..], &b"Basic"[..]).unwrap())
}

fn content_type(ct: &str) -> Header {
    Header::from_bytes(&b"Content-Type"[..], ct.as_bytes()).unwrap()
}

fn read_body(req: &mut Request) -> std::result::Result<Vec<u8>, HttpError> {
    let mut buf = Vec::new();
    req.as_reader()
        .read_to_end(&mut buf)
        .map_err(|e| HttpError::new(400, &format!("reading body: {e}")))?;
    Ok(buf)
}

fn respond_json(req: Request, out: std::result::Result<(u16, serde_json::Value), HttpError>) {
    let resp = match out {
        Ok((status, body)) => json_response(status, &body),
        Err(e) => e.into_response(),
    };
    let _ = req.respond(resp);
}

/// Decode `name` and `secret` from an `Authorization` header.
///
/// Accepts two schemes, both carrying `base64(name:secret)` — the caller picks:
/// - `Basic` — the standard browser-friendly form; a browser will prompt for it.
/// - `Bearer` — the same payload under the bearer scheme, which programmatic clients often
///   prefer because it does not trigger a browser login dialog. The "token" is the secret; the
///   verification is identical either way.
fn http_credentials(req: &Request) -> Option<(String, String)> {
    let header = req
        .headers()
        .iter()
        .find(|h| h.field.equiv("Authorization"))?;
    let value = header.value.as_str();
    let b64 = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("Bearer "))?;
    let decoded = base64_decode(b64.trim())?;
    let text = String::from_utf8(decoded).ok()?;
    let (name, secret) = text.split_once(':')?;
    Some((name.to_string(), secret.to_string()))
}

/// Minimal standard-alphabet base64 decode — enough for HTTP Basic, no dependency.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim().trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &c in s.as_bytes() {
        let v = val(c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}
