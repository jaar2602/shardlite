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
            (Method::Post, "/v1/execute") => {
                return self.handle_execute(req, role);
            }
            (Method::Post, "/v1/tx") => {
                return self.handle_tx(req, role);
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
        let Some((name, secret)) = basic_credentials(req) else {
            return Err(auth_challenge("authentication required (HTTP Basic)"));
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

/// JSON parameters → SQL values. Supports null, integers, floats, and strings — the ordinary
/// bound-parameter kinds. Arrays and objects are refused with a clear message rather than
/// guessed at.
fn json_params(vals: &[serde_json::Value]) -> std::result::Result<Vec<Value>, HttpError> {
    vals.iter()
        .map(|v| match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Integer(*b as i64)),
            serde_json::Value::Number(n) if n.is_i64() => Ok(Value::Integer(n.as_i64().unwrap())),
            serde_json::Value::Number(n) => Ok(Value::Real(n.as_f64().unwrap())),
            serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
            other => Err(HttpError::new(
                400,
                &format!("unsupported parameter type: {other}"),
            )),
        })
        .collect()
}

// ---- value / response JSON ----

/// One SQL value as clean JSON: null, number, string. A blob becomes an array of byte values
/// — honest and lossless, if verbose; a base64 form could come later.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => serde_json::json!(i),
        Value::Real(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::json!(s),
        Value::Blob(b) => serde_json::json!(b),
    }
}

/// A native `Response` → (HTTP status, JSON) for the bounded (non-streaming) endpoints. The
/// status reflects the outcome: a rejected statement is a 400, not a 200 with an error body.
fn response_to_http(resp: super::protocol::Response) -> (u16, serde_json::Value) {
    use super::protocol::Response as R;
    let status = match &resp {
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
    };
    (status, response_json_body(resp))
}

fn response_json_body(resp: super::protocol::Response) -> serde_json::Value {
    use super::protocol::Response as R;
    match resp {
        R::Rows { columns, rows } => serde_json::json!({
            "columns": columns,
            "rows": rows.iter().map(|r| r.iter().map(value_to_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }),
        R::Changed {
            rows_affected,
            last_insert_rowid,
        } => serde_json::json!({
            "rows_affected": rows_affected,
            "last_insert_rowid": last_insert_rowid,
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
        R::Rejected { message } => serde_json::json!({ "error": message, "rejected": true }),
        R::Error { message, retryable } => {
            serde_json::json!({ "error": message, "retryable": retryable })
        }
        other => serde_json::json!({ "error": format!("unexpected response: {other:?}") }),
    }
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

/// Decode the username and secret from an HTTP Basic `Authorization` header.
fn basic_credentials(req: &Request) -> Option<(String, String)> {
    let header = req
        .headers()
        .iter()
        .find(|h| h.field.equiv("Authorization"))?;
    let value = header.value.as_str();
    let b64 = value.strip_prefix("Basic ")?;
    let decoded = base64_decode(b64)?;
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
