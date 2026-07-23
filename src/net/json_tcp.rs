//! A length-prefixed JSON protocol over a persistent TCP socket.
//!
//! # Why this exists
//!
//! The native protocol is fast but `bincode`, which is Rust-specific and version-locked — not
//! safe to reimplement in other languages. HTTP is cross-language but pays header overhead and
//! (without care) a connection per request. This is the middle path a driver in any language
//! can implement in a page of code: a held TCP socket carrying `[4-byte big-endian length]
//! [JSON]` frames, stable across versions because the payload is plain JSON.
//!
//! It reuses the same [`super::server::handle`] core the native and HTTP paths do — it adds no
//! storage or cluster logic, only a framing and a JSON translation.
//!
//! # The exchange
//!
//! One request frame in; one or more response frames out. Bounded operations answer with a
//! single `{"result": ...}` (or `{"error": ..., "status": N}`). A query streams: `{"columns":
//! [...]}`, then a `{"row": [...]}` per row, then `{"end": true}` — so a million-row result
//! never materialises, on either side. The bounded reader channel plus the blocking socket
//! write give the same end-to-end backpressure the HTTP path has.
//!
//! Authentication, when configured, is the first frame: `{"op":"auth","name","secret"}`,
//! verified by the same challenge-response the native handshake uses. The secret crosses the
//! wire, so — exactly like HTTP Basic — the server refuses to start with auth enabled and no
//! transport security unless the operator passes the insecure acknowledgement.

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::shard::ShardManager;

use super::auth::{self, Requirement, Role};
use super::json;
use super::protocol::{Request, Response};
use super::server::NodeServices;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const STREAM_DEPTH: usize = 256;

#[derive(Debug, Clone)]
pub struct JsonTcpConfig {
    pub addr: String,
    /// Permit auth over a plaintext socket. Off by default: the secret crosses the wire.
    pub insecure: bool,
}

impl Default for JsonTcpConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4620".into(),
            insecure: false,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    accepted: AtomicU64,
    requests: AtomicU64,
    auth_failures: AtomicU64,
}

pub struct JsonTcpServer {
    listener: TcpListener,
    shards: Arc<ShardManager>,
    services: NodeServices,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
}

impl JsonTcpServer {
    pub fn bind(
        shards: Arc<ShardManager>,
        services: NodeServices,
        cfg: JsonTcpConfig,
    ) -> Result<Self> {
        let auth_on = services.auth.as_ref().is_some_and(|a| !a.is_empty());
        if auth_on && !cfg.insecure {
            return Err(Error::Protocol(
                "the JSON-TCP server has authentication enabled but no transport security: the \
                 secret crosses the wire in clear. Run it on a trusted network or behind a TLS \
                 tunnel and pass the insecure acknowledgement, or disable auth."
                    .into(),
            ));
        }
        if !auth_on {
            tracing::warn!("JSON-TCP server has no authentication: any client has full access");
        }
        let listener = TcpListener::bind(&cfg.addr)
            .map_err(|e| Error::Protocol(format!("binding JSON-TCP {}: {e}", cfg.addr)))?;
        tracing::info!(addr = %cfg.addr, "JSON-TCP listening");
        Ok(Self {
            listener,
            shards,
            services,
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.local_addr().ok()
    }

    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Accept until shutdown. Thread per connection, like the native server.
    pub fn serve(&self) {
        for stream in self.listener.incoming() {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let Ok(stream) = stream else { continue };
            self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            let shards = Arc::clone(&self.shards);
            let services = self.services.clone();
            let counters = Arc::clone(&self.counters);
            std::thread::Builder::new()
                .name("shardlite-jsontcp".into())
                .spawn(move || {
                    if let Err(e) = serve_conn(stream, &shards, &services, &counters) {
                        tracing::debug!(error = %e, "JSON-TCP connection ended");
                    }
                })
                .ok();
        }
    }
}

/// A connection's authentication state.
enum Gate {
    Open,
    Unauthed,
    Authed(Role),
}

fn serve_conn(
    stream: TcpStream,
    shards: &ShardManager,
    services: &NodeServices,
    counters: &Counters,
) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    let auth_on = services.auth.as_ref().is_some_and(|a| !a.is_empty());
    let mut gate = if auth_on { Gate::Unauthed } else { Gate::Open };

    while let Some(frame) = read_frame(&mut reader)? {
        counters.requests.fetch_add(1, Ordering::Relaxed);
        let op = frame.get("op").and_then(|v| v.as_str()).unwrap_or("");

        // The doorman. Until authed, only `auth` is accepted.
        if matches!(gate, Gate::Unauthed) && op != "auth" {
            write_error(&mut writer, 401, "authentication required")?;
            continue;
        }

        match op {
            "auth" => {
                let auth = services.auth.as_ref();
                let name = frame.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let secret = frame.get("secret").and_then(|v| v.as_str()).unwrap_or("");
                match auth.and_then(|a| verify(a, name, secret)) {
                    Some(role) => {
                        gate = Gate::Authed(role);
                        write_frame(
                            &mut writer,
                            &serde_json::json!({ "result": { "ok": true } }),
                        )?;
                    }
                    None => {
                        counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                        write_error(&mut writer, 401, "authentication failed")?;
                        // A failed auth closes the socket, as on the HTTP and native paths.
                        return Ok(());
                    }
                }
            }
            "query" => {
                if !permits(&gate, Requirement::Read) {
                    write_error(&mut writer, 403, "not permitted")?;
                    continue;
                }
                stream_query(&mut writer, shards, services, &frame)?;
            }
            "info" => reply(&mut writer, json::info_json(shards))?,
            "stats" => reply(&mut writer, json::fleet_stats_json(shards))?,
            "cluster" => {
                if !permits(&gate, Requirement::Read) {
                    write_error(&mut writer, 403, "not permitted")?;
                    continue;
                }
                reply(&mut writer, json::cluster_json(shards, services))?;
            }
            "schema" => {
                if !permits(&gate, Requirement::Read) {
                    write_error(&mut writer, 403, "not permitted")?;
                    continue;
                }
                let sh = frame.get("shard").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                match shards.schema_version(crate::shard::ShardId(sh)) {
                    Ok(v) => reply(
                        &mut writer,
                        serde_json::json!({ "shard": sh, "schema_version": v }),
                    )?,
                    Err(e) => write_error(&mut writer, error_status(&e), &e.to_string())?,
                }
            }
            "frames" => {
                if !permits(&gate, Requirement::Admin) {
                    write_error(&mut writer, 403, "not permitted")?;
                    continue;
                }
                let sh = frame.get("shard").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                reply(&mut writer, json::frames_json_for(shards, sh))?;
            }
            _ => {
                // Everything else maps to a native Request, handled once and answered with one
                // frame — the same core the native and HTTP paths call.
                match build_request(op, &frame) {
                    Ok((req, need)) => {
                        if !permits(&gate, need) {
                            write_error(&mut writer, 403, "not permitted")?;
                            continue;
                        }
                        let resp = super::server::handle(req, shards, services);
                        let status = json::response_status(&resp);
                        let body = json::response_json_body(resp);
                        if status == 200 {
                            reply(&mut writer, body)?;
                        } else {
                            let msg = body
                                .get("error")
                                .and_then(|e| e.as_str())
                                .unwrap_or("error");
                            write_error(&mut writer, status, msg)?;
                        }
                    }
                    Err(msg) => write_error(&mut writer, 400, &msg)?,
                }
            }
        }
    }
    Ok(())
}

/// Write a `{"result": <body>}` frame.
fn reply(w: &mut TcpStream, body: serde_json::Value) -> std::io::Result<()> {
    write_frame(w, &serde_json::json!({ "result": body }))
}

/// A read query against one shard, streamed frame by frame.
fn stream_query(
    writer: &mut TcpStream,
    shards: &ShardManager,
    services: &NodeServices,
    frame: &serde_json::Value,
) -> std::io::Result<()> {
    let shard = frame.get("shard").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let sql = frame.get("sql").and_then(|v| v.as_str()).unwrap_or("");
    let params = frame
        .get("params")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let stmt = match json::statement_from(sql, &params) {
        Ok(s) => s,
        Err(e) => return write_error(writer, 400, &e),
    };
    // A shard this node does not own is answered by its owner, not the local (replica or empty)
    // file. Streaming across the wire is not worth it for one shard, so forward through the shared
    // handler and re-emit its rows as frames.
    if services
        .router
        .as_ref()
        .is_some_and(|r| !r.is_mine(crate::shard::ShardId(shard)))
    {
        let resp = super::server::handle(
            Request::Query {
                shard,
                statement: stmt,
                consistency: super::protocol::ReadConsistency::Linearizable,
            },
            shards,
            services,
        );
        return match resp {
            Response::Rows { columns, rows } => {
                write_frame(writer, &serde_json::json!({ "columns": columns }))?;
                for r in rows {
                    let cells: Vec<serde_json::Value> = r.iter().map(json::value_to_json).collect();
                    write_frame(writer, &serde_json::json!({ "row": cells }))?;
                }
                write_frame(writer, &serde_json::json!({ "end": true }))
            }
            other => {
                let status = json::response_status(&other);
                let body = json::response_json_body(other);
                let msg = body
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("error");
                write_error(writer, status, msg)
            }
        };
    }
    match shards.query_stream(crate::shard::ShardId(shard), stmt, STREAM_DEPTH) {
        Err(e) => write_error(writer, error_status(&e), &e.to_string()),
        Ok(rx) => {
            use crate::shard::reader_fleet::StreamMsg;
            for msg in rx {
                match msg {
                    StreamMsg::Columns(c) => {
                        write_frame(writer, &serde_json::json!({ "columns": c }))?
                    }
                    StreamMsg::Row(r) => {
                        let cells: Vec<serde_json::Value> =
                            r.iter().map(json::value_to_json).collect();
                        write_frame(writer, &serde_json::json!({ "row": cells }))?
                    }
                    StreamMsg::Done => {
                        return write_frame(writer, &serde_json::json!({ "end": true }));
                    }
                    StreamMsg::Failed(e) => return write_error(writer, 400, &e),
                }
            }
            write_frame(writer, &serde_json::json!({ "end": true }))
        }
    }
}

/// Map an op + frame to a native `Request` and its role requirement.
fn build_request(
    op: &str,
    f: &serde_json::Value,
) -> std::result::Result<(Request, Requirement), String> {
    use crate::storage::exec::Statement;
    let shard = f.get("shard").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let sql = f
        .get("sql")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let params = f
        .get("params")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    match op {
        "query_all" => Ok((
            Request::QueryAll {
                statement: Statement::new(&sql),
            },
            Requirement::Read,
        )),
        // Auto-routed: the server picks the shard(s). A read returns rows, a write the count. Its
        // permission follows its verb, matching `auth::required`.
        "run" => {
            let need = match crate::db::first_keyword(&sql).as_str() {
                "CREATE" | "DROP" | "ALTER" => Requirement::Admin,
                "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => Requirement::Write,
                _ => Requirement::Read,
            };
            Ok((
                Request::Run {
                    statement: json::statement_from(&sql, &params)?,
                },
                need,
            ))
        }
        "execute" => Ok((
            Request::Execute {
                shard,
                statements: vec![json::statement_from(&sql, &params)?],
            },
            Requirement::Write,
        )),
        "tx" => {
            let stmts = f
                .get("statements")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let mut out = Vec::with_capacity(stmts.len());
            for s in &stmts {
                let ss = s.get("sql").and_then(|v| v.as_str()).unwrap_or("");
                let p = s
                    .get("params")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                out.push(json::statement_from(ss, &p)?);
            }
            Ok((
                Request::Transaction {
                    shard,
                    statements: out,
                },
                Requirement::Write,
            ))
        }
        "execute_all" => Ok((
            Request::ExecuteAll {
                statement: Statement::new(&sql),
            },
            Requirement::Admin,
        )),
        "route" => {
            let key = f
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .as_bytes()
                .to_vec();
            Ok((Request::Route { key }, Requirement::Read))
        }
        "list_users" => Ok((Request::ListUsers, Requirement::Admin)),
        "create_user" => {
            let name = f
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let secret = f.get("secret").and_then(|v| v.as_str()).unwrap_or("");
            let role: Role = f
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .parse()
                .map_err(|e: Error| e.to_string())?;
            Ok((
                Request::CreateUser {
                    name,
                    key: auth::derive_key(secret),
                    role,
                },
                Requirement::Admin,
            ))
        }
        "drop_user" => {
            let name = f
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Ok((Request::DropUser { name }, Requirement::Admin))
        }
        _ => Err(format!("unknown op: {op}")),
    }
}

fn permits(gate: &Gate, need: Requirement) -> bool {
    match gate {
        Gate::Open => true,
        Gate::Authed(role) => role.permits(need),
        Gate::Unauthed => false,
    }
}

fn verify(auth: &super::auth::AuthConfig, name: &str, secret: &str) -> Option<Role> {
    let nonce = auth::nonce().ok()?;
    let proof = auth::prove(&auth::derive_key(secret), &nonce);
    auth.verify(name, &nonce, &proof)
}

fn error_status(e: &Error) -> u16 {
    match e {
        Error::ReaderPoolBusy | Error::WriterBusy | Error::TooManyConnections { .. } => 503,
        Error::NotLeader { .. } => 409,
        Error::Unsupported(_) => 400,
        _ => 500,
    }
}

// -- framing --

fn write_frame(w: &mut TcpStream, value: &serde_json::Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::other(
            "response frame exceeds the size limit",
        ));
    }
    let mut framed = Vec::with_capacity(4 + bytes.len());
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    w.write_all(&framed)?;
    w.flush()
}

fn write_error(w: &mut TcpStream, status: u16, message: &str) -> std::io::Result<()> {
    write_frame(
        w,
        &serde_json::json!({ "error": message, "status": status }),
    )
}

fn read_frame(r: &mut impl Read) -> std::io::Result<Option<serde_json::Value>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(std::io::Error::other(
            "request frame announces an over-limit size",
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
