//! A thread-per-connection server over a [`ShardManager`].

use std::collections::BTreeSet;
use std::io::BufReader;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::cluster::ClusterNode;
use crate::error::{Error, Result};
use crate::replication::{FrameLog, Served};
use crate::shard::{ShardId, ShardManager};
use crate::storage::exec::{Executed, Outcome};

use super::protocol::{
    PROTOCOL_VERSION, ReadConsistency, Request, Response, ShardOutcome, read_message, write_message,
};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: String,
    /// Hard cap on concurrent connections.
    ///
    /// This is the load-shedding boundary that actually binds. Each connection has one
    /// request in flight, so the bounded write queue behind them cannot fill from
    /// connections alone — refusing the connection is the honest first line, and the queue
    /// bound is the second.
    pub max_connections: usize,
    /// How long a connection may sit idle before being closed. Bounds the cost of a client
    /// that connects and then disappears without closing.
    pub idle_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4600".into(),
            max_connections: 256,
            idle_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    accepted: AtomicU64,
    refused_at_capacity: AtomicU64,
    auth_failures: AtomicU64,
    authz_refused: AtomicU64,
    requests: AtomicU64,
    errors: AtomicU64,
    abandoned_freezes: AtomicU64,
    live: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerStats {
    pub accepted: u64,
    /// Connections turned away because the cap was reached. Counted rather than merely
    /// dropped, so shedding is visible instead of looking like clients giving up.
    pub refused_at_capacity: u64,
    pub requests: u64,
    pub errors: u64,
    /// Failed authentication attempts. A number that climbs is someone guessing.
    pub auth_failures: u64,
    /// Requests refused because the authenticated role does not permit them. Counted rather
    /// than silently erroring, because a client repeatedly hitting this is misconfigured —
    /// or probing.
    pub authz_refused: u64,
    /// Snapshot freezes released because the connection holding one went away. Each is a
    /// follower that died mid-bootstrap; a node accumulating them is one whose WAL keeps
    /// being pinned by copies that never finish.
    pub abandoned_freezes: u64,
    pub live: usize,
}

/// The optional capabilities a node may have beyond serving queries.
///
/// A struct rather than four `Option` parameters: they are independent, they are all absent
/// on a standalone node, and a positional list of `None`s at every call site says nothing
/// about which is which.
#[derive(Default, Clone)]
pub struct NodeServices {
    /// Recent frames, so a follower that fell briefly behind can resume without a full
    /// bootstrap. `None` when this node is not capturing.
    pub frames: Option<Arc<FrameLog>>,
    /// Election participation.
    pub cluster: Option<Arc<ClusterNode>>,
    /// Quorum confirmation. Independent of `cluster` because a follower's subscription is
    /// what reports its position, and that works whether or not elections are running.
    pub acks: Option<Arc<crate::replication::AckTracker>>,
    /// This node's replicated copies, so it can say how far behind they are when a caller
    /// asks for a bounded staleness.
    pub follower: Option<Arc<crate::replication::Follower>>,
    /// Sends shard work to the node that owns it. Without it a client can only reach shards
    /// that happen to live on the node it connected to.
    pub router: Option<Arc<super::forward::Router>>,
    /// Who may connect and what they may do. `None` or empty means the server is open —
    /// announced loudly at bind, because an unsecured database should be a decision, not a
    /// discovery.
    pub auth: Option<Arc<super::auth::AuthConfig>>,
}

type Wrap = Arc<dyn Fn(TcpStream) -> Result<super::transport::Stream> + Send + Sync>;

pub struct Server {
    listener: TcpListener,
    shards: Arc<ShardManager>,
    services: NodeServices,
    cfg: ServerConfig,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
    /// How an accepted socket becomes a [`transport::Stream`]. Plaintext by default; TLS
    /// once [`Server::with_tls`] has supplied a certificate. The accept loop calls this and
    /// never learns which it got.
    wrap: Wrap,
}

impl Server {
    pub fn bind(shards: Arc<ShardManager>, cfg: ServerConfig) -> Result<Self> {
        Self::bind_with_frames(shards, None, cfg)
    }

    /// Bind with a frame log, making this node able to serve followers.
    ///
    /// The same `FrameLog` must be the manager's sink, or the server will serve an empty
    /// history while the frames go elsewhere.
    pub fn bind_with_frames(
        shards: Arc<ShardManager>,
        frames: Option<Arc<FrameLog>>,
        cfg: ServerConfig,
    ) -> Result<Self> {
        Self::bind_with(
            shards,
            NodeServices {
                frames,
                ..Default::default()
            },
            cfg,
        )
    }

    /// Bind with whatever this node is capable of — a full replica set member.
    pub fn bind_with(
        shards: Arc<ShardManager>,
        services: NodeServices,
        cfg: ServerConfig,
    ) -> Result<Self> {
        let listener = TcpListener::bind(&cfg.addr)
            .map_err(|e| Error::Protocol(format!("binding {}: {e}", cfg.addr)))?;
        Self::from_listener(listener, shards, services, cfg)
    }

    fn warn_if_open(services: &NodeServices) {
        if services.auth.as_ref().is_none_or(|a| a.is_empty()) {
            tracing::warn!(
                "authentication is NOT configured: this server accepts any connection and \
                 grants it everything, including the replication stream. Fine on a trusted \
                 network; a decision worth having made on any other"
            );
        }
    }

    /// Serve on a listener the caller already holds.
    ///
    /// Exists because cluster membership is circular to set up: a node needs its peers'
    /// addresses, and an address is only known once something is bound. Binding, reading the
    /// port, dropping, and rebinding leaves a window in which another process takes the port
    /// — which is not hypothetical, it is an `Address already in use` failure that showed up
    /// once in twenty-four concurrent suite runs. Handing the listener over closes it.
    pub fn from_listener(
        listener: TcpListener,
        shards: Arc<ShardManager>,
        services: NodeServices,
        cfg: ServerConfig,
    ) -> Result<Self> {
        tracing::info!(addr = %listener.local_addr().map(|a| a.to_string()).unwrap_or_default(),
            max_connections = cfg.max_connections, "listening");
        Self::warn_if_open(&services);
        Ok(Self {
            listener,
            shards,
            services,
            cfg,
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
            wrap: Arc::new(|tcp| Ok(super::transport::Stream::Plain(tcp))),
        })
    }

    /// Serve TLS. Every accepted connection is wrapped with this certificate; a client must
    /// then speak TLS or the handshake fails. This is the one call that turns encryption on —
    /// omit it and the server is plaintext, exactly as before.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls: super::transport::TlsServerConfig) -> Self {
        self.wrap = Arc::new(move |tcp| tls.accept(tcp));
        tracing::info!("TLS enabled: connections are encrypted");
        self
    }

    /// Share this server's shard manager, e.g. with an HTTP gateway on the same node.
    pub fn shards_arc(&self) -> Arc<ShardManager> {
        Arc::clone(&self.shards)
    }

    /// A clone of this server's node services, for a co-located gateway.
    pub fn services_clone(&self) -> NodeServices {
        self.services.clone()
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::Protocol(format!("local_addr: {e}")))
    }

    pub fn stats(&self) -> ServerStats {
        ServerStats {
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            refused_at_capacity: self.counters.refused_at_capacity.load(Ordering::Relaxed),
            auth_failures: self.counters.auth_failures.load(Ordering::Relaxed),
            authz_refused: self.counters.authz_refused.load(Ordering::Relaxed),
            requests: self.counters.requests.load(Ordering::Relaxed),
            errors: self.counters.errors.load(Ordering::Relaxed),
            abandoned_freezes: self.counters.abandoned_freezes.load(Ordering::Relaxed),
            live: self.counters.live.load(Ordering::Relaxed),
        }
    }

    /// A handle that stops [`Self::serve`] at the next accept.
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Accept until shutdown. Blocks.
    pub fn serve(&self) -> Result<()> {
        for stream in self.listener.incoming() {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let stream = match stream {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            };
            // Socket options go on the raw TCP before any TLS wrapping — a wrapped stream has
            // none of its own. The read timeout is this connection's idle bound.
            let _ = stream.set_read_timeout(Some(self.cfg.idle_timeout));
            let _ = stream.set_nodelay(true);

            let stream = match (self.wrap)(stream) {
                Ok(s) => s,
                Err(e) => {
                    // A failed TLS setup is this connection's problem, not the listener's.
                    tracing::debug!(error = %e, "could not wrap connection; dropping it");
                    continue;
                }
            };

            let live = self.counters.live.load(Ordering::Relaxed);
            if live >= self.cfg.max_connections {
                // Refuse loudly rather than queue: a connection accepted and then starved
                // looks like a hang to the client, while a refusal is actionable.
                self.counters
                    .refused_at_capacity
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    live,
                    limit = self.cfg.max_connections,
                    "refusing a connection: at capacity"
                );
                // Answer over the same transport the client is speaking — a plaintext error
                // to a TLS client would be an unreadable blob. `stream` is already wrapped.
                let mut stream = stream;
                let _ = write_message(
                    &mut stream,
                    &Response::Error {
                        message: Error::TooManyConnections {
                            current: live,
                            limit: self.cfg.max_connections,
                        }
                        .to_string(),
                        retryable: true,
                    },
                );
                continue;
            }

            self.counters.live.fetch_add(1, Ordering::Relaxed);
            self.counters.accepted.fetch_add(1, Ordering::Relaxed);

            let shards = Arc::clone(&self.shards);
            let services = self.services.clone();
            let counters = Arc::clone(&self.counters);
            let idle = self.cfg.idle_timeout;
            std::thread::Builder::new()
                .name("meshdb-conn".into())
                .spawn(move || {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "?".into());
                    if let Err(e) = serve_connection(stream, &shards, &services, &counters, idle) {
                        tracing::debug!(peer, error = %e, "connection ended");
                    }
                    counters.live.fetch_sub(1, Ordering::Relaxed);
                })
                .map_err(|e| Error::Protocol(format!("spawning connection thread: {e}")))?;
        }
        Ok(())
    }
}

fn serve_connection(
    stream: super::transport::Stream,
    shards: &ShardManager,
    services: &NodeServices,
    counters: &Counters,
    _idle: Duration,
) -> Result<()> {
    // Timeouts and nodelay are set on the raw socket in `serve`, before it is wrapped for
    // TLS — a `Stream` has no socket options of its own to set.
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".into());
    // One buffered stream. Reads go through the buffer; each write borrows the stream
    // beneath it via `get_mut`, since read and write never overlap in this ping-pong
    // protocol and a TLS stream cannot be split into halves.
    let mut r = BufReader::new(stream);

    /// Where this connection stands with the doorman.
    enum Gate {
        /// No authentication configured; everything is permitted.
        Open,
        /// Nothing has happened yet; only `Hello` is acceptable.
        Fresh,
        /// A challenge is outstanding; only `Auth` is acceptable.
        Challenged([u8; 32]),
        /// Proven. The role bounds every request from here on.
        Authed(super::auth::Role),
    }
    let auth_cfg = services.auth.as_ref().filter(|a| !a.is_empty()).cloned();
    let mut gate = if auth_cfg.is_some() {
        Gate::Fresh
    } else {
        Gate::Open
    };

    // Freezes this connection holds. A freeze suspends checkpointing, so one abandoned by a
    // follower that crashed mid-bootstrap would grow the WAL without bound — the connection
    // must release what it took, whatever way it ends.
    let mut held: BTreeSet<ShardId> = BTreeSet::new();

    // A client-held transaction, if one is open. Statements are *buffered* here rather than
    // applied one at a time against a held-open SQLite transaction — that would pin the
    // writer thread for the whole round trip and defeat group commit, which is exactly why
    // BEGIN used to be refused. Buffered, the writer is engaged only at COMMIT, for one
    // atomic batch, and everyone else's writes keep flowing in between. An abandoned
    // transaction (connection dropped mid-transaction) simply vanishes: nothing was applied.
    let mut txn: Option<Txn> = None;

    let result = (|| -> Result<()> {
        loop {
            // A closed or idle connection ends the loop as an error, which the caller logs
            // at debug — disconnection is ordinary, not a fault.
            let req: Request = read_message(&mut r)?;
            counters.requests.fetch_add(1, Ordering::Relaxed);

            // The doorman, before anything else looks at the request.
            if let Some(auth) = &auth_cfg {
                match &gate {
                    Gate::Open => unreachable!("auth is configured"),
                    Gate::Authed(role) => {
                        let need = super::auth::required(&req);
                        if !role.permits(need) {
                            counters.authz_refused.fetch_add(1, Ordering::Relaxed);
                            write_message(
                                r.get_mut(),
                                &Response::Error {
                                    message: format!(
                                        "not permitted: this connection is authenticated with \
                                         the {role} role, and this request requires {need:?}"
                                    ),
                                    retryable: false,
                                },
                            )?;
                            continue;
                        }
                        // Falls through to normal handling.
                    }
                    Gate::Fresh => {
                        match req {
                            Request::Hello { version, .. } => {
                                if version != PROTOCOL_VERSION {
                                    write_message(
                                        r.get_mut(),
                                        &Response::Error {
                                            message: format!(
                                                "protocol version {version} is not supported; \
                                                 this server speaks {PROTOCOL_VERSION}"
                                            ),
                                            retryable: false,
                                        },
                                    )?;
                                    continue;
                                }
                                // Fail closed: no entropy, no challenge, no connection.
                                let nonce = super::auth::nonce()?;
                                gate = Gate::Challenged(nonce);
                                write_message(r.get_mut(), &Response::Challenge { nonce })?;
                            }
                            _ => {
                                // One message for every pre-auth request, so the refusal
                                // teaches nothing about what exists.
                                write_message(
                                    r.get_mut(),
                                    &Response::Error {
                                        message: "authentication required".into(),
                                        retryable: false,
                                    },
                                )?;
                            }
                        }
                        continue;
                    }
                    Gate::Challenged(nonce) => {
                        match req {
                            Request::Auth {
                                ref name,
                                ref proof,
                            } => {
                                match auth.verify(name, nonce, proof) {
                                    Some(role) => {
                                        gate = Gate::Authed(role);
                                        write_message(
                                            r.get_mut(),
                                            &Response::Welcome {
                                                version: PROTOCOL_VERSION,
                                                shard_count: shards.shard_count(),
                                                epoch: shards.epoch(),
                                            },
                                        )?;
                                        continue;
                                    }
                                    None => {
                                        counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                                        tracing::warn!(peer, name, "authentication failed");
                                        let _ = write_message(
                                            r.get_mut(),
                                            &Response::Error {
                                                message: "authentication failed".into(),
                                                retryable: false,
                                            },
                                        );
                                        // The connection closes. Each guess costs a fresh
                                        // connection and a fresh nonce, so the handshake
                                        // cannot be hammered in place.
                                        return Ok(());
                                    }
                                }
                            }
                            _ => {
                                write_message(
                                    r.get_mut(),
                                    &Response::Error {
                                        message: "authentication required".into(),
                                        retryable: false,
                                    },
                                )?;
                                continue;
                            }
                        }
                    }
                }
            }

            // Noted before the request is consumed; applied only if it actually succeeded.
            let freeze = match &req {
                Request::SnapshotBegin { shard } => Some((ShardId(*shard), true)),
                Request::SnapshotEnd { shard } => Some((ShardId(*shard), false)),
                _ => None,
            };

            let resp = match session_step(&mut txn, req, shards, services) {
                SessionStep::Handled(r) => r,
                SessionStep::Passthrough(req) => handle(req, shards, services),
            };
            match (freeze, &resp) {
                (Some((shard, true)), Response::SnapshotInfo { .. }) => {
                    held.insert(shard);
                }
                (Some((shard, false)), Response::Ok) => {
                    held.remove(&shard);
                }
                _ => {}
            }

            if matches!(resp, Response::Error { .. }) {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
            write_message(r.get_mut(), &resp)?;
        }
    })();

    for shard in held {
        // Warned, not merely handled: an abandoned freeze means a follower died mid-copy,
        // and a node where that happens repeatedly is one whose WAL keeps being pinned.
        counters.abandoned_freezes.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            %shard,
            "connection ended still holding a snapshot freeze; releasing it so checkpointing \
             can resume"
        );
        if let Err(e) = shards.end_snapshot(shard) {
            tracing::error!(%shard, error = %e, "releasing an abandoned snapshot freeze failed");
        }
    }

    result
}

pub(crate) fn handle(req: Request, shards: &ShardManager, services: &NodeServices) -> Response {
    // A request that arrived already forwarded is handled here or refused, never passed on
    // again — that is what stops two nodes with briefly different maps bouncing it forever.
    if let Request::Direct(inner) = req {
        return handle_local(*inner, shards, services);
    }

    // A read may be answerable here even though another node owns the shard — that is the
    // whole point of the weaker levels. Decided before the generic ownership rule.
    if let Request::Query {
        shard, consistency, ..
    } = &req
    {
        let id = ShardId(*shard);
        if can_serve_read(id, *consistency, shards, services) {
            return handle_local(req, shards, services);
        }
        // This node cannot honour the level. If there is nowhere to forward to, say so
        // rather than answering from a copy that does not meet the guarantee — rows that
        // quietly break the level are worse than no rows.
        if services.router.is_none() {
            return Response::TooStale {
                shard: *shard,
                have: local_position(id, shards, services),
                need: match consistency {
                    ReadConsistency::AtLeastLsn(n) => *n,
                    _ => 0,
                },
            };
        }
    }

    // Shard-targeted work goes to the node that owns the shard.
    if let Some(router) = &services.router
        && let Some(shard) = target_shard(&req)
        && !router.is_mine(shard)
    {
        return match router.forward(shard, req) {
            // Passed back unchanged: a node that rewrote a forwarded failure as its own
            // would make every problem look local.
            Ok(response) => response,
            Err(e) => error_response(e),
        };
    }

    handle_local(req, shards, services)
}

/// How far this node's own copy of `shard` has got.
fn local_position(shard: ShardId, shards: &ShardManager, services: &NodeServices) -> u64 {
    match shards.mode(shard) {
        // Led: what this node has committed itself.
        crate::shard::mode::ShardMode::Led => shards.last_lsn(shard),
        // Followed: what it has durably applied — not what it has received.
        crate::shard::mode::ShardMode::Followed => services
            .follower
            .as_ref()
            .map(|f| f.position(shard).applied_lsn)
            .unwrap_or(0),
    }
}

/// Whether this node may answer a read itself, given the freshness asked for.
///
/// Decided by what this node **is** for the shard, not by whether a router happens to be
/// configured. An earlier version treated "no router" as "I can satisfy anything", which is
/// true for a standalone node — every shard is `Led` — and false for a replica, which then
/// answered `Linearizable` reads from a copy that was behind. The guarantee was a lie in
/// exactly the deployment where it mattered.
fn can_serve_read(
    shard: ShardId,
    consistency: ReadConsistency,
    shards: &ShardManager,
    services: &NodeServices,
) -> bool {
    match shards.mode(shard) {
        // This node writes the shard, so it holds every acknowledged write by definition.
        crate::shard::mode::ShardMode::Led => true,
        crate::shard::mode::ShardMode::Followed => {
            // Not leading a shard is not the same as replicating it. A node can be neither —
            // it simply does not hold the shard — and such a node has an empty file that
            // would answer "no such table", or worse, zero rows for a table that exists
            // elsewhere. Having applied something is the evidence that a copy exists at all.
            let have = local_position(shard, shards, services);
            if have == 0 {
                return false;
            }
            match consistency {
                // Only the leader reflects every acknowledged write.
                ReadConsistency::Linearizable => false,
                // Any copy will do — including one still catching up.
                ReadConsistency::Stale => true,
                // Only if this copy has actually reached the position asked for.
                ReadConsistency::AtLeastLsn(n) => have >= n,
            }
        }
    }
}

/// The shard a request is about, if it is about one.
fn target_shard(req: &Request) -> Option<ShardId> {
    match req {
        Request::Query { shard, .. }
        | Request::Execute { shard, .. }
        | Request::Transaction { shard, .. }
        | Request::SchemaApply { shard, .. } => Some(ShardId(*shard)),
        _ => None,
    }
}

/// An open client transaction: which shard it is on, and the writes buffered for COMMIT.
struct Txn {
    shard: u32,
    staged: Vec<crate::storage::exec::Statement>,
    bytes: usize,
}

/// A transaction cannot grow without bound in server memory. These caps bound one
/// connection's buffered writes; past them the statement is refused and the client must
/// COMMIT or ROLLBACK. Sized to hold a large batch insert comfortably while refusing a
/// runaway.
const MAX_TXN_STATEMENTS: usize = 100_000;
const MAX_TXN_BYTES: usize = 64 * 1024 * 1024;

enum SessionStep {
    /// The session handled this request; here is the reply.
    Handled(Response),
    /// Not a transaction concern; hand it to the normal request handler.
    Passthrough(Request),
}

fn refuse(message: &str) -> Response {
    Response::Error {
        message: message.to_string(),
        retryable: false,
    }
}

/// Apply transaction semantics to a request, buffering writes and flushing at COMMIT.
///
/// Returns `Passthrough` for anything that is not a transaction concern, so ordinary traffic
/// — every existing client, which never sends BEGIN — is untouched and behaves exactly as
/// before.
fn session_step(
    txn: &mut Option<Txn>,
    req: Request,
    shards: &ShardManager,
    services: &NodeServices,
) -> SessionStep {
    // Reads inside a transaction cannot see the buffered-but-unapplied writes, so answering
    // one would return a state that is a lie about this transaction. Refuse rather than
    // mislead — the project's rule everywhere else too.
    if txn.is_some() && matches!(req, Request::Query { .. } | Request::QueryAll { .. }) {
        return SessionStep::Handled(refuse(
            "reads are not supported inside a transaction: it buffers writes and applies them              atomically at COMMIT, so a read here could not see them. COMMIT first, or read              on a separate connection.",
        ));
    }

    let Request::Execute { shard, statements } = req else {
        return SessionStep::Passthrough(req);
    };

    // Nothing transactional here and none open: the common path, untouched.
    let touches_txn = statements
        .iter()
        .any(|s| is_txn_keyword(&crate::db::first_keyword(&s.sql)));
    if txn.is_none() && !touches_txn {
        return SessionStep::Passthrough(Request::Execute { shard, statements });
    }

    let mut last = Response::Ok;
    for stmt in statements {
        let kw = crate::db::first_keyword(&stmt.sql);
        match kw.as_str() {
            "BEGIN" => {
                if txn.is_some() {
                    return SessionStep::Handled(refuse(
                        "a transaction is already open on this connection; nested transactions                          are not supported",
                    ));
                }
                *txn = Some(Txn {
                    shard,
                    staged: Vec::new(),
                    bytes: 0,
                });
                last = Response::Ok;
            }
            "COMMIT" | "END" => {
                let Some(open) = txn.take() else {
                    return SessionStep::Handled(refuse("COMMIT without an open transaction"));
                };
                // The durable ack. The buffer is applied as one atomic batch through the
                // normal path — routing to the shard's owner and waiting for quorum — so the
                // COMMIT reply arrives only once the whole transaction is durable.
                last = if open.staged.is_empty() {
                    Response::Changed {
                        rows_affected: 0,
                        last_insert_rowid: 0,
                    }
                } else {
                    handle(
                        Request::Transaction {
                            shard: open.shard,
                            statements: open.staged,
                        },
                        shards,
                        services,
                    )
                };
            }
            "ROLLBACK" => {
                // Nothing was applied, so discarding the buffer is the whole of a rollback.
                *txn = None;
                last = Response::Ok;
            }
            "SAVEPOINT" | "RELEASE" => {
                return SessionStep::Handled(refuse(
                    "savepoints are not supported: a buffered transaction is applied atomically                      at COMMIT, with no intermediate points to roll back to",
                ));
            }
            _ => {
                // An ordinary statement. Decisions are taken through immutable reads so the
                // cross-shard abort can clear the transaction without a borrow conflict.
                match txn.as_ref() {
                    None => {
                        // A real statement outside any transaction, in a request that also
                        // carried a transaction keyword (unusual ordering). Apply it now, as
                        // it would be without a transaction at all.
                        last = handle(
                            Request::Execute {
                                shard,
                                statements: vec![stmt],
                            },
                            shards,
                            services,
                        );
                    }
                    Some(open) if open.shard != shard => {
                        // Cross-shard atomicity does not exist in this design, so a
                        // transaction is bound to the shard it began on. Abort rather than
                        // silently split it.
                        let began = open.shard;
                        *txn = None;
                        return SessionStep::Handled(refuse(&format!(
                            "a transaction is limited to one shard: it began on shard {began}                              but a statement targets shard {shard}. Cross-shard transactions                              are not atomic in this design and are refused rather than                              half-applied."
                        )));
                    }
                    Some(open)
                        if open.staged.len() >= MAX_TXN_STATEMENTS
                            || open.bytes + statement_size(&stmt) > MAX_TXN_BYTES =>
                    {
                        return SessionStep::Handled(refuse(
                            "transaction too large; COMMIT or ROLLBACK and use smaller batches",
                        ));
                    }
                    Some(_) => {
                        let open = txn.as_mut().expect("checked open");
                        open.bytes += statement_size(&stmt);
                        open.staged.push(stmt);
                        last = Response::Staged {
                            queued: open.staged.len() as u64,
                        };
                    }
                }
            }
        }
    }
    SessionStep::Handled(last)
}

/// A statement's approximate memory footprint, for the transaction buffer cap.
///
/// Counts the actual size of text and blob parameters — an earlier version counted a flat 16
/// bytes per parameter, so a transaction of large blobs would sail past the byte cap while the
/// counter thought it was empty, which is exactly the runaway the cap exists to stop.
fn statement_size(stmt: &crate::storage::exec::Statement) -> usize {
    use crate::storage::exec::Value;
    let params: usize = stmt
        .params
        .iter()
        .map(|v| match v {
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
            _ => 8,
        })
        .sum();
    stmt.sql.len() + params
}

/// Transaction-control keywords the session intercepts.
fn is_txn_keyword(kw: &str) -> bool {
    matches!(
        kw,
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
    )
}

fn handle_local(req: Request, shards: &ShardManager, services: &NodeServices) -> Response {
    let frames = services.frames.as_deref();
    let cluster = services.cluster.as_deref();
    match req {
        Request::Hello { version, client } => {
            if version != PROTOCOL_VERSION {
                // Say exactly what mismatched. A decode failure three messages later would
                // be far harder to diagnose.
                return Response::Error {
                    message: format!(
                        "protocol version {version} is not supported; this server speaks \
                         {PROTOCOL_VERSION}"
                    ),
                    retryable: false,
                };
            }
            tracing::debug!(client, "client connected");
            Response::Welcome {
                version: PROTOCOL_VERSION,
                shard_count: shards.config().shard_count,
                epoch: shards.epoch(),
            }
        }

        Request::Info => {
            let wc = crate::storage::wal_conversion_stats();
            Response::Info {
                shard_count: shards.config().shard_count,
                epoch: shards.epoch(),
                wal_retries: wc.retries,
                contended_opens: wc.contended_opens,
            }
        }

        Request::Route { key } => Response::Routed {
            shard: shards.route(&key).0,
        },

        Request::Query {
            shard,
            statement,
            consistency: _,
        } => match shards.query(ShardId(shard), statement) {
            Ok(o) => outcome_to_response(o),
            Err(e) => error_response(e),
        },

        Request::QueryAll { statement } => {
            // The planner only takes SQL text; parameters would have to be understood by it
            // to be pushed down safely, which it does not do yet.
            if !statement.params.is_empty() {
                return Response::Error {
                    message: "a cross-shard query cannot carry bound parameters yet; the \
                              planner does not inspect them, so it cannot prove the query \
                              is safe to fan out"
                        .into(),
                    retryable: false,
                };
            }
            match shards.query_all_shards(&statement.sql) {
                Ok(r) => Response::Rows {
                    columns: r.columns,
                    rows: r.rows,
                },
                Err(e) => error_response(e),
            }
        }

        // The COMMIT of a client transaction: all-or-nothing, and durable before it returns.
        Request::Transaction { shard, statements } => {
            match shards.execute_txn(ShardId(shard), statements) {
                Ok(outcomes) => {
                    // A single rejection voids the whole transaction — report it as an error,
                    // not a partial success, because nothing was applied.
                    if let Some(Outcome::Rejected(m)) =
                        outcomes.iter().find(|o| matches!(o, Outcome::Rejected(_)))
                    {
                        Response::Error {
                            message: m.clone(),
                            retryable: false,
                        }
                    } else {
                        let rows: u64 = outcomes
                            .iter()
                            .map(|o| match o {
                                Outcome::Ok(Executed::Changed(w)) => w.rows_affected,
                                _ => 0,
                            })
                            .sum();
                        let last = outcomes
                            .iter()
                            .rev()
                            .find_map(|o| match o {
                                Outcome::Ok(Executed::Changed(w)) => Some(w.last_insert_rowid),
                                _ => None,
                            })
                            .unwrap_or(0);
                        Response::Changed {
                            rows_affected: rows,
                            last_insert_rowid: last,
                        }
                    }
                }
                Err(e) => error_response(e),
            }
        }

        Request::Execute { shard, statements } => {
            match shards.execute(ShardId(shard), statements) {
                Ok(outcomes) => {
                    // Report the first rejection rather than the last success, so a caller
                    // is never told a batch succeeded when part of it did not.
                    for o in &outcomes {
                        if let Outcome::Rejected(m) = o {
                            return Response::Rejected { message: m.clone() };
                        }
                    }
                    outcomes
                        .into_iter()
                        .next_back()
                        .map(outcome_to_response)
                        .unwrap_or(Response::Changed {
                            rows_affected: 0,
                            last_insert_rowid: 0,
                        })
                }
                Err(e) => error_response(e),
            }
        }

        // A cluster-wide schema change. Each shard's own owner applies it, so no node needs
        // to hold every shard — which under placement no node does. The roll is sequential
        // and its per-shard results are reported individually, because there is no atomicity
        // across shards and pretending otherwise would hide a partial application.
        Request::ExecuteAll { statement } => {
            let mut outcomes = Vec::new();
            for s in 0..shards.shard_count() {
                let shard = ShardId(s);
                let local = services.router.as_ref().is_none_or(|r| r.is_mine(shard));
                let outcome = if local {
                    match shards.apply_ddl_to(shard, statement.clone()) {
                        Ok(_) => ShardOutcome::Ok,
                        Err(e) => ShardOutcome::Rejected(e.to_string()),
                    }
                } else {
                    let router = services.router.as_ref().expect("checked above");
                    match router.forward(
                        shard,
                        Request::SchemaApply {
                            shard: s,
                            ddl: statement.clone(),
                        },
                    ) {
                        Ok(Response::SchemaVersion { .. }) => ShardOutcome::Ok,
                        Ok(Response::Error { message, .. })
                        | Ok(Response::Rejected { message }) => ShardOutcome::Rejected(message),
                        Ok(other) => {
                            ShardOutcome::Rejected(format!("unexpected response: {other:?}"))
                        }
                        Err(e) => ShardOutcome::Rejected(e.to_string()),
                    }
                };
                outcomes.push((s, outcome));
            }
            Response::AllShards { outcomes }
        }

        Request::Subscribe {
            node,
            shard,
            epoch,
            from_lsn,
            max_txns,
        } => {
            // The request *is* the acknowledgement: asking from `from_lsn` is proof this
            // follower holds everything below it. Recording it here, before serving, is what
            // releases writers waiting for a quorum — there is no separate ack message to be
            // lost, reordered, or forgotten.
            if node != 0
                && from_lsn > 0
                && let Some(acks) = &services.acks
            {
                acks.record(node, ShardId(shard), from_lsn - 1);
            }
            serve_subscribe(
                shards,
                frames,
                ShardId(shard),
                epoch,
                from_lsn,
                max_txns as usize,
            )
        }

        Request::SnapshotBegin { shard } => match shards.begin_snapshot(ShardId(shard)) {
            Ok((epoch, lsn, path)) => match std::fs::metadata(&path) {
                Ok(m) => Response::SnapshotInfo {
                    shard,
                    epoch,
                    lsn,
                    total_bytes: m.len(),
                },
                Err(e) => {
                    // Release the freeze we just took, or checkpointing stays suspended for
                    // a snapshot nobody is going to read.
                    let _ = shards.end_snapshot(ShardId(shard));
                    error_response(Error::Protocol(format!("sizing snapshot: {e}")))
                }
            },
            Err(e) => error_response(e),
        },

        Request::SnapshotRead { shard, offset, len } => {
            match read_snapshot_chunk(shards, ShardId(shard), offset, len as usize) {
                Ok(data) => Response::SnapshotChunk { data },
                Err(e) => error_response(e),
            }
        }

        Request::SnapshotEnd { shard } => match shards.end_snapshot(ShardId(shard)) {
            Ok(true) => Response::Ok,
            Ok(false) => Response::Error {
                message: format!(
                    "the snapshot of shard_{shard} was invalidated while it was being read;                      retake it"
                ),
                retryable: true,
            },
            Err(e) => error_response(e),
        },

        Request::SchemaApply { shard, ddl } => match shards.apply_ddl_to(ShardId(shard), ddl) {
            Ok(version) => Response::SchemaVersion { shard, version },
            Err(e) => error_response(e),
        },

        // Unwrapped above; reaching here would mean a nested wrapper, which nothing sends.
        Request::Direct(inner) => handle_local(*inner, shards, services),

        // Answered inside the connection's handshake when authentication is configured;
        // reaching here means it is not.
        Request::Auth { .. } => Response::Error {
            message: "authentication is not enabled on this server".into(),
            retryable: false,
        },

        // User management. Reaching here means the connection is already an Admin (the
        // requirement map guarantees it), so what remains is the store and the one extra rule.
        Request::CreateUser { name, key, role } => match &services.auth {
            None => Response::Error {
                message: "authentication is not enabled on this server, so there are no users                           to manage"
                    .into(),
                retryable: false,
            },
            Some(auth) => {
                // An admin is a client; the cluster role is for machines. Letting an admin
                // mint a cluster credential would tunnel straight through the wall between the
                // two — the whole point of keeping Cluster off the client ladder.
                if role == super::auth::Role::Cluster {
                    Response::Error {
                        message: "an admin may not create a cluster user; cluster credentials                                   are provisioned at deploy time, not over the wire"
                            .into(),
                        retryable: false,
                    }
                } else {
                    match auth.create(&name, key, role) {
                        Ok(()) => Response::Ok,
                        Err(e) => error_response(e),
                    }
                }
            }
        },

        Request::DropUser { name } => match &services.auth {
            None => Response::Error {
                message: "authentication is not enabled on this server".into(),
                retryable: false,
            },
            Some(auth) => match auth.drop_user(&name) {
                Ok(true) => Response::Ok,
                Ok(false) => Response::Error {
                    message: format!("no such user: {name}"),
                    retryable: false,
                },
                Err(e) => error_response(e),
            },
        },

        Request::ListUsers => match &services.auth {
            None => Response::Error {
                message: "authentication is not enabled on this server".into(),
                retryable: false,
            },
            Some(auth) => Response::Users {
                users: auth.list(),
            },
        },

        Request::Vote(req) => match cluster {
            Some(c) => match c.handle_vote_request(&req) {
                Ok(reply) => Response::Voted(reply),
                Err(e) => error_response(e),
            },
            None => not_a_member("a vote request"),
        },

        Request::Beat(hb) => match cluster {
            Some(c) => match c.handle_heartbeat(&hb) {
                Ok(reply) => Response::Beat(reply),
                Err(e) => error_response(e),
            },
            None => not_a_member("a heartbeat"),
        },
    }
}

/// A standalone node must say plainly that it is not a cluster member.
///
/// Answering a vote request with a generic error, or worse with silence, would look to the
/// candidate exactly like an unreachable peer — and a node misconfigured out of its own
/// cluster would then present as a network fault forever.
fn not_a_member(what: &str) -> Response {
    Response::Error {
        message: format!(
            "this node is not a cluster member, so it cannot answer {what}; it was started \
             without cluster configuration"
        ),
        retryable: false,
    }
}

fn read_snapshot_chunk(
    shards: &ShardManager,
    shard: ShardId,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    // Bounded so one request cannot ask for a reply larger than the frame limit.
    let len = len.min(super::protocol::MAX_FRAME_BYTES / 2);
    let path = shard.path(shards.dir());
    let mut f = std::fs::File::open(&path)
        .map_err(|e| Error::Protocol(format!("opening {}: {e}", path.display())))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| Error::Protocol(format!("seeking to {offset}: {e}")))?;

    let mut buf = vec![0u8; len];
    let mut got = 0;
    while got < len {
        match f.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => return Err(Error::Protocol(format!("reading snapshot: {e}"))),
        }
    }
    buf.truncate(got);
    Ok(buf)
}

/// Serve a follower asking for frames from a position.
///
/// This is the pull half of replication: a follower that falls behind asks for what it
/// needs instead of waiting to be pushed at. What it cannot do yet is retain history — the
/// primary only holds frames until its sink consumes them — so a request for anything other
/// than the live position is answered with `NeedsBootstrap` rather than silently serving a
/// gap.
fn serve_subscribe(
    shards: &ShardManager,
    frames: Option<&FrameLog>,
    shard: ShardId,
    epoch: u64,
    from_lsn: u64,
    max_txns: usize,
) -> Response {
    let Some(current_epoch) = shards.epoch() else {
        return Response::Error {
            message: "this node is not capturing frames, so it has no stream to subscribe to \
                      (capture is off)"
                .into(),
            retryable: false,
        };
    };

    // Epoch 0 is "I hold no copy and claim no generation" — a fresh follower. It is not a
    // mismatch, so it is not refused here; whether it can be served depends only on whether
    // the frames it needs are still retained, which the frame log answers below. Refusing it
    // outright would force a snapshot even when the whole history is still in hand.
    if epoch != 0 && epoch != current_epoch {
        return Response::NeedsBootstrap {
            shard: shard.0,
            reason: format!(
                "follower is on epoch {epoch}, this node is on {current_epoch}; a new epoch \
                 is a new stream whose positions cannot be compared to the old ones"
            ),
        };
    }

    let Some(log) = frames else {
        return Response::Error {
            message: "this node has no frame log, so it cannot serve followers".into(),
            retryable: false,
        };
    };

    match log.serve(shard, from_lsn, max_txns.clamp(1, 512)) {
        Served::Frames(txns) => Response::Frames {
            shard: shard.0,
            epoch: current_epoch,
            txns,
        },
        Served::UpToDate => Response::UpToDate { shard: shard.0 },
        // Retention is bounded, so history the follower needs may already be gone. Sending
        // what *is* retained would leave a hole the follower could not see; saying "too old"
        // sends it to bootstrap instead.
        Served::TooOld { lowest_retained } => Response::NeedsBootstrap {
            shard: shard.0,
            reason: format!(
                "frames before LSN {lowest_retained} are no longer retained; take a snapshot \
                 and resume from it"
            ),
        },
    }
}

fn outcome_to_response(o: Outcome) -> Response {
    match o {
        Outcome::Ok(Executed::Rows(r)) => Response::Rows {
            columns: r.columns,
            rows: r.rows,
        },
        Outcome::Ok(Executed::Changed(w)) => Response::Changed {
            rows_affected: w.rows_affected,
            last_insert_rowid: w.last_insert_rowid,
        },
        Outcome::Rejected(m) => Response::Rejected { message: m },
    }
}

/// Distinguish backpressure from a real fault, so a client knows whether retrying helps.
fn error_response(e: Error) -> Response {
    let retryable = matches!(
        e,
        Error::WriterBusy | Error::ReaderPoolBusy | Error::TooManyConnections { .. }
    );
    if !retryable {
        tracing::warn!(error = %e, "request failed");
    }
    Response::Error {
        message: e.to_string(),
        retryable,
    }
}
