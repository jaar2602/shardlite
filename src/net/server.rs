//! A thread-per-connection server over a [`ShardManager`].

use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::shard::{ShardId, ShardManager};
use crate::storage::exec::{Executed, Outcome};

use super::protocol::{
    PROTOCOL_VERSION, Request, Response, ShardOutcome, read_message, write_message,
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
    requests: AtomicU64,
    errors: AtomicU64,
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
    pub live: usize,
}

pub struct Server {
    listener: TcpListener,
    shards: Arc<ShardManager>,
    cfg: ServerConfig,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    pub fn bind(shards: Arc<ShardManager>, cfg: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&cfg.addr)
            .map_err(|e| Error::Protocol(format!("binding {}: {e}", cfg.addr)))?;
        tracing::info!(addr = %listener.local_addr().map(|a| a.to_string()).unwrap_or_default(),
            max_connections = cfg.max_connections, "listening");
        Ok(Self {
            listener,
            shards,
            cfg,
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
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
            requests: self.counters.requests.load(Ordering::Relaxed),
            errors: self.counters.errors.load(Ordering::Relaxed),
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
                let mut w = BufWriter::new(&stream);
                let _ = write_message(
                    &mut w,
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
            let counters = Arc::clone(&self.counters);
            let idle = self.cfg.idle_timeout;
            std::thread::Builder::new()
                .name("meshdb-conn".into())
                .spawn(move || {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "?".into());
                    if let Err(e) = serve_connection(stream, &shards, &counters, idle) {
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
    stream: TcpStream,
    shards: &ShardManager,
    counters: &Counters,
    idle: Duration,
) -> Result<()> {
    stream
        .set_read_timeout(Some(idle))
        .map_err(|e| Error::Protocol(format!("set_read_timeout: {e}")))?;
    stream
        .set_nodelay(true)
        .map_err(|e| Error::Protocol(format!("set_nodelay: {e}")))?;

    let mut r = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| Error::Protocol(format!("cloning stream: {e}")))?,
    );
    let mut w = BufWriter::new(stream);

    loop {
        // A closed or idle connection ends the loop as an error, which the caller logs at
        // debug — disconnection is ordinary, not a fault.
        let req: Request = read_message(&mut r)?;
        counters.requests.fetch_add(1, Ordering::Relaxed);

        let resp = handle(req, shards);
        if matches!(resp, Response::Error { .. }) {
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        write_message(&mut w, &resp)?;
    }
}

fn handle(req: Request, shards: &ShardManager) -> Response {
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

        Request::Query { shard, statement } => match shards.query(ShardId(shard), statement) {
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

        Request::ExecuteAll { statement } => match shards.execute_all_shards(statement) {
            Ok(results) => Response::AllShards {
                outcomes: results
                    .into_iter()
                    .map(|(id, o)| {
                        (
                            id.0,
                            match o {
                                Outcome::Rejected(m) => ShardOutcome::Rejected(m),
                                Outcome::Ok(_) => ShardOutcome::Ok,
                            },
                        )
                    })
                    .collect(),
            },
            Err(e) => error_response(e),
        },

        Request::Subscribe {
            shard,
            epoch,
            from_lsn,
        } => serve_subscribe(shards, ShardId(shard), epoch, from_lsn),
    }
}

/// Serve a follower asking for frames from a position.
///
/// This is the pull half of replication: a follower that falls behind asks for what it
/// needs instead of waiting to be pushed at. What it cannot do yet is retain history — the
/// primary only holds frames until its sink consumes them — so a request for anything other
/// than the live position is answered with `NeedsBootstrap` rather than silently serving a
/// gap.
fn serve_subscribe(shards: &ShardManager, shard: ShardId, epoch: u64, from_lsn: u64) -> Response {
    let Some(current_epoch) = shards.epoch() else {
        return Response::Error {
            message: "this node is not capturing frames, so it has no stream to subscribe to \
                      (capture is off)"
                .into(),
            retryable: false,
        };
    };

    if epoch != current_epoch {
        return Response::NeedsBootstrap {
            shard: shard.0,
            reason: format!(
                "follower is on epoch {epoch}, this node is on {current_epoch}; a new epoch \
                 is a new stream whose positions cannot be compared to the old ones"
            ),
        };
    }

    let last = shards.last_lsn(shard);
    if from_lsn > last + 1 {
        return Response::NeedsBootstrap {
            shard: shard.0,
            reason: format!(
                "follower asked for LSN {from_lsn} but this node has only reached {last}"
            ),
        };
    }

    // Frames are not retained after the sink consumes them, so anything older than the live
    // position cannot be replayed. Saying so beats serving a partial answer.
    if from_lsn <= last {
        return Response::NeedsBootstrap {
            shard: shard.0,
            reason: format!(
                "frames before LSN {} are no longer retained; take a snapshot and resume \
                 from it",
                last + 1
            ),
        };
    }

    // Caught up: nothing new since `last`. An empty batch is the honest answer to a poll,
    // and is distinguishable from `NeedsBootstrap` precisely because the two are different
    // situations.
    Response::Frames {
        shard: shard.0,
        epoch: current_epoch,
        txns: Vec::new(),
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
