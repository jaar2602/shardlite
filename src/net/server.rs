//! A thread-per-connection server over a [`ShardManager`].

use std::collections::BTreeSet;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::replication::{FrameLog, Served};
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
    /// Snapshot freezes released because the connection holding one went away. Each is a
    /// follower that died mid-bootstrap; a node accumulating them is one whose WAL keeps
    /// being pinned by copies that never finish.
    pub abandoned_freezes: u64,
    pub live: usize,
}

pub struct Server {
    listener: TcpListener,
    shards: Arc<ShardManager>,
    /// Recent frames, so a follower that fell briefly behind can resume without a full
    /// bootstrap. `None` when this node is not capturing.
    frames: Option<Arc<FrameLog>>,
    cfg: ServerConfig,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
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
        let listener = TcpListener::bind(&cfg.addr)
            .map_err(|e| Error::Protocol(format!("binding {}: {e}", cfg.addr)))?;
        tracing::info!(addr = %listener.local_addr().map(|a| a.to_string()).unwrap_or_default(),
            max_connections = cfg.max_connections, "listening");
        Ok(Self {
            listener,
            shards,
            frames,
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
            let frames = self.frames.clone();
            let counters = Arc::clone(&self.counters);
            let idle = self.cfg.idle_timeout;
            std::thread::Builder::new()
                .name("meshdb-conn".into())
                .spawn(move || {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "?".into());
                    if let Err(e) =
                        serve_connection(stream, &shards, frames.as_deref(), &counters, idle)
                    {
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
    frames: Option<&FrameLog>,
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

    // Freezes this connection holds. A freeze suspends checkpointing, so one abandoned by a
    // follower that crashed mid-bootstrap would grow the WAL without bound — the connection
    // must release what it took, whatever way it ends.
    let mut held: BTreeSet<ShardId> = BTreeSet::new();

    let result = (|| -> Result<()> {
        loop {
            // A closed or idle connection ends the loop as an error, which the caller logs
            // at debug — disconnection is ordinary, not a fault.
            let req: Request = read_message(&mut r)?;
            counters.requests.fetch_add(1, Ordering::Relaxed);

            // Noted before the request is consumed; applied only if it actually succeeded.
            let freeze = match &req {
                Request::SnapshotBegin { shard } => Some((ShardId(*shard), true)),
                Request::SnapshotEnd { shard } => Some((ShardId(*shard), false)),
                _ => None,
            };

            let resp = handle(req, shards, frames);
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
            write_message(&mut w, &resp)?;
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

fn handle(req: Request, shards: &ShardManager, frames: Option<&FrameLog>) -> Response {
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
            max_txns,
        } => serve_subscribe(
            shards,
            frames,
            ShardId(shard),
            epoch,
            from_lsn,
            max_txns as usize,
        ),

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
