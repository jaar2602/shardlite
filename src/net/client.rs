//! A blocking client.

use std::io::BufReader;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::storage::exec::{QueryResult, Statement, Value};

use super::protocol::{
    PROTOCOL_VERSION, Request, Response, ShardOutcome, read_message, write_message,
};

/// A client-held transaction on one shard.
///
/// Writes buffer on the server; [`Self::commit`] applies them atomically and returns the
/// durable acknowledgement. Dropping without committing rolls back — nothing was applied, so
/// rollback is simply discarding the buffer.
/// The result of a routed [`Client::run`]: rows from a read, or the count from a write.
#[derive(Debug, Clone, PartialEq)]
pub enum RunResult {
    Rows(QueryResult),
    Changed {
        rows_affected: u64,
        last_insert_rowid: i64,
    },
}

pub struct Transaction<'a> {
    client: &'a mut Client,
    shard: u32,
    finished: bool,
}

impl Transaction<'_> {
    /// Buffer a write into the transaction. Returns how many statements it now holds. The
    /// write is not durable — nor even applied — until [`Self::commit`].
    pub fn execute(&mut self, sql: impl Into<Statement>) -> Result<u64> {
        match self.client.round_trip(Request::Execute {
            shard: self.shard,
            statements: vec![sql.into()],
        })? {
            Response::Staged { queued } => Ok(queued),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// Apply the whole transaction atomically and wait for it to become durable.
    ///
    /// Returns `(rows_affected, last_insert_rowid)` for the batch. This is the durable ack:
    /// it arrives only after a quorum holds the write.
    pub fn commit(mut self) -> Result<(u64, i64)> {
        self.finished = true;
        match self.client.round_trip(Request::Execute {
            shard: self.shard,
            statements: vec![Statement::new("COMMIT")],
        })? {
            Response::Changed {
                rows_affected,
                last_insert_rowid,
            } => Ok((rows_affected, last_insert_rowid)),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// Send a raw request on the transaction's connection. For tests that need to probe the
    /// server's in-transaction behaviour directly.
    #[doc(hidden)]
    pub fn raw(&mut self, req: Request) -> Result<Response> {
        self.client.round_trip(req)
    }

    /// Discard the transaction. Nothing was applied, so this only drops the server's buffer.
    pub fn rollback(mut self) -> Result<()> {
        self.finished = true;
        match self.client.round_trip(Request::Execute {
            shard: self.shard,
            statements: vec![Statement::new("ROLLBACK")],
        })? {
            Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // A transaction dropped without commit or rollback — a `?` early-return, a panic —
        // must not linger on the server holding a buffer. Best-effort rollback; nothing was
        // applied, so there is nothing to undo, only a buffer to free.
        if !self.finished {
            let _ = self.client.round_trip(Request::Execute {
                shard: self.shard,
                statements: vec![Statement::new("ROLLBACK")],
            });
        }
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("shard_count", &self.shard_count)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

pub struct Client {
    // One buffered stream, not a split reader/writer pair. The protocol is strict
    // request-then-response, so a single connection carries both directions — and a TLS
    // stream cannot be split into independent halves anyway. Reads go through the buffer;
    // writes go straight to the stream beneath it via `get_mut`, and `write_message` already
    // frames each message into one write, so a write buffer would buy nothing.
    conn: BufReader<super::transport::Stream>,
    shard_count: u32,
    epoch: Option<u64>,
}

impl Client {
    pub fn connect(addr: &str) -> Result<Self> {
        Self::connect_with(addr, Duration::from_secs(30))
    }

    /// Connect and authenticate as `name`.
    ///
    /// The secret never crosses the wire: the server sends a fresh nonce and this answers
    /// with a keyed hash over it. See `net::auth` for what that does and does not protect.
    pub fn connect_as(addr: &str, name: &str, secret: &str) -> Result<Self> {
        let t = Duration::from_secs(30);
        Self::connect_full(
            addr,
            t,
            t,
            Some((name.to_string(), super::auth::derive_key(secret))),
        )
    }

    pub fn connect_with(addr: &str, timeout: Duration) -> Result<Self> {
        Self::connect_bounded(addr, timeout, timeout)
    }

    /// Connect with an explicit bound on both the TCP handshake and every subsequent I/O.
    ///
    /// The default timeouts are sized for clients, where waiting is better than failing. They
    /// are actively wrong for the cluster loop: a peer that is *hung* rather than crashed —
    /// backlogged, paused, half-partitioned — accepts the connection and then never answers,
    /// and a 30 second read would freeze the election loop for 30 seconds. A leader frozen
    /// that long never evaluates its lease, so it never steps down, and it keeps its write
    /// gate open the whole time. Bounding both waits well below the election timeout is what
    /// makes an unresponsive peer indistinguishable from a dead one, which is the only way the
    /// lease can do its job.
    pub fn connect_bounded(addr: &str, connect: Duration, io: Duration) -> Result<Self> {
        Self::connect_full(addr, connect, io, None)
    }

    /// Resolve, connect, and apply the timeouts and nodelay every connection needs.
    fn connect_tcp(addr: &str, connect: Duration, io: Duration) -> Result<TcpStream> {
        if crate::failpoint::active("network.outbound") {
            return Err(Error::Busy(format!(
                "injected outbound network partition blocks {addr}"
            )));
        }
        let resolved = addr
            .to_socket_addrs()
            .map_err(|e| Error::Protocol(format!("resolving {addr}: {e}")))?
            .next()
            .ok_or_else(|| Error::Protocol(format!("{addr} resolved to no address")))?;
        let stream = TcpStream::connect_timeout(&resolved, connect)
            .map_err(|e| Error::Protocol(format!("connecting to {addr}: {e}")))?;
        stream
            .set_read_timeout(Some(io))
            .map_err(|e| Error::Protocol(format!("set_read_timeout: {e}")))?;
        // Without this a peer that stops reading blocks the sender in `write` instead, which
        // freezes the loop just as thoroughly as a missing read timeout.
        stream
            .set_write_timeout(Some(io))
            .map_err(|e| Error::Protocol(format!("set_write_timeout: {e}")))?;
        stream
            .set_nodelay(true)
            .map_err(|e| Error::Protocol(format!("set_nodelay: {e}")))?;
        Ok(stream)
    }

    /// Connect over TLS, verifying the server per `tls`, then authenticate as `credentials`.
    ///
    /// This is how a client gets encryption. Which verification applies — a real CA or the
    /// dangerous accept-any mode — is entirely the `tls` config's business; see
    /// `net::transport`.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        addr: &str,
        connect: Duration,
        io: Duration,
        credentials: Option<(String, super::auth::Key)>,
        tls: &super::transport::TlsClientConfig,
    ) -> Result<Self> {
        let tcp = Self::connect_tcp(addr, connect, io)?;
        let stream = tls.connect(tcp)?;
        Self::from_stream(stream, credentials)
    }

    /// The full-fat constructor: bounded waits plus optional credentials.
    pub fn connect_full(
        addr: &str,
        connect: Duration,
        io: Duration,
        credentials: Option<(String, super::auth::Key)>,
    ) -> Result<Self> {
        let tcp = Self::connect_tcp(addr, connect, io)?;
        Self::from_stream(super::transport::Stream::Plain(tcp), credentials)
    }

    /// Run the handshake over an established stream, plaintext or TLS alike.
    fn from_stream(
        stream: super::transport::Stream,
        credentials: Option<(String, super::auth::Key)>,
    ) -> Result<Self> {
        let mut me = Self {
            conn: BufReader::new(stream),
            shard_count: 0,
            epoch: None,
        };

        match me.round_trip(Request::Hello {
            version: PROTOCOL_VERSION,
            client: format!("shardlite-client/{}", env!("CARGO_PKG_VERSION")),
        })? {
            Response::Welcome {
                shard_count, epoch, ..
            } => {
                me.shard_count = shard_count;
                me.epoch = epoch;
                Ok(me)
            }
            // The server wants proof. Answer the nonce, or say plainly that credentials are
            // needed — a bare "unexpected response" would send someone digging through
            // protocol code for what is a configuration matter.
            Response::Challenge { nonce } => {
                let Some((name, key)) = credentials else {
                    return Err(Error::Protocol(
                        "this server requires authentication; connect with credentials \
                         (Client::connect_as)"
                            .into(),
                    ));
                };
                let proof = super::auth::prove(&key, &nonce);
                match me.round_trip(Request::Auth { name, proof })? {
                    Response::Welcome {
                        shard_count, epoch, ..
                    } => {
                        me.shard_count = shard_count;
                        me.epoch = epoch;
                        Ok(me)
                    }
                    Response::Error { message, .. } => Err(Error::Protocol(message)),
                    other => Err(Error::Protocol(format!(
                        "expected a welcome after authenticating, got {other:?}"
                    ))),
                }
            }
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(Error::Protocol(format!(
                "expected a welcome, got {other:?}"
            ))),
        }
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// Which shard a key belongs to. Asked of the server so a client never has to reimplement
    /// the routing hash — a client-side copy that drifted would silently misroute every key.
    pub fn route(&mut self, key: &[u8]) -> Result<u32> {
        match self.round_trip(Request::Route { key: key.to_vec() })? {
            Response::Routed { shard } => Ok(shard),
            other => Err(unexpected(other)),
        }
    }

    /// Read a shard at the strongest level. See [`Self::query_with`] for a weaker one.
    pub fn query(&mut self, shard: u32, sql: impl Into<Statement>) -> Result<QueryResult> {
        self.query_with(shard, sql, super::protocol::ReadConsistency::default())
    }

    /// Read a shard at a chosen freshness.
    ///
    /// `Stale` and `AtLeastLsn` can be served by a follower, which is what spreads reads off
    /// the leader. `Linearizable` always goes to the leader.
    pub fn query_with(
        &mut self,
        shard: u32,
        sql: impl Into<Statement>,
        consistency: super::protocol::ReadConsistency,
    ) -> Result<QueryResult> {
        match self.round_trip(Request::Query {
            consistency,
            shard,
            statement: sql.into(),
        })? {
            Response::Rows { columns, rows } => Ok(QueryResult { columns, rows }),
            other => Err(unexpected(other)),
        }
    }

    /// A read across every shard, merged by the server's planner.
    pub fn query_all(&mut self, sql: &str) -> Result<QueryResult> {
        match self.round_trip(Request::QueryAll {
            statement: Statement::new(sql),
        })? {
            Response::Rows { columns, rows } => Ok(QueryResult { columns, rows }),
            other => Err(unexpected(other)),
        }
    }

    /// Run one statement without naming a shard — the server routes it by the table's shard key.
    /// This is the "just connect and run SQL" path: a read returns rows, a write returns the count
    /// affected, and the caller never has to know the cluster is sharded. DDL, a keyed write, a
    /// point read, a fan-out read and a multi-shard write are all handled server-side.
    pub fn run(&mut self, sql: &str) -> Result<RunResult> {
        match self.round_trip(Request::Run {
            statement: Statement::new(sql),
        })? {
            Response::Rows { columns, rows } => Ok(RunResult::Rows(QueryResult { columns, rows })),
            Response::Changed {
                rows_affected,
                last_insert_rowid,
            } => Ok(RunResult::Changed {
                rows_affected,
                last_insert_rowid,
            }),
            other => Err(unexpected(other)),
        }
    }

    /// Write to one shard. Returns `(rows_affected, last_insert_rowid)`.
    pub fn execute(&mut self, shard: u32, sql: impl Into<Statement>) -> Result<(u64, i64)> {
        self.execute_batch(shard, vec![sql.into()])
    }

    pub fn execute_batch(&mut self, shard: u32, statements: Vec<Statement>) -> Result<(u64, i64)> {
        match self.round_trip(Request::Execute { shard, statements })? {
            Response::Changed {
                rows_affected,
                last_insert_rowid,
            } => Ok((rows_affected, last_insert_rowid)),
            Response::Rows { .. } => Ok((0, 0)),
            other => Err(unexpected(other)),
        }
    }

    /// Apply a statement to every shard, as DDL requires.
    ///
    /// Returns per-shard outcomes rather than a single verdict: there is no atomicity across
    /// shards, so a partial failure has to be visible as one.
    pub fn execute_all(&mut self, sql: &str) -> Result<Vec<(u32, ShardOutcome)>> {
        match self.round_trip(Request::ExecuteAll {
            statement: Statement::new(sql),
        })? {
            Response::AllShards { outcomes } => Ok(outcomes),
            other => Err(unexpected(other)),
        }
    }

    /// Write a key's row to whichever shard the key routes to.
    pub fn put(&mut self, key: &[u8], sql: &str, params: Vec<Value>) -> Result<(u64, i64)> {
        let shard = self.route(key)?;
        self.execute(shard, Statement::with_params(sql, params))
    }

    /// Begin a transaction on `shard`.
    ///
    /// Statements run through the returned [`Transaction`] are buffered on the server and
    /// applied as one atomic batch at [`Transaction::commit`], which returns only once the
    /// whole transaction is durable (quorum-acknowledged). Nothing is applied until then, so
    /// the writer is never pinned across your think-time — and an abandoned transaction
    /// simply vanishes.
    ///
    /// A transaction is limited to one shard: there is no atomic commit across shards in this
    /// design.
    pub fn begin(&mut self, shard: u32) -> Result<Transaction<'_>> {
        match self.round_trip(Request::Execute {
            shard,
            statements: vec![Statement::new("BEGIN")],
        })? {
            Response::Ok => Ok(Transaction {
                client: self,
                shard,
                finished: false,
            }),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// Create or replace a user at runtime. The secret is hashed here — only the derived key
    /// crosses the wire, so the plaintext never leaves this machine. Requires admin
    /// credentials on this connection.
    ///
    /// The key still grants access, so run user management over TLS or a trusted network.
    pub fn create_user(&mut self, name: &str, secret: &str, role: super::auth::Role) -> Result<()> {
        match self.round_trip(Request::CreateUser {
            name: name.to_string(),
            key: super::auth::derive_key(secret),
            role,
        })? {
            Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// Remove a user at runtime. Requires admin credentials.
    pub fn drop_user(&mut self, name: &str) -> Result<()> {
        match self.round_trip(Request::DropUser {
            name: name.to_string(),
        })? {
            Response::Ok => Ok(()),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// List users (names and roles, never keys). Requires admin credentials.
    pub fn list_users(&mut self) -> Result<Vec<(String, super::auth::Role)>> {
        match self.round_trip(Request::ListUsers)? {
            Response::Users { users } => Ok(users),
            Response::Error { message, .. } => Err(Error::Protocol(message)),
            other => Err(unexpected(other)),
        }
    }

    /// Send a request and return the raw response.
    ///
    /// The escape hatch for verbs outside the ordinary client surface — subscription and
    /// snapshot transfer, used by [`super::replica::Replica`].
    pub fn request(&mut self, req: Request) -> Result<Response> {
        self.round_trip(req)
    }

    /// Ask a peer for its vote.
    pub(crate) fn request_vote(
        &mut self,
        req: &crate::cluster::VoteRequest,
    ) -> Result<crate::cluster::VoteReply> {
        match self.round_trip(Request::Vote(req.clone()))? {
            Response::Voted(r) => Ok(r),
            other => Err(Error::Protocol(format!(
                "unexpected response to a vote request: {other:?}"
            ))),
        }
    }

    /// Assert leadership to a peer and renew the lease.
    pub(crate) fn heartbeat(
        &mut self,
        hb: &crate::cluster::Heartbeat,
    ) -> Result<crate::cluster::HeartbeatReply> {
        match self.round_trip(Request::Beat(hb.clone()))? {
            Response::Beat(r) => Ok(r),
            other => Err(Error::Protocol(format!(
                "unexpected response to a heartbeat: {other:?}"
            ))),
        }
    }

    pub fn catalog_state(
        &mut self,
    ) -> Result<(
        crate::cluster::Catalog,
        Option<crate::cluster::CatalogProposal>,
    )> {
        match self.round_trip(Request::CatalogGet)? {
            Response::CatalogState {
                committed,
                prepared,
            } => Ok((committed, prepared)),
            other => Err(Error::Protocol(format!(
                "unexpected response to a catalog read: {other:?}"
            ))),
        }
    }

    pub fn catalog_prepare(
        &mut self,
        leader: u64,
        proposal: &crate::cluster::CatalogProposal,
    ) -> Result<()> {
        match self.round_trip(Request::CatalogPrepare {
            leader,
            proposal: proposal.clone(),
        })? {
            Response::CatalogPrepared { term, version }
                if term == proposal.term && version == proposal.catalog.version =>
            {
                Ok(())
            }
            other => Err(Error::Protocol(format!(
                "unexpected response to catalog prepare: {other:?}"
            ))),
        }
    }

    pub fn catalog_commit(&mut self, leader: u64, term: u64, version: u64) -> Result<()> {
        match self.round_trip(Request::CatalogCommit {
            leader,
            term,
            version,
        })? {
            Response::CatalogCommitted { version: committed } if committed == version => Ok(()),
            other => Err(Error::Protocol(format!(
                "unexpected response to catalog commit: {other:?}"
            ))),
        }
    }

    pub fn catalog_command(
        &mut self,
        command: crate::cluster::CatalogCommand,
    ) -> Result<crate::cluster::CatalogCommandResult> {
        match self.round_trip(Request::CatalogCommand(command))? {
            Response::CatalogChanged(result) => Ok(result),
            other => Err(Error::Protocol(format!(
                "unexpected response to catalog command: {other:?}"
            ))),
        }
    }

    pub fn catalog_install(
        &mut self,
        leader: u64,
        term: u64,
        catalog: &crate::cluster::Catalog,
    ) -> Result<()> {
        match self.round_trip(Request::CatalogInstall {
            leader,
            term,
            catalog: catalog.clone(),
        })? {
            Response::CatalogInstalled { version } if version == catalog.version => Ok(()),
            other => Err(Error::Protocol(format!(
                "unexpected response to catalog install: {other:?}"
            ))),
        }
    }

    pub fn split_image_info(
        &mut self,
        operation: u64,
        side: super::protocol::SplitImageSide,
    ) -> Result<(u64, [u8; 32])> {
        match self.round_trip(Request::SplitImageInfo { operation, side })? {
            Response::SplitImageInfo {
                total_bytes,
                digest,
            } => Ok((total_bytes, digest)),
            other => Err(Error::Protocol(format!(
                "unexpected split image metadata response: {other:?}"
            ))),
        }
    }

    pub fn split_image_read(
        &mut self,
        operation: u64,
        side: super::protocol::SplitImageSide,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>> {
        match self.round_trip(Request::SplitImageRead {
            operation,
            side,
            offset,
            len,
        })? {
            Response::SplitImageChunk { data } => Ok(data),
            other => Err(Error::Protocol(format!(
                "unexpected split image chunk response: {other:?}"
            ))),
        }
    }

    fn round_trip(&mut self, req: Request) -> Result<Response> {
        // Write straight through the buffer to the stream; read back through the buffer.
        write_message(self.conn.get_mut(), &req)?;
        let resp: Response = read_message(&mut self.conn)?;
        match resp {
            // A rejection is a result, not a transport failure, so it is surfaced as the
            // same `Rejected` the local API produces rather than as a protocol error.
            Response::Error {
                message,
                retryable: true,
            } => Err(Error::Busy(message)),
            Response::Error {
                message,
                retryable: false,
            } => Err(Error::Protocol(message)),
            other => Ok(other),
        }
    }
}

fn unexpected(r: Response) -> Error {
    match r {
        Response::Rejected { message } => Error::Unsupported(message),
        other => Error::Protocol(format!("unexpected response: {other:?}")),
    }
}
