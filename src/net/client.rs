//! A blocking client.

use std::io::{BufReader, BufWriter};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::storage::exec::{QueryResult, Statement, Value};

use super::protocol::{
    PROTOCOL_VERSION, Request, Response, ShardOutcome, read_message, write_message,
};

pub struct Client {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    shard_count: u32,
    epoch: Option<u64>,
}

impl Client {
    pub fn connect(addr: &str) -> Result<Self> {
        Self::connect_with(addr, Duration::from_secs(30))
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

        let mut me = Self {
            r: BufReader::new(
                stream
                    .try_clone()
                    .map_err(|e| Error::Protocol(format!("cloning stream: {e}")))?,
            ),
            w: BufWriter::new(stream),
            shard_count: 0,
            epoch: None,
        };

        match me.round_trip(Request::Hello {
            version: PROTOCOL_VERSION,
            client: format!("meshdb-client/{}", env!("CARGO_PKG_VERSION")),
        })? {
            Response::Welcome {
                shard_count, epoch, ..
            } => {
                me.shard_count = shard_count;
                me.epoch = epoch;
                Ok(me)
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

    pub fn query(&mut self, shard: u32, sql: impl Into<Statement>) -> Result<QueryResult> {
        match self.round_trip(Request::Query {
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

    fn round_trip(&mut self, req: Request) -> Result<Response> {
        write_message(&mut self.w, &req)?;
        let resp: Response = read_message(&mut self.r)?;
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
