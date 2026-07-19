//! A blocking client.

use std::io::{BufReader, BufWriter};
use std::net::TcpStream;
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
        let stream = TcpStream::connect(addr)
            .map_err(|e| Error::Protocol(format!("connecting to {addr}: {e}")))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| Error::Protocol(format!("set_read_timeout: {e}")))?;
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
