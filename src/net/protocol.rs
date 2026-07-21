//! Wire protocol: framing and messages.
//!
//! Every message is a `u32` big-endian length followed by that many bytes of bincode. The
//! length prefix is checked against a cap before allocating, so a malformed or hostile
//! header cannot make the server reserve gigabytes on the strength of four bytes it has not
//! validated.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::replication::StreamTxn;
use crate::shard::ShardId;
use crate::storage::exec::{Statement, Value};

/// Bumped when the wire format changes incompatibly. Checked at handshake so a mismatched
/// peer is told exactly that, rather than failing later as a confusing decode error.
pub const PROTOCOL_VERSION: u32 = 3;

/// Largest single message. Snapshot chunks are the biggest legitimate payload, so this sits
/// comfortably above the chunk size while still refusing anything absurd.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// How fresh a read has to be.
///
/// The default is [`ReadConsistency::Linearizable`], and deliberately so: a caller that says
/// nothing about freshness gets the strongest guarantee, not the fastest answer. A weaker
/// default would silently hand stale rows to code that never considered the question.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Any copy will do, however far behind. The cheapest read, and the only one a follower
    /// can always answer.
    Stale,
    /// A copy that has applied at least this position. Lets a caller read its own writes
    /// without pinning every read to the leader: remember the LSN a write returned, then ask
    /// for at least that.
    AtLeastLsn(u64),
    /// The shard's current leader. The only level that reflects every acknowledged write.
    #[default]
    Linearizable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Request {
    /// First message on a connection.
    Hello {
        version: u32,
        /// Informational; appears in logs to make a connection identifiable.
        client: String,
    },
    /// Run a read against one shard.
    Query {
        shard: u32,
        statement: Statement,
        consistency: ReadConsistency,
    },
    /// Run a read across every shard, merged.
    QueryAll { statement: Statement },
    /// Run one statement, routed by its shard key — the server picks the shard(s). This is how a
    /// client runs SQL without knowing the cluster is sharded: a keyed write lands on its shard, a
    /// point read hits one shard, any other read fans out, and DDL reaches every shard.
    Run { statement: Statement },
    /// Run a write against one shard.
    Execute {
        shard: u32,
        statements: Vec<Statement>,
    },
    /// Apply a statement to every shard. How DDL is propagated.
    ExecuteAll { statement: Statement },
    /// Apply `statements` as one atomic transaction on `shard`. All commit, or none do — the
    /// COMMIT of a client-held transaction, distinct from `Execute` whose statements are
    /// independent and isolated per-statement.
    Transaction {
        shard: u32,
        statements: Vec<Statement>,
    },
    /// Route a key to its shard, so a client can target writes without knowing the hash.
    Route { key: Vec<u8> },
    /// Ask what this node is.
    Info,
    /// Stream committed frames for `shard` starting at `from_lsn`.
    ///
    /// A follower asks for what it needs rather than being pushed at, so a follower that
    /// falls behind can catch up instead of being stuck.
    Subscribe {
        /// Which follower is asking. The request is also its acknowledgement — asking from
        /// `from_lsn` is proof it holds everything below — so the leader needs to know whose
        /// position this is. Zero means an anonymous reader that does not count toward any
        /// quorum.
        node: u64,
        shard: u32,
        epoch: u64,
        from_lsn: u64,
        /// Cap on transactions per response, so one reply cannot exceed the frame limit.
        max_txns: u32,
    },
    /// Begin a snapshot of `shard`, freezing it. Answered with its identity and size.
    SnapshotBegin { shard: u32 },
    /// Read `len` bytes of the frozen snapshot from `offset`.
    SnapshotRead { shard: u32, offset: u64, len: u32 },
    /// Release the freeze. Must be sent, or checkpointing stays suspended.
    SnapshotEnd { shard: u32 },
    /// Apply a schema change to one shard and return its new version.
    SchemaApply { shard: u32, ddl: Statement },
    /// Handle this request here, without forwarding it on.
    ///
    /// How a forwarded request is distinguished from a fresh one. Without it, two nodes with
    /// briefly different placement maps could forward the same request back and forth
    /// forever; with it, the second node refuses instead of bouncing it.
    Direct(Box<Request>),
    /// The answer to a [`Response::Challenge`]: `proof = blake3::keyed_hash(key, nonce)`.
    /// The secret itself never crosses the wire.
    Auth { name: String, proof: [u8; 32] },
    /// Create or replace a user, at runtime. Carries the *derived key*, not the secret — the
    /// plaintext never leaves the operator's machine. Admin-only, and an admin may not mint a
    /// `Cluster` user.
    CreateUser {
        name: String,
        key: [u8; 32],
        role: crate::net::auth::Role,
    },
    /// Remove a user. Admin-only.
    DropUser { name: String },
    /// List users (names and roles, never keys). Admin-only.
    ListUsers,
    /// A peer is standing for election.
    Vote(crate::cluster::VoteRequest),
    /// A peer claims leadership and is renewing its lease.
    Beat(crate::cluster::Heartbeat),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Response {
    Welcome {
        version: u32,
        shard_count: u32,
        epoch: Option<u64>,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// No copy on this node is fresh enough for the level asked for, and the leader could not
    /// be reached. Distinct from an error: the data exists, this node just cannot honour the
    /// guarantee, and saying so beats returning rows that quietly break it.
    TooStale {
        shard: u32,
        have: u64,
        need: u64,
    },
    Changed {
        rows_affected: u64,
        last_insert_rowid: i64,
    },
    /// Per-shard outcomes for an `ExecuteAll`. Not collapsed, because there is no atomicity
    /// across shards and a partial failure must be visible as such.
    AllShards {
        outcomes: Vec<(u32, ShardOutcome)>,
    },
    Routed {
        shard: u32,
    },
    Info {
        shard_count: u32,
        epoch: Option<u64>,
        wal_retries: u64,
        contended_opens: u64,
    },
    /// A batch of replication frames.
    Frames {
        shard: u32,
        epoch: u64,
        txns: Vec<StreamTxn>,
    },
    /// The subscription cannot be served from frames; the follower must bootstrap.
    NeedsBootstrap {
        shard: u32,
        reason: String,
    },
    /// The follower is level with the primary. Distinct from an empty `Frames` batch only
    /// in intent, but the distinction is worth keeping: one means "nothing new", the other
    /// would mean "here is what you asked for".
    UpToDate {
        shard: u32,
    },
    SnapshotInfo {
        shard: u32,
        epoch: u64,
        lsn: u64,
        total_bytes: u64,
    },
    SnapshotChunk {
        /// Empty when the snapshot has been fully read.
        data: Vec<u8>,
    },
    Ok,
    /// A write was buffered into an open transaction, not yet applied. `queued` is how many
    /// statements the transaction now holds. Durability comes at COMMIT, not here.
    Staged {
        queued: u64,
    },
    /// Authentication is required: prove knowledge of a secret by answering this nonce.
    /// Fresh per connection, so a recorded handshake replays as nothing.
    Challenge {
        nonce: [u8; 32],
    },
    /// The users on a server: names and roles, never keys.
    Users {
        users: Vec<(String, crate::net::auth::Role)>,
    },
    SchemaVersion {
        shard: u32,
        version: i64,
    },
    Voted(crate::cluster::VoteReply),
    Beat(crate::cluster::HeartbeatReply),
    /// The statement was rejected deterministically — bad SQL, constraint violation. A
    /// result, not a transport failure.
    Rejected {
        message: String,
    },
    /// The request failed. `retryable` distinguishes backpressure from a real fault, so a
    /// client can tell "slow down" from "this will never work".
    Error {
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardOutcome {
    Ok,
    Rejected(String),
}

fn config() -> impl bincode::config::Config {
    bincode::config::standard().with_limit::<MAX_FRAME_BYTES>()
}

pub fn write_message<T: Serialize, W: Write>(w: &mut W, msg: &T) -> Result<()> {
    let body = bincode::serde::encode_to_vec(msg, config())
        .map_err(|e| Error::Protocol(format!("encoding: {e}")))?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(Error::Protocol(format!(
            "message of {} bytes exceeds the {MAX_FRAME_BYTES} byte limit",
            body.len()
        )));
    }
    // Length prefix and body written as one buffer, then flushed. One write means the
    // 4-byte prefix cannot leave as its own tiny TCP segment ahead of the body, and it means
    // callers no longer need a separate BufWriter — which matters now that the same code path
    // carries a TLS stream that cannot be split into independent read and write halves.
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(&body);
    w.write_all(&framed)
        .map_err(|e| Error::Protocol(format!("writing message: {e}")))?;
    w.flush()
        .map_err(|e| Error::Protocol(format!("flushing: {e}")))?;
    Ok(())
}

pub fn read_message<T: for<'de> Deserialize<'de>, R: Read>(r: &mut R) -> Result<T> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)
        .map_err(|e| Error::Protocol(format!("reading length: {e}")))?;
    let len = u32::from_be_bytes(len) as usize;

    // Checked before allocating. Trusting a length prefix is how a four-byte header turns
    // into a gigabyte allocation.
    if len > MAX_FRAME_BYTES {
        return Err(Error::Protocol(format!(
            "peer announced a {len} byte message, over the {MAX_FRAME_BYTES} byte limit"
        )));
    }

    let mut body = vec![0u8; len];
    r.read_exact(&mut body)
        .map_err(|e| Error::Protocol(format!("reading body: {e}")))?;
    let (msg, _) = bincode::serde::decode_from_slice(&body, config())
        .map_err(|e| Error::Protocol(format!("decoding: {e}")))?;
    Ok(msg)
}

impl From<ShardId> for u32 {
    fn from(s: ShardId) -> u32 {
        s.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip() {
        let mut buf = Vec::new();
        let req = Request::Execute {
            shard: 3,
            statements: vec![Statement::with_params(
                "INSERT INTO t VALUES (?1, ?2)",
                vec![Value::Integer(7), Value::Text("hello".into())],
            )],
        };
        write_message(&mut buf, &req).unwrap();
        let back: Request = read_message(&mut buf.as_slice()).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn every_value_kind_survives_the_wire() {
        let mut buf = Vec::new();
        let resp = Response::Rows {
            columns: vec!["a".into(), "b".into()],
            rows: vec![vec![
                Value::Null,
                Value::Integer(-9),
                Value::Real(1.5),
                Value::Text("héllo".into()),
                Value::Blob(vec![0, 255, 7]),
            ]],
        };
        write_message(&mut buf, &resp).unwrap();
        let back: Response = read_message(&mut buf.as_slice()).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn an_oversized_length_prefix_is_refused_before_allocating() {
        // A four-byte header must not be able to make the reader reserve gigabytes.
        let mut framed = Vec::new();
        framed.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = read_message::<Request, _>(&mut framed.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("over the"),
            "expected a limit error, got: {err}"
        );
    }

    #[test]
    fn a_truncated_message_is_an_error_not_a_hang() {
        let mut buf = Vec::new();
        write_message(&mut buf, &Request::Info).unwrap();
        buf.truncate(buf.len() - 1);
        assert!(read_message::<Request, _>(&mut buf.as_slice()).is_err());
    }
}
