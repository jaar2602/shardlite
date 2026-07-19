//! Network transport: protocol, server and client.
//!
//! # Why threads rather than async
//!
//! Every request ultimately calls into the writer or reader fleets, which are blocking
//! threads. An async server would hop through `spawn_blocking` for every call, adding a
//! dispatch per request and an impedance mismatch, in exchange for scaling to many *idle*
//! connections — which is not the shape of a database aimed at a 1 CPU / 150 MB node.
//!
//! So each connection gets a thread, and the connection count is capped. That cap is where
//! load shedding actually happens: with one in-flight request per connection, the bounded
//! write queue behind it cannot fill from connections alone, so refusing the connection is
//! the honest first line and the queue bound remains a second.

pub mod client;
pub mod protocol;
pub mod replica;
pub mod server;

pub use client::Client;
pub use protocol::{Request, Response, ShardOutcome};
pub use replica::{Replica, ReplicaConfig, ReplicaStats};
pub use server::{NodeServices, Server, ServerConfig, ServerStats};
