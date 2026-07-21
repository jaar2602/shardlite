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

pub mod auth;
pub mod client;
pub mod forward;
#[cfg(feature = "http")]
pub mod http_gateway;
#[cfg(any(feature = "http", feature = "json-tcp"))]
pub mod json;
#[cfg(feature = "json-tcp")]
pub mod json_tcp;
pub mod protocol;
pub mod replica;
pub mod server;
pub mod transport;

pub use auth::{AuthConfig, Role};
pub use client::{Client, RunResult};
pub use forward::{ForwardStats, Router};
#[cfg(feature = "http")]
pub use http_gateway::{HttpConfig, HttpGateway};
#[cfg(feature = "json-tcp")]
pub use json_tcp::{JsonTcpConfig, JsonTcpServer};
pub use protocol::{ReadConsistency, Request, Response, ShardOutcome};
pub use replica::{Replica, ReplicaConfig, ReplicaStats};
pub use server::{NodeServices, Server, ServerConfig, ServerStats};
