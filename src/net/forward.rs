//! Sending a request to the node that owns the shard.
//!
//! # Why the server forwards rather than the client routing
//!
//! A client would otherwise need the placement map, connections to every node, and its own
//! handling of a map that changed since it last looked. Forwarding keeps all of that on the
//! server, where the map already lives and is already kept current by heartbeats. A client
//! connects to any node and its work reaches the right one.
//!
//! The cost is an extra hop for a misdirected request. That is the right trade here: the
//! alternative pushes cluster topology into every client, and a client with a stale map is a
//! client writing to the wrong node.
//!
//! # Forwarding cannot loop
//!
//! Two nodes whose placement maps disagree for a moment could otherwise pass a request back
//! and forth forever. A forwarded request is wrapped in [`Request::Direct`], which means
//! "handle this here or refuse it" — so the second node answers rather than bouncing it, and
//! the refusal names the real problem instead of hanging.
//!
//! # A forwarded failure is not this node's failure
//!
//! Errors come back as they are. A node that rewrote them as its own would make every
//! problem look local, and the first thing an operator needs to know is which node actually
//! refused.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::cluster::ClusterNode;
use crate::error::{Error, Result};
use crate::shard::ShardId;

use super::client::Client;
use super::protocol::{Request, Response};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardStats {
    /// Requests handed to another node because it owns the shard.
    pub forwarded: u64,
    /// Forwards that could not be delivered — the owner was unreachable, or its address is
    /// unknown. Each one is a request that failed for a reason nothing else would show.
    pub failed: u64,
}

/// Routes shard work to whichever node owns it.
pub struct Router {
    cluster: std::sync::Arc<ClusterNode>,
    /// One connection per peer, rebuilt on failure. Kept separate from the election loop's
    /// links so a slow forward cannot delay a heartbeat, which the lease depends on.
    links: Mutex<BTreeMap<u64, Client>>,
    timeout: Duration,
    forwarded: AtomicU64,
    failed: AtomicU64,
}

impl Router {
    pub fn new(cluster: std::sync::Arc<ClusterNode>) -> Self {
        Self {
            cluster,
            links: Mutex::new(BTreeMap::new()),
            timeout: Duration::from_secs(10),
            forwarded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> ForwardStats {
        ForwardStats {
            forwarded: self.forwarded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }

    /// Which node owns `shard`, if placement has said.
    pub fn owner(&self, shard: ShardId) -> Option<u64> {
        self.cluster.placement().owner(shard)
    }

    /// Whether this node should handle `shard` itself.
    ///
    /// An unassigned shard is handled locally rather than refused: before the first placement
    /// arrives every node's map is empty, and refusing then would make a single-node or
    /// just-started cluster unable to do anything at all.
    pub fn is_mine(&self, shard: ShardId) -> bool {
        match self.owner(shard) {
            Some(owner) => owner == self.cluster.id(),
            None => true,
        }
    }

    /// Send `req` to the node that owns `shard` and return its answer.
    pub fn forward(&self, shard: ShardId, req: Request) -> Result<Response> {
        let Some(owner) = self.owner(shard) else {
            return Err(Error::NoOwner {
                shard: shard.to_string(),
            });
        };
        let Some(addr) = self.cluster.peer_addr(owner).map(str::to_owned) else {
            self.failed.fetch_add(1, Ordering::Relaxed);
            return Err(Error::NoOwner {
                shard: format!("{shard} (owner is node {owner}, whose address is unknown)"),
            });
        };

        // Wrapped so the far side answers rather than forwarding again.
        let wrapped = Request::Direct(Box::new(req));
        self.forwarded.fetch_add(1, Ordering::Relaxed);

        // Take the connection *out* of the map before using it, so the lock is never held
        // across network I/O. Held across, one hung owner — accepting connections, answering
        // nothing — would block every forward on this node for the full timeout, whatever
        // shard or owner they were bound for. That is the same disease that once froze the
        // election loop, sitting in the write path.
        //
        // The cost is benign: two threads forwarding to the same peer at once find the map
        // empty and each open a connection; whichever finishes last parks its connection for
        // reuse and the other's is dropped. Connection churn under a race, never a stall.
        let cached = self.links.lock().expect("router links").remove(&owner);

        if let Some(mut client) = cached {
            match client.request(wrapped.clone()) {
                Ok(r) => {
                    self.links
                        .lock()
                        .expect("router links")
                        .insert(owner, client);
                    return Ok(r);
                }
                Err(e) => {
                    tracing::debug!(owner, error = %e, "forward link failed; reconnecting");
                }
            }
        }

        match Client::connect_bounded(&addr, self.timeout, self.timeout) {
            Ok(mut client) => match client.request(wrapped) {
                Ok(r) => {
                    self.links
                        .lock()
                        .expect("router links")
                        .insert(owner, client);
                    Ok(r)
                }
                Err(e) => {
                    self.failed.fetch_add(1, Ordering::Relaxed);
                    Err(e)
                }
            },
            Err(e) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(owner, addr, error = %e, "cannot reach the owner of a shard");
                Err(e)
            }
        }
    }

    /// Bound on a forwarded round trip. The default is client-sized; tests shrink it.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("node", &self.cluster.id())
            .finish_non_exhaustive()
    }
}
