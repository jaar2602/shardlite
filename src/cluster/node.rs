//! Driving the election state machine over the network.
//!
//! [`super::election::Election`] is deliberately free of I/O: it returns [`Action`]s and never
//! performs them. This is the file that performs them — a tick loop, one connection per peer,
//! and the handlers the server calls when a peer's messages arrive.
//!
//! # Connections to peers are kept, not remade
//!
//! Heartbeats go out several times a second. Reconnecting for each one would spend more time
//! in TCP handshakes than in the protocol, so a connection per peer is cached and rebuilt only
//! after a failure. A peer that is down therefore costs one failed connect per heartbeat, not
//! one per message — and that failure is what the lease is measuring anyway.
//!
//! # An unreachable peer is not an error
//!
//! Nodes go away; that is the entire premise. A failed heartbeat is logged at debug and
//! counted, never propagated — the correct response to an unreachable peer is to keep going
//! and let the lease decide, not to unwind.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::net::Client;

use super::durability::Durability;
use super::election::{Action, Election, Heartbeat, HeartbeatReply, Role, VoteReply, VoteRequest};
use super::fence::Fence;
use super::placement::Placement;
use super::term::NodeId;

/// Where this node's durability comes from.
///
/// A trait because the answer differs by role and must stay honest: a leader reports what its
/// own shards have committed, a follower reports what it has actually applied. Hardcoding
/// either would make one of the two lie during an election, which is precisely when the
/// number matters.
pub trait DurabilitySource: Send + Sync {
    fn durability(&self) -> Durability;
}

#[derive(Debug, Default)]
struct Counters {
    elections_started: AtomicU64,
    became_leader: AtomicU64,
    stepped_down: AtomicU64,
    heartbeats_sent: AtomicU64,
    peer_unreachable: AtomicU64,
    votes_granted: AtomicU64,
    votes_refused: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterStats {
    pub elections_started: u64,
    pub became_leader: u64,
    /// Times this node lost leadership. Repeated step-downs mean an unstable cluster —
    /// usually a heartbeat interval too close to the election timeout, or a flapping link.
    pub stepped_down: u64,
    pub heartbeats_sent: u64,
    /// Failed sends to a peer. Ordinary in isolation; sustained, it is a partition.
    pub peer_unreachable: u64,
    pub votes_granted: u64,
    pub votes_refused: u64,
}

pub struct ClusterNode {
    id: NodeId,
    election: Mutex<Election>,
    fence: Arc<Fence>,
    /// Peer id to address.
    peers: BTreeMap<NodeId, String>,
    /// Cached connections, rebuilt on failure.
    links: Mutex<BTreeMap<NodeId, Client>>,
    durability: Arc<dyn DurabilitySource>,
    /// Every shard in the cluster. The coordinator spreads these across live members; a
    /// node opens write gates only for the ones assigned to it.
    shards: Vec<crate::shard::ShardId>,
    /// The assignment currently in force on this node.
    placement: Mutex<Placement>,
    /// Peers that answered the most recent heartbeat round. The coordinator assigns shards
    /// only to members it can currently reach — a shard assigned to a node that is gone has
    /// no leader at all, which is worse than an uneven spread.
    live: Mutex<std::collections::BTreeSet<NodeId>>,
    /// Bound on every peer round trip. Deliberately a fraction of the election timeout: a
    /// peer that is hung rather than dead must look dead before the lease is due, or the
    /// leader is still blocked in a socket read at the moment it should be stepping down.
    peer_timeout: Duration,
    counters: Counters,
    stop: AtomicBool,
}

impl ClusterNode {
    pub fn new(
        id: NodeId,
        election: Election,
        fence: Arc<Fence>,
        peers: BTreeMap<NodeId, String>,
        durability: Arc<dyn DurabilitySource>,
        shards: Vec<crate::shard::ShardId>,
    ) -> Self {
        // A third of the election timeout: long enough to survive ordinary scheduling noise,
        // short enough that both peers can be tried and the lease still evaluated on time.
        let peer_timeout = election.election_timeout() / 3;
        Self {
            id,
            election: Mutex::new(election),
            fence,
            peers,
            links: Mutex::new(BTreeMap::new()),
            durability,
            shards,
            placement: Mutex::new(Placement::default()),
            live: Mutex::new(std::collections::BTreeSet::new()),
            peer_timeout,
            counters: Counters::default(),
            stop: AtomicBool::new(false),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn fence(&self) -> &Arc<Fence> {
        &self.fence
    }

    pub fn role(&self) -> Role {
        self.election.lock().expect("election mutex").role()
    }

    pub fn is_leader(&self) -> bool {
        self.election.lock().expect("election mutex").is_leader()
    }

    pub fn term(&self) -> u64 {
        self.election.lock().expect("election mutex").term()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.election.lock().expect("election mutex").leader()
    }

    pub fn stats(&self) -> ClusterStats {
        ClusterStats {
            elections_started: self.counters.elections_started.load(Ordering::Relaxed),
            became_leader: self.counters.became_leader.load(Ordering::Relaxed),
            stepped_down: self.counters.stepped_down.load(Ordering::Relaxed),
            heartbeats_sent: self.counters.heartbeats_sent.load(Ordering::Relaxed),
            peer_unreachable: self.counters.peer_unreachable.load(Ordering::Relaxed),
            votes_granted: self.counters.votes_granted.load(Ordering::Relaxed),
            votes_refused: self.counters.votes_refused.load(Ordering::Relaxed),
        }
    }

    /// The assignment this node is currently acting on.
    pub fn placement(&self) -> Placement {
        self.placement.lock().expect("placement mutex").clone()
    }

    /// Shards this node currently leads.
    pub fn led_shards(&self) -> Vec<crate::shard::ShardId> {
        self.fence.led_shards()
    }

    /// Adopt an assignment: open write gates for the shards it gives this node, and close
    /// every other.
    ///
    /// [`Fence::open_for`] replaces the whole set, so a shard taken away is closed without
    /// this having to compute the difference — the subtraction that would otherwise be easy
    /// to miss, leaving this node writing a shard another node now owns.
    fn apply_placement(&self, p: &Placement) {
        let mine = p.shards_for(self.id);
        {
            let mut current = self.placement.lock().expect("placement mutex");
            if *current == *p {
                return;
            }
            *current = p.clone();
        }
        tracing::info!(
            node = self.id,
            term = p.term,
            leads = mine.len(),
            of = p.assignments.len(),
            "applying placement"
        );
        self.fence.open_for(&mine, p.term);
    }

    /// Compute the assignment this node would publish as coordinator.
    fn plan(&self, term: u64) -> Placement {
        let mut members: Vec<NodeId> = self
            .live
            .lock()
            .expect("live mutex")
            .iter()
            .copied()
            .collect();
        // Always include self: a coordinator is by definition reachable from itself, and a
        // leader that excluded itself would assign away every shard on the first round.
        members.push(self.id);
        Placement::balanced(self.shards.len() as u32, &members, term)
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// One turn of the state machine. Separate from [`Self::run`] so tests can step it.
    pub fn tick_once(&self, now: Instant) -> Result<()> {
        let durability = self.durability.durability();
        let action = {
            let mut e = self.election.lock().expect("election mutex");
            e.tick(now, &durability)?
        };
        if let Some(action) = action {
            self.perform(action, &durability, now)?;
        }
        Ok(())
    }

    /// Tick until stopped. Blocks.
    pub fn run(&self, interval: Duration) {
        while !self.stop.load(Ordering::Relaxed) {
            if let Err(e) = self.tick_once(Instant::now()) {
                // Losing a tick is survivable; the next one retries. Exiting the loop would
                // leave the node silently out of the cluster, which is far worse.
                tracing::warn!(node = self.id, error = %e, "cluster tick failed");
            }
            std::thread::sleep(interval);
        }
        tracing::info!(node = self.id, "cluster loop stopped");
    }

    fn perform(&self, action: Action, durability: &Durability, now: Instant) -> Result<()> {
        match action {
            Action::RequestVotes(term) => {
                self.counters
                    .elections_started
                    .fetch_add(1, Ordering::Relaxed);
                self.campaign(term, durability, now)?;
            }
            Action::Heartbeat(term) => self.beat(term, now)?,
            Action::BecameLeader(term) => {
                self.counters.became_leader.fetch_add(1, Ordering::Relaxed);
                // A new coordinator publishes an assignment rather than seizing every shard.
                // On the first round no peer has answered yet, so it takes everything and
                // gives shards back as peers reply — which is safe, being the leader, and
                // self-correcting within a heartbeat.
                let plan = self.plan(term);
                self.apply_placement(&plan);
                // Beat immediately so followers learn who leads without waiting out a timeout.
                self.beat(term, now)?;
            }
            Action::SteppedDown { term, ref why } => {
                self.counters.stepped_down.fetch_add(1, Ordering::Relaxed);
                // Close the gate *first*. Anything else on this path — draining queues,
                // notifying peers — happens after writes have already stopped, or the window
                // this is meant to close stays open for exactly as long as that work takes.
                self.fence.close(why);
                tracing::warn!(node = self.id, term, why, "no longer leader");
            }
        }
        Ok(())
    }

    fn campaign(&self, term: u64, durability: &Durability, now: Instant) -> Result<()> {
        let req = VoteRequest {
            term,
            candidate: self.id,
            durability: durability.clone(),
        };
        for (&peer, addr) in &self.peers {
            let reply: Option<VoteReply> = self.ask(peer, addr, |c| c.request_vote(&req));
            let Some(reply) = reply else { continue };

            let action = {
                let mut e = self.election.lock().expect("election mutex");
                e.on_vote_reply(peer, &reply, now)?
            };
            if let Some(action) = action {
                self.perform(action, durability, now)?;
                // Either this node just won, or it was deposed. Asking the rest for votes in
                // a term it no longer holds would be noise.
                return Ok(());
            }
        }
        Ok(())
    }

    fn beat(&self, term: u64, now: Instant) -> Result<()> {
        // Recompute before sending, so a member that appeared or vanished last round is
        // reflected in the map this round.
        let plan = self.plan(term);
        self.apply_placement(&plan);

        let hb = Heartbeat {
            term,
            leader: self.id,
            placement: plan,
        };

        let mut answered = std::collections::BTreeSet::new();
        for (&peer, addr) in &self.peers {
            self.counters
                .heartbeats_sent
                .fetch_add(1, Ordering::Relaxed);
            let reply: Option<HeartbeatReply> = self.ask(peer, addr, |c| c.heartbeat(&hb));
            let Some(reply) = reply else { continue };
            if reply.ok {
                answered.insert(peer);
            }

            let action = {
                let mut e = self.election.lock().expect("election mutex");
                e.on_heartbeat_reply(peer, &reply, now)?
            };
            if let Some(action) = action {
                let durability = self.durability.durability();
                self.perform(action, &durability, now)?;
                return Ok(());
            }
        }
        *self.live.lock().expect("live mutex") = answered;
        Ok(())
    }

    /// A node on its way out must stop counting toward anyone's quorum.
    ///
    /// Otherwise stopping a node does not actually remove it: its server keeps answering
    /// heartbeats on connections already open, so a leader that has genuinely lost its
    /// cluster keeps being told it still has one — and keeps writing. Departure has to be
    /// visible to peers, not just to this node's own loop.
    fn check_participating(&self) -> Result<()> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(crate::error::Error::Departed);
        }
        Ok(())
    }

    /// Send to a peer over its cached connection, rebuilding it once on failure.
    ///
    /// Returns `None` when the peer is unreachable, which is an ordinary condition and not an
    /// error — the lease is what decides whether it matters.
    fn ask<T>(&self, peer: NodeId, addr: &str, f: impl Fn(&mut Client) -> Result<T>) -> Option<T> {
        let mut links = self.links.lock().expect("links mutex");

        if let Some(client) = links.get_mut(&peer) {
            match f(client) {
                Ok(v) => return Some(v),
                Err(e) => {
                    // A dead cached connection is the common case after a peer restarts.
                    tracing::debug!(node = self.id, peer, error = %e, "peer link failed; reconnecting");
                    links.remove(&peer);
                }
            }
        }

        match Client::connect_bounded(addr, self.peer_timeout, self.peer_timeout) {
            Ok(mut client) => match f(&mut client) {
                Ok(v) => {
                    links.insert(peer, client);
                    Some(v)
                }
                Err(e) => {
                    self.counters
                        .peer_unreachable
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(node = self.id, peer, error = %e, "peer request failed");
                    None
                }
            },
            Err(e) => {
                self.counters
                    .peer_unreachable
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(node = self.id, peer, addr, error = %e, "peer unreachable");
                None
            }
        }
    }

    /// A peer is standing for election. Called by the server.
    pub fn handle_vote_request(&self, req: &VoteRequest) -> Result<VoteReply> {
        self.check_participating()?;
        let mine = self.durability.durability();
        let (reply, action) = {
            let mut e = self.election.lock().expect("election mutex");
            let was_leader = e.is_leader();
            let reply = e.on_vote_request(req, Instant::now(), &mine)?;
            // Granting a vote to a higher term deposes this node; the gate must close.
            let action = (was_leader && !e.is_leader()).then(|| Action::SteppedDown {
                term: e.term(),
                why: format!("candidate {} carried a higher term", req.candidate),
            });
            (reply, action)
        };
        if let Some(action) = action {
            self.perform(action, &mine, Instant::now())?;
        }

        if reply.granted {
            self.counters.votes_granted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.votes_refused.fetch_add(1, Ordering::Relaxed);
        }
        Ok(reply)
    }

    /// A peer claims leadership. Called by the server.
    pub fn handle_heartbeat(&self, hb: &Heartbeat) -> Result<HeartbeatReply> {
        self.check_participating()?;

        // Adopt the assignment only from a coordinator that is at least as current as this
        // node. A map from an older term comes from a coordinator that has been deposed, and
        // its opinion about who owns what is exactly what fencing exists to reject.
        if hb.term >= self.term() && hb.placement.term >= self.placement().term {
            self.apply_placement(&hb.placement);
        }

        let (reply, deposed) = {
            let mut e = self.election.lock().expect("election mutex");
            let was_leader = e.is_leader();
            let reply = e.on_heartbeat(hb, Instant::now())?;
            (reply, was_leader && !e.is_leader())
        };
        if deposed {
            let durability = self.durability.durability();
            self.perform(
                Action::SteppedDown {
                    term: hb.term,
                    why: format!("node {} leads a higher term", hb.leader),
                },
                &durability,
                Instant::now(),
            )?;
        }
        Ok(reply)
    }
}

impl std::fmt::Debug for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterNode")
            .field("id", &self.id)
            .field("peers", &self.peers.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// A primary reports what its own shards have committed.
impl DurabilitySource for crate::shard::ShardManager {
    fn durability(&self) -> Durability {
        // No capture means no stream, and so no positions to compare. Epoch 0 is the honest
        // answer: every node in such a cluster reports it, so they stay comparable with each
        // other and with nothing else.
        let mut d = Durability::new(self.epoch().unwrap_or(0));
        for s in 0..self.shard_count() {
            let shard = crate::shard::ShardId(s);
            d.shards.insert(shard, self.last_lsn(shard));
        }
        d
    }
}

/// A follower reports what it has actually applied — not what it has received.
///
/// The distinction is the whole point. Reporting received-but-unapplied frames would let a
/// node win an election on the strength of data it has not durably written, which is exactly
/// the acknowledged-write loss the election restriction exists to prevent.
impl DurabilitySource for crate::replication::Follower {
    fn durability(&self) -> Durability {
        let positions = self.positions();
        let epoch = positions.values().map(|p| p.epoch).max().unwrap_or(0);
        let mut d = Durability::new(epoch);
        for (shard, pos) in positions {
            // A position from an older epoch cannot be counted toward this one.
            d.shards.insert(
                shard,
                if pos.epoch == epoch {
                    pos.applied_lsn
                } else {
                    0
                },
            );
        }
        d
    }
}
