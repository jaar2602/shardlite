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

/// Wall-clock ms since the epoch, for display-only timestamps (not consensus logic — that uses
/// `Instant`).
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    handover_failed: AtomicU64,
    /// Placement applications that actually changed the map — i.e. shards moved. The core "how
    /// often is the cluster reshuffling" signal: a healthy cluster reshuffles rarely (a node
    /// joined/left); a rising rate means flapping links or an unstable leader.
    placement_changes: AtomicU64,
    /// Wall-clock ms of the last placement change, so an operator sees recency, not just a count.
    last_change_ms: AtomicU64,
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
    /// Placement changes whose file handover failed. Each one is a shard this node was told
    /// to lead and could not safely take, so it is still being led by nobody.
    pub handover_failed: u64,
    /// Placement applications that moved shards. A high or rising rate means the cluster is
    /// reshuffling too often — the "is this happening too frequently?" signal.
    pub placement_changes: u64,
    /// Wall-clock ms of the last placement change (0 if none yet).
    pub last_change_ms: u64,
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
    /// Serialises the *application* of a placement — the map update and the gate changes
    /// together. The `placement` mutex alone is not enough: it guards the compare-and-record
    /// but was dropped before the gates were touched, so two heartbeat threads could record
    /// P1 then P2 and yet apply their gates in the order P2 then P1, leaving the map and the
    /// gates describing different worlds.
    applying: Mutex<()>,
    /// Drives the file handover when placement moves a shard. `None` on a node with no
    /// replication configured — there is then no other writer competing for the files, so
    /// opening and closing gates is the whole of ownership.
    ownership: Option<Arc<super::promotion::Promotion>>,
    /// Per-shard ownership marks. Kept in step with placement even when no `Promotion` is
    /// attached, so a node cannot claim to lead a shard the map gave to someone else — the
    /// fence and the mode saying different things is how a read gets answered by a node with
    /// no copy.
    modes: Option<Arc<crate::shard::mode::ShardModes>>,
    /// Peers that answered the most recent heartbeat round. The coordinator assigns shards
    /// only to members it can currently reach — a shard assigned to a node that is gone has
    /// no leader at all, which is worse than an uneven spread.
    live: Mutex<std::collections::BTreeSet<NodeId>>,
    /// Credentials for talking to peers, when the cluster requires authentication. Every
    /// node shares the cluster principal — peers are one trust domain, and per-peer secrets
    /// would multiply key management without changing what a compromised node can do.
    credentials: Option<(String, crate::net::auth::Key)>,
    /// Bound on every peer round trip. Deliberately a fraction of the election timeout: a
    /// peer that is hung rather than dead must look dead before the lease is due, or the
    /// leader is still blocked in a socket read at the moment it should be stepping down.
    peer_timeout: Duration,
    counters: Counters,
    stop: AtomicBool,
    /// Set by an operator to drain shards off this node WITHOUT removing it: it keeps voting
    /// (counts for quorum) but the leader assigns it no shards, so its shards move away via the
    /// ordinary fenced handover. The safe way to rebalance a healthy node — purely subtractive,
    /// so it cannot create a second writer. Advertised to the leader in each [`HeartbeatReply`].
    cordoned: AtomicBool,
    /// Members that reported themselves cordoned in the last heartbeat round; excluded from
    /// shard assignment (see [`Self::plan`]) but not from quorum.
    cordoned_members: Mutex<std::collections::BTreeSet<NodeId>>,
    /// Shards an operator has asked THIS node to host (a desired-placement hint). Advertised to
    /// the leader in each heartbeat; the coordinator honours it when this node is eligible.
    preferred: Mutex<std::collections::BTreeSet<crate::shard::ShardId>>,
    /// The leader's collected view of every member's preferred shards, gathered from the last
    /// heartbeat round: shard → the node that wants it. Fed into [`Placement::with_preferences`].
    preferences: Mutex<std::collections::BTreeMap<crate::shard::ShardId, NodeId>>,
    /// Wired by the deployment layer: notified when this node takes over a shard, so it can recover
    /// the shard's data (e.g. from S3) when it has none locally. `None` disables auto-recovery.
    recovery: std::sync::OnceLock<Arc<dyn crate::shard::ShardRecovery>>,
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
            applying: Mutex::new(()),
            ownership: None,
            modes: None,
            credentials: None,
            live: Mutex::new(std::collections::BTreeSet::new()),
            peer_timeout,
            counters: Counters::default(),
            stop: AtomicBool::new(false),
            cordoned: AtomicBool::new(false),
            cordoned_members: Mutex::new(std::collections::BTreeSet::new()),
            preferred: Mutex::new(std::collections::BTreeSet::new()),
            preferences: Mutex::new(std::collections::BTreeMap::new()),
            recovery: std::sync::OnceLock::new(),
        }
    }

    /// Attach a recovery hook, notified when this node takes over a shard so it can rebuild the
    /// shard's data from an archive (see [`crate::shard::ShardRecovery`]). Set once, after build.
    pub fn set_recovery(&self, recovery: Arc<dyn crate::shard::ShardRecovery>) {
        let _ = self.recovery.set(recovery);
    }

    /// Ask (or stop asking) for this node to host `shards` — an operator's desired-placement hint,
    /// honoured by the coordinator when this node is eligible (live and not cordoned). Purely a
    /// bias on the single authoritative map: a hint the leader cannot satisfy (this node down or
    /// cordoned) simply falls back to balance, so it can never create a second owner.
    pub fn set_preferred(&self, shards: &[crate::shard::ShardId], prefer: bool) {
        let mut set = self.preferred.lock().expect("preferred mutex");
        for &s in shards {
            if prefer {
                set.insert(s);
            } else {
                set.remove(&s);
            }
        }
    }

    pub fn preferred_shards(&self) -> Vec<crate::shard::ShardId> {
        self.preferred
            .lock()
            .expect("preferred mutex")
            .iter()
            .copied()
            .collect()
    }

    /// Cordon (`true`) or un-cordon (`false`) this node: while cordoned it keeps voting but is
    /// assigned no shards, so its shards drain to other members on the next placement round. Safe
    /// to toggle at any time — it only ever removes this node from *assignment*, never adds a
    /// conflicting one.
    pub fn set_cordoned(&self, cordoned: bool) {
        self.cordoned.store(cordoned, Ordering::Relaxed);
    }

    pub fn is_cordoned(&self) -> bool {
        self.cordoned.load(Ordering::Relaxed)
    }

    /// Voluntarily give up leadership (if this node holds it) so a peer takes over, while staying
    /// in the cluster with its shards — unlike [`Self::stop`], which removes the node. Returns
    /// `true` if it stepped down, `false` if it was not the leader. Safe: it never picks the
    /// successor or forces a term, so the ordinary election still guarantees a single leader.
    pub fn request_step_down(&self) -> Result<bool> {
        let now = Instant::now();
        let action = {
            let mut e = self.election.lock().expect("election mutex");
            e.request_step_down(now)
        };
        match action {
            Some(action) => {
                let durability = self.durability.durability();
                self.perform(action, &durability, now)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Members the leader saw report themselves cordoned in the last round (leader's view).
    pub fn cordoned_members(&self) -> Vec<NodeId> {
        self.cordoned_members
            .lock()
            .expect("cordoned mutex")
            .iter()
            .copied()
            .collect()
    }

    /// Attach the handover that placement changes drive.
    ///
    /// Without it a node told to lead a shard opens the gate without first taking the file
    /// from the replication path — writing SQL into a file still being rewritten as raw
    /// pages.
    pub fn with_ownership(mut self, promotion: Arc<super::promotion::Promotion>) -> Self {
        self.ownership = Some(promotion);
        self
    }

    /// Authenticate to peers as `name`. Required once any node in the cluster enables
    /// authentication — an unauthenticated heartbeat is refused like any other request.
    pub fn with_cluster_credentials(mut self, name: &str, secret: &str) -> Self {
        self.credentials = Some((name.to_string(), crate::net::auth::derive_key(secret)));
        self
    }

    /// Keep shard ownership marks in step with placement without a full handover.
    ///
    /// For a node that holds no replicated copies, marking is all that is needed: there is no
    /// file to hand over, only a claim to drop.
    pub fn with_modes(mut self, modes: Arc<crate::shard::mode::ShardModes>) -> Self {
        self.modes = Some(modes);
        self
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Where a peer listens, for forwarding work to the node that owns a shard.
    pub fn peer_addr(&self, node: NodeId) -> Option<&str> {
        self.peers.get(&node).map(|s| s.as_str())
    }

    /// Configured voting peers and their native shardlite addresses, sorted by node ID.
    ///
    /// This is a snapshot for operator-facing diagnostics. It deliberately says only who is
    /// configured; callers that need liveness must combine it with [`Self::live_members`] and
    /// must remember that only the current leader has a meaningful heartbeat view.
    pub fn peers(&self) -> Vec<(NodeId, String)> {
        self.peers
            .iter()
            .map(|(&node, addr)| (node, addr.clone()))
            .collect()
    }

    /// Peers that answered the leader's latest heartbeat round.
    ///
    /// Followers do not heartbeat their peers, so an empty set on a follower means "not
    /// observed", not "every peer is down". The HTTP topology response preserves that
    /// distinction rather than manufacturing liveness.
    pub fn live_members(&self) -> Vec<NodeId> {
        self.live
            .lock()
            .expect("live mutex")
            .iter()
            .copied()
            .collect()
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
            handover_failed: self.counters.handover_failed.load(Ordering::Relaxed),
            placement_changes: self.counters.placement_changes.load(Ordering::Relaxed),
            last_change_ms: self.counters.last_change_ms.load(Ordering::Relaxed),
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
        // One application at a time, and *skip* rather than queue behind one in flight.
        // Blocking would be worse on both sides: a handover can legitimately take seconds
        // (it waits for the pull loop to rest), and heartbeat threads queued behind it would
        // stall their replies — the hung-peer failure mode again, self-inflicted. Skipping
        // is safe because a skipped placement is not recorded, so the next heartbeat, at
        // most one interval away, simply retries it.
        let Ok(_applying) = self.applying.try_lock() else {
            return;
        };

        let mine = p.shards_for(self.id);
        // Shards this node is newly taking over (owned now, not before) — the ones that may need
        // their data recovered from an archive on failover.
        let newly_mine: Vec<crate::shard::ShardId>;
        {
            let mut current = self.placement.lock().expect("placement mutex");
            if *current == *p {
                return;
            }
            let old_mine: std::collections::BTreeSet<crate::shard::ShardId> =
                current.shards_for(self.id).into_iter().collect();
            newly_mine = mine
                .iter()
                .copied()
                .filter(|s| !old_mine.contains(s))
                .collect();
            *current = p.clone();
        }
        // A real reshuffle: record it and when, so operators can see how often shards move.
        self.counters
            .placement_changes
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .last_change_ms
            .store(unix_millis(), Ordering::Relaxed);
        tracing::info!(
            node = self.id,
            term = p.term,
            leads = mine.len(),
            of = p.assignments.len(),
            "applying placement"
        );

        match &self.ownership {
            // The handover takes the files before opening their gates, and gives files up
            // before closing them. Ordering lives in `Promotion`, not here.
            Some(promotion) => {
                if let Err(e) = promotion.apply(&mine, p.term) {
                    // Do not open gates on a failed handover. The next heartbeat republishes
                    // the same map and this retries; leading a shard whose file is still
                    // being rewritten would be worse than leading it late.
                    self.counters
                        .handover_failed
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        node = self.id,
                        error = %e,
                        "placement handover failed; gates left as they were, will retry"
                    );
                    // Forget the map so the retry is not skipped as a no-op.
                    *self.placement.lock().expect("placement mutex") = Placement::default();
                }
            }
            None => {
                if let Some(modes) = &self.modes {
                    use crate::shard::mode::ShardMode;
                    for &shard in p.assignments.keys() {
                        let ours = mine.contains(&shard);
                        modes.set(
                            shard,
                            if ours {
                                ShardMode::Led
                            } else {
                                ShardMode::Followed
                            },
                        );
                    }
                }
                self.fence.open_for(&mine, p.term)
            }
        }

        // A shard newly ours may have no local data (its previous owner is gone). Notify the
        // recovery hook so it can rebuild it from the archive; the hook returns promptly and does
        // any download on its own thread, so this does not stall the placement path.
        if let Some(recovery) = self.recovery.get() {
            for &shard in &newly_mine {
                recovery.on_take_ownership(shard);
            }
        }
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

        // Drop cordoned members from *assignment* only — they still voted (they are in `live`),
        // so quorum is unchanged; they simply receive no shards, which drains them. If everyone
        // is cordoned, ignore the cordons rather than leave every shard unowned — an unassigned
        // shard has no leader at all, which is strictly worse than honouring a cordon.
        let cordoned = self
            .cordoned_members
            .lock()
            .expect("cordoned mutex")
            .clone();
        let self_cordoned = self.is_cordoned();
        let eligible: Vec<NodeId> = members
            .iter()
            .copied()
            .filter(|n| !cordoned.contains(n) && !(self_cordoned && *n == self.id))
            .collect();
        let members = if eligible.is_empty() {
            members
        } else {
            eligible
        };

        // Honour operator desired-placement hints: a shard whose preferred host is eligible goes
        // there, the rest stay balanced. `with_preferences` ignores a hint for an ineligible node,
        // so a hint can never leave a shard unowned or doubly owned.
        let preferences = self.preferences.lock().expect("preferences mutex").clone();
        Placement::with_preferences(self.shards.len() as u32, &members, &preferences, term)
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
                //
                // `step_down`, not `close`: it raises the fence to the deposing term as it
                // closes. Heartbeats are handled on every connection thread, so a placement
                // carrying the *old* term can be mid-application right now — and a plain
                // close would be undone the moment that thread reaches `open_for`. Raising
                // the bar makes the late open refuse itself as stale.
                self.fence.step_down(term, why);
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
        let mut cordoned = std::collections::BTreeSet::new();
        // shard → node that wants it. Peers iterate in ascending id order, and we only take the
        // first claimant, so a shard preferred by two nodes deterministically goes to the lower id.
        let mut preferences: std::collections::BTreeMap<crate::shard::ShardId, NodeId> =
            std::collections::BTreeMap::new();
        // This node's own preferences count too (it is not among its peers).
        if !self.is_cordoned() {
            for s in self.preferred_shards() {
                preferences.entry(s).or_insert(self.id);
            }
        }
        for (&peer, addr) in &self.peers {
            self.counters
                .heartbeats_sent
                .fetch_add(1, Ordering::Relaxed);
            let reply: Option<HeartbeatReply> = self.ask(peer, addr, |c| c.heartbeat(&hb));
            let Some(reply) = reply else { continue };
            if reply.ok {
                answered.insert(peer);
            }
            if reply.cordoned {
                cordoned.insert(peer);
            } else {
                for &s in &reply.prefers {
                    preferences.entry(crate::shard::ShardId(s)).or_insert(peer);
                }
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
        *self.cordoned_members.lock().expect("cordoned mutex") = cordoned;
        *self.preferences.lock().expect("preferences mutex") = preferences;
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

        match Client::connect_full(
            addr,
            self.peer_timeout,
            self.peer_timeout,
            self.credentials.clone(),
        ) {
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

        let (mut reply, deposed) = {
            let mut e = self.election.lock().expect("election mutex");
            let was_leader = e.is_leader();
            let reply = e.on_heartbeat(hb, Instant::now())?;
            (reply, was_leader && !e.is_leader())
        };
        // Tell the coordinator whether we are cordoned (drain us) and which shards we would host.
        reply.cordoned = self.is_cordoned();
        reply.prefers = self.preferred_shards().iter().map(|s| s.0).collect();
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
