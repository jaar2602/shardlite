//! Raft's election algorithm, and only that.
//!
//! # What is here and what is deliberately not
//!
//! Terms, votes, a heartbeat lease, and the election restriction. There is no replicated log,
//! no log matching, no commit index — frames travel out of band with their own epoch and
//! dense LSN, and [`super::durability`] re-establishes over those positions the guarantee
//! that Raft's log matching would otherwise provide.
//!
//! # The two rules everything rests on
//!
//! 1. **A node votes at most once per term**, durably ([`super::term`]).
//! 2. **A higher term always wins.** Any message carrying a higher term makes this node a
//!    follower immediately, whatever it was doing.
//!
//! Together these give at most one leader per term. The election restriction then makes that
//! leader one that holds every acknowledged write.
//!
//! # A leader that cannot reach a quorum must step down
//!
//! This is the rule that is easy to omit and fatal to omit. Winning an election is not a
//! permanent grant: a leader partitioned away from the cluster still believes it leads, and
//! if it keeps accepting writes there are two writers on one shard. So leadership is a
//! **lease** — it survives only as long as a quorum keeps answering heartbeats, and a leader
//! that loses contact steps itself down without needing to be told. Fencing
//! ([`super::fence`]) is the second line of defence for the window before it notices; this is
//! the first.
//!
//! # Time is injected
//!
//! Every method that cares about time takes `now`. Elections are all about timeouts, and
//! tests that depend on real sleeping are both slow and flaky — the interesting cases here
//! are precisely the ones that need the clock moved by hand.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::Result;

use super::durability::Durability;
use super::term::{NodeId, Term, TermStore};

#[derive(Debug, Clone)]
pub struct ElectionConfig {
    pub node: NodeId,
    /// Every other voting member. This node is implicitly a member.
    pub peers: Vec<NodeId>,
    /// How long without hearing from a leader before standing for election. The actual value
    /// used is jittered per node so that peers do not all time out together and split the
    /// vote.
    pub election_timeout: Duration,
    /// How often a leader sends heartbeats. Must be comfortably below `election_timeout`, or
    /// followers time out a healthy leader.
    pub heartbeat_interval: Duration,
}

impl ElectionConfig {
    /// Sized for the floor profile: failover inside the 5 s budget, without heartbeats being
    /// so frequent that they cost meaningful CPU on 0.33 of a core.
    pub fn floor(node: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            node,
            peers,
            election_timeout: Duration::from_millis(1500),
            heartbeat_interval: Duration::from_millis(300),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.heartbeat_interval * 3 > self.election_timeout {
            return Err(crate::error::Error::ClusterConfig(format!(
                "heartbeat_interval {:?} is too close to election_timeout {:?}; a follower \
                 would time out a healthy leader after one or two lost packets. Keep the \
                 heartbeat at most a third of the timeout.",
                self.heartbeat_interval, self.election_timeout
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate: NodeId,
    /// How up to date the candidate is. A voter refuses anyone that cannot show it holds the
    /// voter's own writes.
    pub durability: Durability,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VoteReply {
    pub term: Term,
    pub granted: bool,
    /// Why, when refused. A stalled election with no explanation is the worst possible
    /// operational state — the cluster is down and nothing says which node is holding out.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Heartbeat {
    pub term: Term,
    pub leader: NodeId,
    /// Who leads which shard. Carried on the heartbeat rather than fetched separately: the
    /// heartbeat already proves the sender is the current coordinator, so the map arrives
    /// with its authority attached and there is no window where a node has one without the
    /// other.
    pub placement: super::placement::Placement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeartbeatReply {
    pub term: Term,
    /// False when the sender is stale — it has been deposed and does not know yet.
    pub ok: bool,
    /// The replier has been cordoned by an operator: keep counting it for quorum, but do not
    /// assign it new shards. Set by [`super::node::ClusterNode::handle_heartbeat`] (the election
    /// itself has no view of node-level operator state). Defaults false for older peers.
    #[serde(default)]
    pub cordoned: bool,
    /// Shards this node is the operator-preferred host for. The coordinator honours these in
    /// placement when the replier is eligible (see `Placement::with_preferences`). Empty by
    /// default; set alongside `cordoned` in `handle_heartbeat`.
    #[serde(default)]
    pub prefers: Vec<u32>,
}

/// What the caller must do as a result of a state change. Returned rather than performed, so
/// the state machine stays free of I/O and therefore testable without a network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Stand for election: send `VoteRequest` to every peer.
    RequestVotes(Term),
    /// Send a heartbeat to every peer.
    Heartbeat(Term),
    /// This node just became leader for `term`. Start the writers.
    BecameLeader(Term),
    /// This node is no longer leader. **Stop writing before doing anything else.**
    SteppedDown { term: Term, why: String },
}

#[derive(Debug)]
pub struct Election {
    cfg: ElectionConfig,
    terms: TermStore,
    role: Role,
    leader: Option<NodeId>,
    /// When this node last heard from a live leader, or last renewed its own lease.
    last_contact: Instant,
    /// When a leader last sent heartbeats, so it does not send them every tick.
    last_heartbeat: Instant,
    /// Votes received this term, including this node's own.
    votes: BTreeSet<NodeId>,
    /// Peers that have acknowledged this node's leadership since `lease_start`.
    acks: BTreeSet<NodeId>,
    lease_start: Instant,
    /// After a voluntary step-down, don't campaign until this instant — so a peer's election
    /// timer fires first and takes leadership, instead of this node immediately re-winning. Safe:
    /// it only *delays* this node, and if no peer steps up the delay lapses and it campaigns again
    /// (better a leader late than none).
    stepdown_backoff: Option<Instant>,
}

impl Election {
    pub fn new(cfg: ElectionConfig, terms: TermStore, now: Instant) -> Result<Self> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            terms,
            role: Role::Follower,
            leader: None,
            last_contact: now,
            last_heartbeat: now,
            votes: BTreeSet::new(),
            acks: BTreeSet::new(),
            lease_start: now,
            stepdown_backoff: None,
        })
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn election_timeout(&self) -> Duration {
        self.cfg.election_timeout
    }

    pub fn term(&self) -> Term {
        self.terms.term()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Members that must agree for a decision to carry: a strict majority of the cluster,
    /// this node included.
    ///
    /// Not `div_ceil`, despite the shape — clippy's suggestion is a different function. A
    /// majority of N is `N / 2 + 1`, which for N = 6 is 4; `5.div_ceil(2)` is 3, and a
    /// "quorum" of 3 in a 6-node cluster would let two halves both elect a leader.
    #[allow(clippy::manual_div_ceil)]
    fn quorum(&self) -> usize {
        (self.cfg.peers.len() + 1) / 2 + 1
    }

    /// This node's election timeout, jittered deterministically.
    ///
    /// Without jitter every follower times out at the same instant, all stand for election
    /// together, all split the vote, and the cluster can fail to elect for several rounds.
    /// The jitter is derived from `(node, term)` rather than a random source: it differs
    /// across nodes and across retries, which is all that is needed, and it keeps this state
    /// machine deterministic and so testable.
    fn timeout(&self) -> Duration {
        let base = self.cfg.election_timeout;
        let mut h = self.cfg.node.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= self.terms.term().wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 31;
        // Spread over [base, 2 * base).
        base + Duration::from_nanos(h % base.as_nanos().max(1) as u64)
    }

    /// Advance time. Returns what the caller must do.
    pub fn tick(&mut self, now: Instant, durability: &Durability) -> Result<Option<Action>> {
        match self.role {
            Role::Follower | Role::Candidate => {
                // Honour a voluntary step-down back-off: stay put so a peer takes leadership first.
                if let Some(until) = self.stepdown_backoff {
                    if now < until {
                        return Ok(None);
                    }
                    self.stepdown_backoff = None;
                }
                if now.duration_since(self.last_contact) >= self.timeout() {
                    return self.stand_for_election(now, durability);
                }
                Ok(None)
            }
            Role::Leader => {
                // The lease. A leader that has not heard from a quorum since the lease began
                // has no evidence it still leads, and must stop before it writes anything
                // else. This is what makes a partitioned leader safe rather than merely
                // unlikely.
                if now.duration_since(self.lease_start) >= self.cfg.election_timeout
                    && self.acks.len() + 1 < self.quorum()
                {
                    let why = format!(
                        "lease expired: only {} of {} members acknowledged this leader within \
                         {:?}; stepping down rather than writing without a quorum",
                        self.acks.len() + 1,
                        self.cfg.peers.len() + 1,
                        self.cfg.election_timeout
                    );
                    return Ok(Some(self.step_down(why, now)));
                }

                if now.duration_since(self.last_heartbeat) >= self.cfg.heartbeat_interval {
                    self.last_heartbeat = now;
                    return Ok(Some(Action::Heartbeat(self.terms.term())));
                }
                Ok(None)
            }
        }
    }

    /// Operator-requested voluntary step-down: relinquish leadership now, and back off from
    /// re-campaigning for a couple of election timeouts so a peer takes over instead of this node
    /// immediately re-winning. Returns the `SteppedDown` action to perform (fence release, counter),
    /// or `None` if this node is not currently the leader (nothing to give up). It never picks the
    /// successor or forces a term — the ordinary election does that, so there is never two leaders.
    pub fn request_step_down(&mut self, now: Instant) -> Option<Action> {
        if self.role != Role::Leader {
            return None;
        }
        self.stepdown_backoff = Some(now + self.cfg.election_timeout * 2);
        Some(self.step_down("operator requested step-down".into(), now))
    }

    fn stand_for_election(
        &mut self,
        now: Instant,
        durability: &Durability,
    ) -> Result<Option<Action>> {
        let next = self.terms.term() + 1;
        self.terms.advance_to(next)?;
        // Vote for self, durably, exactly as for any other candidate.
        self.terms.grant_vote(next, self.cfg.node)?;

        self.role = Role::Candidate;
        self.leader = None;
        self.last_contact = now;
        self.votes.clear();
        self.votes.insert(self.cfg.node);
        let _ = durability;

        tracing::info!(node = self.cfg.node, term = next, "standing for election");

        // A single-node cluster is its own quorum and wins immediately.
        if self.votes.len() >= self.quorum() {
            return Ok(Some(self.become_leader(now)));
        }
        Ok(Some(Action::RequestVotes(next)))
    }

    /// Handle a peer standing for election.
    pub fn on_vote_request(
        &mut self,
        req: &VoteRequest,
        now: Instant,
        mine: &Durability,
    ) -> Result<VoteReply> {
        // A higher term deposes this node before anything else is considered.
        if req.term > self.terms.term() {
            self.observe_term(
                req.term,
                now,
                format!("candidate {} has a higher term", req.candidate),
            )?;
        }

        let current = self.terms.term();
        if req.term < current {
            return Ok(VoteReply {
                term: current,
                granted: false,
                reason: format!("stale term {}; this node is on {current}", req.term),
            });
        }

        // The election restriction. Checked *before* recording a vote, so a candidate that
        // cannot lead never consumes this node's single vote for the term — otherwise one
        // unqualified candidate could block a qualified one for a whole term.
        let comparison = req.durability.compare_to(mine);
        if !matches!(comparison, super::durability::Comparison::AtLeast) {
            let reason = comparison.why();
            tracing::info!(
                node = self.cfg.node,
                candidate = req.candidate,
                term = req.term,
                %reason,
                "refusing a vote"
            );
            return Ok(VoteReply {
                term: current,
                granted: false,
                reason,
            });
        }

        let granted = self.terms.grant_vote(req.term, req.candidate)?;
        if granted {
            // Only a granted vote defers the timeout. Refusing one is not evidence that a
            // leader exists, so it must not keep this node from standing itself.
            self.last_contact = now;
        }
        Ok(VoteReply {
            term: self.terms.term(),
            granted,
            reason: if granted {
                String::new()
            } else {
                format!(
                    "already voted for {:?} in term {}",
                    self.terms.voted_for(),
                    req.term
                )
            },
        })
    }

    /// Handle a reply to this node's own vote request.
    pub fn on_vote_reply(
        &mut self,
        from: NodeId,
        reply: &VoteReply,
        now: Instant,
    ) -> Result<Option<Action>> {
        if reply.term > self.terms.term() {
            return Ok(Some(self.observe_term(
                reply.term,
                now,
                format!("voter {from} is on a higher term"),
            )?));
        }
        // A reply for a term this node has moved on from says nothing about the current one.
        if self.role != Role::Candidate || reply.term != self.terms.term() {
            return Ok(None);
        }
        if !reply.granted {
            return Ok(None);
        }

        self.votes.insert(from);
        if self.votes.len() >= self.quorum() {
            return Ok(Some(self.become_leader(now)));
        }
        Ok(None)
    }

    /// Handle a heartbeat from a node claiming leadership.
    pub fn on_heartbeat(&mut self, hb: &Heartbeat, now: Instant) -> Result<HeartbeatReply> {
        let current = self.terms.term();
        if hb.term < current {
            // The sender has been deposed and does not know. Telling it the real term is what
            // makes it step down promptly instead of waiting for its own lease to expire.
            return Ok(HeartbeatReply {
                term: current,
                ok: false,
                cordoned: false,
                prefers: Vec::new(),
            });
        }

        if hb.term > current {
            self.observe_term(
                hb.term,
                now,
                format!("leader {} has a higher term", hb.leader),
            )?;
        }

        // A heartbeat in the current term settles the question: there is a leader, so this
        // node is a follower, even if it was mid-election.
        self.role = Role::Follower;
        self.leader = Some(hb.leader);
        self.last_contact = now;
        Ok(HeartbeatReply {
            term: self.terms.term(),
            ok: true,
            cordoned: false,
            prefers: Vec::new(),
        })
    }

    /// Handle a peer's answer to this node's heartbeat. Renews the lease.
    pub fn on_heartbeat_reply(
        &mut self,
        from: NodeId,
        reply: &HeartbeatReply,
        now: Instant,
    ) -> Result<Option<Action>> {
        if reply.term > self.terms.term() {
            return Ok(Some(self.observe_term(
                reply.term,
                now,
                format!("peer {from} is on a higher term"),
            )?));
        }
        if self.role != Role::Leader || !reply.ok {
            return Ok(None);
        }

        self.acks.insert(from);
        // A quorum has confirmed this leader, so the lease restarts from now.
        if self.acks.len() + 1 >= self.quorum() {
            self.lease_start = now;
            self.last_contact = now;
            self.acks.clear();
        }
        Ok(None)
    }

    /// Adopt a higher term, stepping down if this node was leading.
    ///
    /// Deliberately does **not** defer this node's election timer. Seeing a higher term means
    /// someone is campaigning, not that a leader exists — and a candidate that can never win,
    /// because the election restriction refuses it, would otherwise suppress the whole cluster
    /// forever: each time it stands it bumps the term, every peer resets its timer, and no
    /// qualified node ever gets to stand. The timer is deferred only by evidence a leader
    /// actually exists (a heartbeat) or by this node committing its vote.
    fn observe_term(&mut self, term: Term, now: Instant, why: String) -> Result<Action> {
        self.terms.advance_to(term)?;
        self.votes.clear();
        if self.role == Role::Leader {
            // A deposed leader does get a fresh timeout, so it settles as a follower rather
            // than immediately campaigning and inflating the term further.
            return Ok(self.step_down(why, now));
        }
        self.role = Role::Follower;
        self.leader = None;
        Ok(Action::SteppedDown {
            term: self.terms.term(),
            why,
        })
    }

    fn become_leader(&mut self, now: Instant) -> Action {
        let term = self.terms.term();
        self.role = Role::Leader;
        self.leader = Some(self.cfg.node);
        self.lease_start = now;
        self.last_contact = now;
        // Heartbeat immediately rather than after one interval, so followers learn about the
        // new leader without waiting out a timeout they might otherwise act on.
        self.last_heartbeat = now - self.cfg.heartbeat_interval;
        self.acks.clear();
        tracing::info!(node = self.cfg.node, term, "became leader");
        Action::BecameLeader(term)
    }

    fn step_down(&mut self, why: String, now: Instant) -> Action {
        let term = self.terms.term();
        tracing::warn!(node = self.cfg.node, term, %why, "stepping down");
        self.role = Role::Follower;
        self.leader = None;
        self.last_contact = now;
        self.votes.clear();
        self.acks.clear();
        Action::SteppedDown { term, why }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct Node {
        e: Election,
        _dir: TempDir,
    }

    fn node(id: NodeId, peers: Vec<NodeId>, now: Instant) -> Node {
        let dir = TempDir::new().unwrap();
        let terms = TermStore::open(dir.path()).unwrap();
        Node {
            e: Election::new(ElectionConfig::floor(id, peers), terms, now).unwrap(),
            _dir: dir,
        }
    }

    /// Long enough that any node's jittered timeout has certainly elapsed.
    fn past_timeout() -> Duration {
        ElectionConfig::floor(0, vec![]).election_timeout * 3
    }

    #[test]
    fn a_follower_that_hears_nothing_stands_for_election() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);

        assert_eq!(n.e.tick(t0, &d).unwrap(), None, "not yet");
        let action = n.e.tick(t0 + past_timeout(), &d).unwrap();
        assert_eq!(action, Some(Action::RequestVotes(1)));
        assert_eq!(n.e.role(), Role::Candidate);
        assert_eq!(n.e.term(), 1);
    }

    #[test]
    fn a_candidate_with_a_majority_becomes_leader() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap();

        // One peer of two is a majority in a three-node cluster, with this node's own vote.
        let reply = VoteReply {
            term: 1,
            granted: true,
            reason: String::new(),
        };
        let action = n.e.on_vote_reply(2, &reply, t0).unwrap();
        assert_eq!(action, Some(Action::BecameLeader(1)));
        assert!(n.e.is_leader());
    }

    #[test]
    fn a_candidate_without_a_majority_does_not_lead() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3, 4, 5], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap();

        // Two of five, including itself: short of the three needed.
        let granted = VoteReply {
            term: 1,
            granted: true,
            reason: String::new(),
        };
        assert_eq!(n.e.on_vote_reply(2, &granted, t0).unwrap(), None);
        assert_eq!(n.e.role(), Role::Candidate);
        assert!(!n.e.is_leader());
    }

    #[test]
    fn a_single_node_cluster_elects_itself() {
        let t0 = Instant::now();
        let mut n = node(1, vec![], t0);
        let d = Durability::new(1);
        let action = n.e.tick(t0 + past_timeout(), &d).unwrap();
        assert_eq!(action, Some(Action::BecameLeader(1)));
    }

    #[test]
    fn only_one_leader_can_exist_per_term() {
        // The property the whole file is for. Two candidates in one term, three voters:
        // whoever asks second is refused, because each voter has one vote per term.
        let t0 = Instant::now();
        let mut voter = node(3, vec![1, 2], t0);
        let d = Durability::new(1);

        let a = VoteRequest {
            term: 1,
            candidate: 1,
            durability: d.clone(),
        };
        let b = VoteRequest {
            term: 1,
            candidate: 2,
            durability: d.clone(),
        };

        assert!(voter.e.on_vote_request(&a, t0, &d).unwrap().granted);
        let second = voter.e.on_vote_request(&b, t0, &d).unwrap();
        assert!(!second.granted, "one vote per term");
        assert!(second.reason.contains("already voted"), "{}", second.reason);
    }

    #[test]
    fn a_candidate_that_is_behind_is_refused() {
        // The election restriction. Electing this candidate would lose the voter's writes.
        let t0 = Instant::now();
        let mut voter = node(3, vec![1, 2], t0);
        let mine = Durability::new(1).with(crate::shard::ShardId(0), 50);
        let req = VoteRequest {
            term: 1,
            candidate: 1,
            durability: Durability::new(1).with(crate::shard::ShardId(0), 10),
        };

        let reply = voter.e.on_vote_request(&req, t0, &mine).unwrap();
        assert!(!reply.granted);
        assert!(reply.reason.contains("lose writes"), "{}", reply.reason);
    }

    #[test]
    fn refusing_an_unqualified_candidate_does_not_spend_the_vote() {
        // If the restriction were checked after recording the vote, one candidate that is
        // behind could consume every voter's single vote and block a qualified candidate for
        // the whole term — a cluster that cannot elect for no visible reason.
        let t0 = Instant::now();
        let mut voter = node(3, vec![1, 2], t0);
        let mine = Durability::new(1).with(crate::shard::ShardId(0), 50);

        let behind = VoteRequest {
            term: 1,
            candidate: 1,
            durability: Durability::new(1).with(crate::shard::ShardId(0), 10),
        };
        assert!(!voter.e.on_vote_request(&behind, t0, &mine).unwrap().granted);

        let qualified = VoteRequest {
            term: 1,
            candidate: 2,
            durability: Durability::new(1).with(crate::shard::ShardId(0), 50),
        };
        assert!(
            voter
                .e
                .on_vote_request(&qualified, t0, &mine)
                .unwrap()
                .granted,
            "the qualified candidate must still be able to win this term"
        );
    }

    #[test]
    fn a_higher_term_deposes_a_leader() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap();
        n.e.on_vote_reply(
            2,
            &VoteReply {
                term: 1,
                granted: true,
                reason: String::new(),
            },
            t0,
        )
        .unwrap();
        assert!(n.e.is_leader());

        // A heartbeat from a newer leader ends this one, immediately.
        let reply =
            n.e.on_heartbeat(
                &Heartbeat {
                    term: 5,
                    leader: 2,
                    placement: Default::default(),
                },
                t0,
            )
            .unwrap();
        assert!(reply.ok);
        assert_eq!(n.e.role(), Role::Follower);
        assert_eq!(n.e.term(), 5);
        assert_eq!(n.e.leader(), Some(2));
    }

    #[test]
    fn a_stale_heartbeat_is_refused_and_tells_the_sender_the_real_term() {
        // How a deposed leader finds out promptly rather than waiting out its own lease.
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap(); // term 1
        n.e.tick(t0 + past_timeout() * 2, &d).unwrap(); // term 2

        let reply =
            n.e.on_heartbeat(
                &Heartbeat {
                    term: 1,
                    leader: 9,
                    placement: Default::default(),
                },
                t0,
            )
            .unwrap();
        assert!(!reply.ok);
        assert_eq!(reply.term, 2, "the sender must learn the real term");
    }

    #[test]
    fn a_leader_that_loses_its_quorum_steps_down() {
        // The rule that makes a partitioned leader safe. Without it the leader keeps
        // accepting writes on one side of a partition while a new leader accepts them on the
        // other — two writers on one shard, which is exactly what this project exists to
        // prevent.
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap();
        n.e.on_vote_reply(
            2,
            &VoteReply {
                term: 1,
                granted: true,
                reason: String::new(),
            },
            t0,
        )
        .unwrap();
        assert!(n.e.is_leader());

        // Peers go silent: heartbeats are sent, nothing answers.
        let timeout = n.e.cfg.election_timeout;
        let mut t = t0;
        let mut stepped = None;
        for _ in 0..20 {
            t += Duration::from_millis(200);
            if let Some(Action::SteppedDown { why, .. }) = n.e.tick(t, &d).unwrap() {
                stepped = Some(why);
                break;
            }
        }
        let why = stepped.expect("a leader with no quorum must step down");
        assert!(why.contains("lease expired"), "{why}");
        assert!(!n.e.is_leader());
        assert!(
            t >= t0 + timeout,
            "it must not step down before the lease is up"
        );
    }

    #[test]
    fn a_leader_answered_by_a_quorum_keeps_its_lease() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);
        n.e.tick(t0 + past_timeout(), &d).unwrap();
        n.e.on_vote_reply(
            2,
            &VoteReply {
                term: 1,
                granted: true,
                reason: String::new(),
            },
            t0,
        )
        .unwrap();

        let ok = HeartbeatReply {
            term: 1,
            ok: true,
            cordoned: false,
            prefers: Vec::new(),
        };
        let mut t = t0;
        for _ in 0..30 {
            t += Duration::from_millis(200);
            n.e.tick(t, &d).unwrap();
            n.e.on_heartbeat_reply(2, &ok, t).unwrap();
        }
        assert!(
            n.e.is_leader(),
            "a leader a quorum keeps answering must keep leading"
        );
    }

    #[test]
    fn a_leader_sends_heartbeats_on_its_interval() {
        let t0 = Instant::now();
        let mut n = node(1, vec![], t0);
        let d = Durability::new(1);
        assert_eq!(
            n.e.tick(t0 + past_timeout(), &d).unwrap(),
            Some(Action::BecameLeader(1))
        );

        let t1 = t0 + past_timeout();
        assert_eq!(
            n.e.tick(t1, &d).unwrap(),
            Some(Action::Heartbeat(1)),
            "a new leader heartbeats at once rather than after one interval"
        );
        assert_eq!(n.e.tick(t1, &d).unwrap(), None, "and not again immediately");
        assert_eq!(
            n.e.tick(t1 + Duration::from_millis(300), &d).unwrap(),
            Some(Action::Heartbeat(1))
        );
    }

    #[test]
    fn a_heartbeat_stops_a_follower_standing_for_election() {
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let d = Durability::new(1);

        let mut t = t0;
        for _ in 0..10 {
            t += Duration::from_millis(400);
            n.e.on_heartbeat(
                &Heartbeat {
                    term: 1,
                    leader: 2,
                    placement: Default::default(),
                },
                t,
            )
            .unwrap();
            assert_eq!(n.e.tick(t, &d).unwrap(), None);
        }
        assert_eq!(n.e.role(), Role::Follower);
        assert_eq!(n.e.leader(), Some(2));
    }

    #[test]
    fn a_refused_vote_does_not_defer_this_nodes_own_election() {
        // Being asked for a vote is not evidence a leader exists. Treating it as contact
        // would let a partitioned candidate keep the rest of the cluster from ever electing.
        let t0 = Instant::now();
        let mut n = node(1, vec![2, 3], t0);
        let mine = Durability::new(1).with(crate::shard::ShardId(0), 99);

        // The candidate keeps standing, bumping the term each time — exactly what an
        // unqualified candidate does. Run well past the jitter ceiling of 2x the base.
        let mut t = t0;
        for term in 1..=12 {
            t += Duration::from_millis(400);
            let behind = VoteRequest {
                term,
                candidate: 2,
                durability: Durability::new(1).with(crate::shard::ShardId(0), 1),
            };
            assert!(!n.e.on_vote_request(&behind, t, &mine).unwrap().granted);
        }
        let action = n.e.tick(t, &mine).unwrap();
        assert!(
            matches!(action, Some(Action::RequestVotes(_))),
            "this node should have stood for election by now, got {action:?}"
        );
    }

    #[test]
    fn jitter_keeps_peers_from_timing_out_together() {
        // Without this, every follower stands at the same instant, splits the vote, and the
        // cluster can fail to elect for several rounds.
        let t0 = Instant::now();
        let a = node(1, vec![2, 3], t0);
        let b = node(2, vec![1, 3], t0);
        let c = node(3, vec![1, 2], t0);
        let (x, y, z) = (a.e.timeout(), b.e.timeout(), c.e.timeout());
        assert!(x != y || y != z, "timeouts must differ: {x:?} {y:?} {z:?}");
        for t in [x, y, z] {
            let base = a.e.cfg.election_timeout;
            assert!(t >= base && t < base * 2, "{t:?} outside [base, 2*base)");
        }
    }

    #[test]
    fn a_heartbeat_faster_than_a_third_of_the_timeout_is_refused() {
        // A heartbeat too close to the timeout means one or two lost packets depose a
        // healthy leader. Failing at construction beats discovering it as election churn.
        let dir = TempDir::new().unwrap();
        let terms = TermStore::open(dir.path()).unwrap();
        let cfg = ElectionConfig {
            node: 1,
            peers: vec![2, 3],
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(200),
        };
        let err = Election::new(cfg, terms, Instant::now()).unwrap_err();
        assert!(err.to_string().contains("too close"), "{err}");
    }
}
