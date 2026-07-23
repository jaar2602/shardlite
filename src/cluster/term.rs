//! Terms and votes — the durable state election safety rests on.
//!
//! # Why this file is fsynced when the rest of the project is not
//!
//! Election safety reduces to one rule: **a node votes at most once per term**. If a node
//! votes for A, crashes, restarts having forgotten the vote, and then votes for B in the
//! same term, both can reach a majority and two leaders exist at once. Two leaders means two
//! writers on one shard, which is precisely the corruption every other part of this design
//! is built to prevent.
//!
//! So the vote must be durable *before* it is sent — not after, not concurrently. That makes
//! this the one place that pays for a temp-write-fsync-rename cycle: an ordinary
//! `write()` can be torn or sit in the page cache, and a vote that is only *probably* on
//! disk is not a vote at all.
//!
//! # Why a higher term always wins
//!
//! Terms are a logical clock. Seeing a higher term anywhere means this node's information is
//! stale, so it steps down immediately and unconditionally. Nothing about the local state is
//! worth preserving against newer information — trying to defend a stale leadership is how
//! split brain happens.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Error, Result};

/// Which node. Stable across restarts; supplied by configuration.
pub type NodeId = u64;

/// A logical clock. Monotonic, and a higher one always wins.
pub type Term = u64;

const HEADER: &str = "shardlite-term-v1";
const FILE: &str = "term";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct State {
    term: Term,
    /// Who this node voted for in `term`, if anyone. Reset to `None` when the term advances,
    /// because a new term is a new election.
    voted_for: Option<NodeId>,
}

/// Durable term and vote for one node.
#[derive(Debug)]
pub struct TermStore {
    path: PathBuf,
    state: Mutex<State>,
}

impl TermStore {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)
            .map_err(|e| Error::Manifest(format!("creating {}: {e}", dir.display())))?;
        let path = dir.join(FILE);
        let state = if path.exists() {
            Self::read(&path)?
        } else {
            State::default()
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub fn term(&self) -> Term {
        self.state.lock().expect("term mutex").term
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.state.lock().expect("term mutex").voted_for
    }

    /// Advance to `term`, clearing the vote. Returns whether anything changed.
    ///
    /// Refuses to go backwards: a term is a clock, and a clock that can be wound back is not
    /// one. Going backwards would let a node vote twice in what it believes are different
    /// terms but the cluster sees as one.
    pub fn advance_to(&self, term: Term) -> Result<bool> {
        let mut g = self.state.lock().expect("term mutex");
        if term <= g.term {
            return Ok(false);
        }
        let next = State {
            term,
            voted_for: None,
        };
        Self::persist(&self.path, next)?;
        *g = next;
        Ok(true)
    }

    /// Record a vote for `candidate` in `term`, if this node has not already voted otherwise.
    ///
    /// Returns whether the vote was granted. The write is durable before this returns, so a
    /// caller that sends the grant only after seeing `true` can never have promised a vote
    /// it might forget.
    ///
    /// Re-granting to the *same* candidate is allowed and is not a second vote — it is the
    /// idempotent answer to a retried request, which a candidate whose response was lost will
    /// certainly send.
    pub fn grant_vote(&self, term: Term, candidate: NodeId) -> Result<bool> {
        let mut g = self.state.lock().expect("term mutex");

        // A vote in a stale term is meaningless; the candidate is behind.
        if term < g.term {
            return Ok(false);
        }

        // A new term is a new election, so any previous vote no longer applies.
        let mut next = if term > g.term {
            State {
                term,
                voted_for: None,
            }
        } else {
            *g
        };

        match next.voted_for {
            Some(already) if already != candidate => {
                // Already spoken for. Persist the term advance if there was one, so the
                // higher term is not forgotten just because the vote was refused.
                if next.term > g.term {
                    Self::persist(&self.path, next)?;
                    *g = next;
                }
                return Ok(false);
            }
            Some(_) => {
                // The same candidate asking again. Already durable; nothing to write.
                return Ok(true);
            }
            None => {}
        }

        next.voted_for = Some(candidate);
        // Durable *before* returning true. The entire point of this file.
        Self::persist(&self.path, next)?;
        *g = next;
        Ok(true)
    }

    /// Write and fsync, then rename, then fsync the directory.
    ///
    /// Each step matters: without the temp file a crash mid-write leaves a torn record;
    /// without fsyncing the file the rename can expose an empty one; without fsyncing the
    /// directory the rename itself may not survive. A vote that is only probably on disk
    /// cannot be relied on to have happened.
    fn persist(path: &Path, state: State) -> Result<()> {
        let body = format!(
            "{HEADER}\nterm={}\nvoted_for={}\n",
            state.term,
            state
                .voted_for
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into())
        );

        let tmp = path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp)
                .map_err(|e| Error::Manifest(format!("creating {}: {e}", tmp.display())))?;
            f.write_all(body.as_bytes())
                .map_err(|e| Error::Manifest(format!("writing {}: {e}", tmp.display())))?;
            f.sync_all()
                .map_err(|e| Error::Manifest(format!("fsyncing {}: {e}", tmp.display())))?;
        }
        fs::rename(&tmp, path)
            .map_err(|e| Error::Manifest(format!("renaming into {}: {e}", path.display())))?;

        // Fsync the directory so the rename itself is durable.
        if let Some(dir) = path.parent()
            && let Ok(d) = fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn read(path: &Path) -> Result<State> {
        let text = fs::read_to_string(path)
            .map_err(|e| Error::Manifest(format!("reading {}: {e}", path.display())))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != HEADER {
            return Err(Error::Manifest(format!(
                "{} has header `{header}`, expected `{HEADER}`; refusing to guess at a term \
                 file this node does not recognise",
                path.display()
            )));
        }

        let mut s = State::default();
        for line in lines {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            let v = v.trim();
            match k.trim() {
                // A term that will not parse is not recoverable by guessing. Reading it as 0
                // would silently re-run elections this node has already voted in.
                "term" => {
                    s.term = v.parse().map_err(|_| {
                        Error::Manifest(format!("{} has an unparseable term `{v}`", path.display()))
                    })?
                }
                "voted_for" => {
                    s.voted_for = if v == "-" {
                        None
                    } else {
                        Some(v.parse().map_err(|_| {
                            Error::Manifest(format!(
                                "{} has an unparseable vote `{v}`",
                                path.display()
                            ))
                        })?)
                    }
                }
                _ => {}
            }
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn a_node_votes_at_most_once_per_term() {
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();

        assert!(t.grant_vote(1, 7).unwrap(), "the first vote is granted");
        assert!(
            !t.grant_vote(1, 9).unwrap(),
            "a second candidate in the same term must be refused"
        );
        assert_eq!(t.voted_for(), Some(7));
    }

    #[test]
    fn re_asking_gets_the_same_answer_rather_than_a_second_vote() {
        // A candidate whose response was lost will retry. That must be idempotent, not a
        // refusal — refusing would deny a candidate the vote it already legitimately won.
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();
        assert!(t.grant_vote(3, 4).unwrap());
        assert!(t.grant_vote(3, 4).unwrap());
        assert_eq!(t.voted_for(), Some(4));
    }

    #[test]
    fn a_vote_survives_a_restart() {
        // The whole reason this file is fsynced. A node that forgets its vote can vote twice
        // in one term, and two leaders can then exist at once.
        let dir = TempDir::new().unwrap();
        {
            let t = TermStore::open(dir.path()).unwrap();
            assert!(t.grant_vote(5, 2).unwrap());
        }
        let t2 = TermStore::open(dir.path()).unwrap();
        assert_eq!(t2.term(), 5);
        assert_eq!(t2.voted_for(), Some(2));
        assert!(
            !t2.grant_vote(5, 8).unwrap(),
            "the restarted node must remember it already voted"
        );
    }

    #[test]
    fn a_new_term_is_a_new_election() {
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();
        assert!(t.grant_vote(1, 7).unwrap());
        // A later term clears the vote, so a different candidate can win it.
        assert!(t.grant_vote(2, 9).unwrap());
        assert_eq!(t.term(), 2);
        assert_eq!(t.voted_for(), Some(9));
    }

    #[test]
    fn a_stale_term_is_refused_without_losing_the_current_one() {
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();
        t.advance_to(9).unwrap();
        assert!(!t.grant_vote(4, 1).unwrap(), "a stale candidate loses");
        assert_eq!(t.term(), 9, "and cannot drag the term backwards");
        assert_eq!(t.voted_for(), None);
    }

    #[test]
    fn a_refused_vote_still_records_the_higher_term() {
        // Refusing the vote and forgetting the term would let this node vote again in a term
        // it has already seen, which is the same failure as forgetting the vote itself.
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();
        assert!(t.grant_vote(1, 7).unwrap());
        // Term 2 from a candidate: the vote clears, so this is granted.
        assert!(t.grant_vote(2, 8).unwrap());
        // Another candidate in term 2 is refused, but term 2 stays.
        assert!(!t.grant_vote(2, 9).unwrap());

        drop(t);
        let t2 = TermStore::open(dir.path()).unwrap();
        assert_eq!(t2.term(), 2);
        assert_eq!(t2.voted_for(), Some(8));
    }

    #[test]
    fn the_term_never_goes_backwards() {
        let dir = TempDir::new().unwrap();
        let t = TermStore::open(dir.path()).unwrap();
        assert!(t.advance_to(5).unwrap());
        assert!(!t.advance_to(3).unwrap(), "backwards is refused");
        assert!(!t.advance_to(5).unwrap(), "and equal is not a change");
        assert_eq!(t.term(), 5);
    }

    #[test]
    fn a_corrupt_term_file_is_refused_rather_than_guessed_at() {
        // Reading an unparseable term as 0 would silently re-run elections this node has
        // already voted in. Refusing is the only safe answer.
        let dir = TempDir::new().unwrap();
        {
            let t = TermStore::open(dir.path()).unwrap();
            t.grant_vote(4, 1).unwrap();
        }
        std::fs::write(dir.path().join(FILE), format!("{HEADER}\nterm=banana\n")).unwrap();
        let err = TermStore::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("unparseable term"), "{err}");
    }

    #[test]
    fn a_foreign_file_is_refused() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(FILE), "something-else\nterm=3\n").unwrap();
        let err = TermStore::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("expected"), "{err}");
    }
}
