//! Quorum replication for catalog values over the existing cluster transport.
//!
//! A value is chosen once a strict majority has fsynced the exact proposal. Commit messages make
//! it visible; they can be retried because each voter retains its prepared value. Election
//! durability includes the latest prepared catalog digest, so a candidate that could forget a
//! quorum-chosen value cannot win.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::net::Client;

use super::{Catalog, CatalogProposal, CatalogStore, ClusterNode};

pub struct CatalogQuorum {
    cluster: Arc<ClusterNode>,
    store: Arc<CatalogStore>,
    timeout: Duration,
    credentials: Option<(String, crate::net::auth::Key)>,
}

impl CatalogQuorum {
    pub fn new(cluster: Arc<ClusterNode>, store: Arc<CatalogStore>) -> Self {
        Self {
            cluster,
            store,
            timeout: Duration::from_millis(500),
            credentials: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_credentials(mut self, name: &str, secret: &str) -> Self {
        self.credentials = Some((name.to_string(), crate::net::auth::derive_key(secret)));
        self
    }

    pub fn snapshot(&self) -> Catalog {
        self.store.snapshot()
    }

    /// Apply against the latest local version. Catalog commands are serialized by the store; a
    /// concurrent winner is reported as a stale command and the caller retries from fresh state.
    pub fn change_current<T>(&self, change: impl FnOnce(&mut Catalog) -> Result<T>) -> Result<T> {
        let version = self.store.snapshot().version;
        self.change(version, change)
    }

    /// Best-effort repair for members that missed catalog publication.
    ///
    /// Voters first receive an idempotent prepare/commit replay from [`Self::republish_committed`].
    /// Snapshot installation then lets a member that was offline for several catalog versions
    /// catch up without requiring an unbounded metadata log.
    pub fn repair_members(&self) -> Result<()> {
        self.require_leader()?;
        let catalog = self.store.snapshot();
        let term = self.cluster.term();
        for member in catalog
            .members
            .values()
            .filter(|member| member.node != self.cluster.id())
        {
            let Some(mut client) = self.connect(&member.address) else {
                continue;
            };
            let _ = client.catalog_install(self.cluster.id(), term, &catalog);
        }
        Ok(())
    }

    /// Build, quorum-prepare, and publish one catalog state-machine command.
    pub fn change<T>(
        &self,
        expected_version: u64,
        change: impl FnOnce(&mut Catalog) -> Result<T>,
    ) -> Result<T> {
        self.require_leader()?;
        let (output, proposal) =
            self.store
                .propose(self.cluster.term(), expected_version, change)?;
        self.commit_proposal(proposal)?;
        Ok(output)
    }

    /// Retry an exact proposal after a timeout. Sending a different value in the same term/version
    /// is refused by every voter, including this one.
    pub fn commit_proposal(&self, proposal: CatalogProposal) -> Result<()> {
        self.require_leader()?;
        let term = self.cluster.term();
        if proposal.term != term {
            return Err(Error::ClusterConfig(format!(
                "catalog proposal is from term {}, current leader is in term {term}; recover it \
                 into the current term before retrying",
                proposal.term
            )));
        }

        let current = self.store.snapshot();
        let advances_catalog = proposal.catalog.version > current.version;
        // The entry that *enters* joint consensus must itself be accepted by a majority of both
        // old and new sets. Committing it under only the old quorum and then crashing could leave
        // the new voters without the joint value, while the surviving nodes require them to elect
        // a leader. Once joint consensus is active, current.voter_sets() already returns both.
        let voter_sets = match (
            current.voter_transition.as_ref(),
            proposal.catalog.voter_transition.as_ref(),
        ) {
            (None, Some(transition)) => (transition.old.clone(), Some(transition.new.clone())),
            _ => current.voter_sets(),
        };
        let mut voters = voter_sets.0.clone();
        if let Some(next) = &voter_sets.1 {
            voters.extend(next);
        }
        self.store.prepare(proposal.clone())?;
        if advances_catalog {
            crate::failpoint::hit("catalog.after_local_prepare");
        }
        let mut prepared = BTreeSet::new();
        if voters.contains(&self.cluster.id()) {
            prepared.insert(self.cluster.id());
        }
        for peer in voters
            .iter()
            .copied()
            .filter(|peer| *peer != self.cluster.id())
        {
            let Some(address) = current
                .members
                .get(&peer)
                .map(|member| member.address.as_str())
            else {
                continue;
            };
            let Some(mut client) = self.connect(address) else {
                continue;
            };
            if client.catalog_prepare(self.cluster.id(), &proposal).is_ok() {
                prepared.insert(peer);
            }
        }
        if !has_joint_quorum(&prepared, &voter_sets.0, voter_sets.1.as_ref()) {
            return Err(Error::ClusterConfig(format!(
                "catalog version {} was prepared by {:?}, short of the required voter quorum(s). \
                 It remains durable but uncommitted and may be retried with the same proposal.",
                proposal.catalog.version, prepared,
            )));
        }

        // A prepared majority makes the value chosen. Publish locally first, then best-effort to
        // every voter. A missed remote commit is repairable from its prepared copy; it cannot elect
        // a candidate that lacks the chosen digest.
        self.store
            .commit_prepared(proposal.term, proposal.catalog.version)?;
        if advances_catalog {
            crate::failpoint::hit("catalog.after_local_commit");
        }
        self.cluster.refresh_catalog_voters()?;
        let mut published = usize::from(voters.contains(&self.cluster.id()));
        for peer in voters
            .iter()
            .copied()
            .filter(|peer| *peer != self.cluster.id())
        {
            let Some(address) = current
                .members
                .get(&peer)
                .map(|member| member.address.as_str())
            else {
                continue;
            };
            let Some(mut client) = self.connect(address) else {
                continue;
            };
            if client
                .catalog_commit(self.cluster.id(), proposal.term, proposal.catalog.version)
                .is_ok()
            {
                published += 1;
            }
        }
        // Storage members do not vote, but routing and placement still have to reach them. They
        // may jump directly to the chosen snapshot; voters never use this path.
        for member in proposal
            .catalog
            .members
            .values()
            .filter(|member| member.node != self.cluster.id() && !voters.contains(&member.node))
        {
            let Some(mut client) = self.connect(&member.address) else {
                continue;
            };
            let _ = client.catalog_install(self.cluster.id(), proposal.term, &proposal.catalog);
        }
        tracing::info!(
            version = proposal.catalog.version,
            term = proposal.term,
            prepared = prepared.len(),
            published,
            voters = voters.len(),
            "catalog value committed"
        );
        Ok(())
    }

    /// Re-propose this node's durable pending value in a newly elected term.
    pub fn recover_prepared(&self) -> Result<bool> {
        self.require_leader()?;
        let Some(previous) = self.store.prepared_snapshot() else {
            return Ok(false);
        };
        let proposal = CatalogProposal {
            term: self.cluster.term(),
            expected_version: previous.expected_version,
            catalog: previous.catalog,
        };
        self.commit_proposal(proposal)?;
        Ok(true)
    }

    /// Re-publish the current committed value in this leader's term. This repairs voters that
    /// prepared the value but missed its old leader's commit message, and voters exactly one
    /// version behind.
    pub fn republish_committed(&self) -> Result<()> {
        self.require_leader()?;
        let catalog = self.store.snapshot();
        let proposal = CatalogProposal {
            term: self.cluster.term(),
            expected_version: catalog.version.saturating_sub(1),
            catalog,
        };
        self.commit_proposal(proposal)
    }

    fn require_leader(&self) -> Result<()> {
        let store_position = self.store.durability_position();
        if self.cluster.catalog_position() != Some(store_position) {
            return Err(Error::ClusterConfig(
                "catalog quorum is not attached to this node's election durability; refusing a \
                 mutation that a stale candidate could forget"
                    .into(),
            ));
        }
        if !self.cluster.is_leader() {
            return Err(Error::ClusterConfig(format!(
                "node {} is not the catalog leader; current leader is {:?}",
                self.cluster.id(),
                self.cluster.leader()
            )));
        }
        Ok(())
    }

    fn connect(&self, address: &str) -> Option<Client> {
        match Client::connect_full(
            address,
            self.timeout,
            self.timeout,
            self.credentials.clone(),
        ) {
            Ok(client) => Some(client),
            Err(error) => {
                tracing::debug!(address, %error, "catalog voter is unreachable");
                None
            }
        }
    }
}

fn has_joint_quorum(
    acknowledgements: &BTreeSet<u64>,
    current: &BTreeSet<u64>,
    next: Option<&BTreeSet<u64>>,
) -> bool {
    let majority =
        |set: &BTreeSet<u64>| acknowledgements.intersection(set).count() >= set.len() / 2 + 1;
    majority(current) && next.is_none_or(majority)
}

impl std::fmt::Debug for CatalogQuorum {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogQuorum")
            .field("node", &self.cluster.id())
            .field("version", &self.store.snapshot().version)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        ClusterId, Compatibility, Durability, DurabilitySource, Election, ElectionConfig, Fence,
        MemberRole, TermStore,
    };
    use crate::shard::{LinearV1, Routing, ShardId};
    use tempfile::TempDir;

    struct EmptyDurability;

    impl DurabilitySource for EmptyDurability {
        fn durability(&self) -> Durability {
            Durability::new(0)
        }
    }

    fn single_voter() -> (TempDir, Arc<CatalogStore>, Arc<ClusterNode>) {
        let dir = TempDir::new().unwrap();
        let routing = Routing::LinearV1(LinearV1::ONE);
        let mut catalog = Catalog::bootstrap(
            ClusterId([4; 16]),
            1,
            "node-1:4600".into(),
            routing,
            Compatibility::current(routing).unwrap(),
        )
        .unwrap();
        catalog.initialize_placements(1).unwrap();
        let store = Arc::new(CatalogStore::create(dir.path(), catalog).unwrap());
        let now = std::time::Instant::now();
        let election = Election::new(
            ElectionConfig {
                node: 1,
                peers: vec![],
                election_timeout: Duration::from_millis(30),
                heartbeat_interval: Duration::from_millis(10),
            },
            TermStore::open(&dir.path().join("cluster")).unwrap(),
            now,
        )
        .unwrap();
        let cluster = Arc::new(
            ClusterNode::new(
                1,
                election,
                Arc::new(Fence::new(1, 0)),
                Default::default(),
                Arc::new(EmptyDurability),
                vec![ShardId(0)],
            )
            .with_catalog(Arc::clone(&store)),
        );
        (dir, store, cluster)
    }

    #[test]
    fn a_single_voter_commits_a_catalog_change_locally() {
        let (_dir, store, cluster) = single_voter();
        let now = std::time::Instant::now();
        cluster.tick_once(now + Duration::from_secs(1)).unwrap();
        assert!(cluster.is_leader());

        let quorum = CatalogQuorum::new(Arc::clone(&cluster), Arc::clone(&store));
        let operation = quorum
            .change(1, |catalog| {
                let compatibility = catalog.compatibility.clone();
                catalog.start_join(None, &compatibility, 2, 1, "node-2:4600".into())
            })
            .unwrap();
        let catalog = store.snapshot();
        assert_eq!(catalog.version, 2);
        assert_eq!(catalog.members[&2].role, MemberRole::Learner);
        assert_eq!(catalog.operations[&operation].destination, Some(2));
        assert!(store.prepared_snapshot().is_none());
    }

    #[test]
    fn a_follower_cannot_mutate_the_catalog() {
        let (_dir, store, cluster) = single_voter();
        let quorum = CatalogQuorum::new(cluster, store);
        let error = quorum.change(1, |_| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("not the catalog leader"),
            "{error}"
        );
    }

    #[test]
    fn a_new_leader_automatically_recovers_its_durable_prepared_value() {
        let (_dir, store, cluster) = single_voter();
        let now = std::time::Instant::now();
        cluster.tick_once(now + Duration::from_secs(1)).unwrap();
        assert!(cluster.is_leader());

        let (_, proposal) = store
            .propose(cluster.term(), 1, |catalog| {
                let compatibility = catalog.compatibility.clone();
                catalog.start_join(None, &compatibility, 2, 1, "node-2:4600".into())
            })
            .unwrap();
        store.prepare(proposal).unwrap();
        assert_eq!(store.snapshot().version, 1);
        assert_eq!(
            store
                .prepared_snapshot()
                .as_ref()
                .map(|prepared| prepared.catalog.version),
            Some(2)
        );

        let control = super::super::CatalogControl::new(Arc::new(CatalogQuorum::new(
            Arc::clone(&cluster),
            Arc::clone(&store),
        )));
        control.repair_cluster().unwrap();

        assert_eq!(store.snapshot().version, 2);
        assert_eq!(store.snapshot().members[&2].role, MemberRole::Learner);
        assert!(store.prepared_snapshot().is_none());
    }
}
