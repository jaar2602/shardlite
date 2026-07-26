//! Online logical split reconciliation for linear routing.
//!
//! The visible source remains authoritative while two hidden shadows are built. Small durable
//! trigger logs record stable row identities dirtied after the snapshot barrier; replay reads the
//! resulting row from the source, so nondeterministic SQL and user-trigger side effects are never
//! re-executed.
//! Only the selected source shard is fenced for the final replay and two-file install.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::net::Client;
use crate::shard::{Routing, ShardId, ShardManager};
use crate::storage::exec::Outcome;

use super::{
    CatalogCommand, CatalogControl, CatalogOperation, CatalogStore, ClusterNode, OperationKind,
    OperationPhase, SplitProgress,
};

const INTERNAL_PREFIX: &str = "__shardlite_split_";
pub(crate) const BACKPRESSURE_SENTINEL: &str = "SHARDLITE_SPLIT_BACKPRESSURE";
const DEFAULT_BACKFILL_BATCH_ROWS: usize = 2_048;

#[derive(Debug, Clone)]
struct SplitTable {
    name: String,
    /// Declared routing key. `None` is a keyless table anchored to shard 0.
    key: Option<String>,
    /// A representative identity name (the first column for a tuple identity), retained for
    /// compatibility with rowid handling and diagnostics.
    identity: String,
    /// Columns comprising the stable row identity.  WITHOUT ROWID tables may have a composite
    /// primary key; the identity is represented as a tuple in capture logs/checkpoints.
    identity_columns: Vec<String>,
    identity_is_rowid: bool,
    columns: Vec<String>,
    log: String,
    state: String,
    triggers: [String; 6],
}

pub struct SplitSupervisor {
    node: u64,
    manager: Arc<ShardManager>,
    catalog: Arc<CatalogStore>,
    cluster: Arc<ClusterNode>,
    control: Arc<CatalogControl>,
    credentials: Option<(String, String)>,
}

impl SplitSupervisor {
    pub fn new(
        node: u64,
        manager: Arc<ShardManager>,
        catalog: Arc<CatalogStore>,
        cluster: Arc<ClusterNode>,
        control: Arc<CatalogControl>,
        credentials: Option<(String, String)>,
    ) -> Self {
        Self {
            node,
            manager,
            catalog,
            cluster,
            control,
            credentials,
        }
    }

    pub fn run(&self, interval: Duration) {
        while !self.cluster.is_stopped() {
            if let Err(error) = self.tick_once() {
                tracing::warn!(node = self.node, %error, "split reconciliation will retry");
            }
            std::thread::sleep(interval);
        }
    }

    pub fn tick_once(&self) -> Result<()> {
        let catalog = self.catalog.snapshot();
        self.manager
            .set_catalog_routing(catalog.routing, catalog.routing_epoch)?;
        let Some(operation) = catalog.operations.values().find(|operation| {
            operation.kind == OperationKind::Split && !operation.phase.terminal()
        }) else {
            return Ok(());
        };
        let owner = operation
            .source
            .ok_or_else(|| Error::ClusterConfig("split has no source owner".into()))?;
        let participant = operation.split_required_nodes.contains(&self.node);

        // The source gate was deliberately closed at the final replay barrier. Once routing is
        // committed, reopen the complete current primary set in one replacement operation; opening
        // only the two split shards would accidentally close unrelated shards.
        if self.node == owner
            && matches!(
                operation.phase,
                OperationPhase::Committed | OperationPhase::Cleaning | OperationPhase::Complete
            )
        {
            let mine: Vec<ShardId> = catalog
                .placements
                .iter()
                .filter_map(|(&shard, placement)| (placement.primary == self.node).then_some(shard))
                .collect();
            self.cluster.fence().open_for(&mine, self.cluster.term());
        }

        if self.cluster.is_leader() {
            let progress = match operation.phase {
                OperationPhase::Planned => Some(SplitProgress::BeginSnapshot),
                OperationPhase::Prepared => Some(SplitProgress::BeginFencing),
                OperationPhase::Installing
                    if operation
                        .split_required_nodes
                        .is_subset(&operation.split_installed_nodes) =>
                {
                    Some(SplitProgress::Commit)
                }
                OperationPhase::Committed => Some(SplitProgress::BeginCleanup),
                OperationPhase::Cleaning
                    if operation
                        .split_required_nodes
                        .is_subset(&operation.split_cleaned_nodes) =>
                {
                    Some(SplitProgress::Complete)
                }
                _ => None,
            };
            if let Some(progress) = progress {
                self.control.apply(CatalogCommand::Split {
                    operation: operation.id,
                    progress,
                })?;
                return Ok(());
            }
        }

        if operation.phase == OperationPhase::Installing && participant {
            self.install_and_ack(operation, owner)?;
            return Ok(());
        }
        if operation.phase == OperationPhase::Cleaning && participant {
            self.cleanup_and_ack(operation, owner)?;
            return Ok(());
        }
        if self.node != owner {
            return Ok(());
        }

        match operation.phase {
            OperationPhase::Snapshotting => {
                if let Err(error) = self.capture_and_snapshot(operation) {
                    self.submit(CatalogCommand::Split {
                        operation: operation.id,
                        progress: SplitProgress::Abort {
                            reason: error.to_string(),
                        },
                    })?;
                    return Ok(());
                }
            }
            OperationPhase::CatchingUp => self.build_and_replay(operation)?,
            OperationPhase::Fencing => self.fence_and_finalize(operation)?,
            _ => {}
        }
        Ok(())
    }

    fn capture_and_snapshot(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        self.manager.ensure_open(source)?;
        let policy = self.catalog.snapshot().scaling_policy;
        let source_path = source.path(self.manager.dir());
        let source_bytes = fs::metadata(&source_path)
            .map_err(|error| {
                Error::Manifest(format!(
                    "reading split source size {}: {error}",
                    source_path.display()
                ))
            })?
            .len();
        if policy.max_split_source_bytes > 0 && source_bytes > policy.max_split_source_bytes {
            return Err(Error::ClusterConfig(format!(
                "split source is {source_bytes} bytes, above policy max_split_source_bytes {}; \
                 raise the resource budget before retrying",
                policy.max_split_source_bytes
            )));
        }
        let tables = inspect_tables(&source_path, operation.id, &self.manager.shard_keys())?;
        install_capture(&self.manager, source, &tables, split_capture_limit(&policy))?;
        crate::failpoint::hit("split.owner.after_capture_install");

        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        fs::create_dir_all(&paths.dir).map_err(|error| {
            Error::Manifest(format!("creating {}: {error}", paths.dir.display()))
        })?;
        self.manager.snapshot(source, &paths.snapshot)?;
        sync_file(&paths.snapshot)?;
        sync_dir(&paths.dir)?;
        crate::failpoint::hit("split.owner.after_snapshot_file");
        let barrier = max_sequence(&source.path(self.manager.dir()), &tables)?;
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::SnapshotReady { barrier },
        })
    }

    fn build_and_replay(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        let after = split_after(operation)?;
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        let tables = inspect_tables(
            &source.path(self.manager.dir()),
            operation.id,
            &self.manager.shard_keys(),
        )?;
        if !paths.backfill_ready.exists() {
            build_shadows_resumable(
                &paths,
                &tables,
                source,
                destination,
                after,
                split_backfill_batch_rows(&self.catalog.snapshot().scaling_policy),
            )?;
            durable_marker(&paths.backfill_ready, b"ready\n")?;
            crate::failpoint::hit("split.owner.after_backfill");
        }
        drop_shadow_triggers(&paths, &tables)?;
        let replay = replay_dirty(
            &source.path(self.manager.dir()),
            &paths,
            &tables,
            source,
            destination,
            after,
        )?;
        prune_replayed(&self.manager, source, &tables, &replay.consumed)?;
        crate::failpoint::hit("split.owner.after_replay");
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Replay {
                sequence: replay.highest,
            },
        })?;
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Prepare,
        })
    }

    fn fence_and_finalize(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        let after = split_after(operation)?;
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        self.cluster
            .fence()
            .close_shard(source, "logical split reached its final replay barrier");
        self.manager.drain_writes(source)?;

        let sequence = if paths.finalized.exists() {
            read_marker_u64(&paths.finalized)?
        } else {
            let tables = inspect_tables(
                &source.path(self.manager.dir()),
                operation.id,
                &self.manager.shard_keys(),
            )?;
            drop_shadow_triggers(&paths, &tables)?;
            let replay = replay_dirty(
                &source.path(self.manager.dir()),
                &paths,
                &tables,
                source,
                destination,
                after,
            )?;
            // The source gate is already closed. Do not try to prune through its writer queue:
            // that queue correctly refuses every write past the fence. Cleanup drops the capture
            // tables after routing commit; only the shadow files need to be durable here.
            cleanup_shadow_capture(&paths, &tables)?;
            restore_user_triggers(&paths.snapshot, &paths.left, &paths.right)?;
            verify_shadow(&paths.left)?;
            verify_shadow(&paths.right)?;
            durable_marker(&paths.finalized, format!("{}\n", replay.highest).as_bytes())?;
            crate::failpoint::hit("split.owner.after_finalize");
            replay.highest
        };
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Fenced { sequence },
        })
    }

    fn install_and_ack(&self, operation: &CatalogOperation, owner: u64) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        fs::create_dir_all(&paths.dir).map_err(|error| {
            Error::Manifest(format!("creating {}: {error}", paths.dir.display()))
        })?;
        if self.node != owner {
            self.pull_split_image(
                owner,
                operation.id,
                crate::net::protocol::SplitImageSide::Source,
            )?;
            self.pull_split_image(
                owner,
                operation.id,
                crate::net::protocol::SplitImageSide::Destination,
            )?;
        }
        // The marker is durable, but it is intentionally not trusted by itself: a crash or an
        // operator restoring only the marker must never make a replica acknowledge images it does
        // not hold.  The shadow files and their installed destinations are compared before a
        // previously completed install is reused.
        let already_installed = paths.installed.exists()
            && split_install_matches(
                &paths,
                &source.path(self.manager.dir()),
                &destination.path(self.manager.dir()),
            );
        if !already_installed {
            remove_if_exists(&paths.installed)?;
            self.manager.follow(source)?;
            self.manager.follow(destination)?;
            install_shadow(&paths.left, &source.path(self.manager.dir()))?;
            install_shadow(&paths.right, &destination.path(self.manager.dir()))?;
            sync_dir(self.manager.dir())?;
            durable_marker(&paths.installed, b"installed\n")?;
            crate::failpoint::hit("split.owner.after_install");
            if self.node == owner {
                self.manager.lead(source);
                self.manager.lead(destination);
            }
        }
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::ReplicaInstalled { node: self.node },
        })
    }

    fn cleanup_and_ack(&self, operation: &CatalogOperation, owner: u64) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        if self.node == owner {
            let tables = inspect_tables(
                &source.path(self.manager.dir()),
                operation.id,
                &self.manager.shard_keys(),
            )?;
            cleanup_capture(&self.manager, source, &tables)?;
            cleanup_capture(&self.manager, destination, &tables)?;
        }
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        let mut cleanup = vec![
            paths.snapshot.clone(),
            paths.left.clone(),
            paths.right.clone(),
            paths.backfill_checkpoint.clone(),
            paths.backfill_ready.clone(),
            paths.finalized.clone(),
            paths.installed.clone(),
        ];
        for database in [&paths.snapshot, &paths.left, &paths.right] {
            cleanup.push(sqlite_sidecar(database, "-wal"));
            cleanup.push(sqlite_sidecar(database, "-shm"));
            cleanup.push(sqlite_sidecar(database, "-journal"));
        }
        for path in &cleanup {
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(Error::Manifest(format!(
                    "removing split staging file {}: {error}",
                    path.display()
                )));
            }
        }
        if let Err(error) = fs::remove_dir(&paths.dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(Error::Manifest(format!(
                "removing split staging directory {}: {error}",
                paths.dir.display()
            )));
        }
        sync_dir(&self.manager.dir().join("cluster").join("splits"))?;
        crate::failpoint::hit("split.owner.after_cleanup");
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::ReplicaCleaned { node: self.node },
        })
    }

    fn pull_split_image(
        &self,
        owner: u64,
        operation: u64,
        side: crate::net::protocol::SplitImageSide,
    ) -> Result<()> {
        let catalog = self.catalog.snapshot();
        let address = catalog
            .members
            .get(&owner)
            .map(|member| member.address.clone())
            .ok_or_else(|| {
                Error::ClusterConfig(format!("split image owner node {owner} is not a member"))
            })?;
        let credentials = self
            .credentials
            .as_ref()
            .map(|(name, secret)| (name.clone(), crate::net::auth::derive_key(secret)));
        let mut client = Client::connect_full(
            &address,
            Duration::from_secs(2),
            Duration::from_secs(10),
            credentials,
        )?;
        let (total_bytes, expected_digest) = client.split_image_info(operation, side)?;
        let destination = staged_image_path(self.manager.dir(), operation, side);
        if destination.exists() && digest_path(&destination)? == expected_digest {
            return Ok(());
        }
        let partial = destination.with_extension("pulling");
        let mut offset = fs::metadata(&partial)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if offset > total_bytes {
            remove_if_exists(&partial)?;
            offset = 0;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&partial)
            .map_err(|error| {
                Error::Manifest(format!(
                    "opening split image {}: {error}",
                    partial.display()
                ))
            })?;
        while offset < total_bytes {
            let wanted = (total_bytes - offset)
                .min(crate::replication::bootstrap::DEFAULT_CHUNK as u64)
                as u32;
            let chunk = client.split_image_read(operation, side, offset, wanted)?;
            if chunk.is_empty() {
                return Err(Error::Protocol(format!(
                    "split image ended at byte {offset}, expected {total_bytes}"
                )));
            }
            if crate::failpoint::active("disk.split.write") {
                return Err(Error::Manifest(format!(
                    "injected disk-full fault while writing split image {}",
                    partial.display()
                )));
            }
            if crate::failpoint::active("disk.split.short_write") {
                let prefix = (chunk.len() / 2).max(1).min(chunk.len());
                file.write_all(&chunk[..prefix]).map_err(|error| {
                    Error::Manifest(format!(
                        "short-writing split image {}: {error}",
                        partial.display()
                    ))
                })?;
                return Err(Error::Manifest(format!(
                    "injected short write while writing split image {}",
                    partial.display()
                )));
            }
            if crate::failpoint::active("disk.split.corrupt") {
                let mut corrupted = chunk.clone();
                corrupted[0] ^= 0xA5;
                file.write_all(&corrupted).map_err(|error| {
                    Error::Manifest(format!(
                        "writing split image {}: {error}",
                        partial.display()
                    ))
                })?;
            } else {
                file.write_all(&chunk).map_err(|error| {
                    Error::Manifest(format!(
                        "writing split image {}: {error}",
                        partial.display()
                    ))
                })?;
            }
            offset += chunk.len() as u64;
        }
        if crate::failpoint::active("disk.split.fsync") {
            return Err(Error::Manifest(format!(
                "injected fsync failure while writing split image {}",
                partial.display()
            )));
        }
        file.sync_all().map_err(|error| {
            Error::Manifest(format!(
                "fsyncing split image {}: {error}",
                partial.display()
            ))
        })?;
        drop(file);
        if digest_path(&partial)? != expected_digest {
            remove_if_exists(&partial)?;
            return Err(Error::Protocol(format!(
                "split image for operation {operation} failed its digest"
            )));
        }
        fs::rename(&partial, &destination).map_err(|error| {
            Error::Manifest(format!(
                "installing pulled split image {}: {error}",
                destination.display()
            ))
        })?;
        sync_dir(destination.parent().expect("split image parent"))?;
        Ok(())
    }

    fn submit(&self, command: CatalogCommand) -> Result<()> {
        if self.cluster.is_leader() {
            self.control.apply(command)?;
            return Ok(());
        }
        let leader = self.cluster.leader().ok_or_else(|| {
            Error::ClusterConfig("catalog has no elected leader; split will retry".into())
        })?;
        let address = self.cluster.peer_addr(leader).ok_or_else(|| {
            Error::ClusterConfig(format!("catalog leader node {leader} has no address"))
        })?;
        let credentials = self
            .credentials
            .as_ref()
            .map(|(name, secret)| (name.clone(), crate::net::auth::derive_key(secret)));
        let mut client = Client::connect_full(
            &address,
            Duration::from_secs(2),
            Duration::from_secs(10),
            credentials,
        )?;
        client.catalog_command(command)?;
        Ok(())
    }
}

fn split_install_matches(paths: &SplitPaths, source: &Path, destination: &Path) -> bool {
    if !paths.left.exists() || !paths.right.exists() || !source.exists() || !destination.exists() {
        return false;
    }
    match (
        digest_path(&paths.left),
        digest_path(source),
        digest_path(&paths.right),
        digest_path(destination),
    ) {
        (Ok(left), Ok(source), Ok(right), Ok(destination)) => {
            left == source && right == destination
        }
        _ => false,
    }
}

#[derive(Debug)]
struct SplitPaths {
    dir: PathBuf,
    snapshot: PathBuf,
    left: PathBuf,
    right: PathBuf,
    backfill_checkpoint: PathBuf,
    backfill_ready: PathBuf,
    finalized: PathBuf,
    installed: PathBuf,
}

impl SplitPaths {
    fn new(root: &Path, operation: u64) -> Self {
        let dir = root
            .join("cluster")
            .join("splits")
            .join(format!("operation-{operation}"));
        Self {
            snapshot: dir.join("source.snapshot.db"),
            left: dir.join("left.shadow.db"),
            right: dir.join("right.shadow.db"),
            backfill_checkpoint: dir.join("backfill.checkpoint"),
            backfill_ready: dir.join("backfill.ready"),
            finalized: dir.join("finalized"),
            installed: dir.join("installed"),
            dir,
        }
    }
}

pub(crate) fn staged_image_path(
    root: &Path,
    operation: u64,
    side: crate::net::protocol::SplitImageSide,
) -> PathBuf {
    let paths = SplitPaths::new(root, operation);
    match side {
        crate::net::protocol::SplitImageSide::Source => paths.left,
        crate::net::protocol::SplitImageSide::Destination => paths.right,
    }
}

fn split_source(operation: &CatalogOperation) -> Result<ShardId> {
    operation
        .shard
        .ok_or_else(|| Error::ClusterConfig("split operation has no source shard".into()))
}

fn split_destination(operation: &CatalogOperation) -> Result<ShardId> {
    match operation.routing_before {
        Some(Routing::LinearV1(routing)) => Ok(routing.split_destination()),
        _ => Err(Error::ClusterConfig(
            "split operation has no linear source routing".into(),
        )),
    }
}

fn split_after(operation: &CatalogOperation) -> Result<Routing> {
    operation
        .routing_after
        .ok_or_else(|| Error::ClusterConfig("split operation has no destination routing".into()))
}

fn inspect_tables(
    path: &Path,
    operation: u64,
    shard_keys: &crate::query::ShardKeys,
) -> Result<Vec<SplitTable>> {
    let connection = open_readonly(path)?;
    let mut statement = connection
        .prepare(
            "SELECT name, COALESCE(sql, '') FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .map_err(Error::Sqlite)?;
    let raw = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)?;
    let mut tables = Vec::new();
    for (name, sql) in raw {
        if name.starts_with(INTERNAL_PREFIX) {
            continue;
        }
        let index = tables.len();
        let virtual_table = connection
            .query_row(
                "SELECT 1 FROM pragma_table_list WHERE name = ?1 AND type = 'virtual' LIMIT 1",
                [&name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(Error::Sqlite)?
            .is_some()
            || sql
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE VIRTUAL");
        if virtual_table {
            return Err(Error::Unsupported(format!(
                "online split does not support virtual table `{name}`"
            )));
        }
        let key = shard_keys.get(&name.to_ascii_lowercase()).cloned();
        let columns = table_columns(&connection, &name)?;
        let primary: Vec<String> = table_info(&connection, &name)?
            .into_iter()
            .filter_map(|(column, primary)| (primary > 0).then_some(column))
            .collect();
        let sole_primary_key = key
            .as_ref()
            .is_some_and(|key| primary.len() == 1 && primary[0].eq_ignore_ascii_case(key));
        let (identity, identity_columns, identity_is_rowid) = if sole_primary_key {
            let identity = key.clone().expect("sole primary key has key");
            (identity.clone(), vec![identity], false)
        } else {
            let upper_sql = sql.to_ascii_uppercase();
            if upper_sql.contains("WITHOUT ROWID") {
                if primary.is_empty() {
                    return Err(Error::Unsupported(format!(
                        "online split cannot identify WITHOUT ROWID table `{name}` without a primary key"
                    )));
                }
                let identity = primary.first().cloned().expect("non-empty primary key");
                (identity, primary, false)
            } else {
                let identity = ["rowid", "_rowid_", "oid"]
                .into_iter()
                .find(|candidate| {
                    columns
                        .iter()
                        .all(|column| !column.eq_ignore_ascii_case(candidate))
                })
                .ok_or_else(|| {
                    Error::Unsupported(format!(
                        "online split cannot identify rows in `{name}` because all SQLite rowid \
                         aliases are shadowed by columns"
                    ))
                })?
                .to_string();
                (identity.clone(), vec![identity], true)
            }
        };
        if let Some(key) = &key {
            let types_sql = format!(
                "SELECT DISTINCT typeof({}) FROM {} LIMIT 4",
                quote_ident(key),
                quote_ident(&name)
            );
            let mut types = connection.prepare(&types_sql).map_err(Error::Sqlite)?;
            let values = types
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(Error::Sqlite)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::Sqlite)?;
            if values
                .iter()
                .any(|kind| kind != "integer" && kind != "text" && kind != "blob")
            {
                return Err(Error::Unsupported(format!(
                    "online split requires integer, text, or BLOB shard keys; `{name}` contains {}",
                    values.join(", ")
                )));
            }
        }
        for identity_column in &identity_columns {
            let types_sql = format!(
                "SELECT DISTINCT typeof({}) FROM {} LIMIT 4",
                quote_ident(identity_column),
                quote_ident(&name)
            );
            let mut types = connection.prepare(&types_sql).map_err(Error::Sqlite)?;
            let values = types
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(Error::Sqlite)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::Sqlite)?;
            if values
                .iter()
                .any(|kind| kind != "integer" && kind != "text" && kind != "blob")
            {
                return Err(Error::Unsupported(format!(
                    "online split requires integer, text, or BLOB identity columns; `{name}.{identity_column}` contains {}",
                    values.join(", ")
                )));
            }
        }
        let stem = format!("{INTERNAL_PREFIX}{operation}_{index}");
        let state = format!("{INTERNAL_PREFIX}{operation}_state");
        tables.push(SplitTable {
            name,
            key,
            identity,
            identity_columns,
            identity_is_rowid,
            columns,
            log: format!("{stem}_log"),
            state,
            triggers: [
                format!("{stem}_guard_insert"),
                format!("{stem}_guard_update"),
                format!("{stem}_guard_delete"),
                format!("{stem}_capture_insert"),
                format!("{stem}_capture_update"),
                format!("{stem}_capture_delete"),
            ],
        });
    }
    Ok(tables)
}

fn table_info(connection: &Connection, table: &str) -> Result<Vec<(String, i64)>> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut statement = connection.prepare(&sql).map_err(Error::Sqlite)?;
    statement
        .query_map([], |row| Ok((row.get(1)?, row.get(5)?)))
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>> {
    Ok(table_info(connection, table)?
        .into_iter()
        .map(|(name, _)| name)
        .collect())
}

fn install_capture(
    manager: &ShardManager,
    shard: ShardId,
    tables: &[SplitTable],
    max_rows: u64,
) -> Result<()> {
    if max_rows == 0 {
        return Err(Error::ShardConfig(
            "split capture row limit must be at least one".into(),
        ));
    }
    let mut statements = Vec::new();
    if let Some(first) = tables.first() {
        statements.push(crate::storage::exec::Statement::new(format!(
            "CREATE TABLE IF NOT EXISTS {} (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 pending INTEGER NOT NULL CHECK (pending >= 0),
                 next_seq INTEGER NOT NULL CHECK (next_seq >= 0)
             )",
            quote_ident(&first.state)
        )));
        statements.push(crate::storage::exec::Statement::new(format!(
            "INSERT OR IGNORE INTO {} (singleton, pending, next_seq) VALUES (1, 0, 0)",
            quote_ident(&first.state)
        )));
    }
    for table in tables {
        statements.push(crate::storage::exec::Statement::new(format!(
            "CREATE TABLE IF NOT EXISTS {} (seq INTEGER PRIMARY KEY, {})",
            quote_ident(&table.log),
            table
                .identity_columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("key_{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
        for (trigger, timing) in [
            (&table.triggers[0], "INSERT"),
            (&table.triggers[2], "DELETE"),
        ] {
            statements.push(crate::storage::exec::Statement::new(format!(
                "CREATE TRIGGER IF NOT EXISTS {} BEFORE {timing} ON {}
                 WHEN (SELECT pending FROM {} WHERE singleton = 1) >= {max_rows}
                 BEGIN
                   SELECT RAISE(ABORT, '{BACKPRESSURE_SENTINEL}: retained dirty-key limit \
                         {max_rows} reached; retry after split catch-up');
                 END",
                quote_ident(trigger),
                quote_ident(&table.name),
                quote_ident(&table.state),
            )));
        }
        // Replay addresses rows by their captured identity.  Updating any identity component
        // during a split would otherwise leave the old key in one shadow and create the new key
        // in the other; fence this operation as an explicit retryable write instead.
        let immutable_identity = {
            let changed = table
                .identity_columns
                .iter()
                .map(|column| format!("NOT (OLD.{0} IS NEW.{0})", quote_ident(column)))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!(
                "SELECT CASE WHEN {changed} THEN
                   RAISE(ABORT, 'online split requires stable identity values for table {table_name}')
                 END;",
                table_name = table.name.replace('\'', "''"),
            )
        };
        statements.push(crate::storage::exec::Statement::new(format!(
            "CREATE TRIGGER IF NOT EXISTS {} BEFORE UPDATE ON {} BEGIN
               {}
               SELECT CASE WHEN
                 (SELECT pending FROM {} WHERE singleton = 1) >= {max_rows}
               THEN RAISE(ABORT, '{BACKPRESSURE_SENTINEL}: retained dirty-key limit \
                    {max_rows} reached; retry after split catch-up') END;
             END",
            quote_ident(&table.triggers[1]),
            quote_ident(&table.name),
            immutable_identity,
            quote_ident(&table.state),
        )));
        for (trigger, timing, value) in [
            (&table.triggers[3], "INSERT", "NEW"),
            (&table.triggers[4], "UPDATE", "NEW"),
            (&table.triggers[5], "DELETE", "OLD"),
        ] {
            let key_columns = table
                .identity_columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("key_{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let key_values = table
                .identity_columns
                .iter()
                .map(|column| format!("{value}.{}", quote_ident(column)))
                .collect::<Vec<_>>()
                .join(", ");
            statements.push(crate::storage::exec::Statement::new(format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER {timing} ON {} BEGIN
                   UPDATE {} SET pending = pending + 1, next_seq = next_seq + 1
                     WHERE singleton = 1;
                   INSERT INTO {} (seq, {key_columns})
                     SELECT next_seq, {key_values} FROM {} WHERE singleton = 1;
                 END",
                quote_ident(trigger),
                quote_ident(&table.name),
                quote_ident(&table.state),
                quote_ident(&table.log),
                quote_ident(&table.state),
            )));
        }
    }
    ensure_outcomes(manager.execute_txn(shard, statements)?)
}

fn cleanup_capture(manager: &ShardManager, shard: ShardId, tables: &[SplitTable]) -> Result<()> {
    let mut statements = Vec::new();
    for table in tables {
        for trigger in &table.triggers {
            statements.push(crate::storage::exec::Statement::new(format!(
                "DROP TRIGGER IF EXISTS {}",
                quote_ident(trigger)
            )));
        }
        statements.push(crate::storage::exec::Statement::new(format!(
            "DROP TABLE IF EXISTS {}",
            quote_ident(&table.log)
        )));
    }
    if let Some(first) = tables.first() {
        statements.push(crate::storage::exec::Statement::new(format!(
            "DROP TABLE IF EXISTS {}",
            quote_ident(&first.state)
        )));
    }
    ensure_outcomes(manager.execute_txn(shard, statements)?)
}

fn ensure_outcomes(outcomes: Vec<Outcome>) -> Result<()> {
    for outcome in outcomes {
        if let Outcome::Rejected(message) = outcome {
            return Err(Error::Unsupported(message));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum CheckpointKey {
    Integer(i64),
    Text(String),
    Blob(Vec<u8>),
    Tuple(Vec<CheckpointKey>),
}

#[derive(Debug)]
struct BackfillRow {
    identity: CheckpointKey,
    routing_key: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BackfillCheckpoint {
    table: usize,
    after: Option<CheckpointKey>,
}

fn build_shadows_resumable(
    paths: &SplitPaths,
    tables: &[SplitTable],
    source: ShardId,
    destination: ShardId,
    after: Routing,
    batch_rows: usize,
) -> Result<()> {
    if batch_rows == 0 {
        return Err(Error::ShardConfig(
            "split backfill batch size must be at least one".into(),
        ));
    }
    let mut checkpoint = if paths.backfill_checkpoint.exists() {
        let checkpoint = read_backfill_checkpoint(&paths.backfill_checkpoint)?;
        // A checkpoint is only useful together with the two files it advances.  Treat a
        // half-created staging directory as a failed operation instead of silently marking the
        // split ready with rows missing from one shadow.  The marker is written after both files
        // have been fsynced, so this can only happen after manual tampering or a lost filesystem
        // entry; either way retrying from an inconsistent cursor would be unsafe.
        if !paths.left.exists() || !paths.right.exists() {
            return Err(Error::Manifest(format!(
                "split backfill checkpoint {} has no matching shadow files",
                paths.backfill_checkpoint.display()
            )));
        }
        if checkpoint.table > tables.len() {
            return Err(Error::Manifest(format!(
                "split backfill checkpoint {} references table {} but source has {} tables",
                paths.backfill_checkpoint.display(),
                checkpoint.table,
                tables.len()
            )));
        }
        if checkpoint.table == tables.len() && checkpoint.after.is_some() {
            return Err(Error::Manifest(format!(
                "split backfill checkpoint {} has a row cursor after the final table",
                paths.backfill_checkpoint.display()
            )));
        }
        checkpoint
    } else {
        remove_if_exists(&paths.left)?;
        remove_if_exists(&paths.right)?;
        copy_shadow_source(&paths.snapshot, &paths.left, "left")?;
        copy_shadow_source(&paths.snapshot, &paths.right, "right")?;
        let left = open_shadow(&paths.left)?;
        let right = open_shadow(&paths.right)?;
        drop_all_triggers(&left)?;
        drop_all_triggers(&right)?;
        drop(left);
        drop(right);
        sync_file(&paths.left)?;
        sync_file(&paths.right)?;
        let checkpoint = BackfillCheckpoint {
            table: 0,
            after: None,
        };
        write_backfill_checkpoint(&paths.backfill_checkpoint, &checkpoint)?;
        checkpoint
    };

    let snapshot = open_readonly(&paths.snapshot)?;
    while checkpoint.table < tables.len() {
        let table = &tables[checkpoint.table];
        let rows = read_backfill_rows(&snapshot, table, checkpoint.after.as_ref(), batch_rows)?;
        if rows.is_empty() {
            checkpoint.table += 1;
            checkpoint.after = None;
            write_backfill_checkpoint(&paths.backfill_checkpoint, &checkpoint)?;
            continue;
        }

        let mut left = open_shadow(&paths.left)?;
        let mut right = open_shadow(&paths.right)?;
        let left_tx = left.transaction().map_err(Error::Sqlite)?;
        let right_tx = right.transaction().map_err(Error::Sqlite)?;
        let delete = format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.name),
            identity_predicate(&table.identity_columns, 1)
        );
        for row in &rows {
            let identity = checkpoint_values(&row.identity);
            match row
                .routing_key
                .as_ref()
                .map(|key| route_value(after, key))
                .transpose()?
                .unwrap_or(source)
            {
                shard if shard == source => {
                    right_tx
                        .execute(&delete, rusqlite::params_from_iter(identity.iter()))
                        .map_err(Error::Sqlite)?;
                }
                shard if shard == destination => {
                    left_tx
                        .execute(&delete, rusqlite::params_from_iter(identity.iter()))
                        .map_err(Error::Sqlite)?;
                }
                shard => {
                    return Err(Error::ClusterConfig(format!(
                        "linear split routed a row from {source} to unexpected {shard}"
                    )));
                }
            }
        }
        left_tx.commit().map_err(Error::Sqlite)?;
        right_tx.commit().map_err(Error::Sqlite)?;
        drop(left);
        drop(right);
        sync_file(&paths.left)?;
        sync_file(&paths.right)?;
        checkpoint.after = rows.last().map(|row| row.identity.clone());
        write_backfill_checkpoint(&paths.backfill_checkpoint, &checkpoint)?;
        crate::failpoint::hit("split.owner.after_backfill_batch");
    }
    Ok(())
}

fn split_capture_limit(policy: &super::ScalingPolicy) -> u64 {
    std::env::var("SHARDLITE_SPLIT_LOG_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(policy.split_log_max_rows.max(1))
}

fn split_backfill_batch_rows(policy: &super::ScalingPolicy) -> usize {
    std::env::var("SHARDLITE_SPLIT_BACKFILL_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(
            usize::try_from(policy.split_backfill_batch_rows)
                .unwrap_or(DEFAULT_BACKFILL_BATCH_ROWS),
        )
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Manifest(format!(
            "removing stale split file {}: {error}",
            path.display()
        ))),
    }
}

fn copy_shadow_source(source: &Path, destination: &Path, side: &str) -> Result<()> {
    fs::copy(source, destination).map_err(|error| {
        Error::Manifest(format!(
            "creating {side} split shadow {}: {error}",
            destination.display()
        ))
    })?;
    Ok(())
}

fn read_backfill_rows(
    snapshot: &Connection,
    table: &SplitTable,
    after: Option<&CheckpointKey>,
    limit: usize,
) -> Result<Vec<BackfillRow>> {
    let projection = table
        .identity_columns
        .iter()
        .map(|column| quote_ident(column))
        .chain(
            table
                .key
                .iter()
                .filter(|key| {
                    !table
                        .identity_columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(key))
                })
                .map(|key| quote_ident(key)),
        )
        .collect::<Vec<_>>()
        .join(", ");
    let identity_order = table
        .identity_columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let (sql, values) = match after {
        Some(key) => (
            format!(
                "SELECT {projection} FROM {table} WHERE ({identity_order}) > ({placeholders}) ORDER BY {identity_order} LIMIT ?{limit_index}",
                table = quote_ident(&table.name),
                limit_index = table.identity_columns.len() + 1,
                placeholders = (1..=table.identity_columns.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            checkpoint_values(key)
                .into_iter()
                .chain(std::iter::once(Value::Integer(limit as i64)))
                .collect(),
        ),
        None => (
            format!(
                "SELECT {projection} FROM {} ORDER BY {identity_order} LIMIT ?1",
                quote_ident(&table.name),
            ),
            vec![Value::Integer(limit as i64)],
        ),
    };
    let mut statement = snapshot.prepare(&sql).map_err(Error::Sqlite)?;
    statement
        .query_map(rusqlite::params_from_iter(values.iter()), |row| {
            let identities = (0..table.identity_columns.len())
                .map(|index| checkpoint_from_ref(row.get_ref(index)?))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let identity = if identities.len() == 1 {
                identities.into_iter().next().expect("one identity")
            } else {
                CheckpointKey::Tuple(identities)
            };
            let routing_key = table.key.as_ref().and_then(|key| {
                table
                    .identity_columns
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(key))
                    .map(|index| {
                        checkpoint_values(&identity)
                            .get(index)
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .or_else(|| {
                        let offset = table.identity_columns.len();
                        Some(owned_value(row.get_ref(offset).ok()?))
                    })
            });
            Ok(BackfillRow {
                identity,
                routing_key,
            })
        })
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)
}

fn checkpoint_value(key: &CheckpointKey) -> Value {
    match key {
        CheckpointKey::Integer(value) => Value::Integer(*value),
        CheckpointKey::Text(value) => Value::Text(value.clone()),
        CheckpointKey::Blob(value) => Value::Blob(value.clone()),
        CheckpointKey::Tuple(_) => Value::Null,
    }
}

fn checkpoint_values(key: &CheckpointKey) -> Vec<Value> {
    match key {
        CheckpointKey::Tuple(values) => values.iter().map(checkpoint_value).collect(),
        value => vec![checkpoint_value(value)],
    }
}

fn checkpoint_from_ref(value: ValueRef<'_>) -> rusqlite::Result<CheckpointKey> {
    Ok(match value {
        ValueRef::Integer(value) => CheckpointKey::Integer(value),
        ValueRef::Text(value) => CheckpointKey::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => CheckpointKey::Blob(value.to_vec()),
        other => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                other.data_type(),
                "split checkpoint identity is not integer, text, or BLOB".into(),
            ));
        }
    })
}

fn identity_predicate(columns: &[String], offset: usize) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ?{}", quote_ident(column), offset + index))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn write_backfill_checkpoint(path: &Path, checkpoint: &BackfillCheckpoint) -> Result<()> {
    let bytes = bincode::serde::encode_to_vec(checkpoint, bincode::config::standard()).map_err(
        |error| {
            Error::Manifest(format!(
                "encoding split checkpoint {}: {error}",
                path.display()
            ))
        },
    )?;
    durable_marker(path, &bytes)
}

fn read_backfill_checkpoint(path: &Path) -> Result<BackfillCheckpoint> {
    let bytes = fs::read(path)
        .map_err(|error| Error::Manifest(format!("reading {}: {error}", path.display())))?;
    let (checkpoint, consumed): (BackfillCheckpoint, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map_err(
            |error| {
                Error::Manifest(format!(
                    "parsing split checkpoint {}: {error}",
                    path.display()
                ))
            },
        )?;
    if consumed != bytes.len() {
        return Err(Error::Manifest(format!(
            "split checkpoint {} has trailing bytes",
            path.display()
        )));
    }
    Ok(checkpoint)
}

#[derive(Debug)]
struct ReplayResult {
    highest: u64,
    consumed: Vec<(String, u64)>,
}

fn replay_dirty(
    source_path: &Path,
    paths: &SplitPaths,
    tables: &[SplitTable],
    source: ShardId,
    destination: ShardId,
    after: Routing,
) -> Result<ReplayResult> {
    let source_db = open_readonly(source_path)?;
    let mut left = open_shadow(&paths.left)?;
    let mut right = open_shadow(&paths.right)?;
    let mut highest = 0;
    let mut consumed = Vec::new();
    for table in tables {
        let log_query = format!(
            "SELECT seq, {} FROM {} ORDER BY seq",
            table
                .identity_columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("key_{i}"))
                .collect::<Vec<_>>()
                .join(", "),
            quote_ident(&table.log)
        );
        let mut log = source_db.prepare(&log_query).map_err(Error::Sqlite)?;
        let entries = log
            .query_map([], |row| {
                let keys = (0..table.identity_columns.len())
                    .map(|index| checkpoint_from_ref(row.get_ref(index + 1)?))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let key = if keys.len() == 1 {
                    keys.into_iter().next().expect("one identity")
                } else {
                    CheckpointKey::Tuple(keys)
                };
                Ok((row.get::<_, i64>(0)? as u64, key))
            })
            .map_err(Error::Sqlite)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)?;
        if entries.is_empty() {
            continue;
        }
        let table_highest = entries.last().map(|entry| entry.0).unwrap_or_default();
        let select = format!(
            "SELECT {} FROM {} WHERE {} LIMIT 1",
            table
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            quote_ident(&table.name),
            identity_predicate(&table.identity_columns, 1)
        );
        let delete = format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.name),
            identity_predicate(&table.identity_columns, 1)
        );
        let insert_columns = if table.identity_is_rowid {
            std::iter::once(quote_ident(&table.identity))
                .chain(table.columns.iter().map(|column| quote_ident(column)))
                .collect::<Vec<_>>()
        } else {
            table
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
        };
        let insert = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
            quote_ident(&table.name),
            insert_columns.join(", "),
            (1..=insert_columns.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let left_tx = left.transaction().map_err(Error::Sqlite)?;
        let right_tx = right.transaction().map_err(Error::Sqlite)?;
        for (sequence, key) in entries {
            highest = highest.max(sequence);
            let key_values = checkpoint_values(&key);
            left_tx
                .execute(&delete, rusqlite::params_from_iter(key_values.iter()))
                .map_err(Error::Sqlite)?;
            right_tx
                .execute(&delete, rusqlite::params_from_iter(key_values.iter()))
                .map_err(Error::Sqlite)?;
            let row = source_db
                .query_row(
                    &select,
                    rusqlite::params_from_iter(key_values.iter()),
                    |row| {
                        (0..table.columns.len())
                            .map(|index| row.get::<_, Value>(index))
                            .collect::<std::result::Result<Vec<_>, _>>()
                    },
                )
                .optional()
                .map_err(Error::Sqlite)?;
            let Some(values) = row else {
                continue;
            };
            let target = match &table.key {
                Some(routing_key) => {
                    let index = table
                        .columns
                        .iter()
                        .position(|column| column.eq_ignore_ascii_case(routing_key))
                        .expect("inspected shard key is a table column");
                    route_value(after, &values[index])?
                }
                None => source,
            };
            let insert_values = if table.identity_is_rowid {
                std::iter::once(&key_values[0])
                    .chain(values.iter())
                    .collect::<Vec<_>>()
            } else {
                values.iter().collect::<Vec<_>>()
            };
            if target == source {
                left_tx
                    .execute(&insert, rusqlite::params_from_iter(insert_values.iter()))
                    .map_err(Error::Sqlite)?;
            } else if target == destination {
                right_tx
                    .execute(&insert, rusqlite::params_from_iter(insert_values.iter()))
                    .map_err(Error::Sqlite)?;
            } else {
                return Err(Error::ClusterConfig(format!(
                    "linear split routed a replay row from {source} to unexpected {target}"
                )));
            }
        }
        left_tx.commit().map_err(Error::Sqlite)?;
        right_tx.commit().map_err(Error::Sqlite)?;
        consumed.push((table.log.clone(), table_highest));
    }
    drop(left);
    drop(right);
    sync_file(&paths.left)?;
    sync_file(&paths.right)?;
    Ok(ReplayResult { highest, consumed })
}

fn prune_replayed(
    manager: &ShardManager,
    shard: ShardId,
    tables: &[SplitTable],
    consumed: &[(String, u64)],
) -> Result<()> {
    let Some(state) = tables.first().map(|table| table.state.as_str()) else {
        return Ok(());
    };
    let mut statements = Vec::new();
    for (log, through) in consumed {
        statements.push(crate::storage::exec::Statement::new(format!(
            "UPDATE {} SET pending = pending - (
                 SELECT COUNT(*) FROM {} WHERE seq <= {through}
             ) WHERE singleton = 1",
            quote_ident(state),
            quote_ident(log),
        )));
        statements.push(crate::storage::exec::Statement::new(format!(
            "DELETE FROM {} WHERE seq <= {through}",
            quote_ident(log)
        )));
    }
    if statements.is_empty() {
        return Ok(());
    }
    ensure_outcomes(manager.execute_txn(shard, statements)?)
}

fn max_sequence(path: &Path, tables: &[SplitTable]) -> Result<u64> {
    let connection = open_readonly(path)?;
    let mut maximum = 0;
    for table in tables {
        let sql = format!(
            "SELECT COALESCE(MAX(seq), 0) FROM {}",
            quote_ident(&table.log)
        );
        maximum = maximum.max(
            connection
                .query_row(&sql, [], |row| row.get::<_, i64>(0))
                .map_err(Error::Sqlite)? as u64,
        );
    }
    Ok(maximum)
}

fn drop_shadow_triggers(paths: &SplitPaths, _tables: &[SplitTable]) -> Result<()> {
    let left = open_shadow(&paths.left)?;
    let right = open_shadow(&paths.right)?;
    drop_all_triggers(&left)?;
    drop_all_triggers(&right)?;
    Ok(())
}

fn cleanup_shadow_capture(paths: &SplitPaths, tables: &[SplitTable]) -> Result<()> {
    for path in [&paths.left, &paths.right] {
        let connection = open_shadow(path)?;
        for table in tables {
            connection
                .execute_batch(&format!(
                    "DROP TABLE IF EXISTS {};",
                    quote_ident(&table.log)
                ))
                .map_err(Error::Sqlite)?;
        }
        if let Some(first) = tables.first() {
            connection
                .execute_batch(&format!(
                    "DROP TABLE IF EXISTS {};",
                    quote_ident(&first.state)
                ))
                .map_err(Error::Sqlite)?;
        }
    }
    sync_file(&paths.left)?;
    sync_file(&paths.right)?;
    Ok(())
}

fn drop_all_triggers(connection: &Connection) -> Result<()> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name")
        .map_err(Error::Sqlite)?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)?;
    drop(statement);
    for name in names {
        connection
            .execute_batch(&format!("DROP TRIGGER {}", quote_ident(&name)))
            .map_err(Error::Sqlite)?;
    }
    Ok(())
}

fn restore_user_triggers(snapshot: &Path, left: &Path, right: &Path) -> Result<()> {
    let source = open_readonly(snapshot)?;
    let mut statement = source
        .prepare(
            "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name NOT LIKE \
             '__shardlite_split_%' ORDER BY name",
        )
        .map_err(Error::Sqlite)?;
    let triggers = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)?;
    for path in [left, right] {
        let connection = open_shadow(path)?;
        for trigger in &triggers {
            connection.execute_batch(trigger).map_err(Error::Sqlite)?;
        }
    }
    Ok(())
}

fn verify_shadow(path: &Path) -> Result<()> {
    let connection = open_readonly(path)?;
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(Error::Sqlite)?;
    if result != "ok" {
        return Err(Error::Manifest(format!(
            "split shadow {} failed integrity_check: {result}",
            path.display()
        )));
    }
    Ok(())
}

fn install_shadow(shadow: &Path, destination: &Path) -> Result<()> {
    let installing = destination.with_extension("db.split-installing");
    if let Err(error) = fs::remove_file(&installing)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::Manifest(format!(
            "removing stale {}: {error}",
            installing.display()
        )));
    }
    fs::hard_link(shadow, &installing).map_err(|error| {
        Error::Manifest(format!(
            "linking split shadow {} to {}: {error}",
            shadow.display(),
            installing.display()
        ))
    })?;
    fs::rename(&installing, destination).map_err(|error| {
        Error::Manifest(format!(
            "installing split shadow as {}: {error}",
            destination.display()
        ))
    })?;
    for sidecar in [
        crate::storage::checkpoint::wal_path_for(destination),
        sqlite_sidecar(destination, "-shm"),
    ] {
        if let Err(error) = fs::remove_file(&sidecar)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(Error::Manifest(format!(
                "removing stale SQLite sidecar {}: {error}",
                sidecar.display()
            )));
        }
    }
    Ok(())
}

fn sqlite_sidecar(database: &Path, suffix: &str) -> PathBuf {
    let name = database
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    database.with_file_name(format!("{name}{suffix}"))
}

fn open_readonly(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(Error::Sqlite)
}

fn open_shadow(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path).map_err(Error::Sqlite)?;
    connection
        .execute_batch("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;")
        .map_err(Error::Sqlite)?;
    Ok(connection)
}

fn route_value(routing: Routing, value: &Value) -> Result<ShardId> {
    let bytes = match value {
        Value::Integer(value) => value.to_le_bytes().to_vec(),
        Value::Text(value) => value.as_bytes().to_vec(),
        Value::Blob(value) => value.clone(),
        _ => {
            return Err(Error::Unsupported(
                "online split encountered a shard key outside integer, text, or BLOB".into(),
            ));
        }
    };
    Ok(routing.route(&bytes))
}

fn owned_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Integer(value),
        ValueRef::Real(value) => Value::Real(value),
        ValueRef::Text(value) => Value::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn durable_marker(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)
        .map_err(|error| Error::Manifest(format!("writing {}: {error}", temporary.display())))?;
    sync_file(&temporary)?;
    fs::rename(&temporary, path)
        .map_err(|error| Error::Manifest(format!("installing {}: {error}", path.display())))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_marker_u64(path: &Path) -> Result<u64> {
    let value = fs::read_to_string(path)
        .map_err(|error| Error::Manifest(format!("reading {}: {error}", path.display())))?;
    value.trim().parse().map_err(|error| {
        Error::Manifest(format!("parsing split marker {}: {error}", path.display()))
    })
}

fn sync_file(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::Manifest(format!("fsyncing {}: {error}", path.display())))
}

fn sync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::Manifest(format!("fsyncing {}: {error}", path.display())))
}

fn digest_path(path: &Path) -> Result<[u8; 32]> {
    use std::io::Read;
    let mut file = fs::File::open(path)
        .map_err(|error| Error::Manifest(format!("opening {}: {error}", path.display())))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| Error::Manifest(format!("hashing {}: {error}", path.display())))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

impl std::fmt::Debug for SplitSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SplitSupervisor")
            .field("node", &self.node)
            .field("catalog_version", &self.catalog.snapshot().version)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{LinearV1, ShardConfig};
    use crate::storage::exec::Statement;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[test]
    fn snapshot_plus_dirty_key_replay_matches_the_live_source() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        {
            let connection = Connection::open(source.path(dir.path())).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     CREATE TABLE users (
                         id TEXT PRIMARY KEY,
                         payload BLOB
                     ) STRICT;
                     CREATE TRIGGER users_fill_payload AFTER INSERT ON users
                     WHEN NEW.payload IS NULL
                     BEGIN
                         UPDATE users SET payload = randomblob(12) WHERE id = NEW.id;
                     END;
                     INSERT INTO users VALUES ('alpha', X'01');
                     INSERT INTO users VALUES ('bravo', X'02');
                     INSERT INTO users VALUES ('charlie', X'03');",
                )
                .unwrap();
        }
        let manager = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 4,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        manager
            .set_routing(Routing::LinearV1(LinearV1::ONE))
            .unwrap();
        manager.declare_shard_key("users", "id").unwrap();
        let tables = inspect_tables(&source.path(dir.path()), 7, &manager.shard_keys()).unwrap();
        install_capture(&manager, source, &tables, 100).unwrap();

        let paths = SplitPaths::new(dir.path(), 7);
        fs::create_dir_all(&paths.dir).unwrap();
        manager.snapshot(source, &paths.snapshot).unwrap();
        let destination = ShardId(1);
        let after = Routing::LinearV1(LinearV1::for_shard_count(2).unwrap());
        build_shadows_resumable(&paths, &tables, source, destination, after, 2).unwrap();

        ensure_outcomes(
            manager
                .execute_txn(
                    source,
                    vec![
                        Statement::new("INSERT INTO users (id, payload) VALUES ('delta', NULL)"),
                        Statement::new("UPDATE users SET payload = X'AA' WHERE id = 'alpha'"),
                        Statement::new("DELETE FROM users WHERE id = 'bravo'"),
                    ],
                )
                .unwrap(),
        )
        .unwrap();
        replay_dirty(
            &source.path(dir.path()),
            &paths,
            &tables,
            source,
            destination,
            after,
        )
        .unwrap();

        let live = rows(&source.path(dir.path()));
        let left = rows(&paths.left);
        let right = rows(&paths.right);
        let mut union = left.clone();
        for (key, value) in &right {
            assert!(union.insert(key.clone(), value.clone()).is_none());
        }
        assert_eq!(union, live);
        for key in left.keys() {
            assert_eq!(after.route(key.as_bytes()), source);
        }
        for key in right.keys() {
            assert_eq!(after.route(key.as_bytes()), destination);
        }
        assert_eq!(live["alpha"], vec![0xaa]);
        assert!(!live.contains_key("bravo"));
        assert_eq!(live["delta"].len(), 12);
    }

    #[test]
    fn capture_limit_backpressures_then_reopens_after_replay_pruning() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        {
            let connection = Connection::open(source.path(dir.path())).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     CREATE TABLE users (id TEXT PRIMARY KEY, payload TEXT) STRICT;",
                )
                .unwrap();
        }
        let manager = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        manager.declare_shard_key("users", "id").unwrap();
        let tables = inspect_tables(&source.path(dir.path()), 9, &manager.shard_keys()).unwrap();
        install_capture(&manager, source, &tables, 2).unwrap();

        for key in ["a", "b"] {
            let outcome = manager
                .execute_one(
                    source,
                    Statement::new(format!("INSERT INTO users VALUES ('{key}', 'accepted')")),
                )
                .unwrap();
            assert!(matches!(outcome, Outcome::Ok(_)));
        }
        let blocked = manager
            .execute_one(
                source,
                Statement::new("INSERT INTO users VALUES ('c', 'retry')"),
            )
            .unwrap();
        assert!(
            matches!(&blocked, Outcome::Rejected(message) if message.contains(BACKPRESSURE_SENTINEL)),
            "capture cap must be an explicit retryable sentinel, got {blocked:?}"
        );

        let through = max_sequence(&source.path(dir.path()), &tables).unwrap();
        prune_replayed(
            &manager,
            source,
            &tables,
            &[(tables[0].log.clone(), through)],
        )
        .unwrap();
        let retried = manager
            .execute_one(
                source,
                Statement::new("INSERT INTO users VALUES ('c', 'retry')"),
            )
            .unwrap();
        assert!(matches!(retried, Outcome::Ok(_)));
    }

    #[test]
    fn rowid_identity_supports_blob_composite_keys_and_anchors_keyless_tables() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        {
            let connection = Connection::open(source.path(dir.path())).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     CREATE TABLE events (
                         tenant BLOB NOT NULL,
                         sequence INTEGER NOT NULL,
                         payload TEXT,
                         PRIMARY KEY (tenant, sequence)
                     ) STRICT;
                     CREATE TABLE settings (name TEXT, value TEXT) STRICT;
                     INSERT INTO events VALUES (X'01', 1, 'a'), (X'01', 2, 'b'),
                                               (X'F0', 1, 'c');
                     INSERT INTO settings VALUES ('mode', 'before');",
                )
                .unwrap();
        }
        let manager = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 4,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        manager
            .set_routing(Routing::LinearV1(LinearV1::ONE))
            .unwrap();
        manager.declare_shard_key("events", "tenant").unwrap();
        let tables = inspect_tables(&source.path(dir.path()), 11, &manager.shard_keys()).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(
            tables
                .iter()
                .find(|table| table.name == "events")
                .unwrap()
                .identity_is_rowid
        );
        assert!(
            tables
                .iter()
                .find(|table| table.name == "settings")
                .unwrap()
                .key
                .is_none()
        );
        install_capture(&manager, source, &tables, 100).unwrap();

        let paths = SplitPaths::new(dir.path(), 11);
        fs::create_dir_all(&paths.dir).unwrap();
        manager.snapshot(source, &paths.snapshot).unwrap();
        let destination = ShardId(1);
        let after = Routing::LinearV1(LinearV1::for_shard_count(2).unwrap());
        build_shadows_resumable(&paths, &tables, source, destination, after, 1).unwrap();
        ensure_outcomes(
            manager
                .execute_txn(
                    source,
                    vec![
                        Statement::new(
                            "UPDATE events SET payload = 'changed' \
                             WHERE tenant = X'01' AND sequence = 2",
                        ),
                        Statement::new("DELETE FROM events WHERE tenant = X'F0'"),
                        Statement::new("INSERT INTO events VALUES (X'AA', 7, 'new')"),
                        Statement::new("UPDATE settings SET value = 'after' WHERE name = 'mode'"),
                        Statement::new("INSERT INTO settings VALUES ('extra', 'kept-left')"),
                    ],
                )
                .unwrap(),
        )
        .unwrap();
        replay_dirty(
            &source.path(dir.path()),
            &paths,
            &tables,
            source,
            destination,
            after,
        )
        .unwrap();

        let live_events = event_rows(&source.path(dir.path()));
        let left_events = event_rows(&paths.left);
        let right_events = event_rows(&paths.right);
        let mut union = left_events.clone();
        for (key, value) in right_events {
            assert!(union.insert(key, value).is_none());
        }
        assert_eq!(union, live_events);
        assert_eq!(
            settings_rows(&paths.left),
            settings_rows(&source.path(dir.path()))
        );
        assert!(settings_rows(&paths.right).is_empty());
    }

    #[test]
    fn without_rowid_composite_identity_is_captured_as_a_tuple() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        {
            let connection = Connection::open(source.path(dir.path())).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     CREATE TABLE pairs (
                         tenant TEXT NOT NULL,
                         sequence INTEGER NOT NULL,
                         payload TEXT,
                         PRIMARY KEY (tenant, sequence)
                     ) WITHOUT ROWID;
                     INSERT INTO pairs VALUES ('a', 1, 'one'), ('a', 2, 'two'), ('b', 1, 'three');",
                )
                .unwrap();
        }
        let manager = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        manager
            .set_routing(Routing::LinearV1(LinearV1::ONE))
            .unwrap();
        manager.declare_shard_key("pairs", "tenant").unwrap();
        let tables = inspect_tables(&source.path(dir.path()), 71, &manager.shard_keys()).unwrap();
        assert_eq!(tables[0].identity_columns, vec!["tenant", "sequence"]);
        assert!(!tables[0].identity_is_rowid);
        install_capture(&manager, source, &tables, 100).unwrap();
        let moved_identity = manager
            .execute_one(
                source,
                Statement::new("UPDATE pairs SET sequence = 9 WHERE tenant = 'a' AND sequence = 1"),
            )
            .unwrap();
        assert!(
            matches!(moved_identity, Outcome::Rejected(message) if message.contains("stable identity"))
        );
        ensure_outcomes(
            manager
                .execute_txn(
                    source,
                    vec![
                        Statement::new("UPDATE pairs SET payload = 'changed' WHERE tenant = 'a' AND sequence = 2"),
                        Statement::new("DELETE FROM pairs WHERE tenant = 'b' AND sequence = 1"),
                        Statement::new("INSERT INTO pairs VALUES ('c', 4, 'new')"),
                    ],
                )
                .unwrap(),
        )
        .unwrap();
        let connection = Connection::open(source.path(dir.path())).unwrap();
        let mut statement = connection
            .prepare("SELECT key_0, key_1 FROM __shardlite_split_71_0_log ORDER BY seq")
            .unwrap();
        let keys = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            keys,
            vec![("a".into(), 2), ("b".into(), 1), ("c".into(), 4)]
        );

        let paths = SplitPaths::new(dir.path(), 71);
        fs::create_dir_all(&paths.dir).unwrap();
        manager.snapshot(source, &paths.snapshot).unwrap();
        let after = Routing::LinearV1(LinearV1::for_shard_count(2).unwrap());
        build_shadows_resumable(&paths, &tables, source, ShardId(1), after, 2).unwrap();
    }

    #[test]
    fn virtual_tables_are_refused_before_capture_install() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        let connection = Connection::open(source.path(dir.path())).unwrap();
        connection
            .execute_batch("CREATE VIRTUAL TABLE docs USING fts5(body);")
            .unwrap();
        let error = inspect_tables(
            &source.path(dir.path()),
            73,
            &crate::query::ShardKeys::default(),
        )
        .unwrap_err();
        assert!(
            matches!(error, Error::Unsupported(message) if message.contains("virtual table `docs`"))
        );
    }

    #[test]
    fn backfill_resumes_from_a_durable_row_cursor_without_duplication() {
        let dir = TempDir::new().unwrap();
        let source = ShardId(0);
        {
            let connection = Connection::open(source.path(dir.path())).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     CREATE TABLE users (id TEXT PRIMARY KEY, payload BLOB) STRICT;
                     INSERT INTO users VALUES ('a', X'61'), ('b', X'62'), ('c', X'63'), ('d', X'64');",
                )
                .unwrap();
        }
        let manager = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        manager
            .set_routing(Routing::LinearV1(LinearV1::ONE))
            .unwrap();
        manager.declare_shard_key("users", "id").unwrap();
        let tables = inspect_tables(&source.path(dir.path()), 13, &manager.shard_keys()).unwrap();
        let paths = SplitPaths::new(dir.path(), 13);
        fs::create_dir_all(&paths.dir).unwrap();
        manager.snapshot(source, &paths.snapshot).unwrap();
        copy_shadow_source(&paths.snapshot, &paths.left, "left").unwrap();
        copy_shadow_source(&paths.snapshot, &paths.right, "right").unwrap();
        drop_all_triggers(&open_shadow(&paths.left).unwrap()).unwrap();
        drop_all_triggers(&open_shadow(&paths.right).unwrap()).unwrap();

        let after = Routing::LinearV1(LinearV1::for_shard_count(2).unwrap());
        let first_target = after.route(b"a");
        let opposite = if first_target == source {
            &paths.right
        } else {
            &paths.left
        };
        let connection = open_shadow(opposite).unwrap();
        connection
            .execute("DELETE FROM users WHERE id = 'a'", [])
            .unwrap();
        drop(connection);
        write_backfill_checkpoint(
            &paths.backfill_checkpoint,
            &BackfillCheckpoint {
                table: 0,
                after: Some(CheckpointKey::Text("a".into())),
            },
        )
        .unwrap();

        build_shadows_resumable(&paths, &tables, source, ShardId(1), after, 1).unwrap();
        let left = rows(&paths.left);
        let right = rows(&paths.right);
        let mut union = left.clone();
        for (key, value) in right {
            assert!(union.insert(key, value).is_none());
        }
        assert_eq!(union.len(), 4);
        assert_eq!(union["a"], b"a".to_vec());
        assert_eq!(
            read_backfill_checkpoint(&paths.backfill_checkpoint)
                .unwrap()
                .table,
            1
        );
    }

    #[test]
    fn a_checkpoint_without_shadow_files_is_rejected() {
        let dir = TempDir::new().unwrap();
        let paths = SplitPaths::new(dir.path(), 17);
        fs::create_dir_all(&paths.dir).unwrap();
        write_backfill_checkpoint(
            &paths.backfill_checkpoint,
            &BackfillCheckpoint {
                table: 0,
                after: None,
            },
        )
        .unwrap();
        let error = build_shadows_resumable(
            &paths,
            &[],
            ShardId(0),
            ShardId(1),
            Routing::LinearV1(LinearV1::ONE),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("no matching shadow files"));
    }

    fn event_rows(path: &Path) -> BTreeMap<(Vec<u8>, i64), String> {
        let connection = open_readonly(path).unwrap();
        let mut statement = connection
            .prepare("SELECT tenant, sequence, payload FROM events ORDER BY tenant, sequence")
            .unwrap();
        statement
            .query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap()
    }

    fn settings_rows(path: &Path) -> BTreeMap<String, String> {
        let connection = open_readonly(path).unwrap();
        let mut statement = connection
            .prepare("SELECT name, value FROM settings ORDER BY name")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap()
    }

    fn rows(path: &Path) -> BTreeMap<String, Vec<u8>> {
        let connection = open_readonly(path).unwrap();
        let mut statement = connection
            .prepare("SELECT id, payload FROM users ORDER BY id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap()
    }
}
