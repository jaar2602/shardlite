//! Online logical split reconciliation for linear routing.
//!
//! The visible source remains authoritative while two hidden shadows are built. Small durable
//! trigger logs record primary keys dirtied after the snapshot barrier; replay reads the resulting
//! row from the source, so nondeterministic SQL and user-trigger side effects are never re-executed.
//! Only the selected source shard is fenced for the final replay and two-file install.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::error::{Error, Result};
use crate::net::Client;
use crate::shard::{Routing, ShardId, ShardManager};
use crate::storage::exec::Outcome;

use super::{
    CatalogCommand, CatalogControl, CatalogOperation, CatalogStore, ClusterNode, OperationKind,
    OperationPhase, SplitProgress,
};

const INTERNAL_PREFIX: &str = "__shardlite_split_";

#[derive(Debug, Clone)]
struct SplitTable {
    name: String,
    key: String,
    columns: Vec<String>,
    log: String,
    triggers: [String; 3],
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

        if self.cluster.is_leader() {
            let progress = match operation.phase {
                OperationPhase::Planned => Some(SplitProgress::BeginSnapshot),
                OperationPhase::Prepared => Some(SplitProgress::BeginFencing),
                OperationPhase::Committed => Some(SplitProgress::BeginCleanup),
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
            OperationPhase::Installing => self.install_and_commit(operation)?,
            OperationPhase::Cleaning => self.cleanup(operation)?,
            _ => {}
        }
        Ok(())
    }

    fn capture_and_snapshot(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        self.manager.ensure_open(source)?;
        let tables = inspect_tables(
            &source.path(self.manager.dir()),
            operation.id,
            &self.manager.shard_keys(),
        )?;
        install_capture(&self.manager, source, &tables)?;
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
            build_shadows(&paths, &tables, source, destination, after)?;
            durable_marker(&paths.backfill_ready, b"ready\n")?;
            crate::failpoint::hit("split.owner.after_backfill");
        }
        drop_shadow_triggers(&paths, &tables)?;
        let sequence = replay_dirty(
            &source.path(self.manager.dir()),
            &paths,
            &tables,
            source,
            destination,
            after,
        )?;
        crate::failpoint::hit("split.owner.after_replay");
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Replay { sequence },
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
            let sequence = replay_dirty(
                &source.path(self.manager.dir()),
                &paths,
                &tables,
                source,
                destination,
                after,
            )?;
            restore_user_triggers(&paths.snapshot, &paths.left, &paths.right)?;
            verify_shadow(&paths.left)?;
            verify_shadow(&paths.right)?;
            durable_marker(&paths.finalized, format!("{sequence}\n").as_bytes())?;
            crate::failpoint::hit("split.owner.after_finalize");
            sequence
        };
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Fenced { sequence },
        })
    }

    fn install_and_commit(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        if !paths.installed.exists() {
            self.manager.follow(source)?;
            self.manager.follow(destination)?;
            install_shadow(&paths.left, &source.path(self.manager.dir()))?;
            install_shadow(&paths.right, &destination.path(self.manager.dir()))?;
            sync_dir(self.manager.dir())?;
            durable_marker(&paths.installed, b"installed\n")?;
            crate::failpoint::hit("split.owner.after_install");
            self.manager.lead(source);
            self.manager.lead(destination);
        }
        self.submit(CatalogCommand::Split {
            operation: operation.id,
            progress: SplitProgress::Commit,
        })
    }

    fn cleanup(&self, operation: &CatalogOperation) -> Result<()> {
        let source = split_source(operation)?;
        let destination = split_destination(operation)?;
        let tables = inspect_tables(
            &source.path(self.manager.dir()),
            operation.id,
            &self.manager.shard_keys(),
        )?;
        cleanup_capture(&self.manager, source, &tables)?;
        cleanup_capture(&self.manager, destination, &tables)?;
        let paths = SplitPaths::new(self.manager.dir(), operation.id);
        let mut cleanup = vec![
            paths.snapshot.clone(),
            paths.left.clone(),
            paths.right.clone(),
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
            progress: SplitProgress::Complete,
        })
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

#[derive(Debug)]
struct SplitPaths {
    dir: PathBuf,
    snapshot: PathBuf,
    left: PathBuf,
    right: PathBuf,
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
            backfill_ready: dir.join("backfill.ready"),
            finalized: dir.join("finalized"),
            installed: dir.join("installed"),
            dir,
        }
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
        if sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("CREATE VIRTUAL")
        {
            return Err(Error::Unsupported(format!(
                "online split does not support virtual table `{name}`"
            )));
        }
        let key = shard_keys
            .get(&name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "online split requires every table to declare a shard key; `{name}` has none"
                ))
            })?;
        let columns = table_columns(&connection, &name)?;
        let primary: Vec<String> = table_info(&connection, &name)?
            .into_iter()
            .filter_map(|(column, primary)| (primary > 0).then_some(column))
            .collect();
        if primary.len() != 1 || !primary[0].eq_ignore_ascii_case(&key) {
            return Err(Error::Unsupported(format!(
                "online split currently requires `{name}` shard key `{key}` to be its only PRIMARY \
                 KEY column"
            )));
        }
        let types_sql = format!(
            "SELECT DISTINCT typeof({}) FROM {} LIMIT 3",
            quote_ident(&key),
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
            .any(|kind| kind != "integer" && kind != "text")
        {
            return Err(Error::Unsupported(format!(
                "online split requires text or integer shard keys; `{name}` contains {}",
                values.join(", ")
            )));
        }
        let stem = format!("{INTERNAL_PREFIX}{operation}_{index}");
        tables.push(SplitTable {
            name,
            key,
            columns,
            log: format!("{stem}_log"),
            triggers: [
                format!("{stem}_insert"),
                format!("{stem}_update"),
                format!("{stem}_delete"),
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

fn install_capture(manager: &ShardManager, shard: ShardId, tables: &[SplitTable]) -> Result<()> {
    let mut statements = Vec::new();
    for table in tables {
        statements.push(crate::storage::exec::Statement::new(format!(
            "CREATE TABLE IF NOT EXISTS {} (seq INTEGER PRIMARY KEY AUTOINCREMENT, key)",
            quote_ident(&table.log)
        )));
        for (trigger, timing, value) in [
            (&table.triggers[0], "INSERT", "NEW"),
            (&table.triggers[1], "UPDATE", "NEW"),
            (&table.triggers[2], "DELETE", "OLD"),
        ] {
            statements.push(crate::storage::exec::Statement::new(format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER {timing} ON {} BEGIN INSERT INTO {} (key) \
                 VALUES ({value}.{}); END",
                quote_ident(trigger),
                quote_ident(&table.name),
                quote_ident(&table.log),
                quote_ident(&table.key)
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

fn build_shadows(
    paths: &SplitPaths,
    tables: &[SplitTable],
    source: ShardId,
    destination: ShardId,
    after: Routing,
) -> Result<()> {
    fs::copy(&paths.snapshot, &paths.left).map_err(|error| {
        Error::Manifest(format!(
            "creating left split shadow {}: {error}",
            paths.left.display()
        ))
    })?;
    fs::copy(&paths.snapshot, &paths.right).map_err(|error| {
        Error::Manifest(format!(
            "creating right split shadow {}: {error}",
            paths.right.display()
        ))
    })?;
    let snapshot = open_readonly(&paths.snapshot)?;
    let mut left = open_shadow(&paths.left)?;
    let mut right = open_shadow(&paths.right)?;
    drop_all_triggers(&left)?;
    drop_all_triggers(&right)?;
    {
        let left_tx = left.transaction().map_err(Error::Sqlite)?;
        let right_tx = right.transaction().map_err(Error::Sqlite)?;
        for table in tables {
            let query = format!(
                "SELECT {} FROM {}",
                quote_ident(&table.key),
                quote_ident(&table.name)
            );
            let mut statement = snapshot.prepare(&query).map_err(Error::Sqlite)?;
            let mut rows = statement.query([]).map_err(Error::Sqlite)?;
            let delete = format!(
                "DELETE FROM {} WHERE {} = ?1",
                quote_ident(&table.name),
                quote_ident(&table.key)
            );
            while let Some(row) = rows.next().map_err(Error::Sqlite)? {
                let key = owned_value(row.get_ref(0).map_err(Error::Sqlite)?);
                match route_value(after, &key)? {
                    shard if shard == source => {
                        right_tx
                            .execute(&delete, params![key])
                            .map_err(Error::Sqlite)?;
                    }
                    shard if shard == destination => {
                        left_tx
                            .execute(&delete, params![key])
                            .map_err(Error::Sqlite)?;
                    }
                    shard => {
                        return Err(Error::ClusterConfig(format!(
                            "linear split routed a row from {source} to unexpected {shard}"
                        )));
                    }
                }
            }
        }
        left_tx.commit().map_err(Error::Sqlite)?;
        right_tx.commit().map_err(Error::Sqlite)?;
    }
    drop(left);
    drop(right);
    sync_file(&paths.left)?;
    sync_file(&paths.right)?;
    Ok(())
}

fn replay_dirty(
    source_path: &Path,
    paths: &SplitPaths,
    tables: &[SplitTable],
    source: ShardId,
    destination: ShardId,
    after: Routing,
) -> Result<u64> {
    let source_db = open_readonly(source_path)?;
    let mut left = open_shadow(&paths.left)?;
    let mut right = open_shadow(&paths.right)?;
    let mut highest = 0;
    for table in tables {
        let log_query = format!(
            "SELECT seq, key FROM {} ORDER BY seq",
            quote_ident(&table.log)
        );
        let mut log = source_db.prepare(&log_query).map_err(Error::Sqlite)?;
        let entries = log
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)? as u64, owned_value(row.get_ref(1)?)))
            })
            .map_err(Error::Sqlite)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)?;
        if entries.is_empty() {
            continue;
        }
        let select = format!(
            "SELECT {} FROM {} WHERE {} = ?1 LIMIT 1",
            table
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            quote_ident(&table.name),
            quote_ident(&table.key)
        );
        let delete = format!(
            "DELETE FROM {} WHERE {} = ?1",
            quote_ident(&table.name),
            quote_ident(&table.key)
        );
        let insert = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
            quote_ident(&table.name),
            table
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            (1..=table.columns.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let left_tx = left.transaction().map_err(Error::Sqlite)?;
        let right_tx = right.transaction().map_err(Error::Sqlite)?;
        for (sequence, key) in entries {
            highest = highest.max(sequence);
            left_tx
                .execute(&delete, params![&key])
                .map_err(Error::Sqlite)?;
            right_tx
                .execute(&delete, params![&key])
                .map_err(Error::Sqlite)?;
            let row = source_db
                .query_row(&select, params![&key], |row| {
                    (0..table.columns.len())
                        .map(|index| row.get::<_, Value>(index))
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
                .optional()
                .map_err(Error::Sqlite)?;
            let Some(values) = row else {
                continue;
            };
            let target = route_value(after, &key)?;
            if target == source {
                left_tx
                    .execute(&insert, rusqlite::params_from_iter(values.iter()))
                    .map_err(Error::Sqlite)?;
            } else if target == destination {
                right_tx
                    .execute(&insert, rusqlite::params_from_iter(values.iter()))
                    .map_err(Error::Sqlite)?;
            } else {
                return Err(Error::ClusterConfig(format!(
                    "linear split routed a replay row from {source} to unexpected {target}"
                )));
            }
        }
        left_tx.commit().map_err(Error::Sqlite)?;
        right_tx.commit().map_err(Error::Sqlite)?;
    }
    drop(left);
    drop(right);
    sync_file(&paths.left)?;
    sync_file(&paths.right)?;
    Ok(highest)
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
        _ => {
            return Err(Error::Unsupported(
                "online split encountered a non-text/non-integer shard key".into(),
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
        install_capture(&manager, source, &tables).unwrap();

        let paths = SplitPaths::new(dir.path(), 7);
        fs::create_dir_all(&paths.dir).unwrap();
        manager.snapshot(source, &paths.snapshot).unwrap();
        let destination = ShardId(1);
        let after = Routing::LinearV1(LinearV1::for_shard_count(2).unwrap());
        build_shadows(&paths, &tables, source, destination, after).unwrap();

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
