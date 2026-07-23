//! The WAL frame inspector, against real shard WALs.

use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::exec::Statement;
use shardlite::vfs::inspect_wal;
use tempfile::TempDir;

fn wal_bytes(dir: &std::path::Path, shard: u32) -> Vec<u8> {
    let db = dir.join(format!("shard_{shard}.db"));
    let wal = shardlite::storage::checkpoint::wal_path_for(&db);
    std::fs::read(&wal).unwrap()
}

#[test]
fn it_decodes_a_real_shards_frame_stream() {
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 1,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT",
    ))
    .unwrap();
    for i in 1..=5 {
        m.execute_one(
            ShardId(0),
            Statement::new(format!("INSERT INTO t VALUES ({i}, 'x')")),
        )
        .unwrap();
    }

    let report = inspect_wal(&wal_bytes(dir.path(), 0));
    let header = report
        .header
        .expect("a real shard WAL must have a valid header");
    assert_eq!(header.page_size, 4096, "the floor profile uses 4 KiB pages");

    // The CREATE plus five INSERTs are six committed transactions.
    assert_eq!(
        report.transactions(),
        6,
        "every committed transaction should be visible as a commit frame"
    );

    // Every current frame belongs to this generation, and each transaction ends in a commit.
    let commits = report.current_frames().filter(|f| f.is_commit()).count();
    assert_eq!(commits, 6);
    assert_eq!(
        report.uncommitted_frames(),
        0,
        "at rest, nothing should sit past the last commit"
    );
    assert_eq!(report.trailing_bytes, 0, "no partial frame at rest");
}

#[test]
fn a_commit_frame_reports_the_database_size() {
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 1,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();

    let report = inspect_wal(&wal_bytes(dir.path(), 0));
    let commit = report
        .current_frames()
        .find(|f| f.is_commit())
        .expect("there must be a commit frame");
    assert!(
        commit.db_size_after_commit > 0,
        "a commit frame's db-size marker must be non-zero — that is what makes it a commit"
    );
    // A non-commit frame, if any, has a zero marker.
    for f in report.current_frames().filter(|f| !f.is_commit()) {
        assert_eq!(f.db_size_after_commit, 0);
    }
}

#[test]
fn a_non_wal_file_has_no_header() {
    let report = inspect_wal(b"this is not a WAL file, just some bytes");
    assert!(report.header.is_none());
    assert!(report.frames.is_empty());
    assert_eq!(report.trailing_bytes, report.file_bytes);
}

#[test]
fn an_empty_input_is_handled() {
    let report = inspect_wal(&[]);
    assert!(report.header.is_none());
    assert_eq!(report.file_bytes, 0);
}

#[test]
fn frames_from_a_previous_generation_are_marked_leftover() {
    // A WAL is reused in place; a checkpoint rotates the salt. Frames left from before the
    // rotation carry the old salt and must be flagged as not part of the current stream, so
    // the tool does not count stale pages as live transactions.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 1,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT",
    ))
    .unwrap();
    // Write a good many rows so the WAL holds real frames.
    for i in 1..=30 {
        m.execute_one(
            ShardId(0),
            Statement::new(format!("INSERT INTO t VALUES ({i}, 'padding-padding')")),
        )
        .unwrap();
    }

    let mut bytes = wal_bytes(dir.path(), 0);
    let report = inspect_wal(&bytes);
    assert!(report.transactions() >= 1);

    // Corrupt the header salt: now every frame's salt mismatches, so all are "leftover".
    // (This synthesises the post-checkpoint state where the header rotated but frames did
    // not — exactly what `current` must detect.)
    if let Some(header) = &report.header {
        assert!(header.page_size > 0);
    }
    bytes[16] ^= 0xff; // flip a salt byte in the header
    let after = inspect_wal(&bytes);
    assert!(
        after.frames.iter().all(|f| !f.current),
        "with a rotated header salt, every existing frame must read as leftover"
    );
    assert_eq!(
        after.transactions(),
        0,
        "leftover frames must not be counted as committed transactions"
    );
}
