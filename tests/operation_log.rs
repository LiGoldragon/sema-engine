use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, CommitRequest, CommitSequence, Engine, EngineOpen, EngineRecord, QueryPlan,
    RecordKey, SnapshotId, TableDescriptor, TableName,
};
use signal_sema::SemaOperation;
use tempfile::TempDir;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct LoggedRecord {
    key: String,
    body: String,
}

impl LoggedRecord {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for LoggedRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

struct LogFixture {
    directory: TempDir,
}

impl LogFixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("temp dir is created"),
        }
    }

    fn database_path(&self) -> PathBuf {
        self.directory.path().join("engine.redb")
    }

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(self.database_path(), SchemaVersion::new(1)))
            .expect("engine opens")
    }

    fn descriptor(&self) -> TableDescriptor<LoggedRecord> {
        TableDescriptor::new(TableName::new("logged_records"))
    }
}

#[test]
fn assert_writes_commit_log_entry_with_committed_snapshot() {
    let fixture = LogFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let receipt = engine
        .assert(Assertion::new(records, LoggedRecord::new("alpha", "first")))
        .expect("assert succeeds");

    assert_eq!(receipt.snapshot(), SnapshotId::new(1));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(log.len(), 1);
    assert_eq!(receipt.commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].snapshot(), SnapshotId::new(1));
    let head = log[0].operations().head();
    assert_eq!(head.operation(), SemaOperation::Assert);
    assert_eq!(head.table_name(), "logged_records");
    assert_eq!(head.key().map(RecordKey::as_str), Some("alpha"));
}

#[test]
fn commit_log_and_snapshot_cursor_survive_reopen() {
    let fixture = LogFixture::new();
    {
        let mut engine = fixture.open_engine();
        let records = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(records, LoggedRecord::new("alpha", "first")))
            .expect("first assert succeeds");
        engine
            .assert(Assertion::new(records, LoggedRecord::new("beta", "second")))
            .expect("second assert succeeds");
    }

    let mut reopened = fixture.open_engine();
    let records = reopened
        .register_table(fixture.descriptor())
        .expect("table reference is reconstructed");
    let snapshot = reopened
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");
    let log = reopened.commit_log().expect("commit log reads");

    assert_eq!(snapshot.snapshot(), SnapshotId::new(2));
    assert_eq!(snapshot.records().len(), 2);
    assert_eq!(
        reopened.current_commit_sequence().unwrap(),
        CommitSequence::new(2)
    );
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].snapshot(), SnapshotId::new(1));
    assert_eq!(
        log[0].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(log[1].commit_sequence(), CommitSequence::new(2));
    assert_eq!(log[1].snapshot(), SnapshotId::new(2));
    assert_eq!(
        log[1].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(reopened.latest_snapshot().unwrap(), SnapshotId::new(2));
}

#[test]
fn replay_from_sequence_uses_durable_commit_sequence_cursor() {
    let fixture = LogFixture::new();
    {
        let mut engine = fixture.open_engine();
        let records = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        let first = engine
            .assert(Assertion::new(records, LoggedRecord::new("alpha", "first")))
            .expect("first assert succeeds");
        let second = engine
            .assert(Assertion::new(records, LoggedRecord::new("beta", "second")))
            .expect("second assert succeeds");
        assert_eq!(first.commit_sequence(), CommitSequence::new(1));
        assert_eq!(second.commit_sequence(), CommitSequence::new(2));
    }

    let reopened = fixture.open_engine();
    let replay = reopened
        .replay_from_sequence(CommitSequence::new(2))
        .expect("replay reads");

    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].commit_sequence(), CommitSequence::new(2));
    assert_eq!(replay[0].snapshot(), SnapshotId::new(2));
    assert_eq!(
        replay[0].operations().head().key().map(RecordKey::as_str),
        Some("beta")
    );
}

#[test]
fn rejected_commit_does_not_advance_commit_sequence() {
    let fixture = LogFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    let error = engine
        .commit(CommitRequest::new(records).mutate(LoggedRecord::new("missing", "body")))
        .expect_err("missing mutate is rejected");

    assert!(matches!(error, sema_engine::Error::RecordNotFound { .. }));
    assert_eq!(
        engine.current_commit_sequence().unwrap(),
        CommitSequence::genesis()
    );
    assert!(engine.commit_log().expect("commit log reads").is_empty());
}
