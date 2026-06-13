use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, rancor};
use sema_engine::{
    Assertion, CommitRequest, CommitSequence, Engine, EngineOpen, EngineRecord, FamilyName,
    QueryPlan, RecordKey, Retraction, SchemaHash, SchemaVersion, SnapshotIdentifier,
    TableDescriptor, TableName, VersionedPayload, VersionedStoreName, VersioningPolicy,
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
        self.directory.path().join("engine.sema")
    }

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(self.database_path(), SchemaVersion::new(1)))
            .expect("engine opens")
    }

    fn open_versioned_engine(&self) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("operation-log-fixture")),
            ),
        )
        .expect("versioned engine opens")
    }

    fn descriptor(&self) -> TableDescriptor<LoggedRecord> {
        TableDescriptor::new(
            TableName::new("logged_records"),
            FamilyName::new("logged-record"),
            SchemaHash::for_label("logged-record-v1"),
        )
    }

    fn decode_payload(&self, payload: &VersionedPayload) -> LoggedRecord {
        rkyv::from_bytes::<LoggedRecord, rancor::Error>(
            payload.bytes().expect("payload carries record bytes"),
        )
        .expect("payload decodes")
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

    assert_eq!(receipt.snapshot(), SnapshotIdentifier::new(1));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(log.len(), 1);
    assert_eq!(receipt.commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].snapshot(), SnapshotIdentifier::new(1));
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

    assert_eq!(snapshot.snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(snapshot.records().len(), 2);
    assert_eq!(
        reopened.current_commit_sequence().unwrap(),
        CommitSequence::new(2)
    );
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].snapshot(), SnapshotIdentifier::new(1));
    assert_eq!(
        log[0].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(log[1].commit_sequence(), CommitSequence::new(2));
    assert_eq!(log[1].snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(
        log[1].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(
        reopened.latest_snapshot().unwrap(),
        SnapshotIdentifier::new(2)
    );
}

#[test]
fn versioned_commit_log_is_opt_in() {
    let fixture = LogFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, LoggedRecord::new("alpha", "first")))
        .expect("assert succeeds");

    assert_eq!(engine.commit_log().expect("commit log reads").len(), 1);
    assert!(
        engine
            .versioned_commit_log()
            .expect("versioned commit log reads")
            .is_empty(),
        "default engine open must not emit payload-bearing version entries"
    );
}

#[test]
fn versioned_commit_log_carries_payloads_and_digest_chain() {
    let fixture = LogFixture::new();
    {
        let mut engine = fixture.open_versioned_engine();
        let records = engine
            .register_table(fixture.descriptor())
            .expect("table registers");

        engine
            .assert(Assertion::new(records, LoggedRecord::new("alpha", "first")))
            .expect("assert succeeds");
        engine
            .retract(Retraction::new(records, RecordKey::new("alpha")))
            .expect("retract succeeds");
    }

    let engine = fixture.open_versioned_engine();
    let log = engine
        .versioned_commit_log()
        .expect("versioned commit log reads");
    let tail = engine
        .versioned_replay_from_sequence(CommitSequence::new(2))
        .expect("versioned replay tail reads");
    assert_eq!(log.len(), 2);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commit_sequence(), CommitSequence::new(2));
    assert_eq!(log[0].store_name().as_str(), "operation-log-fixture");
    assert_eq!(log[0].schema_hash(), engine.store_schema_hash());
    assert_eq!(log[0].commit_sequence(), CommitSequence::new(1));
    assert_eq!(log[0].snapshot(), SnapshotIdentifier::new(1));
    assert_eq!(log[0].previous_entry_digest(), None);
    assert_eq!(log[1].previous_entry_digest(), Some(log[0].entry_digest()));
    assert_ne!(log[0].entry_digest(), log[1].entry_digest());

    let first_operation = log[0].operations().head();
    assert_eq!(first_operation.operation(), SemaOperation::Assert);
    assert_eq!(first_operation.table_name(), "logged_records");
    assert_eq!(first_operation.key().map(RecordKey::as_str), Some("alpha"));
    assert_eq!(
        fixture.decode_payload(first_operation.payload()),
        LoggedRecord::new("alpha", "first")
    );

    let second_operation = log[1].operations().head();
    assert_eq!(second_operation.operation(), SemaOperation::Retract);
    assert_eq!(second_operation.key().map(RecordKey::as_str), Some("alpha"));
    assert!(second_operation.payload().is_tombstone());
}

#[test]
fn versioned_commit_log_preserves_atomic_operation_bundle() {
    let fixture = LogFixture::new();
    let mut engine = fixture.open_versioned_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    engine
        .commit(
            CommitRequest::new(records)
                .assert(LoggedRecord::new("alpha", "first"))
                .assert(LoggedRecord::new("beta", "second")),
        )
        .expect("commit succeeds");

    let log = engine
        .versioned_commit_log()
        .expect("versioned commit log reads");
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].operation_count(), 2);
    assert_eq!(
        log[0].operations().head().key().map(RecordKey::as_str),
        Some("alpha")
    );
    assert_eq!(
        log[0].operations().tail()[0].key().map(RecordKey::as_str),
        Some("beta")
    );
    assert_eq!(
        fixture.decode_payload(log[0].operations().tail()[0].payload()),
        LoggedRecord::new("beta", "second")
    );
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
    assert_eq!(replay[0].snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(
        replay[0].operations().head().key().map(RecordKey::as_str),
        Some("beta")
    );
}

#[test]
fn versioned_replay_from_sequence_uses_the_same_inclusive_cursor() {
    let fixture = LogFixture::new();
    let mut engine = fixture.open_versioned_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    for key in ["alpha", "beta", "gamma"] {
        engine
            .assert(Assertion::new(records, LoggedRecord::new(key, "body")))
            .expect("assert succeeds");
    }

    let replay = engine
        .versioned_replay_from_sequence(CommitSequence::new(2))
        .expect("versioned replay reads");
    let empty = engine
        .versioned_replay_from_sequence(CommitSequence::new(4))
        .expect("empty versioned replay reads");

    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].commit_sequence(), CommitSequence::new(2));
    assert_eq!(replay[1].commit_sequence(), CommitSequence::new(3));
    assert_eq!(
        replay[0].operations().head().key().map(RecordKey::as_str),
        Some("beta")
    );
    assert_eq!(
        replay[1].operations().head().key().map(RecordKey::as_str),
        Some("gamma")
    );
    assert!(empty.is_empty());
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
