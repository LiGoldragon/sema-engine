//! Witnesses for checkpoints with payload: a checkpoint folds the
//! versioned log into sorted, content-addressed segments plus typed
//! metadata; it chains from its predecessor; it is a derived artifact
//! that logs no versioned entry; and it refuses stores whose history
//! predates versioning.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitSequence, Engine, EngineOpen, EngineRecord, FamilyName, Mutation,
    PortableCheckpoint, RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor,
    TableName, VersionedStoreName, VersioningPolicy,
};
use tempfile::TempDir;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Thought {
    key: String,
    body: String,
}

impl Thought {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for Thought {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

struct Fixture {
    directory: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("temp dir is created"),
        }
    }

    fn database_path(&self, name: &str) -> PathBuf {
        self.directory.path().join(format!("{name}.sema"))
    }

    fn open_versioned(&self, name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(name), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(name))),
        )
        .expect("versioned engine opens")
    }

    fn descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }
}

#[test]
fn checkpoint_persists_metadata_and_segments_durably() {
    let fixture = Fixture::new();
    let receipt = {
        let mut engine = fixture.open_versioned("durable-checkpoint");
        let thoughts = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        engine
            .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
            .expect("mutate alpha");
        engine.checkpoint().expect("checkpoint writes")
    };

    let reopened = fixture.open_versioned("durable-checkpoint");
    let checkpoint = reopened
        .latest_checkpoint()
        .expect("checkpoint loads")
        .expect("checkpoint exists");
    let metadata = checkpoint.metadata();

    assert_eq!(metadata.sequence(), receipt.sequence());
    assert_eq!(metadata.view_digest(), receipt.view_digest());
    assert_eq!(metadata.checkpoint_digest(), receipt.checkpoint_digest());
    assert_eq!(metadata.previous_checkpoint_digest(), None);
    assert_eq!(metadata.covered().first(), CommitSequence::new(1));
    assert_eq!(metadata.covered().last(), CommitSequence::new(3));
    assert_eq!(metadata.store_name().as_str(), "durable-checkpoint");
    assert_eq!(metadata.store_schema_hash(), reopened.store_schema_hash());
    assert_eq!(metadata.family_inventory().families().len(), 1);
    // Two kept rows: alpha (revised) and beta. The fold compacted the
    // mutate into alpha's final payload.
    assert_eq!(receipt.row_count(), 2);
    assert_eq!(checkpoint.rows().len(), 2);
    // The covered head names the last folded entry for chain
    // continuation.
    let log = reopened.versioned_commit_log().expect("log reads");
    assert_eq!(metadata.covered_entry_digest(), log[2].entry_digest());
    // Checkpoint::verify ran inside latest_checkpoint; run it again
    // explicitly as the witness.
    checkpoint.verify().expect("checkpoint verifies");
}

#[test]
fn checkpoint_is_a_derived_artifact_and_logs_no_versioned_entry() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("derived-artifact");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");

    let sequence_before = engine.current_commit_sequence().expect("sequence reads");
    let log_before = engine.versioned_commit_log().expect("log reads").len();
    engine.checkpoint().expect("checkpoint writes");

    assert_eq!(
        engine.current_commit_sequence().expect("sequence reads"),
        sequence_before
    );
    assert_eq!(
        engine.versioned_commit_log().expect("log reads").len(),
        log_before
    );
    assert_eq!(
        engine.commit_log().expect("commit log reads").len(),
        log_before
    );
}

#[test]
fn portable_checkpoint_bytes_round_trip_and_verify() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("portable-checkpoint");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine
        .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
        .expect("assert beta");
    engine.checkpoint().expect("checkpoint writes");

    let checkpoint = engine
        .latest_checkpoint()
        .expect("checkpoint loads")
        .expect("checkpoint exists");
    let portable = checkpoint.to_portable().expect("checkpoint serializes");

    assert!(!portable.as_bytes().is_empty());

    let decoded = portable.decode().expect("portable checkpoint decodes");
    assert_eq!(decoded.metadata(), checkpoint.metadata());
    assert_eq!(decoded.segments(), checkpoint.segments());
    assert_eq!(decoded.rows(), checkpoint.rows());
}

#[test]
fn portable_checkpoint_rejects_doctored_bytes() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("portable-checkpoint-tamper");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine.checkpoint().expect("checkpoint writes");

    let checkpoint = engine
        .latest_checkpoint()
        .expect("checkpoint loads")
        .expect("checkpoint exists");
    let mut bytes = checkpoint
        .to_portable()
        .expect("checkpoint serializes")
        .into_bytes();
    let midpoint = bytes.len() / 2;
    bytes[midpoint] ^= 0xff;

    let error = PortableCheckpoint::from_bytes(bytes)
        .decode()
        .expect_err("doctored checkpoint is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointDecode { .. }
            | sema_engine::Error::CheckpointDigestMismatch { .. }
            | sema_engine::Error::SegmentDigestMismatch { .. }
            | sema_engine::Error::ViewDigestMismatch { .. }
    ));
}

#[test]
fn second_checkpoint_chains_from_the_first_and_extends_coverage() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("chained-checkpoints");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    let first = engine.checkpoint().expect("first checkpoint writes");

    engine
        .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
        .expect("assert beta");
    engine
        .retract(Retraction::new(thoughts, RecordKey::new("alpha")))
        .expect("retract alpha");
    let second = engine.checkpoint().expect("second checkpoint writes");

    let checkpoint = engine
        .latest_checkpoint()
        .expect("checkpoint loads")
        .expect("checkpoint exists");
    let metadata = checkpoint.metadata();

    assert_eq!(metadata.sequence(), first.sequence().next());
    assert_eq!(
        metadata.previous_checkpoint_digest(),
        Some(first.checkpoint_digest())
    );
    assert_eq!(metadata.covered().first(), CommitSequence::new(1));
    assert_eq!(metadata.covered().last(), CommitSequence::new(3));
    // alpha was retracted after the first checkpoint: the second
    // checkpoint's view keeps only beta.
    assert_eq!(second.row_count(), 1);
    assert_eq!(checkpoint.rows()[0].key().to_owned_string(), "beta");
}

#[test]
fn checkpoint_without_new_entries_returns_typed_error() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("nothing-to-cover");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    // An empty log has nothing to cover.
    let error = engine.checkpoint().expect_err("empty log is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointNothingToCover
    ));

    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine.checkpoint().expect("checkpoint writes");

    // No entries beyond the covered range: nothing to cover again.
    let error = engine
        .checkpoint()
        .expect_err("checkpoint without new entries is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointNothingToCover
    ));
}

#[test]
fn checkpoint_requires_a_versioned_store() {
    let fixture = Fixture::new();
    let mut engine = Engine::open(EngineOpen::new(
        fixture.database_path("unversioned"),
        SchemaVersion::new(1),
    ))
    .expect("engine opens");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");

    let error = engine
        .checkpoint()
        .expect_err("unversioned store cannot checkpoint");
    assert!(matches!(
        error,
        sema_engine::Error::VersioningNotEnabled { .. }
    ));
}

#[test]
fn history_written_before_versioning_cannot_checkpoint() {
    let fixture = Fixture::new();
    let path = fixture.database_path("late-versioning");
    {
        let mut engine = Engine::open(EngineOpen::new(path.clone(), SchemaVersion::new(1)))
            .expect("engine opens unversioned");
        let thoughts = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("unversioned assert");
    }

    let mut engine = Engine::open(
        EngineOpen::new(path, SchemaVersion::new(1)).with_versioning(VersioningPolicy::new(
            VersionedStoreName::new("late-versioning"),
        )),
    )
    .expect("engine reopens versioned");
    let thoughts = engine
        .register_table(fixture.descriptor())
        .expect("table re-registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
        .expect("versioned assert");

    let error = engine
        .checkpoint()
        .expect_err("incomplete versioned log is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::VersionedLogIncomplete {
            commits: 2,
            versioned: 1
        }
    ));
}
