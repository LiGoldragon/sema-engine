//! Witnesses for the engine-owned import (restore): a checkpoint plus
//! versioned-log suffix ingested into a fresh store reproduces the
//! original through the normal query surface — records, identified
//! counters, digest chain, commit cursors, and tombstones — without
//! routing through ordinary assert/mutate. Interrupting an import
//! leaves a store that cleanly retries.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Checkpoint, Engine, EngineOpen, EngineRecord, FamilyDirectory, FamilyName,
    IdentifiedAssertion, IdentifiedQueryPlan, IdentifiedRetraction, IdentifiedTableDescriptor,
    IdentifiedTableReference, Mutation, QueryPlan, RecordKey, Retraction, RowMaterializer,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference, VersionedCommitLogEntry,
    VersionedStoreName, VersioningPolicy,
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Tally {
    note: String,
}

impl Tally {
    fn new(note: impl Into<String>) -> Self {
        Self { note: note.into() }
    }
}

/// The component's typed knowledge of its families for
/// materialization: thought rows land in the thoughts table, tally
/// rows in the identified tallies table.
struct Families {
    thoughts: TableReference<Thought>,
    tallies: IdentifiedTableReference<Tally>,
}

impl Families {
    fn new() -> Self {
        Self {
            thoughts: TableReference::new(TableName::new("thoughts")),
            tallies: IdentifiedTableReference::new(TableName::new("tallies")),
        }
    }
}

impl FamilyDirectory for Families {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().family().as_str() {
            "thought" => row.apply(self.thoughts),
            "tally" => row.apply_identified(self.tallies),
            other => Err(sema_engine::Error::FamilyUnknown {
                family: other.to_owned(),
            }),
        }
    }
}

/// A directory that fails every row — the in-library shape of an
/// interrupted import.
struct FailingDirectory;

impl FamilyDirectory for FailingDirectory {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        Err(sema_engine::Error::FamilyUnknown {
            family: row.family().family().as_str().to_owned(),
        })
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

    fn open_store(&self, file: &str, store_name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(file), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(store_name))),
        )
        .expect("versioned engine opens")
    }

    fn thought_descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }

    fn tally_descriptor(&self) -> IdentifiedTableDescriptor<Tally> {
        IdentifiedTableDescriptor::new(
            TableName::new("tallies"),
            FamilyName::new("tally"),
            SchemaHash::for_label("tally-v1"),
        )
    }

    /// Populate a source store, checkpoint mid-history, continue
    /// writing, and export (checkpoint, suffix). The suffix carries a
    /// retract tombstone so the witness can prove tombstones survive
    /// the import.
    fn populated_export(&self, name: &str) -> (Engine, Checkpoint, Vec<VersionedCommitLogEntry>) {
        let mut source = self.open_store(name, "restore-witness");
        let thoughts = source
            .register_table(self.thought_descriptor())
            .expect("thoughts register");
        let tallies = source
            .register_identified_table(self.tally_descriptor())
            .expect("tallies register");

        source
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        source
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        source
            .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
            .expect("mutate alpha");
        let first_tally = source
            .assert_identified(IdentifiedAssertion::new(tallies, Tally::new("one")))
            .expect("assert first tally");
        source
            .assert_identified(IdentifiedAssertion::new(tallies, Tally::new("two")))
            .expect("assert second tally");
        source
            .retract_identified(IdentifiedRetraction::new(tallies, first_tally.identifier()))
            .expect("retract first tally");

        source.checkpoint().expect("checkpoint writes");

        // Post-checkpoint suffix: a new record and a tombstone.
        source
            .assert(Assertion::new(thoughts, Thought::new("gamma", "third")))
            .expect("assert gamma");
        source
            .retract(Retraction::new(thoughts, RecordKey::new("beta")))
            .expect("retract beta");

        let checkpoint = source
            .latest_checkpoint()
            .expect("checkpoint loads")
            .expect("checkpoint exists");
        let suffix = source
            .versioned_replay_from_sequence(checkpoint.metadata().covered().last().next())
            .expect("suffix reads");
        (source, checkpoint, suffix)
    }
}

#[test]
fn import_restores_the_normal_query_surface_counters_chain_and_tombstones() {
    let fixture = Fixture::new();
    let (source, checkpoint, suffix) = fixture.populated_export("source");
    assert_eq!(suffix.len(), 2);

    let mut target = fixture.open_store("target", "restore-witness");
    let mut session = target.begin_import().expect("import session mints");
    session
        .ingest_checkpoint(checkpoint)
        .expect("checkpoint ingests");
    session.ingest_suffix(suffix.clone());
    let receipt = session.commit(&Families::new()).expect("import commits");

    assert_eq!(receipt.suffix_count(), 2);
    assert_eq!(
        receipt.commit_sequence(),
        source.current_commit_sequence().expect("source sequence")
    );

    // The normal query surface reads identical records through
    // re-registered descriptors (identity matches the restored
    // catalog).
    let thoughts = target
        .register_table(fixture.thought_descriptor())
        .expect("thoughts re-register against restored catalog");
    let tallies = target
        .register_identified_table(fixture.tally_descriptor())
        .expect("tallies re-register against restored catalog");
    let rebuilt = target
        .match_records(QueryPlan::all(thoughts))
        .expect("match succeeds");
    assert_eq!(
        rebuilt.records(),
        &[
            Thought::new("alpha", "revised"),
            Thought::new("gamma", "third")
        ]
    );
    // A retracted key stays absent.
    assert!(
        target
            .match_records(QueryPlan::key(thoughts, RecordKey::new("beta")))
            .expect("match succeeds")
            .records()
            .is_empty()
    );
    // Identified rows restore under their original identifiers.
    let restored_tallies = target
        .match_identified(IdentifiedQueryPlan::all(tallies))
        .expect("identified match succeeds");
    assert_eq!(restored_tallies.records().len(), 1);
    assert_eq!(restored_tallies.records()[0].identifier().value(), 2);
    assert_eq!(restored_tallies.records()[0].value(), &Tally::new("two"));

    // Counters and cursors are indistinguishable from the original.
    assert_eq!(
        target.current_commit_sequence().expect("target sequence"),
        source.current_commit_sequence().expect("source sequence")
    );
    assert_eq!(
        target.latest_snapshot().expect("target snapshot"),
        source.latest_snapshot().expect("source snapshot")
    );

    // The imported log carries the suffix verbatim — same digests,
    // same chain — and beta's tombstone survives in it.
    let imported_log = target.versioned_commit_log().expect("imported log reads");
    assert_eq!(imported_log, suffix);
    let tombstone = imported_log[1].operations().head();
    assert!(tombstone.payload().is_tombstone());
    assert_eq!(tombstone.key().map(RecordKey::as_str), Some("beta"));

    // The restored identified counter never re-mints an identifier
    // the original already allocated — including the retracted one.
    let new_tally = target
        .assert_identified(IdentifiedAssertion::new(tallies, Tally::new("three")))
        .expect("post-import assert");
    assert_eq!(new_tally.identifier().value(), 3);

    // New writes continue the imported digest chain.
    target
        .assert(Assertion::new(thoughts, Thought::new("delta", "fourth")))
        .expect("post-import assert");
    let extended = target.versioned_commit_log().expect("extended log reads");
    let continuation = &extended[extended.len() - 2];
    assert_eq!(
        extended
            .last()
            .expect("chain has entries")
            .previous_entry_digest(),
        Some(continuation.entry_digest())
    );
}

#[test]
fn import_requires_a_fresh_store() {
    let fixture = Fixture::new();
    let mut used = fixture.open_store("used", "restore-witness");
    let thoughts = used
        .register_table(fixture.thought_descriptor())
        .expect("table registers");
    used.assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");

    let error = used
        .begin_import()
        .map(|_session| ())
        .expect_err("used store cannot import");
    assert!(matches!(error, sema_engine::Error::ImportStoreNotFresh));
}

#[test]
fn import_rejects_a_checkpoint_for_another_store() {
    let fixture = Fixture::new();
    let (_source, checkpoint, _suffix) = fixture.populated_export("source");

    let mut target = fixture.open_store("target", "another-store");
    let mut session = target.begin_import().expect("import session mints");
    session
        .ingest_checkpoint(checkpoint)
        .expect("checkpoint ingests");
    let error = session
        .commit(&Families::new())
        .expect_err("foreign checkpoint is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::ImportStoreNameMismatch { .. }
    ));
}

#[test]
fn import_session_requires_exactly_one_checkpoint() {
    let fixture = Fixture::new();
    let (_source, checkpoint, _suffix) = fixture.populated_export("source");

    let mut target = fixture.open_store("target", "restore-witness");
    {
        let session = target.begin_import().expect("import session mints");
        let error = session
            .commit(&Families::new())
            .expect_err("commit without checkpoint is rejected");
        assert!(matches!(error, sema_engine::Error::ImportMissingCheckpoint));
    }
    {
        let mut session = target.begin_import().expect("import session re-mints");
        session
            .ingest_checkpoint(checkpoint.clone())
            .expect("first checkpoint ingests");
        let error = session
            .ingest_checkpoint(checkpoint)
            .expect_err("second checkpoint is rejected");
        assert!(matches!(
            error,
            sema_engine::Error::ImportCheckpointAlreadyIngested
        ));
    }
}

#[test]
fn interrupted_import_rolls_back_and_cleanly_retries() {
    let fixture = Fixture::new();
    let (source, checkpoint, suffix) = fixture.populated_export("source");

    let mut target = fixture.open_store("target", "restore-witness");
    {
        let mut session = target.begin_import().expect("import session mints");
        session
            .ingest_checkpoint(checkpoint.clone())
            .expect("checkpoint ingests");
        session.ingest_suffix(suffix.clone());
        let error = session
            .commit(&FailingDirectory)
            .expect_err("failing directory aborts the import");
        assert!(matches!(error, sema_engine::Error::FamilyUnknown { .. }));
    }

    // The aborted transaction left no partial state: the store is
    // still fresh, so the retry mints a new session and succeeds.
    let mut retry = target.begin_import().expect("store is still fresh");
    retry
        .ingest_checkpoint(checkpoint)
        .expect("checkpoint re-ingests");
    retry.ingest_suffix(suffix);
    retry.commit(&Families::new()).expect("retry commits");

    let thoughts = target
        .register_table(fixture.thought_descriptor())
        .expect("thoughts re-register");
    assert_eq!(
        target
            .match_records(QueryPlan::all(thoughts))
            .expect("match succeeds")
            .records()
            .len(),
        2
    );
    assert_eq!(
        target.current_commit_sequence().expect("target sequence"),
        source.current_commit_sequence().expect("source sequence")
    );
}
