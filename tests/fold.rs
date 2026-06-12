//! Witnesses for rebuild-from-log: the fold of the versioned log is
//! the definition of the materialized view. Rebuilding re-derives the
//! family tables from (latest checkpoint +) the log, clears keys the
//! log retracted, reproduces identical query results, and is
//! deterministic — two rebuilds name the same view digest.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyDirectory, FamilyName, IdentifiedAssertion,
    IdentifiedQueryPlan, IdentifiedTableDescriptor, IdentifiedTableReference, Mutation, QueryPlan,
    RecordKey, Retraction, RowMaterializer, SchemaHash, SchemaVersion, TableDescriptor, TableName,
    TableReference, VersionedStoreName, VersioningPolicy,
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
}

#[test]
fn rebuild_after_checkpoint_and_further_writes_reproduces_query_results() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("rebuild-witness");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    let tallies = engine
        .register_identified_table(fixture.tally_descriptor())
        .expect("tallies register");

    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine
        .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
        .expect("assert beta");
    engine
        .assert_identified(IdentifiedAssertion::new(tallies, Tally::new("one")))
        .expect("assert tally");
    engine.checkpoint().expect("checkpoint writes");
    engine
        .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
        .expect("mutate alpha");
    engine
        .retract(Retraction::new(thoughts, RecordKey::new("beta")))
        .expect("retract beta");

    let before = engine
        .match_records(QueryPlan::all(thoughts))
        .expect("match before rebuild");
    let identified_before = engine
        .match_identified(IdentifiedQueryPlan::all(tallies))
        .expect("identified match before rebuild");

    let receipt = engine
        .rebuild_from_log(&Families::new())
        .expect("rebuild succeeds");
    let after = engine
        .match_records(QueryPlan::all(thoughts))
        .expect("match after rebuild");
    let identified_after = engine
        .match_identified(IdentifiedQueryPlan::all(tallies))
        .expect("identified match after rebuild");

    assert_eq!(after.records(), before.records());
    assert_eq!(identified_after.records(), identified_before.records());
    // alpha + tally kept; beta touched-but-retracted cleared.
    assert_eq!(receipt.row_count(), 2);
    assert_eq!(receipt.cleared_count(), 1);
    assert_eq!(receipt.folded_entry_count(), 2);

    // The fold is deterministic: a second rebuild names the same
    // canonical view digest.
    let again = engine
        .rebuild_from_log(&Families::new())
        .expect("second rebuild succeeds");
    assert_eq!(again.view_digest(), receipt.view_digest());
}

#[test]
fn rebuild_clears_a_stale_row_the_log_retracted() {
    let fixture = Fixture::new();
    let path = fixture.database_path("stale-row");
    {
        let mut engine = fixture.open_versioned("stale-row");
        let thoughts = engine
            .register_table(fixture.thought_descriptor())
            .expect("thoughts register");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "kept")))
            .expect("assert alpha");
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "doomed")))
            .expect("assert beta");
        engine
            .retract(Retraction::new(thoughts, RecordKey::new("beta")))
            .expect("retract beta");
    }

    // Corrupt the materialized table behind the engine's back: resurrect
    // the retracted row through the raw storage kernel. (No engine path
    // can do this — StorageReader is read-only.)
    {
        const THOUGHTS: sema::Table<String, Thought> = sema::Table::new("thoughts");
        let schema = sema::Schema {
            version: SchemaVersion::new(1),
        };
        let storage = sema::Sema::open_with_schema(&path, &schema).expect("kernel opens");
        storage
            .write(|transaction| {
                THOUGHTS.insert(
                    transaction,
                    "beta".to_owned(),
                    &Thought::new("beta", "resurrected"),
                )
            })
            .expect("stale row writes");
    }

    let mut engine = fixture.open_versioned("stale-row");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts re-register");
    assert_eq!(
        engine
            .match_records(QueryPlan::all(thoughts))
            .expect("match reads the corruption")
            .records()
            .len(),
        2
    );

    engine
        .rebuild_from_log(&Families::new())
        .expect("rebuild succeeds");

    // The fold touched beta (assert then retract) and did not keep it:
    // the rebuild's tombstone row cleared the stale resurrection.
    assert_eq!(
        engine
            .match_records(QueryPlan::all(thoughts))
            .expect("match after rebuild")
            .records(),
        &[Thought::new("alpha", "kept")]
    );
}

#[test]
fn rebuild_without_checkpoint_folds_the_whole_log() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("full-log-rebuild");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine
        .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
        .expect("mutate alpha");

    let receipt = engine
        .rebuild_from_log(&Families::new())
        .expect("rebuild succeeds");

    assert_eq!(receipt.folded_entry_count(), 2);
    assert_eq!(receipt.row_count(), 1);
    assert_eq!(
        engine
            .match_records(QueryPlan::all(thoughts))
            .expect("match after rebuild")
            .records(),
        &[Thought::new("alpha", "revised")]
    );
}

#[test]
fn rebuild_requires_a_versioned_store() {
    let fixture = Fixture::new();
    let mut engine = Engine::open(EngineOpen::new(
        fixture.database_path("unversioned"),
        SchemaVersion::new(1),
    ))
    .expect("engine opens");
    engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");

    let error = engine
        .rebuild_from_log(&Families::new())
        .expect_err("unversioned store cannot rebuild");
    assert!(matches!(
        error,
        sema_engine::Error::VersioningNotEnabled { .. }
    ));
}
