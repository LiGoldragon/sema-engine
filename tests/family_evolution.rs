//! Witnesses for the family-evolution primitive: a table descriptor
//! declares its prior stored shapes, and registration against a store
//! whose catalog names a declared prior migrates the rows through the
//! engine — atomically with the catalog rewrite, logged as row
//! history, fail-closed for any undeclared identity.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Error, FamilyName, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, VersionedPayload, VersionedStoreName,
    VersioningPolicy,
};
use signal_sema::SemaOperation;
use tempfile::TempDir;

const THOUGHTS: TableName = TableName::new("thoughts");

/// The retired first-generation shape: no mood field.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct ThoughtV1 {
    key: String,
    body: String,
}

impl ThoughtV1 {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }

    fn into_current(self) -> Thought {
        Thought {
            key: self.key,
            body: self.body,
            mood: String::from("carried"),
        }
    }
}

impl EngineRecord for ThoughtV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

/// The current shape: a mood field appended.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Thought {
    key: String,
    body: String,
    mood: String,
}

impl Thought {
    fn new(key: impl Into<String>, body: impl Into<String>, mood: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
            mood: mood.into(),
        }
    }
}

impl EngineRecord for Thought {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

/// A shape whose field layout is incompatible with `ThoughtV1` bytes,
/// for the fail-closed decode witness.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct CountedThought {
    key: String,
    count: u64,
    weight: u64,
    depth: u64,
}

impl EngineRecord for CountedThought {
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

    fn open_plain(&self, name: &str) -> Engine {
        Engine::open(EngineOpen::new(
            self.database_path(name),
            SchemaVersion::new(1),
        ))
        .expect("engine opens")
    }

    fn open_versioned(&self, name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(name), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(name))),
        )
        .expect("versioned engine opens")
    }

    fn family(&self) -> FamilyName {
        FamilyName::new("thought")
    }

    fn hash_v1(&self) -> SchemaHash {
        SchemaHash::for_label("thought-v1")
    }

    fn hash_v2(&self) -> SchemaHash {
        SchemaHash::for_label("thought-v2")
    }

    fn descriptor_v1(&self) -> TableDescriptor<ThoughtV1> {
        TableDescriptor::new(THOUGHTS, self.family(), self.hash_v1())
    }

    fn descriptor_v2_with_prior(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(THOUGHTS, self.family(), self.hash_v2())
            .with_prior::<ThoughtV1>(self.hash_v1(), ThoughtV1::into_current)
    }

    fn seed_v1(&self, engine: &mut Engine, rows: &[ThoughtV1]) {
        let table = engine
            .register_table(self.descriptor_v1())
            .expect("v1 family registers");
        for row in rows {
            engine
                .assert(Assertion::new(table, row.clone()))
                .expect("v1 row asserts");
        }
    }
}

#[test]
fn evolution_carries_rows_across_a_family_identity_bump() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("carried");
    fixture.seed_v1(
        &mut engine,
        &[
            ThoughtV1::new("alpha", "first"),
            ThoughtV1::new("beta", "second"),
        ],
    );
    drop(engine);

    let mut evolved = fixture.open_plain("carried");
    let table = evolved
        .register_table(fixture.descriptor_v2_with_prior())
        .expect("evolution registers the v2 family");
    let mut rows = evolved
        .match_records(QueryPlan::all(table))
        .expect("current rows read")
        .records()
        .to_vec();
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        rows,
        vec![
            Thought::new("alpha", "first", "carried"),
            Thought::new("beta", "second", "carried"),
        ],
    );
}

#[test]
fn evolved_store_reopens_as_existing_registration() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("reopens");
    fixture.seed_v1(&mut engine, &[ThoughtV1::new("alpha", "first")]);
    drop(engine);

    let mut evolved = fixture.open_plain("reopens");
    evolved
        .register_table(fixture.descriptor_v2_with_prior())
        .expect("evolution registers");
    drop(evolved);

    // The second open under the evolved identity is an ordinary
    // Existing registration — no steps consulted, rows intact.
    let mut reopened = fixture.open_plain("reopens");
    let table = reopened
        .register_table(TableDescriptor::<Thought>::new(
            THOUGHTS,
            fixture.family(),
            fixture.hash_v2(),
        ))
        .expect("evolved identity re-registers as existing");
    let rows = reopened
        .match_records(QueryPlan::all(table))
        .expect("rows read");
    assert_eq!(rows.records().len(), 1);
}

#[test]
fn undeclared_stored_identity_keeps_failing_closed() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("closed");
    fixture.seed_v1(&mut engine, &[ThoughtV1::new("alpha", "first")]);
    drop(engine);

    // v2 descriptor without any declared prior: the original typed
    // mismatch surfaces, and the store is untouched.
    let mut plain = fixture.open_plain("closed");
    let outcome = plain.register_table(TableDescriptor::<Thought>::new(
        THOUGHTS,
        fixture.family(),
        fixture.hash_v2(),
    ));
    assert!(matches!(
        outcome,
        Err(Error::FamilyIdentityMismatch { .. })
    ));
    drop(plain);

    let mut untouched = fixture.open_plain("closed");
    let table = untouched
        .register_table(fixture.descriptor_v1())
        .expect("prior identity still registers after the refused open");
    let rows = untouched
        .match_records(QueryPlan::all(table))
        .expect("prior rows read");
    assert_eq!(rows.records().len(), 1);
}

#[test]
fn another_family_at_the_same_table_is_not_evolvable() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("foreign");
    fixture.seed_v1(&mut engine, &[ThoughtV1::new("alpha", "first")]);
    drop(engine);

    // Same table coordinate, different family name, matching prior
    // hash: a reused coordinate is a genuine incompatibility.
    let mut foreign = fixture.open_plain("foreign");
    let outcome = foreign.register_table(
        TableDescriptor::<Thought>::new(THOUGHTS, FamilyName::new("memo"), fixture.hash_v2())
            .with_prior::<ThoughtV1>(fixture.hash_v1(), ThoughtV1::into_current),
    );
    assert!(matches!(
        outcome,
        Err(Error::FamilyIdentityMismatch { .. })
    ));
}

#[test]
fn empty_family_evolves_as_catalog_only_rewrite() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("empty");
    engine
        .register_table(fixture.descriptor_v1())
        .expect("v1 family registers");
    drop(engine);

    let mut evolved = fixture.open_plain("empty");
    let before = evolved.commit_log().expect("commit log reads").len();
    let table = evolved
        .register_table(fixture.descriptor_v2_with_prior())
        .expect("empty family evolves");
    let after = evolved.commit_log().expect("commit log reads").len();
    assert_eq!(before, after, "an empty evolution logs no commit entry");
    let rows = evolved.match_records(QueryPlan::all(table)).expect("reads");
    assert!(rows.records().is_empty());
}

#[test]
fn a_prior_that_does_not_decode_fails_and_leaves_the_store_intact() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_plain("intact");
    fixture.seed_v1(&mut engine, &[ThoughtV1::new("alpha", "first")]);
    drop(engine);

    // The declared prior shape does not match the stored bytes: the
    // validated decode refuses, and nothing changes.
    let mut wrong = fixture.open_plain("intact");
    let outcome = wrong.register_table(
        TableDescriptor::<Thought>::new(THOUGHTS, fixture.family(), fixture.hash_v2())
            .with_prior::<CountedThought>(fixture.hash_v1(), |counted| {
                Thought::new(counted.key, "reinterpreted", "never")
            }),
    );
    assert!(outcome.is_err(), "mismatched prior bytes must refuse");
    drop(wrong);

    let mut untouched = fixture.open_plain("intact");
    let table = untouched
        .register_table(fixture.descriptor_v1())
        .expect("store still carries the prior identity");
    let rows = untouched
        .match_records(QueryPlan::all(table))
        .expect("prior rows read");
    assert_eq!(
        rows.records().to_vec(),
        vec![ThoughtV1::new("alpha", "first")],
    );
}

#[test]
fn two_declared_generations_each_evolve_directly() {
    let fixture = Fixture::new();

    // A store parked at v1 and a store parked at v2 both land on v3
    // through their own declared step.
    let hash_v3 = SchemaHash::for_label("thought-v3");
    let descriptor_v3 = || {
        TableDescriptor::<Thought>::new(THOUGHTS, FamilyName::new("thought"), hash_v3)
            .with_prior::<ThoughtV1>(SchemaHash::for_label("thought-v1"), ThoughtV1::into_current)
            .with_prior::<Thought>(SchemaHash::for_label("thought-v2"), |thought| thought)
    };

    let mut at_v1 = fixture.open_plain("ladder-v1");
    fixture.seed_v1(&mut at_v1, &[ThoughtV1::new("alpha", "first")]);
    drop(at_v1);
    let mut evolved_v1 = fixture.open_plain("ladder-v1");
    let table = evolved_v1
        .register_table(descriptor_v3())
        .expect("v1 store evolves to v3");
    assert_eq!(
        evolved_v1
            .match_records(QueryPlan::all(table))
            .expect("reads")
            .records(),
        &[Thought::new("alpha", "first", "carried")],
    );

    let mut at_v2 = fixture.open_plain("ladder-v2");
    let v2_table = at_v2
        .register_table(TableDescriptor::<Thought>::new(
            THOUGHTS,
            fixture.family(),
            fixture.hash_v2(),
        ))
        .expect("v2 family registers");
    at_v2
        .assert(Assertion::new(v2_table, Thought::new("beta", "second", "byte-compatible")))
        .expect("v2 row asserts");
    drop(at_v2);
    let mut evolved_v2 = fixture.open_plain("ladder-v2");
    let table = evolved_v2
        .register_table(descriptor_v3())
        .expect("v2 store evolves to v3");
    assert_eq!(
        evolved_v2
            .match_records(QueryPlan::all(table))
            .expect("reads")
            .records(),
        &[Thought::new("beta", "second", "byte-compatible")],
    );
}

#[test]
fn versioned_evolution_logs_retraction_and_assertion_as_row_history() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("logged");
    fixture.seed_v1(&mut engine, &[ThoughtV1::new("alpha", "first")]);
    drop(engine);

    let mut evolved = fixture.open_versioned("logged");
    evolved
        .register_table(fixture.descriptor_v2_with_prior())
        .expect("versioned store evolves");
    let log = evolved
        .versioned_commit_log()
        .expect("versioned log reads");
    let evolution_entry = log.last().expect("evolution entry exists");

    let operations: Vec<_> = evolution_entry.operations().iter().collect();
    assert_eq!(operations.len(), 2);
    assert_eq!(operations[0].operation(), SemaOperation::Retract);
    assert_eq!(operations[0].family().schema_hash(), fixture.hash_v1());
    assert_eq!(*operations[0].payload(), VersionedPayload::tombstone());
    assert_eq!(operations[1].operation(), SemaOperation::Assert);
    assert_eq!(operations[1].family().schema_hash(), fixture.hash_v2());
    assert_ne!(*operations[1].payload(), VersionedPayload::tombstone());

    // The entry's derived store hash names the evolved inventory.
    assert_eq!(evolution_entry.schema_hash(), evolved.store_schema_hash());
}
