use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    AggregatePlan, Assertion, Engine, EngineOpen, EngineRecord, FieldSelection, KeyRange, Mutation,
    QueryPlan, ReadOperator, RecordKey, RecursionMode, Retraction, RuleSetRef, SnapshotId,
    TableDescriptor, TableName, UnificationPlan,
};
use signal_core::SignalVerb;
use tempfile::TempDir;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct ToyRecord {
    key: String,
    body: String,
}

impl ToyRecord {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for ToyRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

struct EngineFixture {
    directory: TempDir,
}

impl EngineFixture {
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

    fn toy_descriptor(&self) -> TableDescriptor<ToyRecord> {
        TableDescriptor::new(TableName::new("toy_records"))
    }
}

#[test]
fn engine_executes_assert_and_match_over_registered_record_family() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    let tables = engine.list_tables();
    let receipt = engine
        .assert(Assertion::new(
            records,
            ToyRecord::new("first", "stored through engine"),
        ))
        .expect("assert succeeds");

    assert_eq!(receipt.verb(), SignalVerb::Assert);
    assert_eq!(receipt.table().as_str(), "toy_records");
    assert_eq!(receipt.snapshot(), SnapshotId::new(1));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name(), "toy_records");

    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");

    assert_eq!(snapshot.verb(), SignalVerb::Match);
    assert_eq!(snapshot.snapshot(), SnapshotId::new(1));
    assert_eq!(
        snapshot.records(),
        &[ToyRecord::new("first", "stored through engine")]
    );
}

#[test]
fn engine_reopens_registered_catalog_and_matches_existing_records() {
    let fixture = EngineFixture::new();
    {
        let mut engine = fixture.open_engine();
        let records = engine
            .register_table(fixture.toy_descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(
                records,
                ToyRecord::new("persisted", "catalog"),
            ))
            .expect("assert succeeds");
    }

    let mut reopened = fixture.open_engine();
    let records = reopened
        .register_table(fixture.toy_descriptor())
        .expect("table reference is reconstructed");
    let snapshot = reopened
        .match_records(QueryPlan::key(records, RecordKey::new("persisted")))
        .expect("match succeeds");

    assert_eq!(
        snapshot.records(),
        &[ToyRecord::new("persisted", "catalog")]
    );
}

#[test]
fn engine_executes_key_range_read_plan_over_registered_record_family() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    for key in ["alpha", "beta", "gamma"] {
        engine
            .assert(Assertion::new(records, ToyRecord::new(key, key)))
            .expect("assert succeeds");
    }

    let snapshot = engine
        .match_records(QueryPlan::key_range(
            records,
            KeyRange::between(RecordKey::new("alpha"), RecordKey::new("beta")),
        ))
        .expect("range match succeeds");

    assert_eq!(
        snapshot.records(),
        &[
            ToyRecord::new("alpha", "alpha"),
            ToyRecord::new("beta", "beta")
        ]
    );
}

#[test]
fn engine_executes_mutate_and_retract_over_existing_record_family() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("assert succeeds");

    let mutation = engine
        .mutate(Mutation::new(records, ToyRecord::new("alpha", "second")))
        .expect("mutate succeeds");
    let updated = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match after mutate succeeds");

    assert_eq!(mutation.verb(), SignalVerb::Mutate);
    assert_eq!(mutation.snapshot(), SnapshotId::new(2));
    assert_eq!(updated.records(), &[ToyRecord::new("alpha", "second")]);

    let retraction = engine
        .retract(Retraction::new(records, RecordKey::new("alpha")))
        .expect("retract succeeds");
    let removed = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match after retract succeeds");
    let log = engine.operation_log().expect("operation log reads");

    assert_eq!(retraction.verb(), SignalVerb::Retract);
    assert_eq!(retraction.snapshot(), SnapshotId::new(3));
    assert!(removed.records().is_empty());
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].verb(), SignalVerb::Assert);
    assert_eq!(log[1].verb(), SignalVerb::Mutate);
    assert_eq!(log[2].verb(), SignalVerb::Retract);
}

#[test]
fn mutate_and_retract_missing_records_return_typed_errors() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let mutation_error = engine
        .mutate(Mutation::new(records, ToyRecord::new("missing", "body")))
        .expect_err("mutating a missing record is rejected");
    let retraction_error = engine
        .retract(Retraction::new(records, RecordKey::new("missing")))
        .expect_err("retracting a missing record is rejected");

    assert!(matches!(
        mutation_error,
        sema_engine::Error::RecordNotFound { .. }
    ));
    assert!(matches!(
        retraction_error,
        sema_engine::Error::RecordNotFound { .. }
    ));
    assert!(
        engine
            .operation_log()
            .expect("operation log reads")
            .is_empty()
    );
}

#[test]
fn sema_engine_owns_demoted_read_plan_operator_vocabulary() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let plans = [
        (
            QueryPlan::<ToyRecord>::constrain(records, UnificationPlan::new(["name"])),
            ReadOperator::Constrain,
        ),
        (
            QueryPlan::<ToyRecord>::project(records, FieldSelection::named(["key", "body"])),
            ReadOperator::Project,
        ),
        (
            QueryPlan::<ToyRecord>::aggregate(records, AggregatePlan::new("count")),
            ReadOperator::Aggregate,
        ),
        (
            QueryPlan::<ToyRecord>::infer(records, RuleSetRef::new("taxonomy")),
            ReadOperator::Infer,
        ),
        (
            QueryPlan::<ToyRecord>::recurse(records, RecursionMode::new("depends-on")),
            ReadOperator::Recurse,
        ),
    ];

    for (plan, operator) in plans {
        assert_eq!(plan.read_plan().operator(), operator);
    }
}

#[test]
fn unsupported_read_plan_operator_returns_typed_error() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let error = engine
        .match_records(QueryPlan::<ToyRecord>::project(
            records,
            FieldSelection::named(["body"]),
        ))
        .expect_err("project exists as a read plan but is not executable yet");

    assert!(matches!(
        error,
        sema_engine::Error::UnsupportedReadPlan {
            operator: ReadOperator::Project
        }
    ));
}

#[test]
fn engine_rejects_unregistered_record_family() {
    let fixture = EngineFixture::new();
    let engine = fixture.open_engine();
    let records = sema_engine::TableReference::new(TableName::new("unregistered"));
    let error = engine
        .assert(Assertion::new(records, ToyRecord::new("missing", "family")))
        .expect_err("unregistered table is rejected");

    assert!(matches!(
        error,
        sema_engine::Error::TableNotRegistered { .. }
    ));
}
