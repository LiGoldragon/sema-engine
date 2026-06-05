use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    AggregatePlan, Assertion, CommitRequest, CommitSequence, Engine, EngineOpen, EngineRecord,
    FieldSelection, IdentifiedAssertion, IdentifiedMutation, IdentifiedQueryPlan,
    IdentifiedRetraction, IdentifiedTableDescriptor, KeyRange, Mutation, QueryPlan, ReadOperator,
    RecordIdentifier, RecordKey, RecursionMode, Retraction, RuleSetRef, SchemaVersion,
    SnapshotIdentifier, TableDescriptor, TableName, UnificationPlan,
};
use signal_sema::SemaOperation;
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
        self.directory.path().join("engine.sema")
    }

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(self.database_path(), SchemaVersion::new(1)))
            .expect("engine opens")
    }

    fn toy_descriptor(&self) -> TableDescriptor<ToyRecord> {
        TableDescriptor::new(TableName::new("toy_records"))
    }

    fn identified_descriptor(&self) -> IdentifiedTableDescriptor<ToyRecord> {
        IdentifiedTableDescriptor::new(TableName::new("identified_toy_records"))
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

    assert_eq!(receipt.operation(), SemaOperation::Assert);
    assert_eq!(receipt.table().as_str(), "toy_records");
    assert_eq!(receipt.commit_sequence(), CommitSequence::new(1));
    assert_eq!(receipt.snapshot(), SnapshotIdentifier::new(1));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name(), "toy_records");

    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");

    assert_eq!(snapshot.operation(), SemaOperation::Match);
    assert_eq!(snapshot.snapshot(), SnapshotIdentifier::new(1));
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
fn engine_allocates_numeric_identifiers_for_identified_record_family() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_identified_table(fixture.identified_descriptor())
        .expect("identified table registers");

    let first = engine
        .assert_identified(IdentifiedAssertion::new(
            records,
            ToyRecord::new("ignored-domain-key", "first identified"),
        ))
        .expect("first identified assert succeeds");
    let second = engine
        .assert_identified(IdentifiedAssertion::new(
            records,
            ToyRecord::new("another-domain-key", "second identified"),
        ))
        .expect("second identified assert succeeds");
    let snapshot = engine
        .match_identified(IdentifiedQueryPlan::identifier(
            records,
            RecordIdentifier::new(2),
        ))
        .expect("identified match succeeds");

    assert_eq!(first.identifier(), RecordIdentifier::new(1));
    assert_eq!(first.commit_sequence(), CommitSequence::new(1));
    assert_eq!(second.identifier(), RecordIdentifier::new(2));
    assert_eq!(second.commit_sequence(), CommitSequence::new(2));
    assert_eq!(snapshot.records().len(), 1);
    assert_eq!(snapshot.records()[0].identifier(), RecordIdentifier::new(2));
    assert_eq!(
        snapshot.records()[0].value(),
        &ToyRecord::new("another-domain-key", "second identified")
    );
}

#[test]
fn identified_record_counter_survives_reopen_and_retract_advances_sequence() {
    let fixture = EngineFixture::new();
    {
        let mut engine = fixture.open_engine();
        let records = engine
            .register_identified_table(fixture.identified_descriptor())
            .expect("identified table registers");
        engine
            .assert_identified(IdentifiedAssertion::new(
                records,
                ToyRecord::new("alpha", "first"),
            ))
            .expect("first identified assert succeeds");
    }

    let mut reopened = fixture.open_engine();
    let records = reopened
        .register_identified_table(fixture.identified_descriptor())
        .expect("identified table reference is reconstructed");
    let second = reopened
        .assert_identified(IdentifiedAssertion::new(
            records,
            ToyRecord::new("beta", "second"),
        ))
        .expect("second identified assert succeeds after reopen");
    let retraction = reopened
        .retract_identified(IdentifiedRetraction::new(records, RecordIdentifier::new(1)))
        .expect("identified retract succeeds");
    let remaining = reopened
        .match_identified(IdentifiedQueryPlan::all(records))
        .expect("identified all match succeeds");

    assert_eq!(second.identifier(), RecordIdentifier::new(2));
    assert_eq!(second.commit_sequence(), CommitSequence::new(2));
    assert_eq!(retraction.operation(), SemaOperation::Retract);
    assert_eq!(retraction.identifier(), RecordIdentifier::new(1));
    assert_eq!(retraction.commit_sequence(), CommitSequence::new(3));
    assert_eq!(remaining.records().len(), 1);
    assert_eq!(
        remaining.records()[0].identifier(),
        RecordIdentifier::new(2)
    );
}

#[test]
fn engine_mutates_identified_record_without_changing_identifier() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_identified_table(fixture.identified_descriptor())
        .expect("identified table registers");
    engine
        .assert_identified(IdentifiedAssertion::new(
            records,
            ToyRecord::new("alpha", "first"),
        ))
        .expect("identified assert succeeds");

    let mutation = engine
        .mutate_identified(IdentifiedMutation::new(
            records,
            RecordIdentifier::new(1),
            ToyRecord::new("alpha", "mutated"),
        ))
        .expect("identified mutate succeeds");
    let updated = engine
        .match_identified(IdentifiedQueryPlan::identifier(
            records,
            RecordIdentifier::new(1),
        ))
        .expect("identified match succeeds");
    let missing_error = engine
        .mutate_identified(IdentifiedMutation::new(
            records,
            RecordIdentifier::new(99),
            ToyRecord::new("missing", "mutated"),
        ))
        .expect_err("missing identified mutate is rejected");

    assert_eq!(mutation.operation(), SemaOperation::Mutate);
    assert_eq!(mutation.identifier(), RecordIdentifier::new(1));
    assert_eq!(mutation.commit_sequence(), CommitSequence::new(2));
    assert_eq!(mutation.snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(updated.records().len(), 1);
    assert_eq!(updated.records()[0].identifier(), RecordIdentifier::new(1));
    assert_eq!(
        updated.records()[0].value(),
        &ToyRecord::new("alpha", "mutated")
    );
    assert!(matches!(
        missing_error,
        sema_engine::Error::RecordNotFound { .. }
    ));
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

    assert_eq!(mutation.operation(), SemaOperation::Mutate);
    assert_eq!(mutation.commit_sequence(), CommitSequence::new(2));
    assert_eq!(mutation.snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(updated.records(), &[ToyRecord::new("alpha", "second")]);

    let retraction = engine
        .retract(Retraction::new(records, RecordKey::new("alpha")))
        .expect("retract succeeds");
    let removed = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match after retract succeeds");
    let log = engine.commit_log().expect("commit log reads");

    assert_eq!(retraction.operation(), SemaOperation::Retract);
    assert_eq!(retraction.commit_sequence(), CommitSequence::new(3));
    assert_eq!(retraction.snapshot(), SnapshotIdentifier::new(3));
    assert!(removed.records().is_empty());
    assert_eq!(log.len(), 3);
    assert_eq!(
        log[0].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(
        log[1].operations().head().operation(),
        SemaOperation::Mutate
    );
    assert_eq!(
        log[2].operations().head().operation(),
        SemaOperation::Retract
    );
}

#[test]
fn validate_dry_run_returns_validate_receipt_without_commit_log_write() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("assert succeeds");
    let log_before = engine.commit_log().expect("commit log reads");

    let receipt = engine
        .validate(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("validate succeeds");
    let log_after = engine.commit_log().expect("commit log reads again");

    assert_eq!(receipt.operation(), SemaOperation::Validate);
    assert_eq!(receipt.table().as_str(), "toy_records");
    assert_eq!(receipt.snapshot(), SnapshotIdentifier::new(1));
    assert_eq!(receipt.record_count(), 1);
    assert_eq!(log_after, log_before);
}

#[test]
fn validate_uses_same_typed_plan_errors_as_match() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let error = engine
        .validate(QueryPlan::<ToyRecord>::project(
            records,
            FieldSelection::named(["body"]),
        ))
        .expect_err("validate rejects unsupported plan");

    assert!(matches!(
        error,
        sema_engine::Error::UnsupportedReadPlan {
            operator: ReadOperator::Project
        }
    ));
}

#[test]
fn commit_lands_write_bundle_under_one_snapshot() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("first seed succeeds");
    engine
        .assert(Assertion::new(records, ToyRecord::new("gamma", "retire")))
        .expect("second seed succeeds");

    let receipt = engine
        .commit(
            CommitRequest::new(records)
                .assert(ToyRecord::new("beta", "new"))
                .mutate(ToyRecord::new("alpha", "second"))
                .retract(RecordKey::new("gamma")),
        )
        .expect("commit bundle succeeds");
    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");
    let log = engine.commit_log().expect("commit log reads");

    assert_eq!(receipt.snapshot(), SnapshotIdentifier::new(3));
    assert_eq!(receipt.commit_sequence(), CommitSequence::new(3));
    assert_eq!(receipt.operation_count(), 3);
    assert_eq!(
        snapshot.records(),
        &[
            ToyRecord::new("alpha", "second"),
            ToyRecord::new("beta", "new")
        ]
    );
    assert_eq!(log.len(), 3);
    let multi = &log[2];
    assert_eq!(multi.snapshot(), SnapshotIdentifier::new(3));
    assert_eq!(multi.operation_count(), 3);
    let operations = multi.operations();
    assert_eq!(operations.head().operation(), SemaOperation::Assert);
    assert_eq!(operations.tail()[0].operation(), SemaOperation::Mutate);
    assert_eq!(operations.tail()[1].operation(), SemaOperation::Retract);
}

#[test]
fn commit_missing_record_rolls_back_entire_bundle() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let error = engine
        .commit(
            CommitRequest::new(records)
                .assert(ToyRecord::new("beta", "should not persist"))
                .mutate(ToyRecord::new("missing", "body")),
        )
        .expect_err("missing mutation rejects the whole bundle");
    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds after failed commit");

    assert!(matches!(error, sema_engine::Error::RecordNotFound { .. }));
    assert!(snapshot.records().is_empty());
    assert!(engine.commit_log().expect("commit log reads").is_empty());
    assert_eq!(
        engine.latest_snapshot().unwrap(),
        SnapshotIdentifier::genesis()
    );
}

#[test]
fn commit_empty_batch_returns_typed_error() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");

    let error = engine
        .commit(CommitRequest::<ToyRecord>::new(records))
        .expect_err("empty commit request is rejected");

    assert!(matches!(error, sema_engine::Error::EmptyCommit { .. }));
}

#[test]
fn commit_duplicate_keys_return_typed_error_before_writing() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("seed succeeds");

    let error = engine
        .commit(
            CommitRequest::new(records)
                .mutate(ToyRecord::new("alpha", "second"))
                .retract(RecordKey::new("alpha")),
        )
        .expect_err("duplicate keys are rejected");
    let snapshot = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match succeeds after duplicate-key rejection");
    let log = engine.commit_log().expect("commit log reads");

    assert!(matches!(
        error,
        sema_engine::Error::DuplicateWriteKey { .. }
    ));
    assert_eq!(snapshot.records(), &[ToyRecord::new("alpha", "first")]);
    assert_eq!(log.len(), 1);
    assert_eq!(
        log[0].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(
        engine.latest_snapshot().unwrap(),
        SnapshotIdentifier::new(1)
    );
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
    assert!(engine.commit_log().expect("commit log reads").is_empty());
}

#[test]
fn assert_against_existing_key_returns_typed_duplicate_assert_error() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("seed assert succeeds");

    let error = engine
        .assert(Assertion::new(
            records,
            ToyRecord::new("alpha", "overwrite attempt"),
        ))
        .expect_err("assert against existing key is rejected");
    let snapshot = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match succeeds after duplicate-assert rejection");
    let log = engine.commit_log().expect("commit log reads");

    assert!(matches!(
        error,
        sema_engine::Error::DuplicateAssertKey { .. }
    ));
    assert_eq!(snapshot.records(), &[ToyRecord::new("alpha", "first")]);
    assert_eq!(log.len(), 1);
    assert_eq!(
        log[0].operations().head().operation(),
        SemaOperation::Assert
    );
    assert_eq!(
        engine.latest_snapshot().unwrap(),
        SnapshotIdentifier::new(1)
    );
}

#[test]
fn commit_assert_against_existing_key_rolls_back_entire_bundle() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(records, ToyRecord::new("alpha", "first")))
        .expect("seed assert succeeds");

    let error = engine
        .commit(
            CommitRequest::new(records)
                .assert(ToyRecord::new("beta", "should not persist"))
                .assert(ToyRecord::new("alpha", "overwrite attempt")),
        )
        .expect_err("commit with duplicate assert key is rejected");
    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds after rejection");
    let log = engine.commit_log().expect("commit log reads");

    assert!(matches!(
        error,
        sema_engine::Error::DuplicateAssertKey { .. }
    ));
    assert_eq!(snapshot.records(), &[ToyRecord::new("alpha", "first")]);
    assert_eq!(log.len(), 1);
    assert_eq!(
        engine.latest_snapshot().unwrap(),
        SnapshotIdentifier::new(1)
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
