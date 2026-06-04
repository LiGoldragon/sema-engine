//! Witness tests for the `signal-core` ↔ `sema-engine` seam.
//!
//! Per the audit at
//! `~/primary/reports/second-operator-assistant/2-signal-core-sema-engine-fit-audit-2026-05-17.md`
//! §1, every transitional wire-level `SignalVerb` maps onto an
//! operation-shaped `Engine` method and the engine stamps the matching Sema
//! operation into the commit log. These tests build a
//! `signal_core::Request<Payload>` on a toy payload and exercise the dispatch
//! into the engine end-to-end, so the old wire verb and the lowered Sema
//! operation are observable along the whole path: wire `Operation::verb`,
//! payload's `signal_verb()`, engine call kind, engine receipt operation, and
//! commit-log entry operation.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitRequest, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey,
    Retraction, SchemaVersion, SnapshotIdentifier, TableDescriptor, TableName,
};
use signal_core::{
    NonEmpty, Operation, Request, RequestPayload, RequestRejectionReason, SignalVerb,
};
use signal_sema::SemaOperation;
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

/// Toy contract payload. Stands in for what the
/// `signal_channel!` macro emits in a real contract crate
/// (signal-persona-mind's `MindRequest`, etc.). Each variant
/// declares its transitional `SignalVerb` through the `RequestPayload` trait so
/// the legacy wire-level `Operation::verb` round-trips with the payload's
/// reported verb.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ThoughtRequest {
    Submit(Thought),
    Replace(Thought),
    Retire(RecordKey),
    ListAll,
}

impl RequestPayload for ThoughtRequest {
    fn signal_verb(&self) -> SignalVerb {
        match self {
            Self::Submit(_) => SignalVerb::Assert,
            Self::Replace(_) => SignalVerb::Mutate,
            Self::Retire(_) => SignalVerb::Retract,
            Self::ListAll => SignalVerb::Match,
        }
    }
}

struct SeamFixture {
    directory: TempDir,
}

impl SeamFixture {
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

    fn descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(TableName::new("thoughts"))
    }
}

/// Walk one `Operation<ThoughtRequest>` into the engine call its
/// verb selects, returning the snapshot the engine stamps on the
/// resulting receipt. This mirrors the dispatch a triad daemon
/// writes around `Engine` today (and the boilerplate the audit
/// names as a candidate for a `signal_channel!`-emitted dispatcher
/// trait).
fn dispatch_single(
    engine: &Engine,
    table: sema_engine::TableReference<Thought>,
    operation: &Operation<ThoughtRequest>,
) -> SnapshotIdentifier {
    assert_eq!(
        operation.verb(),
        operation.payload().signal_verb(),
        "Operation verb must agree with payload signal_verb()",
    );
    match operation.payload() {
        ThoughtRequest::Submit(thought) => {
            let receipt = engine
                .assert(Assertion::new(table, thought.clone()))
                .expect("assert succeeds");
            assert_eq!(receipt.operation(), SemaOperation::Assert);
            receipt.snapshot()
        }
        ThoughtRequest::Replace(thought) => {
            let receipt = engine
                .mutate(Mutation::new(table, thought.clone()))
                .expect("mutate succeeds");
            assert_eq!(receipt.operation(), SemaOperation::Mutate);
            receipt.snapshot()
        }
        ThoughtRequest::Retire(key) => {
            let receipt = engine
                .retract(Retraction::new(table, key.clone()))
                .expect("retract succeeds");
            assert_eq!(receipt.operation(), SemaOperation::Retract);
            receipt.snapshot()
        }
        ThoughtRequest::ListAll => {
            let snapshot = engine
                .match_records(QueryPlan::all(table))
                .expect("match succeeds");
            assert_eq!(snapshot.operation(), SemaOperation::Match);
            snapshot.snapshot()
        }
    }
}

#[test]
fn signal_core_assert_operation_lands_as_engine_assert_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    let request = ThoughtRequest::Submit(Thought::new("alpha", "first thought")).into_request();
    let checked = request.into_checked().expect("legacy check passes");
    let operation = checked.operations.head();

    let snapshot = dispatch_single(&engine, table, operation);

    assert_eq!(snapshot, SnapshotIdentifier::new(1));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].snapshot(), snapshot);
    let head = log[0].operations().head();
    assert_eq!(head.operation(), SemaOperation::Assert);
    assert_eq!(head.table_name(), "thoughts");
    assert_eq!(head.key().map(RecordKey::as_str), Some("alpha"));
}

#[test]
fn signal_core_mutate_operation_lands_as_engine_mutate_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    // Seed an Assert so the Mutate has a record to transition.
    let seed = ThoughtRequest::Submit(Thought::new("alpha", "original")).into_request();
    let seed_checked = seed.into_checked().expect("legacy check passes");
    let _ = dispatch_single(&engine, table, seed_checked.operations.head());

    let mutate = ThoughtRequest::Replace(Thought::new("alpha", "revised")).into_request();
    let mutate_checked = mutate.into_checked().expect("legacy check passes");
    let snapshot = dispatch_single(&engine, table, mutate_checked.operations.head());

    assert_eq!(snapshot, SnapshotIdentifier::new(2));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(log.len(), 2);
    let mutate_entry = log
        .iter()
        .find(|entry| entry.operations().head().operation() == SemaOperation::Mutate)
        .expect("commit log carries the mutate entry");
    assert_eq!(mutate_entry.snapshot(), snapshot);
    let head = mutate_entry.operations().head();
    assert_eq!(head.table_name(), "thoughts");
    assert_eq!(head.key().map(RecordKey::as_str), Some("alpha"));

    let read = engine
        .match_records(QueryPlan::key(table, RecordKey::new("alpha")))
        .expect("match succeeds");
    assert_eq!(read.records(), &[Thought::new("alpha", "revised")]);
}

#[test]
fn signal_core_retract_operation_lands_as_engine_retract_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    let seed = ThoughtRequest::Submit(Thought::new("alpha", "soon retired")).into_request();
    let _ = dispatch_single(
        &engine,
        table,
        seed.into_checked().unwrap().operations.head(),
    );

    let retire = ThoughtRequest::Retire(RecordKey::new("alpha")).into_request();
    let checked = retire.into_checked().expect("legacy check passes");
    let snapshot = dispatch_single(&engine, table, checked.operations.head());

    assert_eq!(snapshot, SnapshotIdentifier::new(2));
    let log = engine.commit_log().expect("commit log reads");
    let retract_entry = log
        .iter()
        .find(|entry| entry.operations().head().operation() == SemaOperation::Retract)
        .expect("commit log carries the retract entry");
    assert_eq!(retract_entry.snapshot(), snapshot);
    assert_eq!(
        retract_entry
            .operations()
            .head()
            .key()
            .map(RecordKey::as_str),
        Some("alpha"),
    );

    let read = engine
        .match_records(QueryPlan::all(table))
        .expect("match succeeds");
    assert!(
        read.records().is_empty(),
        "the retracted record is gone after engine.retract"
    );
}

#[test]
fn signal_core_multi_operation_request_lands_as_one_commit_log_entry_with_ordered_operations() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    // Seed records the Mutate and Retract can target inside one
    // atomic commit. Two pre-commit asserts so the wire bundle below
    // exercises assert + mutate + retract — the full write spine.
    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed alpha");
    engine
        .assert(Assertion::new(table, Thought::new("gamma", "seeded")))
        .expect("seed gamma");
    let pre_log_len = engine.commit_log().expect("commit log reads").len();

    // Build a multi-operation `signal_core::Request` carrying three
    // verb-tagged operations in order: Assert, Mutate, Retract.
    let operations = NonEmpty::try_from_vec(vec![
        Operation::new(
            SignalVerb::Assert,
            ThoughtRequest::Submit(Thought::new("beta", "newcomer")),
        ),
        Operation::new(
            SignalVerb::Mutate,
            ThoughtRequest::Replace(Thought::new("alpha", "revised")),
        ),
        Operation::new(
            SignalVerb::Retract,
            ThoughtRequest::Retire(RecordKey::new("gamma")),
        ),
    ])
    .expect("non-empty vector");
    let request: Request<ThoughtRequest> = Request::from_operations(operations);
    let checked = request.into_checked().expect("legacy check passes");

    // Translate the wire bundle into the engine's structural commit.
    // The translation is the boilerplate today; per the audit §4.4 a
    // future `signal_channel!`-emitted dispatcher trait can absorb
    // most of this match.
    let mut commit = CommitRequest::new(table);
    for operation in checked.operations.iter() {
        assert_eq!(
            operation.verb(),
            operation.payload().signal_verb(),
            "per-operation verb still matches payload signal_verb()",
        );
        commit = match operation.payload().clone() {
            ThoughtRequest::Submit(thought) => commit.assert(thought),
            ThoughtRequest::Replace(thought) => commit.mutate(thought),
            ThoughtRequest::Retire(key) => commit.retract(key),
            ThoughtRequest::ListAll => {
                panic!("read-shaped payload inside a multi-operation write commit")
            }
        };
    }
    let receipt = engine
        .commit(commit)
        .expect("multi-operation commit succeeds");

    assert_eq!(receipt.operation_count(), 3);
    assert_eq!(receipt.snapshot(), SnapshotIdentifier::new(3));

    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(
        log.len(),
        pre_log_len + 1,
        "multi-operation commit lands one CommitLogEntry, not one per operation",
    );
    let entry = log
        .last()
        .expect("commit log has the multi-operation entry at the tail");
    assert_eq!(entry.snapshot(), receipt.snapshot());

    let per_operation_effects: Vec<SemaOperation> = entry
        .operations()
        .iter()
        .map(|operation| operation.operation())
        .collect();
    assert_eq!(
        per_operation_effects,
        vec![
            SemaOperation::Assert,
            SemaOperation::Mutate,
            SemaOperation::Retract
        ],
        "Sema operations preserve the wire order: Assert, then Mutate, then Retract",
    );

    let per_operation_keys: Vec<Option<&str>> = entry
        .operations()
        .iter()
        .map(|operation| operation.key().map(RecordKey::as_str))
        .collect();
    assert_eq!(
        per_operation_keys,
        vec![Some("beta"), Some("alpha"), Some("gamma")]
    );

    // Confirm the commit's effects landed atomically: alpha revised,
    // beta added, gamma gone.
    let snapshot = engine
        .match_records(QueryPlan::all(table))
        .expect("match succeeds");
    let mut records = snapshot.records().to_vec();
    records.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        records,
        vec![
            Thought::new("alpha", "revised"),
            Thought::new("beta", "newcomer"),
        ],
    );
}

#[test]
fn signal_core_match_operation_lands_as_engine_match_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");

    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed alpha");
    engine
        .assert(Assertion::new(table, Thought::new("beta", "seeded")))
        .expect("seed beta");

    let query = ThoughtRequest::ListAll.into_request();
    let checked = query.into_checked().expect("legacy check passes");
    let operation = checked.operations.head();
    assert_eq!(operation.verb(), SignalVerb::Match);
    assert_eq!(operation.payload().signal_verb(), SignalVerb::Match);

    let snapshot = dispatch_single(&engine, table, operation);

    // Match observes the latest committed snapshot from the seeds
    // above without writing a new commit-log entry.
    assert_eq!(snapshot, SnapshotIdentifier::new(2));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(
        log.len(),
        2,
        "Match must not write a commit-log entry — only Assert/Mutate/Retract do",
    );
}

#[test]
fn signal_core_legacy_check_catches_verb_payload_mismatch_before_engine_call() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let pre_log_len = engine.commit_log().expect("commit log reads").len();

    // Hand-build an Operation whose declared verb disagrees with
    // its payload's signal_verb(). signal-core's transitional Request
    // check must reject before any engine call runs.
    let operation = Operation::new(
        SignalVerb::Mutate,
        ThoughtRequest::Submit(Thought::new("alpha", "first")),
    );
    let request: Request<ThoughtRequest> = Request::from_operations(NonEmpty::single(operation));

    let rejection = request
        .check()
        .expect_err("verb/payload mismatch must be rejected");
    assert!(matches!(
        rejection,
        RequestRejectionReason::VerbPayloadMismatch { index: 0 }
    ));

    let post_log_len = engine.commit_log().expect("commit log reads").len();
    assert_eq!(
        post_log_len, pre_log_len,
        "no engine commit happened — the wire layer rejected first",
    );

    // Ensure the table is still empty.
    let snapshot = engine
        .match_records(QueryPlan::all(table))
        .expect("match succeeds");
    assert!(snapshot.records().is_empty());
}
