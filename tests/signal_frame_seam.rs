//! Witness tests for the `signal-frame` -> `sema-engine` seam.
//!
//! Public component contracts carry contract-local payloads. The daemon
//! lowers those typed payloads into Sema engine calls, and the engine
//! stamps the resulting `signal-sema` operation class into its commit
//! log. No universal frame verb participates in that path.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitRequest, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, SnapshotIdentifier, TableDescriptor,
    TableName,
};
use signal_frame::{NonEmpty, Request, RequestPayload};
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

/// Toy contract payload. In a real component this enum is emitted by
/// the signal contract crate. The frame kernel only carries payloads;
/// daemon dispatch maps each variant into an engine operation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ThoughtRequest {
    Submit(Thought),
    Replace(Thought),
    Retire(RecordKey),
    ListAll,
}

impl RequestPayload for ThoughtRequest {}

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
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }
}

struct EngineDispatcher<'engine> {
    engine: &'engine Engine,
    table: sema_engine::TableReference<Thought>,
}

impl<'engine> EngineDispatcher<'engine> {
    fn new(engine: &'engine Engine, table: sema_engine::TableReference<Thought>) -> Self {
        Self { engine, table }
    }

    fn dispatch_single(&self, request: &ThoughtRequest) -> SnapshotIdentifier {
        match request {
            ThoughtRequest::Submit(thought) => {
                let receipt = self
                    .engine
                    .assert(Assertion::new(self.table, thought.clone()))
                    .expect("assert succeeds");
                assert_eq!(receipt.operation(), SemaOperation::Assert);
                receipt.snapshot()
            }
            ThoughtRequest::Replace(thought) => {
                let receipt = self
                    .engine
                    .mutate(Mutation::new(self.table, thought.clone()))
                    .expect("mutate succeeds");
                assert_eq!(receipt.operation(), SemaOperation::Mutate);
                receipt.snapshot()
            }
            ThoughtRequest::Retire(key) => {
                let receipt = self
                    .engine
                    .retract(Retraction::new(self.table, key.clone()))
                    .expect("retract succeeds");
                assert_eq!(receipt.operation(), SemaOperation::Retract);
                receipt.snapshot()
            }
            ThoughtRequest::ListAll => {
                let snapshot = self
                    .engine
                    .match_records(QueryPlan::all(self.table))
                    .expect("match succeeds");
                assert_eq!(snapshot.operation(), SemaOperation::Match);
                snapshot.snapshot()
            }
        }
    }

    fn dispatch_commit(&self, request: Request<ThoughtRequest>) -> sema_engine::CommitReceipt {
        let mut commit = CommitRequest::new(self.table);
        for payload in request.payloads().iter() {
            commit = match payload.clone() {
                ThoughtRequest::Submit(thought) => commit.assert(thought),
                ThoughtRequest::Replace(thought) => commit.mutate(thought),
                ThoughtRequest::Retire(key) => commit.retract(key),
                ThoughtRequest::ListAll => {
                    panic!("read-shaped payload inside a multi-operation write commit")
                }
            };
        }
        self.engine
            .commit(commit)
            .expect("multi-operation commit succeeds")
    }
}

#[test]
fn signal_frame_submit_payload_lands_as_engine_assert_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let dispatcher = EngineDispatcher::new(&engine, table);

    let request = ThoughtRequest::Submit(Thought::new("alpha", "first thought")).into_request();
    let snapshot = dispatcher.dispatch_single(request.payloads().head());

    assert_eq!(snapshot, SnapshotIdentifier::new(1));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].snapshot(), snapshot);
    let head = log[0].operations().head();
    assert_eq!(head.operation(), SemaOperation::Assert);
    assert_eq!(head.table_name(), "thoughts");
    assert_eq!(
        head.key().map(RecordKey::to_owned_string).as_deref(),
        Some("alpha")
    );
}

#[test]
fn signal_frame_replace_payload_lands_as_engine_mutate_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let dispatcher = EngineDispatcher::new(&engine, table);

    let seed = ThoughtRequest::Submit(Thought::new("alpha", "original")).into_request();
    let _ = dispatcher.dispatch_single(seed.payloads().head());

    let mutate = ThoughtRequest::Replace(Thought::new("alpha", "revised")).into_request();
    let snapshot = dispatcher.dispatch_single(mutate.payloads().head());

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
    assert_eq!(
        head.key().map(RecordKey::to_owned_string).as_deref(),
        Some("alpha")
    );

    let read = engine
        .match_records(QueryPlan::key(table, RecordKey::new("alpha")))
        .expect("match succeeds");
    assert_eq!(read.records(), &[Thought::new("alpha", "revised")]);
}

#[test]
fn signal_frame_retire_payload_lands_as_engine_retract_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let dispatcher = EngineDispatcher::new(&engine, table);

    let seed = ThoughtRequest::Submit(Thought::new("alpha", "soon retired")).into_request();
    let _ = dispatcher.dispatch_single(seed.payloads().head());

    let retire = ThoughtRequest::Retire(RecordKey::new("alpha")).into_request();
    let snapshot = dispatcher.dispatch_single(retire.payloads().head());

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
            .map(RecordKey::to_owned_string)
            .as_deref(),
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
fn signal_frame_multi_payload_request_lands_as_one_ordered_commit_log_entry() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let dispatcher = EngineDispatcher::new(&engine, table);

    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed alpha");
    engine
        .assert(Assertion::new(table, Thought::new("gamma", "seeded")))
        .expect("seed gamma");
    let pre_log_len = engine.commit_log().expect("commit log reads").len();

    let payloads = NonEmpty::try_from_vec(vec![
        ThoughtRequest::Submit(Thought::new("beta", "newcomer")),
        ThoughtRequest::Replace(Thought::new("alpha", "revised")),
        ThoughtRequest::Retire(RecordKey::new("gamma")),
    ])
    .expect("non-empty vector");
    let request: Request<ThoughtRequest> = Request::from_payloads(payloads);
    let receipt = dispatcher.dispatch_commit(request);

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
        "Sema operations preserve payload order: Assert, then Mutate, then Retract",
    );

    let per_operation_keys: Vec<Option<String>> = entry
        .operations()
        .iter()
        .map(|operation| operation.key().map(RecordKey::to_owned_string))
        .collect();
    assert_eq!(
        per_operation_keys,
        vec![
            Some("beta".to_owned()),
            Some("alpha".to_owned()),
            Some("gamma".to_owned())
        ]
    );

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
fn signal_frame_match_payload_lands_as_engine_match_with_matching_operation() {
    let fixture = SeamFixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let dispatcher = EngineDispatcher::new(&engine, table);

    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed alpha");
    engine
        .assert(Assertion::new(table, Thought::new("beta", "seeded")))
        .expect("seed beta");

    let query = ThoughtRequest::ListAll.into_request();
    let snapshot = dispatcher.dispatch_single(query.payloads().head());

    assert_eq!(snapshot, SnapshotIdentifier::new(2));
    let log = engine.commit_log().expect("commit log reads");
    assert_eq!(
        log.len(),
        2,
        "Match must not write a commit-log entry — only Assert/Mutate/Retract do",
    );
}
