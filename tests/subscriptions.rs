use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, CommitRequest, DeltaKind, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan,
    RecordKey, Retraction, SequenceRange, SinkError, SnapshotId, SubscriptionDeliveryMode,
    SubscriptionEvent, SubscriptionSink, TableDescriptor, TableName,
};
use tempfile::TempDir;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct SubscribedRecord {
    key: String,
    body: String,
}

impl SubscribedRecord {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for SubscribedRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

struct SubscriptionFixture {
    directory: TempDir,
}

impl SubscriptionFixture {
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

    fn descriptor(&self) -> TableDescriptor<SubscribedRecord> {
        TableDescriptor::new(TableName::new("subscribed_records"))
    }
}

struct RecordEventLog {
    events: Mutex<Vec<SubscriptionEvent<SubscribedRecord>>>,
    delivered: Mutex<Option<Sender<()>>>,
}

impl RecordEventLog {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            delivered: Mutex::new(None),
        }
    }

    fn with_signal(delivered: Sender<()>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            delivered: Mutex::new(Some(delivered)),
        }
    }

    fn events(&self) -> Vec<SubscriptionEvent<SubscribedRecord>> {
        self.events.lock().expect("events lock").clone()
    }
}

impl SubscriptionSink<SubscribedRecord> for RecordEventLog {
    fn deliver(&self, event: SubscriptionEvent<SubscribedRecord>) -> Result<(), SinkError> {
        let is_delta = matches!(event, SubscriptionEvent::Delta(_));
        self.events.lock().expect("events lock").push(event);
        if is_delta && let Some(delivered) = self.delivered.lock().expect("delivered lock").as_ref()
        {
            let _ = delivered.send(());
        }
        Ok(())
    }
}

struct DeltaFailingSink {
    initial_events: Mutex<Vec<SubscriptionEvent<SubscribedRecord>>>,
    failed_delta: Sender<()>,
}

impl DeltaFailingSink {
    fn new(failed_delta: Sender<()>) -> Self {
        Self {
            initial_events: Mutex::new(Vec::new()),
            failed_delta,
        }
    }
}

impl SubscriptionSink<SubscribedRecord> for DeltaFailingSink {
    fn deliver(&self, event: SubscriptionEvent<SubscribedRecord>) -> Result<(), SinkError> {
        match event {
            SubscriptionEvent::InitialSnapshot(_) => {
                self.initial_events
                    .lock()
                    .expect("initial events lock")
                    .push(event);
                Ok(())
            }
            SubscriptionEvent::Delta(_) => {
                self.failed_delta.send(()).expect("failure signal sends");
                Err(SinkError::new("queue full"))
            }
        }
    }
}

struct InlineRecordEventLog {
    events: Mutex<Vec<SubscriptionEvent<SubscribedRecord>>>,
}

impl InlineRecordEventLog {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<SubscriptionEvent<SubscribedRecord>> {
        self.events.lock().expect("events lock").clone()
    }
}

impl SubscriptionSink<SubscribedRecord> for InlineRecordEventLog {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }

    fn deliver(&self, event: SubscriptionEvent<SubscribedRecord>) -> Result<(), SinkError> {
        self.events.lock().expect("events lock").push(event);
        Ok(())
    }
}

struct BlockingDeltaSink {
    entered: Sender<()>,
    release: Mutex<Receiver<()>>,
}

impl BlockingDeltaSink {
    fn new(entered: Sender<()>, release: Receiver<()>) -> Self {
        Self {
            entered,
            release: Mutex::new(release),
        }
    }
}

impl SubscriptionSink<SubscribedRecord> for BlockingDeltaSink {
    fn deliver(&self, event: SubscriptionEvent<SubscribedRecord>) -> Result<(), SinkError> {
        match event {
            SubscriptionEvent::InitialSnapshot(_) => Ok(()),
            SubscriptionEvent::Delta(_) => {
                self.entered.send(()).expect("entered signal sends");
                self.release
                    .lock()
                    .expect("release lock")
                    .recv()
                    .expect("release signal arrives");
                Ok(())
            }
        }
    }
}

#[test]
fn subscribe_initial_snapshot_uses_latest_committed_snapshot() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "one"),
        ))
        .expect("first assert succeeds");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("beta", "two"),
        ))
        .expect("second assert succeeds");

    let sink = Arc::new(RecordEventLog::new());
    let receipt = engine
        .subscribe(QueryPlan::all(records), sink.clone())
        .expect("subscription succeeds");
    let registrations = engine
        .subscription_registrations()
        .expect("registrations read");

    assert_eq!(receipt.handle().id().value(), 1);
    assert_eq!(receipt.initial().snapshot().snapshot(), SnapshotId::new(2));
    assert_eq!(receipt.initial().snapshot().records().len(), 2);
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].id().value(), 1);
    assert_eq!(registrations[0].table_name(), "subscribed_records");
    assert_eq!(registrations[0].snapshot(), SnapshotId::new(2));
    assert!(matches!(
        &sink.events()[0],
        SubscriptionEvent::InitialSnapshot(initial)
            if initial.snapshot().snapshot() == SnapshotId::new(2)
    ));
}

#[test]
fn subscribe_delta_fires_after_commit_is_visible() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let (delivered_sender, delivered_receiver) = mpsc::channel();
    let sink = Arc::new(RecordEventLog::with_signal(delivered_sender));
    engine
        .subscribe(QueryPlan::all(records), sink.clone())
        .expect("subscription succeeds");

    let receipt = engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "visible"),
        ))
        .expect("assert succeeds");
    let matched = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match succeeds");

    assert_eq!(receipt.snapshot(), SnapshotId::new(1));
    delivered_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("delta is delivered");
    assert_eq!(
        matched.records(),
        &[SubscribedRecord::new("alpha", "visible")]
    );
    assert!(sink.events().iter().any(|event| {
        matches!(
            event,
            SubscriptionEvent::Delta(delta)
                if delta.snapshot() == SnapshotId::new(1)
                    && delta.record() == &SubscribedRecord::new("alpha", "visible")
        )
    }));
}

#[test]
fn subscribe_delta_kind_tracks_write_verb() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "first"),
        ))
        .expect("assert succeeds before subscription");
    let sink = Arc::new(InlineRecordEventLog::new());
    engine
        .subscribe(QueryPlan::all(records), sink.clone())
        .expect("subscription succeeds");

    engine
        .mutate(Mutation::new(
            records,
            SubscribedRecord::new("alpha", "second"),
        ))
        .expect("mutate succeeds");
    engine
        .retract(Retraction::new(records, RecordKey::new("alpha")))
        .expect("retract succeeds");

    let delta_kinds = sink
        .events()
        .into_iter()
        .filter_map(|event| match event {
            SubscriptionEvent::InitialSnapshot(_) => None,
            SubscriptionEvent::Delta(delta) => Some(delta.kind()),
        })
        .collect::<Vec<_>>();

    assert_eq!(delta_kinds, [DeltaKind::Mutate, DeltaKind::Retract]);
}

#[test]
fn subscribe_commit_bundle_delivers_per_operation_deltas_after_single_snapshot_commit() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "first"),
        ))
        .expect("first seed succeeds");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("gamma", "retire"),
        ))
        .expect("second seed succeeds");
    let sink = Arc::new(InlineRecordEventLog::new());
    engine
        .subscribe(QueryPlan::all(records), sink.clone())
        .expect("subscription succeeds");

    let receipt = engine
        .commit(
            CommitRequest::new(records)
                .assert(SubscribedRecord::new("beta", "new"))
                .mutate(SubscribedRecord::new("alpha", "second"))
                .retract(RecordKey::new("gamma")),
        )
        .expect("commit bundle succeeds");

    let delta_facts = sink
        .events()
        .into_iter()
        .filter_map(|event| match event {
            SubscriptionEvent::InitialSnapshot(_) => None,
            SubscriptionEvent::Delta(delta) => {
                Some((delta.kind(), delta.snapshot(), delta.record().clone()))
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(receipt.snapshot(), SnapshotId::new(3));
    assert_eq!(receipt.operation_count(), 3);
    assert_eq!(
        delta_facts,
        [
            (
                DeltaKind::Assert,
                SnapshotId::new(3),
                SubscribedRecord::new("beta", "new")
            ),
            (
                DeltaKind::Mutate,
                SnapshotId::new(3),
                SubscribedRecord::new("alpha", "second")
            ),
            (
                DeltaKind::Retract,
                SnapshotId::new(3),
                SubscribedRecord::new("gamma", "retire")
            )
        ]
    );
}

#[test]
fn subscribe_sink_failure_does_not_roll_back_commit() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let (failed_sender, failed_receiver) = mpsc::channel();
    let sink = Arc::new(DeltaFailingSink::new(failed_sender));
    engine
        .subscribe(QueryPlan::all(records), sink)
        .expect("subscription succeeds");

    let receipt = engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "committed"),
        ))
        .expect("assert returns receipt despite sink failure");
    let matched = engine
        .match_records(QueryPlan::key(records, RecordKey::new("alpha")))
        .expect("match succeeds");

    assert_eq!(receipt.snapshot(), SnapshotId::new(1));
    failed_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("failing sink receives delta");
    assert_eq!(
        matched.records(),
        &[SubscribedRecord::new("alpha", "committed")]
    );
}

#[test]
fn subscribe_inline_sink_receives_delta_before_assert_returns() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let sink = Arc::new(InlineRecordEventLog::new());
    engine
        .subscribe(QueryPlan::all(records), sink.clone())
        .expect("subscription succeeds");

    let receipt = engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "inline"),
        ))
        .expect("assert succeeds");

    assert_eq!(receipt.snapshot(), SnapshotId::new(1));
    assert!(sink.events().iter().any(|event| {
        matches!(
            event,
            SubscriptionEvent::Delta(delta)
                if delta.snapshot() == SnapshotId::new(1)
                    && delta.record() == &SubscribedRecord::new("alpha", "inline")
        )
    }));
}

#[test]
fn subscribe_blocking_sink_does_not_freeze_later_writes() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let sink = Arc::new(BlockingDeltaSink::new(entered_sender, release_receiver));
    engine
        .subscribe(QueryPlan::all(records), sink)
        .expect("subscription succeeds");
    let engine = Arc::new(engine);
    let blocked_engine = Arc::clone(&engine);

    let blocked = std::thread::spawn(move || {
        blocked_engine
            .assert(Assertion::new(
                records,
                SubscribedRecord::new("alpha", "blocked sink"),
            ))
            .expect("blocked assert eventually returns")
    });
    entered_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("blocking sink entered");

    let second = engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("beta", "later write"),
        ))
        .expect("later write succeeds while first delivery is blocked");
    release_sender.send(()).expect("release signal sends");
    release_sender
        .send(())
        .expect("second release signal sends");
    let first = blocked.join().expect("blocked thread joins");
    let matched = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");

    assert_eq!(first.snapshot(), SnapshotId::new(1));
    assert_eq!(second.snapshot(), SnapshotId::new(2));
    assert_eq!(matched.records().len(), 2);
}

#[test]
fn subscribe_survives_process_restart_as_registration() {
    let fixture = SubscriptionFixture::new();
    {
        let mut engine = fixture.open_engine();
        let records = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        let sink = Arc::new(RecordEventLog::new());
        engine
            .subscribe(QueryPlan::all(records), sink)
            .expect("subscription succeeds");
    }

    let reopened = fixture.open_engine();
    let registrations = reopened
        .subscription_registrations()
        .expect("registrations read");

    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].id().value(), 1);
    assert_eq!(registrations[0].table_name(), "subscribed_records");
}

#[test]
fn commit_log_range_replays_from_snapshot_cursor() {
    let fixture = SubscriptionFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.descriptor())
        .expect("table registers");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("alpha", "one"),
        ))
        .expect("first assert succeeds");
    engine
        .assert(Assertion::new(
            records,
            SubscribedRecord::new("beta", "two"),
        ))
        .expect("second assert succeeds");

    let replay = engine
        .commit_log_range(SequenceRange::from(SnapshotId::new(2)))
        .expect("commit log range reads");

    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].snapshot(), SnapshotId::new(2));
    assert_eq!(
        replay[0].operations().head().key().map(RecordKey::as_str),
        Some("beta")
    );
}
