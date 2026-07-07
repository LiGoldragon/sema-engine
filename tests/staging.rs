//! Witnesses for the durable staging seam: an engaged session buffers
//! the ordinary write surface into one un-committed operation group
//! with read-your-writes overlay reads; the parked group either
//! materializes atomically — byte-for-byte what the equivalent direct
//! writes would have committed — or discards without a trace; and an
//! occupied slot survives a process restart for crash recovery.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, DeltaKind, Engine, EngineOpen, EngineRecord, Error, FamilyDirectory, FamilyName,
    IdentifiedAssertion, IdentifiedQueryPlan, IdentifiedTableDescriptor, IdentifiedTableReference,
    Mutation, QueryPlan, RecordKey, Retraction, RowMaterializer, SchemaHash, SchemaVersion,
    SinkError, SubscriptionDeliveryMode, SubscriptionEvent, SubscriptionSink, TableDescriptor,
    TableName, TableReference, VersionedStoreName, VersioningPolicy,
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

impl FamilyDirectory for Families {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().family().as_str() {
            "thought" => row.apply(self.thoughts),
            "tally" => row.apply_identified(self.tallies),
            other => Err(Error::FamilyUnknown {
                family: other.to_owned(),
            }),
        }
    }

    fn announce(&self, delta: sema_engine::DeltaAnnouncer<'_>) -> sema_engine::Result<()> {
        match delta.family().family().as_str() {
            "thought" => delta.announce(self.thoughts),
            "tally" => delta.announce_identified(self.tallies),
            other => Err(Error::FamilyUnknown {
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

    /// Twin stores share the store NAME (digests fold it) while living
    /// at distinct paths, so a staged store and a direct store produce
    /// comparable bytes.
    fn open_versioned_as(&self, path_name: &str, store_name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(path_name), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(store_name))),
        )
        .expect("versioned engine opens")
    }

    fn open_versioned(&self, name: &str) -> Engine {
        self.open_versioned_as(name, name)
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

struct Registered {
    engine: Engine,
    families: Families,
}

impl Registered {
    fn open(fixture: &Fixture, path_name: &str, store_name: &str) -> Self {
        let mut engine = fixture.open_versioned_as(path_name, store_name);
        let thoughts = engine
            .register_table(fixture.thought_descriptor())
            .expect("thoughts register");
        let tallies = engine
            .register_identified_table(fixture.tally_descriptor())
            .expect("tallies register");
        Self {
            engine,
            families: Families { thoughts, tallies },
        }
    }

    /// Every observable durable surface, for no-trace and twin
    /// comparisons.
    fn observable_state(&self) -> ObservableState {
        ObservableState {
            versioned_log: self
                .engine
                .versioned_commit_log()
                .expect("versioned log reads"),
            commit_log_length: self.engine.commit_log().expect("commit log reads").len(),
            outbox: self
                .engine
                .unshipped_outbox()
                .expect("outbox reads")
                .iter()
                .map(|row| (row.commit_sequence().value(), row.entry_digest()))
                .collect(),
            chain_head: self
                .engine
                .versioned_chain_head()
                .expect("chain head reads"),
            commit_sequence: self
                .engine
                .current_commit_sequence()
                .expect("sequence reads")
                .value(),
            snapshot: self
                .engine
                .latest_snapshot()
                .expect("snapshot reads")
                .value(),
            thoughts: self
                .engine
                .match_records(QueryPlan::all(self.families.thoughts))
                .expect("thoughts read")
                .records()
                .to_vec(),
            tallies: self
                .engine
                .match_identified(IdentifiedQueryPlan::all(self.families.tallies))
                .expect("tallies read")
                .records()
                .iter()
                .map(|row| (row.identifier().value(), row.value().clone()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObservableState {
    versioned_log: Vec<sema_engine::VersionedCommitLogEntry>,
    commit_log_length: usize,
    outbox: Vec<(u64, sema_engine::EntryDigest)>,
    chain_head: Option<sema_engine::EntryDigest>,
    commit_sequence: u64,
    snapshot: u64,
    thoughts: Vec<Thought>,
    tallies: Vec<(u64, Tally)>,
}

/// The operation sequence both twins run: a plain assert, an
/// identified assert, a read-dependent assert (its key derives from a
/// full-table read that must see the earlier writes — the
/// supersede-shaped case), a mutate, and a retract.
struct SequenceReceipts {
    tally_identifier: u64,
    derived_key: String,
}

impl SequenceReceipts {
    fn run(registered: &Registered) -> Self {
        let engine = &registered.engine;
        let families = &registered.families;
        engine
            .assert(Assertion::new(
                families.thoughts,
                Thought::new("alpha", "first"),
            ))
            .expect("alpha asserts");
        let tally = engine
            .assert_identified(IdentifiedAssertion::new(
                families.tallies,
                Tally::new("one"),
            ))
            .expect("tally asserts");
        // Read-dependent step: the key of the next record derives from
        // what a full-table read observes right now, so staged-vs-direct
        // equality proves the read-your-writes overlay.
        let visible = engine
            .match_records(QueryPlan::all(families.thoughts))
            .expect("thoughts read mid-sequence");
        let derived_key = format!("derived-{}", visible.records().len());
        engine
            .assert(Assertion::new(
                families.thoughts,
                Thought::new(derived_key.clone(), "second"),
            ))
            .expect("derived asserts");
        engine
            .mutate(Mutation::new(
                families.thoughts,
                Thought::new("alpha", "revised"),
            ))
            .expect("alpha mutates");
        engine
            .retract(Retraction::new(families.thoughts, RecordKey::new("alpha")))
            .expect("alpha retracts");
        Self {
            tally_identifier: tally.identifier().value(),
            derived_key,
        }
    }
}

#[test]
fn stage_then_discard_leaves_no_observable_trace() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "discard", "discard");
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("standing", "committed before staging"),
        ))
        .expect("baseline write");
    let before = registered.observable_state();

    registered
        .engine
        .begin_staged_group()
        .expect("session engages");
    SequenceReceipts::run(&registered);
    let receipt = registered
        .engine
        .park_staged_group()
        .expect("group parks")
        .expect("non-empty group stages");
    assert_ne!(
        Some(receipt.prospective_head()),
        before.chain_head,
        "the prospective head names a would-be advance"
    );

    let discard = registered
        .engine
        .discard_staged_group()
        .expect("group discards");
    assert_eq!(discard.prospective_head(), receipt.prospective_head());
    assert_eq!(discard.entry_count(), receipt.entry_count());

    let after = registered.observable_state();
    assert_eq!(
        before, after,
        "a discarded group must leave records, head, logs, outbox, and counters unchanged"
    );
    assert!(
        registered
            .engine
            .staged_group()
            .expect("slot reads")
            .is_none(),
        "the slot is empty after a discard"
    );
}

#[test]
fn staged_group_materializes_byte_for_byte_with_direct_writes() {
    let fixture = Fixture::new();
    let direct = Registered::open(&fixture, "direct-twin", "twin");
    let staged = Registered::open(&fixture, "staged-twin", "twin");

    let direct_receipts = SequenceReceipts::run(&direct);

    staged.engine.begin_staged_group().expect("session engages");
    let staged_receipts = SequenceReceipts::run(&staged);
    assert_eq!(
        direct_receipts.tally_identifier, staged_receipts.tally_identifier,
        "identified mints must match the direct path"
    );
    assert_eq!(
        direct_receipts.derived_key, staged_receipts.derived_key,
        "read-dependent key derivation must observe the staged overlay"
    );
    let receipt = staged
        .engine
        .park_staged_group()
        .expect("group parks")
        .expect("non-empty group stages");
    staged
        .engine
        .materialize_staged_group(&staged.families)
        .expect("group materializes");

    let direct_state = direct.observable_state();
    let staged_state = staged.observable_state();
    assert_eq!(
        direct_state, staged_state,
        "stage-then-materialize must equal the direct writes byte-for-byte"
    );
    assert_eq!(
        Some(receipt.prospective_head()),
        staged_state.chain_head,
        "the granted digest is exactly the head the store now stands at"
    );

    // The next identified mint continues identically on both twins,
    // proving the durable counters advanced the same way.
    let direct_next = direct
        .engine
        .assert_identified(IdentifiedAssertion::new(
            direct.families.tallies,
            Tally::new("after"),
        ))
        .expect("direct next mint");
    let staged_next = staged
        .engine
        .assert_identified(IdentifiedAssertion::new(
            staged.families.tallies,
            Tally::new("after"),
        ))
        .expect("staged next mint");
    assert_eq!(
        direct_next.identifier().value(),
        staged_next.identifier().value()
    );
}

#[test]
fn prospective_head_equals_materialized_head() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "prospect", "prospect");
    registered.engine.begin_staged_group().expect("engages");
    SequenceReceipts::run(&registered);
    let receipt = registered
        .engine
        .park_staged_group()
        .expect("parks")
        .expect("stages");
    let materialized = registered
        .engine
        .materialize_staged_group(&registered.families)
        .expect("materializes");
    assert_eq!(materialized.head(), receipt.prospective_head());
    assert_eq!(
        registered
            .engine
            .versioned_chain_head()
            .expect("head reads"),
        Some(receipt.prospective_head())
    );
}

#[test]
fn occupied_slot_survives_reopen_and_recovery_materialize_matches_the_twin() {
    let fixture = Fixture::new();
    // The pre-crash twin materializes immediately; the crashing store
    // parks, reopens, and materializes in recovery.
    let twin = Registered::open(&fixture, "recover-twin", "recover");
    twin.engine.begin_staged_group().expect("twin engages");
    SequenceReceipts::run(&twin);
    twin.engine
        .park_staged_group()
        .expect("twin parks")
        .expect("twin stages");
    twin.engine
        .materialize_staged_group(&twin.families)
        .expect("twin materializes");

    let receipt = {
        let crashing = Registered::open(&fixture, "recover", "recover");
        crashing.engine.begin_staged_group().expect("engages");
        SequenceReceipts::run(&crashing);
        crashing
            .engine
            .park_staged_group()
            .expect("parks")
            .expect("stages")
        // The engine drops here: the process "crashed" between the
        // grant and the materialization.
    };

    let reopened = Registered::open(&fixture, "recover", "recover");
    let summary = reopened
        .engine
        .staged_group()
        .expect("slot reads")
        .expect("the parked slot survives the restart");
    assert_eq!(summary.prospective_head(), receipt.prospective_head());
    assert_eq!(summary.entry_count(), receipt.entry_count());
    assert_eq!(summary.base_predecessor(), None);

    reopened
        .engine
        .materialize_staged_group(&reopened.families)
        .expect("recovery materializes");
    assert!(
        reopened
            .engine
            .staged_group()
            .expect("slot reads")
            .is_none()
    );
    assert_eq!(
        reopened.observable_state(),
        twin.observable_state(),
        "recovery materialization equals materializing before the crash"
    );
}

#[test]
fn occupied_slot_resolves_by_discard_after_reopen() {
    let fixture = Fixture::new();
    {
        let crashing = Registered::open(&fixture, "recover-discard", "recover-discard");
        crashing.engine.begin_staged_group().expect("engages");
        SequenceReceipts::run(&crashing);
        crashing
            .engine
            .park_staged_group()
            .expect("parks")
            .expect("stages");
    }
    let reopened = Registered::open(&fixture, "recover-discard", "recover-discard");
    let fresh_twin = Registered::open(&fixture, "recover-discard-twin", "recover-discard");
    assert!(reopened.engine.staged_group().expect("reads").is_some());
    reopened.engine.discard_staged_group().expect("discards");
    assert!(reopened.engine.staged_group().expect("reads").is_none());
    assert_eq!(
        reopened.observable_state(),
        fresh_twin.observable_state(),
        "a discarded recovery slot leaves the store exactly as never written"
    );
}

#[test]
fn begin_is_refused_while_a_parked_group_stands_or_a_session_is_engaged() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "refuse-begin", "refuse-begin");

    registered.engine.begin_staged_group().expect("engages");
    assert!(
        matches!(
            registered.engine.begin_staged_group(),
            Err(Error::StagingSessionEngaged)
        ),
        "a second engagement is a typed refusal"
    );
    SequenceReceipts::run(&registered);
    registered
        .engine
        .park_staged_group()
        .expect("parks")
        .expect("stages");
    assert!(
        matches!(
            registered.engine.begin_staged_group(),
            Err(Error::StagingSlotOccupied)
        ),
        "an unresolved parked group refuses a new build"
    );
}

#[test]
fn materialize_after_an_interleaved_commit_is_refused_fail_closed() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "interleave", "interleave");
    registered.engine.begin_staged_group().expect("engages");
    SequenceReceipts::run(&registered);
    registered
        .engine
        .park_staged_group()
        .expect("parks")
        .expect("stages");
    // A write landing after the park (for example a Disabled-mode
    // writer or an older engine) moves the base out from under the
    // parked group.
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("interloper", "moved the head"),
        ))
        .expect("interleaved write commits directly");
    assert!(
        matches!(
            registered
                .engine
                .materialize_staged_group(&registered.families),
            Err(Error::StagingBaseMoved { .. })
        ),
        "a parked group whose base moved must refuse to materialize"
    );
    registered.engine.discard_staged_group().expect("discards");
}

#[test]
fn empty_park_stages_nothing_and_disengages() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "empty-park", "empty-park");
    registered.engine.begin_staged_group().expect("engages");
    assert!(
        registered
            .engine
            .park_staged_group()
            .expect("empty park succeeds")
            .is_none(),
        "an empty buffer parks nothing"
    );
    assert!(
        registered
            .engine
            .staged_group()
            .expect("slot reads")
            .is_none()
    );
    // Disengaged: a direct write commits normally again.
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("direct", "after empty park"),
        ))
        .expect("direct write after disengage");
    assert_eq!(
        registered
            .engine
            .versioned_commit_log()
            .expect("log reads")
            .len(),
        1
    );
}

#[test]
fn abandon_drops_the_buffer_without_a_durable_trace() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "abandon", "abandon");
    let before = registered.observable_state();
    registered.engine.begin_staged_group().expect("engages");
    SequenceReceipts::run(&registered);
    registered.engine.abandon_staged_group().expect("abandons");
    assert_eq!(before, registered.observable_state());
    assert!(
        registered
            .engine
            .staged_group()
            .expect("slot reads")
            .is_none()
    );
    registered
        .engine
        .abandon_staged_group()
        .expect("abandon is idempotent");
}

#[test]
fn materialize_and_discard_on_an_empty_slot_are_typed_refusals() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "empty-slot", "empty-slot");
    assert!(matches!(
        registered
            .engine
            .materialize_staged_group(&registered.families),
        Err(Error::StagingSlotEmpty { .. })
    ));
    assert!(matches!(
        registered.engine.discard_staged_group(),
        Err(Error::StagingSlotEmpty { .. })
    ));
    assert!(matches!(
        registered.engine.park_staged_group(),
        Err(Error::StagingSessionNotEngaged { .. })
    ));
}

#[test]
fn engaged_reads_overlay_and_disengaged_reads_return_to_committed_state() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "overlay", "overlay");
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("committed", "before"),
        ))
        .expect("baseline");
    let committed_sequence = registered
        .engine
        .current_commit_sequence()
        .expect("sequence reads");

    registered.engine.begin_staged_group().expect("engages");
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("staged", "buffered"),
        ))
        .expect("staged assert buffers");
    registered
        .engine
        .retract(Retraction::new(
            registered.families.thoughts,
            RecordKey::new("committed"),
        ))
        .expect("staged retract buffers");
    let tally = registered
        .engine
        .assert_identified(IdentifiedAssertion::new(
            registered.families.tallies,
            Tally::new("staged tally"),
        ))
        .expect("staged identified assert buffers");

    let engaged_thoughts = registered
        .engine
        .match_records(QueryPlan::all(registered.families.thoughts))
        .expect("engaged read");
    assert_eq!(
        engaged_thoughts.records(),
        &[Thought::new("staged", "buffered")],
        "the engaged read sees the staged assert and the staged retract"
    );
    let engaged_tallies = registered
        .engine
        .match_identified(IdentifiedQueryPlan::all(registered.families.tallies))
        .expect("engaged identified read");
    assert_eq!(engaged_tallies.records().len(), 1);
    assert_eq!(
        engaged_tallies.records()[0].identifier(),
        tally.identifier()
    );
    assert_eq!(
        registered
            .engine
            .current_commit_sequence()
            .expect("engaged sequence")
            .value(),
        committed_sequence.value() + 3,
        "the engaged sequence reflects the buffered entries"
    );

    registered.engine.abandon_staged_group().expect("abandons");
    let committed_thoughts = registered
        .engine
        .match_records(QueryPlan::all(registered.families.thoughts))
        .expect("committed read");
    assert_eq!(
        committed_thoughts.records(),
        &[Thought::new("committed", "before")],
        "after disengaging, reads return to committed state"
    );
    assert_eq!(
        registered
            .engine
            .current_commit_sequence()
            .expect("sequence reads"),
        committed_sequence
    );
}

struct RecordEventLog {
    events: Mutex<Vec<SubscriptionEvent<Thought>>>,
}

impl RecordEventLog {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn delta_kinds(&self) -> Vec<DeltaKind> {
        self.events
            .lock()
            .expect("events lock")
            .iter()
            .filter_map(|event| match event {
                SubscriptionEvent::Delta(delta) => Some(delta.kind()),
                SubscriptionEvent::InitialSnapshot(_) => None,
            })
            .collect()
    }
}

impl SubscriptionSink<Thought> for RecordEventLog {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }

    fn deliver(&self, event: SubscriptionEvent<Thought>) -> Result<(), SinkError> {
        self.events.lock().expect("events lock").push(event);
        Ok(())
    }
}

#[test]
fn subscription_deltas_deliver_at_materialize_not_at_buffer_time() {
    let fixture = Fixture::new();
    let registered = Registered::open(&fixture, "deltas", "deltas");
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("standing", "to be retracted"),
        ))
        .expect("baseline");
    let sink = Arc::new(RecordEventLog::new());
    registered
        .engine
        .subscribe(QueryPlan::all(registered.families.thoughts), sink.clone())
        .expect("subscription registers");

    registered.engine.begin_staged_group().expect("engages");
    registered
        .engine
        .assert(Assertion::new(
            registered.families.thoughts,
            Thought::new("fresh", "asserted"),
        ))
        .expect("buffers");
    registered
        .engine
        .mutate(Mutation::new(
            registered.families.thoughts,
            Thought::new("fresh", "mutated"),
        ))
        .expect("buffers");
    registered
        .engine
        .retract(Retraction::new(
            registered.families.thoughts,
            RecordKey::new("standing"),
        ))
        .expect("buffers");
    assert!(
        sink.delta_kinds().is_empty(),
        "nothing is delivered while the group is only buffered"
    );
    registered
        .engine
        .park_staged_group()
        .expect("parks")
        .expect("stages");
    assert!(
        sink.delta_kinds().is_empty(),
        "nothing is delivered while the group is only parked"
    );

    registered
        .engine
        .materialize_staged_group(&registered.families)
        .expect("materializes");
    assert_eq!(
        sink.delta_kinds(),
        vec![DeltaKind::Assert, DeltaKind::Mutate, DeltaKind::Retract],
        "materialization delivers exactly the deltas the direct writes would have"
    );
}
