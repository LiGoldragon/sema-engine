//! Falsification tests for the gaps named in
//! the May 17 sema-engine fit audit.
//!
//! Each test tries to disprove a claimed gap by composing existing
//! `Engine` API. The criterion: **the composition must be
//! elegant**. Inelegant composition counts as evidence the gap is
//! real; elegant composition dissolves the gap into ergonomic
//! preference.
//!
//! Outcomes are documented per test; the audit report's verdict
//! is revised in light of these results.
//!
//! Author: second-operator-assistant
//! Date: 2026-05-17

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash,
    SchemaVersion, SinkError, SnapshotIdentifier, SubscriptionEvent, SubscriptionIdentifier,
    SubscriptionSink, TableDescriptor, TableName,
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
struct ActivityEntry {
    key: String,
    note: String,
}

impl ActivityEntry {
    fn new(key: impl Into<String>, note: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            note: note.into(),
        }
    }
}

impl EngineRecord for ActivityEntry {
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

    fn database_path(&self) -> PathBuf {
        self.directory.path().join("engine.sema")
    }

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(self.database_path(), SchemaVersion::new(1)))
            .expect("engine opens")
    }

    fn thought_descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }
}

// ─── Gap 1 falsification: validate_write ─────────────────
//
// Claim under test (from audit §4.1): "sema-engine lacks an
// `Engine::validate_write(CommitRequest)` so every component
// re-implements integrity checks locally."
//
// Steelman against the gap: a component can dry-run any write
// using a small composition over `Engine::match_records` —
// without bypassing the typed surface, without re-implementing
// engine integrity logic, and without the engine growing a new
// API. If the composition is elegant, the gap is ergonomic
// preference, not capability.

#[test]
fn validate_write_dissolves_into_match_records_dry_run() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.thought_descriptor())
        .expect("table registers");

    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed");
    let pre_log_len = engine.commit_log().expect("commit log").len();

    // Dry-run a would-duplicate Assert with one match_records probe.
    // Three lines — no parallel reducer, no schema check
    // duplication, no domain-specific re-implementation.
    let probe = engine
        .match_records(QueryPlan::key(table, RecordKey::new("alpha")))
        .expect("probe");
    let would_duplicate = !probe.records().is_empty();
    assert!(would_duplicate);

    // Dry-run a fresh-key Assert: probe returns empty, so the
    // component knows the Assert would succeed.
    let fresh = engine
        .match_records(QueryPlan::key(table, RecordKey::new("beta")))
        .expect("probe");
    assert!(fresh.records().is_empty());

    // No engine writes occurred during dry-run.
    let post_log_len = engine.commit_log().expect("commit log").len();
    assert_eq!(post_log_len, pre_log_len);

    // Verdict: the "gap" was an ergonomic preference. Component-side
    // composition of `match_records` covers single-operation write
    // pre-validation in ~3 lines. No engine API extension required.
}

#[test]
fn multi_operation_write_dry_run_composes_with_match_records_plus_local_staging() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.thought_descriptor())
        .expect("table registers");

    engine
        .assert(Assertion::new(table, Thought::new("alpha", "seeded")))
        .expect("seed");
    let pre_log_len = engine.commit_log().expect("commit log").len();

    // A multi-operation batch the component wants to validate before
    // committing. Carries both a collision with existing data
    // ("alpha") and a within-batch duplicate ("beta" twice).
    let proposed: Vec<Thought> = vec![
        Thought::new("alpha", "would-collide-with-existing"),
        Thought::new("beta", "first beta"),
        Thought::new("beta", "second beta — within-batch duplicate"),
    ];

    // Dry-run: walk proposals, stage keys locally, probe engine
    // for existing.
    let mut staged: HashSet<RecordKey> = HashSet::new();
    let mut failure: Option<DryRunFailure> = None;
    for proposal in &proposed {
        let key = proposal.record_key();
        if !staged.insert(key.clone()) {
            failure = Some(DryRunFailure::DuplicateWithinBatch(key));
            break;
        }
        let probe = engine
            .match_records(QueryPlan::key(table, key.clone()))
            .expect("probe");
        if !probe.records().is_empty() {
            failure = Some(DryRunFailure::DuplicateWithExisting(key));
            break;
        }
    }

    assert_eq!(
        failure,
        Some(DryRunFailure::DuplicateWithExisting(RecordKey::new(
            "alpha"
        ))),
        "the first-found collision is the existing 'alpha' row",
    );
    let post_log_len = engine.commit_log().expect("commit log").len();
    assert_eq!(post_log_len, pre_log_len, "no engine write happened");

    // The whole dry-run is one `HashSet` + a `match_records` probe
    // per operation. About 12 lines of composition. The engine's own
    // typed errors (`DuplicateWriteKey`, `DuplicateAssertKey`,
    // `RecordNotFound`) map onto `DryRunFailure` variants
    // one-for-one — no domain-specific re-implementation, just a
    // sequence of probes.

    // Verdict: dissolves the multi-operation part of Gap 1. The
    // composition is small, typed, single-responsibility.
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DryRunFailure {
    DuplicateWithinBatch(RecordKey),
    DuplicateWithExisting(RecordKey),
}

// ─── Gap 3 falsification: unsubscribe ────────────────────
//
// Claim under test (from audit §4.3): "the engine has no
// `unsubscribe(SubscriptionHandle)` so the
// `SubscriptionRegistry` grows monotonically for the daemon's
// lifetime."
//
// Steelman against the gap: a component supervisor tracks
// active subscription IDs externally and the sink filters
// deltas by handle. For stable subscription sets, the engine
// registry's monotonic growth is bounded by daemon lifetime;
// daemon shutdown drops the engine and reclaims everything.

struct SupervisorBackedSink {
    active: Arc<Mutex<HashSet<SubscriptionIdentifier>>>,
    received: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl SubscriptionSink<Thought> for SupervisorBackedSink {
    fn delivery_mode(&self) -> sema_engine::SubscriptionDeliveryMode {
        sema_engine::SubscriptionDeliveryMode::Inline
    }
    fn deliver(&self, event: SubscriptionEvent<Thought>) -> Result<(), SinkError> {
        let handle_id = match &event {
            SubscriptionEvent::InitialSnapshot(initial) => initial.handle().id(),
            SubscriptionEvent::Delta(delta) => delta.handle().id(),
        };
        let active = self.active.lock().expect("supervisor lock");
        if active.contains(&handle_id) {
            self.received.fetch_add(1, Ordering::SeqCst);
        } else {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[test]
fn subscription_lifetime_can_be_managed_externally_via_handle_id_filter() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.thought_descriptor())
        .expect("table registers");

    let supervisor: Arc<Mutex<HashSet<SubscriptionIdentifier>>> =
        Arc::new(Mutex::new(HashSet::new()));
    let received = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let sink: Arc<dyn SubscriptionSink<Thought>> = Arc::new(SupervisorBackedSink {
        active: Arc::clone(&supervisor),
        received: Arc::clone(&received),
        dropped: Arc::clone(&dropped),
    });

    let receipt = engine
        .subscribe(QueryPlan::all(table), Arc::clone(&sink))
        .expect("subscribe");
    supervisor
        .lock()
        .expect("lock")
        .insert(receipt.handle().id());

    // Reset counters: the initial-snapshot delivery happens inside
    // `engine.subscribe(...)`, before we inserted the id into the
    // supervisor. For this test we observe only post-commit
    // deltas. (In real consumer code the supervisor inserts before
    // the subscribe call, but the test's ordering would require
    // restructuring; cleaner to reset and observe.)
    received.store(0, Ordering::SeqCst);
    dropped.store(0, Ordering::SeqCst);

    // First commit: supervisor is tracking this subscription.
    // Delta counts as received.
    engine
        .assert(Assertion::new(table, Thought::new("alpha", "first")))
        .expect("assert alpha");
    assert_eq!(received.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);

    // Supervisor "unsubscribes" by removing the id. The engine's
    // registry still tracks the subscription; the engine still
    // calls `sink.deliver(...)`; the sink discards by handle id.
    supervisor
        .lock()
        .expect("lock")
        .remove(&receipt.handle().id());

    engine
        .assert(Assertion::new(table, Thought::new("beta", "second")))
        .expect("assert beta");
    assert_eq!(received.load(Ordering::SeqCst), 1, "no new accepted delta");
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the engine still called deliver; sink dropped it",
    );

    // The engine still has the subscription registered durably.
    let registrations = engine.subscription_registrations().expect("registrations");
    assert_eq!(registrations.len(), 1, "engine-side registry growth");

    // Verdict: the supervisor pattern works for the correctness
    // dimension (no spurious deltas reach domain code) but does
    // not reclaim engine-side resources. For a daemon with stable
    // subscription set, no churn cost. For a daemon with high
    // subscription churn, the engine's registry grows. The gap
    // is real for churn workloads but small for steady-state.
    //
    // The bigger cost the supervisor pattern doesn't fix is the
    // detached-thread-per-delta in
    // `SubscriptionDeliveryMode::Detached` (subscribe.rs:415).
    // Switching to Inline mode (as this test does) is the right
    // move for actor-shaped sinks — and a stronger argument for
    // a future `unsubscribe` API than registry growth alone.
}

// ─── Gap 2 inspection: commit_multi ──────────────────────
//
// Claim under test (from audit §4.2): "cross-table atomic commits
// required raw storage-kernel writes, bypassing the engine's
// operation-tracking surface." (That write surface has since been
// removed; the engine hands out a read-only `StorageReader`.)
//
// This is a witness: what happens when a component tries to
// express cross-table atomicity today through only the typed
// engine surface?

#[test]
fn cross_table_writes_via_two_engine_commits_are_not_engine_atomic() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts table registers");
    let activities = engine
        .register_table(TableDescriptor::<ActivityEntry>::new(
            TableName::new("activities"),
            FamilyName::new("activity-entry"),
            SchemaHash::for_label("activity-entry-v1"),
        ))
        .expect("activities table registers");

    engine
        .assert(Assertion::new(
            thoughts,
            Thought::new("alpha", "the thought"),
        ))
        .expect("assert thought");
    let mid_log_len = engine.commit_log().expect("commit log").len();
    assert_eq!(mid_log_len, 1);

    engine
        .assert(Assertion::new(
            activities,
            ActivityEntry::new("a-1", "thought:alpha committed"),
        ))
        .expect("assert activity");
    let final_log_len = engine.commit_log().expect("commit log").len();
    assert_eq!(final_log_len, 2);

    // The two commits land as two separate `CommitLogEntry` values
    // at two snapshot IDs. A reader of the commit log sees them
    // as sequential, not atomic. If a peer subscribes to one
    // table, its delta arrives without coordination with the
    // other table's delta. If the second `assert` fails — disk
    // full, schema mismatch, panic — the first commit is durable
    // and the second is not.
    let log = engine.commit_log().expect("commit log");
    assert_eq!(log[0].snapshot(), SnapshotIdentifier::new(1));
    assert_eq!(log[1].snapshot(), SnapshotIdentifier::new(2));
    assert_eq!(log[0].operations().head().table_name(), "thoughts");
    assert_eq!(log[1].operations().head().table_name(), "activities");

    // Verdict: cross-table atomicity through the typed engine
    // surface is genuinely not expressible, and the old escape
    // hatch — a raw storage-kernel write that bypassed commit-log
    // emission, the snapshot bump, and subscription delivery — no
    // longer exists: `Engine::storage_reader()` has no write
    // affordance. A consumer that needs cross-table atomicity must
    // wait for the typed surface to grow it. Per the inelegance
    // criterion, the gap holds.
    //
    // The pressure is light today: no consumer pushes for
    // cross-table atomic writes. The workspace pattern
    // (one component per concern, single-writer-actor per
    // engine, one engine per component) localises writes per
    // table. The gap becomes load-bearing when one logical
    // operation spans tables — e.g. a `RoleHandoff` that
    // simultaneously retracts a claim from role A and asserts
    // it for role B, where the failure of either operation should
    // reject both.
    //
    // Resolution paths, in order of preference:
    //
    // 1. **Schema first (path 2 in revised audit §4).** If the
    //    operation can live in one table (e.g. claims as
    //    `Mutate` instead of `Retract+Assert`), atomicity is
    //    structural in the existing API. This is usually the
    //    cleanest answer.
    // 2. **Extend `Engine::commit` (path 1 in audit §4.2).**
    //    When the schema genuinely requires multiple tables,
    //    grow the typed surface to accept a multi-table commit
    //    request, preserving commit-log + snapshot + delta
    //    delivery semantics.
    // 3. **Document the constraint.** Consumers that hit it
    //    surface a contract-design issue; there is no raw-write
    //    path to reach for.
}

// ─── New observations from the falsification pass ────────

#[test]
fn read_after_write_is_two_engine_calls_with_monotonic_snapshot_identifiers() {
    // The wire's `Request<P>` allows mixing Assert + Match in
    // one bundle in principle (Subscribe is the only positional
    // constraint per `Request::check`). The engine doesn't
    // support mixed operations in one `CommitRequest`. What does the
    // composition look like through two calls?
    //
    // Two engine calls. The Match observes the same snapshot
    // ID the Assert produced. The single-writer-per-actor
    // discipline (per sema-engine ARCH §Constraints) means no
    // other writer can interleave between Assert and Match.
    // OCC is dissolved by actor discipline, not by an
    // engine-level snapshot pin.
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.thought_descriptor())
        .expect("table registers");

    let assert_receipt = engine
        .assert(Assertion::new(table, Thought::new("alpha", "after write")))
        .expect("assert");
    let read = engine
        .match_records(QueryPlan::key(table, RecordKey::new("alpha")))
        .expect("match");

    assert_eq!(read.snapshot(), assert_receipt.snapshot());
    assert_eq!(read.records(), &[Thought::new("alpha", "after write")]);

    // No gap. The audit's open question about mixed-operation
    // request shapes is dissolved: two calls suffice, the
    // monotonic snapshot resolves the freshness question, and
    // the single-writer discipline removes the race.
}

#[test]
fn engine_subscription_registrations_are_listable_for_introspection() {
    // While `unsubscribe` is missing, the engine exposes the
    // current registrations through
    // `subscription_registrations()`. A daemon supervisor can
    // list, count, and observe the state without growing the
    // API. The introspection surface for subscriptions is honest
    // if not yet pruneable.
    let fixture = Fixture::new();
    let mut engine = fixture.open_engine();
    let table = engine
        .register_table(fixture.thought_descriptor())
        .expect("table registers");

    assert_eq!(
        engine
            .subscription_registrations()
            .expect("registrations")
            .len(),
        0
    );

    let sink: Arc<dyn SubscriptionSink<Thought>> = Arc::new(SupervisorBackedSink {
        active: Arc::new(Mutex::new(HashSet::new())),
        received: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::new(AtomicUsize::new(0)),
    });
    let _ = engine
        .subscribe(QueryPlan::all(table), sink)
        .expect("subscribe");

    let registrations = engine.subscription_registrations().expect("registrations");
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].table_name(), "thoughts");
}
