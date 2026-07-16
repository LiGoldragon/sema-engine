//! Witnesses for the mirror outbox: every write choke point lands an
//! outbox row in the same transaction as its domain row, metadata
//! commit log entry, and versioned entry; the unshipped suffix is
//! exactly the unacknowledged range; acknowledgement is idempotent
//! and fork-safe; and durability levels are observable per entry and
//! per store.
//!
//! Crash discipline, the honest in-library form: the aborted-write
//! witness below proves a rejected transaction leaves no partial
//! outbox row (redb rolls the whole transaction back); the
//! interrupted-import witness lives in `tests/import.rs`. What is NOT
//! expressible in-library: a power loss between redb's fsync
//! boundaries — that guarantee belongs to redb's shadow paging, not
//! to this crate.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitRequest, CommitSequence, Durability, Engine, EngineOpen, EngineRecord,
    EntryDigest, FamilyName, IdentifiedAssertion, IdentifiedMutation, IdentifiedRetraction,
    IdentifiedTableDescriptor, MirrorAcknowledgement, MirrorHead, Mutation, QueryPlan, RecordKey,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, VersionedRecoveryTopology,
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
            EngineOpen::new(self.database_path(name), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new(name))
                    .with_recovery_topology(VersionedRecoveryTopology::Mirror),
            ),
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
fn every_write_choke_point_lands_an_outbox_row() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("choke-points");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    let tallies = engine
        .register_identified_table(fixture.tally_descriptor())
        .expect("tallies register");

    // Domain-keyed singles, identified variants, and a multi-op
    // commit — every public write choke point.
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert");
    engine
        .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
        .expect("mutate");
    engine
        .retract(Retraction::new(thoughts, RecordKey::new("alpha")))
        .expect("retract");
    let tally = engine
        .assert_identified(IdentifiedAssertion::new(tallies, Tally::new("one")))
        .expect("assert identified");
    engine
        .mutate_identified(IdentifiedMutation::new(
            tallies,
            tally.identifier(),
            Tally::new("revised"),
        ))
        .expect("mutate identified");
    engine
        .retract_identified(IdentifiedRetraction::new(tallies, tally.identifier()))
        .expect("retract identified");
    engine
        .commit(
            CommitRequest::new(thoughts)
                .assert(Thought::new("beta", "second"))
                .assert(Thought::new("gamma", "third")),
        )
        .expect("multi-operation commit");

    let log = engine.versioned_commit_log().expect("versioned log reads");
    let outbox = engine.unshipped_outbox().expect("outbox reads");
    assert_eq!(log.len(), 7);
    assert_eq!(outbox.len(), 7);
    for (entry, row) in log.iter().zip(&outbox) {
        assert_eq!(row.commit_sequence(), entry.commit_sequence());
        assert_eq!(row.entry_digest(), entry.entry_digest());
    }
}

#[test]
fn domain_row_commit_log_versioned_entry_and_outbox_row_commit_atomically() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("same-transaction");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");

    // Positive half: one successful write shows the same commit
    // sequence on all four surfaces.
    let receipt = engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert succeeds");
    let commit_log = engine.commit_log().expect("commit log reads");
    let versioned_log = engine.versioned_commit_log().expect("versioned log reads");
    let outbox = engine.unshipped_outbox().expect("outbox reads");
    assert_eq!(commit_log.len(), 1);
    assert_eq!(versioned_log.len(), 1);
    assert_eq!(outbox.len(), 1);
    assert_eq!(commit_log[0].commit_sequence(), receipt.commit_sequence());
    assert_eq!(
        versioned_log[0].commit_sequence(),
        receipt.commit_sequence()
    );
    assert_eq!(outbox[0].commit_sequence(), receipt.commit_sequence());
    assert_eq!(
        engine
            .match_records(QueryPlan::key(thoughts, RecordKey::new("alpha")))
            .expect("match succeeds")
            .records()
            .len(),
        1
    );

    // Negative half: a rejected bundle rolls back every surface — no
    // domain row, no commit log entry, no versioned entry, and no
    // partial outbox row survive the aborted transaction.
    let error = engine
        .commit(
            CommitRequest::new(thoughts)
                .assert(Thought::new("beta", "should not persist"))
                .assert(Thought::new("alpha", "duplicate")),
        )
        .expect_err("duplicate assert rejects the bundle");
    assert!(matches!(
        error,
        sema_engine::Error::DuplicateAssertKey { .. }
    ));
    assert_eq!(engine.commit_log().expect("commit log reads").len(), 1);
    assert_eq!(
        engine
            .versioned_commit_log()
            .expect("versioned log reads")
            .len(),
        1
    );
    assert_eq!(engine.unshipped_outbox().expect("outbox reads").len(), 1);
    assert!(
        engine
            .match_records(QueryPlan::key(thoughts, RecordKey::new("beta")))
            .expect("match succeeds")
            .records()
            .is_empty()
    );
}

#[test]
fn unshipped_suffix_is_exactly_the_unacknowledged_range() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("unshipped-range");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    for key in ["alpha", "beta", "gamma"] {
        engine
            .assert(Assertion::new(thoughts, Thought::new(key, "body")))
            .expect("assert succeeds");
    }

    let outbox = engine.unshipped_outbox().expect("outbox reads");
    assert_eq!(outbox.len(), 3);

    let acknowledgement = engine
        .acknowledge_mirror(MirrorHead::from(&outbox[1]))
        .expect("acknowledgement succeeds");
    assert!(matches!(
        acknowledgement,
        MirrorAcknowledgement::Advanced(_)
    ));
    assert_eq!(
        engine.mirror_head().expect("cursor reads"),
        Some(MirrorHead::from(&outbox[1]))
    );

    let unshipped = engine.unshipped_outbox().expect("outbox re-reads");
    assert_eq!(unshipped.len(), 1);
    assert_eq!(unshipped[0].commit_sequence(), CommitSequence::new(3));

    // Durability transitions are observable per entry and per store.
    assert_eq!(
        engine
            .durability_of(CommitSequence::new(1))
            .expect("durability reads"),
        Durability::ServerCommitted
    );
    assert_eq!(
        engine
            .durability_of(CommitSequence::new(2))
            .expect("durability reads"),
        Durability::ServerCommitted
    );
    assert_eq!(
        engine
            .durability_of(CommitSequence::new(3))
            .expect("durability reads"),
        Durability::QueuedForMirror
    );
    assert_eq!(
        engine.store_durability().expect("store durability reads"),
        Durability::QueuedForMirror
    );

    engine
        .acknowledge_mirror(MirrorHead::from(&outbox[2]))
        .expect("head acknowledgement succeeds");
    assert!(engine.unshipped_outbox().expect("outbox reads").is_empty());
    assert_eq!(
        engine.store_durability().expect("store durability reads"),
        Durability::ServerCommitted
    );
}

#[test]
fn duplicate_acknowledgement_is_a_typed_no_op() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("idempotent-acknowledge");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");
    engine
        .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
        .expect("assert beta");

    let outbox = engine.unshipped_outbox().expect("outbox reads");
    let head = MirrorHead::from(&outbox[1]);
    engine
        .acknowledge_mirror(head)
        .expect("first acknowledgement succeeds");

    // Same head again, and an older head: both no-ops returning the
    // standing cursor.
    let duplicate = engine
        .acknowledge_mirror(head)
        .expect("duplicate acknowledgement succeeds");
    assert_eq!(duplicate, MirrorAcknowledgement::Unchanged(head));
    let older = engine
        .acknowledge_mirror(MirrorHead::from(&outbox[0]))
        .expect("older acknowledgement succeeds");
    assert_eq!(older, MirrorAcknowledgement::Unchanged(head));
    assert_eq!(engine.mirror_head().expect("cursor reads"), Some(head));
}

#[test]
fn forked_and_unknown_heads_are_typed_errors() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("fork-detection");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");

    let forked = engine
        .acknowledge_mirror(MirrorHead::new(
            CommitSequence::new(1),
            EntryDigest::new([7; 32]),
        ))
        .expect_err("forked head is rejected");
    assert!(matches!(
        forked,
        sema_engine::Error::MirrorHeadForked { sequence: 1, .. }
    ));

    let unknown = engine
        .acknowledge_mirror(MirrorHead::new(
            CommitSequence::new(99),
            EntryDigest::new([7; 32]),
        ))
        .expect_err("unknown head is rejected");
    assert!(matches!(
        unknown,
        sema_engine::Error::MirrorHeadUnknown { sequence: 99 }
    ));
}

#[test]
fn unversioned_stores_never_queue_and_read_local_committed() {
    let fixture = Fixture::new();
    let mut engine = Engine::open(EngineOpen::new(
        fixture.database_path("unversioned"),
        SchemaVersion::new(1),
    ))
    .expect("engine opens");
    let thoughts = engine
        .register_table(fixture.thought_descriptor())
        .expect("thoughts register");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert alpha");

    assert!(engine.unshipped_outbox().expect("outbox reads").is_empty());
    assert_eq!(
        engine
            .durability_of(CommitSequence::new(1))
            .expect("durability reads"),
        Durability::LocalCommitted
    );
    assert_eq!(
        engine.store_durability().expect("store durability reads"),
        Durability::LocalCommitted
    );
    let unknown = engine
        .durability_of(CommitSequence::new(2))
        .expect_err("durability of an unwritten sequence is typed");
    assert!(matches!(
        unknown,
        sema_engine::Error::UnknownCommitSequence { sequence: 2 }
    ));
}
