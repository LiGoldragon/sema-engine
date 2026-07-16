//! Checkpoint-backed version-history compaction preserves current state and
//! restart recovery while removing only an explicitly locally-acknowledged
//! replay prefix.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CompactionFault, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName,
    VersionedHistoryAcknowledgement, VersionedHistoryRetention, VersionedRecoveryTopology,
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

struct Fixture {
    directory: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("temp directory"),
        }
    }

    fn path(&self) -> PathBuf {
        self.directory.path().join("history.sema")
    }

    fn open(&self) -> Engine {
        Engine::open(
            EngineOpen::new(self.path(), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new("history"))),
        )
        .expect("versioned engine opens")
    }

    fn open_mirrored(&self) -> Engine {
        Engine::open(
            EngineOpen::new(self.path(), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("history"))
                    .with_recovery_topology(VersionedRecoveryTopology::Mirror),
            ),
        )
        .expect("mirrored engine opens")
    }

    fn open_zero_retention(&self) -> Engine {
        Engine::open(
            EngineOpen::new(self.path(), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("history"))
                    .with_retention(VersionedHistoryRetention::new(0)),
            ),
        )
        .expect("zero-retention engine opens")
    }

    fn table(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }
}

#[test]
fn local_checkpoint_compaction_bounds_raw_history_and_preserves_restart_view() {
    let fixture = Fixture::new();
    let mut engine = fixture.open();
    let table = engine
        .register_table(fixture.table())
        .expect("table registers");
    engine
        .assert(Assertion::new(table, Thought::new("alpha", "first")))
        .expect("initial write");
    engine
        .mutate(Mutation::new(table, Thought::new("alpha", "current")))
        .expect("current write");

    let compacted = engine
        .compact_versioned_history(
            VersionedHistoryRetention::new(0),
            VersionedHistoryAcknowledgement::LocalCheckpoint,
        )
        .expect("verified local checkpoint compacts history");
    assert_eq!(compacted.compacted_entries(), 2);
    assert_eq!(compacted.retained_entries(), 0);
    assert!(
        engine
            .versioned_commit_log()
            .expect("versioned suffix reads")
            .is_empty()
    );
    engine
        .mutate(Mutation::new(table, Thought::new("alpha", "newest")))
        .expect("post-compaction write");
    engine
        .compact_versioned_history(
            VersionedHistoryRetention::new(0),
            VersionedHistoryAcknowledgement::LocalCheckpoint,
        )
        .expect("subsequent checkpoint replaces its compacted predecessor");
    assert_eq!(
        engine
            .latest_checkpoint()
            .expect("latest checkpoint reads")
            .expect("checkpoint exists")
            .metadata()
            .previous_checkpoint_digest(),
        None,
        "compaction retains one root checkpoint rather than accumulating a chain"
    );

    drop(engine);
    let mut reopened = fixture.open();
    let table = reopened
        .register_table(fixture.table())
        .expect("table registers");
    reopened
        .rebuild_from_log(&ThoughtDirectory { table })
        .expect("checkpoint rebuild succeeds after restart");
    let snapshot = reopened
        .match_records(QueryPlan::all(table))
        .expect("current view queries");
    assert_eq!(snapshot.records(), &[Thought::new("alpha", "newest")]);
}

#[test]
fn local_checkpoint_acknowledgement_is_rejected_for_mirrored_store_without_mutation() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_mirrored();
    let table = engine
        .register_table(fixture.table())
        .expect("table registers");
    engine
        .assert(Assertion::new(table, Thought::new("alpha", "first")))
        .expect("write queues mirror row");

    let error = engine
        .compact_versioned_history(
            VersionedHistoryRetention::new(0),
            VersionedHistoryAcknowledgement::LocalCheckpoint,
        )
        .expect_err("a mirror topology cannot claim local-checkpoint recovery");
    assert!(matches!(
        error,
        sema_engine::Error::HistoryCompactionTopologyMismatch { .. }
    ));
    assert_eq!(
        engine.versioned_commit_log().expect("log reads").len(),
        1,
        "rejected compaction leaves replay history intact"
    );
    assert_eq!(
        engine.unshipped_outbox().expect("outbox reads").len(),
        1,
        "rejected compaction never deletes unacknowledged mirror replay"
    );
    assert!(
        engine
            .latest_checkpoint()
            .expect("checkpoint reads")
            .is_none(),
        "topology validation happens before checkpoint mutation"
    );
}

#[test]
fn configured_finite_policy_compacts_at_lifecycle_boundary() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_zero_retention();
    let table = engine
        .register_table(fixture.table())
        .expect("table registers");
    engine
        .assert(Assertion::new(table, Thought::new("alpha", "first")))
        .expect("write succeeds");

    let compacted = engine
        .compact_configured_versioned_history()
        .expect("configured local retention compacts");
    assert_eq!(
        compacted.compacted_entries(),
        0,
        "normal writes already enforce the finite policy"
    );
    assert!(
        engine.versioned_commit_log().expect("log reads").is_empty(),
        "the finite configured budget is enforced at the lifecycle boundary"
    );
}

#[test]
fn every_durable_compaction_phase_recovers_retraction_heavy_history_after_restart() {
    for fault in [
        CompactionFault::AfterPlanPersisted,
        CompactionFault::AfterRetractionsApplied,
        CompactionFault::AfterCheckpointPublished,
        CompactionFault::AfterHistoryFloorAdvanced,
    ] {
        let fixture = Fixture::new();
        let mut engine = fixture.open_zero_retention();
        let table = engine
            .register_table(fixture.table())
            .expect("table registers");
        for sequence in 0..12 {
            engine
                .assert(Assertion::new(
                    table,
                    Thought::new(format!("thought-{sequence}"), "retained or retired"),
                ))
                .expect("history write");
        }
        engine.begin_compaction().expect("compaction begins");
        for sequence in 0..9 {
            engine
                .retract(Retraction::new(
                    table,
                    RecordKey::new(format!("thought-{sequence}")),
                ))
                .expect("retraction enters complete staged plan");
        }
        if fault == CompactionFault::AfterPlanPersisted {
            engine.inject_compaction_fault(fault);
            assert!(
                engine.park_compaction().is_err(),
                "fault interrupts after plan"
            );
        } else {
            assert!(engine.park_compaction().expect("plan persists"));
            engine.inject_compaction_fault(fault);
            assert!(
                engine
                    .resume_compaction(&ThoughtDirectory { table })
                    .is_err(),
                "fault interrupts after durable phase"
            );
        }
        drop(engine);

        let mut reopened = Engine::open_recovering(
            EngineOpen::new(fixture.path(), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("history"))
                    .with_retention(VersionedHistoryRetention::new(0)),
            ),
            &ThoughtDirectory {
                table: sema_engine::TableReference::new(TableName::new("thoughts")),
            },
        )
        .expect("supervised recovery resolves the intent before serving");
        let table = reopened
            .register_table(fixture.table())
            .expect("table registers after restart");
        reopened
            .resume_compaction(&ThoughtDirectory { table })
            .expect("restart resolves every durable phase before use");
        assert!(
            reopened
                .compaction_intent()
                .expect("intent reads")
                .is_none(),
            "intent clears only after history floor is consistent"
        );
        let rows = reopened
            .match_records(QueryPlan::all(table))
            .expect("view reads after recovery");
        assert_eq!(
            rows.records().len(),
            3,
            "all planned rows retract exactly once"
        );
        assert!(
            reopened
                .versioned_commit_log()
                .expect("history reads")
                .is_empty(),
            "zero policy advances the floor only after verified checkpoint"
        );
        assert!(
            reopened
                .unshipped_outbox()
                .expect("local outbox reads")
                .is_empty(),
            "local topology does not retain a mirror outbox"
        );
    }
}

struct ThoughtDirectory {
    table: sema_engine::TableReference<Thought>,
}

impl sema_engine::FamilyDirectory for ThoughtDirectory {
    fn materialize(&self, row: sema_engine::RowMaterializer<'_>) -> sema_engine::Result<()> {
        row.apply(self.table)
    }
}
