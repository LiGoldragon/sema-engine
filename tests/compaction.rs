//! Checkpoint-backed version-history compaction preserves current state and
//! restart recovery while removing only an explicitly locally-acknowledged
//! replay prefix.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, VersionedHistoryAcknowledgement,
    VersionedHistoryRetention, VersionedStoreName, VersioningPolicy,
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
    let table = engine.register_table(fixture.table()).expect("table registers");
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
    assert!(engine
        .versioned_commit_log()
        .expect("versioned suffix reads")
        .is_empty());

    drop(engine);
    let mut reopened = fixture.open();
    let table = reopened.register_table(fixture.table()).expect("table registers");
    reopened
        .rebuild_from_log(&ThoughtDirectory { table })
        .expect("checkpoint rebuild succeeds after restart");
    let snapshot = reopened
        .match_records(QueryPlan::all(table))
        .expect("current view queries");
    assert_eq!(snapshot.records(), &[Thought::new("alpha", "current")]);
}

struct ThoughtDirectory {
    table: sema_engine::TableReference<Thought>,
}

impl sema_engine::FamilyDirectory for ThoughtDirectory {
    fn materialize(&self, row: sema_engine::RowMaterializer<'_>) -> sema_engine::Result<()> {
        row.apply(self.table)
    }
}
