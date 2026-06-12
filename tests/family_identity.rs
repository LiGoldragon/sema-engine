//! Witnesses for typed family identity in the versioned commit log:
//! replay dispatches on (family, schema hash), the table name is only
//! the current coordinate, the store-level schema hash is derived
//! from the registered inventory, and pre-family-identity stores
//! hard-fail with a typed layout error.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan, RecordKey,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, VersionedReplay,
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
struct Activity {
    key: String,
    note: String,
}

impl Activity {
    fn new(key: impl Into<String>, note: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            note: note.into(),
        }
    }
}

impl EngineRecord for Activity {
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

    fn database_path(&self, name: &str) -> PathBuf {
        self.directory.path().join(format!("{name}.sema"))
    }

    fn open_versioned(&self, name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(name), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(name))),
        )
        .expect("versioned engine opens")
    }

    fn thought_family(&self) -> FamilyName {
        FamilyName::new("thought")
    }

    fn thought_schema_hash(&self) -> SchemaHash {
        SchemaHash::for_label("thought-v1")
    }

    fn thought_descriptor(&self, table: &'static str) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new(table),
            self.thought_family(),
            self.thought_schema_hash(),
        )
    }

    fn activity_descriptor(&self) -> TableDescriptor<Activity> {
        TableDescriptor::new(
            TableName::new("activities"),
            FamilyName::new("activity"),
            SchemaHash::for_label("activity-v1"),
        )
    }
}

#[test]
fn versioned_log_operations_carry_typed_family_identity() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("identity-carrier");
    let thoughts = engine
        .register_table(fixture.thought_descriptor("thoughts"))
        .expect("table registers");
    engine
        .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
        .expect("assert succeeds");

    let log = engine.versioned_commit_log().expect("versioned log reads");
    let operation = log[0].operations().head();
    let identity = operation.family();

    assert_eq!(identity.family(), &fixture.thought_family());
    assert_eq!(identity.schema_hash(), fixture.thought_schema_hash());
    assert_eq!(identity.table_name(), "thoughts");
    assert_eq!(log[0].schema_hash(), engine.store_schema_hash());
}

#[test]
fn versioned_replay_rebuilds_typed_state_dispatching_on_family_identity() {
    let fixture = Fixture::new();
    let entries = {
        let mut source = fixture.open_versioned("replay-source");
        let thoughts = source
            .register_table(fixture.thought_descriptor("thoughts"))
            .expect("thoughts register");
        let activities = source
            .register_table(fixture.activity_descriptor())
            .expect("activities register");

        source
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        source
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        source
            .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
            .expect("mutate alpha");
        source
            .retract(Retraction::new(thoughts, RecordKey::new("beta")))
            .expect("retract beta");
        source
            .assert(Assertion::new(activities, Activity::new("a-1", "noise")))
            .expect("assert activity");

        source.versioned_commit_log().expect("versioned log reads")
    };

    let mut target = fixture.open_versioned("replay-target");
    let thoughts = target
        .register_table(fixture.thought_descriptor("thoughts"))
        .expect("thoughts register in target");

    let receipt = target
        .replay_versioned(VersionedReplay::new(thoughts, entries))
        .expect("replay applies");
    let rebuilt = target
        .match_records(QueryPlan::all(thoughts))
        .expect("match succeeds");

    // Four thought operations applied; the activity-family operation
    // was dispatched away from this family, not silently absorbed.
    assert_eq!(receipt.applied(), 4);
    assert_eq!(receipt.skipped(), 1);
    assert_eq!(rebuilt.records(), &[Thought::new("alpha", "revised")]);
}

#[test]
fn table_rename_with_same_family_replays_into_current_table() {
    let fixture = Fixture::new();
    let entries = {
        let mut source = fixture.open_versioned("rename-source");
        let thoughts = source
            .register_table(fixture.thought_descriptor("thoughts_v1"))
            .expect("old-name table registers");
        source
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        source
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        source.versioned_commit_log().expect("versioned log reads")
    };
    assert_eq!(entries[0].operations().head().table_name(), "thoughts_v1");

    // Same family, same schema hash, new table coordinate.
    let mut target = fixture.open_versioned("rename-target");
    let renamed = target
        .register_table(fixture.thought_descriptor("thoughts_v2"))
        .expect("renamed table registers");

    let receipt = target
        .replay_versioned(VersionedReplay::new(renamed, entries))
        .expect("replay applies across the rename");
    let rebuilt = target
        .match_records(QueryPlan::all(renamed))
        .expect("match succeeds");

    assert_eq!(receipt.applied(), 2);
    assert_eq!(receipt.skipped(), 0);
    assert_eq!(
        rebuilt.records(),
        &[
            Thought::new("alpha", "first"),
            Thought::new("beta", "second")
        ]
    );
}

#[test]
fn store_schema_hash_derives_from_family_inventory_not_table_names() {
    let fixture = Fixture::new();

    let mut original = fixture.open_versioned("derived-hash-original");
    original
        .register_table(fixture.thought_descriptor("thoughts_v1"))
        .expect("table registers");

    // Same family inventory under a renamed table: identical store hash.
    let mut renamed = fixture.open_versioned("derived-hash-renamed");
    renamed
        .register_table(fixture.thought_descriptor("thoughts_v2"))
        .expect("renamed table registers");
    assert_eq!(original.store_schema_hash(), renamed.store_schema_hash());

    // A different family inventory: different store hash.
    let mut grown = fixture.open_versioned("derived-hash-grown");
    grown
        .register_table(fixture.thought_descriptor("thoughts_v1"))
        .expect("table registers");
    grown
        .register_table(fixture.activity_descriptor())
        .expect("second family registers");
    assert_ne!(original.store_schema_hash(), grown.store_schema_hash());
}

#[test]
fn reopened_engine_rejects_conflicting_family_identity_for_registered_table() {
    let fixture = Fixture::new();
    {
        let mut engine = fixture.open_versioned("identity-conflict");
        engine
            .register_table(fixture.thought_descriptor("thoughts"))
            .expect("table registers");
    }

    let mut reopened = fixture.open_versioned("identity-conflict");
    let error = reopened
        .register_table(TableDescriptor::<Thought>::new(
            TableName::new("thoughts"),
            fixture.thought_family(),
            SchemaHash::for_label("thought-v2"),
        ))
        .expect_err("conflicting schema hash is rejected");

    assert!(matches!(
        error,
        sema_engine::Error::FamilyIdentityMismatch { .. }
    ));
}

#[test]
fn family_version_binds_to_one_table_at_a_time() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("family-binding");
    engine
        .register_table(fixture.thought_descriptor("thoughts"))
        .expect("table registers");

    let error = engine
        .register_table(fixture.thought_descriptor("thoughts_shadow"))
        .expect_err("second binding of the same family version is rejected");

    assert!(matches!(
        error,
        sema_engine::Error::FamilyAlreadyBound { .. }
    ));
}

#[test]
fn pre_family_identity_store_hard_fails_with_typed_layout_error() {
    let fixture = Fixture::new();
    let path = fixture.database_path("legacy-layout");

    // Simulate a layout-1 store: engine counters exist, but the
    // layout slot (introduced with typed family identity) does not.
    {
        const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
        let schema = sema::Schema {
            version: SchemaVersion::new(1),
        };
        let storage = sema::Sema::open_with_schema(&path, &schema).expect("kernel opens");
        storage
            .write(|transaction| COUNTERS.insert(transaction, "latest_commit_sequence", &7))
            .expect("legacy counter writes");
    }

    let Err(error) = Engine::open(EngineOpen::new(path, SchemaVersion::new(1))) else {
        panic!("legacy store must be rejected");
    };

    assert!(matches!(
        error,
        sema_engine::Error::StorageLayoutMismatch {
            stored: 1,
            expected: 2
        }
    ));
}
