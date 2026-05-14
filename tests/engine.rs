use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
};
use signal_core::SemaVerb;
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
        self.directory.path().join("engine.redb")
    }

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(self.database_path(), SchemaVersion::new(1)))
            .expect("engine opens")
    }

    fn toy_descriptor(&self) -> TableDescriptor<ToyRecord> {
        TableDescriptor::new(TableName::new("toy_records"))
    }
}

#[test]
fn engine_executes_assert_and_match_over_registered_record_family() {
    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let records = engine
        .register_table(fixture.toy_descriptor())
        .expect("table registers");
    let receipt = engine
        .assert(Assertion::new(
            records,
            ToyRecord::new("first", "stored through engine"),
        ))
        .expect("assert succeeds");

    assert_eq!(receipt.verb(), SemaVerb::Assert);
    assert_eq!(receipt.table().as_str(), "toy_records");

    let snapshot = engine
        .match_records(QueryPlan::all(records))
        .expect("match succeeds");

    assert_eq!(snapshot.verb(), SemaVerb::Match);
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
