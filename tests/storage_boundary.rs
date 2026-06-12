//! Architectural truth witnesses for the storage boundary: the public
//! engine API exposes no durable-write path that bypasses the commit
//! log. The narrowed kernel handoff type, [`sema_engine::StorageReader`],
//! simply has no write affordance — the type system is the witness;
//! these tests pin the source surface so the affordance cannot
//! silently return.

use std::fs;
use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, RecordKey, SchemaHash, SchemaVersion,
    StorageKernelTable, TableDescriptor, TableName,
};
use tempfile::TempDir;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Thought {
    key: String,
    body: String,
}

impl EngineRecord for Thought {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

struct SourceFixture {
    root: PathBuf,
}

impl SourceFixture {
    fn current() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn source(&self, path: &str) -> String {
        fs::read_to_string(self.root.join(path)).expect("source file is readable")
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

    fn open_engine(&self) -> Engine {
        Engine::open(EngineOpen::new(
            self.directory.path().join("engine.sema"),
            SchemaVersion::new(1),
        ))
        .expect("engine opens")
    }
}

#[test]
fn engine_hands_out_no_raw_storage_kernel_or_write_transaction() {
    let fixture = SourceFixture::current();
    let engine_source = fixture.source("src/engine.rs");
    let lib_source = fixture.source("src/lib.rs");

    assert!(
        !engine_source.contains("pub fn storage_kernel"),
        "Engine must not hand out the raw storage kernel"
    );
    assert!(
        !engine_source.contains("-> &sema::Sema"),
        "no public method may return the raw kernel handle"
    );
    assert!(
        !lib_source.contains("Sema as StorageKernel,")
            && !lib_source.contains("Sema as StorageKernel}"),
        "lib must not re-export the raw kernel handle type"
    );
    assert!(
        !lib_source.contains("WriteTransaction"),
        "lib must not re-export a storage write transaction type"
    );
}

#[test]
fn storage_reader_has_a_read_affordance_and_no_write_affordance() {
    let fixture = SourceFixture::current();
    let engine_source = fixture.source("src/engine.rs");

    let reader_definition = engine_source
        .split("pub struct StorageReader")
        .nth(1)
        .expect("StorageReader is defined in engine.rs");
    assert!(
        reader_definition.contains("pub fn read"),
        "StorageReader must expose transitional read access"
    );
    assert!(
        !reader_definition.contains("pub fn write"),
        "StorageReader must not expose a write transaction"
    );
}

#[test]
fn storage_reader_observes_state_written_through_logged_choke_points() {
    const COUNTERS: StorageKernelTable<&'static str, u64> =
        StorageKernelTable::new("__sema_engine_counters");

    let fixture = EngineFixture::new();
    let mut engine = fixture.open_engine();
    let thoughts = engine
        .register_table(TableDescriptor::<Thought>::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        ))
        .expect("table registers");
    engine
        .assert(Assertion::new(
            thoughts,
            Thought {
                key: "alpha".into(),
                body: "first".into(),
            },
        ))
        .expect("assert succeeds");

    let observed = engine
        .storage_reader()
        .read(|transaction| COUNTERS.get(transaction, "latest_commit_sequence"))
        .expect("reader reads");

    assert_eq!(observed, Some(1));
}
