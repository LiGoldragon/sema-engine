//! Witnesses for rebuild-from-log on storage-layout skew.
//!
//! Slice A bumped `STORAGE_LAYOUT` 4 -> 5, adding the persisted
//! chain-head digest slot and the two log-count counters as derived
//! state beside the versioned commit log. That bump was additive: it
//! left the data tables and the versioned-log format untouched. So a
//! layout-4 store that already opted into versioning carries a complete
//! versioned log from which the layout-5 derived slots are fully
//! recomputable. Opening such a store under the layout-5 engine refolds
//! those slots from the log — verified by recomputation, never trusting
//! a stored digest — and re-stamps the layout, instead of hard-failing
//! and taking the daemon down on deploy.
//!
//! A store at an older layout with no versioned log is a pre-versioning
//! store whose derived state is not log-recoverable; it keeps the typed
//! hard-fail. Re-opening an already-layout-5 store is a no-op.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, EntryDigest, FamilyDirectory, FamilyName,
    QueryPlan, RecordKey, RowMaterializer, SchemaHash, SchemaVersion, TableDescriptor, TableName,
    TableReference, VersionedStoreName, VersioningPolicy,
};
use tempfile::TempDir;

/// The engine-internal counters table, reconstructed by name so a test
/// can read and rewrite the layout slot and the derived counters
/// exactly as an old store's on-disk shape would have them.
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
/// The engine-internal chain-head slot, reconstructed by name so a test
/// can delete it to simulate a pre-layout-5 store.
const CHAIN_HEAD: sema::Table<&'static str, EntryDigest> =
    sema::Table::new("__sema_engine_chain_head");
const STORAGE_LAYOUT_KEY: &str = "engine_storage_layout";
const LATEST_ENTRY_DIGEST_KEY: &str = "latest_entry_digest";
const COMMIT_LOG_COUNT_KEY: &str = "commit_log_count";
const VERSIONED_LOG_COUNT_KEY: &str = "versioned_log_count";
const LAYOUT_FOUR: u64 = 4;
const CURRENT_LAYOUT: u64 = 7;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Note {
    key: String,
    body: String,
}

impl Note {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for Note {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

/// The component's typed knowledge of its one family, so a rebuild fold
/// can re-materialize notes into the notes table.
struct Notes {
    notes: TableReference<Note>,
}

impl Notes {
    fn new() -> Self {
        Self {
            notes: TableReference::new(TableName::new("notes")),
        }
    }
}

impl FamilyDirectory for Notes {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().family().as_str() {
            "note" => row.apply(self.notes),
            other => Err(sema_engine::Error::FamilyUnknown {
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

    fn open_versioned(&self, name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(name), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(name))),
        )
        .expect("versioned engine opens")
    }

    fn descriptor(&self) -> TableDescriptor<Note> {
        TableDescriptor::new(
            TableName::new("notes"),
            FamilyName::new("note"),
            SchemaHash::for_label("note-v1"),
        )
    }

    /// Open the kernel directly over a store's file — the affordance an
    /// old-store simulation needs to rewrite the layout slot and the
    /// derived slots that a pre-layout-5 store would not have carried.
    fn open_kernel(&self, name: &str) -> sema::Sema {
        let schema = sema::Schema {
            version: SchemaVersion::new(1),
        };
        sema::Sema::open_with_schema(&self.database_path(name), &schema).expect("kernel opens")
    }
}

/// HEADLINE WITNESS: a layout-4 store with a versioned log upgrades in
/// place at open. Write several entries (so the log and CHAIN_HEAD
/// populate under layout 5), then simulate an old store by deleting the
/// CHAIN_HEAD and the log-count counters and re-stamping the layout
/// counter back to 4. Re-opening SUCCEEDS: the layout re-stamps to 5,
/// the rebuilt CHAIN_HEAD equals a fresh fold's head, the data surface
/// is identical to before, and a subsequent assert chains correctly off
/// the rebuilt head with a clean fold.
#[test]
fn layout_four_store_with_versioned_log_rebuilds_derived_slots_on_open() {
    const ENTRY_COUNT: u64 = 7;
    let fixture = Fixture::new();
    let store = "rebuild-headline";

    // Populate a layout-5 store: log + CHAIN_HEAD + counts all written.
    let (records_before, head_before, sequence_before) = {
        let mut engine = fixture.open_versioned(store);
        let table = engine
            .register_table(fixture.descriptor())
            .expect("notes table registers");
        for index in 0..ENTRY_COUNT {
            engine
                .assert(Assertion::new(
                    table,
                    Note::new(format!("note-{index}"), format!("body {index}")),
                ))
                .expect("assert succeeds");
        }
        let head = engine
            .versioned_chain_head()
            .expect("chain head reads")
            .expect("a chain head exists after writes");
        let records = engine
            .match_records(QueryPlan::all(table))
            .expect("query reads")
            .records()
            .to_vec();
        let sequence = engine
            .current_commit_sequence()
            .expect("commit sequence reads");
        assert_eq!(records.len() as u64, ENTRY_COUNT);
        assert_eq!(sequence.value(), ENTRY_COUNT);
        (records, head, sequence)
    };

    // Simulate an old layout-4 store: delete the layout-5 derived slots
    // and re-stamp the layout counter back to 4. Use the kernel table
    // access available to a test in this crate; the data tables and the
    // versioned log stay untouched, exactly as slice A's additive bump
    // left a real layout-4 store.
    {
        let kernel = fixture.open_kernel(store);
        kernel
            .write(|transaction| {
                CHAIN_HEAD.remove(transaction, LATEST_ENTRY_DIGEST_KEY)?;
                COUNTERS.remove(transaction, COMMIT_LOG_COUNT_KEY)?;
                COUNTERS.remove(transaction, VERSIONED_LOG_COUNT_KEY)?;
                COUNTERS.insert(transaction, STORAGE_LAYOUT_KEY, &LAYOUT_FOUR)
            })
            .expect("old-store simulation writes");

        // Confirm the simulated old shape: layout 4, no chain head.
        let stamped = kernel
            .read(|transaction| COUNTERS.get(transaction, STORAGE_LAYOUT_KEY))
            .expect("layout slot reads");
        assert_eq!(stamped, Some(LAYOUT_FOUR));
        let head = kernel
            .read(|transaction| CHAIN_HEAD.get(transaction, LATEST_ENTRY_DIGEST_KEY))
            .expect("chain head reads");
        assert_eq!(head, None, "old store carries no chain-head slot");
    }

    // RE-OPEN: the layout-5 engine refolds the derived slots and the
    // open succeeds rather than hard-failing. redb is exclusive per
    // process, so no kernel handle is held while this engine lives;
    // the persisted re-stamp is verified after it drops.
    let mut reopened = fixture.open_versioned(store);
    let table = reopened
        .register_table(fixture.descriptor())
        .expect("notes table re-registers (idempotent)");

    // The rebuilt CHAIN_HEAD equals a fresh full fold's head: the last
    // versioned entry's own recomputed digest. The persisted slot must
    // equal the recomputation, never trust a stored value.
    let rebuilt_head = reopened
        .versioned_chain_head()
        .expect("rebuilt chain head reads")
        .expect("rebuilt chain head present");
    let log = reopened
        .versioned_commit_log()
        .expect("versioned log reads");
    let fresh_fold_head = log.last().expect("log is non-empty").entry_digest();
    assert_eq!(
        rebuilt_head, fresh_fold_head,
        "rebuilt CHAIN_HEAD equals a fresh fold's head",
    );
    assert_eq!(
        rebuilt_head, head_before,
        "rebuilt head matches the pre-skew head",
    );

    // A full rebuild_from_log folds the whole chain without a break:
    // every entry digest recomputed and verified link by link. A forked
    // or tampered chain would surface as VersionedChainBroken.
    let receipt = reopened
        .rebuild_from_log(&Notes::new())
        .expect("rebuild folds the whole chain cleanly");
    assert_eq!(receipt.folded_entry_count() as u64, ENTRY_COUNT);
    assert_eq!(receipt.row_count() as u64, ENTRY_COUNT);

    // The query/data surface is identical to before the skew: the data
    // tables were never touched, only the derived slots refolded.
    let records_after = reopened
        .match_records(QueryPlan::all(table))
        .expect("query reads")
        .records()
        .to_vec();
    assert_eq!(
        records_after, records_before,
        "data surface identical after rebuild",
    );

    // A subsequent assert chains correctly off the rebuilt head: a
    // strictly-increasing unique sequence and an entry whose predecessor
    // is the rebuilt head, folding cleanly.
    reopened
        .assert(Assertion::new(
            table,
            Note::new("note-after", "body after rebuild"),
        ))
        .expect("post-rebuild assert succeeds");
    let sequence_after = reopened
        .current_commit_sequence()
        .expect("commit sequence reads");
    assert_eq!(
        sequence_after.value(),
        sequence_before.value() + 1,
        "sequence increments off the rebuilt state",
    );
    let new_log = reopened
        .versioned_commit_log()
        .expect("versioned log reads");
    let new_entry = new_log.last().expect("a new entry exists");
    assert_eq!(
        new_entry.previous_entry_digest(),
        Some(rebuilt_head),
        "the new entry chains off the rebuilt head",
    );
    assert_eq!(
        new_entry.commit_sequence().value(),
        ENTRY_COUNT + 1,
        "the new entry takes the next contiguous sequence",
    );
    let final_receipt = reopened
        .rebuild_from_log(&Notes::new())
        .expect("post-assert rebuild folds cleanly");
    assert_eq!(final_receipt.folded_entry_count() as u64, ENTRY_COUNT + 1);

    // Drop the engine, then verify the open path durably re-stamped the
    // layout counter to the current layout.
    drop(reopened);
    let kernel = fixture.open_kernel(store);
    let stamped = kernel
        .read(|transaction| COUNTERS.get(transaction, STORAGE_LAYOUT_KEY))
        .expect("layout slot reads");
    assert_eq!(
        stamped,
        Some(CURRENT_LAYOUT),
        "layout re-stamped to current"
    );
}

/// A store at an older layout with NO versioned log still hard-fails
/// with the typed `StorageLayoutMismatch`: its layout-5 derived state is
/// not recoverable from a (nonexistent) log, so the previous-engine
/// migration owns it rather than an in-place refold.
#[test]
fn old_layout_store_without_versioned_log_still_hard_fails_typed() {
    let fixture = Fixture::new();
    let store = "no-log";

    // A layout-4 store that never opted into versioning: stamp the
    // layout slot and write a non-versioned engine counter, but never a
    // versioned log entry.
    {
        let kernel = fixture.open_kernel(store);
        kernel
            .write(|transaction| {
                COUNTERS.insert(transaction, "latest_commit_sequence", &3)?;
                COUNTERS.insert(transaction, STORAGE_LAYOUT_KEY, &LAYOUT_FOUR)
            })
            .expect("layout-4 no-log store writes");
    }

    let Err(error) = Engine::open(
        EngineOpen::new(fixture.database_path(store), SchemaVersion::new(1))
            .with_versioning(VersioningPolicy::new(VersionedStoreName::new(store))),
    ) else {
        panic!("layout-4 store with no versioned log must be rejected");
    };
    assert!(
        matches!(
            error,
            sema_engine::Error::StorageLayoutMismatch {
                stored: 4,
                expected: 7,
            }
        ),
        "got {error:?}",
    );
}

/// Re-opening an already-layout-5 store performs NO rebuild: it is a
/// pure no-op on the derived slots. The chain-head slot is byte-for-byte
/// unchanged across the close/reopen, and the commit sequence does not
/// move — proof the open path wrote nothing.
#[test]
fn reopening_a_layout_five_store_does_not_rebuild() {
    const ENTRY_COUNT: u64 = 5;
    let fixture = Fixture::new();
    let store = "idempotent";

    let head_before = {
        let mut engine = fixture.open_versioned(store);
        let table = engine
            .register_table(fixture.descriptor())
            .expect("notes table registers");
        for index in 0..ENTRY_COUNT {
            engine
                .assert(Assertion::new(
                    table,
                    Note::new(format!("note-{index}"), format!("body {index}")),
                ))
                .expect("assert succeeds");
        }
        engine
            .versioned_chain_head()
            .expect("chain head reads")
            .expect("a chain head exists")
    };

    // Capture the chain-head slot directly from the kernel before reopen.
    let slot_before = fixture
        .open_kernel(store)
        .read(|transaction| CHAIN_HEAD.get(transaction, LATEST_ENTRY_DIGEST_KEY))
        .expect("chain head reads")
        .expect("chain head slot present");
    assert_eq!(slot_before, head_before);

    // Re-open: layout already current, so the open path takes the
    // no-op branch and writes nothing.
    let reopened = fixture.open_versioned(store);
    let head_after = reopened
        .versioned_chain_head()
        .expect("chain head reads")
        .expect("a chain head exists after reopen");
    assert_eq!(
        head_after, head_before,
        "chain head unchanged across an idempotent reopen",
    );
    assert_eq!(
        reopened
            .current_commit_sequence()
            .expect("commit sequence reads")
            .value(),
        ENTRY_COUNT,
        "commit sequence did not move — no rebuild ran",
    );

    // Drop the engine (redb is exclusive per process) before reading
    // the persisted slot directly: it is byte-for-byte the same as
    // before the reopen, proving the open path wrote nothing.
    drop(reopened);
    let slot_after = fixture
        .open_kernel(store)
        .read(|transaction| CHAIN_HEAD.get(transaction, LATEST_ENTRY_DIGEST_KEY))
        .expect("chain head reads")
        .expect("chain head slot present after reopen");
    assert_eq!(
        slot_after, slot_before,
        "chain-head slot untouched by an idempotent reopen",
    );
}
