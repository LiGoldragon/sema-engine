//! Witnesses for the engine's internal write-serialization lock and
//! the persisted chain-head digest.
//!
//! The lock makes the engine single-writer under a shared `&Engine`:
//! many threads asserting through one `Arc<Engine>` still produce a
//! versioned log with strictly increasing, unique commit sequences and
//! an intact digest chain that `CanonicalView::fold` (reached through
//! `rebuild_from_log`) accepts. The persisted chain-head digest is an
//! O(1) optimization the write path uses to mint each entry's
//! predecessor; it is never an integrity source, so it must equal the
//! digest a fresh fold recomputes for the chain head.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitSequence, Engine, EngineOpen, EngineRecord, FamilyDirectory, FamilyName,
    RebuildReceipt, RecordKey, RowMaterializer, SchemaHash, SchemaVersion, TableDescriptor,
    TableName, TableReference, VersionedStoreName, VersioningPolicy,
};
use tempfile::TempDir;

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

/// The component's typed knowledge of its one family, for the rebuild
/// fold to re-materialize notes into the notes table.
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
        self.open_named(name, name)
    }

    /// Open a versioned engine whose on-disk file (`file`) is distinct
    /// from its versioned store name (`store`). A restore must reuse the
    /// source's store name in a fresh file, since a checkpoint carries
    /// its store name and import refuses a mismatched policy.
    fn open_named(&self, file: &str, store: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(file), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(store))),
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
}

/// The internal write lock serializes writers sharing one `Arc<Engine>`:
/// every thread's assert lands a distinct versioned entry, so the log
/// carries strictly increasing, contiguous, unique commit sequences and
/// the recomputed digest chain a `rebuild_from_log` fold verifies stays
/// intact. Without the lock, two threads could read the same chain head
/// and commit sequence and fork the chain or duplicate a sequence.
#[test]
fn arc_shared_engine_serializes_concurrent_writers() {
    const THREAD_COUNT: u64 = 8;
    const PER_THREAD: u64 = 16;

    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("concurrency");
    engine
        .register_table(fixture.descriptor())
        .expect("notes table registers");
    let engine = Arc::new(engine);

    let handles: Vec<_> = (0..THREAD_COUNT)
        .map(|thread_index| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for write_index in 0..PER_THREAD {
                    let table = TableReference::new(TableName::new("notes"));
                    let key = format!("note-{thread_index}-{write_index}");
                    engine
                        .assert(Assertion::new(
                            table,
                            Note::new(key, format!("body {thread_index}/{write_index}")),
                        ))
                        .expect("concurrent assert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("writer thread joins");
    }

    let total = THREAD_COUNT * PER_THREAD;
    let log = engine
        .versioned_commit_log()
        .expect("versioned commit log reads");
    assert_eq!(log.len() as u64, total, "every assert lands one entry");

    // Strictly increasing, unique, contiguous commit sequences — a
    // duplicate or a gap would prove two writers raced the sequence.
    let sequences: Vec<CommitSequence> =
        log.iter().map(|entry| entry.commit_sequence()).collect();
    for window in sequences.windows(2) {
        assert!(
            window[1].value() == window[0].value() + 1,
            "commit sequences are strictly increasing by one: {:?} then {:?}",
            window[0],
            window[1],
        );
    }
    assert_eq!(sequences.first().map(|sequence| sequence.value()), Some(1));
    assert_eq!(
        sequences.last().map(|sequence| sequence.value()),
        Some(total),
    );

    // The recomputed digest chain holds: rebuild folds every entry,
    // recomputing each digest from its fields and verifying the link to
    // its predecessor. A forked chain would surface as a typed
    // `VersionedChainBroken`, not a passing rebuild.
    let receipt: RebuildReceipt = engine
        .rebuild_from_log(&Notes::new())
        .expect("rebuild folds the whole chain without a break");
    assert_eq!(receipt.folded_entry_count() as u64, total);
    assert_eq!(
        receipt.row_count() as u64,
        total,
        "every key kept a distinct row",
    );
}

/// The persisted chain-head digest is an optimization, never an
/// integrity source: it must equal the digest a fresh fold recomputes
/// for the chain head. A `rebuild_from_log` runs `CanonicalView::fold`,
/// which recomputes every entry digest from its fields and verifies the
/// chain link by link; its success proves the stored chain-head digest
/// equals its own recomputation. A fresh store restored from a
/// checkpoint plus suffix recomputes the same head independently.
#[test]
fn cached_chain_head_equals_the_fresh_fold_recomputation() {
    let fixture = Fixture::new();
    let mut engine = fixture.open_versioned("cached-head");
    let table = engine
        .register_table(fixture.descriptor())
        .expect("notes table registers");

    // No versioned entry yet: the cached head is absent.
    assert_eq!(
        engine.versioned_chain_head().expect("chain head reads"),
        None,
    );

    // Early entries the checkpoint will fold into rows.
    for index in 0..8u64 {
        engine
            .assert(Assertion::new(
                table,
                Note::new(format!("early-{index}"), format!("body {index}")),
            ))
            .expect("early assert succeeds");
    }
    let checkpoint_receipt = engine.checkpoint().expect("checkpoint folds the log");
    assert_ne!(checkpoint_receipt.row_count(), 0);

    // Post-checkpoint entries form a real versioned-log suffix, so the
    // restored store reproduces a defined chain head rather than an
    // empty one.
    for index in 0..5u64 {
        engine
            .assert(Assertion::new(
                table,
                Note::new(format!("late-{index}"), format!("body {index}")),
            ))
            .expect("late assert succeeds");
    }

    let cached_head = engine
        .versioned_chain_head()
        .expect("chain head reads")
        .expect("a chain head exists after writes");
    let log = engine
        .versioned_commit_log()
        .expect("versioned commit log reads");
    let last_entry_digest = log.last().expect("log is non-empty").entry_digest();

    // The cached slot equals the most recent entry's own digest.
    assert_eq!(cached_head, last_entry_digest);

    // Independent recomputation: import into a fresh store folds the
    // checkpoint rows plus the suffix from scratch, recomputing every
    // entry digest from its fields and verifying the chain link by link.
    // A mismatch between a stored digest and its recomputation surfaces
    // as a typed `VersionedChainBroken`; a clean restore landing the
    // identical chain head proves the cached head equals the fresh
    // fold's recomputation for the chain head.
    let checkpoint = engine
        .latest_checkpoint()
        .expect("checkpoint reads")
        .expect("a checkpoint exists");
    let suffix = engine
        .versioned_replay_from_sequence(checkpoint.metadata().covered().last().next())
        .expect("suffix reads");
    assert_eq!(suffix.len(), 5, "the suffix carries the post-checkpoint writes");

    let mut restored = fixture.open_named("cached-head-restored", "cached-head");
    let mut session = restored.begin_import().expect("import session opens");
    session
        .ingest_checkpoint(checkpoint)
        .expect("checkpoint ingests");
    session.ingest_suffix(suffix);
    session
        .commit(&Notes::new())
        .expect("import folds checkpoint plus suffix");

    assert_eq!(
        restored
            .versioned_chain_head()
            .expect("restored chain head reads")
            .expect("restored chain head present"),
        cached_head,
        "a fresh fold-restored store recomputes the identical chain head",
    );
}
