//! Tamper witnesses: the fold surface's integrity errors, proven by
//! doctoring real stores and artifacts. Forged log rows break the
//! digest chain at their successor; rewritten payloads fail per-entry
//! digest recomputation; doctored checkpoint metadata, forged
//! segments, and fixed-up outer digests are rejected at load, at
//! ingest, and at verify; a dead latest-checkpoint cursor and a
//! dangling segment reference are typed at load before verification
//! runs; a stamped store schema hash must re-derive
//! from its carried inventory; and an outbox row that disagrees with
//! the logged entry refuses acknowledgement. Doctoring goes through
//! the raw storage kernel (no engine path can write engine tables) or
//! through byte splices — the in-memory shape of on-disk tampering.

use std::path::PathBuf;

use rkyv::rancor;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Checkpoint, CheckpointMetadata, CheckpointSegment, Engine, EngineOpen, EngineRecord,
    FamilyDirectory, FamilyName, MirrorHead, OutboxEntry, RecordKey, RowMaterializer, SchemaHash,
    SchemaVersion, SegmentDigest, TableDescriptor, TableName, TableReference,
    VersionedCommitLogEntry, VersionedLogOperation, VersionedPayload, VersionedStoreName,
    VersioningPolicy, ViewRow,
};
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;
use tempfile::TempDir;

/// The engine's internal tables, redeclared for doctoring. The engine
/// never exposes a write path to them — `StorageReader` is read-only —
/// so the tests reach them through the raw storage kernel.
const VERSIONED_COMMIT_LOG: sema::Table<u64, VersionedCommitLogEntry> =
    sema::Table::new("__sema_engine_versioned_commit_log");
const CHECKPOINTS: sema::Table<u64, CheckpointMetadata> =
    sema::Table::new("__sema_engine_checkpoints");
const CHECKPOINT_SEGMENTS: sema::Table<&'static [u8; 32], CheckpointSegment> =
    sema::Table::new("__sema_engine_checkpoint_segments");
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
const LATEST_CHECKPOINT_KEY: &str = "latest_checkpoint";

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

struct Families {
    thoughts: TableReference<Thought>,
}

impl Families {
    fn new() -> Self {
        Self {
            thoughts: TableReference::new(TableName::new("thoughts")),
        }
    }
}

impl FamilyDirectory for Families {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().family().as_str() {
            "thought" => row.apply(self.thoughts),
            other => Err(sema_engine::Error::FamilyUnknown {
                family: other.to_owned(),
            }),
        }
    }
}

/// Byte-splice tamper tool: serialize a value, replace one byte run
/// with an equal-length replacement, decode the doctored bytes back.
/// Every untouched field survives verbatim — exactly what on-disk
/// tampering looks like to the verifying reader.
struct Splice {
    find: Vec<u8>,
    replace: Vec<u8>,
}

impl Splice {
    fn new(find: &[u8], replace: &[u8]) -> Self {
        assert_eq!(find.len(), replace.len(), "splice must preserve layout");
        Self {
            find: find.to_vec(),
            replace: replace.to_vec(),
        }
    }

    /// Splice that flips one bit inside a 32-byte digest field.
    fn flipping(target: &[u8; 32]) -> Self {
        let mut replacement = *target;
        replacement[0] ^= 0x01;
        Self::new(target, &replacement)
    }

    fn entry(&self, entry: &VersionedCommitLogEntry) -> VersionedCommitLogEntry {
        let bytes = rkyv::to_bytes::<rancor::Error>(entry).expect("entry serializes");
        rkyv::from_bytes::<VersionedCommitLogEntry, rancor::Error>(&self.bytes(&bytes))
            .expect("doctored entry decodes")
    }

    fn metadata(&self, metadata: &CheckpointMetadata) -> CheckpointMetadata {
        let bytes = rkyv::to_bytes::<rancor::Error>(metadata).expect("metadata serializes");
        rkyv::from_bytes::<CheckpointMetadata, rancor::Error>(&self.bytes(&bytes))
            .expect("doctored metadata decodes")
    }

    fn bytes(&self, bytes: &[u8]) -> Vec<u8> {
        let position = bytes
            .windows(self.find.len())
            .position(|window| window == self.find)
            .expect("target bytes are present");
        let mut doctored = bytes.to_vec();
        doctored[position..position + self.replace.len()].copy_from_slice(&self.replace);
        doctored
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
        self.open_store(name, name)
    }

    fn open_store(&self, file: &str, store_name: &str) -> Engine {
        Engine::open(
            EngineOpen::new(self.database_path(file), SchemaVersion::new(1))
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(store_name))),
        )
        .expect("versioned engine opens")
    }

    fn descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }

    /// Open the `.sema` file through the raw storage kernel — the
    /// doctoring surface. The engine handle must be dropped first;
    /// redb holds an exclusive file lock.
    fn raw_kernel(&self, name: &str) -> sema::Sema {
        sema::Sema::open_with_schema(
            &self.database_path(name),
            &sema::Schema {
                version: SchemaVersion::new(1),
            },
        )
        .expect("raw kernel opens")
    }

    /// A store holding one checkpoint over one asserted record,
    /// returning the verified checkpoint artifact.
    fn checkpointed(&self, name: &str) -> Checkpoint {
        let mut engine = self.open_versioned(name);
        let thoughts = engine
            .register_table(self.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        engine.checkpoint().expect("checkpoint writes");
        engine
            .latest_checkpoint()
            .expect("checkpoint loads")
            .expect("checkpoint exists")
    }
}

/// A self-consistent replacement for a logged entry: same chain
/// coordinates and key, different record payload, freshly computed
/// digest. The forgery is internally valid — only its relation to the
/// rest of the store can betray it.
fn forged_replacement(genuine: &VersionedCommitLogEntry, body: &str) -> VersionedCommitLogEntry {
    let operation = genuine.operations().head();
    let key = operation.key().expect("logged operation is keyed").clone();
    let payload = rkyv::to_bytes::<rancor::Error>(&Thought::new(key.to_owned_string(), body))
        .expect("forged payload serializes");
    VersionedCommitLogEntry::new(
        genuine.store_name().clone(),
        genuine.schema_hash(),
        genuine.commit_sequence(),
        genuine.snapshot(),
        genuine.previous_entry_digest(),
        NonEmpty::single(VersionedLogOperation::new(
            SemaOperation::Assert,
            operation.family().clone(),
            Some(key),
            VersionedPayload::record(payload.as_slice().to_vec()),
        )),
    )
}

#[test]
fn forged_log_row_breaks_the_digest_chain_at_its_successor() {
    let fixture = Fixture::new();
    let genuine_first = {
        let mut engine = fixture.open_versioned("forged-row");
        let thoughts = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        engine.versioned_commit_log().expect("log reads")[0].clone()
    };

    // Replace the first logged entry wholesale with an internally
    // valid forgery through the raw kernel.
    let forged = forged_replacement(&genuine_first, "forged");
    assert_ne!(forged.entry_digest(), genuine_first.entry_digest());
    fixture
        .raw_kernel("forged-row")
        .write(|transaction| {
            VERSIONED_COMMIT_LOG.insert(
                transaction,
                genuine_first.commit_sequence().value(),
                &forged,
            )
        })
        .expect("forged row writes");

    // The successor's predecessor link still names the genuine
    // digest, so every fold surface detects the rewrite at sequence 2.
    let engine = fixture.open_versioned("forged-row");
    let error = engine
        .rebuild_from_log(&Families::new())
        .expect_err("rebuild detects the forgery");
    assert!(matches!(
        error,
        sema_engine::Error::VersionedChainBroken { sequence: 2 }
    ));
    let error = engine
        .checkpoint()
        .expect_err("checkpoint detects the forgery");
    assert!(matches!(
        error,
        sema_engine::Error::VersionedChainBroken { sequence: 2 }
    ));
}

#[test]
fn rewritten_payload_fails_per_entry_digest_recomputation() {
    let fixture = Fixture::new();
    let genuine = {
        let mut engine = fixture.open_versioned("rewritten-payload");
        let thoughts = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        engine.versioned_commit_log().expect("log reads")[0].clone()
    };

    // Rewrite the logged record bytes while keeping the stored entry
    // digest: the tampered entry still names the digest of the
    // history it no longer carries.
    let tampered = Splice::new(b"first", b"f1rst").entry(&genuine);
    assert_eq!(tampered.entry_digest(), genuine.entry_digest());
    fixture
        .raw_kernel("rewritten-payload")
        .write(|transaction| {
            VERSIONED_COMMIT_LOG.insert(transaction, genuine.commit_sequence().value(), &tampered)
        })
        .expect("tampered row writes");

    // The fold recomputes every entry digest from the entry's own
    // fields: the stored digest cannot ride the chain.
    let engine = fixture.open_versioned("rewritten-payload");
    let error = engine
        .rebuild_from_log(&Families::new())
        .expect_err("rebuild recomputes every digest");
    assert!(matches!(
        error,
        sema_engine::Error::VersionedChainBroken { sequence: 1 }
    ));
}

#[test]
fn import_rejects_a_suffix_entry_whose_digest_does_not_recompute() {
    let fixture = Fixture::new();
    let (checkpoint, suffix) = {
        let mut source = fixture.open_versioned("tamper-source");
        let thoughts = source
            .register_table(fixture.descriptor())
            .expect("table registers");
        source
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        source.checkpoint().expect("checkpoint writes");
        source
            .assert(Assertion::new(thoughts, Thought::new("gamma", "third")))
            .expect("assert gamma");
        let checkpoint = source
            .latest_checkpoint()
            .expect("checkpoint loads")
            .expect("checkpoint exists");
        let suffix = source
            .versioned_replay_from_sequence(checkpoint.metadata().covered().last().next())
            .expect("suffix reads");
        (checkpoint, suffix)
    };
    assert_eq!(suffix.len(), 1);

    let tampered = vec![Splice::new(b"third", b"th1rd").entry(&suffix[0])];

    let mut target = fixture.open_store("tamper-target", "tamper-source");
    let mut session = target.begin_import().expect("import session mints");
    session
        .ingest_checkpoint(checkpoint.clone())
        .expect("genuine checkpoint ingests");
    session.ingest_suffix(tampered);
    let error = session
        .commit(&Families::new())
        .expect_err("tampered suffix is rejected");
    assert!(matches!(
        error,
        sema_engine::Error::VersionedChainBroken { sequence: 2 }
    ));

    // Nothing was imported: the store is still fresh, so the genuine
    // suffix imports cleanly on retry.
    assert!(target.versioned_commit_log().expect("log reads").is_empty());
    let mut retry = target.begin_import().expect("store is still fresh");
    retry
        .ingest_checkpoint(checkpoint)
        .expect("checkpoint re-ingests");
    retry.ingest_suffix(suffix);
    retry
        .commit(&Families::new())
        .expect("genuine suffix imports");
}

#[test]
fn doctored_checkpoint_metadata_is_rejected_at_load_and_at_ingest() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("doctored-metadata");
    let metadata = genuine.metadata();

    // Flip one bit of the stored checkpoint digest in the persisted
    // metadata row.
    let doctored = Splice::flipping(metadata.checkpoint_digest().bytes()).metadata(metadata);
    fixture
        .raw_kernel("doctored-metadata")
        .write(|transaction| {
            CHECKPOINTS.insert(transaction, metadata.sequence().value(), &doctored)
        })
        .expect("doctored metadata writes");

    let engine = fixture.open_versioned("doctored-metadata");
    let error = engine
        .latest_checkpoint()
        .expect_err("load verifies the checkpoint digest");
    let sema_engine::Error::CheckpointDigestMismatch { stored, computed } = error else {
        panic!("expected CheckpointDigestMismatch, got {error:?}");
    };
    assert_eq!(computed, metadata.checkpoint_digest());
    assert_ne!(stored, computed);

    // Import ingestion rejects the same doctored artifact.
    let mut target = fixture.open_store("doctored-target", "doctored-metadata");
    let mut session = target.begin_import().expect("import session mints");
    let error = session
        .ingest_checkpoint(Checkpoint::new(doctored, genuine.segments().to_vec()))
        .expect_err("ingest verifies the checkpoint digest");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointDigestMismatch { .. }
    ));
}

#[test]
fn dead_latest_checkpoint_cursor_is_a_typed_missing_row() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("dead-cursor");
    let dead = genuine.metadata().sequence().value() + 7;

    // Point the latest-checkpoint cursor at a sequence no checkpoint
    // row was ever written under.
    fixture
        .raw_kernel("dead-cursor")
        .write(|transaction| COUNTERS.insert(transaction, LATEST_CHECKPOINT_KEY, &dead))
        .expect("dead cursor writes");

    let engine = fixture.open_versioned("dead-cursor");
    let error = engine
        .latest_checkpoint()
        .expect_err("the cursor names a row that does not exist");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointRowMissing { sequence } if sequence == dead
    ));
}

#[test]
fn forged_segment_under_a_genuine_address_is_rejected_at_load() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("forged-segment");
    let reference = genuine.metadata().segments()[0];
    let rows = genuine.segments()[0].rows().to_vec();

    // Forge a segment whose first row carries a different payload and
    // store it under the genuine content address.
    let mut altered = rows.clone();
    altered[0] = ViewRow::new(
        rows[0].family().clone(),
        rows[0].key().clone(),
        VersionedPayload::record(b"forged".to_vec()),
    );
    let forged = CheckpointSegment::new(altered);
    assert_ne!(forged.digest(), reference.digest());
    let address = reference.digest();
    fixture
        .raw_kernel("forged-segment")
        .write(|transaction| CHECKPOINT_SEGMENTS.insert(transaction, address.bytes(), &forged))
        .expect("forged segment writes");

    let engine = fixture.open_versioned("forged-segment");
    let error = engine
        .latest_checkpoint()
        .expect_err("load verifies every content address");
    let sema_engine::Error::SegmentDigestMismatch {
        referenced,
        computed,
    } = error
    else {
        panic!("expected SegmentDigestMismatch, got {error:?}");
    };
    assert_eq!(referenced, reference.digest());
    assert_eq!(computed, forged.digest());
}

#[test]
fn flipped_segment_reference_dangles_at_load_before_verification_runs() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("dangling-reference");
    let metadata = genuine.metadata();
    let reference = metadata.segments()[0];
    let mut flipped = *reference.digest().bytes();
    flipped[0] ^= 0x01;
    let flipped = SegmentDigest::new(flipped);

    // Flip one bit of the stored segment-reference digest: the
    // metadata row now names a content address no segment lives under.
    let doctored = Splice::flipping(reference.digest().bytes()).metadata(metadata);
    fixture
        .raw_kernel("dangling-reference")
        .write(|transaction| {
            CHECKPOINTS.insert(transaction, metadata.sequence().value(), &doctored)
        })
        .expect("doctored metadata writes");

    // The segment fetch runs before checkpoint verification, so the
    // dangling reference fires first — carrying the flipped digest,
    // a real in-bounds address, never a fabricated zero.
    let engine = fixture.open_versioned("dangling-reference");
    let error = engine
        .latest_checkpoint()
        .expect_err("the flipped reference dangles");
    let sema_engine::Error::SegmentMissing { digest } = error else {
        panic!("expected SegmentMissing, got {error:?}");
    };
    assert_eq!(digest, flipped);
    assert_ne!(digest, reference.digest());
    assert_ne!(digest, SegmentDigest::new([0; 32]));
}

#[test]
fn fixed_up_outer_digest_still_fails_on_the_view_digest() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("lying-view");
    let metadata = genuine.metadata();

    // First pass: a flipped view digest alone is caught by the outer
    // checkpoint digest.
    let lying = Splice::flipping(metadata.view_digest().bytes()).metadata(metadata);
    let error = Checkpoint::new(lying.clone(), genuine.segments().to_vec())
        .verify()
        .expect_err("outer digest catches the raw lie");
    let sema_engine::Error::CheckpointDigestMismatch { computed, .. } = error else {
        panic!("expected CheckpointDigestMismatch, got {error:?}");
    };

    // Second pass: the forger recomputes the outer digest over the
    // lying fields. The fold of the segment rows still exposes the
    // view digest as a lie.
    let consistent =
        Splice::new(lying.checkpoint_digest().bytes(), computed.bytes()).metadata(&lying);
    let error = Checkpoint::new(consistent.clone(), genuine.segments().to_vec())
        .verify()
        .expect_err("view digest catches the fixed-up forgery");
    let sema_engine::Error::ViewDigestMismatch { expected, computed } = error else {
        panic!("expected ViewDigestMismatch, got {error:?}");
    };
    assert_eq!(expected, consistent.view_digest());
    assert_eq!(computed, metadata.view_digest());

    // Import ingestion rejects the forgery the same way.
    let mut target = fixture.open_store("lying-view-target", "lying-view");
    let mut session = target.begin_import().expect("import session mints");
    let error = session
        .ingest_checkpoint(Checkpoint::new(consistent, genuine.segments().to_vec()))
        .expect_err("ingest rejects the forgery");
    assert!(matches!(
        error,
        sema_engine::Error::ViewDigestMismatch { .. }
    ));
}

#[test]
fn segment_count_mismatch_is_typed_in_both_directions() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("segment-count");
    let metadata = genuine.metadata().clone();
    let segment = genuine.segments()[0].clone();

    // Fewer segments than references: the first unmatched reference
    // is named — a real digest, never a fabricated zero.
    let error = Checkpoint::new(metadata.clone(), Vec::new())
        .verify()
        .expect_err("missing segment is typed");
    let sema_engine::Error::SegmentMissing { digest } = error else {
        panic!("expected SegmentMissing, got {error:?}");
    };
    assert_eq!(digest, metadata.segments()[0].digest());
    assert_ne!(digest, SegmentDigest::new([0; 32]));

    // More segments than references: a surplus segment no reference
    // names is its own variant.
    let error = Checkpoint::new(metadata, vec![segment.clone(), segment])
        .verify()
        .expect_err("surplus segment is typed");
    assert!(matches!(
        error,
        sema_engine::Error::SegmentSurplus {
            referenced: 1,
            supplied: 2
        }
    ));
}

#[test]
fn import_rejects_a_schema_hash_that_does_not_re_derive_from_the_inventory() {
    let fixture = Fixture::new();
    let genuine = fixture.checkpointed("lying-schema");
    let metadata = genuine.metadata();

    // Forge the stamped store schema hash and recompute the outer
    // digest over it: the artifact now passes every content-address
    // check.
    let lying = Splice::flipping(metadata.store_schema_hash().bytes()).metadata(metadata);
    let error = Checkpoint::new(lying.clone(), genuine.segments().to_vec())
        .verify()
        .expect_err("outer digest catches the raw lie");
    let sema_engine::Error::CheckpointDigestMismatch { computed, .. } = error else {
        panic!("expected CheckpointDigestMismatch, got {error:?}");
    };
    let consistent =
        Splice::new(lying.checkpoint_digest().bytes(), computed.bytes()).metadata(&lying);
    let forged = Checkpoint::new(consistent, genuine.segments().to_vec());
    forged
        .verify()
        .expect("digest-consistent forgery passes content checks");

    // The stamped hash must re-derive from the carried inventory:
    // import catches what content verification alone cannot see.
    let mut target = fixture.open_store("lying-schema-target", "lying-schema");
    let mut session = target.begin_import().expect("import session mints");
    session
        .ingest_checkpoint(forged)
        .expect("content checks pass");
    let error = session
        .commit(&Families::new())
        .expect_err("schema re-derivation rejects the forgery");
    assert!(matches!(
        error,
        sema_engine::Error::CheckpointSchemaMismatch { .. }
    ));
}

#[test]
fn acknowledging_a_head_over_a_rewritten_log_is_a_typed_mismatch() {
    let fixture = Fixture::new();
    let (genuine, head) = {
        let mut engine = fixture.open_versioned("outbox-mismatch");
        let thoughts = engine
            .register_table(fixture.descriptor())
            .expect("table registers");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        let entry = engine.versioned_commit_log().expect("log reads")[0].clone();
        let outbox: Vec<OutboxEntry> = engine.unshipped_outbox().expect("outbox reads");
        (entry, MirrorHead::from(&outbox[0]))
    };

    // Rewrite the logged entry under the outbox row's feet: the
    // replacement is internally valid but carries a different digest.
    let forged = forged_replacement(&genuine, "forged");
    fixture
        .raw_kernel("outbox-mismatch")
        .write(|transaction| {
            VERSIONED_COMMIT_LOG.insert(transaction, genuine.commit_sequence().value(), &forged)
        })
        .expect("forged row writes");

    // An honest mirror echoing the recorded outbox head is refused:
    // the row no longer matches the logged entry at that sequence.
    let engine = fixture.open_versioned("outbox-mismatch");
    let error = engine
        .acknowledge_mirror(head)
        .expect_err("mismatched outbox row is typed");
    assert!(matches!(
        error,
        sema_engine::Error::OutboxEntryMismatch { sequence: 1 }
    ));
}
