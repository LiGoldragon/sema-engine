//! Per-component instantiation + tests for the version-controlled-state
//! spike. In production the families, the closed `DemoRecord` enum, the
//! sealed-trait impls, and the per-family `encode`/`decode` are
//! macro-emitted into the component crate; here they are hand-written,
//! in-crate, under `cfg(test)`, standing in for that generated code. This
//! is what lets the spike exercise the closed-sum decode and the
//! same-file-vs-separate-file rebuild on identical semantics.

use rkyv::{Archive, Deserialize, Serialize};

use super::error::VersionedError;
use super::family::{RecordFamily, sealed};
use super::identity::{RecordKey, SchemaHash};
use super::migration::Reducer;
use super::record::{ComponentRecord, FoldKey};

// ── A spirit-shaped multi-family payload: two schema versions of an
//    "entry" family plus a "referent" family (report 92 §5). ──

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[rkyv(derive(Debug))]
struct EntryV1 {
    id: u64,
    name: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[rkyv(derive(Debug))]
struct EntryV2 {
    id: u64,
    name: String,
    score: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[rkyv(derive(Debug))]
struct Referent {
    entry_id: u64,
    label: String,
}

impl sealed::Sealed for EntryV1 {}
impl sealed::Sealed for EntryV2 {}
impl sealed::Sealed for Referent {}

impl RecordFamily for EntryV1 {
    type Value = EntryV1;
    fn schema_label() -> &'static str {
        "demo.entry.v1"
    }
    fn record_key(value: &EntryV1) -> RecordKey {
        RecordKey::new(value.id.to_le_bytes().to_vec())
    }
    fn encode(value: &EntryV1) -> Result<Vec<u8>, VersionedError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .map(|bytes| bytes.to_vec())
            .map_err(VersionedError::Encode)
    }
    fn decode(bytes: &[u8]) -> Result<EntryV1, VersionedError> {
        rkyv::from_bytes::<EntryV1, rkyv::rancor::Error>(bytes).map_err(VersionedError::Decode)
    }
}

impl RecordFamily for EntryV2 {
    type Value = EntryV2;
    fn schema_label() -> &'static str {
        "demo.entry.v2"
    }
    fn record_key(value: &EntryV2) -> RecordKey {
        RecordKey::new(value.id.to_le_bytes().to_vec())
    }
    fn encode(value: &EntryV2) -> Result<Vec<u8>, VersionedError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .map(|bytes| bytes.to_vec())
            .map_err(VersionedError::Encode)
    }
    fn decode(bytes: &[u8]) -> Result<EntryV2, VersionedError> {
        rkyv::from_bytes::<EntryV2, rkyv::rancor::Error>(bytes).map_err(VersionedError::Decode)
    }
}

impl RecordFamily for Referent {
    type Value = Referent;
    fn schema_label() -> &'static str {
        "demo.referent.v1"
    }
    fn record_key(value: &Referent) -> RecordKey {
        RecordKey::new(value.entry_id.to_le_bytes().to_vec())
    }
    fn encode(value: &Referent) -> Result<Vec<u8>, VersionedError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .map(|bytes| bytes.to_vec())
            .map_err(VersionedError::Encode)
    }
    fn decode(bytes: &[u8]) -> Result<Referent, VersionedError> {
        rkyv::from_bytes::<Referent, rkyv::rancor::Error>(bytes).map_err(VersionedError::Decode)
    }
}

/// The component's closed record sum. Decode is a closed match over the
/// known families with NO fallback — the knife-edge held (report 92 §6).
enum DemoRecord {
    EntryV1(EntryV1),
    EntryV2(EntryV2),
    Referent(Referent),
}

impl ComponentRecord for DemoRecord {
    fn decode(schema_hash: SchemaHash, payload: &[u8]) -> Result<Self, VersionedError> {
        if schema_hash == EntryV1::schema_hash() {
            Ok(DemoRecord::EntryV1(EntryV1::decode(payload)?))
        } else if schema_hash == EntryV2::schema_hash() {
            Ok(DemoRecord::EntryV2(EntryV2::decode(payload)?))
        } else if schema_hash == Referent::schema_hash() {
            Ok(DemoRecord::Referent(Referent::decode(payload)?))
        } else {
            Err(VersionedError::UnknownSchemaHash(schema_hash))
        }
    }

    fn fold_key(&self) -> FoldKey {
        match self {
            DemoRecord::EntryV1(value) => {
                FoldKey::new(EntryV1::schema_hash(), EntryV1::record_key(value))
            }
            DemoRecord::EntryV2(value) => {
                FoldKey::new(EntryV2::schema_hash(), EntryV2::record_key(value))
            }
            DemoRecord::Referent(value) => {
                FoldKey::new(Referent::schema_hash(), Referent::record_key(value))
            }
        }
    }

    fn digest_contribution(&self) -> Vec<u8> {
        self.reencode()
            .map(|(_hash, bytes)| bytes)
            .expect("demo families always re-encode")
    }

    fn reencode(&self) -> Result<(SchemaHash, Vec<u8>), VersionedError> {
        match self {
            DemoRecord::EntryV1(value) => Ok((EntryV1::schema_hash(), EntryV1::encode(value)?)),
            DemoRecord::EntryV2(value) => Ok((EntryV2::schema_hash(), EntryV2::encode(value)?)),
            DemoRecord::Referent(value) => Ok((Referent::schema_hash(), Referent::encode(value)?)),
        }
    }
}

/// The component-authored reducer for the entry family's v1 -> v2 step:
/// the new `score` field defaults to 0. This is the domain meaning that
/// only the component can supply (report 92 §6).
struct EntryV1ToV2;

impl Reducer for EntryV1ToV2 {
    fn from_schema(&self) -> SchemaHash {
        EntryV1::schema_hash()
    }
    fn to_schema(&self) -> SchemaHash {
        EntryV2::schema_hash()
    }
    fn migrate(&self, old_payload: &[u8]) -> Result<Vec<u8>, VersionedError> {
        let old = EntryV1::decode(old_payload)?;
        let new = EntryV2 {
            id: old.id,
            name: old.name,
            score: 0,
        };
        EntryV2::encode(&new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned::identity::RecordKey;
    use crate::versioned::log::{FrameFileLog, SameFileLog, VersionedLog};
    use crate::versioned::migration::{ReducerSet, SchemaTransition};
    use tempfile::tempdir;

    /// Assert the same three records (two entries + a referent) into any
    /// backend. Generic over the backend so both arms run identical input.
    fn populate(log: &mut impl VersionedLog) -> Result<(), VersionedError> {
        log.commit_assert::<EntryV1>(&EntryV1 {
            id: 1,
            name: "alpha".to_string(),
        })?;
        log.commit_assert::<EntryV1>(&EntryV1 {
            id: 2,
            name: "beta".to_string(),
        })?;
        log.commit_assert::<Referent>(&Referent {
            entry_id: 1,
            label: "link".to_string(),
        })?;
        Ok(())
    }

    #[test]
    fn same_file_and_frame_file_rebuild_to_identical_digest() -> Result<(), VersionedError> {
        let dir = tempdir()?;
        let mut same = SameFileLog::open(&dir.path().join("store.sema"))?;
        let mut frame = FrameFileLog::open(&dir.path().join("log.frames"))?;
        populate(&mut same)?;
        populate(&mut frame)?;

        let reducers = ReducerSet::new();
        let same_view = same.rebuild::<DemoRecord>(&reducers)?;
        let frame_view = frame.rebuild::<DemoRecord>(&reducers)?;

        assert_eq!(same_view.record_count(), 3);
        assert_eq!(frame_view.record_count(), 3);
        // The central claim: the two file layouts are observationally
        // identical after rebuild — same content, same head.
        assert_eq!(same_view.state_digest(), frame_view.state_digest());
        assert_eq!(same.head()?.map(|head| head.sequence().value()), Some(2));
        assert_eq!(frame.head()?.map(|head| head.sequence().value()), Some(2));
        Ok(())
    }

    /// Retract on either backend removes exactly the keyed record.
    fn assert_retract<Log: VersionedLog>(mut log: Log) -> Result<(), VersionedError> {
        populate(&mut log)?;
        log.commit_retract::<EntryV1>(RecordKey::new(1u64.to_le_bytes().to_vec()))?;
        let view = log.rebuild::<DemoRecord>(&ReducerSet::new())?;
        assert_eq!(view.record_count(), 2);
        Ok(())
    }

    #[test]
    fn retract_removes_record_in_both_backends() -> Result<(), VersionedError> {
        let dir = tempdir()?;
        assert_retract(SameFileLog::open(&dir.path().join("a.sema"))?)?;
        assert_retract(FrameFileLog::open(&dir.path().join("b.frames"))?)?;
        Ok(())
    }

    #[test]
    fn schema_transition_migrates_via_reducer() -> Result<(), VersionedError> {
        let dir = tempdir()?;
        let mut log = SameFileLog::open(&dir.path().join("subject.sema"))?;
        populate(&mut log)?;
        log.commit_transition(&SchemaTransition::new(
            EntryV1::schema_hash(),
            EntryV2::schema_hash(),
        ))?;

        // Without the reducer, replaying the transition fails loudly —
        // the migration's domain step is not optional.
        let missing = log.rebuild::<DemoRecord>(&ReducerSet::new());
        assert!(matches!(
            missing,
            Err(VersionedError::MissingReducer { .. })
        ));

        // With the reducer, the two entries become v2; the referent is
        // untouched.
        let reducers = ReducerSet::new().with(Box::new(EntryV1ToV2));
        let migrated = log.rebuild::<DemoRecord>(&reducers)?;
        assert_eq!(migrated.record_count(), 3);

        // Oracle: a world built directly from the post-migration records
        // must have the identical state digest.
        let mut oracle = SameFileLog::open(&dir.path().join("oracle.sema"))?;
        oracle.commit_assert::<EntryV2>(&EntryV2 {
            id: 1,
            name: "alpha".to_string(),
            score: 0,
        })?;
        oracle.commit_assert::<EntryV2>(&EntryV2 {
            id: 2,
            name: "beta".to_string(),
            score: 0,
        })?;
        oracle.commit_assert::<Referent>(&Referent {
            entry_id: 1,
            label: "link".to_string(),
        })?;
        let oracle_view = oracle.rebuild::<DemoRecord>(&ReducerSet::new())?;
        assert_eq!(migrated.state_digest(), oracle_view.state_digest());
        Ok(())
    }

    #[test]
    fn frame_log_ships_as_byte_suffix() -> Result<(), VersionedError> {
        let dir = tempdir()?;
        let source_path = dir.path().join("source.frames");
        let mut source = FrameFileLog::open(&source_path)?;
        populate(&mut source)?;

        // "Ship" by copying the file bytes (a peer at genesis gets the
        // whole suffix). Opening the copy and rebuilding must reproduce the
        // source exactly — the cheapest possible backup unit.
        let peer_path = dir.path().join("peer.frames");
        std::fs::copy(&source_path, &peer_path)?;
        let peer = FrameFileLog::open(&peer_path)?;

        let reducers = ReducerSet::new();
        assert_eq!(
            source.rebuild::<DemoRecord>(&reducers)?.state_digest(),
            peer.rebuild::<DemoRecord>(&reducers)?.state_digest()
        );
        Ok(())
    }

    #[test]
    fn unknown_schema_hash_is_rejected_not_fallen_back() {
        // The knife-edge: a hash for no known family is an error, never a
        // silent generic-record decode (report 92 §6).
        let bogus = SchemaHash::of_label("not.a.real.family");
        let result = DemoRecord::decode(bogus, &[]);
        assert!(matches!(result, Err(VersionedError::UnknownSchemaHash(_))));
    }

    #[test]
    fn tampered_chain_is_detected_on_rebuild() -> Result<(), VersionedError> {
        let dir = tempdir()?;
        let mut log = FrameFileLog::open(&dir.path().join("chain.frames"))?;
        populate(&mut log)?;

        // Read the raw entries, corrupt one frame's bytes on disk, and
        // confirm rebuild rejects it (digest mismatch -> CorruptEntry, or
        // a broken link).
        let frames_path = dir.path().join("chain.frames");
        let mut bytes = std::fs::read(&frames_path)?;
        // Flip a byte well past the first frame's header.
        let midpoint = bytes.len() / 2;
        bytes[midpoint] ^= 0xFF;
        std::fs::write(&frames_path, &bytes)?;

        let reopened = FrameFileLog::open(&frames_path)?;
        let result = reopened.rebuild::<DemoRecord>(&ReducerSet::new());
        assert!(result.is_err());
        Ok(())
    }
}
