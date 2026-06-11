//! The materialized view: a fold over the log into typed records, plus the
//! full-blake3 state digest used as the rebuild correctness oracle.

use std::collections::BTreeMap;

use super::envelope::{OperationKind, ReplayEnvelope};
use super::error::VersionedError;
use super::identity::{CommitSequence, EntryDigest};
use super::migration::{ReducerSet, SchemaTransition};
use super::record::{ComponentRecord, FoldKey};

/// The rebuildable read view — redb tables demoted to something the log can
/// regenerate (report 92). Generic over a component's closed
/// [`ComponentRecord`]; the genericity is one type parameter, fully typed.
pub struct MaterializedView<Record: ComponentRecord> {
    records: BTreeMap<FoldKey, Record>,
    applied_through: Option<CommitSequence>,
}

impl<Record: ComponentRecord> MaterializedView<Record> {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            applied_through: None,
        }
    }

    /// Fold an entire entry slice into a fresh view, verifying the hash
    /// chain as it goes. The shared rebuild path for both log backends, so
    /// the same-file and separate-file arms are compared on identical
    /// replay semantics.
    pub fn from_entries(
        entries: &[ReplayEnvelope],
        reducers: &ReducerSet,
    ) -> Result<Self, VersionedError> {
        let mut view = Self::new();
        let mut prior: Option<EntryDigest> = None;
        for envelope in entries {
            envelope.verify(prior)?;
            view.apply(envelope, reducers)?;
            prior = Some(envelope.entry_digest());
        }
        Ok(view)
    }

    /// Apply one entry, advancing the applied-watermark. Read-after-write
    /// locally is preserved because callers apply before returning success
    /// (report 92 §2: the local view never lags; only a remote mirror does).
    pub fn apply(
        &mut self,
        envelope: &ReplayEnvelope,
        reducers: &ReducerSet,
    ) -> Result<(), VersionedError> {
        match envelope.operation() {
            OperationKind::Assert => {
                let record = Record::decode(envelope.schema_hash(), envelope.payload())?;
                self.records.insert(record.fold_key(), record);
            }
            OperationKind::Retract => {
                let fold_key = FoldKey::new(envelope.schema_hash(), envelope.key().clone());
                self.records.remove(&fold_key);
            }
            OperationKind::SchemaTransition => {
                let transition = SchemaTransition::decode(envelope.payload())?;
                self.migrate_family(&transition, reducers)?;
            }
            OperationKind::Checkpoint => {
                // A checkpoint bounds replay in production; the in-memory
                // fold needs no action beyond advancing the watermark.
            }
        }
        self.applied_through = Some(envelope.sequence());
        Ok(())
    }

    fn migrate_family(
        &mut self,
        transition: &SchemaTransition,
        reducers: &ReducerSet,
    ) -> Result<(), VersionedError> {
        let reducer = reducers.find(transition.from(), transition.to()).ok_or(
            VersionedError::MissingReducer {
                from: transition.from(),
                to: transition.to(),
            },
        )?;
        let affected: Vec<FoldKey> = self
            .records
            .keys()
            .filter(|fold_key| fold_key.family() == transition.from())
            .cloned()
            .collect();
        for fold_key in affected {
            if let Some(record) = self.records.remove(&fold_key) {
                let (_old_hash, old_payload) = record.reencode()?;
                let new_payload = reducer.migrate(&old_payload)?;
                let migrated = Record::decode(transition.to(), &new_payload)?;
                self.records.insert(migrated.fold_key(), migrated);
            }
        }
        Ok(())
    }

    /// Full 32-byte blake3 over the sorted view — the rebuild correctness
    /// oracle. Report 92 §5 insists on the *full* digest, not spirit's
    /// truncated-`u64` marker, so the oracle resists collisions under fuzz.
    /// `BTreeMap` iteration is sorted, so the digest is order-independent of
    /// insertion.
    pub fn state_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for (fold_key, record) in &self.records {
            hasher.update(fold_key.family().as_bytes());
            let key_bytes = fold_key.key().as_bytes();
            hasher.update(&(key_bytes.len() as u32).to_le_bytes());
            hasher.update(key_bytes);
            let contribution = record.digest_contribution();
            hasher.update(&(contribution.len() as u32).to_le_bytes());
            hasher.update(&contribution);
        }
        *hasher.finalize().as_bytes()
    }

    pub fn applied_through(&self) -> Option<CommitSequence> {
        self.applied_through
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl<Record: ComponentRecord> Default for MaterializedView<Record> {
    fn default() -> Self {
        Self::new()
    }
}
