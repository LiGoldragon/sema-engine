//! The typed replay envelope — the authoritative, hash-linked log entry.

use rkyv::{Archive, Deserialize, Serialize};

use super::error::VersionedError;
use super::identity::{CommitSequence, EntryDigest, RecordKey, SchemaHash};

/// What an entry does to the materialized view (report 92 §1 envelope:
/// "op kind | table id | record key | schema-hash | payload | ...").
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum OperationKind {
    Assert,
    Retract,
    SchemaTransition,
    Checkpoint,
}

impl OperationKind {
    /// Stable byte for digest input — fixed, never reordered.
    const fn tag(self) -> u8 {
        match self {
            OperationKind::Assert => 0,
            OperationKind::Retract => 1,
            OperationKind::SchemaTransition => 2,
            OperationKind::Checkpoint => 3,
        }
    }
}

/// One authoritative log entry. "Typed in motion, erased at rest": the
/// `payload` is opaque rkyv bytes here, and `schema_hash` selects the
/// decoder that restores the typed value on replay. Being a *concrete*
/// type, it derives rkyv trivially and stores directly in a
/// `sema::Table<u64, ReplayEnvelope>` — exactly as `sema-engine` stores its
/// own `CommitLogEntry`. The generic-vs-rkyv-HRTB problem never appears
/// because genericity lives in the closed-enum decode step, not here.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct ReplayEnvelope {
    sequence: CommitSequence,
    operation: OperationKind,
    schema_hash: SchemaHash,
    key: RecordKey,
    payload: Vec<u8>,
    previous_digest: EntryDigest,
    entry_digest: EntryDigest,
}

impl ReplayEnvelope {
    /// Build an entry and seal it with its content digest.
    pub fn new(
        sequence: CommitSequence,
        operation: OperationKind,
        schema_hash: SchemaHash,
        key: RecordKey,
        payload: Vec<u8>,
        previous_digest: EntryDigest,
    ) -> Self {
        let mut envelope = Self {
            sequence,
            operation,
            schema_hash,
            key,
            payload,
            previous_digest,
            entry_digest: EntryDigest::ZERO,
        };
        envelope.entry_digest = envelope.compute_digest();
        envelope
    }

    pub fn sequence(&self) -> CommitSequence {
        self.sequence
    }
    pub fn operation(&self) -> OperationKind {
        self.operation
    }
    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }
    pub fn key(&self) -> &RecordKey {
        &self.key
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn previous_digest(&self) -> EntryDigest {
        self.previous_digest
    }
    pub fn entry_digest(&self) -> EntryDigest {
        self.entry_digest
    }

    /// Recompute the digest over canonical field bytes. Excludes
    /// `entry_digest` itself; field order is fixed.
    fn compute_digest(&self) -> EntryDigest {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.sequence.value().to_le_bytes());
        hasher.update(&[self.operation.tag()]);
        hasher.update(self.schema_hash.as_bytes());
        hasher.update(&(self.key.as_bytes().len() as u32).to_le_bytes());
        hasher.update(self.key.as_bytes());
        hasher.update(&(self.payload.len() as u32).to_le_bytes());
        hasher.update(&self.payload);
        hasher.update(self.previous_digest.as_bytes());
        EntryDigest::from_bytes(*hasher.finalize().as_bytes())
    }

    /// Verify integrity (recomputed digest matches stored) and chain
    /// linkage (recorded predecessor matches the actual prior digest, or
    /// ZERO at genesis). The replay-time tamper / torn-write guard.
    pub fn verify(&self, prior: Option<EntryDigest>) -> Result<(), VersionedError> {
        if self.compute_digest() != self.entry_digest {
            return Err(VersionedError::CorruptEntry {
                sequence: self.sequence.value(),
            });
        }
        let expected = prior.unwrap_or(EntryDigest::ZERO);
        if self.previous_digest != expected {
            return Err(VersionedError::BrokenChain {
                sequence: self.sequence.value(),
                recorded: self.previous_digest,
                actual: expected,
            });
        }
        Ok(())
    }
}
