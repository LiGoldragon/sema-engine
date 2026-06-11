//! The log abstraction and its two concrete backends — the same-file vs
//! separate-file experiment arms (report 92 §5). The trait's default
//! methods (entry construction, commit verbs, rebuild) are the reusable
//! core; the backends differ only in *where the bytes rest*.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use rkyv::rancor;

use super::envelope::{OperationKind, ReplayEnvelope};
use super::error::VersionedError;
use super::family::RecordFamily;
use super::identity::{CommitSequence, EntryDigest, RecordKey};
use super::migration::{ReducerSet, SchemaTransition};
use super::record::ComponentRecord;
use super::view::MaterializedView;

/// The head of the log: the latest sequence and its content digest. The
/// pair a peer needs to ask for "everything after my head."
#[derive(Debug, Clone, Copy)]
pub struct LogHead {
    sequence: CommitSequence,
    digest: EntryDigest,
}

impl LogHead {
    pub fn new(sequence: CommitSequence, digest: EntryDigest) -> Self {
        Self { sequence, digest }
    }
    pub fn sequence(&self) -> CommitSequence {
        self.sequence
    }
    pub fn digest(&self) -> EntryDigest {
        self.digest
    }
}

/// An append-only, hash-linked, payload-bearing log: the reusable core's
/// central abstraction. A backend implements the three primitives; the
/// commit verbs and rebuild come for free as default methods.
pub trait VersionedLog {
    /// Append a sealed entry. This is the durable commit point.
    fn append(&mut self, envelope: &ReplayEnvelope) -> Result<(), VersionedError>;

    /// Every entry at or after `start`, in sequence order.
    fn entries_from(&self, start: CommitSequence) -> Result<Vec<ReplayEnvelope>, VersionedError>;

    /// The current head, or `None` for an empty log.
    fn head(&self) -> Result<Option<LogHead>, VersionedError>;

    /// Next (sequence, predecessor-digest) for a new entry — one place
    /// builds entry coordinates for every backend.
    fn next_coordinates(&self) -> Result<(CommitSequence, EntryDigest), VersionedError> {
        Ok(match self.head()? {
            Some(head) => (head.sequence().next(), head.digest()),
            None => (CommitSequence::genesis(), EntryDigest::ZERO),
        })
    }

    /// Commit an assertion of a typed family value.
    fn commit_assert<Family: RecordFamily>(
        &mut self,
        value: &Family::Value,
    ) -> Result<CommitSequence, VersionedError> {
        let (sequence, previous_digest) = self.next_coordinates()?;
        let envelope = ReplayEnvelope::new(
            sequence,
            OperationKind::Assert,
            Family::schema_hash(),
            Family::record_key(value),
            Family::encode(value)?,
            previous_digest,
        );
        self.append(&envelope)?;
        Ok(sequence)
    }

    /// Commit a retraction of a key within a family.
    fn commit_retract<Family: RecordFamily>(
        &mut self,
        key: RecordKey,
    ) -> Result<CommitSequence, VersionedError> {
        let (sequence, previous_digest) = self.next_coordinates()?;
        let envelope = ReplayEnvelope::new(
            sequence,
            OperationKind::Retract,
            Family::schema_hash(),
            key,
            Vec::new(),
            previous_digest,
        );
        self.append(&envelope)?;
        Ok(sequence)
    }

    /// Commit a schema-transition entry. The reducer is applied at replay,
    /// not here — the log records the *intent* to migrate.
    fn commit_transition(
        &mut self,
        transition: &SchemaTransition,
    ) -> Result<CommitSequence, VersionedError> {
        let (sequence, previous_digest) = self.next_coordinates()?;
        let envelope = ReplayEnvelope::new(
            sequence,
            OperationKind::SchemaTransition,
            SchemaTransition::marker_hash(),
            RecordKey::empty(),
            transition.encode()?,
            previous_digest,
        );
        self.append(&envelope)?;
        Ok(sequence)
    }

    /// Rebuild the materialized view by folding the whole log.
    fn rebuild<Record: ComponentRecord>(
        &self,
        reducers: &ReducerSet,
    ) -> Result<MaterializedView<Record>, VersionedError> {
        let entries = self.entries_from(CommitSequence::genesis())?;
        MaterializedView::from_entries(&entries, reducers)
    }
}

// ─── Arm A: same-file (payload-bearing table in one sema redb file) ───

const LOG_TABLE: sema::Table<u64, ReplayEnvelope> = sema::Table::new("__versioned_log");
const SPIKE_SCHEMA: sema::Schema = sema::Schema {
    version: sema::SchemaVersion::new(1),
};

/// The log lives as a table inside the component's one `sema` file, written
/// in a single redb transaction (Option A). Free atomicity: the entry — and
/// in a full design, any view write — commit together and cannot diverge
/// (report 92 §5). The cost is the backup unit: shipping means exporting
/// entries, not copying a byte suffix.
pub struct SameFileLog {
    store: sema::Sema,
}

impl SameFileLog {
    pub fn open(path: &Path) -> Result<Self, VersionedError> {
        Ok(Self {
            store: sema::Sema::open_with_schema(path, &SPIKE_SCHEMA)?,
        })
    }
}

impl VersionedLog for SameFileLog {
    fn append(&mut self, envelope: &ReplayEnvelope) -> Result<(), VersionedError> {
        self.store
            .write(|txn| LOG_TABLE.insert(txn, envelope.sequence().value(), envelope))?;
        Ok(())
    }

    fn entries_from(&self, start: CommitSequence) -> Result<Vec<ReplayEnvelope>, VersionedError> {
        let rows = self
            .store
            .read(|txn| LOG_TABLE.range(txn, start.value()..))?;
        Ok(rows
            .into_iter()
            .map(|(_sequence, envelope)| envelope)
            .collect())
    }

    fn head(&self) -> Result<Option<LogHead>, VersionedError> {
        let rows = self.store.read(|txn| LOG_TABLE.iter(txn))?;
        Ok(rows.last().map(|(_sequence, envelope)| {
            LogHead::new(envelope.sequence(), envelope.entry_digest())
        }))
    }
}

// ─── Arm C: separate-file (byte-shippable append-frame log) ───

/// The log is a bespoke append-frame file the view rebuilds from (Option
/// C). Each frame is a 4-byte big-endian length prefix then the entry's
/// rkyv archive — the length-prefixed-rkyv framing blessed by
/// `skills/rust/storage-and-wire.md`, NOT a hand-rolled grammar parser.
/// Backup is the cheapest possible: copy the byte suffix after the peer's
/// head. The per-append `sync_all` is the durable commit point. The cost is
/// lost free atomicity with any separate view (would need a watermark).
pub struct FrameFileLog {
    path: PathBuf,
}

impl FrameFileLog {
    /// Open or create the frame file. Append mode preserves existing
    /// frames, so opening a shipped copy resumes from its content.
    pub fn open(path: &Path) -> Result<Self, VersionedError> {
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl VersionedLog for FrameFileLog {
    fn append(&mut self, envelope: &ReplayEnvelope) -> Result<(), VersionedError> {
        let bytes = rkyv::to_bytes::<rancor::Error>(envelope)
            .map(|bytes| bytes.to_vec())
            .map_err(VersionedError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&(bytes.len() as u32).to_be_bytes())?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn entries_from(&self, start: CommitSequence) -> Result<Vec<ReplayEnvelope>, VersionedError> {
        let data = match std::fs::read(&self.path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut entries = Vec::new();
        let mut offset = 0usize;
        while offset < data.len() {
            if offset + 4 > data.len() {
                return Err(VersionedError::TruncatedFrame {
                    offset: offset as u64,
                });
            }
            let length = u32::from_be_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .expect("a 4-byte slice always converts to [u8; 4]"),
            ) as usize;
            offset += 4;
            if offset + length > data.len() {
                return Err(VersionedError::TruncatedFrame {
                    offset: offset as u64,
                });
            }
            let envelope =
                rkyv::from_bytes::<ReplayEnvelope, rancor::Error>(&data[offset..offset + length])
                    .map_err(VersionedError::Decode)?;
            offset += length;
            if envelope.sequence().value() >= start.value() {
                entries.push(envelope);
            }
        }
        Ok(entries)
    }

    fn head(&self) -> Result<Option<LogHead>, VersionedError> {
        let entries = self.entries_from(CommitSequence::genesis())?;
        Ok(entries
            .last()
            .map(|envelope| LogHead::new(envelope.sequence(), envelope.entry_digest())))
    }
}
