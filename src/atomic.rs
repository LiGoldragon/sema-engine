//! Heterogeneous typed writes that share one engine transaction.
//!
//! A group is deliberately opaque: callers can add records from any registered
//! table, while the engine retains the typed table handles needed to materialize
//! every operation in the same SEMA write transaction.

use std::collections::HashSet;

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;

use crate::commit_log::CommitLog;
use crate::engine::{COUNTERS, LATEST_COMMIT_SEQUENCE_KEY, LATEST_SNAPSHOT_KEY};
use crate::{
    CommitLogEntry, CommitLogOperation, CommitSequence, DeltaKind, Engine, EngineStoredRecord,
    EngineStoredValue, Error, RecordKey, Result, SnapshotIdentifier, TableName, TableReference,
    VersionedLogOperation, VersionedPayload,
};

/// A heterogeneous write group. Construct it with [`Engine::begin_atomic_commit`].
pub struct AtomicCommit {
    writes: Vec<Box<dyn AtomicWrite>>,
}

impl AtomicCommit {
    pub(crate) fn new() -> Self {
        Self { writes: Vec::new() }
    }

    pub fn assert<RecordValue>(mut self, table: TableReference<RecordValue>, record: RecordValue) -> Self
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>>,
    {
        let key = record.record_key();
        self.writes.push(Box::new(TypedAtomicWrite::new(
            AtomicKind::Assert, table, key, Some(record),
        )));
        self
    }

    pub fn mutate<RecordValue>(mut self, table: TableReference<RecordValue>, record: RecordValue) -> Self
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>>,
    {
        let key = record.record_key();
        self.writes.push(Box::new(TypedAtomicWrite::new(
            AtomicKind::Mutate, table, key, Some(record),
        )));
        self
    }

    pub fn retract<RecordValue>(mut self, table: TableReference<RecordValue>, key: RecordKey) -> Self
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>>,
    {
        self.writes.push(Box::new(TypedAtomicWrite::new(
            AtomicKind::Retract, table, key, None,
        )));
        self
    }

    pub fn operation_count(&self) -> usize {
        self.writes.len()
    }
}

/// Receipt for one heterogeneous commit. All listed effects share this marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicCommitReceipt {
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    operation_count: usize,
}

impl AtomicCommitReceipt {
    pub(crate) fn new(commit_sequence: CommitSequence, snapshot: SnapshotIdentifier, operation_count: usize) -> Self {
        Self { commit_sequence, snapshot, operation_count }
    }

    pub fn commit_sequence(&self) -> CommitSequence { self.commit_sequence }
    pub fn snapshot(&self) -> SnapshotIdentifier { self.snapshot }
    pub fn operation_count(&self) -> usize { self.operation_count }
}

#[derive(Clone, Copy)]
enum AtomicKind { Assert, Mutate, Retract }

impl AtomicKind {
    fn operation(self) -> SemaOperation {
        match self {
            Self::Assert => SemaOperation::Assert,
            Self::Mutate => SemaOperation::Mutate,
            Self::Retract => SemaOperation::Retract,
        }
    }

    fn delta(self) -> DeltaKind {
        match self {
            Self::Assert => DeltaKind::Assert,
            Self::Mutate => DeltaKind::Mutate,
            Self::Retract => DeltaKind::Retract,
        }
    }
}

trait AtomicWrite: Send + Sync {
    fn table(&self) -> TableName;
    fn key(&self) -> &RecordKey;
    fn prepare(&self, engine: &Engine) -> Result<PreparedAtomicWrite>;
    fn materialize(&self, transaction: &sema::WriteTransaction) -> sema::Result<()>;
}

struct TypedAtomicWrite<RecordValue> {
    kind: AtomicKind,
    table: TableReference<RecordValue>,
    key: RecordKey,
    record: Option<RecordValue>,
}

impl<RecordValue> TypedAtomicWrite<RecordValue> {
    fn new(kind: AtomicKind, table: TableReference<RecordValue>, key: RecordKey, record: Option<RecordValue>) -> Self {
        Self { kind, table, key, record }
    }
}

impl<RecordValue> AtomicWrite for TypedAtomicWrite<RecordValue>
where
    RecordValue: EngineStoredValue + Send + Sync + 'static,
    <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>>,
{
    fn table(&self) -> TableName { *self.table.name() }
    fn key(&self) -> &RecordKey { &self.key }
    fn prepare(&self, engine: &Engine) -> Result<PreparedAtomicWrite> {
        engine.ensure_atomic_registered(&self.table)?;
        let prior = engine.atomic_read(&self.table, &self.key)?;
        match (self.kind, prior.as_ref()) {
            (AtomicKind::Assert, Some(_)) => return Err(engine.atomic_duplicate_assert(&self.table, &self.key)),
            (AtomicKind::Mutate | AtomicKind::Retract, None) => return Err(engine.atomic_missing(&self.table, &self.key)),
            _ => {}
        }
        let payload = match self.kind {
            AtomicKind::Retract => VersionedPayload::tombstone(),
            AtomicKind::Assert | AtomicKind::Mutate => engine.atomic_payload(*self.table.name(), self.record.as_ref().expect("record-bearing write"))?,
        };
        let delta_record = match self.kind {
            AtomicKind::Retract => prior.expect("validated present retract"),
            AtomicKind::Assert | AtomicKind::Mutate => self.record.as_ref().expect("record-bearing write").clone(),
        };
        Ok(PreparedAtomicWrite {
            operation: CommitLogOperation::new(self.kind.operation(), *self.table.name(), Some(self.key.clone())),
            versioned: engine.atomic_versioned_operation(self.kind.operation(), *self.table.name(), self.key.clone(), payload)?,
            delta: Box::new(TypedAtomicDelta {
                kind: self.kind.delta(), table: *self.table.name(), key: self.key.clone(), record: delta_record,
            }),
        })
    }

    fn materialize(&self, transaction: &sema::WriteTransaction) -> sema::Result<()> {
        match self.kind {
            AtomicKind::Assert | AtomicKind::Mutate => self.table.sema_table().insert(
                transaction, self.key.to_owned_string(), self.record.as_ref().expect("record-bearing write"),
            ),
            AtomicKind::Retract => { self.table.sema_table().remove(transaction, self.key.to_owned_string())?; Ok(()) }
        }
    }
}

struct PreparedAtomicWrite {
    operation: CommitLogOperation,
    versioned: Option<VersionedLogOperation>,
    delta: Box<dyn AtomicDelta>,
}

trait AtomicDelta {
    fn announce(&self, engine: &Engine, snapshot: SnapshotIdentifier);
}

struct TypedAtomicDelta<RecordValue> {
    kind: DeltaKind,
    table: TableName,
    key: RecordKey,
    record: RecordValue,
}

impl<RecordValue> AtomicDelta for TypedAtomicDelta<RecordValue>
where
    RecordValue: EngineStoredValue + Send + Sync + 'static,
    <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>>,
{
    fn announce(&self, engine: &Engine, snapshot: SnapshotIdentifier) {
        engine.atomic_announce(self.kind, self.table, &self.key, snapshot, &self.record);
    }
}

impl Engine {
    /// Begin a transaction that may contain writes for multiple registered tables.
    pub fn begin_atomic_commit(&self) -> AtomicCommit { AtomicCommit::new() }

    /// Preflight and materialize a heterogeneous group in one SEMA transaction.
    /// Deltas are delivered only after that transaction commits.
    pub fn commit_atomic(&self, commit: AtomicCommit) -> Result<AtomicCommitReceipt> {
        let _write_guard = self.write_guard();
        if commit.writes.is_empty() {
            return Err(Error::EmptyAtomicCommit);
        }
        let mut coordinates = HashSet::new();
        let mut prepared = Vec::with_capacity(commit.writes.len());
        for write in &commit.writes {
            let coordinate = (write.table().as_str().to_owned(), write.key().clone());
            if !coordinates.insert(coordinate) {
                return Err(Error::DuplicateWriteKey {
                    table: write.table().as_str().to_owned(), key: write.key().to_owned_string(),
                });
            }
            prepared.push(write.prepare(self)?);
        }
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let operations = NonEmpty::try_from_vec(prepared.iter().map(|item| item.operation.clone()).collect())
            .map_err(|_| Error::EmptyAtomicCommit)?;
        let entry = CommitLogEntry::new(commit_sequence, snapshot, operations);
        let versioned_operations: Vec<_> = prepared.iter().filter_map(|item| item.versioned.clone()).collect();
        let versioned_entry = if self.versioning_policy.is_some() {
            self.versioned_entry(commit_sequence, snapshot, NonEmpty::try_from_vec(versioned_operations).map_err(|_| Error::EmptyAtomicCommit)?)?
        } else { None };
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            for write in &commit.writes { write.materialize(transaction)?; }
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(transaction, LATEST_COMMIT_SEQUENCE_KEY, &commit_sequence.value())?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;
        for item in &prepared { item.delta.announce(self, snapshot); }
        Ok(AtomicCommitReceipt::new(commit_sequence, snapshot, prepared.len()))
    }
}
