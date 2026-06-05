use signal_sema::SemaOperation;

use crate::{
    CommitSequence, EngineRecord, IdentifiedTableReference, RecordIdentifier, RecordKey,
    SnapshotIdentifier, TableName, TableReference,
};

#[derive(Debug, Clone)]
pub struct Assertion<RecordValue> {
    table: TableReference<RecordValue>,
    record: RecordValue,
}

impl<RecordValue> Assertion<RecordValue> {
    pub fn new(table: TableReference<RecordValue>, record: RecordValue) -> Self {
        Self { table, record }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn record(&self) -> &RecordValue {
        &self.record
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationReceipt {
    operation: SemaOperation,
    table: TableName,
    key: RecordKey,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
}

#[derive(Debug, Clone)]
pub struct IdentifiedAssertion<RecordValue> {
    table: IdentifiedTableReference<RecordValue>,
    record: RecordValue,
}

#[derive(Debug, Clone)]
pub struct IdentifiedRetraction<RecordValue> {
    table: IdentifiedTableReference<RecordValue>,
    identifier: RecordIdentifier,
}

#[derive(Debug, Clone)]
pub struct IdentifiedMutation<RecordValue> {
    table: IdentifiedTableReference<RecordValue>,
    identifier: RecordIdentifier,
    record: RecordValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedMutationReceipt {
    operation: SemaOperation,
    table: TableName,
    identifier: RecordIdentifier,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
}

impl MutationReceipt {
    pub fn new(
        operation: SemaOperation,
        table: TableName,
        key: RecordKey,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
    ) -> Self {
        Self {
            operation,
            table,
            key,
            commit_sequence,
            snapshot,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}

impl<RecordValue> IdentifiedAssertion<RecordValue> {
    pub fn new(table: IdentifiedTableReference<RecordValue>, record: RecordValue) -> Self {
        Self { table, record }
    }

    pub fn table(&self) -> &IdentifiedTableReference<RecordValue> {
        &self.table
    }

    pub fn record(&self) -> &RecordValue {
        &self.record
    }
}

impl<RecordValue> IdentifiedRetraction<RecordValue> {
    pub fn new(table: IdentifiedTableReference<RecordValue>, identifier: RecordIdentifier) -> Self {
        Self { table, identifier }
    }

    pub fn table(&self) -> &IdentifiedTableReference<RecordValue> {
        &self.table
    }

    pub fn identifier(&self) -> RecordIdentifier {
        self.identifier
    }
}

impl<RecordValue> IdentifiedMutation<RecordValue> {
    pub fn new(
        table: IdentifiedTableReference<RecordValue>,
        identifier: RecordIdentifier,
        record: RecordValue,
    ) -> Self {
        Self {
            table,
            identifier,
            record,
        }
    }

    pub fn table(&self) -> &IdentifiedTableReference<RecordValue> {
        &self.table
    }

    pub fn identifier(&self) -> RecordIdentifier {
        self.identifier
    }

    pub fn record(&self) -> &RecordValue {
        &self.record
    }
}

impl IdentifiedMutationReceipt {
    pub fn new(
        operation: SemaOperation,
        table: TableName,
        identifier: RecordIdentifier,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
    ) -> Self {
        Self {
            operation,
            table,
            identifier,
            commit_sequence,
            snapshot,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn identifier(&self) -> RecordIdentifier {
        self.identifier
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}

impl<RecordValue> Assertion<RecordValue>
where
    RecordValue: EngineRecord,
{
    pub fn record_key(&self) -> RecordKey {
        self.record.record_key()
    }
}

#[derive(Debug, Clone)]
pub struct Mutation<RecordValue> {
    table: TableReference<RecordValue>,
    record: RecordValue,
}

impl<RecordValue> Mutation<RecordValue> {
    pub fn new(table: TableReference<RecordValue>, record: RecordValue) -> Self {
        Self { table, record }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn record(&self) -> &RecordValue {
        &self.record
    }
}

impl<RecordValue> Mutation<RecordValue>
where
    RecordValue: EngineRecord,
{
    pub fn record_key(&self) -> RecordKey {
        self.record.record_key()
    }
}

#[derive(Debug, Clone)]
pub struct Retraction<RecordValue> {
    table: TableReference<RecordValue>,
    key: RecordKey,
}

impl<RecordValue> Retraction<RecordValue> {
    pub fn new(table: TableReference<RecordValue>, key: RecordKey) -> Self {
        Self { table, key }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }
}

/// A request to commit one or more typed write operations as one
/// transaction. Single-operation uses a length-1 batch.
///
/// Renamed from `AtomicBatch` per DA/62 §5 — atomicity is structural.
#[derive(Debug, Clone)]
pub struct CommitRequest<RecordValue> {
    table: TableReference<RecordValue>,
    operations: Vec<WriteOperation<RecordValue>>,
}

impl<RecordValue> CommitRequest<RecordValue> {
    pub fn new(table: TableReference<RecordValue>) -> Self {
        Self {
            table,
            operations: Vec::new(),
        }
    }

    pub fn assert(mut self, record: RecordValue) -> Self {
        self.operations.push(WriteOperation::Assert(record));
        self
    }

    pub fn mutate(mut self, record: RecordValue) -> Self {
        self.operations.push(WriteOperation::Mutate(record));
        self
    }

    pub fn retract(mut self, key: RecordKey) -> Self {
        self.operations.push(WriteOperation::Retract(key));
        self
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn operations(&self) -> &[WriteOperation<RecordValue>] {
        &self.operations
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// One typed write effect inside a [`CommitRequest`]. Renamed from
/// `AtomicOperation` per DA/62 §5.
#[derive(Debug, Clone)]
pub enum WriteOperation<RecordValue> {
    Assert(RecordValue),
    Mutate(RecordValue),
    Retract(RecordKey),
}

impl<RecordValue> WriteOperation<RecordValue> {
    pub fn operation(&self) -> SemaOperation {
        match self {
            Self::Assert(_) => SemaOperation::Assert,
            Self::Mutate(_) => SemaOperation::Mutate,
            Self::Retract(_) => SemaOperation::Retract,
        }
    }
}

/// Receipt from a successful [`crate::Engine::commit`]. The receipt
/// names the committed snapshot and the number of per-operation
/// effects. The top-level no longer carries a single operation;
/// per-operation effects live in the commit log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    table: TableName,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    operation_count: usize,
}

impl CommitReceipt {
    pub fn new(
        table: TableName,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        operation_count: usize,
    ) -> Self {
        Self {
            table,
            commit_sequence,
            snapshot,
            operation_count,
        }
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn operation_count(&self) -> usize {
        self.operation_count
    }
}
