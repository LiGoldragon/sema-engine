use signal_core::SignalVerb;

use crate::{EngineRecord, RecordKey, SnapshotId, TableName, TableReference};

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
    verb: SignalVerb,
    table: TableName,
    key: RecordKey,
    snapshot: SnapshotId,
}

impl MutationReceipt {
    pub fn new(verb: SignalVerb, table: TableName, key: RecordKey, snapshot: SnapshotId) -> Self {
        Self {
            verb,
            table,
            key,
            snapshot,
        }
    }

    pub fn verb(&self) -> SignalVerb {
        self.verb
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn snapshot(&self) -> SnapshotId {
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

#[derive(Debug, Clone)]
pub struct AtomicBatch<RecordValue> {
    table: TableReference<RecordValue>,
    operations: Vec<AtomicOperation<RecordValue>>,
}

impl<RecordValue> AtomicBatch<RecordValue> {
    pub fn new(table: TableReference<RecordValue>) -> Self {
        Self {
            table,
            operations: Vec::new(),
        }
    }

    pub fn assert(mut self, record: RecordValue) -> Self {
        self.operations.push(AtomicOperation::Assert(record));
        self
    }

    pub fn mutate(mut self, record: RecordValue) -> Self {
        self.operations.push(AtomicOperation::Mutate(record));
        self
    }

    pub fn retract(mut self, key: RecordKey) -> Self {
        self.operations.push(AtomicOperation::Retract(key));
        self
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn operations(&self) -> &[AtomicOperation<RecordValue>] {
        &self.operations
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

#[derive(Debug, Clone)]
pub enum AtomicOperation<RecordValue> {
    Assert(RecordValue),
    Mutate(RecordValue),
    Retract(RecordKey),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicReceipt {
    verb: SignalVerb,
    table: TableName,
    snapshot: SnapshotId,
    operation_count: usize,
}

impl AtomicReceipt {
    pub fn new(
        verb: SignalVerb,
        table: TableName,
        snapshot: SnapshotId,
        operation_count: usize,
    ) -> Self {
        Self {
            verb,
            table,
            snapshot,
            operation_count,
        }
    }

    pub fn verb(&self) -> SignalVerb {
        self.verb
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }

    pub fn operation_count(&self) -> usize {
        self.operation_count
    }
}
