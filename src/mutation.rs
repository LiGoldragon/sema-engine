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
