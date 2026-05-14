use signal_core::SemaVerb;

use crate::{RecordKey, SnapshotId, TableName, TableReference};

#[derive(Debug, Clone)]
pub struct QueryPlan<RecordValue> {
    table: TableReference<RecordValue>,
    filter: QueryFilter,
}

impl<RecordValue> QueryPlan<RecordValue> {
    pub fn all(table: TableReference<RecordValue>) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
        }
    }

    pub fn key(table: TableReference<RecordValue>, key: RecordKey) -> Self {
        Self {
            table,
            filter: QueryFilter::Key(key),
        }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn filter(&self) -> &QueryFilter {
        &self.filter
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryFilter {
    All,
    Key(RecordKey),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySnapshot<RecordValue> {
    verb: SemaVerb,
    table: TableName,
    snapshot: SnapshotId,
    records: Vec<RecordValue>,
}

impl<RecordValue> QuerySnapshot<RecordValue> {
    pub fn new(
        verb: SemaVerb,
        table: TableName,
        snapshot: SnapshotId,
        records: Vec<RecordValue>,
    ) -> Self {
        Self {
            verb,
            table,
            snapshot,
            records,
        }
    }

    pub fn verb(&self) -> SemaVerb {
        self.verb
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }

    pub fn records(&self) -> &[RecordValue] {
        &self.records
    }
}
