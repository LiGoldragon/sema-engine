use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_core::NonEmpty;
use signal_sema::SemaOperation;

use crate::{RecordKey, SnapshotId, TableName};

/// Durable record of one commit: a request that committed all its
/// write effects (or none) under a single [`SnapshotId`]. A single-operation
/// `assert` / `mutate` / `retract` is just a length-1 commit.
///
/// Note: the spec name in DA/61 §8 + DA/62 §5 is `CommitLogEntry`;
/// `Atomic` is no longer carried at the top level — atomicity is
/// structural via the NonEmpty operations list.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CommitLogEntry {
    snapshot: SnapshotId,
    operations: NonEmpty<CommitLogOperation>,
}

impl CommitLogEntry {
    pub fn new(snapshot: SnapshotId, operations: NonEmpty<CommitLogOperation>) -> Self {
        Self {
            snapshot,
            operations,
        }
    }

    pub fn single(snapshot: SnapshotId, operation: CommitLogOperation) -> Self {
        Self {
            snapshot,
            operations: NonEmpty::single(operation),
        }
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }

    pub fn operations(&self) -> &NonEmpty<CommitLogOperation> {
        &self.operations
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CommitLogOperation {
    operation: SemaOperation,
    table_name: String,
    key: Option<RecordKey>,
}

impl CommitLogOperation {
    pub fn new(operation: SemaOperation, table: TableName, key: Option<RecordKey>) -> Self {
        Self {
            operation,
            table_name: table.as_str().to_owned(),
            key,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn key(&self) -> Option<&RecordKey> {
        self.key.as_ref()
    }
}
