use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;

use crate::{CommitSequence, RecordKey, SnapshotIdentifier, TableName};

/// Durable record of one commit: a request that committed all its
/// write effects (or none) under a single [`SnapshotIdentifier`]. A single-operation
/// `assert` / `mutate` / `retract` is just a length-1 commit.
///
/// Note: the spec name in DA/61 §8 + DA/62 §5 is `CommitLogEntry`;
/// `Atomic` is no longer carried at the top level — atomicity is
/// structural via the NonEmpty operations list.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CommitLogEntry {
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    operations: NonEmpty<CommitLogOperation>,
}

impl CommitLogEntry {
    pub fn new(
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        operations: NonEmpty<CommitLogOperation>,
    ) -> Self {
        Self {
            commit_sequence,
            snapshot,
            operations,
        }
    }

    pub fn single(
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        operation: CommitLogOperation,
    ) -> Self {
        Self {
            commit_sequence,
            snapshot,
            operations: NonEmpty::single(operation),
        }
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn operations(&self) -> &NonEmpty<CommitLogOperation> {
        &self.operations
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// A versioned commit log entry projects down to its metadata commit
/// log entry: same sequence and snapshot, payloads dropped. Import
/// uses this to restore the metadata log beside the verbatim
/// versioned suffix.
impl From<&crate::VersionedCommitLogEntry> for CommitLogEntry {
    fn from(entry: &crate::VersionedCommitLogEntry) -> Self {
        let head = CommitLogOperation::from(entry.operations().head());
        let tail = entry
            .operations()
            .tail()
            .iter()
            .map(CommitLogOperation::from)
            .collect();
        Self {
            commit_sequence: entry.commit_sequence(),
            snapshot: entry.snapshot(),
            operations: NonEmpty::from_head_and_tail(head, tail),
        }
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
            table_name: table.into(),
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

/// A versioned log operation projects down to its metadata commit
/// log operation: same operation, table coordinate, and key; the
/// payload drops. The logged coordinate is already a bare string, so
/// the projection constructs the row directly instead of widening the
/// typed public constructor to accept strings.
impl From<&crate::VersionedLogOperation> for CommitLogOperation {
    fn from(operation: &crate::VersionedLogOperation) -> Self {
        Self {
            operation: operation.operation(),
            table_name: operation.family().table_name().to_owned(),
            key: operation.key().cloned(),
        }
    }
}
