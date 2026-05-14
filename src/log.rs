use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_core::SemaVerb;

use crate::{RecordKey, SnapshotId, TableName};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct OperationLogEntry {
    snapshot: SnapshotId,
    verb: SemaVerb,
    table_name: String,
    key: Option<RecordKey>,
}

impl OperationLogEntry {
    pub fn new(
        snapshot: SnapshotId,
        verb: SemaVerb,
        table: TableName,
        key: Option<RecordKey>,
    ) -> Self {
        Self {
            snapshot,
            verb,
            table_name: table.as_str().to_owned(),
            key,
        }
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }

    pub fn verb(&self) -> SemaVerb {
        self.verb
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn key(&self) -> Option<&RecordKey> {
        self.key.as_ref()
    }
}
