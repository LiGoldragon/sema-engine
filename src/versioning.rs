use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;

use crate::{CommitSequence, RecordKey, SnapshotIdentifier, TableName};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct VersioningPolicy {
    store_name: VersionedStoreName,
    schema_hash: SchemaHash,
}

impl VersioningPolicy {
    pub fn new(store_name: VersionedStoreName, schema_hash: SchemaHash) -> Self {
        Self {
            store_name,
            schema_hash,
        }
    }

    pub fn store_name(&self) -> &VersionedStoreName {
        &self.store_name
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct VersionedStoreName(String);

impl VersionedStoreName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn for_label(label: impl AsRef<[u8]>) -> Self {
        Self(*blake3::hash(label.as_ref()).as_bytes())
    }

    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct EntryDigest([u8; 32]);

impl EntryDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn from_entry_fields(
        store_name: &VersionedStoreName,
        schema_hash: SchemaHash,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        previous_entry_digest: Option<EntryDigest>,
        operations: &NonEmpty<VersionedLogOperation>,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        Self::update_bytes(&mut hasher, b"sema-engine-versioned-commit-log-entry-v1");
        Self::update_bytes(&mut hasher, store_name.as_str().as_bytes());
        Self::update_bytes(&mut hasher, schema_hash.bytes());
        hasher.update(&commit_sequence.value().to_le_bytes());
        hasher.update(&snapshot.value().to_le_bytes());
        match previous_entry_digest {
            Some(digest) => {
                hasher.update(&[1]);
                Self::update_bytes(&mut hasher, digest.bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        };
        hasher.update(&(operations.len() as u64).to_le_bytes());
        for operation in operations {
            operation.update_digest(&mut hasher);
        }
        Self(*hasher.finalize().as_bytes())
    }

    fn update_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum VersionedPayload {
    Record { bytes: Vec<u8> },
    Tombstone,
}

impl VersionedPayload {
    pub fn record(bytes: Vec<u8>) -> Self {
        Self::Record { bytes }
    }

    pub const fn tombstone() -> Self {
        Self::Tombstone
    }

    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Record { bytes } => Some(bytes),
            Self::Tombstone => None,
        }
    }

    pub const fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Record { bytes } => {
                hasher.update(&[1]);
                EntryDigest::update_bytes(hasher, bytes);
            }
            Self::Tombstone => {
                hasher.update(&[0]);
            }
        };
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct VersionedLogOperation {
    operation: SemaOperation,
    table_name: String,
    key: Option<RecordKey>,
    payload: VersionedPayload,
}

impl VersionedLogOperation {
    pub fn new(
        operation: SemaOperation,
        table_name: TableName,
        key: Option<RecordKey>,
        payload: VersionedPayload,
    ) -> Self {
        Self {
            operation,
            table_name: table_name.as_str().to_owned(),
            key,
            payload,
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

    pub fn payload(&self) -> &VersionedPayload {
        &self.payload
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        EntryDigest::update_bytes(hasher, self.operation.as_record_head().as_bytes());
        EntryDigest::update_bytes(hasher, self.table_name.as_bytes());
        match &self.key {
            Some(key) => {
                hasher.update(&[1]);
                EntryDigest::update_bytes(hasher, key.as_str().as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        };
        self.payload.update_digest(hasher);
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct VersionedCommitLogEntry {
    store_name: VersionedStoreName,
    schema_hash: SchemaHash,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    previous_entry_digest: Option<EntryDigest>,
    entry_digest: EntryDigest,
    operations: NonEmpty<VersionedLogOperation>,
}

impl VersionedCommitLogEntry {
    pub fn new(
        store_name: VersionedStoreName,
        schema_hash: SchemaHash,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        previous_entry_digest: Option<EntryDigest>,
        operations: NonEmpty<VersionedLogOperation>,
    ) -> Self {
        let entry_digest = EntryDigest::from_entry_fields(
            &store_name,
            schema_hash,
            commit_sequence,
            snapshot,
            previous_entry_digest,
            &operations,
        );
        Self {
            store_name,
            schema_hash,
            commit_sequence,
            snapshot,
            previous_entry_digest,
            entry_digest,
            operations,
        }
    }

    pub fn store_name(&self) -> &VersionedStoreName {
        &self.store_name
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn previous_entry_digest(&self) -> Option<EntryDigest> {
        self.previous_entry_digest
    }

    pub fn entry_digest(&self) -> EntryDigest {
        self.entry_digest
    }

    pub fn operations(&self) -> &NonEmpty<VersionedLogOperation> {
        &self.operations
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}
