use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;

use crate::{Catalog, CommitSequence, RecordKey, SnapshotIdentifier, TableName, TableReference};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct VersioningPolicy {
    store_name: VersionedStoreName,
}

impl VersioningPolicy {
    pub fn new(store_name: VersionedStoreName) -> Self {
        Self { store_name }
    }

    pub fn store_name(&self) -> &VersionedStoreName {
        &self.store_name
    }
}

/// The raw-versioned-entry budget a component elects before it requests
/// compaction. A checkpoint preserves the current typed view while compacting
/// entries beyond this budget; it is not an unacknowledged mirror substitute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedHistoryRetention {
    maximum_live_entries: u64,
}

impl VersionedHistoryRetention {
    pub const fn new(maximum_live_entries: u64) -> Self {
        Self {
            maximum_live_entries,
        }
    }

    pub const fn maximum_live_entries(&self) -> u64 {
        self.maximum_live_entries
    }
}

/// The durable acknowledgement that permits a checkpoint-covered prefix to
/// leave the local replay and outbox planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedHistoryAcknowledgement {
    /// No external replay or mirror consumer is configured. The verified local
    /// checkpoint is the complete crash-recovery artifact.
    LocalCheckpoint,
    /// A configured mirror has durably acknowledged this exact history head.
    Mirror(crate::MirrorHead),
}

/// The outcome of a checkpoint-backed version-history compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedHistoryCompaction {
    compacted_entries: u64,
    retained_entries: u64,
    checkpoint_sequence: Option<crate::CheckpointSequence>,
}

impl VersionedHistoryCompaction {
    pub(crate) const fn new(
        compacted_entries: u64,
        retained_entries: u64,
        checkpoint_sequence: Option<crate::CheckpointSequence>,
    ) -> Self {
        Self {
            compacted_entries,
            retained_entries,
            checkpoint_sequence,
        }
    }

    pub const fn compacted_entries(&self) -> u64 {
        self.compacted_entries
    }

    pub const fn retained_entries(&self) -> u64 {
        self.retained_entries
    }

    pub const fn checkpoint_sequence(&self) -> Option<crate::CheckpointSequence> {
        self.checkpoint_sequence
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

/// The schema declaration name of a record family: the stable semantic
/// identity that survives table renames. Replay dispatches on the
/// family, never on the table coordinate.
#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[rkyv(derive(Debug))]
pub struct FamilyName(String);

impl FamilyName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FamilyName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// blake3 content hash of one family's schema declaration: the
/// per-family schema version identity. Supplied as a typed value at
/// registration; schema generation produces it from the `.schema`
/// source.
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
#[rkyv(derive(Debug))]
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

impl std::fmt::Display for SchemaHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Derived store-level schema identity: a domain-separated blake3 hash
/// over the sorted (family, schema hash) inventory of the catalog.
/// Never hand-supplied — table names are excluded so a rename keeps
/// store identity stable.
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
pub struct StoreSchemaHash([u8; 32]);

impl StoreSchemaHash {
    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn from_inventory(mut inventory: Vec<(&FamilyName, SchemaHash)>) -> Self {
        inventory.sort();
        let mut hasher = blake3::Hasher::new();
        EntryDigest::update_bytes(&mut hasher, b"sema-engine-store-schema-hash-v1");
        hasher.update(&(inventory.len() as u64).to_le_bytes());
        for (family, schema_hash) in inventory {
            EntryDigest::update_bytes(&mut hasher, family.as_str().as_bytes());
            EntryDigest::update_bytes(&mut hasher, schema_hash.bytes());
        }
        Self(*hasher.finalize().as_bytes())
    }
}

impl From<&Catalog> for StoreSchemaHash {
    fn from(catalog: &Catalog) -> Self {
        Self::from_inventory(
            catalog
                .registrations()
                .iter()
                .map(|registration| {
                    (
                        registration.identity().family(),
                        registration.identity().schema_hash(),
                    )
                })
                .collect(),
        )
    }
}

impl From<&[FamilyIdentity]> for StoreSchemaHash {
    fn from(families: &[FamilyIdentity]) -> Self {
        Self::from_inventory(
            families
                .iter()
                .map(|identity| (identity.family(), identity.schema_hash()))
                .collect(),
        )
    }
}

impl std::fmt::Display for StoreSchemaHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The full identity of a registered record family: the family name
/// and per-family schema hash carry the durable semantic identity;
/// the table name is only the current storage coordinate.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct FamilyIdentity {
    family: FamilyName,
    schema_hash: SchemaHash,
    table_name: String,
}

impl FamilyIdentity {
    pub fn new(family: FamilyName, schema_hash: SchemaHash, table: TableName) -> Self {
        Self {
            family,
            schema_hash,
            table_name: table.as_str().to_owned(),
        }
    }

    pub fn family(&self) -> &FamilyName {
        &self.family
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Whether two identities name the same family version: identical
    /// family name and per-family schema hash. The table coordinate
    /// is deliberately ignored — this is the replay dispatch relation.
    pub fn shares_family(&self, other: &FamilyIdentity) -> bool {
        self.family == other.family && self.schema_hash == other.schema_hash
    }

    pub(crate) fn update_digest(&self, hasher: &mut blake3::Hasher) {
        EntryDigest::update_bytes(hasher, self.family.as_str().as_bytes());
        EntryDigest::update_bytes(hasher, self.schema_hash.bytes());
        EntryDigest::update_bytes(hasher, self.table_name.as_bytes());
    }

    /// The durable next-record-identifier counter key for this
    /// family's table — the same key [`crate::TableName`] derives, so
    /// a staged identified assert advances exactly the counter the
    /// direct write would have.
    pub(crate) fn identified_counter_key(&self) -> String {
        format!(
            "{}:{}",
            self.table_name,
            crate::table::IDENTIFIED_COUNTER_SUFFIX
        )
    }
}

impl std::fmt::Display for FamilyIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}@{} (table {})",
            self.family, self.schema_hash, self.table_name
        )
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

    pub(crate) fn from_entry_fields(
        store_name: &VersionedStoreName,
        schema_hash: StoreSchemaHash,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        previous_entry_digest: Option<EntryDigest>,
        operations: &NonEmpty<VersionedLogOperation>,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        Self::update_bytes(&mut hasher, b"sema-engine-versioned-commit-log-entry-v2");
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

    pub(crate) fn update_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
}

impl std::fmt::Display for EntryDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
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

    pub(crate) fn update_digest(&self, hasher: &mut blake3::Hasher) {
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
    family: FamilyIdentity,
    key: Option<RecordKey>,
    payload: VersionedPayload,
}

impl VersionedLogOperation {
    pub fn new(
        operation: SemaOperation,
        family: FamilyIdentity,
        key: Option<RecordKey>,
        payload: VersionedPayload,
    ) -> Self {
        Self {
            operation,
            family,
            key,
            payload,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn family(&self) -> &FamilyIdentity {
        &self.family
    }

    /// The table coordinate the operation landed in when it was
    /// logged. Replay must dispatch on [`Self::family`], not on this.
    pub fn table_name(&self) -> &str {
        self.family.table_name()
    }

    pub fn key(&self) -> Option<&RecordKey> {
        self.key.as_ref()
    }

    pub fn payload(&self) -> &VersionedPayload {
        &self.payload
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        EntryDigest::update_bytes(hasher, self.operation.as_record_head().as_bytes());
        self.family.update_digest(hasher);
        match &self.key {
            Some(key) => {
                hasher.update(&[1]);
                key.update_digest(hasher);
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
    schema_hash: StoreSchemaHash,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    previous_entry_digest: Option<EntryDigest>,
    entry_digest: EntryDigest,
    operations: NonEmpty<VersionedLogOperation>,
}

impl VersionedCommitLogEntry {
    pub fn new(
        store_name: VersionedStoreName,
        schema_hash: StoreSchemaHash,
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

    /// The derived store-level schema identity at the time of the
    /// entry — see [`StoreSchemaHash`].
    pub fn schema_hash(&self) -> StoreSchemaHash {
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

/// One replay request: fold versioned log entries into the registered
/// family the table reference names. Operations dispatch on family
/// identity, so entries logged under an earlier table name land in
/// the family's current table.
pub struct VersionedReplay<RecordValue> {
    table: TableReference<RecordValue>,
    entries: Vec<VersionedCommitLogEntry>,
}

impl<RecordValue> VersionedReplay<RecordValue> {
    pub fn new(table: TableReference<RecordValue>, entries: Vec<VersionedCommitLogEntry>) -> Self {
        Self { table, entries }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn entries(&self) -> &[VersionedCommitLogEntry] {
        &self.entries
    }
}

/// Outcome of a versioned replay: how many logged operations applied
/// to the requested family and how many belonged to other families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayReceipt {
    applied: usize,
    skipped: usize,
}

impl ReplayReceipt {
    pub fn new(applied: usize, skipped: usize) -> Self {
        Self { applied, skipped }
    }

    pub fn applied(&self) -> usize {
        self.applied
    }

    pub fn skipped(&self) -> usize {
        self.skipped
    }
}
