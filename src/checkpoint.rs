//! Checkpoints with payload. A checkpoint *digest* verifies a state;
//! a checkpoint *segment* restores one: segments carry the actual
//! sorted (family identity, key, payload) rows of the canonical view,
//! content-addressed by blake3. Checkpoints are derived artifacts of
//! the versioned log — writing one logs no versioned entry, because
//! the log already contains everything the checkpoint folds.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, rancor};

use crate::fold::{ViewDigest, ViewRow};
use crate::{
    CommitSequence, EntryDigest, Error, FamilyIdentity, Result, SnapshotIdentifier,
    StoreSchemaHash, VersionedStoreName,
};

/// Soft byte budget for one checkpoint segment's payload bytes.
/// redb loads each value whole, so one segment per store would force
/// a single arbitrarily large read; chunking at a fixed budget bounds
/// every segment read and write while keeping segment counts low.
/// Splitting is deterministic: walk the sorted view and start a new
/// segment when adding a row would exceed the budget.
pub(crate) const SEGMENT_BYTE_BUDGET: usize = 1024 * 1024;

/// Ordinal of a checkpoint within one store: 1 for the first
/// checkpoint, advancing by one. Distinct from [`CommitSequence`],
/// which counts commits, not checkpoints.
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
pub struct CheckpointSequence(u64);

impl CheckpointSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn first() -> Self {
        Self(1)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Inclusive range of commit sequences whose effects a checkpoint's
/// view contains.
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
pub struct CommitSequenceRange {
    first: CommitSequence,
    last: CommitSequence,
}

impl CommitSequenceRange {
    pub const fn new(first: CommitSequence, last: CommitSequence) -> Self {
        Self { first, last }
    }

    pub const fn first(self) -> CommitSequence {
        self.first
    }

    pub const fn last(self) -> CommitSequence {
        self.last
    }

    pub fn contains(self, sequence: CommitSequence) -> bool {
        self.first <= sequence && sequence <= self.last
    }
}

/// blake3 content address of one checkpoint segment's rows.
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
pub struct SegmentDigest([u8; 32]);

impl SegmentDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for SegmentDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// blake3 digest naming one whole checkpoint: every metadata field
/// plus the ordered segment references. Chains checkpoints through
/// `previous_checkpoint_digest`.
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
pub struct CheckpointDigest([u8; 32]);

impl CheckpointDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for CheckpointDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Durable next-record-identifier state for one engine-identified
/// table, captured into and restored from checkpoint inventories so
/// an imported store never re-mints an identifier the original store
/// already allocated — including identifiers whose records were
/// later retracted.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedCounter {
    table_name: String,
    next_identifier: u64,
}

impl IdentifiedCounter {
    pub fn new(table_name: impl Into<String>, next_identifier: u64) -> Self {
        Self {
            table_name: table_name.into(),
            next_identifier,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn next_identifier(&self) -> u64 {
        self.next_identifier
    }

    /// The engine counter-table key this row restores into.
    pub(crate) fn counter_key(&self) -> String {
        format!(
            "{}:{}",
            self.table_name,
            crate::table::IDENTIFIED_COUNTER_SUFFIX
        )
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        EntryDigest::update_bytes(hasher, self.table_name.as_bytes());
        hasher.update(&self.next_identifier.to_le_bytes());
    }
}

/// The registered family inventory a checkpoint captures: every
/// family identity (with its current table coordinate) plus the
/// identified-counter rows. Import restores this inventory verbatim
/// so the restored catalog and counters match the original store.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FamilyInventory {
    families: Vec<FamilyIdentity>,
    identified_counters: Vec<IdentifiedCounter>,
}

impl FamilyInventory {
    pub fn new(families: Vec<FamilyIdentity>, identified_counters: Vec<IdentifiedCounter>) -> Self {
        Self {
            families,
            identified_counters,
        }
    }

    pub fn families(&self) -> &[FamilyIdentity] {
        &self.families
    }

    pub fn identified_counters(&self) -> &[IdentifiedCounter] {
        &self.identified_counters
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&(self.families.len() as u64).to_le_bytes());
        for family in &self.families {
            family.update_digest(hasher);
        }
        hasher.update(&(self.identified_counters.len() as u64).to_le_bytes());
        for counter in &self.identified_counters {
            counter.update_digest(hasher);
        }
    }
}

/// Reference from checkpoint metadata to one content-addressed
/// segment, in view order.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReference {
    digest: SegmentDigest,
    row_count: u64,
}

impl SegmentReference {
    pub fn new(digest: SegmentDigest, row_count: u64) -> Self {
        Self { digest, row_count }
    }

    pub fn digest(&self) -> SegmentDigest {
        self.digest
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    fn update_digest(&self, hasher: &mut blake3::Hasher) {
        EntryDigest::update_bytes(hasher, self.digest.bytes());
        hasher.update(&self.row_count.to_le_bytes());
    }
}

/// One content-addressed chunk of the canonical sorted view: a
/// consecutive slice of [`ViewRow`]s in view order. The digest a
/// segment is stored and referenced under is computed over exactly
/// these rows.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSegment {
    rows: Vec<ViewRow>,
}

impl CheckpointSegment {
    pub fn new(rows: Vec<ViewRow>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[ViewRow] {
        &self.rows
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn digest(&self) -> SegmentDigest {
        let mut hasher = blake3::Hasher::new();
        EntryDigest::update_bytes(&mut hasher, b"sema-engine-checkpoint-segment-v1");
        hasher.update(&(self.rows.len() as u64).to_le_bytes());
        for row in &self.rows {
            row.update_digest(&mut hasher);
        }
        SegmentDigest::new(*hasher.finalize().as_bytes())
    }

    pub fn reference(&self) -> SegmentReference {
        SegmentReference::new(self.digest(), self.rows.len() as u64)
    }

    /// Chunk a sorted view-row stream into segments at
    /// [`SEGMENT_BYTE_BUDGET`], always keeping at least one row per
    /// segment.
    pub(crate) fn chunk(rows: &[ViewRow]) -> Vec<Self> {
        let mut segments = Vec::new();
        let mut current: Vec<ViewRow> = Vec::new();
        let mut current_bytes = 0usize;
        for row in rows {
            let row_bytes = row.payload().bytes().map_or(0, <[u8]>::len) + row.key().encoded_len();
            if !current.is_empty() && current_bytes + row_bytes > SEGMENT_BYTE_BUDGET {
                segments.push(Self::new(std::mem::take(&mut current)));
                current_bytes = 0;
            }
            current_bytes += row_bytes;
            current.push(row.clone());
        }
        if !current.is_empty() {
            segments.push(Self::new(current));
        }
        segments
    }
}

/// Durable description of one checkpoint. The metadata alone can
/// *verify* a reconstructed state (digests, covered range, schema
/// identity); restoring one takes the referenced segments.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMetadata {
    sequence: CheckpointSequence,
    store_name: VersionedStoreName,
    store_schema_hash: StoreSchemaHash,
    family_inventory: FamilyInventory,
    covered: CommitSequenceRange,
    covered_snapshot: SnapshotIdentifier,
    covered_entry_digest: EntryDigest,
    view_digest: ViewDigest,
    previous_checkpoint_digest: Option<CheckpointDigest>,
    segments: Vec<SegmentReference>,
    checkpoint_digest: CheckpointDigest,
}

impl CheckpointMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sequence: CheckpointSequence,
        store_name: VersionedStoreName,
        store_schema_hash: StoreSchemaHash,
        family_inventory: FamilyInventory,
        covered: CommitSequenceRange,
        covered_snapshot: SnapshotIdentifier,
        covered_entry_digest: EntryDigest,
        view_digest: ViewDigest,
        previous_checkpoint_digest: Option<CheckpointDigest>,
        segments: Vec<SegmentReference>,
    ) -> Self {
        let mut metadata = Self {
            sequence,
            store_name,
            store_schema_hash,
            family_inventory,
            covered,
            covered_snapshot,
            covered_entry_digest,
            view_digest,
            previous_checkpoint_digest,
            segments,
            checkpoint_digest: CheckpointDigest::new([0; 32]),
        };
        metadata.checkpoint_digest = metadata.computed_digest();
        metadata
    }

    pub fn sequence(&self) -> CheckpointSequence {
        self.sequence
    }

    pub fn store_name(&self) -> &VersionedStoreName {
        &self.store_name
    }

    pub fn store_schema_hash(&self) -> StoreSchemaHash {
        self.store_schema_hash
    }

    pub fn family_inventory(&self) -> &FamilyInventory {
        &self.family_inventory
    }

    pub fn covered(&self) -> CommitSequenceRange {
        self.covered
    }

    pub fn covered_snapshot(&self) -> SnapshotIdentifier {
        self.covered_snapshot
    }

    /// Entry digest of the last covered versioned log entry — the
    /// chain head a continuing log suffix must name as its
    /// predecessor.
    pub fn covered_entry_digest(&self) -> EntryDigest {
        self.covered_entry_digest
    }

    pub fn view_digest(&self) -> ViewDigest {
        self.view_digest
    }

    pub fn previous_checkpoint_digest(&self) -> Option<CheckpointDigest> {
        self.previous_checkpoint_digest
    }

    pub fn segments(&self) -> &[SegmentReference] {
        &self.segments
    }

    pub fn checkpoint_digest(&self) -> CheckpointDigest {
        self.checkpoint_digest
    }

    fn computed_digest(&self) -> CheckpointDigest {
        let mut hasher = blake3::Hasher::new();
        EntryDigest::update_bytes(&mut hasher, b"sema-engine-checkpoint-v1");
        hasher.update(&self.sequence.value().to_le_bytes());
        EntryDigest::update_bytes(&mut hasher, self.store_name.as_str().as_bytes());
        EntryDigest::update_bytes(&mut hasher, self.store_schema_hash.bytes());
        self.family_inventory.update_digest(&mut hasher);
        hasher.update(&self.covered.first().value().to_le_bytes());
        hasher.update(&self.covered.last().value().to_le_bytes());
        hasher.update(&self.covered_snapshot.value().to_le_bytes());
        EntryDigest::update_bytes(&mut hasher, self.covered_entry_digest.bytes());
        EntryDigest::update_bytes(&mut hasher, self.view_digest.bytes());
        match self.previous_checkpoint_digest {
            Some(digest) => {
                hasher.update(&[1]);
                EntryDigest::update_bytes(&mut hasher, digest.bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hasher.update(&(self.segments.len() as u64).to_le_bytes());
        for segment in &self.segments {
            segment.update_digest(&mut hasher);
        }
        CheckpointDigest::new(*hasher.finalize().as_bytes())
    }
}

/// One whole checkpoint: the metadata plus its segments in view
/// order. This is the portable restore artifact an import session
/// ingests.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    metadata: CheckpointMetadata,
    segments: Vec<CheckpointSegment>,
}

impl Checkpoint {
    pub fn new(metadata: CheckpointMetadata, segments: Vec<CheckpointSegment>) -> Self {
        Self { metadata, segments }
    }

    pub fn metadata(&self) -> &CheckpointMetadata {
        &self.metadata
    }

    pub fn segments(&self) -> &[CheckpointSegment] {
        &self.segments
    }

    /// Serialize this checkpoint into its portable byte artifact.
    pub fn to_portable(&self) -> Result<PortableCheckpoint> {
        PortableCheckpoint::from_checkpoint(self)
    }

    /// All view rows in view order, concatenated across segments.
    pub fn rows(&self) -> Vec<ViewRow> {
        self.segments
            .iter()
            .flat_map(|segment| segment.rows().iter().cloned())
            .collect()
    }

    /// Verify every content address: the checkpoint digest over the
    /// metadata, each segment against its reference, and the view
    /// digest against the folded segment rows.
    pub fn verify(&self) -> Result<()> {
        let computed = self.metadata.computed_digest();
        if computed != self.metadata.checkpoint_digest {
            return Err(Error::CheckpointDigestMismatch {
                stored: self.metadata.checkpoint_digest,
                computed,
            });
        }
        match self.segments.len().cmp(&self.metadata.segments.len()) {
            std::cmp::Ordering::Less => {
                // The first reference past the supplied count names
                // what is missing; the index is in bounds by
                // construction, so the digest is always a real
                // reference, never fabricated.
                return Err(Error::SegmentMissing {
                    digest: self.metadata.segments[self.segments.len()].digest(),
                });
            }
            std::cmp::Ordering::Greater => {
                return Err(Error::SegmentSurplus {
                    referenced: self.metadata.segments.len(),
                    supplied: self.segments.len(),
                });
            }
            std::cmp::Ordering::Equal => {}
        }
        for (segment, reference) in self.segments.iter().zip(&self.metadata.segments) {
            let computed = segment.digest();
            if computed != reference.digest() {
                return Err(Error::SegmentDigestMismatch {
                    referenced: reference.digest(),
                    computed,
                });
            }
        }
        let view = crate::fold::CanonicalView::fold(&self.rows(), &[], None)?;
        let computed_view = view.digest();
        if computed_view != self.metadata.view_digest {
            return Err(Error::ViewDigestMismatch {
                expected: self.metadata.view_digest,
                computed: computed_view,
            });
        }
        Ok(())
    }
}

/// Owned byte artifact for moving a checkpoint across processes,
/// sockets, or repositories. Decoding always verifies the checkpoint
/// before handing it back to an import path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableCheckpoint {
    bytes: Vec<u8>,
}

impl PortableCheckpoint {
    pub fn from_checkpoint(checkpoint: &Checkpoint) -> Result<Self> {
        let bytes = rkyv::to_bytes::<rancor::Error>(checkpoint).map_err(|source| {
            Error::CheckpointEncode {
                message: source.to_string(),
            }
        })?;
        Ok(Self {
            bytes: bytes.into_vec(),
        })
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn decode(&self) -> Result<Checkpoint> {
        let checkpoint =
            rkyv::from_bytes::<Checkpoint, rancor::Error>(&self.bytes).map_err(|source| {
                Error::CheckpointDecode {
                    message: source.to_string(),
                }
            })?;
        checkpoint.verify()?;
        Ok(checkpoint)
    }
}

/// Typed receipt from [`crate::Engine::checkpoint`]: what was
/// covered, how it is named, and how much payload it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReceipt {
    sequence: CheckpointSequence,
    covered: CommitSequenceRange,
    view_digest: ViewDigest,
    checkpoint_digest: CheckpointDigest,
    segment_count: usize,
    row_count: usize,
}

impl CheckpointReceipt {
    pub(crate) fn new(
        sequence: CheckpointSequence,
        covered: CommitSequenceRange,
        view_digest: ViewDigest,
        checkpoint_digest: CheckpointDigest,
        segment_count: usize,
        row_count: usize,
    ) -> Self {
        Self {
            sequence,
            covered,
            view_digest,
            checkpoint_digest,
            segment_count,
            row_count,
        }
    }

    pub fn sequence(&self) -> CheckpointSequence {
        self.sequence
    }

    pub fn covered(&self) -> CommitSequenceRange {
        self.covered
    }

    pub fn view_digest(&self) -> ViewDigest {
        self.view_digest
    }

    pub fn checkpoint_digest(&self) -> CheckpointDigest {
        self.checkpoint_digest
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }
}
