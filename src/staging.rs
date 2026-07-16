//! Durable staging seam: an engaged staging session buffers the
//! engine's ordinary write surface into one un-committed operation
//! group, parks the built group durably with its prospective head
//! digest, and later either materializes the group atomically or
//! discards it. The seam exists for an external acceptance gate — a
//! caller stages a head-advancing operation group, asks its authorizer
//! to grant the prospective head digest, and only a grant materializes
//! the parked entries; every other verdict discards them, leaving no
//! observable trace.
//!
//! While a session is engaged, the write methods buffer instead of
//! committing — same validation, same receipts, allocation carried
//! forward from the captured base — and the session-scoped read
//! surface ([`crate::Engine::match_records`],
//! [`crate::Engine::match_identified`], the counters and marker)
//! overlays the buffered operations onto committed state, so
//! read-dependent build logic observes exactly the state a direct
//! write sequence would have left. Engagement is an engine-wide mode:
//! the engine's owner serializes head-advancing intake, so exactly one
//! build runs at a time.
//!
//! The parked staging slot is machinery, not acceptance: it is
//! invisible to every read surface, it never survives a refusal, and
//! it is durable for exactly one reason — a group granted but not yet
//! materialized when the process died must still be materializable at
//! reopen, so an occupied slot surfaces through
//! [`crate::Engine::staged_group`] before a daemon opens its intake.
//! One slot: at most one staged group exists at a time.

use std::collections::HashMap;

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_frame::NonEmpty;

use crate::subscribe::SubscriptionRegistry;
use crate::{
    CommitSequence, DeltaKind, EngineStoredValue, EntryDigest, Error, FamilyIdentity,
    IdentifiedTableReference, RecordIdentifier, RecordKey, Result, SnapshotIdentifier,
    StoreSchemaHash, TableReference, VersionedCommitLogEntry, VersionedLogOperation,
    VersionedPayload, VersionedStoreName,
};

const STAGING_SLOT: sema::Table<&'static str, StagedOperationGroup> =
    sema::Table::new("__sema_engine_staging_slot");
const STAGED_GROUP_KEY: &str = "staged_group";

/// One durably parked operation group: the ordered would-be versioned
/// commit log entries a head-advancing operation built, chained from
/// the store's current head, waiting for an external grant, plus the
/// pre-image payloads its retracts removed (for subscription deltas at
/// materialize). The last entry's digest is the prospective head the
/// grant binds. Everything a materialize needs rides here, so a
/// process restart between park and materialize loses nothing.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StagedOperationGroup {
    entries: Vec<VersionedCommitLogEntry>,
    retract_pre_images: Vec<StagedPreImage>,
}

impl StagedOperationGroup {
    pub(crate) fn new(
        entries: Vec<VersionedCommitLogEntry>,
        retract_pre_images: Vec<StagedPreImage>,
    ) -> Self {
        Self {
            entries,
            retract_pre_images,
        }
    }

    pub fn entries(&self) -> &[VersionedCommitLogEntry] {
        &self.entries
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// The digest the staged group's final entry would leave as the
    /// store head — the digest an external grant binds. `None` only
    /// for an empty group, which the park choke point never writes.
    pub fn prospective_head(&self) -> Option<EntryDigest> {
        self.entries
            .last()
            .map(VersionedCommitLogEntry::entry_digest)
    }

    /// The chain head the group was built over: the first entry's
    /// predecessor digest. Materialization requires the store to still
    /// stand exactly here.
    pub fn base_predecessor(&self) -> Option<EntryDigest> {
        self.entries
            .first()
            .and_then(VersionedCommitLogEntry::previous_entry_digest)
    }

    pub fn first_commit_sequence(&self) -> Option<CommitSequence> {
        self.entries
            .first()
            .map(VersionedCommitLogEntry::commit_sequence)
    }

    pub fn last_commit_sequence(&self) -> Option<CommitSequence> {
        self.entries
            .last()
            .map(VersionedCommitLogEntry::commit_sequence)
    }

    pub(crate) fn retract_pre_image(
        &self,
        entry_index: usize,
        operation_index: usize,
    ) -> Option<&[u8]> {
        self.retract_pre_images
            .iter()
            .find(|pre_image| {
                pre_image.entry_index == entry_index as u64
                    && pre_image.operation_index == operation_index as u64
            })
            .map(|pre_image| pre_image.bytes.as_slice())
    }
}

/// The payload a staged retract removed, positioned by entry and
/// operation index within the group, parked so the materialize-time
/// retract delta can carry the removed record exactly as a direct
/// retract's delta does.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StagedPreImage {
    entry_index: u64,
    operation_index: u64,
    bytes: Vec<u8>,
}

impl StagedPreImage {
    pub(crate) fn new(entry_index: u64, operation_index: u64, bytes: Vec<u8>) -> Self {
        Self {
            entry_index,
            operation_index,
            bytes,
        }
    }
}

/// Open-time projection of an occupied staging slot: enough for a
/// daemon's crash-recovery pass to re-ask its authorizer with the
/// staged digest and decide materialize-or-discard, without exposing
/// the parked payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedGroupSummary {
    prospective_head: EntryDigest,
    base_predecessor: Option<EntryDigest>,
    entry_count: usize,
    first_commit_sequence: CommitSequence,
    last_commit_sequence: CommitSequence,
}

impl StagedGroupSummary {
    pub(crate) fn new(
        prospective_head: EntryDigest,
        base_predecessor: Option<EntryDigest>,
        entry_count: usize,
        first_commit_sequence: CommitSequence,
        last_commit_sequence: CommitSequence,
    ) -> Self {
        Self {
            prospective_head,
            base_predecessor,
            entry_count,
            first_commit_sequence,
            last_commit_sequence,
        }
    }

    pub fn prospective_head(&self) -> EntryDigest {
        self.prospective_head
    }

    /// The chain head the group chains from; `None` for a group built
    /// over an empty store.
    pub fn base_predecessor(&self) -> Option<EntryDigest> {
        self.base_predecessor
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub fn first_commit_sequence(&self) -> CommitSequence {
        self.first_commit_sequence
    }

    pub fn last_commit_sequence(&self) -> CommitSequence {
        self.last_commit_sequence
    }
}

/// Receipt from parking a built operation group: the prospective head
/// digest the external grant must bind, plus the staged range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageReceipt {
    prospective_head: EntryDigest,
    entry_count: usize,
    first_commit_sequence: CommitSequence,
    last_commit_sequence: CommitSequence,
}

impl StageReceipt {
    pub(crate) fn new(
        prospective_head: EntryDigest,
        entry_count: usize,
        first_commit_sequence: CommitSequence,
        last_commit_sequence: CommitSequence,
    ) -> Self {
        Self {
            prospective_head,
            entry_count,
            first_commit_sequence,
            last_commit_sequence,
        }
    }

    pub fn prospective_head(&self) -> EntryDigest {
        self.prospective_head
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub fn first_commit_sequence(&self) -> CommitSequence {
        self.first_commit_sequence
    }

    pub fn last_commit_sequence(&self) -> CommitSequence {
        self.last_commit_sequence
    }
}

/// Receipt from materializing the staged group: the head the store now
/// stands at (equal to the staged prospective head by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializeStagedReceipt {
    head: EntryDigest,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    entry_count: usize,
}

impl MaterializeStagedReceipt {
    pub(crate) fn new(
        head: EntryDigest,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
        entry_count: usize,
    ) -> Self {
        Self {
            head,
            commit_sequence,
            snapshot,
            entry_count,
        }
    }

    pub fn head(&self) -> EntryDigest {
        self.head
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }
}

/// Receipt from discarding the staged group: what was cleared. Nothing
/// else changed — the store never observed the discarded operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscardStagedReceipt {
    prospective_head: EntryDigest,
    entry_count: usize,
}

impl DiscardStagedReceipt {
    pub(crate) fn new(prospective_head: EntryDigest, entry_count: usize) -> Self {
        Self {
            prospective_head,
            entry_count,
        }
    }

    pub fn prospective_head(&self) -> EntryDigest {
        self.prospective_head
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }
}

/// The staging plane: a scoped capability over the engine's storage
/// that owns the durable one-slot staging table, exactly as
/// [`crate::commit_log::CommitLog`] and [`crate::outbox::Outbox`] own
/// their planes. Reads and the own-transaction writes open on the
/// borrowed storage; the materialize-time clear writes into a
/// transaction the engine opened and lends in, so clearing the slot
/// stays atomic with the appends it accompanies.
pub(crate) struct Staging<'engine> {
    storage: &'engine sema::Sema,
}

impl<'engine> Staging<'engine> {
    pub(crate) fn new(storage: &'engine sema::Sema) -> Self {
        Self { storage }
    }

    /// The parked group, if the slot is occupied.
    pub(crate) fn slot(&self) -> Result<Option<StagedOperationGroup>> {
        Ok(self
            .storage
            .read(|transaction| STAGING_SLOT.get(transaction, STAGED_GROUP_KEY))?)
    }

    /// Park a group in the slot, in its own small transaction. The
    /// engine checks occupancy under its write lock before calling.
    pub(crate) fn park(&self, group: &StagedOperationGroup) -> Result<()> {
        Ok(self
            .storage
            .write(|transaction| STAGING_SLOT.insert(transaction, STAGED_GROUP_KEY, group))?)
    }

    /// Park a group inside an engine-owned transaction, so a durable
    /// compaction intent and its complete retraction plan appear together.
    pub(crate) fn park_in(
        transaction: &sema::WriteTransaction,
        group: &StagedOperationGroup,
    ) -> sema::Result<()> {
        STAGING_SLOT.insert(transaction, STAGED_GROUP_KEY, group)
    }

    /// Clear the slot in its own small transaction — the discard path.
    pub(crate) fn clear(&self) -> Result<()> {
        Ok(self.storage.write(|transaction| {
            STAGING_SLOT.remove(transaction, STAGED_GROUP_KEY)?;
            Ok(())
        })?)
    }

    /// Clear the slot inside a lent write transaction — the
    /// materialize path, atomic with the entry appends beside it.
    pub(crate) fn clear_in(transaction: &sema::WriteTransaction) -> sema::Result<()> {
        STAGING_SLOT.remove(transaction, STAGED_GROUP_KEY)?;
        Ok(())
    }
}

/// The allocation base a staging session was engaged over: the chain
/// head, counters, and identified-counter starts captured at
/// engagement. The park choke point verifies the base is still current
/// under the engine's write lock; any commit that landed since
/// engagement refuses the stale group, fail-closed.
#[derive(Debug, Clone)]
pub(crate) struct StagingBase {
    chain_head: Option<EntryDigest>,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
    /// (counter key, next-identifier value at capture) for every
    /// identified table the session minted from.
    identified_counters: Vec<(String, u64)>,
}

impl StagingBase {
    pub(crate) fn new(
        chain_head: Option<EntryDigest>,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
    ) -> Self {
        Self {
            chain_head,
            commit_sequence,
            snapshot,
            identified_counters: Vec::new(),
        }
    }

    pub(crate) fn chain_head(&self) -> Option<EntryDigest> {
        self.chain_head
    }

    pub(crate) fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub(crate) fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub(crate) fn identified_counters(&self) -> &[(String, u64)] {
        &self.identified_counters
    }
}

/// Whether the session's own earlier operations left a key present or
/// absent — the overlay a later operation's existence check consults
/// before falling back to committed state, mirroring what sequential
/// direct writes would each have observed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum OverlayPresence {
    Present,
    Absent,
}

/// One buffered overlay effect on a table's rows, in group order:
/// an upsert carrying the encoded payload, or a removal.
pub(crate) enum OverlayEffect<'session> {
    Upsert(&'session [u8]),
    Remove,
}

/// The engaged staging session: the in-memory build state between
/// [`crate::Engine::begin_staged_group`] and the park, abandon, or
/// process end that disengages it. Buffered entries, the validation
/// overlay, pending identifier mints, and retract pre-images all live
/// here; nothing is durable until the park.
pub(crate) struct StagingSession {
    base: StagingBase,
    store_name: VersionedStoreName,
    store_schema_hash: StoreSchemaHash,
    entries: Vec<VersionedCommitLogEntry>,
    latest_commit_sequence: CommitSequence,
    latest_snapshot: SnapshotIdentifier,
    previous_entry_digest: Option<EntryDigest>,
    /// (table name, key string) → what the session's earlier
    /// operations left at that key.
    overlay: HashMap<(String, String), OverlayPresence>,
    /// counter key → the next identifier this session would mint.
    pending_identifiers: HashMap<String, RecordIdentifier>,
    retract_pre_images: Vec<StagedPreImage>,
}

impl StagingSession {
    pub(crate) fn new(
        base: StagingBase,
        store_name: VersionedStoreName,
        store_schema_hash: StoreSchemaHash,
    ) -> Self {
        let latest_commit_sequence = base.commit_sequence();
        let latest_snapshot = base.snapshot();
        let previous_entry_digest = base.chain_head();
        Self {
            base,
            store_name,
            store_schema_hash,
            entries: Vec::new(),
            latest_commit_sequence,
            latest_snapshot,
            previous_entry_digest,
            overlay: HashMap::new(),
            pending_identifiers: HashMap::new(),
            retract_pre_images: Vec::new(),
        }
    }

    pub(crate) fn base(&self) -> &StagingBase {
        &self.base
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// The commit sequence the session stands at: the base plus every
    /// buffered entry — what `current_commit_sequence` reports while
    /// engaged.
    pub(crate) fn engaged_commit_sequence(&self) -> CommitSequence {
        self.latest_commit_sequence
    }

    /// The snapshot the session stands at, mirroring
    /// [`Self::engaged_commit_sequence`].
    pub(crate) fn engaged_snapshot(&self) -> SnapshotIdentifier {
        self.latest_snapshot
    }

    /// Buffer one entry: allocate the next commit sequence and
    /// snapshot exactly as the matching direct write would have, chain
    /// the entry digest from the previous one, and return the
    /// (sequence, snapshot) pair for the caller's receipt.
    pub(crate) fn append_entry(
        &mut self,
        operations: NonEmpty<VersionedLogOperation>,
    ) -> (CommitSequence, SnapshotIdentifier) {
        let commit_sequence = self.latest_commit_sequence.next();
        let snapshot = self.latest_snapshot.next();
        let entry = VersionedCommitLogEntry::new(
            self.store_name.clone(),
            self.store_schema_hash,
            commit_sequence,
            snapshot,
            self.previous_entry_digest,
            operations,
        );
        self.previous_entry_digest = Some(entry.entry_digest());
        self.latest_commit_sequence = commit_sequence;
        self.latest_snapshot = snapshot;
        self.entries.push(entry);
        (commit_sequence, snapshot)
    }

    /// Park a retract's removed payload beside the entry that will be
    /// buffered next (`entry_index =` the current entry count) so the
    /// materialize-time delta carries it.
    pub(crate) fn park_pre_image(&mut self, operation_index: usize, bytes: Vec<u8>) {
        self.retract_pre_images.push(StagedPreImage::new(
            self.entries.len() as u64,
            operation_index as u64,
            bytes,
        ));
    }

    /// What the session's earlier operations say about a key, if
    /// anything; the caller falls back to committed state on `None`.
    pub(crate) fn overlay_presence(
        &self,
        table_name: &str,
        key: &RecordKey,
    ) -> Option<OverlayPresence> {
        self.overlay
            .get(&(table_name.to_owned(), key.to_owned_string()))
            .copied()
    }

    pub(crate) fn set_overlay(
        &mut self,
        table_name: &str,
        key: &RecordKey,
        presence: OverlayPresence,
    ) {
        self.overlay
            .insert((table_name.to_owned(), key.to_owned_string()), presence);
    }

    /// Mint the next record identifier for an identified table:
    /// counter-derived under the captured base, advanced in-session so
    /// consecutive mints are consecutive identifiers. The first mint
    /// per table records the durable counter's captured start in the
    /// base for the park-time currency check.
    pub(crate) fn mint_identifier(
        &mut self,
        counter_key: String,
        durable_next: impl FnOnce() -> Result<RecordIdentifier>,
    ) -> Result<RecordIdentifier> {
        let identifier = match self.pending_identifiers.get(&counter_key) {
            Some(next) => *next,
            None => {
                let first = durable_next()?;
                self.base
                    .identified_counters
                    .push((counter_key.clone(), first.value()));
                first
            }
        };
        self.pending_identifiers
            .insert(counter_key, identifier.next());
        Ok(identifier)
    }

    /// The session's buffered effects on one table, in group order —
    /// the overlay a session-scoped read folds onto committed rows.
    pub(crate) fn overlay_effects_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&RecordKey, OverlayEffect<'_>)> {
        let mut effects = Vec::new();
        for entry in &self.entries {
            for operation in entry.operations() {
                if operation.family().table_name() != table_name {
                    continue;
                }
                let Some(key) = operation.key() else {
                    continue;
                };
                let effect = match operation.payload() {
                    VersionedPayload::Record { bytes } => OverlayEffect::Upsert(bytes),
                    VersionedPayload::Tombstone => OverlayEffect::Remove,
                };
                effects.push((key, effect));
            }
        }
        effects
    }

    /// Consume the session into the durable group to park. Empty when
    /// nothing was buffered — the caller skips the park and reports
    /// nothing staged.
    pub(crate) fn into_group(self) -> StagedOperationGroup {
        StagedOperationGroup::new(self.entries, self.retract_pre_images)
    }
}

/// One-shot affordance to deliver one staged operation's subscription
/// delta after materialization, mirroring [`crate::RowMaterializer`]:
/// minted only by the engine, it carries the delta's coordinates and
/// payload bytes, and the component's [`crate::FamilyDirectory`]
/// supplies only the typed dispatch. Assert and mutate deltas carry
/// the staged record; retract deltas carry the parked pre-image, so
/// every delta matches what the equivalent direct write would have
/// delivered.
pub struct DeltaAnnouncer<'engine> {
    registry: &'engine SubscriptionRegistry,
    kind: DeltaKind,
    family: FamilyIdentity,
    key: RecordKey,
    snapshot: SnapshotIdentifier,
    bytes: Vec<u8>,
}

impl<'engine> DeltaAnnouncer<'engine> {
    pub(crate) fn new(
        registry: &'engine SubscriptionRegistry,
        kind: DeltaKind,
        family: FamilyIdentity,
        key: RecordKey,
        snapshot: SnapshotIdentifier,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            registry,
            kind,
            family,
            key,
            snapshot,
            bytes,
        }
    }

    pub fn family(&self) -> &FamilyIdentity {
        &self.family
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    /// Decode and deliver this delta to a domain-keyed family's
    /// subscribers.
    pub fn announce<RecordValue>(self, table: TableReference<RecordValue>) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.check_table(table.name().as_str())?;
        let record =
            rkyv::from_bytes::<RecordValue, rancor::Error>(&self.bytes).map_err(|source| {
                Error::VersionedPayloadDecode {
                    table: table.name().as_str().to_owned(),
                    message: source.to_string(),
                }
            })?;
        self.registry
            .deliver_delta(self.kind, *table.name(), &self.key, self.snapshot, &record);
        Ok(())
    }

    /// Decode and deliver this delta to an engine-identified family's
    /// subscribers.
    pub fn announce_identified<RecordValue>(
        self,
        table: IdentifiedTableReference<RecordValue>,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.check_table(table.name().as_str())?;
        let record =
            rkyv::from_bytes::<RecordValue, rancor::Error>(&self.bytes).map_err(|source| {
                Error::VersionedPayloadDecode {
                    table: table.name().as_str().to_owned(),
                    message: source.to_string(),
                }
            })?;
        self.registry
            .deliver_delta(self.kind, *table.name(), &self.key, self.snapshot, &record);
        Ok(())
    }

    fn check_table(&self, supplied: &str) -> Result<()> {
        if supplied != self.family.table_name() {
            return Err(Error::MaterializeTableMismatch {
                family: self.family.family().as_str().to_owned(),
                expected: self.family.table_name().to_owned(),
                supplied: supplied.to_owned(),
            });
        }
        Ok(())
    }
}
