//! The fold: the versioned log is the authoritative source of truth,
//! and the canonical view is its fold — the per-key last-write state
//! after applying every logged operation in commit order. Checkpoint
//! segments persist a folded view; rebuild re-derives the materialized
//! tables from it; the view digest names it.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_sema::SemaOperation;

use crate::{
    EngineStoredValue, EntryDigest, Error, FamilyIdentity, FamilyName, IdentifiedTableReference,
    RecordKey, Result, SchemaHash, TableReference, VersionedCommitLogEntry, VersionedLogOperation,
    VersionedPayload,
};

/// Full 32-byte blake3 digest of the canonical sorted view: row count,
/// then every (family name, schema hash, key, payload) in view order.
/// The table coordinate is deliberately excluded — the view is
/// semantic, and a rename keeps the view digest stable, matching
/// [`crate::StoreSchemaHash`] discipline.
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
pub struct ViewDigest([u8; 32]);

impl ViewDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for ViewDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One row of the canonical view: the family identity (current table
/// coordinate included), the record key, and the payload bytes that a
/// fold of the log left at that key. Checkpoint segments persist these
/// rows; [`RowMaterializer`] applies them to the materialized tables.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ViewRow {
    family: FamilyIdentity,
    key: RecordKey,
    payload: VersionedPayload,
}

impl ViewRow {
    pub fn new(family: FamilyIdentity, key: RecordKey, payload: VersionedPayload) -> Self {
        Self {
            family,
            key,
            payload,
        }
    }

    pub fn family(&self) -> &FamilyIdentity {
        &self.family
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn payload(&self) -> &VersionedPayload {
        &self.payload
    }

    pub(crate) fn update_digest(&self, hasher: &mut blake3::Hasher) {
        self.family.update_digest(hasher);
        self.key.update_digest(hasher);
        self.payload.update_digest(hasher);
    }
}

/// The semantic coordinate of one view row: family name, per-family
/// schema hash, record key. Orders the canonical view; deliberately
/// excludes the table coordinate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ViewKey {
    family: FamilyName,
    schema_hash: SchemaHash,
    key: RecordKey,
}

impl ViewKey {
    fn new(family: &FamilyIdentity, key: &RecordKey) -> Self {
        Self {
            family: family.family().clone(),
            schema_hash: family.schema_hash(),
            key: key.clone(),
        }
    }
}

/// The identity coordinate a folded key resolves through: family name plus
/// schema hash, exactly as the checkpoint rows and log entries declare it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ViewIdentityKey {
    family: FamilyName,
    schema_hash: SchemaHash,
}

impl ViewIdentityKey {
    fn of(key: &ViewKey) -> Self {
        Self {
            family: key.family.clone(),
            schema_hash: key.schema_hash,
        }
    }

    fn from_identity(identity: &FamilyIdentity) -> Self {
        Self {
            family: identity.family().clone(),
            schema_hash: identity.schema_hash(),
        }
    }
}

/// The fold state: per-key last-write payloads after applying
/// checkpoint rows and versioned log operations in order, plus the
/// set of every key the fold ever touched (for clearing stale
/// materialized rows on rebuild).
pub(crate) struct CanonicalView {
    rows: BTreeMap<ViewKey, Vec<u8>>,
    touched: BTreeSet<ViewKey>,
    /// Every family identity the folded rows and entries themselves
    /// declared, keyed by its resolution coordinate. The log is
    /// self-describing: a retired identity (a declared prior a family
    /// evolution retracted under) resolves from here even though no
    /// current registration carries it.
    identities: BTreeMap<ViewIdentityKey, FamilyIdentity>,
}

impl CanonicalView {
    pub(crate) fn empty() -> Self {
        Self {
            rows: BTreeMap::new(),
            touched: BTreeSet::new(),
            identities: BTreeMap::new(),
        }
    }

    fn declare_identity(&mut self, identity: &FamilyIdentity) {
        self.identities
            .entry(ViewIdentityKey::from_identity(identity))
            .or_insert_with(|| identity.clone());
    }

    /// Fold checkpoint rows (if any) plus versioned log entries into
    /// the canonical view, verifying the entry digest chain link by
    /// link from `chain_head` (the digest the first applied entry must
    /// name as its predecessor). Each entry digest is recomputed from
    /// the entry's own fields, so a tampered entry cannot ride a
    /// stored digest through the chain — the digest covers the store
    /// name, schema hash, sequence, snapshot, predecessor, and every
    /// operation.
    pub(crate) fn fold(
        checkpoint_rows: &[ViewRow],
        entries: &[VersionedCommitLogEntry],
        chain_head: Option<EntryDigest>,
    ) -> Result<Self> {
        let mut view = Self::empty();
        for row in checkpoint_rows {
            view.apply_checkpoint_row(row)?;
        }
        let mut previous = chain_head;
        for entry in entries {
            let recomputed = EntryDigest::from_entry_fields(
                entry.store_name(),
                entry.schema_hash(),
                entry.commit_sequence(),
                entry.snapshot(),
                entry.previous_entry_digest(),
                entry.operations(),
            );
            if entry.previous_entry_digest() != previous || recomputed != entry.entry_digest() {
                return Err(Error::VersionedChainBroken {
                    sequence: entry.commit_sequence().value(),
                });
            }
            previous = Some(entry.entry_digest());
            for operation in entry.operations() {
                view.apply_operation(operation)?;
            }
        }
        Ok(view)
    }

    fn apply_checkpoint_row(&mut self, row: &ViewRow) -> Result<()> {
        let Some(bytes) = row.payload().bytes() else {
            return Err(Error::ReplayTombstonePayload {
                table: row.family().table_name().to_owned(),
            });
        };
        self.declare_identity(row.family());
        let key = ViewKey::new(row.family(), row.key());
        self.touched.insert(key.clone());
        self.rows.insert(key, bytes.to_vec());
        Ok(())
    }

    fn apply_operation(&mut self, operation: &VersionedLogOperation) -> Result<()> {
        let record_key = operation
            .key()
            .cloned()
            .ok_or_else(|| Error::ReplayMissingKey {
                family: operation.family().family().as_str().to_owned(),
            })?;
        self.declare_identity(operation.family());
        let key = ViewKey::new(operation.family(), &record_key);
        self.touched.insert(key.clone());
        match operation.operation() {
            SemaOperation::Assert | SemaOperation::Mutate => {
                let Some(bytes) = operation.payload().bytes() else {
                    return Err(Error::ReplayTombstonePayload {
                        table: operation.family().table_name().to_owned(),
                    });
                };
                self.rows.insert(key, bytes.to_vec());
            }
            SemaOperation::Retract => {
                self.rows.remove(&key);
            }
            other => {
                return Err(Error::ReplayUnsupportedOperation {
                    operation: other.as_record_head().to_owned(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> ViewDigest {
        let mut hasher = blake3::Hasher::new();
        EntryDigest::update_bytes(&mut hasher, b"sema-engine-view-digest-v1");
        hasher.update(&(self.rows.len() as u64).to_le_bytes());
        for (key, payload) in &self.rows {
            EntryDigest::update_bytes(&mut hasher, key.family.as_str().as_bytes());
            EntryDigest::update_bytes(&mut hasher, key.schema_hash.bytes());
            key.key.update_digest(&mut hasher);
            EntryDigest::update_bytes(&mut hasher, payload);
        }
        ViewDigest::new(*hasher.finalize().as_bytes())
    }

    /// Project the view into materializable rows, resolving each
    /// semantic key against the supplied inventory of family
    /// identities for its current table coordinate. Final rows carry
    /// record payloads; keys the fold touched but did not keep become
    /// tombstone rows that clear stale materialized state.
    pub(crate) fn into_rows(self, inventory: &[FamilyIdentity]) -> Result<MaterializeRows> {
        let Self {
            rows: folded,
            touched,
            identities,
        } = self;
        let mut rows = Vec::with_capacity(folded.len());
        let mut cleared = Vec::new();
        for key in &touched {
            if folded.contains_key(key) {
                continue;
            }
            cleared.push(ViewRow::new(
                Self::resolve_identity(&identities, inventory, key)?,
                key.key.clone(),
                VersionedPayload::tombstone(),
            ));
        }
        for (key, payload) in &folded {
            rows.push(ViewRow::new(
                Self::resolve_identity(&identities, inventory, key)?,
                key.key.clone(),
                VersionedPayload::record(payload.clone()),
            ));
        }
        Ok(MaterializeRows { rows, cleared })
    }

    /// Resolve a folded key's full identity: the fold's own declared
    /// identities first — the checkpoint rows and log entries name every
    /// identity they carry, including priors retired by a family
    /// evolution — then the supplied current inventory. A key in neither
    /// is genuinely unknown.
    fn resolve_identity(
        identities: &BTreeMap<ViewIdentityKey, FamilyIdentity>,
        inventory: &[FamilyIdentity],
        key: &ViewKey,
    ) -> Result<FamilyIdentity> {
        if let Some(identity) = identities.get(&ViewIdentityKey::of(key)) {
            return Ok(identity.clone());
        }
        inventory
            .iter()
            .find(|identity| {
                identity.family() == &key.family && identity.schema_hash() == key.schema_hash
            })
            .cloned()
            .ok_or_else(|| Error::FamilyUnknown {
                family: key.family.as_str().to_owned(),
            })
    }
}

/// The materializable projection of a folded view: the final rows to
/// insert plus the tombstone rows that clear keys the fold touched but
/// did not keep.
pub(crate) struct MaterializeRows {
    rows: Vec<ViewRow>,
    cleared: Vec<ViewRow>,
}

impl MaterializeRows {
    pub(crate) fn rows(&self) -> &[ViewRow] {
        &self.rows
    }

    pub(crate) fn cleared(&self) -> &[ViewRow] {
        &self.cleared
    }

    /// All rows in application order: clears first, then inserts.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &ViewRow> {
        self.cleared.iter().chain(self.rows.iter())
    }
}

/// Typed receipt from [`crate::Engine::rebuild_from_log`]: the digest
/// naming the refolded canonical view and how it materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildReceipt {
    view_digest: ViewDigest,
    row_count: usize,
    cleared_count: usize,
    folded_entry_count: usize,
}

impl RebuildReceipt {
    pub(crate) fn new(
        view_digest: ViewDigest,
        row_count: usize,
        cleared_count: usize,
        folded_entry_count: usize,
    ) -> Self {
        Self {
            view_digest,
            row_count,
            cleared_count,
            folded_entry_count,
        }
    }

    pub fn view_digest(&self) -> ViewDigest {
        self.view_digest
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn cleared_count(&self) -> usize {
        self.cleared_count
    }

    /// How many versioned log entries the fold applied on top of the
    /// checkpoint rows (every logged entry when no checkpoint exists).
    pub fn folded_entry_count(&self) -> usize {
        self.folded_entry_count
    }
}

/// A component's typed knowledge of its record families: which Rust
/// record type materializes each family. The engine drives the fold
/// and owns the transaction; the directory only chooses the type and
/// calls [`RowMaterializer::apply`] or
/// [`RowMaterializer::apply_identified`].
pub trait FamilyDirectory {
    fn materialize(&self, row: RowMaterializer<'_>) -> Result<()>;

    /// Deliver one staged operation's subscription delta after a
    /// staged-group materialization commits, by dispatching the typed
    /// decode exactly as [`Self::materialize`] does. The default
    /// delivers nothing — rebuild and import materializations carry no
    /// live subscribers — so only components that route live writes
    /// through the staging seam implement it.
    fn announce(&self, delta: crate::staging::DeltaAnnouncer<'_>) -> Result<()> {
        let _ = delta;
        Ok(())
    }
}

/// One-shot affordance to land one view row in its materialized
/// table. Minted only by the engine inside its own write transaction;
/// the only writes it affords are the row's own insert (record
/// payload) or remove (tombstone), into the row's own family table —
/// no general write authority escapes the engine's choke points.
pub struct RowMaterializer<'transaction> {
    transaction: &'transaction sema::WriteTransaction,
    row: ViewRow,
}

impl<'transaction> RowMaterializer<'transaction> {
    pub(crate) fn new(transaction: &'transaction sema::WriteTransaction, row: ViewRow) -> Self {
        Self { transaction, row }
    }

    pub fn family(&self) -> &FamilyIdentity {
        &self.row.family
    }

    pub fn key(&self) -> &RecordKey {
        &self.row.key
    }

    /// Apply this row to a domain-keyed family table: decode and
    /// insert a record payload, or remove the key for a tombstone.
    pub fn apply<RecordValue>(self, table: TableReference<RecordValue>) -> Result<()>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.check_table(table.name().as_str())?;
        let crate::RecordKey::Domain(key) = &self.row.key else {
            return Err(Error::MaterializeKeyKindMismatch {
                table: table.name().as_str().to_owned(),
                key: self.row.key.to_owned_string(),
                expected: "domain",
                found: "identifier",
            });
        };
        match &self.row.payload {
            VersionedPayload::Record { bytes } => {
                let record = Self::decode::<RecordValue>(bytes, table.name().as_str())?;
                table
                    .sema_table()
                    .insert(self.transaction, key.clone(), &record)?;
            }
            VersionedPayload::Tombstone => {
                table.sema_table().remove(self.transaction, key.clone())?;
            }
        }
        Ok(())
    }

    /// Apply this row to an engine-identified family table. The row
    /// key is the typed record identifier the engine minted when the
    /// operation was logged.
    pub fn apply_identified<RecordValue>(
        self,
        table: IdentifiedTableReference<RecordValue>,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.check_table(table.name().as_str())?;
        let crate::RecordKey::Identifier(identifier) = &self.row.key else {
            return Err(Error::MaterializeKeyKindMismatch {
                table: table.name().as_str().to_owned(),
                key: self.row.key.to_owned_string(),
                expected: "identifier",
                found: "domain",
            });
        };
        let identifier = *identifier;
        match &self.row.payload {
            VersionedPayload::Record { bytes } => {
                let record = Self::decode::<RecordValue>(bytes, table.name().as_str())?;
                table
                    .sema_table()
                    .insert(self.transaction, identifier.value(), &record)?;
            }
            VersionedPayload::Tombstone => {
                table
                    .sema_table()
                    .remove(self.transaction, identifier.value())?;
            }
        }
        Ok(())
    }

    fn check_table(&self, supplied: &str) -> Result<()> {
        if supplied != self.row.family.table_name() {
            return Err(Error::MaterializeTableMismatch {
                family: self.row.family.family().as_str().to_owned(),
                expected: self.row.family.table_name().to_owned(),
                supplied: supplied.to_owned(),
            });
        }
        Ok(())
    }

    fn decode<RecordValue>(bytes: &[u8], table: &str) -> Result<RecordValue>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        rkyv::from_bytes::<RecordValue, rancor::Error>(bytes).map_err(|source| {
            Error::VersionedPayloadDecode {
                table: table.to_owned(),
                message: source.to_string(),
            }
        })
    }
}
