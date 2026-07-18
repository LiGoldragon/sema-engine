use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema::{Schema, SchemaVersion};
use signal_frame::NonEmpty;
use signal_sema::SemaOperation;

use crate::checkpoint::{
    Checkpoint, CheckpointMetadata, CheckpointReceipt, CheckpointSegment, CheckpointSequence,
    CommitSequenceRange, FamilyInventory, IdentifiedCounter, SegmentReference,
};
use crate::commit_log::{CommitLog, LogCounts};
use crate::compaction::{CompactionFault, CompactionIntent, CompactionPhase};
use crate::fold::{CanonicalView, FamilyDirectory, RebuildReceipt, RowMaterializer, ViewRow};
use crate::import::{ImportReceipt, ImportSession};
use crate::log::{CommitLogEntry, CommitLogOperation};
use crate::outbox::{Durability, MirrorAcknowledgement, MirrorHead, Outbox, OutboxEntry};
use crate::staging::{
    DeltaAnnouncer, DiscardStagedReceipt, MaterializeStagedReceipt, OverlayEffect, OverlayPresence,
    StageReceipt, StagedGroupSummary, Staging, StagingBase, StagingSession,
};
use crate::subscribe::{ActiveSubscription, SubscriptionRegistry};
use crate::{
    Catalog, CommitRequest, DeltaKind, EngineStoredRecord, EngineStoredValue, Error,
    EvolutionStep, FamilyIdentity, IdentifiedAssertion, IdentifiedMutation, IdentifiedMutationReceipt,
    IdentifiedQueryPlan, IdentifiedQuerySnapshot, IdentifiedRecord, IdentifiedRetraction,
    IdentifiedTableDescriptor, IdentifiedTableReference, InitialSnapshot, KeyedAssertion,
    KeyedMutation, QueryPlan, QuerySnapshot, RecordIdentifier, RecordKey, ReplayReceipt, Result,
    Retraction, SequenceRange, SnapshotIdentifier, StoreSchemaHash, SubscriptionHandle,
    SubscriptionIdentifier, SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink,
    TableDescriptor, TableName, TableReference, TableRegistration, VersionedCommitLogEntry,
    VersionedHistoryAcknowledgement, VersionedLogOperation, VersionedPayload,
    VersionedRecoveryTopology, VersionedReplay, VersioningPolicy, WriteOperation,
};

const CATALOG: sema::Table<&'static str, TableRegistration> =
    sema::Table::new("__sema_engine_catalog");
pub(crate) const COUNTERS: sema::Table<&'static str, u64> =
    sema::Table::new("__sema_engine_counters");
pub(crate) const LATEST_COMMIT_SEQUENCE_KEY: &str = "latest_commit_sequence";
pub(crate) const LATEST_SNAPSHOT_KEY: &str = "latest_snapshot";
const NEXT_SUBSCRIPTION_KEY: &str = "next_subscription";
const LATEST_CHECKPOINT_KEY: &str = "latest_checkpoint";
const STORAGE_LAYOUT_KEY: &str = "engine_storage_layout";
/// Engine-internal storage layout version. Layout 2 introduced typed
/// family identity in the catalog and versioned log; layout 3 added
/// the mirror outbox row beside every versioned entry; layout 4 made
/// `RecordKey` carry its domain-key vs identifier kind; layout 5
/// persists the versioned chain-head digest in its own slot so the
/// write path reads the predecessor digest in O(1) instead of scanning
/// the whole versioned log. A layout-4 store that opted into versioning
/// (its versioned log is non-empty) upgrades in place at open: the
/// layout-5 derived slots — CHAIN_HEAD and the log counts — refold from
/// the complete versioned log, verified by recomputation, since slice
/// A's bump was additive over the data tables. A store at an older
/// layout with no versioned log still hard-fails at open and is rebuilt
/// through checkpoint import or versioned replay.
const STORAGE_LAYOUT: u64 = 7;
/// The layout of stores written before the layout slot existed.
const LAYOUT_BEFORE_SLOT: u64 = 1;
const SUBSCRIPTIONS: sema::Table<u64, SubscriptionRegistration> =
    sema::Table::new("__sema_engine_subscriptions");
const IDENTIFIED_COUNTERS: sema::Table<String, u64> =
    sema::Table::new("__sema_engine_identified_counters");
const CHECKPOINTS: sema::Table<u64, CheckpointMetadata> =
    sema::Table::new("__sema_engine_checkpoints");
const CHECKPOINT_SEGMENTS: sema::Table<&'static [u8; 32], CheckpointSegment> =
    sema::Table::new("__sema_engine_checkpoint_segments");
const VERSIONING_POLICY: sema::Table<&'static str, VersioningPolicy> =
    sema::Table::new("__sema_engine_versioning_policy");
const VERSIONING_POLICY_KEY: &str = "configured";
const COMPACTION_INTENT: sema::Table<&'static str, CompactionIntent> =
    sema::Table::new("__sema_engine_compaction_intent");
const COMPACTION_INTENT_KEY: &str = "pending";

pub struct Engine {
    pub(crate) storage: sema::Sema,
    catalog: Catalog,
    subscriptions: SubscriptionRegistry,
    pub(crate) versioning_policy: Option<VersioningPolicy>,
    /// Serializes the engine's own read-compute-write path so two
    /// `&self` callers cannot each read the same chain head, commit
    /// sequence, or duplicate-key state and race their writes. Held
    /// across the entire sequence-allocation → predecessor-digest read →
    /// duplicate check → identifier mint → redb write so each mutation
    /// commits as one serialized unit. This guards the engine's *own*
    /// write path; it is not an `Arc<Mutex>` shared across actors, so
    /// it does not violate the no-shared-mutex rule. `Engine` stays
    /// `Send + Sync`, so an `Arc<Engine>` is safely shared across
    /// threads and writers serialize through this lock.
    write_lock: std::sync::Mutex<()>,
    /// The engaged staging session, if a staged operation group is
    /// being built. Engagement is an engine-wide mode: while `Some`,
    /// every write method buffers into the session instead of
    /// committing, and the session-scoped read surface overlays the
    /// buffered operations onto committed state. The engine's owner
    /// serializes head-advancing intake, so at most one build runs at
    /// a time; the durable staging slot backs this session across a
    /// park, and the session itself is memory only.
    staging: std::sync::Mutex<Option<StagingSession>>,
    /// Test-only interruption is an engine-owned typed hook rather than a
    /// process-global environment switch, making restart recovery deterministic.
    compaction_fault: std::sync::Mutex<Option<CompactionFault>>,
}

impl Engine {
    /// Open a store only when no durable compaction recovery is pending.
    /// Call [`Engine::open_recovering`] from a supervised owner when a family
    /// directory is available to resolve a parked group before serving it.
    pub fn open(request: EngineOpen) -> Result<Self> {
        let engine = Self::open_unrecovered(request)?;
        if let Some(intent) = engine.compaction_intent()? {
            return Err(Error::CompactionRecoveryRequired {
                phase: intent.phase(),
            });
        }
        Ok(engine)
    }

    /// Resolve a durable compaction intent before returning a serving engine.
    /// This is the sole startup surface for components with registered tables.
    pub fn open_recovering(request: EngineOpen, directory: &dyn FamilyDirectory) -> Result<Self> {
        let engine = Self::open_unrecovered(request)?;
        engine.resume_compaction(directory)?;
        if engine.versioning_policy.is_some() {
            engine.compact_configured_versioned_history()?;
        }
        Ok(engine)
    }

    fn open_unrecovered(request: EngineOpen) -> Result<Self> {
        let storage = sema::Sema::open_with_schema(request.path(), request.schema())?;
        // Every validation runs before the first engine write: an open
        // that rejects a store must not mutate the store it rejects.
        let layout_plan = Self::validated_storage_layout(&storage)?;
        let registrations = match storage.read(|transaction| CATALOG.iter(transaction)) {
            Ok(rows) => rows
                .into_iter()
                .map(|(_key, registration)| registration)
                .collect(),
            // A catalog row that no longer decodes is a pre-family-identity
            // registration; surface the layout break, not a byte error.
            Err(sema::Error::RkyvDecode { table, .. }) if table == CATALOG.name() => {
                return Err(Error::StorageLayoutMismatch {
                    stored: LAYOUT_BEFORE_SLOT,
                    expected: STORAGE_LAYOUT,
                });
            }
            Err(other) => return Err(other.into()),
        };
        // Apply the layout plan only after every read-only validation
        // passed: a virgin store gets its layout slot stamped; an older
        // store that opted into versioning refolds the layout-introduced
        // derived slots from its complete log and re-stamps. Open is
        // single-threaded, so this is the only writer and needs no lock.
        Self::apply_layout_plan(&storage, layout_plan)?;
        Self::ensure_durable_versioning_policy(&storage, request.versioning_policy())?;
        let catalog = Catalog::new(registrations);
        Ok(Self {
            storage,
            catalog,
            subscriptions: SubscriptionRegistry::new(),
            versioning_policy: request.versioning_policy().cloned(),
            write_lock: std::sync::Mutex::new(()),
            staging: std::sync::Mutex::new(None),
            compaction_fault: std::sync::Mutex::new(None),
        })
    }

    pub fn register_table<RecordValue>(
        &mut self,
        descriptor: TableDescriptor<RecordValue>,
    ) -> Result<TableReference<RecordValue>>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let name = *descriptor.name();
        match self.family_registration_state(&descriptor.family_identity()) {
            Ok(FamilyRegistration::Existing) => {}
            Ok(FamilyRegistration::New(registration)) => {
                self.storage.write(|transaction| {
                    CATALOG.insert(transaction, name.as_str(), &registration)
                })?;
                self.catalog.insert(registration)?;
            }
            Err(mismatch @ Error::FamilyIdentityMismatch { .. }) => {
                // The catalog names a different identity at this table.
                // A declared prior generation of the same family evolves
                // in place; anything else keeps failing closed with the
                // original mismatch.
                let stored = self.registered_family(&name)?;
                let Some(step) = descriptor.evolution_step_for(&stored).cloned() else {
                    return Err(mismatch);
                };
                self.evolve_registered_family(&descriptor, stored, &step)?;
            }
            Err(other) => return Err(other),
        }
        Ok(TableReference::new(name))
    }

    /// Migrate a registered family in place across a schema-identity
    /// bump: read every row in its stored prior shape through the
    /// declared evolution step, then land the rewritten rows, the
    /// evolved catalog registration, and the log entries in one write
    /// transaction — the catalog can never name a shape the rows do
    /// not have.
    ///
    /// The versioned log records the evolution as row history: one
    /// entry retracting each row under the prior family identity and
    /// asserting its converted successor under the evolved identity,
    /// so a canonical-view fold or rebuild materializes only
    /// current-shape rows and never needs the retired shape. An empty
    /// family evolves as a catalog-only rewrite — there are no rows to
    /// carry and nothing material to log, matching plain registration,
    /// which also writes no log entry.
    ///
    /// Registration happens at daemon startup, before any subscription
    /// can exist, so no deltas are delivered for the carried rows.
    fn evolve_registered_family<RecordValue>(
        &mut self,
        descriptor: &TableDescriptor<RecordValue>,
        stored: FamilyIdentity,
        step: &EvolutionStep<RecordValue>,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let name = *descriptor.name();
        let carried = self
            .storage
            .read(|transaction| step.carry_rows(transaction, name))?;
        // The in-memory catalog evolves first so the store schema hash
        // stamped into the versioned entry names the evolved inventory;
        // a failed persist restores it, keeping memory equal to disk.
        let previous = TableRegistration::new(stored.clone());
        let evolved = TableRegistration::new(descriptor.family_identity());
        self.catalog.evolve(name.as_str(), evolved.clone())?;
        let persisted = self.persist_family_evolution(descriptor, &stored, &carried, &evolved);
        if persisted.is_err() {
            self.catalog.evolve(name.as_str(), previous)?;
        }
        persisted
    }

    fn persist_family_evolution<RecordValue>(
        &self,
        descriptor: &TableDescriptor<RecordValue>,
        stored: &FamilyIdentity,
        carried: &[(String, RecordValue)],
        evolved: &TableRegistration,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let name = *descriptor.name();
        if carried.is_empty() {
            return Ok(self.storage.write(|transaction| {
                CATALOG.insert(transaction, name.as_str(), evolved)
            })?);
        }
        let evolved_identity = evolved.identity().clone();
        let mut log_operations = Vec::with_capacity(carried.len() * 2);
        let mut versioned_operations = Vec::with_capacity(carried.len() * 2);
        for (key, _row) in carried {
            let record_key = crate::RecordKey::new(key.clone());
            log_operations.push(CommitLogOperation::new(
                SemaOperation::Retract,
                name,
                Some(record_key.clone()),
            ));
            if self.versioning_policy.is_some() {
                versioned_operations.push(VersionedLogOperation::new(
                    SemaOperation::Retract,
                    stored.clone(),
                    Some(record_key),
                    VersionedPayload::tombstone(),
                ));
            }
        }
        for (key, row) in carried {
            let record_key = crate::RecordKey::new(key.clone());
            log_operations.push(CommitLogOperation::new(
                SemaOperation::Assert,
                name,
                Some(record_key.clone()),
            ));
            if self.versioning_policy.is_some() {
                versioned_operations.push(VersionedLogOperation::new(
                    SemaOperation::Assert,
                    evolved_identity.clone(),
                    Some(record_key),
                    self.versioned_record_payload(name, row)?,
                ));
            }
        }
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let entry = CommitLogEntry::new(
            commit_sequence,
            snapshot,
            NonEmpty::try_from_vec(log_operations).map_err(|_| Error::EmptyCommit {
                table: name.as_str().to_owned(),
            })?,
        );
        let versioned_entry = if self.versioning_policy.is_some() {
            self.versioned_entry(
                commit_sequence,
                snapshot,
                NonEmpty::try_from_vec(versioned_operations).map_err(|_| Error::EmptyCommit {
                    table: name.as_str().to_owned(),
                })?,
            )?
        } else {
            None
        };
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            for (key, row) in carried {
                sema::Table::<String, RecordValue>::new(name.as_str()).insert(
                    transaction,
                    key.clone(),
                    row,
                )?;
            }
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            CATALOG.insert(transaction, name.as_str(), evolved)?;
            Ok(())
        })?;
        self.maintain_versioned_history()
    }

    pub fn register_identified_table<RecordValue>(
        &mut self,
        descriptor: IdentifiedTableDescriptor<RecordValue>,
    ) -> Result<IdentifiedTableReference<RecordValue>> {
        let name = *descriptor.name();
        match self.family_registration_state(&descriptor.family_identity())? {
            FamilyRegistration::Existing => {}
            FamilyRegistration::New(registration) => {
                let counter_key = name.identified_counter_key();
                self.storage.write(|transaction| {
                    CATALOG.insert(transaction, name.as_str(), &registration)?;
                    IDENTIFIED_COUNTERS.insert(
                        transaction,
                        counter_key,
                        &RecordIdentifier::first().value(),
                    )
                })?;
                self.catalog.insert(registration)?;
            }
        }
        Ok(IdentifiedTableReference::new(name))
    }

    pub fn assert_identified<RecordValue>(
        &self,
        assertion: IdentifiedAssertion<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_identified_registered(assertion.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_assert_identified(session, assertion);
        }
        drop(staging);

        let identifier = self.next_record_identifier(assertion.table())?;
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let key = crate::RecordKey::identifier(identifier);
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Assert,
                *assertion.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_record_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Assert,
            *assertion.table().name(),
            Some(key),
            assertion.record(),
        )?;
        let counter_key = assertion.table().name().identified_counter_key();
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                identifier.value(),
                assertion.record(),
            )?;
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            IDENTIFIED_COUNTERS.insert(transaction, counter_key, &identifier.next().value())?;
            Ok(())
        })?;

        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Assert,
            *assertion.table().name(),
            identifier,
            commit_sequence,
            snapshot,
        ))
    }

    pub fn retract_identified<RecordValue>(
        &self,
        retraction: IdentifiedRetraction<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_identified_registered(retraction.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_retract_identified(session, retraction);
        }
        drop(staging);

        let Some(_record) = self.storage.read(|transaction| {
            retraction
                .table()
                .sema_table()
                .get(transaction, retraction.identifier().value())
        })?
        else {
            return Err(
                self.identified_record_not_found(retraction.table(), retraction.identifier())
            );
        };

        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let key = crate::RecordKey::identifier(retraction.identifier());
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Retract,
                *retraction.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_tombstone_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Retract,
            *retraction.table().name(),
            Some(key),
        )?;
        let counts = self.log_counts()?;
        let removed = self.storage.write(|transaction| {
            let removed = retraction
                .table()
                .sema_table()
                .remove(transaction, retraction.identifier().value())?;
            if removed {
                CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
                self.insert_versioned_entry(
                    transaction,
                    &versioned_entry,
                    counts.next_versioned(),
                )?;
                COUNTERS.insert(
                    transaction,
                    LATEST_COMMIT_SEQUENCE_KEY,
                    &commit_sequence.value(),
                )?;
                COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            }
            Ok(removed)
        })?;
        if !removed {
            return Err(
                self.identified_record_not_found(retraction.table(), retraction.identifier())
            );
        }

        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Retract,
            *retraction.table().name(),
            retraction.identifier(),
            commit_sequence,
            snapshot,
        ))
    }

    pub fn mutate_identified<RecordValue>(
        &self,
        mutation: IdentifiedMutation<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_identified_registered(mutation.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_mutate_identified(session, mutation);
        }
        drop(staging);

        let Some(_record) = self.storage.read(|transaction| {
            mutation
                .table()
                .sema_table()
                .get(transaction, mutation.identifier().value())
        })?
        else {
            return Err(self.identified_record_not_found(mutation.table(), mutation.identifier()));
        };

        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let key = crate::RecordKey::identifier(mutation.identifier());
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Mutate,
                *mutation.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_record_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Mutate,
            *mutation.table().name(),
            Some(key),
            mutation.record(),
        )?;
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            mutation.table().sema_table().insert(
                transaction,
                mutation.identifier().value(),
                mutation.record(),
            )?;
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;

        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Mutate,
            *mutation.table().name(),
            mutation.identifier(),
            commit_sequence,
            snapshot,
        ))
    }

    pub fn assert<RecordValue>(
        &self,
        assertion: crate::Assertion<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.assert_keyed(KeyedAssertion::new(
            *assertion.table(),
            assertion.record_key(),
            assertion.record().clone(),
        ))
    }

    pub fn assert_keyed<RecordValue>(
        &self,
        assertion: KeyedAssertion<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_registered(assertion.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_assert_keyed(session, assertion);
        }
        drop(staging);

        let key = assertion.key().clone();
        if self
            .storage
            .read(|transaction| {
                assertion
                    .table()
                    .sema_table()
                    .get(transaction, key.to_owned_string())
            })?
            .is_some()
        {
            return Err(self.duplicate_assert_key(assertion.table(), &key));
        }

        let record = assertion.record().clone();
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Assert,
                *assertion.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_record_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Assert,
            *assertion.table().name(),
            Some(key.clone()),
            assertion.record(),
        )?;
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                assertion.record(),
            )?;
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;
        self.subscriptions.deliver_delta(
            DeltaKind::Assert,
            *assertion.table().name(),
            &key,
            snapshot,
            &record,
        );

        let receipt = crate::MutationReceipt::new(
            SemaOperation::Assert,
            *assertion.table().name(),
            key,
            commit_sequence,
            snapshot,
        );
        drop(_write_guard);
        self.maintain_versioned_history()?;
        Ok(receipt)
    }

    pub fn mutate<RecordValue>(
        &self,
        mutation: crate::Mutation<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.mutate_keyed(KeyedMutation::new(
            *mutation.table(),
            mutation.record_key(),
            mutation.record().clone(),
        ))
    }

    pub fn mutate_keyed<RecordValue>(
        &self,
        mutation: KeyedMutation<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_registered(mutation.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_mutate_keyed(session, mutation);
        }
        drop(staging);

        let key = mutation.key().clone();
        if self
            .storage
            .read(|transaction| {
                mutation
                    .table()
                    .sema_table()
                    .get(transaction, key.to_owned_string())
            })?
            .is_none()
        {
            return Err(self.record_not_found(mutation.table(), &key));
        }

        let record = mutation.record().clone();
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Mutate,
                *mutation.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_record_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Mutate,
            *mutation.table().name(),
            Some(key.clone()),
            mutation.record(),
        )?;
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            mutation.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                mutation.record(),
            )?;
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;
        self.subscriptions.deliver_delta(
            DeltaKind::Mutate,
            *mutation.table().name(),
            &key,
            snapshot,
            &record,
        );

        let receipt = crate::MutationReceipt::new(
            SemaOperation::Mutate,
            *mutation.table().name(),
            key,
            commit_sequence,
            snapshot,
        );
        drop(_write_guard);
        self.maintain_versioned_history()?;
        Ok(receipt)
    }

    pub fn retract<RecordValue>(
        &self,
        retraction: Retraction<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_registered(retraction.table())?;
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_retract(session, retraction);
        }
        drop(staging);

        let Some(record) = self.storage.read(|transaction| {
            retraction
                .table()
                .sema_table()
                .get(transaction, retraction.key().to_owned_string())
        })?
        else {
            return Err(self.record_not_found(retraction.table(), retraction.key()));
        };

        let key = retraction.key().clone();
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let entry = CommitLogEntry::single(
            commit_sequence,
            snapshot,
            CommitLogOperation::new(
                SemaOperation::Retract,
                *retraction.table().name(),
                Some(key.clone()),
            ),
        );
        let versioned_entry = self.versioned_tombstone_entry(
            commit_sequence,
            snapshot,
            SemaOperation::Retract,
            *retraction.table().name(),
            Some(key.clone()),
        )?;
        let counts = self.log_counts()?;
        let removed = self.storage.write(|transaction| {
            let removed = retraction
                .table()
                .sema_table()
                .remove(transaction, key.to_owned_string())?;
            if removed {
                CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
                self.insert_versioned_entry(
                    transaction,
                    &versioned_entry,
                    counts.next_versioned(),
                )?;
                COUNTERS.insert(
                    transaction,
                    LATEST_COMMIT_SEQUENCE_KEY,
                    &commit_sequence.value(),
                )?;
                COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            }
            Ok(removed)
        })?;
        if !removed {
            return Err(self.record_not_found(retraction.table(), &key));
        }
        self.subscriptions.deliver_delta(
            DeltaKind::Retract,
            *retraction.table().name(),
            &key,
            snapshot,
            &record,
        );

        let receipt = crate::MutationReceipt::new(
            SemaOperation::Retract,
            *retraction.table().name(),
            key,
            commit_sequence,
            snapshot,
        );
        drop(_write_guard);
        self.maintain_versioned_history()?;
        Ok(receipt)
    }

    /// Commit a multi-operation write transaction. Renamed from `atomic`
    /// per DA/62 §5; atomicity is structural via the [`CommitRequest`]
    /// shape.
    pub fn commit<RecordValue>(
        &self,
        request: CommitRequest<RecordValue>,
    ) -> Result<crate::CommitReceipt>
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let _write_guard = self.write_guard();
        self.ensure_registered(request.table())?;
        if request.operations().is_empty() {
            return Err(Error::EmptyCommit {
                table: request.table().name().as_str().to_owned(),
            });
        }
        let mut staging = self.staging_guard();
        if let Some(session) = staging.as_mut() {
            return self.staged_commit(session, request);
        }
        drop(staging);

        let mut effect_keys = HashSet::new();
        let mut effects = Vec::new();
        let mut log_operations = Vec::with_capacity(request.operation_count());
        let mut versioned_operations = Vec::with_capacity(request.operation_count());
        let versioned_family = if self.versioning_policy.is_some() {
            Some(self.registered_family(request.table().name())?)
        } else {
            None
        };
        for operation in request.operations() {
            match operation {
                WriteOperation::Assert(record) => {
                    let key = record.record_key();
                    if !effect_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), &key));
                    }
                    if self
                        .storage
                        .read(|transaction| {
                            request
                                .table()
                                .sema_table()
                                .get(transaction, key.to_owned_string())
                        })?
                        .is_some()
                    {
                        return Err(self.duplicate_assert_key(request.table(), &key));
                    }
                    log_operations.push(CommitLogOperation::new(
                        SemaOperation::Assert,
                        *request.table().name(),
                        Some(key.clone()),
                    ));
                    if let Some(family) = versioned_family.as_ref() {
                        versioned_operations.push(VersionedLogOperation::new(
                            SemaOperation::Assert,
                            family.clone(),
                            Some(key.clone()),
                            self.versioned_record_payload(*request.table().name(), record)?,
                        ));
                    }
                    effects.push(CommittedEffect::new(DeltaKind::Assert, key, record.clone()));
                }
                WriteOperation::Mutate(record) => {
                    let key = record.record_key();
                    if !effect_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), &key));
                    }
                    if self
                        .storage
                        .read(|transaction| {
                            request
                                .table()
                                .sema_table()
                                .get(transaction, key.to_owned_string())
                        })?
                        .is_none()
                    {
                        return Err(self.record_not_found(request.table(), &key));
                    }
                    log_operations.push(CommitLogOperation::new(
                        SemaOperation::Mutate,
                        *request.table().name(),
                        Some(key.clone()),
                    ));
                    if let Some(family) = versioned_family.as_ref() {
                        versioned_operations.push(VersionedLogOperation::new(
                            SemaOperation::Mutate,
                            family.clone(),
                            Some(key.clone()),
                            self.versioned_record_payload(*request.table().name(), record)?,
                        ));
                    }
                    effects.push(CommittedEffect::new(DeltaKind::Mutate, key, record.clone()));
                }
                WriteOperation::Retract(key) => {
                    if !effect_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), key));
                    }
                    let Some(record) = self.storage.read(|transaction| {
                        request
                            .table()
                            .sema_table()
                            .get(transaction, key.to_owned_string())
                    })?
                    else {
                        return Err(self.record_not_found(request.table(), key));
                    };
                    log_operations.push(CommitLogOperation::new(
                        SemaOperation::Retract,
                        *request.table().name(),
                        Some(key.clone()),
                    ));
                    if let Some(family) = versioned_family.as_ref() {
                        versioned_operations.push(VersionedLogOperation::new(
                            SemaOperation::Retract,
                            family.clone(),
                            Some(key.clone()),
                            VersionedPayload::tombstone(),
                        ));
                    }
                    effects.push(CommittedEffect::new(
                        DeltaKind::Retract,
                        key.clone(),
                        record,
                    ));
                }
            }
        }

        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let entry = CommitLogEntry::new(
            commit_sequence,
            snapshot,
            NonEmpty::try_from_vec(log_operations).map_err(|_| Error::EmptyCommit {
                table: request.table().name().as_str().to_owned(),
            })?,
        );
        let versioned_entry = if self.versioning_policy.is_some() {
            self.versioned_entry(
                commit_sequence,
                snapshot,
                NonEmpty::try_from_vec(versioned_operations).map_err(|_| Error::EmptyCommit {
                    table: request.table().name().as_str().to_owned(),
                })?,
            )?
        } else {
            None
        };
        let counts = self.log_counts()?;
        self.storage.write(|transaction| {
            for operation in request.operations() {
                match operation {
                    WriteOperation::Assert(record) | WriteOperation::Mutate(record) => {
                        request.table().sema_table().insert(
                            transaction,
                            record.record_key().to_owned_string(),
                            record,
                        )?;
                    }
                    WriteOperation::Retract(key) => {
                        let _removed = request
                            .table()
                            .sema_table()
                            .remove(transaction, key.to_owned_string())?;
                    }
                }
            }
            CommitLog::append_commit(transaction, &entry, counts.next_commit())?;
            self.insert_versioned_entry(transaction, &versioned_entry, counts.next_versioned())?;
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;

        for effect in &effects {
            self.subscriptions.deliver_delta(
                effect.kind(),
                *request.table().name(),
                effect.key(),
                snapshot,
                effect.record(),
            );
        }

        let receipt = crate::CommitReceipt::new(
            *request.table().name(),
            commit_sequence,
            snapshot,
            request.operation_count(),
        );
        drop(_write_guard);
        self.maintain_versioned_history()?;
        Ok(receipt)
    }

    pub fn match_records<RecordValue>(
        &self,
        query: QueryPlan<RecordValue>,
    ) -> Result<QuerySnapshot<RecordValue>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.ensure_registered(query.table())?;
        let staging = self.staging_guard();
        if let Some(session) = staging.as_ref() {
            return self.staged_match_records(session, query);
        }
        drop(staging);

        let (database_marker, records) = match query.read_plan().node() {
            crate::ReadPlanNode::AllRows => self.storage.read(|transaction| {
                Ok((
                    Self::database_marker_from_values(
                        COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                        COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                    ),
                    query
                        .table()
                        .sema_table()
                        .iter(transaction)?
                        .into_iter()
                        .map(|(_key, record)| record)
                        .collect(),
                ))
            })?,
            crate::ReadPlanNode::ByKey(key) => self.storage.read(|transaction| {
                Ok((
                    Self::database_marker_from_values(
                        COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                        COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                    ),
                    query
                        .table()
                        .sema_table()
                        .get(transaction, key.to_owned_string())?
                        .into_iter()
                        .collect(),
                ))
            })?,
            crate::ReadPlanNode::ByKeyRange(range) => self.storage.read(|transaction| {
                Ok((
                    Self::database_marker_from_values(
                        COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                        COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                    ),
                    query
                        .table()
                        .sema_table()
                        .iter(transaction)?
                        .into_iter()
                        .filter_map(|(key, record)| {
                            if range.contains(&crate::RecordKey::new(key)) {
                                Some(record)
                            } else {
                                None
                            }
                        })
                        .collect(),
                ))
            })?,
            node => {
                return Err(Error::UnsupportedReadPlan {
                    operator: node.operator(),
                });
            }
        };

        Ok(QuerySnapshot::new(
            SemaOperation::Match,
            *query.table().name(),
            database_marker,
            records,
        ))
    }

    pub fn match_identified<RecordValue>(
        &self,
        query: IdentifiedQueryPlan<RecordValue>,
    ) -> Result<IdentifiedQuerySnapshot<RecordValue>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.ensure_identified_registered(query.table())?;
        let staging = self.staging_guard();
        if let Some(session) = staging.as_ref() {
            return self.staged_match_identified(session, query);
        }
        drop(staging);

        let (database_marker, records) = match query.read_plan().node() {
            crate::IdentifiedReadPlanNode::AllRows => self.storage.read(|transaction| {
                Ok((
                    Self::database_marker_from_values(
                        COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                        COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                    ),
                    query
                        .table()
                        .sema_table()
                        .iter(transaction)?
                        .into_iter()
                        .map(|(identifier, record)| {
                            IdentifiedRecord::new(RecordIdentifier::new(identifier), record)
                        })
                        .collect(),
                ))
            })?,
            crate::IdentifiedReadPlanNode::ByIdentifier(identifier) => {
                self.storage.read(|transaction| {
                    Ok((
                        Self::database_marker_from_values(
                            COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                            COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                        ),
                        query
                            .table()
                            .sema_table()
                            .get(transaction, identifier.value())?
                            .map(|record| IdentifiedRecord::new(*identifier, record))
                            .into_iter()
                            .collect(),
                    ))
                })?
            }
            crate::IdentifiedReadPlanNode::ByIdentifierRange(range) => {
                self.storage.read(|transaction| {
                    Ok((
                        Self::database_marker_from_values(
                            COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                            COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
                        ),
                        query
                            .table()
                            .sema_table()
                            .iter(transaction)?
                            .into_iter()
                            .filter_map(|(identifier, record)| {
                                let identifier = RecordIdentifier::new(identifier);
                                if range.contains(identifier) {
                                    Some(IdentifiedRecord::new(identifier, record))
                                } else {
                                    None
                                }
                            })
                            .collect(),
                    ))
                })?
            }
        };

        Ok(IdentifiedQuerySnapshot::new(
            SemaOperation::Match,
            *query.table().name(),
            database_marker,
            records,
        ))
    }

    pub fn validate<RecordValue>(
        &self,
        query: QueryPlan<RecordValue>,
    ) -> Result<crate::ValidationReceipt>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let table = *query.table().name();
        let snapshot = self.match_records(query)?;
        Ok(crate::ValidationReceipt::new(
            SemaOperation::Validate,
            table,
            snapshot.database_marker(),
            snapshot.records().len(),
        ))
    }

    /// The latest snapshot. While a staging session is engaged this
    /// reflects the buffered entries — the snapshot the store would
    /// stand at after materialization — so read-dependent build logic
    /// observes the same values a direct write sequence would have
    /// left.
    pub fn latest_snapshot(&self) -> Result<SnapshotIdentifier> {
        if let Some(session) = self.staging_guard().as_ref() {
            return Ok(session.engaged_snapshot());
        }
        self.committed_snapshot()
    }

    /// The current commit sequence, overlaid with the engaged staging
    /// session's buffered entries exactly as [`Self::latest_snapshot`].
    pub fn current_commit_sequence(&self) -> Result<crate::CommitSequence> {
        if let Some(session) = self.staging_guard().as_ref() {
            return Ok(session.engaged_commit_sequence());
        }
        self.committed_commit_sequence()
    }

    pub fn current_database_marker(&self) -> Result<crate::DatabaseMarker> {
        if let Some(session) = self.staging_guard().as_ref() {
            return Ok(crate::DatabaseMarker::new(
                session.engaged_commit_sequence(),
                session.engaged_snapshot(),
            ));
        }
        Ok(self.storage.read(|transaction| {
            Ok(Self::database_marker_from_values(
                COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
            ))
        })?)
    }

    fn committed_snapshot(&self) -> Result<SnapshotIdentifier> {
        let value = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, LATEST_SNAPSHOT_KEY)?
                .map(SnapshotIdentifier::new)
                .unwrap_or_else(SnapshotIdentifier::genesis))
        })?;
        Ok(value)
    }

    fn committed_commit_sequence(&self) -> Result<crate::CommitSequence> {
        let value = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?
                .map(crate::CommitSequence::new)
                .unwrap_or_else(crate::CommitSequence::genesis))
        })?;
        Ok(value)
    }

    /// The persisted versioned chain-head digest — the entry digest of
    /// the most recent versioned commit log entry, read O(1) from its
    /// own slot. `None` when no versioned entry has been written yet.
    /// This is the cached optimization the write path uses to mint the
    /// next entry's predecessor digest; it is never an integrity
    /// source. A `fold` over the whole log recomputes the same digest
    /// from the entry's fields, which is what verifies the chain.
    pub fn versioned_chain_head(&self) -> Result<Option<crate::EntryDigest>> {
        self.latest_versioned_entry_digest()
    }

    pub fn commit_log(&self) -> Result<Vec<CommitLogEntry>> {
        self.log_plane().entries()
    }

    fn commit_log_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<CommitLogEntry>> {
        self.log_plane().entries_from(start)
    }

    pub fn versioned_commit_log(&self) -> Result<Vec<VersionedCommitLogEntry>> {
        self.log_plane().versioned_entries()
    }

    fn versioned_commit_log_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<VersionedCommitLogEntry>> {
        self.log_plane().versioned_entries_from(start)
    }

    pub fn replay_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<CommitLogEntry>> {
        self.commit_log_from_sequence(start)
    }

    pub fn versioned_replay_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<VersionedCommitLogEntry>> {
        self.versioned_commit_log_from_sequence(start)
    }

    pub fn commit_log_range(&self, range: SequenceRange) -> Result<Vec<CommitLogEntry>> {
        self.log_plane().entries_in(range)
    }

    pub fn subscribe<RecordValue>(
        &self,
        plan: QueryPlan<RecordValue>,
        sink: std::sync::Arc<dyn SubscriptionSink<RecordValue>>,
    ) -> Result<SubscriptionReceipt<RecordValue>>
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let snapshot = self.match_records(plan.clone())?;
        let handle = self.next_subscription_handle(&plan, snapshot.snapshot())?;
        let initial = InitialSnapshot::new(handle, snapshot);
        sink.deliver(crate::SubscriptionEvent::InitialSnapshot(initial.clone()))
            .map_err(|error| Error::SubscriptionSink {
                message: error.message().to_owned(),
            })?;
        self.persist_subscription(handle, plan.filter().clone())?;
        self.subscriptions.add(ActiveSubscription::new(
            handle,
            plan,
            sink,
            self.subscriptions.failure_recorder(),
        ))?;
        Ok(SubscriptionReceipt::new(handle, initial))
    }

    pub fn subscription_fanout_failures(&self) -> Result<Vec<crate::SubscriptionFanoutFailure>> {
        self.subscriptions.fanout_failures()
    }

    pub fn subscription_registrations(&self) -> Result<Vec<SubscriptionRegistration>> {
        Ok(self
            .storage
            .read(|transaction| SUBSCRIPTIONS.iter(transaction))?
            .into_iter()
            .map(|(_key, registration)| registration)
            .collect())
    }

    pub fn list_tables(&self) -> Vec<TableRegistration> {
        self.catalog.registrations().to_vec()
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn storage_path(&self) -> &Path {
        self.storage.path()
    }

    /// Read-only storage-kernel access for transitional component-local
    /// tables. There is no write counterpart: every durable write goes
    /// through the engine's logged choke points, so the commit log
    /// stays complete. [`StorageReader`] carrying no write affordance
    /// is the architectural witness.
    pub fn storage_reader(&self) -> StorageReader<'_> {
        StorageReader::new(&self.storage)
    }

    /// Mint the hash-chained log plane over the engine's storage: the
    /// scoped capability that owns the commit-log tables, the cached
    /// chain head, the row counts, and the single append choke point.
    /// Like [`StorageReader`], it borrows the storage for its reads; its
    /// append methods write only their own rows into a write transaction
    /// the engine opens and lends in.
    fn log_plane(&self) -> CommitLog<'_> {
        CommitLog::new(&self.storage)
    }

    /// Mint the mirror-outbox plane over the engine's storage: the
    /// scoped capability that owns the outbox rows, the shipped cursor,
    /// and the durability classification, with a `record` choke point
    /// that writes only the outbox row into a lent write transaction.
    fn outbox_plane(&self) -> Outbox<'_> {
        Outbox::new(&self.storage)
    }

    /// The derived store-level schema identity over the current family
    /// inventory — the value stamped into versioned commit log entries.
    pub fn store_schema_hash(&self) -> StoreSchemaHash {
        StoreSchemaHash::from(&self.catalog)
    }

    /// Fold versioned log entries into the registered family named by
    /// the replay's table reference. Operations dispatch on family
    /// identity — family name plus per-family schema hash — so entries
    /// logged under an earlier table name land in the family's current
    /// table. Application goes through the public write choke points,
    /// so the rebuilt store logs its own complete history.
    pub fn replay_versioned<RecordValue>(
        &self,
        replay: VersionedReplay<RecordValue>,
    ) -> Result<ReplayReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.ensure_registered(replay.table())?;
        let registered = self.registered_family(replay.table().name())?;
        let mut applied = 0;
        let mut skipped = 0;
        for entry in replay.entries() {
            for operation in entry.operations() {
                if !operation.family().shares_family(&registered) {
                    skipped += 1;
                    continue;
                }
                let key = operation
                    .key()
                    .cloned()
                    .ok_or_else(|| Error::ReplayMissingKey {
                        family: registered.family().as_str().to_owned(),
                    })?;
                match operation.operation() {
                    SemaOperation::Assert => {
                        self.assert_keyed(KeyedAssertion::new(
                            *replay.table(),
                            key,
                            self.replayed_record(replay.table(), operation.payload())?,
                        ))?;
                    }
                    SemaOperation::Mutate => {
                        self.mutate_keyed(KeyedMutation::new(
                            *replay.table(),
                            key,
                            self.replayed_record(replay.table(), operation.payload())?,
                        ))?;
                    }
                    SemaOperation::Retract => {
                        self.retract(Retraction::new(*replay.table(), key))?;
                    }
                    other => {
                        return Err(Error::ReplayUnsupportedOperation {
                            operation: other.as_record_head().to_owned(),
                        });
                    }
                }
                applied += 1;
            }
        }
        Ok(ReplayReceipt::new(applied, skipped))
    }

    fn replayed_record<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        payload: &VersionedPayload,
    ) -> Result<RecordValue>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let Some(bytes) = payload.bytes() else {
            return Err(Error::ReplayTombstonePayload {
                table: table.name().as_str().to_owned(),
            });
        };
        rkyv::from_bytes::<RecordValue, rancor::Error>(bytes).map_err(|source| {
            Error::VersionedPayloadDecode {
                table: table.name().as_str().to_owned(),
                message: source.to_string(),
            }
        })
    }

    /// Write a checkpoint with payload: fold the versioned log (on
    /// top of the latest checkpoint, when one exists) into the
    /// canonical view, chunk the sorted rows into content-addressed
    /// segments, and persist metadata plus segments durably in one
    /// write transaction.
    ///
    /// A checkpoint is a derived artifact of the versioned log — it
    /// logs no versioned entry and advances no commit sequence. The
    /// log already contains everything the checkpoint folds; logging
    /// the fold would make history describe itself.
    pub fn checkpoint(&self) -> Result<CheckpointReceipt> {
        let _write_guard = self.write_guard();
        let policy = self
            .versioning_policy
            .as_ref()
            .ok_or_else(|| Error::versioning_not_enabled("checkpoint"))?;
        self.ensure_versioned_log_complete()?;
        let previous = self.latest_checkpoint()?;
        let (previous_rows, previous_metadata) = match previous {
            Some(checkpoint) => (checkpoint.rows(), Some(checkpoint.metadata().clone())),
            None => (Vec::new(), None),
        };
        let after = previous_metadata
            .as_ref()
            .map(|metadata| metadata.covered().last())
            .unwrap_or_else(crate::CommitSequence::genesis);
        let entries = self.versioned_commit_log_from_sequence(after.next())?;
        let (Some(first_entry), Some(last_entry)) = (entries.first(), entries.last()) else {
            return Err(Error::CheckpointNothingToCover);
        };
        let chain_head = previous_metadata
            .as_ref()
            .map(CheckpointMetadata::covered_entry_digest);
        let covered = CommitSequenceRange::new(
            previous_metadata
                .as_ref()
                .map(|metadata| metadata.covered().first())
                .unwrap_or_else(|| first_entry.commit_sequence()),
            last_entry.commit_sequence(),
        );
        let covered_snapshot = last_entry.snapshot();
        let covered_entry_digest = last_entry.entry_digest();
        let sequence = previous_metadata
            .as_ref()
            .map(|metadata| metadata.sequence().next())
            .unwrap_or_else(CheckpointSequence::first);
        let previous_checkpoint_digest = previous_metadata
            .as_ref()
            .map(CheckpointMetadata::checkpoint_digest);

        let view = CanonicalView::fold(&previous_rows, &entries, chain_head)?;
        let view_digest = view.digest();
        let inventory_families = self.family_inventory();
        let materialize = view.into_rows(&inventory_families)?;
        let segments = CheckpointSegment::chunk(materialize.rows());
        let references: Vec<SegmentReference> =
            segments.iter().map(CheckpointSegment::reference).collect();
        let row_count = materialize.rows().len();

        let metadata = CheckpointMetadata::new(
            sequence,
            policy.store_name().clone(),
            self.store_schema_hash(),
            FamilyInventory::new(inventory_families, self.identified_counter_inventory()?),
            covered,
            covered_snapshot,
            covered_entry_digest,
            view_digest,
            previous_checkpoint_digest,
            references,
        );
        let checkpoint_digest = metadata.checkpoint_digest();
        self.storage.write(|transaction| {
            CHECKPOINTS.insert(transaction, sequence.value(), &metadata)?;
            for segment in &segments {
                let digest = segment.digest();
                CHECKPOINT_SEGMENTS.insert(transaction, digest.bytes(), segment)?;
            }
            COUNTERS.insert(transaction, LATEST_CHECKPOINT_KEY, &sequence.value())
        })?;

        Ok(CheckpointReceipt::new(
            sequence,
            covered,
            view_digest,
            checkpoint_digest,
            segments.len(),
            row_count,
        ))
    }

    /// Load the latest stored checkpoint — metadata plus segments —
    /// verifying every content address before returning it. This is
    /// the portable restore artifact an [`ImportSession`] ingests on
    /// the receiving side.
    pub fn latest_checkpoint(&self) -> Result<Option<Checkpoint>> {
        let Some(sequence) = self
            .storage
            .read(|transaction| COUNTERS.get(transaction, LATEST_CHECKPOINT_KEY))?
        else {
            return Ok(None);
        };
        let metadata = self
            .storage
            .read(|transaction| CHECKPOINTS.get(transaction, sequence))?
            .ok_or(Error::CheckpointRowMissing { sequence })?;
        let mut segments = Vec::with_capacity(metadata.segments().len());
        for reference in metadata.segments() {
            let digest = reference.digest();
            let segment = self
                .storage
                .read(|transaction| CHECKPOINT_SEGMENTS.get(transaction, digest.bytes()))?
                .ok_or(Error::SegmentMissing { digest })?;
            segments.push(segment);
        }
        let checkpoint = Checkpoint::new(metadata, segments);
        checkpoint.verify()?;
        Ok(Some(checkpoint))
    }

    /// Compact a verified checkpoint-covered history prefix once the caller
    /// supplies the acknowledgement topology that owns replay recovery. The
    /// method never deletes an unacknowledged mirror outbox row: a configured
    /// mirror must acknowledge at least the checkpoint head, while a component
    /// with no external replay consumer explicitly names its verified local
    /// checkpoint as the recovery acknowledgement.
    pub fn compact_versioned_history(
        &self,
        retention: crate::VersionedHistoryRetention,
        acknowledgement: VersionedHistoryAcknowledgement,
    ) -> Result<crate::VersionedHistoryCompaction> {
        self.validate_history_acknowledgement(acknowledgement)?;
        let retained_before_checkpoint = self.versioned_commit_log()?.len() as u64;
        if retained_before_checkpoint <= retention.maximum_live_entries() {
            return Ok(crate::VersionedHistoryCompaction::new(
                0,
                retained_before_checkpoint,
                self.latest_checkpoint()?
                    .map(|checkpoint| checkpoint.metadata().sequence()),
            ));
        }

        // Checkpoint first, in its own durable transaction. A previously
        // published checkpoint already covering the current head is reused;
        // this is the restart path after a durable compaction intent published
        // and verified it but crashed before advancing the history floor.
        let current = self.current_commit_sequence()?;
        let checkpoint = match self.latest_checkpoint()? {
            Some(checkpoint) if checkpoint.metadata().covered().last() >= current => checkpoint,
            _ => {
                self.checkpoint()?;
                self.latest_checkpoint()?
                    .expect("checkpoint just persisted by Engine::checkpoint")
            }
        };
        let covered = checkpoint.metadata().covered().last();
        if let VersionedHistoryAcknowledgement::Mirror(head) = acknowledgement {
            if head.commit_sequence() < covered {
                return Err(Error::HistoryCompactionUnacknowledged {
                    required: covered.value(),
                    acknowledged: Some(head.commit_sequence().value()),
                });
            }
            let durable = self
                .mirror_head()?
                .map(|stored| stored.commit_sequence().value());
            if durable.is_none_or(|sequence| sequence < covered.value()) {
                return Err(Error::HistoryCompactionUnacknowledged {
                    required: covered.value(),
                    acknowledged: durable,
                });
            }
        }

        let _write_guard = self.write_guard();
        let log = self.log_plane();
        let (commit_keys, versioned_keys) = log.keys_through(covered)?;
        let counts = log
            .counts()?
            .after_removing(commit_keys.len(), versioned_keys.len());
        let outbox_keys = self.outbox_plane().keys_through(covered)?;
        let metadata = checkpoint.metadata();
        let root_checkpoint = CheckpointMetadata::new(
            metadata.sequence(),
            metadata.store_name().clone(),
            metadata.store_schema_hash(),
            metadata.family_inventory().clone(),
            metadata.covered(),
            metadata.covered_snapshot(),
            metadata.covered_entry_digest(),
            metadata.view_digest(),
            None,
            metadata.segments().to_vec(),
        );
        let checkpoint_keys: Vec<u64> = self
            .storage
            .read(|transaction| CHECKPOINTS.iter(transaction))?
            .into_iter()
            .map(|(sequence, _metadata)| sequence)
            .filter(|sequence| *sequence != metadata.sequence().value())
            .collect();
        let retained_segments: std::collections::BTreeSet<[u8; 32]> = metadata
            .segments()
            .iter()
            .map(|reference| *reference.digest().bytes())
            .collect();
        let segment_keys: Vec<[u8; 32]> = self
            .storage
            .read(|transaction| CHECKPOINT_SEGMENTS.iter(transaction))?
            .into_iter()
            .map(|(digest, _segment)| digest)
            .filter(|digest| !retained_segments.contains(digest))
            .collect();
        self.storage.write(|transaction| {
            CommitLog::compact_through(transaction, &commit_keys, &versioned_keys, counts)?;
            Outbox::compact_acknowledged_through(transaction, &outbox_keys)?;
            for sequence in &checkpoint_keys {
                CHECKPOINTS.remove(transaction, *sequence)?;
            }
            for digest in &segment_keys {
                CHECKPOINT_SEGMENTS.remove(transaction, digest)?;
            }
            CHECKPOINTS.insert(transaction, metadata.sequence().value(), &root_checkpoint)
        })?;
        Ok(crate::VersionedHistoryCompaction::new(
            retained_before_checkpoint.saturating_sub(counts.versioned()),
            counts.versioned(),
            Some(root_checkpoint.sequence()),
        ))
    }

    /// Run the durable store policy at a lifecycle boundary. Components call
    /// this after successful writes or from their supervised maintenance tick;
    /// the normal policy is finite, so raw replay history never relies on a
    /// disabled or unbounded default.
    pub fn compact_configured_versioned_history(
        &self,
    ) -> Result<crate::VersionedHistoryCompaction> {
        let policy = self
            .versioning_policy
            .as_ref()
            .ok_or_else(|| Error::versioning_not_enabled("compact_configured_versioned_history"))?;
        let retained = self.versioned_commit_log()?.len() as u64;
        if retained <= policy.retention().maximum_live_entries() {
            return Ok(crate::VersionedHistoryCompaction::new(
                0,
                retained,
                self.latest_checkpoint()?
                    .map(|checkpoint| checkpoint.metadata().sequence()),
            ));
        }
        let acknowledgement = match policy.recovery_topology() {
            VersionedRecoveryTopology::LocalCheckpoint => {
                VersionedHistoryAcknowledgement::LocalCheckpoint
            }
            VersionedRecoveryTopology::Mirror => self
                .mirror_head()?
                .map(VersionedHistoryAcknowledgement::Mirror)
                .ok_or(Error::HistoryCompactionUnacknowledged {
                    required: self.current_commit_sequence()?.value(),
                    acknowledged: None,
                })?,
        };
        self.compact_versioned_history(policy.retention(), acknowledgement)
    }

    /// Apply the finite persisted replay policy after a successful normal
    /// write. A store without durable versioning has no raw history to trim.
    pub(crate) fn maintain_versioned_history(&self) -> Result<()> {
        if self.versioning_policy.is_some() {
            self.compact_configured_versioned_history()?;
        }
        Ok(())
    }

    fn validate_history_acknowledgement(
        &self,
        acknowledgement: VersionedHistoryAcknowledgement,
    ) -> Result<()> {
        let policy = self
            .versioning_policy
            .as_ref()
            .ok_or_else(|| Error::versioning_not_enabled("compact_versioned_history"))?;
        match (policy.recovery_topology(), acknowledgement) {
            (
                VersionedRecoveryTopology::LocalCheckpoint,
                VersionedHistoryAcknowledgement::LocalCheckpoint,
            ) => {
                if let Some(entry) = self.unshipped_outbox()?.last() {
                    return Err(Error::HistoryCompactionUnacknowledged {
                        required: entry.commit_sequence().value(),
                        acknowledged: self
                            .mirror_head()?
                            .map(|head| head.commit_sequence().value()),
                    });
                }
                Ok(())
            }
            (VersionedRecoveryTopology::Mirror, VersionedHistoryAcknowledgement::Mirror(_)) => {
                Ok(())
            }
            (
                VersionedRecoveryTopology::LocalCheckpoint,
                VersionedHistoryAcknowledgement::Mirror(_),
            ) => Err(Error::HistoryCompactionTopologyMismatch {
                topology: "local-checkpoint",
                acknowledgement: "mirror",
            }),
            (
                VersionedRecoveryTopology::Mirror,
                VersionedHistoryAcknowledgement::LocalCheckpoint,
            ) => Err(Error::HistoryCompactionTopologyMismatch {
                topology: "mirror",
                acknowledgement: "local-checkpoint",
            }),
        }
    }

    /// Rebuild the materialized family tables from the authoritative
    /// versioned log: the fold *is* the definition of the view. Folds
    /// the latest checkpoint's rows (when one exists) plus the log
    /// suffix, then re-materializes every folded row in one write
    /// transaction — tombstones first for every key the fold touched
    /// but did not keep, then the final rows. Since every durable
    /// write goes through the logged choke points, the touched-key
    /// set covers every key a materialized table can legally hold.
    ///
    /// Materialization writes tables directly inside the engine's own
    /// transaction; it does not route through assert/mutate, so the
    /// rebuild logs nothing and the log remains the single history.
    pub fn rebuild_from_log(&self, directory: &dyn FamilyDirectory) -> Result<RebuildReceipt> {
        let _write_guard = self.write_guard();
        if self.versioning_policy.is_none() {
            return Err(Error::versioning_not_enabled("rebuild_from_log"));
        }
        self.ensure_versioned_log_complete()?;
        let checkpoint = self.latest_checkpoint()?;
        let (checkpoint_rows, chain_head, after) = match &checkpoint {
            Some(checkpoint) => (
                checkpoint.rows(),
                Some(checkpoint.metadata().covered_entry_digest()),
                checkpoint.metadata().covered().last(),
            ),
            None => (Vec::new(), None, crate::CommitSequence::genesis()),
        };
        let entries = self.versioned_commit_log_from_sequence(after.next())?;
        let view = CanonicalView::fold(&checkpoint_rows, &entries, chain_head)?;
        let view_digest = view.digest();
        let materialize = view.into_rows(&self.family_inventory())?;
        self.storage.write(|transaction| {
            for row in materialize.iter() {
                directory
                    .materialize(RowMaterializer::new(transaction, row.clone()))
                    .map_err(Error::into_storage)?;
            }
            Ok(())
        })?;
        Ok(RebuildReceipt::new(
            view_digest,
            materialize.rows().len(),
            materialize.cleared().len(),
            entries.len(),
        ))
    }

    /// Mint the engine-owned import session for restoring a fresh
    /// store from a checkpoint plus versioned-log suffix. The session
    /// is the only path to the import surface, and while it lives it
    /// exclusively borrows the engine — ordinary mutation handlers
    /// are structurally unable to reach or interleave with it.
    pub fn begin_import(&mut self) -> Result<ImportSession<'_>> {
        if self.versioning_policy.is_none() {
            return Err(Error::versioning_not_enabled("import"));
        }
        self.ensure_fresh_for_import()?;
        Ok(ImportSession::new(self))
    }

    pub(crate) fn apply_import(
        &mut self,
        checkpoint: Checkpoint,
        suffix: Vec<VersionedCommitLogEntry>,
        directory: &dyn FamilyDirectory,
    ) -> Result<ImportReceipt> {
        let policy = self
            .versioning_policy
            .as_ref()
            .ok_or_else(|| Error::versioning_not_enabled("import"))?;
        let writes_mirror_outbox = policy.recovery_topology() == VersionedRecoveryTopology::Mirror;
        let metadata = checkpoint.metadata().clone();
        if metadata.store_name() != policy.store_name() {
            return Err(Error::ImportStoreNameMismatch {
                checkpoint: metadata.store_name().as_str().to_owned(),
                policy: policy.store_name().as_str().to_owned(),
            });
        }
        // The stamped store schema hash must derive from the carried
        // family inventory; a doctored artifact cannot smuggle a
        // mismatched identity past the digest over both.
        let derived = StoreSchemaHash::from(metadata.family_inventory().families());
        if derived != metadata.store_schema_hash() {
            return Err(Error::CheckpointSchemaMismatch {
                checkpoint: metadata.store_schema_hash(),
                current: derived,
            });
        }
        self.ensure_fresh_for_import()?;

        // Fold checkpoint rows plus suffix, verifying the recomputed
        // digest chain from the checkpoint's covered head.
        let view = CanonicalView::fold(
            &checkpoint.rows(),
            &suffix,
            Some(metadata.covered_entry_digest()),
        )?;
        let view_digest = view.digest();
        let materialize = view.into_rows(metadata.family_inventory().families())?;
        let (commit_sequence, snapshot) = match suffix.last() {
            Some(entry) => (entry.commit_sequence(), entry.snapshot()),
            None => (metadata.covered().last(), metadata.covered_snapshot()),
        };
        let registrations: Vec<TableRegistration> = metadata
            .family_inventory()
            .families()
            .iter()
            .map(|identity| TableRegistration::new(identity.clone()))
            .collect();

        self.storage.write(|transaction| {
            for registration in &registrations {
                CATALOG.insert(transaction, registration.table_name(), registration)?;
            }
            for counter in metadata.family_inventory().identified_counters() {
                IDENTIFIED_COUNTERS.insert(
                    transaction,
                    counter.counter_key(),
                    &counter.next_identifier(),
                )?;
            }
            CHECKPOINTS.insert(transaction, metadata.sequence().value(), &metadata)?;
            for segment in checkpoint.segments() {
                let digest = segment.digest();
                CHECKPOINT_SEGMENTS.insert(transaction, digest.bytes(), segment)?;
            }
            COUNTERS.insert(
                transaction,
                LATEST_CHECKPOINT_KEY,
                &metadata.sequence().value(),
            )?;
            // The store is fresh (verified above), so both counts start
            // at zero and advance one per restored entry; after the loop
            // each equals the suffix length and the two stay equal.
            for (index, entry) in suffix.iter().enumerate() {
                let count = index as u64 + 1;
                CommitLog::append_versioned(transaction, entry, count)?;
                if writes_mirror_outbox {
                    Outbox::record(transaction, entry)?;
                }
                CommitLog::append_commit(transaction, &CommitLogEntry::from(entry), count)?;
            }
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &commit_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            for row in materialize.iter() {
                directory
                    .materialize(RowMaterializer::new(transaction, row.clone()))
                    .map_err(Error::into_storage)?;
            }
            Ok(())
        })?;
        for registration in registrations {
            self.catalog.insert(registration)?;
        }

        Ok(ImportReceipt::new(
            metadata.covered(),
            suffix.len(),
            view_digest,
            materialize.rows().len(),
            commit_sequence,
            snapshot,
        ))
    }

    /// The unshipped outbox suffix: every outbox row past the durable
    /// shipped cursor, in commit order. A mirror actor loads the
    /// matching versioned entries through
    /// [`Self::versioned_replay_from_sequence`] and ships those.
    pub fn unshipped_outbox(&self) -> Result<Vec<OutboxEntry>> {
        self.outbox_plane().unshipped()
    }

    /// The durable shipped cursor: the last server-confirmed mirror
    /// head, if any acknowledgement has landed.
    pub fn mirror_head(&self) -> Result<Option<MirrorHead>> {
        self.outbox_plane().mirror_head()
    }

    /// Record a server-confirmed mirror head, advancing the durable
    /// shipped cursor. Idempotent: a head at or behind the cursor is
    /// a typed no-op. A head naming a sequence with no outbox row is
    /// [`Error::MirrorHeadUnknown`]; a head whose digest disagrees
    /// with the recorded outbox row is [`Error::MirrorHeadForked`]. The
    /// engine reads the logged versioned entry's digest through the log
    /// plane and hands it to the outbox plane, which cross-checks it
    /// against the recorded outbox row before advancing the cursor.
    pub fn acknowledge_mirror(&self, head: MirrorHead) -> Result<MirrorAcknowledgement> {
        let _write_guard = self.write_guard();
        let logged_digest = self
            .log_plane()
            .versioned_entry_at(head.commit_sequence())?
            .map(|entry| entry.entry_digest());
        self.outbox_plane().acknowledge(head, logged_digest)
    }

    /// The durability level of one committed write.
    pub fn durability_of(&self, sequence: crate::CommitSequence) -> Result<Durability> {
        if self.log_plane().entry_present(sequence)? {
            self.outbox_plane().durability_of(sequence)
        } else {
            Err(Error::unknown_commit_sequence(sequence))
        }
    }

    /// The durability level of the store's whole state: a store
    /// without versioning never queues, a versioned store with an
    /// empty unshipped suffix is fully server-confirmed (an empty
    /// log is trivially mirrored), and anything unshipped is queued.
    pub fn store_durability(&self) -> Result<Durability> {
        if self.versioning_policy.is_none() {
            return Ok(Durability::LocalCommitted);
        }
        self.outbox_plane().store_durability()
    }

    /// Begin a durable multi-table compaction plan. The following ordinary
    /// retractions are buffered, so the complete typed plan is persisted before
    /// any materialized row can disappear.
    pub fn begin_compaction(&self) -> Result<()> {
        if self.compaction_intent()?.is_some() {
            return Err(Error::CompactionIntentOccupied);
        }
        self.begin_staged_group()
    }

    /// Persist the complete staged retraction plan and its `Planned` intent in
    /// one storage transaction. No derived row, history row, checkpoint, or
    /// outbox row changes here.
    pub fn park_compaction(&self) -> Result<bool> {
        let _write_guard = self.write_guard();
        let mut staging = self.staging_guard();
        let Some(session) = staging.take() else {
            return Err(Error::StagingSessionNotEngaged {
                surface: "park_compaction",
            });
        };
        if session.entry_count() == 0 {
            return Ok(false);
        }
        self.ensure_staging_base_current(session.base())?;
        if self.staging_plane().slot()?.is_some() || self.compaction_intent()?.is_some() {
            return Err(Error::CompactionIntentOccupied);
        }
        let group = session.into_group();
        self.storage.write(|transaction| {
            Staging::park_in(transaction, &group)?;
            COMPACTION_INTENT.insert(
                transaction,
                COMPACTION_INTENT_KEY,
                &CompactionIntent::planned(),
            )
        })?;
        self.interrupt_compaction_at(CompactionFault::AfterPlanPersisted)?;
        Ok(true)
    }

    /// Return the durable compaction lifecycle record, if recovery is pending.
    pub fn compaction_intent(&self) -> Result<Option<CompactionIntent>> {
        Ok(self
            .storage
            .read(|transaction| COMPACTION_INTENT.get(transaction, COMPACTION_INTENT_KEY))?)
    }

    /// Inject one deterministic interruption after a durable phase. This is a
    /// test support hook; production code never calls it.
    pub fn inject_compaction_fault(&self, fault: CompactionFault) {
        *self
            .compaction_fault
            .lock()
            .expect("engine compaction fault lock poisoned") = Some(fault);
    }

    /// Resume a parked compaction before a component admits ordinary work.
    /// `directory` is required only for the one atomic materialization of the
    /// typed plan; all later phases are engine-owned data planes.
    pub fn resume_compaction(&self, directory: &dyn FamilyDirectory) -> Result<()> {
        let Some(intent) = self.compaction_intent()? else {
            return Ok(());
        };
        match intent.phase() {
            CompactionPhase::Planned => {
                self.materialize_staged_group(directory)?;
                self.interrupt_compaction_at(CompactionFault::AfterRetractionsApplied)?;
                self.resume_compaction(directory)
            }
            CompactionPhase::Applied => {
                let final_sequence = intent
                    .final_sequence()
                    .ok_or(Error::CompactionIntentMissingSequence)?;
                let checkpoint_covers_plan = self.latest_checkpoint()?.is_some_and(|checkpoint| {
                    checkpoint.metadata().covered().last() >= final_sequence
                });
                if !checkpoint_covers_plan {
                    self.checkpoint()?;
                }
                // Loading verifies the published artifact and every segment
                // before this durable phase advances.
                let checkpoint = self
                    .latest_checkpoint()?
                    .ok_or(Error::CheckpointRowMissing { sequence: 0 })?;
                if checkpoint.metadata().covered().last() < final_sequence {
                    return Err(Error::CompactionCheckpointBehind {
                        checkpoint: checkpoint.metadata().covered().last().value(),
                        plan: final_sequence.value(),
                    });
                }
                self.storage.write(|transaction| {
                    COMPACTION_INTENT.insert(
                        transaction,
                        COMPACTION_INTENT_KEY,
                        &CompactionIntent::checkpointed(final_sequence),
                    )
                })?;
                self.interrupt_compaction_at(CompactionFault::AfterCheckpointPublished)?;
                self.resume_compaction(directory)
            }
            CompactionPhase::Checkpointed => {
                self.compact_configured_versioned_history()?;
                self.interrupt_compaction_at(CompactionFault::AfterHistoryFloorAdvanced)?;
                self.storage.write(|transaction| {
                    COMPACTION_INTENT.remove(transaction, COMPACTION_INTENT_KEY)
                })?;
                Ok(())
            }
        }
    }

    /// Engage a staging session: from here until a park or abandon,
    /// every write method buffers into one un-committed operation
    /// group instead of committing, and the session-scoped read
    /// surface overlays the buffered operations onto committed state.
    /// Requires a versioned store. Refused while another session is
    /// engaged or while the durable staging slot is occupied — an
    /// unresolved parked group must be materialized or discarded
    /// before a new build begins, so recovery is never silently
    /// overwritten.
    pub fn begin_staged_group(&self) -> Result<()> {
        let _write_guard = self.write_guard();
        let policy = self
            .versioning_policy
            .as_ref()
            .ok_or_else(|| Error::versioning_not_enabled("staging"))?;
        let mut staging = self.staging_guard();
        if staging.is_some() {
            return Err(Error::StagingSessionEngaged);
        }
        if self.staging_plane().slot()?.is_some() {
            return Err(Error::StagingSlotOccupied);
        }
        *staging = Some(StagingSession::new(
            StagingBase::new(
                self.log_plane().chain_head()?,
                self.committed_commit_sequence()?,
                self.committed_snapshot()?,
            ),
            policy.store_name().clone(),
            self.store_schema_hash(),
        ));
        Ok(())
    }

    /// Park the engaged session's buffered group durably and return
    /// the prospective head digest an external grant must bind. The
    /// session disengages either way: an empty buffer parks nothing
    /// and returns `None` so the caller skips its authorization round;
    /// a moved allocation base (a commit landed since engagement)
    /// refuses the stale group fail-closed. Nothing else is touched —
    /// no data row, no log entry, no outbox row, no counter.
    pub fn park_staged_group(&self) -> Result<Option<StageReceipt>> {
        let _write_guard = self.write_guard();
        let mut staging = self.staging_guard();
        let Some(session) = staging.take() else {
            return Err(Error::StagingSessionNotEngaged {
                surface: "park_staged_group",
            });
        };
        if session.entry_count() == 0 {
            return Ok(None);
        }
        self.ensure_staging_base_current(session.base())?;
        if self.staging_plane().slot()?.is_some() {
            return Err(Error::StagingSlotOccupied);
        }
        let group = session.into_group();
        let receipt = StageReceipt::new(
            group
                .prospective_head()
                .expect("a non-empty staged group has a prospective head"),
            group.entry_count(),
            group
                .first_commit_sequence()
                .expect("a non-empty staged group has a first sequence"),
            group
                .last_commit_sequence()
                .expect("a non-empty staged group has a last sequence"),
        );
        self.staging_plane().park(&group)?;
        Ok(Some(receipt))
    }

    /// Abandon the engaged session: drop the in-memory buffer and
    /// disengage, durably touching nothing. For mid-build pipeline
    /// failures; idempotent, so cleanup paths can always call it.
    pub fn abandon_staged_group(&self) -> Result<()> {
        let _write_guard = self.write_guard();
        self.staging_guard().take();
        Ok(())
    }

    /// The durably parked staged group, if the slot is occupied —
    /// surfaced at open so a daemon's crash-recovery pass resolves it
    /// (re-ask the authorizer, then materialize or discard) before the
    /// intake path opens. Never auto-resolved by the engine.
    pub fn staged_group(&self) -> Result<Option<StagedGroupSummary>> {
        let Some(group) = self.staging_plane().slot()? else {
            return Ok(None);
        };
        let (Some(head), Some(first), Some(last)) = (
            group.prospective_head(),
            group.first_commit_sequence(),
            group.last_commit_sequence(),
        ) else {
            return Ok(None);
        };
        Ok(Some(StagedGroupSummary::new(
            head,
            group.base_predecessor(),
            group.entry_count(),
            first,
            last,
        )))
    }

    /// Materialize the parked staged group: one atomic transaction
    /// applies the parked payloads to the family tables (through the
    /// component's directory, exactly as rebuild does), appends the
    /// metadata and versioned log entries, records the outbox rows,
    /// advances every counter the equivalent direct writes would have
    /// advanced, and clears the staging slot. The parked entries apply
    /// verbatim — nothing re-executes — and the produced head must
    /// equal the staged prospective digest, verified by recomputing
    /// every entry digest from its own fields against the current
    /// chain head. Subscription deltas for the group deliver after the
    /// transaction commits through [`FamilyDirectory::announce`],
    /// matching what the equivalent direct writes would have
    /// delivered; an announce error therefore reports a delivery
    /// fault, never a rolled-back append.
    pub fn materialize_staged_group(
        &self,
        directory: &dyn FamilyDirectory,
    ) -> Result<MaterializeStagedReceipt> {
        let _write_guard = self.write_guard();
        if self.staging_guard().is_some() {
            return Err(Error::StagingSessionEngaged);
        }
        let Some(group) = self.staging_plane().slot()? else {
            return Err(Error::StagingSlotEmpty {
                surface: "materialize_staged_group",
            });
        };
        let current_head = self.log_plane().chain_head()?;
        let current_sequence = self.committed_commit_sequence()?;
        let current_snapshot = self.committed_snapshot()?;
        if group.base_predecessor() != current_head {
            return Err(Error::StagingBaseMoved {
                sequence: current_sequence.value(),
            });
        }
        let mut previous = current_head;
        let mut expected_sequence = current_sequence;
        let mut expected_snapshot = current_snapshot;
        for entry in group.entries() {
            expected_sequence = expected_sequence.next();
            expected_snapshot = expected_snapshot.next();
            if entry.previous_entry_digest() != previous
                || entry.commit_sequence() != expected_sequence
                || entry.snapshot() != expected_snapshot
            {
                return Err(Error::StagingBaseMoved {
                    sequence: current_sequence.value(),
                });
            }
            let recomputed = crate::EntryDigest::from_entry_fields(
                entry.store_name(),
                entry.schema_hash(),
                entry.commit_sequence(),
                entry.snapshot(),
                entry.previous_entry_digest(),
                entry.operations(),
            );
            if recomputed != entry.entry_digest() {
                return Err(Error::StagedGroupDigestMismatch {
                    sequence: entry.commit_sequence().value(),
                    staged: entry.entry_digest(),
                    computed: recomputed,
                });
            }
            previous = Some(entry.entry_digest());
        }
        let staged_head = group.prospective_head().ok_or(Error::StagingSlotEmpty {
            surface: "materialize_staged_group",
        })?;
        let produced = previous.expect("a verified non-empty group has a head");
        if produced != staged_head {
            return Err(Error::StagedHeadMismatch {
                staged: staged_head,
                produced,
            });
        }
        let last_entry = group
            .entries()
            .last()
            .expect("a verified non-empty group has a last entry");
        let final_sequence = last_entry.commit_sequence();
        let final_snapshot = last_entry.snapshot();
        let mut identified_advances: HashMap<String, u64> = HashMap::new();
        for entry in group.entries() {
            for operation in entry.operations() {
                if operation.operation() == SemaOperation::Assert
                    && let Some(crate::RecordKey::Identifier(identifier)) = operation.key()
                {
                    identified_advances.insert(
                        operation.family().identified_counter_key(),
                        identifier.next().value(),
                    );
                }
            }
        }
        let counts = self.log_counts()?;
        let completes_compaction = self
            .compaction_intent()?
            .is_some_and(|intent| intent.is_planned());
        let writes_mirror_outbox = self
            .versioning_policy
            .as_ref()
            .is_some_and(|policy| policy.recovery_topology() == VersionedRecoveryTopology::Mirror);
        self.storage.write(|transaction| {
            let mut commit_count = counts.commits();
            let mut versioned_count = counts.versioned();
            for entry in group.entries() {
                for operation in entry.operations() {
                    let key = operation.key().cloned().ok_or_else(|| {
                        Error::ReplayMissingKey {
                            family: operation.family().family().as_str().to_owned(),
                        }
                        .into_storage()
                    })?;
                    directory
                        .materialize(RowMaterializer::new(
                            transaction,
                            ViewRow::new(
                                operation.family().clone(),
                                key,
                                operation.payload().clone(),
                            ),
                        ))
                        .map_err(Error::into_storage)?;
                }
                commit_count += 1;
                versioned_count += 1;
                CommitLog::append_commit(transaction, &CommitLogEntry::from(entry), commit_count)?;
                CommitLog::append_versioned(transaction, entry, versioned_count)?;
                if writes_mirror_outbox {
                    Outbox::record(transaction, entry)?;
                }
            }
            COUNTERS.insert(
                transaction,
                LATEST_COMMIT_SEQUENCE_KEY,
                &final_sequence.value(),
            )?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &final_snapshot.value())?;
            for (counter_key, next_value) in &identified_advances {
                IDENTIFIED_COUNTERS.insert(transaction, counter_key.clone(), next_value)?;
            }
            if completes_compaction {
                COMPACTION_INTENT.insert(
                    transaction,
                    COMPACTION_INTENT_KEY,
                    &CompactionIntent::applied(final_sequence),
                )?;
            }
            Staging::clear_in(transaction)?;
            Ok(())
        })?;
        for (entry_index, entry) in group.entries().iter().enumerate() {
            for (operation_index, operation) in entry.operations().into_iter().enumerate() {
                let Some(key) = operation.key().cloned() else {
                    continue;
                };
                let (kind, bytes) = match (operation.operation(), operation.payload()) {
                    (SemaOperation::Assert, VersionedPayload::Record { bytes }) => {
                        (DeltaKind::Assert, bytes.clone())
                    }
                    (SemaOperation::Mutate, VersionedPayload::Record { bytes }) => {
                        (DeltaKind::Mutate, bytes.clone())
                    }
                    (SemaOperation::Retract, _) => {
                        let Some(pre_image) = group.retract_pre_image(entry_index, operation_index)
                        else {
                            continue;
                        };
                        (DeltaKind::Retract, pre_image.to_vec())
                    }
                    _ => continue,
                };
                directory.announce(DeltaAnnouncer::new(
                    &self.subscriptions,
                    kind,
                    operation.family().clone(),
                    key,
                    entry.snapshot(),
                    bytes,
                ))?;
            }
        }
        Ok(MaterializeStagedReceipt::new(
            staged_head,
            final_sequence,
            final_snapshot,
            group.entry_count(),
        ))
    }

    /// Discard the parked staged group: clear the slot durably,
    /// nothing else. The refused operations never reach any observable
    /// state, so there is nothing to roll back.
    pub fn discard_staged_group(&self) -> Result<DiscardStagedReceipt> {
        let _write_guard = self.write_guard();
        let Some(group) = self.staging_plane().slot()? else {
            return Err(Error::StagingSlotEmpty {
                surface: "discard_staged_group",
            });
        };
        let receipt = DiscardStagedReceipt::new(
            group.prospective_head().ok_or(Error::StagingSlotEmpty {
                surface: "discard_staged_group",
            })?,
            group.entry_count(),
        );
        self.staging_plane().clear()?;
        Ok(receipt)
    }

    fn staging_plane(&self) -> Staging<'_> {
        Staging::new(&self.storage)
    }

    fn interrupt_compaction_at(&self, phase: CompactionFault) -> Result<()> {
        let mut injected = self
            .compaction_fault
            .lock()
            .expect("engine compaction fault lock poisoned");
        if *injected == Some(phase) {
            *injected = None;
            let phase = match phase {
                CompactionFault::AfterPlanPersisted => "plan-persisted",
                CompactionFault::AfterRetractionsApplied => "retractions-applied",
                CompactionFault::AfterCheckpointPublished => "checkpoint-published",
                CompactionFault::AfterHistoryFloorAdvanced => "history-floor-advanced",
            };
            return Err(Error::CompactionInterrupted { phase });
        }
        Ok(())
    }

    /// Acquire the staging-session slot. Poison propagation mirrors
    /// [`Self::write_guard`]: a panic while the session lock is held
    /// leaves build state unrecoverable, so unwrapping is correct.
    fn staging_guard(&self) -> std::sync::MutexGuard<'_, Option<StagingSession>> {
        self.staging
            .lock()
            .expect("engine staging session lock poisoned by a panicking writer")
    }

    /// Whether the allocation base a session captured is still the
    /// store's current state: same chain head, same counters, same
    /// identified-counter starts. Any drift means a commit interleaved
    /// with the build, so the staged group's sequences, digests, or
    /// minted identifiers are stale — refuse, fail-closed.
    fn ensure_staging_base_current(&self, base: &StagingBase) -> Result<()> {
        let current_sequence = self.committed_commit_sequence()?;
        if self.log_plane().chain_head()? != base.chain_head()
            || current_sequence != base.commit_sequence()
            || self.committed_snapshot()? != base.snapshot()
        {
            return Err(Error::StagingBaseMoved {
                sequence: current_sequence.value(),
            });
        }
        for (counter_key, captured_start) in base.identified_counters() {
            let durable = self
                .storage
                .read(|transaction| IDENTIFIED_COUNTERS.get(transaction, counter_key.clone()))?
                .unwrap_or_else(|| RecordIdentifier::first().value());
            if durable != *captured_start {
                return Err(Error::StagingBaseMoved {
                    sequence: current_sequence.value(),
                });
            }
        }
        Ok(())
    }

    fn staged_assert_keyed<RecordValue>(
        &self,
        session: &mut StagingSession,
        assertion: KeyedAssertion<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = assertion.key().clone();
        let table_name = assertion.table().name().as_str();
        if self.staged_key_present(session, table_name, &key, || {
            self.record_present(assertion.table(), &key)
        })? {
            return Err(self.duplicate_assert_key(assertion.table(), &key));
        }
        let family = self.registered_family(assertion.table().name())?;
        let payload =
            self.versioned_record_payload(*assertion.table().name(), assertion.record())?;
        session.set_overlay(table_name, &key, OverlayPresence::Present);
        let (commit_sequence, snapshot) = session.append_entry(NonEmpty::single(
            VersionedLogOperation::new(SemaOperation::Assert, family, Some(key.clone()), payload),
        ));
        Ok(crate::MutationReceipt::new(
            SemaOperation::Assert,
            *assertion.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_mutate_keyed<RecordValue>(
        &self,
        session: &mut StagingSession,
        mutation: KeyedMutation<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = mutation.key().clone();
        let table_name = mutation.table().name().as_str();
        if !self.staged_key_present(session, table_name, &key, || {
            self.record_present(mutation.table(), &key)
        })? {
            return Err(self.record_not_found(mutation.table(), &key));
        }
        let family = self.registered_family(mutation.table().name())?;
        let payload = self.versioned_record_payload(*mutation.table().name(), mutation.record())?;
        session.set_overlay(table_name, &key, OverlayPresence::Present);
        let (commit_sequence, snapshot) = session.append_entry(NonEmpty::single(
            VersionedLogOperation::new(SemaOperation::Mutate, family, Some(key.clone()), payload),
        ));
        Ok(crate::MutationReceipt::new(
            SemaOperation::Mutate,
            *mutation.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_retract<RecordValue>(
        &self,
        session: &mut StagingSession,
        retraction: Retraction<RecordValue>,
    ) -> Result<crate::MutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = retraction.key().clone();
        let table_name = retraction.table().name().as_str();
        let pre_image = self.staged_pre_image(session, table_name, &key, || {
            self.storage
                .read(|transaction| {
                    retraction
                        .table()
                        .sema_table()
                        .get(transaction, key.to_owned_string())
                })?
                .map(|record: RecordValue| {
                    self.versioned_record_payload(*retraction.table().name(), &record)
                })
                .transpose()
        })?;
        let Some(pre_image) = pre_image else {
            return Err(self.record_not_found(retraction.table(), &key));
        };
        let family = self.registered_family(retraction.table().name())?;
        session.set_overlay(table_name, &key, OverlayPresence::Absent);
        session.park_pre_image(0, pre_image);
        let (commit_sequence, snapshot) =
            session.append_entry(NonEmpty::single(VersionedLogOperation::new(
                SemaOperation::Retract,
                family,
                Some(key.clone()),
                VersionedPayload::tombstone(),
            )));
        Ok(crate::MutationReceipt::new(
            SemaOperation::Retract,
            *retraction.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_assert_identified<RecordValue>(
        &self,
        session: &mut StagingSession,
        assertion: IdentifiedAssertion<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let counter_key = assertion.table().name().identified_counter_key();
        let identifier = session.mint_identifier(counter_key, || {
            self.next_record_identifier(assertion.table())
        })?;
        let key = crate::RecordKey::identifier(identifier);
        let family = self.registered_family(assertion.table().name())?;
        let payload =
            self.versioned_record_payload(*assertion.table().name(), assertion.record())?;
        session.set_overlay(
            assertion.table().name().as_str(),
            &key,
            OverlayPresence::Present,
        );
        let (commit_sequence, snapshot) = session.append_entry(NonEmpty::single(
            VersionedLogOperation::new(SemaOperation::Assert, family, Some(key), payload),
        ));
        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Assert,
            *assertion.table().name(),
            identifier,
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_mutate_identified<RecordValue>(
        &self,
        session: &mut StagingSession,
        mutation: IdentifiedMutation<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = crate::RecordKey::identifier(mutation.identifier());
        let table_name = mutation.table().name().as_str();
        if !self.staged_key_present(session, table_name, &key, || {
            self.identified_record_present(mutation.table(), mutation.identifier())
        })? {
            return Err(self.identified_record_not_found(mutation.table(), mutation.identifier()));
        }
        let family = self.registered_family(mutation.table().name())?;
        let payload = self.versioned_record_payload(*mutation.table().name(), mutation.record())?;
        session.set_overlay(table_name, &key, OverlayPresence::Present);
        let (commit_sequence, snapshot) = session.append_entry(NonEmpty::single(
            VersionedLogOperation::new(SemaOperation::Mutate, family, Some(key), payload),
        ));
        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Mutate,
            *mutation.table().name(),
            mutation.identifier(),
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_retract_identified<RecordValue>(
        &self,
        session: &mut StagingSession,
        retraction: IdentifiedRetraction<RecordValue>,
    ) -> Result<IdentifiedMutationReceipt>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = crate::RecordKey::identifier(retraction.identifier());
        let table_name = retraction.table().name().as_str();
        let pre_image = self.staged_pre_image(session, table_name, &key, || {
            self.storage
                .read(|transaction| {
                    retraction
                        .table()
                        .sema_table()
                        .get(transaction, retraction.identifier().value())
                })?
                .map(|record: RecordValue| {
                    self.versioned_record_payload(*retraction.table().name(), &record)
                })
                .transpose()
        })?;
        let Some(pre_image) = pre_image else {
            return Err(
                self.identified_record_not_found(retraction.table(), retraction.identifier())
            );
        };
        let family = self.registered_family(retraction.table().name())?;
        session.set_overlay(table_name, &key, OverlayPresence::Absent);
        session.park_pre_image(0, pre_image);
        let (commit_sequence, snapshot) =
            session.append_entry(NonEmpty::single(VersionedLogOperation::new(
                SemaOperation::Retract,
                family,
                Some(key),
                VersionedPayload::tombstone(),
            )));
        Ok(IdentifiedMutationReceipt::new(
            SemaOperation::Retract,
            *retraction.table().name(),
            retraction.identifier(),
            commit_sequence,
            snapshot,
        ))
    }

    fn staged_commit<RecordValue>(
        &self,
        session: &mut StagingSession,
        request: CommitRequest<RecordValue>,
    ) -> Result<crate::CommitReceipt>
    where
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let family = self.registered_family(request.table().name())?;
        let table_name = request.table().name().as_str();
        let mut request_keys = HashSet::new();
        let mut operations = Vec::with_capacity(request.operation_count());
        let mut overlay_effects = Vec::with_capacity(request.operation_count());
        let mut pre_images = Vec::new();
        for (operation_index, operation) in request.operations().iter().enumerate() {
            match operation {
                WriteOperation::Assert(record) => {
                    let key = record.record_key();
                    if !request_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), &key));
                    }
                    if self.staged_key_present(session, table_name, &key, || {
                        self.record_present(request.table(), &key)
                    })? {
                        return Err(self.duplicate_assert_key(request.table(), &key));
                    }
                    operations.push(VersionedLogOperation::new(
                        SemaOperation::Assert,
                        family.clone(),
                        Some(key.clone()),
                        self.versioned_record_payload(*request.table().name(), record)?,
                    ));
                    overlay_effects.push((key, OverlayPresence::Present));
                }
                WriteOperation::Mutate(record) => {
                    let key = record.record_key();
                    if !request_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), &key));
                    }
                    if !self.staged_key_present(session, table_name, &key, || {
                        self.record_present(request.table(), &key)
                    })? {
                        return Err(self.record_not_found(request.table(), &key));
                    }
                    operations.push(VersionedLogOperation::new(
                        SemaOperation::Mutate,
                        family.clone(),
                        Some(key.clone()),
                        self.versioned_record_payload(*request.table().name(), record)?,
                    ));
                    overlay_effects.push((key, OverlayPresence::Present));
                }
                WriteOperation::Retract(key) => {
                    if !request_keys.insert(key.clone()) {
                        return Err(self.duplicate_write_key(request.table(), key));
                    }
                    let pre_image = self.staged_pre_image(session, table_name, key, || {
                        self.storage
                            .read(|transaction| {
                                request
                                    .table()
                                    .sema_table()
                                    .get(transaction, key.to_owned_string())
                            })?
                            .map(|record: RecordValue| {
                                self.versioned_record_payload(*request.table().name(), &record)
                            })
                            .transpose()
                    })?;
                    let Some(pre_image) = pre_image else {
                        return Err(self.record_not_found(request.table(), key));
                    };
                    operations.push(VersionedLogOperation::new(
                        SemaOperation::Retract,
                        family.clone(),
                        Some(key.clone()),
                        VersionedPayload::tombstone(),
                    ));
                    pre_images.push((operation_index, pre_image));
                    overlay_effects.push((key.clone(), OverlayPresence::Absent));
                }
            }
        }
        for (key, presence) in overlay_effects {
            session.set_overlay(table_name, &key, presence);
        }
        for (operation_index, pre_image) in pre_images {
            session.park_pre_image(operation_index, pre_image);
        }
        let operations = NonEmpty::try_from_vec(operations).map_err(|_| Error::EmptyCommit {
            table: request.table().name().as_str().to_owned(),
        })?;
        let (commit_sequence, snapshot) = session.append_entry(operations);
        Ok(crate::CommitReceipt::new(
            *request.table().name(),
            commit_sequence,
            snapshot,
            request.operation_count(),
        ))
    }

    /// What the engaged session plus committed state say about a key:
    /// the session overlay wins, committed state answers otherwise —
    /// exactly what a sequence of direct writes would each have
    /// observed at its own commit point.
    fn staged_key_present(
        &self,
        session: &StagingSession,
        table_name: &str,
        key: &crate::RecordKey,
        committed: impl FnOnce() -> Result<bool>,
    ) -> Result<bool> {
        match session.overlay_presence(table_name, key) {
            Some(OverlayPresence::Present) => Ok(true),
            Some(OverlayPresence::Absent) => Ok(false),
            None => committed(),
        }
    }

    /// The payload bytes a staged retract removes: the session's last
    /// buffered upsert for the key when one exists, the committed row
    /// otherwise. `None` when the key is absent everywhere — the
    /// caller's typed not-found.
    fn staged_pre_image(
        &self,
        session: &StagingSession,
        table_name: &str,
        key: &crate::RecordKey,
        committed: impl FnOnce() -> Result<Option<VersionedPayload>>,
    ) -> Result<Option<Vec<u8>>> {
        match session.overlay_presence(table_name, key) {
            Some(OverlayPresence::Absent) => Ok(None),
            Some(OverlayPresence::Present) => {
                let effects = session.overlay_effects_for_table(table_name);
                let last_upsert = effects
                    .iter()
                    .rev()
                    .find(|(effect_key, _)| *effect_key == key)
                    .and_then(|(_, effect)| match effect {
                        OverlayEffect::Upsert(bytes) => Some(bytes.to_vec()),
                        OverlayEffect::Remove => None,
                    });
                match last_upsert {
                    Some(bytes) => Ok(Some(bytes)),
                    // The overlay says present but the presence came
                    // from committed state (for example a mutate of a
                    // committed row records Present without the session
                    // having asserted it first) — fall through.
                    None => {
                        Ok(committed()?.and_then(|payload| payload.bytes().map(<[u8]>::to_vec)))
                    }
                }
            }
            None => Ok(committed()?.and_then(|payload| payload.bytes().map(<[u8]>::to_vec))),
        }
    }

    fn staged_match_records<RecordValue>(
        &self,
        session: &StagingSession,
        query: QueryPlan<RecordValue>,
    ) -> Result<QuerySnapshot<RecordValue>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let committed: Vec<(String, RecordValue)> = self
            .storage
            .read(|transaction| query.table().sema_table().iter(transaction))?;
        let mut rows: BTreeMap<String, RecordValue> = committed.into_iter().collect();
        for (key, effect) in session.overlay_effects_for_table(query.table().name().as_str()) {
            match effect {
                OverlayEffect::Upsert(bytes) => {
                    rows.insert(
                        key.to_owned_string(),
                        Self::decode_staged_payload(bytes, query.table().name().as_str())?,
                    );
                }
                OverlayEffect::Remove => {
                    rows.remove(&key.to_owned_string());
                }
            }
        }
        let database_marker = crate::DatabaseMarker::new(
            session.engaged_commit_sequence(),
            session.engaged_snapshot(),
        );
        let records: Vec<RecordValue> = match query.read_plan().node() {
            crate::ReadPlanNode::AllRows => rows.into_values().collect(),
            crate::ReadPlanNode::ByKey(key) => {
                rows.remove(&key.to_owned_string()).into_iter().collect()
            }
            crate::ReadPlanNode::ByKeyRange(range) => rows
                .into_iter()
                .filter(|(key, _)| range.contains(&crate::RecordKey::new(key.clone())))
                .map(|(_, record)| record)
                .collect(),
            node => {
                return Err(Error::UnsupportedReadPlan {
                    operator: node.operator(),
                });
            }
        };
        Ok(QuerySnapshot::new(
            SemaOperation::Match,
            *query.table().name(),
            database_marker,
            records,
        ))
    }

    fn staged_match_identified<RecordValue>(
        &self,
        session: &StagingSession,
        query: IdentifiedQueryPlan<RecordValue>,
    ) -> Result<IdentifiedQuerySnapshot<RecordValue>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let committed: Vec<(u64, RecordValue)> = self
            .storage
            .read(|transaction| query.table().sema_table().iter(transaction))?;
        let mut rows: BTreeMap<u64, RecordValue> = committed.into_iter().collect();
        for (key, effect) in session.overlay_effects_for_table(query.table().name().as_str()) {
            let crate::RecordKey::Identifier(identifier) = key else {
                continue;
            };
            match effect {
                OverlayEffect::Upsert(bytes) => {
                    rows.insert(
                        identifier.value(),
                        Self::decode_staged_payload(bytes, query.table().name().as_str())?,
                    );
                }
                OverlayEffect::Remove => {
                    rows.remove(&identifier.value());
                }
            }
        }
        let database_marker = crate::DatabaseMarker::new(
            session.engaged_commit_sequence(),
            session.engaged_snapshot(),
        );
        let records: Vec<IdentifiedRecord<RecordValue>> = match query.read_plan().node() {
            crate::IdentifiedReadPlanNode::AllRows => rows
                .into_iter()
                .map(|(identifier, record)| {
                    IdentifiedRecord::new(RecordIdentifier::new(identifier), record)
                })
                .collect(),
            crate::IdentifiedReadPlanNode::ByIdentifier(identifier) => rows
                .remove(&identifier.value())
                .map(|record| IdentifiedRecord::new(*identifier, record))
                .into_iter()
                .collect(),
            crate::IdentifiedReadPlanNode::ByIdentifierRange(range) => rows
                .into_iter()
                .filter_map(|(identifier, record)| {
                    let identifier = RecordIdentifier::new(identifier);
                    if range.contains(identifier) {
                        Some(IdentifiedRecord::new(identifier, record))
                    } else {
                        None
                    }
                })
                .collect(),
        };
        Ok(IdentifiedQuerySnapshot::new(
            SemaOperation::Match,
            *query.table().name(),
            database_marker,
            records,
        ))
    }

    fn decode_staged_payload<RecordValue>(bytes: &[u8], table: &str) -> Result<RecordValue>
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

    fn record_present<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &crate::RecordKey,
    ) -> Result<bool>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .storage
            .read(|transaction| table.sema_table().get(transaction, key.to_owned_string()))?
            .is_some())
    }

    fn identified_record_present<RecordValue>(
        &self,
        table: &IdentifiedTableReference<RecordValue>,
        identifier: RecordIdentifier,
    ) -> Result<bool>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .storage
            .read(|transaction| table.sema_table().get(transaction, identifier.value()))?
            .is_some())
    }

    /// Every registered family identity, in catalog order.
    fn family_inventory(&self) -> Vec<FamilyIdentity> {
        self.catalog
            .registrations()
            .iter()
            .map(|registration| registration.identity().clone())
            .collect()
    }

    /// The durable next-record-identifier counters for every
    /// engine-identified table, recovered from their counter keys.
    fn identified_counter_inventory(&self) -> Result<Vec<IdentifiedCounter>> {
        let suffix = format!(":{}", crate::table::IDENTIFIED_COUNTER_SUFFIX);
        Ok(self
            .storage
            .read(|transaction| IDENTIFIED_COUNTERS.iter(transaction))?
            .into_iter()
            .filter_map(|(key, next_identifier)| {
                key.strip_suffix(suffix.as_str())
                    .map(|table_name| IdentifiedCounter::new(table_name, next_identifier))
            })
            .collect())
    }

    /// Every commit must carry a versioned entry for the fold to be
    /// the whole state; a store that wrote history before enabling
    /// versioning cannot checkpoint or rebuild.
    fn ensure_versioned_log_complete(&self) -> Result<()> {
        let counts = self.log_counts()?;
        if counts.commits() != counts.versioned() {
            return Err(Error::VersionedLogIncomplete {
                commits: counts.commits(),
                versioned: counts.versioned(),
            });
        }
        Ok(())
    }

    fn ensure_fresh_for_import(&self) -> Result<()> {
        let fresh = self.catalog.registrations().is_empty()
            && self.storage.read(|transaction| {
                Ok(COUNTERS
                    .get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?
                    .is_none()
                    && COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?.is_none()
                    && COUNTERS.get(transaction, LATEST_CHECKPOINT_KEY)?.is_none()
                    && CATALOG.iter(transaction)?.is_empty())
            })?
            && self.log_plane().logs_empty()?;
        if fresh {
            Ok(())
        } else {
            Err(Error::ImportStoreNotFresh)
        }
    }

    /// Read and validate the layout slot without writing, returning the
    /// open-time plan the caller applies after every other validation
    /// passes. A current-layout store is left alone; a virgin store is
    /// stamped fresh; an older store that already opted into versioning
    /// refolds the layout-introduced derived slots from its complete
    /// log; everything else hard-fails with a typed layout error.
    fn validated_storage_layout(storage: &sema::Sema) -> Result<LayoutOpenPlan> {
        let stored = storage.read(|transaction| COUNTERS.get(transaction, STORAGE_LAYOUT_KEY))?;
        match stored {
            // Current layout: idempotent open, nothing to do.
            Some(layout) if layout == STORAGE_LAYOUT => Ok(LayoutOpenPlan::Current),
            // Forward skew: a layout newer than this build wrote derived
            // state this engine cannot interpret. Refusing to downgrade
            // an unknown-newer store is the only safe move.
            Some(layout) if layout > STORAGE_LAYOUT => Err(Error::StorageLayoutMismatch {
                stored: layout,
                expected: STORAGE_LAYOUT,
            }),
            // Older layout. Slice A's bump to layout 5 was purely
            // additive — it added the CHAIN_HEAD and log-count derived
            // slots without touching the data tables or the versioned
            // log format. So if the store opted into versioning and its
            // versioned log is non-empty, the missing derived slots are
            // recomputable from that log alone: refold them and
            // re-stamp. With no versioned log present this is a
            // pre-versioning store whose derived state is not log-
            // recoverable, so it hard-fails (the previous-engine
            // StoreMigration owns pre-v9 stores); state loss is
            // unacceptable (Spirit 29pb).
            Some(layout) => {
                if CommitLog::versioned_log_present(storage)? {
                    Ok(LayoutOpenPlan::RebuildDerivedSlots)
                } else {
                    Err(Error::StorageLayoutMismatch {
                        stored: layout,
                        expected: STORAGE_LAYOUT,
                    })
                }
            }
            None => {
                // No layout slot: either a virgin store or a layout-1
                // store from before the slot existed. Any engine counter
                // proves prior engine writes, hence layout 1.
                let has_engine_history = storage.read(|transaction| {
                    Ok(COUNTERS
                        .get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?
                        .or(COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?)
                        .or(COUNTERS.get(transaction, NEXT_SUBSCRIPTION_KEY)?)
                        .is_some())
                })?;
                if has_engine_history {
                    return Err(Error::StorageLayoutMismatch {
                        stored: LAYOUT_BEFORE_SLOT,
                        expected: STORAGE_LAYOUT,
                    });
                }
                Ok(LayoutOpenPlan::StampFresh)
            }
        }
    }

    /// Carry out the open-time layout plan. Runs only after every
    /// read-only validation passed, so an open that rejects a store
    /// never mutates the store it rejects.
    fn apply_layout_plan(storage: &sema::Sema, plan: LayoutOpenPlan) -> Result<()> {
        match plan {
            LayoutOpenPlan::Current => Ok(()),
            LayoutOpenPlan::StampFresh => Ok(storage.write(|transaction| {
                COUNTERS.insert(transaction, STORAGE_LAYOUT_KEY, &STORAGE_LAYOUT)
            })?),
            // Refold the layout-introduced derived slots (the persisted
            // chain-head digest and the two log-count counters) from the
            // store's complete versioned log and re-stamp the layout
            // counter. The log plane owns those slots, so the refold lives
            // there; the engine names the layout slot and target version.
            LayoutOpenPlan::RebuildDerivedSlots => {
                CommitLog::rebuild_derived_slots(storage, STORAGE_LAYOUT_KEY, STORAGE_LAYOUT)
            }
        }
    }

    /// Bind one finite recovery policy to the store itself. A later daemon
    /// cannot reinterpret a local-checkpoint store as mirrored (or the
    /// reverse) merely by changing its process configuration. An old layout-5
    /// versioned store obtains this row during its validated layout-6 open;
    /// that migration is additive and never deletes historical rows.
    fn ensure_durable_versioning_policy(
        storage: &sema::Sema,
        requested: Option<&VersioningPolicy>,
    ) -> Result<()> {
        let recorded = storage
            .read(|transaction| VERSIONING_POLICY.get(transaction, VERSIONING_POLICY_KEY))?;
        match (recorded, requested) {
            (Some(_), None) => Err(Error::VersioningPolicyRequired),
            (Some(recorded), Some(requested)) if recorded == *requested => Ok(()),
            (Some(_), Some(_)) => Err(Error::VersioningPolicyMismatch),
            (None, None) => Ok(()),
            (None, Some(requested)) => Ok(storage.write(|transaction| {
                VERSIONING_POLICY.insert(transaction, VERSIONING_POLICY_KEY, requested)
            })?),
        }
    }

    fn family_registration_state(&self, identity: &FamilyIdentity) -> Result<FamilyRegistration> {
        if let Some(stored) = self
            .catalog
            .registrations()
            .iter()
            .find(|registration| registration.table_name() == identity.table_name())
        {
            if stored.identity() != identity {
                return Err(Error::FamilyIdentityMismatch {
                    table: identity.table_name().to_owned(),
                    stored: stored.identity().to_string(),
                    declared: identity.to_string(),
                });
            }
            return Ok(FamilyRegistration::Existing);
        }
        if let Some(bound) = self.catalog.registration_for_family(identity) {
            return Err(Error::FamilyAlreadyBound {
                family: identity.family().as_str().to_owned(),
                existing: bound.table_name().to_owned(),
                table: identity.table_name().to_owned(),
            });
        }
        Ok(FamilyRegistration::New(TableRegistration::new(
            identity.clone(),
        )))
    }

    fn registered_family(&self, name: &TableName) -> Result<FamilyIdentity> {
        self.catalog
            .family_identity(name)
            .cloned()
            .ok_or_else(|| Error::TableNotRegistered {
                table: name.as_str().to_owned(),
            })
    }

    /// Acquire the engine's write-serialization lock for the duration
    /// of a single mutation. Every mutator holds the returned guard
    /// across its whole read-compute-write, so sequence allocation,
    /// the predecessor-digest read, the duplicate-key check, identifier
    /// minting, and the redb write execute as one serialized unit and
    /// two concurrent `&self` callers can never fork the digest chain
    /// or duplicate a commit sequence. The lock poisons only if a
    /// writer panics mid-mutation — an unrecoverable state in which the
    /// redb transaction has already rolled back — so propagating the
    /// poison through `expect` is correct.
    pub(crate) fn write_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .expect("engine write lock poisoned by a panicking writer")
    }

    pub(crate) fn next_snapshot(&self) -> Result<SnapshotIdentifier> {
        Ok(self.committed_snapshot()?.next())
    }

    pub(crate) fn next_commit_sequence(&self) -> Result<crate::CommitSequence> {
        Ok(self.committed_commit_sequence()?.next())
    }

    fn database_marker_from_values(
        commit_sequence: Option<u64>,
        snapshot: Option<u64>,
    ) -> crate::DatabaseMarker {
        crate::DatabaseMarker::new(
            commit_sequence
                .map(crate::CommitSequence::new)
                .unwrap_or_else(crate::CommitSequence::genesis),
            snapshot
                .map(SnapshotIdentifier::new)
                .unwrap_or_else(SnapshotIdentifier::genesis),
        )
    }

    fn next_record_identifier<RecordValue>(
        &self,
        table: &IdentifiedTableReference<RecordValue>,
    ) -> Result<RecordIdentifier> {
        let counter_key = table.name().identified_counter_key();
        let value = self.storage.read(|transaction| {
            Ok(IDENTIFIED_COUNTERS
                .get(transaction, counter_key)?
                .map(RecordIdentifier::new)
                .unwrap_or_else(RecordIdentifier::first))
        })?;
        Ok(value)
    }

    fn ensure_registered<RecordValue>(&self, table: &TableReference<RecordValue>) -> Result<()> {
        if self.catalog.is_registered(table.name()) {
            Ok(())
        } else {
            Err(Error::TableNotRegistered {
                table: table.name().as_str().to_owned(),
            })
        }
    }

    fn ensure_identified_registered<RecordValue>(
        &self,
        table: &IdentifiedTableReference<RecordValue>,
    ) -> Result<()> {
        if self.catalog.is_registered(table.name()) {
            Ok(())
        } else {
            Err(Error::TableNotRegistered {
                table: table.name().as_str().to_owned(),
            })
        }
    }

    fn record_not_found<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &crate::RecordKey,
    ) -> Error {
        Error::RecordNotFound {
            table: table.name().as_str().to_owned(),
            key: key.to_owned_string(),
        }
    }

    fn identified_record_not_found<RecordValue>(
        &self,
        table: &IdentifiedTableReference<RecordValue>,
        identifier: RecordIdentifier,
    ) -> Error {
        Error::RecordNotFound {
            table: table.name().as_str().to_owned(),
            key: identifier.value().to_string(),
        }
    }

    fn duplicate_write_key<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &crate::RecordKey,
    ) -> Error {
        Error::DuplicateWriteKey {
            table: table.name().as_str().to_owned(),
            key: key.to_owned_string(),
        }
    }

    fn duplicate_assert_key<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &crate::RecordKey,
    ) -> Error {
        Error::DuplicateAssertKey {
            table: table.name().as_str().to_owned(),
            key: key.to_owned_string(),
        }
    }

    fn next_subscription_handle<RecordValue>(
        &self,
        plan: &QueryPlan<RecordValue>,
        snapshot: SnapshotIdentifier,
    ) -> Result<SubscriptionHandle> {
        let id = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, NEXT_SUBSCRIPTION_KEY)?
                .map(SubscriptionIdentifier::new)
                .unwrap_or_else(SubscriptionIdentifier::first))
        })?;
        Ok(SubscriptionHandle::new(id, *plan.table().name(), snapshot))
    }

    fn persist_subscription(
        &self,
        handle: SubscriptionHandle,
        filter: crate::QueryFilter,
    ) -> Result<()> {
        let registration = SubscriptionRegistration::new(handle, filter);
        self.storage.write(|transaction| {
            SUBSCRIPTIONS.insert(transaction, handle.id().value(), &registration)?;
            COUNTERS.insert(
                transaction,
                NEXT_SUBSCRIPTION_KEY,
                &handle.id().next().value(),
            )?;
            Ok(())
        })?;
        Ok(())
    }

    fn versioned_record_entry<RecordValue>(
        &self,
        commit_sequence: crate::CommitSequence,
        snapshot: SnapshotIdentifier,
        operation: SemaOperation,
        table_name: TableName,
        key: Option<crate::RecordKey>,
        record: &RecordValue,
    ) -> Result<Option<VersionedCommitLogEntry>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        if self.versioning_policy.is_none() {
            return Ok(None);
        }
        let family = self.registered_family(&table_name)?;
        self.versioned_entry(
            commit_sequence,
            snapshot,
            NonEmpty::single(VersionedLogOperation::new(
                operation,
                family,
                key,
                self.versioned_record_payload(table_name, record)?,
            )),
        )
    }

    fn versioned_tombstone_entry(
        &self,
        commit_sequence: crate::CommitSequence,
        snapshot: SnapshotIdentifier,
        operation: SemaOperation,
        table_name: TableName,
        key: Option<crate::RecordKey>,
    ) -> Result<Option<VersionedCommitLogEntry>> {
        if self.versioning_policy.is_none() {
            return Ok(None);
        }
        let family = self.registered_family(&table_name)?;
        self.versioned_entry(
            commit_sequence,
            snapshot,
            NonEmpty::single(VersionedLogOperation::new(
                operation,
                family,
                key,
                VersionedPayload::tombstone(),
            )),
        )
    }

    pub(crate) fn versioned_entry(
        &self,
        commit_sequence: crate::CommitSequence,
        snapshot: SnapshotIdentifier,
        operations: NonEmpty<VersionedLogOperation>,
    ) -> Result<Option<VersionedCommitLogEntry>> {
        let Some(policy) = self.versioning_policy.as_ref() else {
            return Ok(None);
        };
        Ok(Some(VersionedCommitLogEntry::new(
            policy.store_name().clone(),
            self.store_schema_hash(),
            commit_sequence,
            snapshot,
            self.latest_versioned_entry_digest()?,
            operations,
        )))
    }

    pub(crate) fn ensure_atomic_registered<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
    ) -> Result<()> {
        self.ensure_registered(table)
    }

    pub(crate) fn atomic_read<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &RecordKey,
    ) -> Result<Option<RecordValue>>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .storage
            .read(|transaction| table.sema_table().get(transaction, key.to_owned_string()))?)
    }

    pub(crate) fn atomic_duplicate_assert<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &RecordKey,
    ) -> Error {
        self.duplicate_assert_key(table, key)
    }

    pub(crate) fn atomic_missing<RecordValue>(
        &self,
        table: &TableReference<RecordValue>,
        key: &RecordKey,
    ) -> Error {
        self.record_not_found(table, key)
    }

    pub(crate) fn atomic_payload<RecordValue>(
        &self,
        table: TableName,
        record: &RecordValue,
    ) -> Result<VersionedPayload>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.versioned_record_payload(table, record)
    }

    pub(crate) fn atomic_versioned_operation(
        &self,
        operation: SemaOperation,
        table: TableName,
        key: RecordKey,
        payload: VersionedPayload,
    ) -> Result<Option<VersionedLogOperation>> {
        self.versioning_policy
            .as_ref()
            .map(|_| {
                self.registered_family(&table)
                    .map(|family| VersionedLogOperation::new(operation, family, Some(key), payload))
            })
            .transpose()
    }

    pub(crate) fn atomic_announce<RecordValue>(
        &self,
        kind: DeltaKind,
        table: TableName,
        key: &RecordKey,
        snapshot: SnapshotIdentifier,
        record: &RecordValue,
    ) where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.subscriptions
            .deliver_delta(kind, table, key, snapshot, record);
    }

    fn versioned_record_payload<RecordValue>(
        &self,
        table_name: TableName,
        record: &RecordValue,
    ) -> Result<VersionedPayload>
    where
        RecordValue: EngineStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let bytes = rkyv::to_bytes::<rancor::Error>(record).map_err(|source| {
            Error::VersionedPayloadEncode {
                table: table_name.as_str().to_owned(),
                message: source.to_string(),
            }
        })?;
        Ok(VersionedPayload::record(bytes.as_slice().to_vec()))
    }

    /// The predecessor digest a freshly-minted versioned entry names —
    /// the digest of the current chain head, read O(1) from the log
    /// plane's persisted slot. This is an optimization only: integrity
    /// never derives from this slot; [`CanonicalView::fold`] recomputes
    /// every entry digest and verifies the chain link by link.
    fn latest_versioned_entry_digest(&self) -> Result<Option<crate::EntryDigest>> {
        self.log_plane().chain_head()
    }

    /// The persisted row counts for both logs, read O(1) through the log
    /// plane. A mutator reads these under the held write lock alongside
    /// the next commit sequence, then writes the incremented counts
    /// inside the same transaction as the rows — keeping the counts
    /// exact without ever materializing either log.
    pub(crate) fn log_counts(&self) -> Result<LogCounts> {
        self.log_plane().counts()
    }

    /// The single durable choke point for one versioned commit: the log
    /// plane lands the versioned entry (advancing its chain head and
    /// count) and the outbox plane records the mirror row beside it, both
    /// inside the one write transaction the engine opened — so data,
    /// log, outbox, and counters commit as one atomic unit. A versioning-
    /// disabled store carries no versioned entry, so this is a no-op.
    pub(crate) fn insert_versioned_entry(
        &self,
        transaction: &sema::WriteTransaction,
        entry: &Option<VersionedCommitLogEntry>,
        versioned_count: u64,
    ) -> sema::Result<()> {
        if let Some(entry) = entry {
            CommitLog::append_versioned(transaction, entry, versioned_count)?;
            if self.versioning_policy.as_ref().is_some_and(|policy| {
                policy.recovery_topology() == VersionedRecoveryTopology::Mirror
            }) {
                Outbox::record(transaction, entry)?;
            }
        }
        Ok(())
    }
}

/// Outcome of validating a family declaration against the persisted
/// catalog: the binding already exists with the same identity, or it
/// is new and carries the registration to persist.
enum FamilyRegistration {
    Existing,
    New(TableRegistration),
}

/// The open-time storage-layout plan: what an open must do to the store
/// after every read-only validation has passed. Decided before any
/// write so a rejected open never mutates the store it rejects.
enum LayoutOpenPlan {
    /// The store is already at the current layout — idempotent open.
    Current,
    /// A virgin store with no engine history: stamp the layout slot.
    StampFresh,
    /// An older versioned store: refold the layout-introduced derived
    /// slots from its complete log and re-stamp to the current layout.
    RebuildDerivedSlots,
}

/// Read-only handle over the engine's storage kernel for transitional
/// component-local tables. The type deliberately has no write surface:
/// durable writes exist only behind the engine's logged choke points.
pub struct StorageReader<'engine> {
    storage: &'engine sema::Sema,
}

impl<'engine> StorageReader<'engine> {
    fn new(storage: &'engine sema::Sema) -> Self {
        Self { storage }
    }

    pub fn read<Row>(
        &self,
        body: impl FnOnce(&sema::ReadTransaction) -> sema::Result<Row>,
    ) -> sema::Result<Row> {
        self.storage.read(body)
    }
}

struct CommittedEffect<RecordValue> {
    kind: DeltaKind,
    key: crate::RecordKey,
    record: RecordValue,
}

impl<RecordValue> CommittedEffect<RecordValue> {
    fn new(kind: DeltaKind, key: crate::RecordKey, record: RecordValue) -> Self {
        Self { kind, key, record }
    }

    fn kind(&self) -> DeltaKind {
        self.kind
    }

    fn key(&self) -> &crate::RecordKey {
        &self.key
    }

    fn record(&self) -> &RecordValue {
        &self.record
    }
}

#[derive(Debug, Clone)]
pub struct EngineOpen {
    path: PathBuf,
    schema: Schema,
    versioning_policy: Option<VersioningPolicy>,
}

impl EngineOpen {
    pub fn new(path: impl Into<PathBuf>, version: SchemaVersion) -> Self {
        Self {
            path: path.into(),
            schema: Schema { version },
            versioning_policy: None,
        }
    }

    pub fn with_versioning(mut self, policy: VersioningPolicy) -> Self {
        self.versioning_policy = Some(policy);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn versioning_policy(&self) -> Option<&VersioningPolicy> {
        self.versioning_policy.as_ref()
    }
}
