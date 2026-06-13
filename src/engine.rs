use std::collections::HashSet;
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
use crate::fold::{CanonicalView, FamilyDirectory, RebuildReceipt, RowMaterializer};
use crate::import::{ImportReceipt, ImportSession};
use crate::log::{CommitLogEntry, CommitLogOperation};
use crate::outbox::{Durability, MirrorAcknowledgement, MirrorHead, OutboxEntry};
use crate::subscribe::{ActiveSubscription, SubscriptionRegistry};
use crate::{
    Catalog, CommitRequest, DeltaKind, EngineStoredRecord, EngineStoredValue, Error,
    FamilyIdentity, IdentifiedAssertion, IdentifiedMutation, IdentifiedMutationReceipt,
    IdentifiedQueryPlan, IdentifiedQuerySnapshot, IdentifiedRecord, IdentifiedRetraction,
    IdentifiedTableDescriptor, IdentifiedTableReference, InitialSnapshot, KeyedAssertion,
    KeyedMutation, QueryPlan, QuerySnapshot, RecordIdentifier, ReplayReceipt, Result, Retraction,
    SequenceRange, SnapshotIdentifier, StoreSchemaHash, SubscriptionHandle, SubscriptionIdentifier,
    SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink, TableDescriptor, TableName,
    TableReference, TableRegistration, VersionedCommitLogEntry, VersionedLogOperation,
    VersionedPayload, VersionedReplay, VersioningPolicy, WriteOperation,
};

const CATALOG: sema::Table<&'static str, TableRegistration> =
    sema::Table::new("__sema_engine_catalog");
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
const LATEST_COMMIT_SEQUENCE_KEY: &str = "latest_commit_sequence";
const LATEST_SNAPSHOT_KEY: &str = "latest_snapshot";
const NEXT_SUBSCRIPTION_KEY: &str = "next_subscription";
const LATEST_CHECKPOINT_KEY: &str = "latest_checkpoint";
const STORAGE_LAYOUT_KEY: &str = "engine_storage_layout";
/// Engine-internal storage layout version. Layout 2 introduced typed
/// family identity in the catalog and versioned log; layout 3 added
/// the mirror outbox row beside every versioned entry — a layout-2
/// store opening under this build would carry versioned entries with
/// no outbox rows, so a mirror would silently ship an incomplete
/// history. Older stores hard-fail at open and are rebuilt through
/// checkpoint import or versioned replay.
const STORAGE_LAYOUT: u64 = 3;
/// The layout of stores written before the layout slot existed.
const LAYOUT_BEFORE_SLOT: u64 = 1;
const COMMIT_LOG: sema::Table<u64, CommitLogEntry> = sema::Table::new("__sema_engine_commit_log");
const VERSIONED_COMMIT_LOG: sema::Table<u64, VersionedCommitLogEntry> =
    sema::Table::new("__sema_engine_versioned_commit_log");
const SUBSCRIPTIONS: sema::Table<u64, SubscriptionRegistration> =
    sema::Table::new("__sema_engine_subscriptions");
const IDENTIFIED_COUNTERS: sema::Table<String, u64> =
    sema::Table::new("__sema_engine_identified_counters");
const CHECKPOINTS: sema::Table<u64, CheckpointMetadata> =
    sema::Table::new("__sema_engine_checkpoints");
const CHECKPOINT_SEGMENTS: sema::Table<&'static [u8; 32], CheckpointSegment> =
    sema::Table::new("__sema_engine_checkpoint_segments");
const OUTBOX: sema::Table<u64, OutboxEntry> = sema::Table::new("__sema_engine_outbox");
const MIRROR_CURSOR: sema::Table<&'static str, MirrorHead> =
    sema::Table::new("__sema_engine_mirror_cursor");
const MIRROR_SHIPPED_KEY: &str = "shipped";

pub struct Engine {
    storage: sema::Sema,
    catalog: Catalog,
    subscriptions: SubscriptionRegistry,
    versioning_policy: Option<VersioningPolicy>,
}

impl Engine {
    pub fn open(request: EngineOpen) -> Result<Self> {
        let storage = sema::Sema::open_with_schema(request.path(), request.schema())?;
        // Every validation runs before the first engine write: an open
        // that rejects a store must not mutate the store it rejects.
        let stamped_layout = Self::validated_storage_layout(&storage)?;
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
        if stamped_layout.is_none() {
            storage.write(|transaction| {
                COUNTERS.insert(transaction, STORAGE_LAYOUT_KEY, &STORAGE_LAYOUT)
            })?;
        }
        let catalog = Catalog::new(registrations);
        Ok(Self {
            storage,
            catalog,
            subscriptions: SubscriptionRegistry::new(),
            versioning_policy: request.versioning_policy().cloned(),
        })
    }

    pub fn register_table<RecordValue>(
        &mut self,
        descriptor: TableDescriptor<RecordValue>,
    ) -> Result<TableReference<RecordValue>> {
        let name = *descriptor.name();
        match self.family_registration_state(&descriptor.family_identity())? {
            FamilyRegistration::Existing => {}
            FamilyRegistration::New(registration) => {
                self.storage.write(|transaction| {
                    CATALOG.insert(transaction, name.as_str(), &registration)
                })?;
                self.catalog.insert(registration)?;
            }
        }
        Ok(TableReference::new(name))
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
        self.ensure_identified_registered(assertion.table())?;

        let identifier = self.next_record_identifier(assertion.table())?;
        let commit_sequence = self.next_commit_sequence()?;
        let snapshot = self.next_snapshot()?;
        let key = crate::RecordKey::new(identifier.value().to_string());
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
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                identifier.value(),
                assertion.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
            self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        self.ensure_identified_registered(retraction.table())?;

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
        let key = crate::RecordKey::new(retraction.identifier().value().to_string());
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
        let removed = self.storage.write(|transaction| {
            let removed = retraction
                .table()
                .sema_table()
                .remove(transaction, retraction.identifier().value())?;
            if removed {
                COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
                self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        self.ensure_identified_registered(mutation.table())?;

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
        let key = crate::RecordKey::new(mutation.identifier().value().to_string());
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
        self.storage.write(|transaction| {
            mutation.table().sema_table().insert(
                transaction,
                mutation.identifier().value(),
                mutation.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
            self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        self.ensure_registered(assertion.table())?;

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
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                assertion.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
            self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        )?;

        Ok(crate::MutationReceipt::new(
            SemaOperation::Assert,
            *assertion.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
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
        self.ensure_registered(mutation.table())?;

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
        self.storage.write(|transaction| {
            mutation.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                mutation.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
            self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        )?;

        Ok(crate::MutationReceipt::new(
            SemaOperation::Mutate,
            *mutation.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
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
        self.ensure_registered(retraction.table())?;

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
        let removed = self.storage.write(|transaction| {
            let removed = retraction
                .table()
                .sema_table()
                .remove(transaction, key.to_owned_string())?;
            if removed {
                COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
                self.insert_versioned_entry(transaction, &versioned_entry)?;
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
        )?;

        Ok(crate::MutationReceipt::new(
            SemaOperation::Retract,
            *retraction.table().name(),
            key,
            commit_sequence,
            snapshot,
        ))
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
        self.ensure_registered(request.table())?;
        if request.operations().is_empty() {
            return Err(Error::EmptyCommit {
                table: request.table().name().as_str().to_owned(),
            });
        }

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
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
            self.insert_versioned_entry(transaction, &versioned_entry)?;
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
            )?;
        }

        Ok(crate::CommitReceipt::new(
            *request.table().name(),
            commit_sequence,
            snapshot,
            request.operation_count(),
        ))
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

    pub fn latest_snapshot(&self) -> Result<SnapshotIdentifier> {
        let value = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, LATEST_SNAPSHOT_KEY)?
                .map(SnapshotIdentifier::new)
                .unwrap_or_else(SnapshotIdentifier::genesis))
        })?;
        Ok(value)
    }

    pub fn current_commit_sequence(&self) -> Result<crate::CommitSequence> {
        let value = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?
                .map(crate::CommitSequence::new)
                .unwrap_or_else(crate::CommitSequence::genesis))
        })?;
        Ok(value)
    }

    pub fn current_database_marker(&self) -> Result<crate::DatabaseMarker> {
        Ok(self.storage.read(|transaction| {
            Ok(Self::database_marker_from_values(
                COUNTERS.get(transaction, LATEST_COMMIT_SEQUENCE_KEY)?,
                COUNTERS.get(transaction, LATEST_SNAPSHOT_KEY)?,
            ))
        })?)
    }

    pub fn commit_log(&self) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    fn commit_log_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| COMMIT_LOG.range(transaction, start.value()..))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    pub fn versioned_commit_log(&self) -> Result<Vec<VersionedCommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    fn versioned_commit_log_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<VersionedCommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.range(transaction, start.value()..))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
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
        Ok(self
            .commit_log()?
            .into_iter()
            .filter(|entry| range.contains(entry.snapshot()))
            .collect())
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
        self.subscriptions
            .add(ActiveSubscription::new(handle, plan, sink))?;
        Ok(SubscriptionReceipt::new(handle, initial))
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
            for entry in &suffix {
                Self::insert_versioned_row(transaction, entry)?;
                COMMIT_LOG.insert(
                    transaction,
                    entry.commit_sequence().value(),
                    &CommitLogEntry::from(entry),
                )?;
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
        let after = self
            .mirror_head()?
            .map(|head| head.commit_sequence())
            .unwrap_or_else(crate::CommitSequence::genesis);
        Ok(self
            .storage
            .read(|transaction| OUTBOX.range(transaction, after.next().value()..))?
            .into_iter()
            .map(|(_sequence, row)| row)
            .collect())
    }

    /// The durable shipped cursor: the last server-confirmed mirror
    /// head, if any acknowledgement has landed.
    pub fn mirror_head(&self) -> Result<Option<MirrorHead>> {
        Ok(self
            .storage
            .read(|transaction| MIRROR_CURSOR.get(transaction, MIRROR_SHIPPED_KEY))?)
    }

    /// Record a server-confirmed mirror head, advancing the durable
    /// shipped cursor. Idempotent: a head at or behind the cursor is
    /// a typed no-op. A head naming a sequence with no outbox row is
    /// [`Error::MirrorHeadUnknown`]; a head whose digest disagrees
    /// with the recorded outbox row is [`Error::MirrorHeadForked`].
    pub fn acknowledge_mirror(&self, head: MirrorHead) -> Result<MirrorAcknowledgement> {
        let sequence = head.commit_sequence();
        let (recorded, logged) = self.storage.read(|transaction| {
            Ok((
                OUTBOX.get(transaction, sequence.value())?,
                VERSIONED_COMMIT_LOG.get(transaction, sequence.value())?,
            ))
        })?;
        let recorded = recorded.ok_or(Error::MirrorHeadUnknown {
            sequence: sequence.value(),
        })?;
        if logged.is_none_or(|entry| entry.entry_digest() != recorded.entry_digest()) {
            return Err(Error::OutboxEntryMismatch {
                sequence: sequence.value(),
            });
        }
        if recorded.entry_digest() != head.entry_digest() {
            return Err(Error::MirrorHeadForked {
                sequence: sequence.value(),
                recorded: recorded.entry_digest(),
                acknowledged: head.entry_digest(),
            });
        }
        if let Some(current) = self.mirror_head()? {
            if sequence <= current.commit_sequence() {
                return Ok(MirrorAcknowledgement::Unchanged(current));
            }
        }
        self.storage
            .write(|transaction| MIRROR_CURSOR.insert(transaction, MIRROR_SHIPPED_KEY, &head))?;
        Ok(MirrorAcknowledgement::Advanced(head))
    }

    /// The durability level of one committed write.
    pub fn durability_of(&self, sequence: crate::CommitSequence) -> Result<Durability> {
        let (commit, outbox_row) = self.storage.read(|transaction| {
            Ok((
                COMMIT_LOG.get(transaction, sequence.value())?,
                OUTBOX.get(transaction, sequence.value())?,
            ))
        })?;
        if commit.is_none() {
            return Err(Error::unknown_commit_sequence(sequence));
        }
        let Some(outbox_row) = outbox_row else {
            return Ok(Durability::LocalCommitted);
        };
        match self.mirror_head()? {
            Some(head) if outbox_row.commit_sequence() <= head.commit_sequence() => {
                Ok(Durability::ServerCommitted)
            }
            _ => Ok(Durability::QueuedForMirror),
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
        if self.unshipped_outbox()?.is_empty() {
            Ok(Durability::ServerCommitted)
        } else {
            Ok(Durability::QueuedForMirror)
        }
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
        let (commits, versioned) = self.storage.read(|transaction| {
            Ok((
                COMMIT_LOG.iter(transaction)?.len() as u64,
                VERSIONED_COMMIT_LOG.iter(transaction)?.len() as u64,
            ))
        })?;
        if commits != versioned {
            return Err(Error::VersionedLogIncomplete { commits, versioned });
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
                    && CATALOG.iter(transaction)?.is_empty()
                    && COMMIT_LOG.iter(transaction)?.is_empty()
                    && VERSIONED_COMMIT_LOG.iter(transaction)?.is_empty())
            })?;
        if fresh {
            Ok(())
        } else {
            Err(Error::ImportStoreNotFresh)
        }
    }

    /// Read and validate the layout slot without writing. Returns the
    /// stamped layout, or `None` for a virgin store whose slot the
    /// caller stamps after every other open-time validation passes.
    fn validated_storage_layout(storage: &sema::Sema) -> Result<Option<u64>> {
        let stored = storage.read(|transaction| COUNTERS.get(transaction, STORAGE_LAYOUT_KEY))?;
        match stored {
            Some(layout) if layout == STORAGE_LAYOUT => Ok(Some(layout)),
            Some(layout) => Err(Error::StorageLayoutMismatch {
                stored: layout,
                expected: STORAGE_LAYOUT,
            }),
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
                Ok(None)
            }
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

    fn next_snapshot(&self) -> Result<SnapshotIdentifier> {
        Ok(self.latest_snapshot()?.next())
    }

    fn next_commit_sequence(&self) -> Result<crate::CommitSequence> {
        Ok(self.current_commit_sequence()?.next())
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

    fn versioned_entry(
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

    fn latest_versioned_entry_digest(&self) -> Result<Option<crate::EntryDigest>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry.entry_digest())
            .next_back())
    }

    fn insert_versioned_entry(
        &self,
        transaction: &sema::WriteTransaction,
        entry: &Option<VersionedCommitLogEntry>,
    ) -> sema::Result<()> {
        if let Some(entry) = entry {
            Self::insert_versioned_row(transaction, entry)?;
        }
        Ok(())
    }

    /// The single durable choke point for versioned history: every
    /// versioned commit log entry lands with its mirror outbox row in
    /// the same write transaction, whether written by a live mutation
    /// or restored verbatim by an import.
    fn insert_versioned_row(
        transaction: &sema::WriteTransaction,
        entry: &VersionedCommitLogEntry,
    ) -> sema::Result<()> {
        VERSIONED_COMMIT_LOG.insert(transaction, entry.commit_sequence().value(), entry)?;
        OUTBOX.insert(
            transaction,
            entry.commit_sequence().value(),
            &OutboxEntry::from(entry),
        )
    }
}

/// Outcome of validating a family declaration against the persisted
/// catalog: the binding already exists with the same identity, or it
/// is new and carries the registration to persist.
enum FamilyRegistration {
    Existing,
    New(TableRegistration),
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
