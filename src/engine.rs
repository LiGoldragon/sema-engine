use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema::{Schema, SchemaVersion};
use signal_core::NonEmpty;
use signal_sema::SemaOperation;

use crate::log::{CommitLogEntry, CommitLogOperation};
use crate::subscribe::{ActiveSubscription, SubscriptionRegistry};
use crate::{
    Catalog, CommitRequest, DeltaKind, EngineStoredRecord, Error, InitialSnapshot, QueryPlan,
    QuerySnapshot, Result, Retraction, SequenceRange, SnapshotIdentifier, SubscriptionHandle,
    SubscriptionId, SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink,
    TableDescriptor, TableReference, TableRegistration, WriteOperation,
};

const CATALOG: sema::Table<&'static str, TableRegistration> =
    sema::Table::new("__sema_engine_catalog");
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
const LATEST_COMMIT_SEQUENCE_KEY: &str = "latest_commit_sequence";
const LATEST_SNAPSHOT_KEY: &str = "latest_snapshot";
const NEXT_SUBSCRIPTION_KEY: &str = "next_subscription";
const COMMIT_LOG: sema::Table<u64, CommitLogEntry> = sema::Table::new("__sema_engine_commit_log");
const SUBSCRIPTIONS: sema::Table<u64, SubscriptionRegistration> =
    sema::Table::new("__sema_engine_subscriptions");

pub struct Engine {
    storage: sema::Sema,
    catalog: Catalog,
    subscriptions: SubscriptionRegistry,
}

impl Engine {
    pub fn open(request: EngineOpen) -> Result<Self> {
        let storage = sema::Sema::open_with_schema(request.path(), request.schema())?;
        let registrations = storage
            .read(|transaction| CATALOG.iter(transaction))?
            .into_iter()
            .map(|(_key, registration)| registration)
            .collect();
        let catalog = Catalog::new(registrations);
        Ok(Self {
            storage,
            catalog,
            subscriptions: SubscriptionRegistry::new(),
        })
    }

    pub fn register_table<RecordValue>(
        &mut self,
        descriptor: TableDescriptor<RecordValue>,
    ) -> Result<TableReference<RecordValue>> {
        let registration = TableRegistration::new(descriptor.name());
        if !self.catalog.is_registered(descriptor.name()) {
            self.storage.write(|transaction| {
                CATALOG.insert(transaction, descriptor.name().as_str(), &registration)
            })?;
            self.catalog.insert(registration)?;
        }
        Ok(TableReference::new(*descriptor.name()))
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
        self.ensure_registered(assertion.table())?;

        let key = assertion.record().record_key();
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
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                assertion.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
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
        self.ensure_registered(mutation.table())?;

        let key = mutation.record().record_key();
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
        self.storage.write(|transaction| {
            mutation.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                mutation.record(),
            )?;
            COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
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
        RecordValue: EngineStoredRecord + Send + Sync + 'static,
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
        let removed = self.storage.write(|transaction| {
            let removed = retraction
                .table()
                .sema_table()
                .remove(transaction, key.to_owned_string())?;
            if removed {
                COMMIT_LOG.insert(transaction, commit_sequence.value(), &entry)?;
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
        RecordValue: EngineStoredRecord,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.ensure_registered(query.table())?;

        let snapshot = self.latest_snapshot()?;
        let records = match query.read_plan().node() {
            crate::ReadPlanNode::AllRows => self.storage.read(|transaction| {
                Ok(query
                    .table()
                    .sema_table()
                    .iter(transaction)?
                    .into_iter()
                    .map(|(_key, record)| record)
                    .collect())
            })?,
            crate::ReadPlanNode::ByKey(key) => self.storage.read(|transaction| {
                Ok(query
                    .table()
                    .sema_table()
                    .get(transaction, key.to_owned_string())?
                    .into_iter()
                    .collect())
            })?,
            crate::ReadPlanNode::ByKeyRange(range) => self.storage.read(|transaction| {
                Ok(query
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
                    .collect())
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
            snapshot,
            records,
        ))
    }

    pub fn validate<RecordValue>(
        &self,
        query: QueryPlan<RecordValue>,
    ) -> Result<crate::ValidationReceipt>
    where
        RecordValue: EngineStoredRecord,
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
            snapshot.snapshot(),
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

    pub fn commit_log(&self) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    pub fn replay_from_sequence(
        &self,
        start: crate::CommitSequence,
    ) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .commit_log()?
            .into_iter()
            .filter(|entry| entry.commit_sequence() >= start)
            .collect())
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

    pub fn storage_kernel(&self) -> &sema::Sema {
        &self.storage
    }

    fn next_snapshot(&self) -> Result<SnapshotIdentifier> {
        Ok(self.latest_snapshot()?.next())
    }

    fn next_commit_sequence(&self) -> Result<crate::CommitSequence> {
        Ok(self.current_commit_sequence()?.next())
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
                .map(SubscriptionId::new)
                .unwrap_or_else(SubscriptionId::first))
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
}

impl EngineOpen {
    pub fn new(path: impl Into<PathBuf>, version: SchemaVersion) -> Self {
        Self {
            path: path.into(),
            schema: Schema { version },
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}
