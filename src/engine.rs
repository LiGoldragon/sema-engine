use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema::{Schema, SchemaVersion};

use crate::subscribe::{ActiveSubscription, SubscriptionRegistry};
use crate::{
    Catalog, EngineStoredRecord, Error, InitialSnapshot, OperationLogEntry, QueryPlan,
    QuerySnapshot, Result, SequenceRange, SnapshotId, SubscriptionHandle, SubscriptionId,
    SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink, TableDescriptor,
    TableReference, TableRegistration,
};

const CATALOG: sema::Table<&'static str, TableRegistration> =
    sema::Table::new("__sema_engine_catalog");
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
const LATEST_SNAPSHOT_KEY: &str = "latest_snapshot";
const NEXT_SUBSCRIPTION_KEY: &str = "next_subscription";
const OPERATION_LOG: sema::Table<u64, OperationLogEntry> =
    sema::Table::new("__sema_engine_operation_log");
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
        if !self.catalog.is_registered(assertion.table().name()) {
            return Err(Error::TableNotRegistered {
                table: assertion.table().name().as_str().to_owned(),
            });
        }

        let key = assertion.record().record_key();
        let record = assertion.record().clone();
        let snapshot = self.next_snapshot()?;
        let operation = OperationLogEntry::new(
            snapshot,
            signal_core::SignalVerb::Assert,
            *assertion.table().name(),
            Some(key.clone()),
        );
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                assertion.record(),
            )?;
            OPERATION_LOG.insert(transaction, snapshot.value(), &operation)?;
            COUNTERS.insert(transaction, LATEST_SNAPSHOT_KEY, &snapshot.value())?;
            Ok(())
        })?;
        self.subscriptions
            .deliver_assert(*assertion.table().name(), &key, snapshot, &record)?;

        Ok(crate::MutationReceipt::new(
            signal_core::SignalVerb::Assert,
            *assertion.table().name(),
            key,
            snapshot,
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
        if !self.catalog.is_registered(query.table().name()) {
            return Err(Error::TableNotRegistered {
                table: query.table().name().as_str().to_owned(),
            });
        }

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
            signal_core::SignalVerb::Match,
            *query.table().name(),
            snapshot,
            records,
        ))
    }

    pub fn latest_snapshot(&self) -> Result<SnapshotId> {
        let value = self.storage.read(|transaction| {
            Ok(COUNTERS
                .get(transaction, LATEST_SNAPSHOT_KEY)?
                .map(SnapshotId::new)
                .unwrap_or_else(SnapshotId::genesis))
        })?;
        Ok(value)
    }

    pub fn operation_log(&self) -> Result<Vec<OperationLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| OPERATION_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    pub fn operation_log_range(&self, range: SequenceRange) -> Result<Vec<OperationLogEntry>> {
        Ok(self
            .operation_log()?
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

    fn next_snapshot(&self) -> Result<SnapshotId> {
        Ok(self.latest_snapshot()?.next())
    }

    fn next_subscription_handle<RecordValue>(
        &self,
        plan: &QueryPlan<RecordValue>,
        snapshot: SnapshotId,
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
