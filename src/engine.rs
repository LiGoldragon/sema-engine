use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema::{Schema, SchemaVersion};

use crate::{
    Catalog, EngineStoredRecord, Error, QueryPlan, QuerySnapshot, Result, TableDescriptor,
    TableReference, TableRegistration,
};

const CATALOG: sema::Table<&'static str, TableRegistration> =
    sema::Table::new("__sema_engine_catalog");

pub struct Engine {
    storage: sema::Sema,
    catalog: Catalog,
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
        Ok(Self { storage, catalog })
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
        RecordValue: EngineStoredRecord,
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
        self.storage.write(|transaction| {
            assertion.table().sema_table().insert(
                transaction,
                key.to_owned_string(),
                assertion.record(),
            )
        })?;
        Ok(crate::MutationReceipt::new(
            signal_core::SemaVerb::Assert,
            *assertion.table().name(),
            key,
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

        let records = self.storage.read(|transaction| match query.filter() {
            crate::QueryFilter::All => Ok(query
                .table()
                .sema_table()
                .iter(transaction)?
                .into_iter()
                .map(|(_key, record)| record)
                .collect()),
            crate::QueryFilter::Key(key) => Ok(query
                .table()
                .sema_table()
                .get(transaction, key.to_owned_string())?
                .into_iter()
                .collect()),
        })?;

        Ok(QuerySnapshot::new(
            signal_core::SemaVerb::Match,
            *query.table().name(),
            records,
        ))
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn storage_path(&self) -> &Path {
        self.storage.path()
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
