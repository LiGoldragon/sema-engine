use std::marker::PhantomData;
use std::sync::Arc;

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use crate::{
    DomainRecordKey, EngineStoredValue, FamilyIdentity, FamilyName, KeyedAssertion, QueryPlan,
    RecordIdentifier, RecordKey, Result, SchemaHash,
};

/// Key suffix for one identified table's durable next-record-identifier
/// counter row. Checkpoint inventories reuse it so an imported store
/// restores the exact counter keys the original engine wrote.
pub(crate) const IDENTIFIED_COUNTER_SUFFIX: &str = "next_record_identifier";

/// The generated declaration of one domain-keyed Sema table.
///
/// Implementations provide only structural data: the current redb coordinate,
/// stable family identity, schema identity, stored record type, and authored
/// key type. The provided constructors are the sole shared lowering from that
/// declaration into the engine API.
pub trait TableSpecification {
    /// The value stored in the table.
    type Record;

    /// The authored type that addresses one record.
    type Key: DomainRecordKey;

    /// Current redb table coordinate.
    const TABLE_NAME: TableName;

    /// Stable encoded family identity; independent of the current spelling.
    const FAMILY_NAME: &'static str;

    /// Content identity of the table declaration's record/key shape.
    const SCHEMA_HASH: SchemaHash;

    /// Build the descriptor registered by the owning component's engine.
    fn descriptor() -> TableDescriptor<Self::Record> {
        TableDescriptor::new(
            Self::TABLE_NAME,
            FamilyName::new(Self::FAMILY_NAME),
            Self::SCHEMA_HASH,
        )
    }

    /// Build the typed table coordinate used by read and write requests.
    fn table_reference() -> TableReference<Self::Record> {
        TableReference::new(Self::TABLE_NAME)
    }

    /// Archive one authored key into the engine's domain-key representation.
    fn record_key(key: &Self::Key) -> Result<RecordKey> {
        key.to_record_key()
    }

    /// Build a keyed assertion whose key type is fixed by this declaration.
    fn assertion(key: &Self::Key, record: Self::Record) -> Result<KeyedAssertion<Self::Record>> {
        Ok(KeyedAssertion::new(
            Self::table_reference(),
            Self::record_key(key)?,
            record,
        ))
    }

    /// Build an exact-key query whose key type is fixed by this declaration.
    fn query(key: &Self::Key) -> Result<QueryPlan<Self::Record>> {
        Ok(QueryPlan::key(
            Self::table_reference(),
            Self::record_key(key)?,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableName {
    value: &'static str,
}

impl TableName {
    pub const fn new(value: &'static str) -> Self {
        Self { value }
    }

    pub fn as_str(&self) -> &'static str {
        self.value
    }

    pub fn identified_counter_key(&self) -> String {
        format!("{}:{IDENTIFIED_COUNTER_SUFFIX}", self.value)
    }
}

impl From<TableName> for String {
    fn from(name: TableName) -> Self {
        name.as_str().to_owned()
    }
}

/// One prior generation of a domain-keyed family's stored shape: the
/// per-family schema hash a store's catalog may still name, plus the
/// typed carry that reads every row in that generation's own decoded
/// shape and converts it forward to the current record type.
///
/// The carry closure captures the prior Rust type and its conversion,
/// so the engine can run an evolution without knowing the prior shape:
/// it lends the closure a read transaction and receives current-shape
/// rows keyed by their stored keys. Decoding is validated — bytes that
/// do not check as the declared prior shape surface a typed decode
/// error rather than a reinterpretation.
pub struct EvolutionStep<RecordValue> {
    prior: SchemaHash,
    carry: Arc<CarryPriorRows<RecordValue>>,
}

/// Reads every row of the named table in a prior generation's shape
/// and returns the rows converted to the current record type, keyed
/// by their stored keys. The carry runs entirely inside the storage
/// kernel's read surface, so it speaks the kernel's result type.
type CarryPriorRows<RecordValue> = dyn Fn(&sema::ReadTransaction, TableName) -> sema::Result<Vec<(String, RecordValue)>>
    + Send
    + Sync;

impl<RecordValue> EvolutionStep<RecordValue> {
    /// Declare that rows stored under `prior` decode as `PriorShape`
    /// and convert forward with `convert`. A byte-compatible prior —
    /// one whose rows already decode as the current type — passes the
    /// current record type as `PriorShape` with the identity
    /// conversion.
    pub fn from_prior<PriorShape>(
        prior: SchemaHash,
        convert: impl Fn(PriorShape) -> RecordValue + Send + Sync + 'static,
    ) -> Self
    where
        RecordValue: 'static,
        PriorShape: EngineStoredValue + Send + Sync + 'static,
        <PriorShape as rkyv::Archive>::Archived: rkyv::Deserialize<PriorShape, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let carry = move |transaction: &sema::ReadTransaction, table: TableName| {
            Ok(sema::Table::<String, PriorShape>::new(table.as_str())
                .iter(transaction)?
                .into_iter()
                .map(|(key, prior_row)| (key, convert(prior_row)))
                .collect())
        };
        Self {
            prior,
            carry: Arc::new(carry),
        }
    }

    pub fn prior_schema_hash(&self) -> SchemaHash {
        self.prior
    }

    pub(crate) fn carry_rows(
        &self,
        transaction: &sema::ReadTransaction,
        table: TableName,
    ) -> sema::Result<Vec<(String, RecordValue)>> {
        (self.carry)(transaction, table)
    }
}

impl<RecordValue> Clone for EvolutionStep<RecordValue> {
    fn clone(&self) -> Self {
        Self {
            prior: self.prior,
            carry: Arc::clone(&self.carry),
        }
    }
}

impl<RecordValue> std::fmt::Debug for EvolutionStep<RecordValue> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EvolutionStep")
            .field("prior", &self.prior)
            .finish_non_exhaustive()
    }
}

/// Declaration of a domain-keyed record family: the current table
/// coordinate plus the typed family identity the engine persists in
/// its catalog and stamps into every versioned log operation. A
/// declaration may additionally carry the family's evolution — the
/// prior stored shapes the engine may find in an existing store's
/// catalog and how to read each forward — via [`Self::with_prior`].
#[derive(Debug, Clone)]
pub struct TableDescriptor<RecordValue> {
    name: TableName,
    family: FamilyName,
    schema_hash: SchemaHash,
    evolution: Vec<EvolutionStep<RecordValue>>,
    record: PhantomData<RecordValue>,
}

#[derive(Debug, Clone)]
pub struct IdentifiedTableDescriptor<RecordValue> {
    name: TableName,
    family: FamilyName,
    schema_hash: SchemaHash,
    record: PhantomData<RecordValue>,
}

impl<RecordValue> TableDescriptor<RecordValue> {
    pub fn new(name: TableName, family: FamilyName, schema_hash: SchemaHash) -> Self {
        Self {
            name,
            family,
            schema_hash,
            evolution: Vec::new(),
            record: PhantomData,
        }
    }

    /// Declare one prior stored generation of this family: rows found
    /// under `prior` decode as `PriorShape` and convert forward with
    /// `convert`. Registration against a store whose catalog names a
    /// declared prior migrates the rows through the engine; an
    /// undeclared stored identity keeps failing closed.
    pub fn with_prior<PriorShape>(
        mut self,
        prior: SchemaHash,
        convert: impl Fn(PriorShape) -> RecordValue + Send + Sync + 'static,
    ) -> Self
    where
        RecordValue: 'static,
        PriorShape: EngineStoredValue + Send + Sync + 'static,
        <PriorShape as rkyv::Archive>::Archived: rkyv::Deserialize<PriorShape, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.evolution
            .push(EvolutionStep::from_prior(prior, convert));
        self
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }

    pub fn family(&self) -> &FamilyName {
        &self.family
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    pub fn family_identity(&self) -> FamilyIdentity {
        FamilyIdentity::new(self.family.clone(), self.schema_hash, self.name)
    }

    /// The declared evolution step matching a stored catalog identity:
    /// same family name, and a declared prior equal to the stored
    /// per-family schema hash. A stored identity from a different
    /// family never matches — a table coordinate reused by another
    /// family is a genuine incompatibility, not an evolution.
    pub(crate) fn evolution_step_for(
        &self,
        stored: &FamilyIdentity,
    ) -> Option<&EvolutionStep<RecordValue>> {
        if stored.family() != &self.family {
            return None;
        }
        self.evolution
            .iter()
            .find(|step| step.prior_schema_hash() == stored.schema_hash())
    }
}

impl<RecordValue> IdentifiedTableDescriptor<RecordValue> {
    pub fn new(name: TableName, family: FamilyName, schema_hash: SchemaHash) -> Self {
        Self {
            name,
            family,
            schema_hash,
            record: PhantomData,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }

    pub fn family(&self) -> &FamilyName {
        &self.family
    }

    pub fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    pub fn family_identity(&self) -> FamilyIdentity {
        FamilyIdentity::new(self.family.clone(), self.schema_hash, self.name)
    }
}

#[derive(Debug)]
pub struct TableReference<RecordValue> {
    name: TableName,
    record: PhantomData<RecordValue>,
}

#[derive(Debug)]
pub struct IdentifiedTableReference<RecordValue> {
    name: TableName,
    record: PhantomData<RecordValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedRecord<RecordValue> {
    identifier: RecordIdentifier,
    value: RecordValue,
}

impl<RecordValue> TableReference<RecordValue> {
    pub const fn new(name: TableName) -> Self {
        Self {
            name,
            record: PhantomData,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }

    pub fn sema_table(&self) -> sema::Table<String, RecordValue> {
        sema::Table::new(self.name.as_str())
    }
}

impl<RecordValue> IdentifiedTableReference<RecordValue> {
    pub const fn new(name: TableName) -> Self {
        Self {
            name,
            record: PhantomData,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }

    pub fn sema_table(&self) -> sema::Table<u64, RecordValue> {
        sema::Table::new(self.name.as_str())
    }
}

impl<RecordValue> IdentifiedRecord<RecordValue> {
    pub fn new(identifier: RecordIdentifier, value: RecordValue) -> Self {
        Self { identifier, value }
    }

    pub fn identifier(&self) -> RecordIdentifier {
        self.identifier
    }

    pub fn value(&self) -> &RecordValue {
        &self.value
    }

    pub fn into_value(self) -> RecordValue {
        self.value
    }
}

impl<RecordValue> Clone for TableReference<RecordValue> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<RecordValue> Copy for TableReference<RecordValue> {}

impl<RecordValue> Clone for IdentifiedTableReference<RecordValue> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<RecordValue> Copy for IdentifiedTableReference<RecordValue> {}
