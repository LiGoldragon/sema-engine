use std::marker::PhantomData;

use crate::{FamilyIdentity, FamilyName, RecordIdentifier, SchemaHash};

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
        format!("{}:next_record_identifier", self.value)
    }
}

impl From<TableName> for String {
    fn from(name: TableName) -> Self {
        name.as_str().to_owned()
    }
}

/// Declaration of a domain-keyed record family: the current table
/// coordinate plus the typed family identity the engine persists in
/// its catalog and stamps into every versioned log operation.
#[derive(Debug, Clone)]
pub struct TableDescriptor<RecordValue> {
    name: TableName,
    family: FamilyName,
    schema_hash: SchemaHash,
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
