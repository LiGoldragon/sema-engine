use std::marker::PhantomData;

use crate::RecordIdentifier;

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

#[derive(Debug, Clone, Copy)]
pub struct TableDescriptor<RecordValue> {
    name: TableName,
    record: PhantomData<RecordValue>,
}

#[derive(Debug, Clone, Copy)]
pub struct IdentifiedTableDescriptor<RecordValue> {
    name: TableName,
    record: PhantomData<RecordValue>,
}

impl<RecordValue> TableDescriptor<RecordValue> {
    pub const fn new(name: TableName) -> Self {
        Self {
            name,
            record: PhantomData,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }
}

impl<RecordValue> IdentifiedTableDescriptor<RecordValue> {
    pub const fn new(name: TableName) -> Self {
        Self {
            name,
            record: PhantomData,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
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
