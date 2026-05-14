use std::marker::PhantomData;

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
}

#[derive(Debug, Clone, Copy)]
pub struct TableDescriptor<RecordValue> {
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

#[derive(Debug)]
pub struct TableReference<RecordValue> {
    name: TableName,
    record: PhantomData<RecordValue>,
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

impl<RecordValue> Clone for TableReference<RecordValue> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<RecordValue> Copy for TableReference<RecordValue> {}
