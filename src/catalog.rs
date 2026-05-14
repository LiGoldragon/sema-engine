use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{Error, Result, TableName};

#[derive(Debug, Clone)]
pub struct Catalog {
    registrations: Vec<TableRegistration>,
}

impl Catalog {
    pub fn new(registrations: Vec<TableRegistration>) -> Self {
        Self { registrations }
    }

    pub fn is_registered(&self, name: &TableName) -> bool {
        self.registrations
            .iter()
            .any(|registration| registration.table_name() == name.as_str())
    }

    pub fn insert(&mut self, registration: TableRegistration) -> Result<()> {
        if self
            .registrations
            .iter()
            .any(|existing| existing.table_name() == registration.table_name())
        {
            return Err(Error::TableAlreadyRegistered {
                table: registration.table_name().to_owned(),
            });
        }
        self.registrations.push(registration);
        Ok(())
    }

    pub fn registrations(&self) -> &[TableRegistration] {
        &self.registrations
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct TableRegistration {
    table_name: String,
}

impl TableRegistration {
    pub fn new(name: &TableName) -> Self {
        Self {
            table_name: name.as_str().to_owned(),
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}
