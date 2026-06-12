use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{Error, FamilyIdentity, Result, TableName};

/// The engine's registered family inventory. Each registration binds
/// one family identity to its current table coordinate; the derived
/// store-level schema hash folds over this inventory.
#[derive(Debug, Clone)]
pub struct Catalog {
    registrations: Vec<TableRegistration>,
}

impl Catalog {
    pub fn new(registrations: Vec<TableRegistration>) -> Self {
        Self { registrations }
    }

    pub fn is_registered(&self, name: &TableName) -> bool {
        self.registration_for_table(name.as_str()).is_some()
    }

    /// The persisted family identity bound to a table name, if any.
    pub fn family_identity(&self, name: &TableName) -> Option<&FamilyIdentity> {
        self.registration_for_table(name.as_str())
            .map(TableRegistration::identity)
    }

    /// The registration already carrying this family version — same
    /// family name and schema hash — regardless of table coordinate.
    pub fn registration_for_family(&self, identity: &FamilyIdentity) -> Option<&TableRegistration> {
        self.registrations
            .iter()
            .find(|registration| registration.identity().shares_family(identity))
    }

    pub fn insert(&mut self, registration: TableRegistration) -> Result<()> {
        if self
            .registration_for_table(registration.table_name())
            .is_some()
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

    fn registration_for_table(&self, table_name: &str) -> Option<&TableRegistration> {
        self.registrations
            .iter()
            .find(|registration| registration.table_name() == table_name)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct TableRegistration {
    identity: FamilyIdentity,
}

impl TableRegistration {
    pub fn new(identity: FamilyIdentity) -> Self {
        Self { identity }
    }

    pub fn identity(&self) -> &FamilyIdentity {
        &self.identity
    }

    pub fn table_name(&self) -> &str {
        self.identity.table_name()
    }
}
