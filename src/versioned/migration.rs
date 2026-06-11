//! Migration as a typed log entry plus a component-authored reducer.

use rkyv::{Archive, Deserialize, Serialize};

use super::error::VersionedError;
use super::identity::SchemaHash;

/// The data of a `SchemaTransition` log entry: the old and new family
/// schema-hashes. Carried in the entry payload. Report 92 §1: a migration
/// is a first-class typed entry, never a byte diff between type sets.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct SchemaTransition {
    from: SchemaHash,
    to: SchemaHash,
}

impl SchemaTransition {
    pub fn new(from: SchemaHash, to: SchemaHash) -> Self {
        Self { from, to }
    }
    pub fn from(&self) -> SchemaHash {
        self.from
    }
    pub fn to(&self) -> SchemaHash {
        self.to
    }

    /// The reserved selector for transition entries (they carry no family
    /// payload of their own — their payload is this record).
    pub fn marker_hash() -> SchemaHash {
        SchemaHash::of_label("__versioned.schema_transition")
    }

    pub fn encode(&self) -> Result<Vec<u8>, VersionedError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(VersionedError::Encode)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, VersionedError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes).map_err(VersionedError::Decode)
    }
}

/// A component-authored migration step. The library *invokes* it during
/// replay; only the component can author the domain transform (report 92
/// §6: reducer bodies encode irreducible domain meaning). Object-safe so a
/// [`ReducerSet`] can hold a heterogeneous set.
pub trait Reducer {
    fn from_schema(&self) -> SchemaHash;
    fn to_schema(&self) -> SchemaHash;
    /// Transform payload bytes under `from_schema` into bytes under
    /// `to_schema`.
    fn migrate(&self, old_payload: &[u8]) -> Result<Vec<u8>, VersionedError>;
}

/// The registered reducers for a component. A data-bearing noun that owns
/// the set, not a free-function bag. Reducer dispatch is behaviour
/// selection (by from/to schema-hash), distinct from the data-erasure the
/// report forbids.
pub struct ReducerSet {
    reducers: Vec<Box<dyn Reducer>>,
}

impl ReducerSet {
    pub fn new() -> Self {
        Self {
            reducers: Vec::new(),
        }
    }

    pub fn with(mut self, reducer: Box<dyn Reducer>) -> Self {
        self.reducers.push(reducer);
        self
    }

    pub fn find(&self, from: SchemaHash, to: SchemaHash) -> Option<&dyn Reducer> {
        self.reducers
            .iter()
            .find(|reducer| reducer.from_schema() == from && reducer.to_schema() == to)
            .map(|reducer| reducer.as_ref())
    }
}

impl Default for ReducerSet {
    fn default() -> Self {
        Self::new()
    }
}
