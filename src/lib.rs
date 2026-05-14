//! Full typed database engine over the `sema` storage kernel.
//!
//! `Engine` composes `sema::Sema` and executes Signal-shaped database
//! verbs over registered typed record families. Components own daemons,
//! actors, sockets, authorization, and domain validation; this crate is
//! only the reusable engine library.

pub mod catalog;
pub mod engine;
pub mod error;
pub mod mutation;
pub mod query;
pub mod record;
pub mod table;

pub use catalog::{Catalog, TableRegistration};
pub use engine::{Engine, EngineOpen};
pub use error::{Error, Result};
pub use mutation::{Assertion, MutationReceipt};
pub use query::{QueryFilter, QueryPlan, QuerySnapshot};
pub use record::{EngineRecord, EngineStoredRecord, RecordKey};
pub use table::{TableDescriptor, TableName, TableReference};
