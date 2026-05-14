//! Full typed database engine over the `sema` storage kernel.
//!
//! `Engine` composes `sema::Sema` and executes Signal-shaped database
//! verbs over registered typed record families. Components own daemons,
//! actors, sockets, authorization, and domain validation; this crate is
//! only the reusable engine library.

pub mod catalog;
pub mod engine;
pub mod error;
pub mod log;
pub mod mutation;
pub mod query;
pub mod record;
pub mod snapshot;
pub mod subscribe;
pub mod table;

pub use catalog::{Catalog, TableRegistration};
pub use engine::{Engine, EngineOpen};
pub use error::{Error, Result};
pub use log::OperationLogEntry;
pub use mutation::{Assertion, MutationReceipt};
pub use query::{
    AggregatePlan, FieldSelection, KeyRange, PredicatePlan, QueryFilter, QueryPlan, QuerySnapshot,
    ReadOperator, ReadPlan, ReadPlanNode, RecursionMode, RuleSetRef, UnificationPlan,
};
pub use record::{EngineRecord, EngineStoredRecord, RecordKey};
pub use snapshot::SnapshotId;
pub use subscribe::{
    DeltaKind, InitialSnapshot, SequenceRange, SinkError, SubscriptionDeliveryFailure,
    SubscriptionDeliveryMode, SubscriptionDelta, SubscriptionEvent, SubscriptionHandle,
    SubscriptionId, SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink,
};
pub use table::{TableDescriptor, TableName, TableReference};
