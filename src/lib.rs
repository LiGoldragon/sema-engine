//! Full typed database engine over the `sema` storage kernel.
//!
//! `Engine` composes `sema::Sema` and executes Sema database operations
//! over registered typed record families. Components own daemons,
//! actors, sockets, authorization, and domain validation; this crate is
//! only the reusable engine library.

pub mod catalog;
pub mod engine;
pub mod error;
pub mod log;
pub mod mutation;
pub mod query;
pub mod record;
pub mod sequence;
pub mod snapshot;
pub mod subscribe;
pub mod table;

pub use catalog::{Catalog, TableRegistration};
pub use engine::{Engine, EngineOpen};
pub use error::{Error, Result};
pub use log::{CommitLogEntry, CommitLogOperation};
pub use mutation::{
    Assertion, CommitReceipt, CommitRequest, IdentifiedAssertion, IdentifiedMutation,
    IdentifiedMutationReceipt, IdentifiedRetraction, KeyedAssertion, KeyedMutation, Mutation,
    MutationReceipt, Retraction, WriteOperation,
};
pub use query::{
    AggregatePlan, FieldSelection, IdentifiedQueryPlan, IdentifiedQuerySnapshot,
    IdentifiedReadPlan, IdentifiedReadPlanNode, KeyRange, PredicatePlan, QueryFilter, QueryPlan,
    QuerySnapshot, ReadOperator, ReadPlan, ReadPlanNode, RecordIdentifierRange, RecursionMode,
    RuleSetRef, UnificationPlan, ValidationReceipt,
};
pub use record::{
    EngineRecord, EngineStoredRecord, EngineStoredValue, RecordIdentifier, RecordKey,
};
pub use sema::{
    Error as StorageKernelError, Result as StorageKernelResult, SchemaVersion,
    Sema as StorageKernel, Table as StorageKernelTable,
};
pub use sequence::CommitSequence;
pub use snapshot::{DatabaseMarker, SnapshotIdentifier};
pub use subscribe::{
    DeltaKind, InitialSnapshot, SequenceRange, SinkError, SubscriptionDeliveryFailure,
    SubscriptionDeliveryMode, SubscriptionDelta, SubscriptionEvent, SubscriptionHandle,
    SubscriptionIdentifier, SubscriptionReceipt, SubscriptionRegistration, SubscriptionSink,
};
pub use table::{
    IdentifiedRecord, IdentifiedTableDescriptor, IdentifiedTableReference, TableDescriptor,
    TableName, TableReference,
};
