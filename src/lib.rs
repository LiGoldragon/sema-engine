//! Full typed database engine over the `sema` storage kernel.
//!
//! `Engine` composes `sema::Sema` and executes Sema database operations
//! over registered typed record families. Components own daemons,
//! actors, sockets, authorization, and domain validation; this crate is
//! only the reusable engine library.
//!
//! The versioned commit log is the authoritative source of truth for
//! component Sema state; the table store is a rebuildable materialized
//! view folded from it. Every durable write goes through the engine's
//! logged choke points — the storage kernel hands out read access only.

pub mod catalog;
pub mod checkpoint;
pub mod commit_log;
pub mod compaction;
pub mod engine;
pub mod error;
pub mod fold;
pub mod import;
pub mod log;
pub mod mutation;
pub mod outbox;
pub mod query;
pub mod record;
pub mod sequence;
pub mod snapshot;
pub mod staging;
pub mod subscribe;
pub mod table;
pub mod versioning;

pub use catalog::{Catalog, TableRegistration};
pub use checkpoint::{
    Checkpoint, CheckpointDigest, CheckpointMetadata, CheckpointReceipt, CheckpointSegment,
    CheckpointSequence, CommitSequenceRange, FamilyInventory, IdentifiedCounter,
    PortableCheckpoint, SegmentDigest, SegmentReference,
};
pub use compaction::{CompactionFault, CompactionIntent, CompactionPhase};
pub use engine::{Engine, EngineOpen, StorageReader};
pub use error::{Error, Result};
pub use fold::{FamilyDirectory, RebuildReceipt, RowMaterializer, ViewDigest, ViewRow};
pub use import::{ImportReceipt, ImportSession};
pub use log::{CommitLogEntry, CommitLogOperation};
pub use mutation::{
    Assertion, CommitReceipt, CommitRequest, IdentifiedAssertion, IdentifiedMutation,
    IdentifiedMutationReceipt, IdentifiedRetraction, KeyedAssertion, KeyedMutation, Mutation,
    MutationReceipt, Retraction, WriteOperation,
};
pub use outbox::{Durability, MirrorAcknowledgement, MirrorHead, OutboxEntry};
pub use query::{
    AggregatePlan, FieldSelection, IdentifiedQueryPlan, IdentifiedQuerySnapshot,
    IdentifiedReadPlan, IdentifiedReadPlanNode, KeyRange, PredicatePlan, QueryFilter, QueryPlan,
    QuerySnapshot, ReadOperator, ReadPlan, ReadPlanNode, RecordIdentifierRange, RecursionMode,
    RuleSetRef, UnificationPlan, ValidationReceipt,
};
pub use record::{
    EngineRecord, EngineStoredRecord, EngineStoredValue, RecordIdentifier, RecordKey, RecordKeyKind,
};
pub use sema::{
    Error as StorageKernelError, ReadTransaction as StorageReadTransaction,
    Result as StorageKernelResult, SchemaVersion, Table as StorageKernelTable,
};
pub use sequence::CommitSequence;
pub use snapshot::{DatabaseMarker, SnapshotIdentifier};
pub use staging::{
    DeltaAnnouncer, DiscardStagedReceipt, MaterializeStagedReceipt, StageReceipt,
    StagedGroupSummary, StagedOperationGroup,
};
pub use subscribe::{
    DeltaKind, InitialSnapshot, SequenceRange, SinkError, SubscriptionDeliveryFailure,
    SubscriptionDeliveryMode, SubscriptionDelta, SubscriptionEvent, SubscriptionFanoutFailure,
    SubscriptionHandle, SubscriptionIdentifier, SubscriptionReceipt, SubscriptionRegistration,
    SubscriptionSink,
};
pub use table::{
    IdentifiedRecord, IdentifiedTableDescriptor, IdentifiedTableReference, TableDescriptor,
    TableName, TableReference,
};
pub use versioning::{
    EntryDigest, FamilyIdentity, FamilyName, ReplayReceipt, SchemaHash, StoreSchemaHash,
    VersionedCommitLogEntry, VersionedHistoryAcknowledgement, VersionedHistoryCompaction,
    VersionedHistoryRetention, VersionedLogOperation, VersionedPayload, VersionedRecoveryTopology,
    VersionedReplay, VersionedStoreName, VersioningPolicy,
};
