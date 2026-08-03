use thiserror::Error;

use crate::{
    CheckpointDigest, CommitSequence, CompactionPhase, EntryDigest, SegmentDigest, StoreSchemaHash,
    ViewDigest,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("sema: {0}")]
    Sema(sema::Error),

    #[error(
        "engine storage layout {stored} does not match this build's layout {expected}; \
         the store was written under an older engine layout and must be rebuilt through \
         checkpoint import or versioned replay"
    )]
    StorageLayoutMismatch { stored: u64, expected: u64 },

    #[error("table is already registered: {table}")]
    TableAlreadyRegistered { table: String },

    #[error("table {table} is registered as {stored}, not as the declared {declared}")]
    FamilyIdentityMismatch {
        table: String,
        stored: String,
        declared: String,
    },

    #[error("family {family} is already bound to table {existing}; cannot bind table {table}")]
    FamilyAlreadyBound {
        family: String,
        existing: String,
        table: String,
    },

    #[error("table is not registered: {table}")]
    TableNotRegistered { table: String },

    #[error("record is not stored: {table}/{key}")]
    RecordNotFound { table: String, key: String },

    #[error("commit request is empty: {table}")]
    EmptyCommit { table: String },

    #[error("atomic commit is empty")]
    EmptyAtomicCommit,

    #[error("commit request contains duplicate key: {table}/{key}")]
    DuplicateWriteKey { table: String, key: String },

    #[error("assert key already exists: {table}/{key}")]
    DuplicateAssertKey { table: String, key: String },

    #[error("versioned payload encode failed for table {table}: {message}")]
    VersionedPayloadEncode { table: String, message: String },

    #[error("versioned payload decode failed for table {table}: {message}")]
    VersionedPayloadDecode { table: String, message: String },

    #[error("typed domain key archive failed: {message}")]
    DomainKeyArchive { message: String },

    #[error(
        "versioned log operation for family {family} has no record key; replay needs keyed operations"
    )]
    ReplayMissingKey { family: String },

    #[error("versioned replay does not apply operation {operation}")]
    ReplayUnsupportedOperation { operation: String },

    #[error(
        "versioned log carries a tombstone payload under a record-bearing operation for table {table}"
    )]
    ReplayTombstonePayload { table: String },

    #[error("{surface} requires a versioned store; open the engine with a VersioningPolicy")]
    VersioningNotEnabled { surface: String },

    #[error(
        "versioned log is incomplete: {commits} commit log entries but {versioned} versioned \
         entries; the store wrote history before versioning was enabled"
    )]
    VersionedLogIncomplete { commits: u64, versioned: u64 },

    #[error("versioned digest chain breaks at commit sequence {sequence}")]
    VersionedChainBroken { sequence: u64 },

    #[error("nothing to checkpoint: no versioned entries beyond the covered range")]
    CheckpointNothingToCover,

    #[error("checkpoint encode failed: {message}")]
    CheckpointEncode { message: String },

    #[error("checkpoint decode failed: {message}")]
    CheckpointDecode { message: String },

    #[error("checkpoint digest mismatch: stored {stored}, computed {computed}")]
    CheckpointDigestMismatch {
        stored: CheckpointDigest,
        computed: CheckpointDigest,
    },

    #[error("checkpoint segment digest mismatch: referenced {referenced}, computed {computed}")]
    SegmentDigestMismatch {
        referenced: SegmentDigest,
        computed: SegmentDigest,
    },

    #[error("checkpoint segment is missing from the store: {digest}")]
    SegmentMissing { digest: SegmentDigest },

    #[error(
        "checkpoint supplies {supplied} segments but its metadata references only {referenced}"
    )]
    SegmentSurplus { referenced: usize, supplied: usize },

    #[error("latest-checkpoint cursor names checkpoint {sequence}, which has no stored row")]
    CheckpointRowMissing { sequence: u64 },

    #[error("view digest mismatch: expected {expected}, computed {computed}")]
    ViewDigestMismatch {
        expected: ViewDigest,
        computed: ViewDigest,
    },

    #[error(
        "checkpoint store schema {checkpoint} does not match the current store schema {current}"
    )]
    CheckpointSchemaMismatch {
        checkpoint: StoreSchemaHash,
        current: StoreSchemaHash,
    },

    #[error("import requires a fresh store: this store already carries engine history")]
    ImportStoreNotFresh,

    #[error("import session has no ingested checkpoint")]
    ImportMissingCheckpoint,

    #[error("import session already ingested a checkpoint")]
    ImportCheckpointAlreadyIngested,

    #[error("import store name mismatch: checkpoint names {checkpoint}, policy names {policy}")]
    ImportStoreNameMismatch { checkpoint: String, policy: String },

    #[error("family {family} is not part of the restored inventory")]
    FamilyUnknown { family: String },

    #[error(
        "row for family {family} targets table {expected}, but the directory supplied table {supplied}"
    )]
    MaterializeTableMismatch {
        family: String,
        expected: String,
        supplied: String,
    },

    #[error("row key {key} for table {table} is a {found} key, expected {expected} key")]
    MaterializeKeyKindMismatch {
        table: String,
        key: String,
        expected: &'static str,
        found: &'static str,
    },

    #[error("no commit log entry at sequence {sequence}")]
    UnknownCommitSequence { sequence: u64 },

    #[error("outbox row at sequence {sequence} does not match its versioned log entry")]
    OutboxEntryMismatch { sequence: u64 },

    #[error("mirror head names sequence {sequence}, which has no outbox row")]
    MirrorHeadUnknown { sequence: u64 },

    #[error(
        "version-history compaction through sequence {required} requires a durable mirror acknowledgement, but the acknowledged head is {acknowledged:?}"
    )]
    HistoryCompactionUnacknowledged {
        required: u64,
        acknowledged: Option<u64>,
    },

    #[error(
        "version-history acknowledgement {acknowledgement} conflicts with the configured {topology} recovery topology"
    )]
    HistoryCompactionTopologyMismatch {
        topology: &'static str,
        acknowledgement: &'static str,
    },

    #[error(
        "versioned store has a durable policy; EngineOpen::with_versioning is required on every reopen"
    )]
    VersioningPolicyRequired,

    #[error("versioned store policy differs from the durable policy recorded at first open")]
    VersioningPolicyMismatch,

    #[error(
        "a durable compaction intent is pending in phase {phase:?}; open through Engine::open_recovering"
    )]
    CompactionRecoveryRequired { phase: CompactionPhase },

    #[error(
        "mirror head fork at sequence {sequence}: outbox recorded {recorded}, \
         acknowledgement names {acknowledged}"
    )]
    MirrorHeadForked {
        sequence: u64,
        recorded: EntryDigest,
        acknowledged: EntryDigest,
    },

    #[error(
        "a staging session is already engaged; park or abandon it before beginning another build"
    )]
    StagingSessionEngaged,

    #[error("a durable compaction intent is already pending; resume it before beginning another")]
    CompactionIntentOccupied,

    #[error("a durable compaction intent advanced without its final planned sequence")]
    CompactionIntentMissingSequence,

    #[error("verified checkpoint ends at {checkpoint}, behind compaction plan ending at {plan}")]
    CompactionCheckpointBehind { checkpoint: u64, plan: u64 },

    #[error("deterministic compaction interruption after {phase}")]
    CompactionInterrupted { phase: &'static str },

    #[error("no staging session is engaged; {surface} requires begin_staged_group first")]
    StagingSessionNotEngaged { surface: &'static str },

    #[error(
        "staging slot is occupied by a parked operation group; materialize or discard it first"
    )]
    StagingSlotOccupied,

    #[error("staging slot is empty: no staged operation group for {surface}")]
    StagingSlotEmpty { surface: &'static str },

    #[error(
        "staging base moved: the store advanced to commit sequence {sequence} after the staged \
         group was built; the stale group is refused"
    )]
    StagingBaseMoved { sequence: u64 },

    #[error(
        "staged group digest mismatch at commit sequence {sequence}: staged {staged}, \
         recomputed {computed}"
    )]
    StagedGroupDigestMismatch {
        sequence: u64,
        staged: EntryDigest,
        computed: EntryDigest,
    },

    #[error(
        "staged head mismatch: materialization produced {produced}, staged prospective head \
         is {staged}"
    )]
    StagedHeadMismatch {
        staged: EntryDigest,
        produced: EntryDigest,
    },

    #[error("subscription registry lock poisoned")]
    SubscriptionRegistryPoisoned,

    #[error("subscription failure log lock poisoned")]
    SubscriptionFailureLogPoisoned,

    #[error("subscription sink failed before registration: {message}")]
    SubscriptionSink { message: String },

    #[error("read plan operator is not implemented yet: {operator:?}")]
    UnsupportedReadPlan { operator: crate::ReadOperator },
}

impl Error {
    pub(crate) fn versioning_not_enabled(surface: &str) -> Self {
        Self::VersioningNotEnabled {
            surface: surface.to_owned(),
        }
    }

    pub(crate) fn unknown_commit_sequence(sequence: CommitSequence) -> Self {
        Self::UnknownCommitSequence {
            sequence: sequence.value(),
        }
    }

    /// Carry a typed engine error across the storage kernel's
    /// closure-scoped transaction boundary. The kernel rolls back the
    /// transaction on `Err`; `From<sema::Error>` unwraps the carried
    /// engine error back out, so the typed failure survives the round
    /// trip end to end.
    pub(crate) fn into_storage(self) -> sema::Error {
        sema::Error::Io(std::io::Error::other(Carrier(self)))
    }
}

impl From<sema::Error> for Error {
    fn from(error: sema::Error) -> Self {
        match error {
            sema::Error::Io(io) if io.get_ref().is_some_and(|inner| inner.is::<Carrier>()) => {
                let inner = io.into_inner().expect("carrier presence was just checked");
                inner
                    .downcast::<Carrier>()
                    .expect("carrier type was just checked")
                    .0
            }
            other => Self::Sema(other),
        }
    }
}

/// Wrapper smuggling a typed engine error through `sema::Error::Io`
/// so a failure inside a storage-kernel closure both rolls back the
/// transaction and resurfaces as the original engine error.
#[derive(Debug)]
struct Carrier(Error);

impl std::fmt::Display for Carrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for Carrier {}

pub type Result<T> = std::result::Result<T, Error>;
