use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error(
        "engine storage layout {stored} does not match this build's layout {expected}; \
         the store predates typed family identity and must be rebuilt"
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

    #[error("commit request contains duplicate key: {table}/{key}")]
    DuplicateWriteKey { table: String, key: String },

    #[error("assert key already exists: {table}/{key}")]
    DuplicateAssertKey { table: String, key: String },

    #[error("versioned payload encode failed for table {table}: {message}")]
    VersionedPayloadEncode { table: String, message: String },

    #[error("versioned payload decode failed for table {table}: {message}")]
    VersionedPayloadDecode { table: String, message: String },

    #[error(
        "versioned log operation for family {family} has no record key; replay needs keyed operations"
    )]
    ReplayMissingKey { family: String },

    #[error("versioned replay does not apply operation {operation}")]
    ReplayUnsupportedOperation { operation: String },

    #[error("subscription registry lock poisoned")]
    SubscriptionRegistryPoisoned,

    #[error("subscription sink failed before registration: {message}")]
    SubscriptionSink { message: String },

    #[error("read plan operator is not implemented yet: {operator:?}")]
    UnsupportedReadPlan { operator: crate::ReadOperator },
}

pub type Result<T> = std::result::Result<T, Error>;
