use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("table is already registered: {table}")]
    TableAlreadyRegistered { table: String },

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

    #[error("subscription registry lock poisoned")]
    SubscriptionRegistryPoisoned,

    #[error("subscription sink failed before registration: {message}")]
    SubscriptionSink { message: String },

    #[error("read plan operator is not implemented yet: {operator:?}")]
    UnsupportedReadPlan { operator: crate::ReadOperator },
}

pub type Result<T> = std::result::Result<T, Error>;
