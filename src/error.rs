use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("table is already registered: {table}")]
    TableAlreadyRegistered { table: String },

    #[error("table is not registered: {table}")]
    TableNotRegistered { table: String },
}

pub type Result<T> = std::result::Result<T, Error>;
