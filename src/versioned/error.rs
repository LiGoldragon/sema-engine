//! The crate-local typed error. Per `skills/rust/errors.md`, boundaries
//! carry a typed enum, never `anyhow`/`eyre`.

use thiserror::Error;

use super::identity::{EntryDigest, SchemaHash};

#[derive(Debug, Error)]
pub enum VersionedError {
    /// The same-file backend's storage kernel failed.
    #[error("storage kernel: {0}")]
    Kernel(#[from] sema::Error),

    /// The separate-file backend's file IO failed.
    #[error("frame-log io: {0}")]
    Io(#[from] std::io::Error),

    /// rkyv could not encode a payload or envelope.
    #[error("rkyv encode: {0}")]
    Encode(rkyv::rancor::Error),

    /// rkyv could not decode a payload or envelope.
    #[error("rkyv decode: {0}")]
    Decode(rkyv::rancor::Error),

    /// A frame's length prefix or body ran past the end of the file —
    /// a torn append (crash mid-write). Recovery truncates at this offset.
    #[error("frame-log truncated at offset {offset}")]
    TruncatedFrame { offset: u64 },

    /// An entry's recomputed digest does not match its stored digest —
    /// the payload or header was tampered with or corrupted.
    #[error("corrupt entry at sequence {sequence}: recomputed digest does not match stored")]
    CorruptEntry { sequence: u64 },

    /// The hash chain is broken: an entry's recorded predecessor digest
    /// does not match the actual prior entry's digest.
    #[error(
        "broken hash chain at sequence {sequence}: entry records prev {recorded:?}, prior entry digest is {actual:?}"
    )]
    BrokenChain {
        sequence: u64,
        recorded: EntryDigest,
        actual: EntryDigest,
    },

    /// A payload's schema-hash matched no family in the component's closed
    /// record enum. This is the *correct* behaviour at the knife-edge:
    /// there is no open fallback to a generic-record blob (report 92 §6).
    #[error("unknown schema hash {0:?}: no family in the closed component record decodes it")]
    UnknownSchemaHash(SchemaHash),

    /// A `SchemaTransition` named a (from, to) pair with no registered
    /// reducer. The library invokes reducers; the component authors them.
    #[error("no reducer registered for schema transition {from:?} -> {to:?}")]
    MissingReducer { from: SchemaHash, to: SchemaHash },
}
