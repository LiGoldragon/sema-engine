//! The component-record contract: the closed enum a component instantiates
//! over its sealed families, plus the view fold coordinate.

use super::error::VersionedError;
use super::identity::{RecordKey, SchemaHash};

/// Where a record folds in the materialized view: family-qualified so two
/// families never collide on a shared key value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FoldKey {
    family: SchemaHash,
    key: RecordKey,
}

impl FoldKey {
    pub fn new(family: SchemaHash, key: RecordKey) -> Self {
        Self { family, key }
    }
    pub fn family(&self) -> SchemaHash {
        self.family
    }
    pub fn key(&self) -> &RecordKey {
        &self.key
    }
}

/// The per-component closed sum over its sealed families. The ONE place
/// schema-hash dispatch happens — a closed match, no open map, no fallback.
/// Implemented per component (macro-emitted in production; hand-written in
/// the `cfg(test)` demo here).
pub trait ComponentRecord: Sized {
    /// Closed-sum decode: resolve the family by schema-hash and decode.
    /// Returns [`VersionedError::UnknownSchemaHash`] for an unknown hash —
    /// never a generic-record fallback. Crossing that line is what the
    /// report (§6) names as the regression to the forbidden store.
    fn decode(schema_hash: SchemaHash, payload: &[u8]) -> Result<Self, VersionedError>;

    /// The fold coordinate this record occupies in the view.
    fn fold_key(&self) -> FoldKey;

    /// Canonical bytes this record contributes to the view's content digest.
    fn digest_contribution(&self) -> Vec<u8>;

    /// Re-encode to (current schema-hash, payload bytes). Migration replay
    /// uses this to feed a record's bytes into a reducer.
    fn reencode(&self) -> Result<(SchemaHash, Vec<u8>), VersionedError>;
}
