//! The sealed record-family trait — the typed unit of genericity.

use super::error::VersionedError;
use super::identity::{RecordKey, SchemaHash};

/// Seal: only in-crate code may implement [`RecordFamily`]. This is what
/// keeps the family set *closed* — the report's hard requirement against an
/// open, stringly-keyed, generic-record store (§4 knife-edge). In
/// production this module is `#[doc(hidden)] pub` and a derive macro emits
/// the `Sealed` + `RecordFamily` impls into the component crate; here the
/// `cfg(test)` demo hand-writes that generated code, in-crate.
pub(crate) mod sealed {
    pub trait Sealed {}
}

/// One record family: one concrete rkyv type set at one schema version.
///
/// `encode`/`decode` are required (not defaulted) so the rkyv serializer
/// bounds land on the *concrete* `Value` at each impl site — which is what
/// a derive macro would emit, and which keeps the generic core free of the
/// `for<'a> Serialize<...>` HRTB bounds that resist `dyn` and clutter
/// generic call sites.
pub trait RecordFamily: sealed::Sealed + Sized {
    /// The stored value type for this family.
    type Value: Clone;

    /// A stable label identifying this exact type set. Spike-grade schema
    /// identity; production would fold the structural schema so a field
    /// change yields a new hash and forces a `SchemaTransition`.
    fn schema_label() -> &'static str;

    /// The content-addressed selector for this family.
    fn schema_hash() -> SchemaHash {
        SchemaHash::of_label(Self::schema_label())
    }

    /// Extract the record's key from its value.
    fn record_key(value: &Self::Value) -> RecordKey;

    /// Encode a value to payload bytes (rkyv, at this family's concrete type).
    fn encode(value: &Self::Value) -> Result<Vec<u8>, VersionedError>;

    /// Decode payload bytes back to a typed value of this family.
    fn decode(bytes: &[u8]) -> Result<Self::Value, VersionedError>;
}
