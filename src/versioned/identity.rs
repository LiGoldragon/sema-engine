//! The identity newtypes: the ordered cursor, the content digests, the
//! schema-hash decoder selector, and the record key.
//!
//! Report 92 §1: digests sit *beside* the monotonic cursor, never
//! replacing it — a blake3 digest is not sortable, has no `next()`, and
//! answers no range query. So [`CommitSequence`] is the cursor;
//! [`EntryDigest`] / [`SchemaHash`] are the content-addressed identities
//! layered beside it.

use rkyv::{Archive, Deserialize, Serialize};

/// Content-addressed identity of one record family's exact rkyv type set.
///
/// This is the typed decoder selector the report calls for: it replaces the
/// catalog's bare `table_name: String` (`sema-engine` `catalog.rs:42-44`)
/// with a collision-resistant hash that a *closed* match resolves to one
/// family. Derived here from a stable family label; in production it would
/// be derived from the family's structural schema so a field change yields
/// a new hash (forcing a `SchemaTransition`).
#[derive(
    Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[rkyv(derive(Debug))]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derive a family's schema-hash from a stable label. Spike-grade: a
    /// production hash would fold the structural schema, not a name.
    pub fn of_label(label: &str) -> Self {
        Self(*blake3::hash(label.as_bytes()).as_bytes())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// blake3 of an entry's canonical bytes. The log is a hash chain: each
/// entry records the prior entry's digest, so the head digest content-
/// addresses the whole database version (report 92 §1).
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct EntryDigest([u8; 32]);

impl EntryDigest {
    /// The genesis predecessor — the prior digest of the first entry.
    pub const ZERO: EntryDigest = EntryDigest([0u8; 32]);

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The ordered cursor. Sortable, `next()`-able, range-queryable — the
/// properties a content digest lacks, which is why it stays beside the
/// digest rather than being replaced by it.
#[derive(
    Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[rkyv(derive(Debug))]
pub struct CommitSequence(u64);

impl CommitSequence {
    pub const fn genesis() -> Self {
        Self(0)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A record's key within its family, as bytes so any keying scheme fits.
/// The owning family decides the encoding via [`super::RecordFamily::record_key`].
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[rkyv(derive(Debug))]
pub struct RecordKey(Vec<u8>);

impl RecordKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
