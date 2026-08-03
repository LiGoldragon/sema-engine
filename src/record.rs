use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{Error, Result};

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[rkyv(derive(Debug))]
pub enum RecordKeyKind {
    Domain,
    Identifier,
}

impl RecordKeyKind {
    pub const fn digest_tag(self) -> u8 {
        match self {
            Self::Domain => 1,
            Self::Identifier => 2,
        }
    }
}

/// A record's key within its family table. A `Domain` key carries an
/// author-supplied string; an `Identifier` key carries the typed
/// [`RecordIdentifier`] the engine minted for an engine-identified
/// family. The two arms are the only legal key shapes, so the enum
/// itself is the discrimination — no string parsing recovers the kind.
#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[rkyv(derive(Debug))]
pub enum RecordKey {
    Domain(String),
    Identifier(RecordIdentifier),
}

impl RecordKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self::domain(value)
    }

    pub fn domain(value: impl Into<String>) -> Self {
        Self::Domain(value.into())
    }

    pub fn identifier(identifier: RecordIdentifier) -> Self {
        Self::Identifier(identifier)
    }

    pub fn kind(&self) -> RecordKeyKind {
        match self {
            Self::Domain(_) => RecordKeyKind::Domain,
            Self::Identifier(_) => RecordKeyKind::Identifier,
        }
    }

    /// The key's canonical string form: the domain string verbatim, or
    /// the identifier's decimal representation. This is the byte
    /// sequence the chain/view digest folds in (see `Self::update_digest`)
    /// and the redb key a domain-keyed family table stores under.
    pub fn to_owned_string(&self) -> String {
        match self {
            Self::Domain(value) => value.clone(),
            Self::Identifier(identifier) => identifier.value().to_string(),
        }
    }

    /// The typed identifier when this is an `Identifier` key; `None`
    /// for a `Domain` key. Infallible: the identifier is carried as a
    /// typed value, so no decimal-string parse can fail.
    pub fn identifier_value(&self) -> Option<RecordIdentifier> {
        match self {
            Self::Domain(_) => None,
            Self::Identifier(identifier) => Some(*identifier),
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        1 + self.to_owned_string().len()
    }

    pub(crate) fn update_digest(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&[self.kind().digest_tag()]);
        crate::EntryDigest::update_bytes(hasher, self.to_owned_string().as_bytes());
    }
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[rkyv(derive(Debug))]
pub struct RecordIdentifier(u64);

impl RecordIdentifier {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn first() -> Self {
        Self::new(1)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self::new(self.0 + 1)
    }
}

pub trait EngineRecord: Clone {
    fn record_key(&self) -> RecordKey;
}

pub trait EngineStoredValue:
    Archive
    + Clone
    + for<'serialize> RkyvSerialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'serialize>, Share>, rancor::Error>,
    >
where
    Self::Archived: RkyvDeserialize<Self, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

/// A typed value that can address a domain-keyed Sema table.
///
/// The complete validated portable archive is encoded into the storage
/// kernel's current string coordinate. Callers never choose that coordinate
/// or stringify the domain value themselves, and two equal typed keys always
/// produce the same redb key bytes.
pub trait DomainRecordKey {
    /// Project this typed key into the engine's domain-key sum.
    fn to_record_key(&self) -> Result<RecordKey>;
}

impl<RecordValue> EngineStoredValue for RecordValue
where
    RecordValue: Archive
        + Clone
        + for<'serialize> RkyvSerialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'serialize>, Share>, rancor::Error>,
        >,
    RecordValue::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

impl<KeyValue> DomainRecordKey for KeyValue
where
    KeyValue: EngineStoredValue,
    KeyValue::Archived: RkyvDeserialize<KeyValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
    fn to_record_key(&self) -> Result<RecordKey> {
        let bytes =
            rkyv::to_bytes::<rancor::Error>(self).map_err(|source| Error::DomainKeyArchive {
                message: source.to_string(),
            })?;
        let mut encoded = String::with_capacity(5 + bytes.len() * 2);
        encoded.push_str("rkyv:");
        for byte in bytes.iter() {
            use std::fmt::Write as _;
            write!(encoded, "{byte:02x}").expect("String writes cannot fail");
        }
        Ok(RecordKey::domain(encoded))
    }
}

pub trait EngineStoredRecord: EngineRecord + EngineStoredValue
where
    Self::Archived: RkyvDeserialize<Self, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

impl<RecordValue> EngineStoredRecord for RecordValue
where
    RecordValue: EngineRecord + EngineStoredValue,
    RecordValue::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

#[cfg(test)]
mod tests {
    use super::{RecordIdentifier, RecordKey};

    /// The exact byte sequence `RecordKey::update_digest` folds into the
    /// chain/view hash, reconstructed independently: the kind tag byte
    /// (unprefixed), then the canonical string length-prefixed
    /// little-endian followed by its bytes. Locking these bytes catches
    /// any drift in the hashed representation — most importantly, that an
    /// `Identifier` key hashes its DECIMAL-string bytes (matching every
    /// store written before `RecordKey` became a sum), never the raw
    /// `u64` little-endian bytes.
    fn expected_digest_input(tag: u8, canonical: &str) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&(canonical.len() as u64).to_le_bytes());
        bytes.extend_from_slice(canonical.as_bytes());
        bytes
    }

    fn digest_of(key: &RecordKey) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        key.update_digest(&mut hasher);
        *hasher.finalize().as_bytes()
    }

    fn digest_of_input(input: &[u8]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(input);
        *hasher.finalize().as_bytes()
    }

    #[test]
    fn domain_key_digest_matches_tag_one_and_string_bytes() {
        let key = RecordKey::domain("alpha");
        let expected = expected_digest_input(1, "alpha");
        assert_eq!(digest_of(&key), digest_of_input(&expected));
    }

    #[test]
    fn identifier_key_digest_matches_tag_two_and_decimal_string_bytes() {
        let key = RecordKey::identifier(RecordIdentifier::new(42));
        // Decimal string "42", NOT the u64 little-endian bytes.
        let expected = expected_digest_input(2, "42");
        assert_eq!(digest_of(&key), digest_of_input(&expected));
    }

    #[test]
    fn identifier_value_is_infallible_projection_of_the_arm() {
        assert_eq!(
            RecordKey::identifier(RecordIdentifier::new(7)).identifier_value(),
            Some(RecordIdentifier::new(7)),
        );
        assert_eq!(RecordKey::domain("alpha").identifier_value(), None);
    }

    #[test]
    fn canonical_string_form_is_stable_for_both_arms() {
        assert_eq!(RecordKey::domain("beta").to_owned_string(), "beta");
        assert_eq!(
            RecordKey::identifier(RecordIdentifier::new(1)).to_owned_string(),
            "1",
        );
    }
}
