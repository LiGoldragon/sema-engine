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
pub struct RecordKey {
    kind: RecordKeyKind,
    value: String,
}

impl RecordKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self::domain(value)
    }

    pub fn domain(value: impl Into<String>) -> Self {
        Self {
            kind: RecordKeyKind::Domain,
            value: value.into(),
        }
    }

    pub fn identifier(identifier: RecordIdentifier) -> Self {
        Self {
            kind: RecordKeyKind::Identifier,
            value: identifier.value().to_string(),
        }
    }

    pub fn kind(&self) -> RecordKeyKind {
        self.kind
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn to_owned_string(&self) -> String {
        self.value.clone()
    }

    pub fn identifier_value(&self) -> Option<RecordIdentifier> {
        if self.kind != RecordKeyKind::Identifier {
            return None;
        }
        self.value.parse().ok().map(RecordIdentifier::new)
    }

    pub(crate) fn encoded_len(&self) -> usize {
        1 + self.value.len()
    }

    pub(crate) fn update_digest(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&[self.kind.digest_tag()]);
        crate::EntryDigest::update_bytes(hasher, self.value.as_bytes());
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
