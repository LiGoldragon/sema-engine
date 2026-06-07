use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::CommitSequence;

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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
pub struct SnapshotIdentifier(u64);

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
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
pub struct DatabaseMarker {
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
}

impl SnapshotIdentifier {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

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

impl DatabaseMarker {
    pub const fn new(commit_sequence: CommitSequence, snapshot: SnapshotIdentifier) -> Self {
        Self {
            commit_sequence,
            snapshot,
        }
    }

    pub const fn genesis() -> Self {
        Self::new(CommitSequence::genesis(), SnapshotIdentifier::genesis())
    }

    pub const fn commit_sequence(self) -> CommitSequence {
        self.commit_sequence
    }

    pub const fn snapshot(self) -> SnapshotIdentifier {
        self.snapshot
    }
}
