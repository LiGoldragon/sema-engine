//! Mirror outbox: the durable queue between local commits and a
//! future mirror actor. Every versioned commit log entry gets an
//! outbox row in the same write transaction at every write choke
//! point, so the unshipped suffix is complete by construction. A
//! mirror actor reads the suffix, ships the matching versioned
//! entries (loaded through [`crate::Engine::versioned_replay_from_sequence`]),
//! and acknowledges the server-confirmed head, advancing a durable
//! shipped cursor. Transport, acknowledgement policy, and the mirror
//! actor itself live outside this library-only crate.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{CommitSequence, EntryDigest, VersionedCommitLogEntry};

/// One durable outbox row: the queue position of a versioned commit
/// log entry plus the digest a server acknowledgement must echo. The
/// versioned entry itself is the shipped payload; the row only names
/// it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxEntry {
    commit_sequence: CommitSequence,
    entry_digest: EntryDigest,
}

impl OutboxEntry {
    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn entry_digest(&self) -> EntryDigest {
        self.entry_digest
    }
}

impl From<&VersionedCommitLogEntry> for OutboxEntry {
    fn from(entry: &VersionedCommitLogEntry) -> Self {
        Self {
            commit_sequence: entry.commit_sequence(),
            entry_digest: entry.entry_digest(),
        }
    }
}

/// A server-confirmed mirror head: the highest commit sequence the
/// mirror stored durably, named by sequence *and* entry digest.
/// Acknowledging a head at or behind the durable cursor is an
/// idempotent no-op; a head whose digest disagrees with the recorded
/// outbox row is a typed [`crate::Error::MirrorHeadForked`].
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorHead {
    commit_sequence: CommitSequence,
    entry_digest: EntryDigest,
}

impl MirrorHead {
    pub fn new(commit_sequence: CommitSequence, entry_digest: EntryDigest) -> Self {
        Self {
            commit_sequence,
            entry_digest,
        }
    }

    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn entry_digest(&self) -> EntryDigest {
        self.entry_digest
    }
}

impl From<&OutboxEntry> for MirrorHead {
    fn from(entry: &OutboxEntry) -> Self {
        Self {
            commit_sequence: entry.commit_sequence(),
            entry_digest: entry.entry_digest(),
        }
    }
}

/// Typed outcome of [`crate::Engine::acknowledge_mirror`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorAcknowledgement {
    /// The acknowledged head advanced the durable shipped cursor.
    Advanced(MirrorHead),
    /// The acknowledged head was already covered by the durable
    /// cursor; nothing changed. Carries the current cursor.
    Unchanged(MirrorHead),
}

impl MirrorAcknowledgement {
    /// The durable shipped cursor after the acknowledgement.
    pub fn head(&self) -> MirrorHead {
        match self {
            Self::Advanced(head) | Self::Unchanged(head) => *head,
        }
    }
}

/// How durable one committed write — or a store's whole state —
/// currently is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Committed in the local store with no mirror queue position:
    /// the store runs without a [`crate::VersioningPolicy`], so
    /// nothing ever queues for a mirror.
    LocalCommitted,
    /// An outbox row exists; no server acknowledgement covers it yet.
    QueuedForMirror,
    /// Covered by the acknowledged mirror head — the server confirmed
    /// durable storage up to and including this entry.
    ServerCommitted,
}
