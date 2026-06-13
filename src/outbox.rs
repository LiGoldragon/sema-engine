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
use sema::WriteTransaction;

use crate::{CommitSequence, EntryDigest, Error, Result, VersionedCommitLogEntry};

const OUTBOX: sema::Table<u64, OutboxEntry> = sema::Table::new("__sema_engine_outbox");
const MIRROR_CURSOR: sema::Table<&'static str, MirrorHead> =
    sema::Table::new("__sema_engine_mirror_cursor");
const MIRROR_SHIPPED_KEY: &str = "shipped";

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

/// The mirror-outbox plane: a scoped capability over the engine's
/// storage that owns the durable outbox rows and the shipped-cursor
/// slot, plus the durability classification derived from them. Minted
/// per call over the borrowed storage, like [`crate::StorageReader`].
/// Its `record` choke point writes only the outbox row beside a
/// versioned entry, into a write transaction the engine opened and
/// lends in — the outbox never opens a transaction of its own, so the
/// engine keeps the row atomic with the versioned entry it mirrors.
pub(crate) struct Outbox<'engine> {
    storage: &'engine sema::Sema,
}

impl<'engine> Outbox<'engine> {
    pub(crate) fn new(storage: &'engine sema::Sema) -> Self {
        Self { storage }
    }

    /// The durable shipped cursor: the last server-confirmed mirror
    /// head, if any acknowledgement has landed.
    pub(crate) fn mirror_head(&self) -> Result<Option<MirrorHead>> {
        Ok(self
            .storage
            .read(|transaction| MIRROR_CURSOR.get(transaction, MIRROR_SHIPPED_KEY))?)
    }

    /// The unshipped outbox suffix: every outbox row past the durable
    /// shipped cursor, in commit order.
    pub(crate) fn unshipped(&self) -> Result<Vec<OutboxEntry>> {
        let after = self
            .mirror_head()?
            .map(|head| head.commit_sequence())
            .unwrap_or_else(CommitSequence::genesis);
        Ok(self
            .storage
            .read(|transaction| OUTBOX.range(transaction, after.next().value()..))?
            .into_iter()
            .map(|(_sequence, row)| row)
            .collect())
    }

    /// The outbox row recorded at one commit sequence, if any.
    pub(crate) fn row_at(&self, sequence: CommitSequence) -> Result<Option<OutboxEntry>> {
        Ok(self
            .storage
            .read(|transaction| OUTBOX.get(transaction, sequence.value()))?)
    }

    /// The single durable choke point for an outbox row: the mirror
    /// queue position of a versioned entry, written into the lent write
    /// transaction beside the entry it mirrors.
    pub(crate) fn record(
        transaction: &WriteTransaction,
        entry: &VersionedCommitLogEntry,
    ) -> sema::Result<()> {
        OUTBOX.insert(
            transaction,
            entry.commit_sequence().value(),
            &OutboxEntry::from(entry),
        )
    }

    /// Record a server-confirmed mirror head, advancing the durable
    /// shipped cursor. Idempotent: a head at or behind the cursor is a
    /// typed no-op. A head naming a sequence with no outbox row is
    /// [`Error::MirrorHeadUnknown`]; a head whose recorded outbox row
    /// disagrees with the logged entry digest is
    /// [`Error::OutboxEntryMismatch`]; a head whose digest disagrees
    /// with the recorded outbox row is [`Error::MirrorHeadForked`]. The
    /// logged entry's digest is supplied by the engine, which reads it
    /// through the commit-log plane it owns.
    pub(crate) fn acknowledge(
        &self,
        head: MirrorHead,
        logged_digest: Option<EntryDigest>,
    ) -> Result<MirrorAcknowledgement> {
        let sequence = head.commit_sequence();
        let recorded = self.row_at(sequence)?.ok_or(Error::MirrorHeadUnknown {
            sequence: sequence.value(),
        })?;
        if logged_digest.is_none_or(|digest| digest != recorded.entry_digest()) {
            return Err(Error::OutboxEntryMismatch {
                sequence: sequence.value(),
            });
        }
        if recorded.entry_digest() != head.entry_digest() {
            return Err(Error::MirrorHeadForked {
                sequence: sequence.value(),
                recorded: recorded.entry_digest(),
                acknowledged: head.entry_digest(),
            });
        }
        if let Some(current) = self.mirror_head()? {
            if sequence <= current.commit_sequence() {
                return Ok(MirrorAcknowledgement::Unchanged(current));
            }
        }
        self.storage
            .write(|transaction| MIRROR_CURSOR.insert(transaction, MIRROR_SHIPPED_KEY, &head))?;
        Ok(MirrorAcknowledgement::Advanced(head))
    }

    /// The durability level of one committed write, given that the
    /// commit exists in the log: an entry with no outbox row is local
    /// only; an outbox row covered by the acknowledged head is
    /// server-confirmed; anything else is queued.
    pub(crate) fn durability_of(&self, sequence: CommitSequence) -> Result<Durability> {
        let Some(outbox_row) = self.row_at(sequence)? else {
            return Ok(Durability::LocalCommitted);
        };
        match self.mirror_head()? {
            Some(head) if outbox_row.commit_sequence() <= head.commit_sequence() => {
                Ok(Durability::ServerCommitted)
            }
            _ => Ok(Durability::QueuedForMirror),
        }
    }

    /// The durability level of a versioned store's whole state: an empty
    /// unshipped suffix is fully server-confirmed (an empty log is
    /// trivially mirrored), and anything unshipped is queued.
    pub(crate) fn store_durability(&self) -> Result<Durability> {
        if self.unshipped()?.is_empty() {
            Ok(Durability::ServerCommitted)
        } else {
            Ok(Durability::QueuedForMirror)
        }
    }
}
