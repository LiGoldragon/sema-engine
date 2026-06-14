//! The hash-chained versioned commit log and its metadata projection.
//!
//! `CommitLog` owns one coherent plane of engine state: the two
//! durable log tables (the metadata [`CommitLogEntry`] log and the
//! authoritative [`VersionedCommitLogEntry`] log), the persisted
//! chain-head digest the write path mints each new entry's predecessor
//! from, and the two row-count counters that let the engine compare the
//! two logs' lengths in O(1). It is the single append choke point: a
//! versioned entry only ever lands here, advancing the chain head and
//! the versioned count in the same write transaction as the row.
//!
//! `CommitLog` is a scoped capability minted over the engine's
//! storage, exactly as [`crate::StorageReader`] and
//! [`crate::RowMaterializer`] are. Its read methods open read
//! transactions on the borrowed storage; its append methods write only
//! their own log rows into a write transaction the engine opened and
//! lends in, so atomicity stays the engine's to coordinate — the log
//! never opens a transaction of its own.

use sema::WriteTransaction;

use crate::log::CommitLogEntry;
use crate::{CommitSequence, EntryDigest, Result, SequenceRange, VersionedCommitLogEntry};

const COMMIT_LOG: sema::Table<u64, CommitLogEntry> = sema::Table::new("__sema_engine_commit_log");
const VERSIONED_COMMIT_LOG: sema::Table<u64, VersionedCommitLogEntry> =
    sema::Table::new("__sema_engine_versioned_commit_log");
/// The persisted versioned chain-head digest: the entry digest of the
/// most recent versioned commit log entry, updated inside the same
/// write transaction as that entry. Reading it is O(1), replacing a
/// full `VERSIONED_COMMIT_LOG` scan on every write. This is an
/// optimization for minting the next entry's predecessor digest only;
/// integrity is never read from here — [`crate::fold::CanonicalView::fold`]
/// recomputes every entry digest and verifies the chain link by link.
const CHAIN_HEAD: sema::Table<&'static str, EntryDigest> =
    sema::Table::new("__sema_engine_chain_head");
const LATEST_ENTRY_DIGEST_KEY: &str = "latest_entry_digest";
const COUNTERS: sema::Table<&'static str, u64> = sema::Table::new("__sema_engine_counters");
/// Row counts for the commit log and the versioned commit log,
/// maintained in `COUNTERS` at the same write transaction as each row
/// insert. They let `LogCounts`-based comparisons run O(1) instead of
/// materializing and rkyv-decoding both whole logs.
const COMMIT_LOG_COUNT_KEY: &str = "commit_log_count";
const VERSIONED_LOG_COUNT_KEY: &str = "versioned_log_count";

/// The hash-chained versioned log plane: a scoped capability over the
/// engine's storage that owns the commit-log tables, the cached chain
/// head, and the row-count counters, plus the single append choke
/// point. Minted per call over the borrowed storage, like
/// [`crate::StorageReader`].
pub(crate) struct CommitLog<'engine> {
    storage: &'engine sema::Sema,
}

impl<'engine> CommitLog<'engine> {
    pub(crate) fn new(storage: &'engine sema::Sema) -> Self {
        Self { storage }
    }

    /// Every metadata commit log entry, in commit order.
    pub(crate) fn entries(&self) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    /// Metadata commit log entries from `start` onward, in commit order.
    pub(crate) fn entries_from(&self, start: CommitSequence) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| COMMIT_LOG.range(transaction, start.value()..))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    /// Metadata commit log entries whose snapshot lies in `range`.
    pub(crate) fn entries_in(&self, range: SequenceRange) -> Result<Vec<CommitLogEntry>> {
        Ok(self
            .entries()?
            .into_iter()
            .filter(|entry| range.contains(entry.snapshot()))
            .collect())
    }

    /// Every versioned commit log entry, in commit order.
    pub(crate) fn versioned_entries(&self) -> Result<Vec<VersionedCommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    /// Versioned commit log entries from `start` onward, in commit
    /// order.
    pub(crate) fn versioned_entries_from(
        &self,
        start: CommitSequence,
    ) -> Result<Vec<VersionedCommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.range(transaction, start.value()..))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect())
    }

    /// The versioned entry recorded at one commit sequence, if any.
    pub(crate) fn versioned_entry_at(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<VersionedCommitLogEntry>> {
        Ok(self
            .storage
            .read(|transaction| VERSIONED_COMMIT_LOG.get(transaction, sequence.value()))?)
    }

    /// Whether a metadata commit log row exists at one commit sequence —
    /// the test for whether a sequence names a real committed write.
    pub(crate) fn entry_present(&self, sequence: CommitSequence) -> Result<bool> {
        Ok(self
            .storage
            .read(|transaction| Ok(COMMIT_LOG.get(transaction, sequence.value())?.is_some()))?)
    }

    /// The predecessor digest a freshly-minted versioned entry names —
    /// the digest of the current chain head. Read O(1) from the
    /// persisted [`CHAIN_HEAD`] slot, which `append_versioned` advances
    /// inside the same transaction as each versioned entry. This is an
    /// optimization only: integrity never derives from this slot;
    /// [`crate::fold::CanonicalView::fold`] recomputes every entry
    /// digest and verifies the chain link by link.
    pub(crate) fn chain_head(&self) -> Result<Option<EntryDigest>> {
        Ok(self
            .storage
            .read(|transaction| CHAIN_HEAD.get(transaction, LATEST_ENTRY_DIGEST_KEY))?)
    }

    /// The persisted row counts for both logs, read O(1) from
    /// `COUNTERS`. A mutator reads these under the engine's held write
    /// lock alongside the next commit sequence, then appends the
    /// incremented counts inside the same transaction as the rows —
    /// keeping the counts exact without ever materializing either log.
    pub(crate) fn counts(&self) -> Result<LogCounts> {
        Ok(self.storage.read(|transaction| {
            Ok(LogCounts::new(
                COUNTERS
                    .get(transaction, COMMIT_LOG_COUNT_KEY)?
                    .unwrap_or(0),
                COUNTERS
                    .get(transaction, VERSIONED_LOG_COUNT_KEY)?
                    .unwrap_or(0),
            ))
        })?)
    }

    /// The single durable choke point for a metadata commit log row:
    /// insert the entry and advance the persisted commit-log count in
    /// the lent write transaction, so the count never drifts from the
    /// rows.
    pub(crate) fn append_commit(
        transaction: &WriteTransaction,
        entry: &CommitLogEntry,
        commit_count: u64,
    ) -> sema::Result<()> {
        COMMIT_LOG.insert(transaction, entry.commit_sequence().value(), entry)?;
        COUNTERS.insert(transaction, COMMIT_LOG_COUNT_KEY, &commit_count)
    }

    /// The single durable choke point for versioned history: the
    /// versioned commit log entry lands, the persisted chain-head digest
    /// advances, and the versioned-log count advances, all in the lent
    /// write transaction. Advancing the head slot and the count here
    /// keeps both correct by construction: neither can disagree with the
    /// last versioned entry the same transaction wrote. The mirror
    /// outbox row beside this entry is the engine's to record through
    /// [`crate::outbox::Outbox`] in the same transaction.
    pub(crate) fn append_versioned(
        transaction: &WriteTransaction,
        entry: &VersionedCommitLogEntry,
        versioned_count: u64,
    ) -> sema::Result<()> {
        VERSIONED_COMMIT_LOG.insert(transaction, entry.commit_sequence().value(), entry)?;
        CHAIN_HEAD.insert(transaction, LATEST_ENTRY_DIGEST_KEY, &entry.entry_digest())?;
        COUNTERS.insert(transaction, VERSIONED_LOG_COUNT_KEY, &versioned_count)
    }

    /// Refold the layout-introduced derived slots (the persisted
    /// chain-head digest and the two log-count counters) from the
    /// store's complete versioned log, then re-stamp the supplied layout
    /// counter. Verification is by recomputation: the fold recomputes
    /// every entry digest from the entry's own fields and verifies the
    /// chain link by link from genesis (`chain_head = None`), so the
    /// rebuilt head can never trust a stored digest. The verified head
    /// is the last entry's recomputed digest, identical to what a fresh
    /// full fold of the same log produces. The data tables are already
    /// correct (slice A's layout bump was additive), so this touches
    /// only the derived slots and needs no family directory — the head
    /// and the counts derive from the raw log alone, before any
    /// component family-to-table registration.
    pub(crate) fn rebuild_derived_slots(
        storage: &sema::Sema,
        layout_key: &'static str,
        layout: u64,
    ) -> Result<()> {
        let entries: Vec<VersionedCommitLogEntry> = storage
            .read(|transaction| VERSIONED_COMMIT_LOG.iter(transaction))?
            .into_iter()
            .map(|(_sequence, entry)| entry)
            .collect();
        // Recompute and verify the whole chain from genesis. A tampered
        // or forked log surfaces as a typed VersionedChainBroken here,
        // aborting the open before any derived slot is written.
        crate::fold::CanonicalView::fold(&[], &entries, None)?;
        let head = entries.last().map(VersionedCommitLogEntry::entry_digest);
        let versioned_count = entries.len() as u64;
        let commit_count = storage
            .read(|transaction| COMMIT_LOG.iter(transaction))?
            .len() as u64;
        Ok(storage.write(|transaction| {
            match &head {
                Some(digest) => {
                    CHAIN_HEAD.insert(transaction, LATEST_ENTRY_DIGEST_KEY, digest)?;
                }
                None => {
                    CHAIN_HEAD.remove(transaction, LATEST_ENTRY_DIGEST_KEY)?;
                }
            }
            COUNTERS.insert(transaction, COMMIT_LOG_COUNT_KEY, &commit_count)?;
            COUNTERS.insert(transaction, VERSIONED_LOG_COUNT_KEY, &versioned_count)?;
            COUNTERS.insert(transaction, layout_key, &layout)
        })?)
    }

    /// Whether the versioned log holds any entry — the open-time test
    /// for whether an older-layout store opted into versioning and so
    /// can refold its derived slots from the log alone.
    pub(crate) fn versioned_log_present(storage: &sema::Sema) -> Result<bool> {
        Ok(storage.read(|transaction| Ok(!VERSIONED_COMMIT_LOG.iter(transaction)?.is_empty()))?)
    }

    /// Whether both logs are empty — part of the fresh-store test an
    /// import session demands before restoring into a store.
    pub(crate) fn logs_empty(&self) -> Result<bool> {
        Ok(self.storage.read(|transaction| {
            Ok(COMMIT_LOG.iter(transaction)?.is_empty()
                && VERSIONED_COMMIT_LOG.iter(transaction)?.is_empty())
        })?)
    }
}

/// The persisted lengths of the commit log and the versioned commit
/// log. Read O(1) and advanced inside each write transaction so the
/// engine can compare them without materializing either log.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LogCounts {
    commits: u64,
    versioned: u64,
}

impl LogCounts {
    fn new(commits: u64, versioned: u64) -> Self {
        Self { commits, versioned }
    }

    pub(crate) fn commits(self) -> u64 {
        self.commits
    }

    pub(crate) fn versioned(self) -> u64 {
        self.versioned
    }

    /// The commit-log count after one more commit entry lands.
    pub(crate) fn next_commit(self) -> u64 {
        self.commits + 1
    }

    /// The versioned-log count after one more versioned entry lands.
    pub(crate) fn next_versioned(self) -> u64 {
        self.versioned + 1
    }
}
