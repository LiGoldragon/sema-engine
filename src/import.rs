//! Engine-owned restore. An [`ImportSession`] is minted only by
//! [`crate::Engine::begin_import`], only for a fresh store, and while
//! it lives it exclusively borrows the engine — ordinary mutation
//! handlers are structurally unable to reach or interleave with the
//! import surface. Committing the session writes the ingested
//! checkpoint and the versioned-log suffix verbatim and materializes
//! the folded view directly: nothing routes through assert/mutate, so
//! no commit sequences are re-minted and no history is double-logged.
//! After import, the store's counters, catalog, logs, and checkpoint
//! rows match the original at the imported range.

use crate::checkpoint::Checkpoint;
use crate::fold::{FamilyDirectory, ViewDigest};
use crate::{
    CommitSequence, CommitSequenceRange, Engine, Error, Result, SnapshotIdentifier,
    VersionedCommitLogEntry,
};

/// One engine-owned restore into a fresh store: exactly one
/// checkpoint, plus the versioned-log suffix continuing past its
/// covered range.
pub struct ImportSession<'engine> {
    engine: &'engine mut Engine,
    checkpoint: Option<Checkpoint>,
    suffix: Vec<VersionedCommitLogEntry>,
}

impl<'engine> ImportSession<'engine> {
    pub(crate) fn new(engine: &'engine mut Engine) -> Self {
        Self {
            engine,
            checkpoint: None,
            suffix: Vec::new(),
        }
    }

    /// Ingest the checkpoint this import restores from, verifying
    /// every content address before accepting it. Exactly one
    /// checkpoint per session.
    pub fn ingest_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<()> {
        if self.checkpoint.is_some() {
            return Err(Error::ImportCheckpointAlreadyIngested);
        }
        checkpoint.verify()?;
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    /// Ingest versioned-log entries continuing past the checkpoint's
    /// covered range, in commit order. The digest chain is verified
    /// against the checkpoint's covered head at commit time.
    pub fn ingest_suffix(&mut self, entries: Vec<VersionedCommitLogEntry>) {
        self.suffix.extend(entries);
    }

    /// Apply the ingested checkpoint and suffix to the fresh store in
    /// one write transaction. The directory materializes each folded
    /// view row into its typed family table.
    pub fn commit(self, directory: &dyn FamilyDirectory) -> Result<ImportReceipt> {
        let Self {
            engine,
            checkpoint,
            suffix,
        } = self;
        let checkpoint = checkpoint.ok_or(Error::ImportMissingCheckpoint)?;
        engine.apply_import(checkpoint, suffix, directory)
    }
}

/// Typed receipt from a committed import: what the checkpoint
/// covered, how much suffix continued it, and where the restored
/// store's durable cursors landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportReceipt {
    covered: CommitSequenceRange,
    suffix_count: usize,
    view_digest: ViewDigest,
    row_count: usize,
    commit_sequence: CommitSequence,
    snapshot: SnapshotIdentifier,
}

impl ImportReceipt {
    pub(crate) fn new(
        covered: CommitSequenceRange,
        suffix_count: usize,
        view_digest: ViewDigest,
        row_count: usize,
        commit_sequence: CommitSequence,
        snapshot: SnapshotIdentifier,
    ) -> Self {
        Self {
            covered,
            suffix_count,
            view_digest,
            row_count,
            commit_sequence,
            snapshot,
        }
    }

    /// The commit sequence range the ingested checkpoint covered.
    pub fn covered(&self) -> CommitSequenceRange {
        self.covered
    }

    pub fn suffix_count(&self) -> usize {
        self.suffix_count
    }

    pub fn view_digest(&self) -> ViewDigest {
        self.view_digest
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// The restored durable commit-sequence high-water mark — the
    /// last suffix entry's sequence, or the checkpoint's covered end
    /// when no suffix was ingested.
    pub fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}
