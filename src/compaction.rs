//! Durable boundary for a multi-table history compaction.
//!
//! The staged-operation slot holds the complete typed retraction plan. This
//! record makes its lifecycle durable: it survives applying that plan until a
//! verified checkpoint and the configured history floor agree with it.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::CommitSequence;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    Planned,
    Applied,
    Checkpointed,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionIntent {
    phase: CompactionPhase,
    final_sequence: Option<CommitSequence>,
}

impl CompactionIntent {
    pub(crate) const fn planned() -> Self {
        Self {
            phase: CompactionPhase::Planned,
            final_sequence: None,
        }
    }

    pub(crate) const fn applied(final_sequence: CommitSequence) -> Self {
        Self {
            phase: CompactionPhase::Applied,
            final_sequence: Some(final_sequence),
        }
    }

    pub(crate) const fn checkpointed(final_sequence: CommitSequence) -> Self {
        Self {
            phase: CompactionPhase::Checkpointed,
            final_sequence: Some(final_sequence),
        }
    }

    pub const fn phase(&self) -> CompactionPhase {
        self.phase
    }

    pub(crate) const fn is_planned(&self) -> bool {
        matches!(self.phase, CompactionPhase::Planned)
    }

    pub const fn final_sequence(&self) -> Option<CommitSequence> {
        self.final_sequence
    }
}

/// Deterministic test interruption points. An injected fault is consumed once,
/// after its named durable phase committed; reopening then exercises recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionFault {
    AfterPlanPersisted,
    AfterRetractionsApplied,
    AfterCheckpointPublished,
    /// The raw history floor and checkpoint artifacts are durable; only
    /// intent cleanup remains. Recovery must be idempotent here.
    AfterHistoryFloorAdvanced,
}
