//! Reusable version-controlled Sema-state library — concept spike.
//!
//! This module is a **designer concept branch**, not production code. It
//! implements the "Option E" candidate from
//! `~/primary/reports/system-designer/92-reusable-versioned-sema-library/`
//! (the synthesis §4): a single reusable generic core, instantiated per
//! component by a **sealed `RecordFamily` trait + a closed component-record
//! enum**, with the schema-hash as the typed decoder selector.
//!
//! It exists to make the report's central claims *runnable* and to test the
//! two coupled axes the psyche asked to compare:
//!
//! - **Genericity without a stringly-typed generic-record store.** The log
//!   entry ([`ReplayEnvelope`]) is a *concrete* type that rests as bytes
//!   (`payload: Vec<u8>` + a [`SchemaHash`] selector) — "erased at rest,
//!   typed in motion." Decoding happens through a **closed match** in the
//!   component's [`ComponentRecord`] impl: no open `HashMap<_, Box<dyn _>>`,
//!   no fallback variant. That closed match is the knife-edge the report
//!   flags (§4/§6); crossing it is what would regress to the forbidden
//!   store.
//! - **Same-file vs separate-file log.** Two [`VersionedLog`] backends —
//!   [`SameFileLog`] (a payload-bearing table inside one `sema` redb file,
//!   one transaction, free atomicity — Option A) and [`FrameFileLog`] (a
//!   byte-shippable append-frame file — Option C). The rebuild-from-log
//!   path and the full-32-byte-blake3 correctness oracle are shared, so the
//!   two arms are compared on identical semantics.
//!
//! What the spike deliberately leaves to production (per the report's
//! "decides nothing" boundary): the kernel inversion itself, the library's
//! home layer, the digest function choice, and — most visibly — the
//! per-component closed enum + sealed-trait impls would be **macro-emitted**
//! in production (`#[doc(hidden)] pub mod sealed` + a derive). Here the
//! demo module hand-writes that generated code, in-crate, under `cfg(test)`.

pub mod envelope;
pub mod error;
pub mod family;
pub mod identity;
pub mod log;
pub mod migration;
pub mod record;
pub mod view;

#[cfg(test)]
mod spike_demo;

pub use envelope::{OperationKind, ReplayEnvelope};
pub use error::VersionedError;
pub use family::RecordFamily;
pub use identity::{CommitSequence, EntryDigest, RecordKey, SchemaHash};
pub use log::{FrameFileLog, LogHead, SameFileLog, VersionedLog};
pub use migration::{Reducer, ReducerSet, SchemaTransition};
pub use record::{ComponentRecord, FoldKey};
pub use view::MaterializedView;
