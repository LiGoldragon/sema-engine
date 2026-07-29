# sema-engine Architecture

`sema-engine` is the workspace's full typed database engine library. It
sits between the `sema` storage kernel and state-bearing component
daemons.

`sema` opens redb files, validates format/schema, and reads/writes typed
rkyv tables behind the component-facing `.sema` file type. `sema-engine`
executes `signal-sema` database operations over registered record
families. Component daemons own actors, sockets, authorization, domain
validation, and their own databases through `Engine`, not through raw
redb calls.

## Direction

The durable direction below is psyche-stated; the constraints and
sections that follow realize it.

- `sema-engine` is the exclusive database-operation boundary for
  state-bearing components: no component daemon makes direct redb calls,
  defines redb tables, runs redb transactions, or maintains its own
  database commit ledger. Per Spirit fosp (Correction): Sema-engine is
  the exclusive interface to the database; no component daemon may make
  direct redb calls.
- The versioned operation log is the authoritative source of truth for
  component Sema state, and the redb store is a rebuildable materialized
  view folded from the log. Per Spirit iir4 (Decision). Durable writes
  go through logged choke points; components receive a read-only
  `StorageReader` with no write affordance, so the log stays complete by
  construction.
- blake3 is the content-addressing primitive throughout: family schema
  hashes, the derived store-level schema hash, segment addresses, and
  the versioned-entry digest chain. Per Spirit x0ja (Constraint).
- Native version control is one reusable library and state loss is
  unacceptable. Components configure a `VersioningPolicy` once rather
  than reimplementing component-local durability journals, and every
  versioned entry lands with a durable mirror outbox row in the same
  write transaction at every choke point. Per Spirit 29pb / j487; per
  Spirit 29pb: atomic server-backed durability, state loss unacceptable.
- The engine surface follows the architecture of its users rather than
  forcing components to build compatibility shims. When a schema-derived
  component exposes a reusable storage need, the answer is to improve
  this shared engine surface, not to port the component onto a
  mismatched API. The crate stays library-only and component-agnostic:
  no daemon binary, socket listener, actor runtime, NOTA parser, or
  component-specific signal contract dependency.
- Durable backing comes first. For the lojix cutover and any new
  consumer, build the durable database backing (live-generation set,
  GC roots, event log persisted with self-resume) before any
  in-memory shortcut, so the on-disk log is authoritative from the
  start rather than retrofitted. Per Spirit fosp.
- The schema component holds the compiled binary runtime schema (O(1)
  lookup, version-diff and namespace tables); NOTA is the source
  authoring format, not the runtime shape. Per Spirit fosp.
- Every path that introduces entries to a versioned branch routes
  through one reusable `IntakePolicy` admission interface, with
  per-component implementations. `IntakePolicy` is not only a rebase
  hook: assert, mutate, retract, replay, import, and any future
  entry-introducing path admit through it unless a later design
  explicitly supersedes this. Per Spirit 2uhh. (`IntakePolicy` is the
  admission half of the versioning library; `VersioningPolicy` names
  the store and schema identity. The admission interface is accepted
  direction; the concrete trait is not yet built.)
- Splitting SEMA out of the daemon into its own process is a
  distant-future consideration that applies only if a component's
  database must become much larger and independently available. It is
  not the current design — the engine is a per-plane library linked
  into each component daemon — and it should not be emphasized in a way
  that suggests an imminent split. Per Spirit en7k.

## Constraints

- `sema-engine` is a Rust library crate.
- `sema-engine` does not ship a daemon binary.
- `sema-engine` does not depend on Kameo.
- `sema-engine` does not depend on tokio.
- `sema-engine` does not depend on NOTA.
- `sema-engine` does not depend on `signal-persona-*` contract crates.
- `Engine` owns a `sema::Sema` handle.
- `Engine` opens storage through `Sema::open_with_schema`.
- `Engine` is a single-owner handle. Snapshot allocation and prevalidation
  read transactions sit outside the write transaction, so concurrent
  callers on the same `Engine` can race the commit log. Component daemons
  must own each `Engine` from one actor and serialise all engine calls
  through that actor.
- Consumers that still have unmigrated component-local tables use
  `Engine::storage_reader()` rather than opening a second `sema::Sema`
  handle to the same `.sema` file. The handoff is read-only:
  `StorageReader` has no write affordance, so no durable write can
  bypass the commit log. Component crates do not depend on `redb`
  directly just to name the read-transaction type
  (`StorageReadTransaction`).
- `Engine` guards an internal storage layout version (currently 7).
  Layout 2 introduced typed family identity; layout 3 added the mirror
  outbox row beside every versioned entry; layout 4 made `RecordKey`
  carry its domain-key vs identifier kind in the archived log/view
  shape; layout 5 persists the versioned chain-head digest in its own
  slot (plus the commit/versioned log counts) so the write path reads
  the predecessor digest in O(1) instead of scanning the whole log.
  Layout 6 adds the durable versioning-policy record, binding store name,
  recovery topology, and finite retention across reopen. Layout 7 adds the
  durable compaction intent; ordinary open refuses a pending intent and a
  supervised recovery-open resolves it before the engine can serve reads or
  writes.
  The layout-4-to-5 bump was additive: it added derived slots without
  touching the data tables or the versioned-log format. So a layout-4
  store that opted into versioning (its versioned log is non-empty)
  upgrades in place at open — the layout-5 derived slots refold from
  the complete versioned log, verified by recomputation (the open-time
  refold reuses `CanonicalView::fold`, recomputing every entry digest
  and verifying the chain link by link from genesis; it never trusts a
  stored head), then the layout re-stamps to 5. This is a raw-log
  refold of the derived slots alone — it runs before any component
  family-to-table registration and materializes no data. A store at an
  older layout with *no* versioned log still hard-fails at open with a
  typed `StorageLayoutMismatch` (its derived state is not log-
  recoverable; the previous-engine migration owns pre-versioning
  stores), as does a forward skew to an unknown-newer layout. A
  rejecting open never writes to the store it rejects — every layout
  plan (stamp a virgin store, refold an older versioned store) is
  applied only after every open-time validation passes.
- `Engine` registers record families before executing database operations.
- Registration declares typed family identity: every `TableDescriptor` /
  `IdentifiedTableDescriptor` carries a `FamilyName` (the schema
  declaration name that survives table renames) and a per-family blake3
  `SchemaHash`. The engine persists the family inventory in its catalog
  and rejects re-registration under a conflicting identity
  (`FamilyIdentityMismatch`) or a second table binding for the same
  family version (`FamilyAlreadyBound`).
- Domain-keyed record families use `TableDescriptor` /
  `TableReference` and either record-provided `RecordKey` values
  (`EngineRecord`) or explicit keys (`KeyedAssertion` /
  `KeyedMutation`) for imported schema/contract record types.
- Engine-identified record families use `IdentifiedTableDescriptor` /
  `IdentifiedTableReference`; `Engine` allocates durable numeric
  `RecordIdentifier` values and persists the next-identifier counter.
- `RecordKey` is a typed sum at the log/view boundary: domain-keyed
  writes carry `RecordKeyKind::Domain`, while identified-family writes
  carry `RecordKeyKind::Identifier`. Their display/storage text may
  match (for example domain key `1` and identifier `1`), but their
  versioned-log digests and canonical view keys remain distinct.
- Engine-identified record families support `Assert`, `Mutate`, `Retract`,
  and `Match` while preserving the engine-assigned `RecordIdentifier`.
- `Assert` writes records through a registered record family.
- `Assert` rejects records whose key already exists in the table with a
  typed `DuplicateAssertKey` error — `Mutate` is the only replacement
  path. The same check runs inside `Engine::commit` for any
  `WriteOperation::Assert` entry; failure rolls back the whole bundle.
- `Mutate` replaces existing records through a registered record family.
- `Retract` removes existing records through a registered record family.
- `Retract` is **destructive at the storage layer**. redb is copy-on-write,
  so the freed pages — the record's bytes — are reclaimed and overwritten by
  later commits, and a retracted record becomes unrecoverable from the file.
  Callers that might need a removed record back must capture it before
  retracting. See `sema` ARCHITECTURE §"Deletion durability".
- `Engine::commit` takes the engine-native `CommitRequest<RecordValue>`
  whose non-empty `Vec<WriteOperation<RecordValue>>` is the atomic unit
  for one registered table. Atomicity is **structural** — the commit's
  operation sequence is the boundary, not a separate operation. Public
  component contracts are different, payload-typed shapes; each consumer
  daemon dispatches its domain-payload request into per-variant engine calls
  (`assert` / `mutate` / `retract` / `commit`).
- `Mutate` and `Retract` reject missing records with typed
  `RecordNotFound` errors.
- A multi-operation commit rejects empty requests (impossible by `NonEmpty`
  type), duplicate write keys within one commit (`DuplicateWriteKey`),
  duplicate Assert keys against table state (`DuplicateAssertKey`), and
  missing mutation or retraction records (`RecordNotFound`) with typed
  errors before writing.
- `Match` reads records through a registered record family.
- `Validate` dry-runs executable read plans through a registered record
  family without mutating storage.
- `ReadPlan` owns query-algebra vocabulary for `Match`, `Subscribe`, and
  `Validate` engine operations.
- `Constrain`, `Project`, `Aggregate`, `Infer`, and `Recurse` are sema-engine
  read-plan operators, not public contract roots.
- The `signal_sema::SemaOperation` set is closed at six operations:
  `Assert`, `Mutate`, `Retract`, `Match`, `Subscribe`, and `Validate`.
  `Atomic` is not an operation; multi-operation atomicity is structural.
- Schema/catalog operations are catalog data under the six operations, not a
  separate `Structure` operation.
- The Sema classification vocabulary stays internal to engine execution
  and observation, off the public contract wire. The six words (`Assert`,
  `Mutate`, `Retract`, `Match`, `Subscribe`, `Validate`) are how the
  engine classifies an operation internally; a component's public
  contract roots are domain verbs, and the daemon derives the Sema class
  from the dispatched request. These words must not appear as
  request-root tags, as an `AuthorizedSignalVerb` enum, or as a
  payloadless `SemaObservation` event in any signal or meta-signal
  contract. Legacy contracts that still carry the classification on the
  wire need a cleanup pass. Per Spirit 7l7l.
- Unsupported read-plan operators return typed `UnsupportedReadPlan` errors
  instead of pretending execution succeeded.
- `Assert`, `Mutate`, and `Retract` write one `CommitLogOperation` entry
  per operation in the same committed write transaction as the domain
  record.
- A multi-operation commit writes one `CommitLogEntry` containing
  `NonEmpty<CommitLogOperation>` in the same committed write transaction
  as the domain records.
- `EngineOpen::with_versioning(VersioningPolicy)` enables the reusable
  versioned-state log for a component database. The policy names only
  the store; schema identity is never hand-supplied. The default
  `EngineOpen` path does not emit payload-bearing version entries.
- `VersioningPolicy` persists one finite retention budget and one recovery topology on first open. The default is a conservative 4,096 raw entries; components choose a typed finite override. A local-checkpoint topology creates no mirror outbox rows, and refuses compaction over unshipped outbox rows only when a durable mirror head was recorded — evidence of a real replay consumer. Outbox rows present with no head ever recorded are vestige of pre-topology engine generations that wrote the outbox unconditionally; local-checkpoint compaction trims them with the covered history rather than blocking on them (v0.11.1). A mirror topology writes every outbox row transactionally and rejects local-checkpoint compaction; it compacts only through a durable server acknowledgement. Later opens reject a policy that differs from the persisted topology or budget.
- Multi-table derived-history retraction uses the engine-owned compaction boundary: it persists the complete typed staged plan before any row changes, atomically applies all planned rows and their version/outbox effects, then survives restart through verified checkpoint publication and configured history-floor advancement. Components register their family directory and resolve a pending intent during open before serving.
- The versioned log is stored in the same `.sema` file as table state.
  A successful write inserts the table mutation, the metadata
  `CommitLogEntry`, and the payload-bearing `VersionedCommitLogEntry` in
  one storage-kernel write transaction.
- `VersionedCommitLogEntry` carries the component store name, the
  derived `StoreSchemaHash` (domain-separated blake3 over the sorted
  (family, schema hash) inventory; table names excluded so a rename
  keeps store identity stable), `CommitSequence`, `SnapshotIdentifier`,
  previous entry digest, entry digest, and
  `NonEmpty<VersionedLogOperation>`.
- `VersionedLogOperation` carries a typed `FamilyIdentity` — family
  name, per-family schema hash, and the table coordinate the operation
  landed in. Replay dispatches on (family, schema hash); the table name
  is only the current coordinate.
- Versioned assert/mutate operations store the rkyv bytes of the typed
  record that landed in the registered table. Versioned retract
  operations store a tombstone with the same table/key identity.
- `versioned_commit_log` and `versioned_replay_from_sequence` expose the
  local log for component backup/mirror code. Suffix reads are storage-key
  range reads over commit sequence, not whole-log materialization followed
  by an in-memory filter; checkpoint, rebuild, and mirror outbox tails use
  the same range-read shape. Network transport, remote acknowledgement
  policy, and server-side retention are not part of `sema-engine`.
- `Engine::replay_versioned(VersionedReplay)` folds versioned log
  entries into a registered family, dispatching each operation on
  family identity. A table renamed between log and replay (same family,
  same schema hash, new table name) replays into the family's current
  table. Application goes through the public write choke points, so a
  rebuilt store logs its own complete history — the log is
  authoritative; the table store is a rebuildable materialized view.
- Every committed write transaction advances a durable `CommitSequence`.
  The sequence is a per-database high-water mark for version handover:
  a next-version daemon can copy state at sequence N, then replay commits
  from N+1 forward.
- Failed commits do not advance `CommitSequence`. The counter is durable
  per database and survives `Engine::close` / `Engine::open`.
- `Engine::current_commit_sequence` returns the current high-water mark
  so a peer reading the handover marker observes the same value the
  next successful commit will exceed.
- `replay_from_sequence` returns commit-log entries by `CommitSequence`.
- `commit_log_range` returns bounded replay entries by `SnapshotIdentifier`.
- `CommitReceipt` carries the committed `CommitSequence`, `SnapshotIdentifier`, and
  operation count. Single-operation and multi-operation commits return the
  same receipt shape.
- `DatabaseMarker` is the compact observed database boundary:
  `CommitSequence` plus `SnapshotIdentifier`.
- Write receipts expose their committed `DatabaseMarker`.
- `QuerySnapshot` carries the `DatabaseMarker` observed from the same
  closure-scoped read transaction as the returned rows.
- `IdentifiedQuerySnapshot` carries the same marker shape for
  engine-identified tables.
- `ValidationReceipt` carries the observed `DatabaseMarker` and record count.
- `Validate` does not write commit-log entries.
- `list_tables` exposes registered table descriptors without exposing
  the mutable catalog.
- `Subscribe` registers durable subscription metadata and returns an
  initial snapshot via the request's `Reply::Accepted` outcome.
- `Subscribe` emits deltas only after the mutation commit succeeds.
- For a multi-operation commit, `Subscribe` emits one per-operation delta after
  the commit succeeds; every delta shares the commit `SnapshotIdentifier`.
- Sink delivery is downstream of the commit and cannot roll back the
  mutation. A durable write reply reports the persistence outcome: once
  the commit succeeds the caller sees a committed write, and any
  post-commit subscription fanout failure is observed separately rather
  than turning a committed write into a rejected one. Per Spirit y3ag.
- Subscription sinks choose detached or inline delivery. Detached is the
  default for blocking sinks; inline exists for actor enqueue sinks that need
  deterministic post-commit ordering without polling.
- Component domain validation happens before calling `Engine`.
- Component actors own ordering, supervision, sockets, and delivery.

## Boundary

```mermaid
flowchart TD
    signal_sema["signal-sema<br/>SemaOperation"]
    signal_frame["signal-frame<br/>NonEmpty utility today"]
    sema["sema<br/>storage kernel"]
    engine["sema-engine<br/>database operation execution"]
    component["component daemon<br/>Kameo actor tree"]
    database["component.sema"]

    engine --> sema
    engine --> signal_sema
    engine --> signal_frame
    component --> engine
    sema --> database
```

`sema-engine` deliberately knows nothing about Persona routing,
terminal delivery, auth sockets, or human text. Those are component
responsibilities.

## Sema short header — symmetric with the wire side

Short headers are universal across both surfaces: the wire (signal)
and the engine (sema). Just as the signal side carries an 8-enum
64-bit short header per message, every sema-engine operation —
executor dispatch, read-plan lowering, command dispatch, and database
reads/writes — carries the same 8-enum 64-bit short-header structure,
with a sema-specific root and sub-enum vocabulary rather than the
signal verbs. The two headers (signal-header and sema-header) use the
same macro machinery but route to different paths. Tap-anywhere
observability therefore extends to sema-side operations identically to
the signal side: a tap can read the header at any engine operation
without decoding the payload. Per Spirit duis. (Accepted direction;
the sema-header type is not yet emitted.)

## Current Surface

A component daemon registers a record family, dispatches its typed
domain requests into per-variant engine calls, and reads through typed
plans:

```rust
let mut engine = Engine::open(EngineOpen::new(database_path, SchemaVersion::new(1)))?;
let family = engine.register_table(TableDescriptor::new(
    TableName::new("thoughts"),
    FamilyName::new("thought"),
    SchemaHash::for_label("thought-v1"),
))?;

// Single-operation writes
engine.assert(Assertion::new(family.clone(), new_thought))?;
engine.mutate(Mutation::new(family.clone(), updated_thought))?;
engine.retract(Retraction::new(family.clone(), retired_key))?;

// Imported contract/schema record types cannot implement this crate's
// EngineRecord trait in the consuming component because of Rust's orphan
// rules. Those consumers supply the key explicitly without wrapping the
// imported record in a local storage duplicate.
engine.assert_keyed(KeyedAssertion::new(family.clone(), RecordKey::new("alpha"), imported))?;
engine.mutate_keyed(KeyedMutation::new(family.clone(), RecordKey::new("alpha"), updated))?;

// Multi-operation commit: atomic by commit structure, not a separate operation.
// Each consumer maps its typed public contract request into per-variant
// engine calls.
engine.commit(
    CommitRequest::new(family.clone())
        .assert(new_thought)
        .mutate(updated_thought)
        .retract(retired_key),
)?;

let snapshot = engine.match_records(QueryPlan::all(family.clone()))?;
let marker = snapshot.database_marker();
let validation = engine.validate(QueryPlan::all(family.clone()))?;
let _tables = engine.list_tables();
let _log = engine.commit_log_range(SequenceRange::from(snapshot.snapshot()))?;
let _subscription = engine.subscribe(QueryPlan::all(family.clone()), sink)?;
engine.storage_reader().read(|transaction| {
    // read-only access to temporary component-local tables that have
    // not moved to engine record families yet; there is no write
    // counterpart
    Ok(())
})?;
```

Components that opt into reusable state versioning configure it at open:

```rust
let open = EngineOpen::new(database_path, SchemaVersion::new(1))
    .with_versioning(VersioningPolicy::new(VersionedStoreName::new("mind")));
let engine = Engine::open(open)?;
let _version_tail = engine.versioned_replay_from_sequence(CommitSequence::new(1))?;
```

Versioned stores checkpoint, restore, rebuild, and observe mirror
durability through the engine-owned fold surface (`directory` is the
component's `FamilyDirectory` impl):

```rust
let receipt = engine.checkpoint()?;
let checkpoint = engine.latest_checkpoint()?.expect("just written");
let suffix = engine
    .versioned_replay_from_sequence(checkpoint.metadata().covered().last().next())?;

// Restore into a fresh store opened under the same VersioningPolicy.
let mut session = fresh_engine.begin_import()?;
session.ingest_checkpoint(checkpoint)?;
session.ingest_suffix(suffix);
let _imported = session.commit(&directory)?;

// Re-derive the materialized tables from the authoritative log.
let _rebuilt = engine.rebuild_from_log(&directory)?;

// Mirror outbox: the unshipped suffix and the durable shipped cursor.
let unshipped = engine.unshipped_outbox()?;
let _outcome = engine.acknowledge_mirror(server_confirmed_head)?;
let _level = engine.store_durability()?;
```

This proves the layering: registered record family, single-operation `Assert` /
`Mutate` / `Retract`, structural multi-operation `commit`, `Match`, `Validate`,
executable `ReadPlan` nodes for all/key/range reads, typed query-algebra
nodes for future constrain/project/aggregate/infer/recurse execution,
typed rkyv values, commit-log cursor, bounded log replay, optional
payload-bearing version replay, table introspection, best-effort
post-commit subscription delivery for write operations, and durable
storage through `sema`.

For schema contracts that need engine-assigned numeric record identity, a
component registers an identified family instead of deriving identity from
the record payload:

```rust
let mut engine = Engine::open(EngineOpen::new(database_path, SchemaVersion::new(1)))?;
let entries = engine.register_identified_table(IdentifiedTableDescriptor::new(
    TableName::new("entries"),
    FamilyName::new("entry"),
    SchemaHash::for_label("entry-v1"),
))?;

let receipt = engine.assert_identified(IdentifiedAssertion::new(entries, entry))?;
let identifier = receipt.identifier();
engine.mutate_identified(IdentifiedMutation::new(entries, identifier, updated_entry))?;
let found = engine.match_identified(IdentifiedQueryPlan::identifier(entries, identifier))?;
engine.retract_identified(IdentifiedRetraction::new(entries, identifier))?;
```

The numeric identifier and the `CommitSequence` are both engine state.
Component daemons do not keep a parallel ledger.

## Family evolution — read-older-shapes chain at registration

A table descriptor may declare its family's evolution: the prior
stored generations the engine may find in an existing store's catalog,
each as a per-family schema hash plus a typed carry that reads rows in
that generation's own decoded shape and converts them forward to the
current record type (`TableDescriptor::with_prior`). Registration
against a store whose catalog names a declared prior migrates the
family in place inside the engine; consumers no longer reach into
`__sema_engine_catalog` or hand-write migration transactions.

- **Atomic**: the rewritten rows, the evolved catalog registration,
  and the log entries land in one write transaction — the catalog can
  never name a shape the rows do not have. A failed carry (including a
  validated decode refusal of mismatched bytes) leaves the store
  untouched.
- **Fail-closed**: a stored identity with no declared step — or from a
  different family reusing the table coordinate — keeps surfacing the
  original typed `FamilyIdentityMismatch`. Nothing is guessed.
- **Logged as row history**: on a versioned store, the evolution entry
  retracts each row under the prior family identity and asserts its
  converted successor under the evolved identity, so canonical-view
  folds and rebuilds materialize only current-shape rows and never
  need the retired shape. The entry's commit sequence is the durable
  age evidence of the migration. An empty family evolves as a
  catalog-only rewrite and logs nothing, matching plain registration.
- **Direct steps, not chained hops**: each declared generation carries
  its own conversion to the current shape. A store parked two
  generations back converts in one step through its own declaration.
- **Domain-keyed families only, today**: `IdentifiedTableDescriptor`
  has no evolution surface yet; identified families keep their
  existing restore paths (checkpoint import, rebuild-from-log). The
  storage-kernel schema-version stamp (`__sema_meta`) is a separate,
  kernel-owned concern: additive version stamping still lives with
  consumers and is not yet atomic with family registration — the
  recorded debt stands.

## Checkpoint — payload-bearing derived artifact

A checkpoint *digest* verifies a state; a checkpoint *segment*
restores one. `Engine::checkpoint()` folds the versioned log (on top
of the latest checkpoint, when one exists) into the canonical view —
the per-key last-write state in sorted (family, schema hash, key)
order — and persists two shapes durably in one write transaction:

- `CheckpointMetadata`: checkpoint sequence, store name, derived
  `StoreSchemaHash`, the `FamilyInventory` (every registered
  `FamilyIdentity` plus the identified-counter rows), the covered
  `CommitSequenceRange`, the covered snapshot and covered entry digest
  (the chain head a continuing suffix must name), the 32-byte blake3
  `ViewDigest` over the canonical sorted view, the optional previous
  checkpoint digest, the ordered segment references, and the
  checkpoint's own blake3 digest over all of it.
- `CheckpointSegment` rows: consecutive chunks of the sorted view
  rows, content-addressed by domain-separated blake3. Chunking is
  deterministic at a fixed byte budget (1 MiB soft), bounding every
  segment read and write.

**Checkpointing logs no versioned entry.** A checkpoint is a derived
artifact of the log: the log already contains everything the
checkpoint folds, so logging the fold would make history describe
itself. Checkpoint creation advances no `CommitSequence` and no
snapshot. A checkpoint initially bounds how much a restore or rebuild must
refold. An explicit typed history-retention request can then compact the
checkpoint-covered prefix only after its configured recovery acknowledgement:
the verified local checkpoint when no external consumer exists, or a durable
mirror acknowledgement when one does. The remaining suffix continues from the
checkpoint's covered digest.

The view digest and the store schema hash deliberately exclude table
coordinates: a table rename keeps both stable. The view digest includes
the `RecordKeyKind` tag, so domain keys and engine identifiers do not
collapse just because their text is the same. The fold verifies the
entry digest chain link by link *and* recomputes each entry digest
from the entry's own fields, so a tampered entry cannot ride a stored
digest through the chain.

`Engine::latest_checkpoint()` loads metadata plus segments and
verifies every content address before returning the portable
`Checkpoint` artifact.

## Import — engine-owned restore

`Engine::begin_import()` mints an `ImportSession` — the only path to
the restore surface. It exists only for a fresh store (typed
`ImportStoreNotFresh` otherwise) and exclusively borrows the engine
while it lives, so ordinary mutation handlers are structurally unable
to reach or interleave with it. The session ingests exactly one
verified `Checkpoint` plus a versioned-log suffix, and `commit`
applies everything in one write transaction:

- catalog registrations and identified counters restore verbatim from
  the checkpoint's family inventory;
- the checkpoint metadata and segments land in the engine's
  checkpoint tables, so later checkpoints chain from it;
- each suffix entry inserts verbatim into the versioned log — original
  sequences, digests, predecessor chain, and tombstones preserved —
  through the same choke point as live writes, so each gets its
  mirror outbox row; the metadata commit log entry is derived by
  projection (`CommitLogEntry::from(&VersionedCommitLogEntry)`);
- the folded view (checkpoint rows + suffix) materializes directly
  into the typed family tables through `RowMaterializer`, never
  through assert/mutate — no double-logging, no re-minted sequences;
- `CommitSequence` and snapshot cursors land at the last suffix
  entry's values (or the checkpoint's covered end).

After import, the store's counters, catalog, logs, tombstones, and
checkpoint rows are indistinguishable from the original at the
imported range; new writes continue the imported digest chain.
History covered by the checkpoint is compacted: per-entry rows for
the covered range exist only on the source, so `durability_of` on a
covered sequence reports `UnknownCommitSequence` in the restored
store. The mirror shipped-cursor is deliberately not restored —
acknowledgement is a transport fact the restore cannot fabricate; the
mirror re-acknowledges idempotently.

The component supplies a `FamilyDirectory`: its typed knowledge of
which Rust record type materializes each family. The engine drives
the fold and owns the transaction; the directory only picks the type
and calls `RowMaterializer::apply` / `apply_identified`, each of
which can write only its own row into its own family table.

## Rebuild-from-log — the fold defines the view

`Engine::rebuild_from_log(directory)` re-derives the materialized
family tables from the authoritative log: fold the latest
checkpoint's rows (when one exists) plus the log suffix, then
re-materialize in one write transaction — tombstone rows first for
every key the fold touched but did not keep, then the final rows.
Because every durable write goes through the logged choke points, the
touched-key set covers every key a materialized table can legally
hold, so touched-key clearing is a full clear by construction. (A row
smuggled in behind the engine's back is cleared only if the log ever
touched its key; rows at never-logged keys cannot exist through any
engine path.) The rebuild writes tables directly inside the engine's
own transaction and logs nothing — the log remains the single
history. `replay_versioned` remains the per-family replay surface.

What remains unsupported: `replay_versioned` routes through the
domain-keyed public choke points, so engine-identified families
cannot replay through it (replaying an identified assert would
re-mint identifiers). Identified families restore through checkpoint
import and rebuild-from-log, which preserve identifiers and counters.
Checkpoint, import, and rebuild require a complete versioned history: either
the retained versioned log, or one verified checkpoint plus its retained suffix.
A store that wrote history before enabling versioning gets a typed
`VersionedLogIncomplete` error.

The open-path derived-slot refold (layout-4-to-5 upgrade, above) is a
narrower, distinct surface from `rebuild_from_log`: it reuses the same
`CanonicalView::fold` to verify the whole chain by recomputation, but
it writes only the derived slots (`CHAIN_HEAD` and the log counts) and
materializes no data tables, so it needs no `FamilyDirectory` and runs
before any family-to-table registration. The data tables are already
correct in a layout-4 store (the bump was additive); only the missing
derived slots are rebuilt. `rebuild_from_log` is the full materialized-
view rebuild and still requires a directory and a registered inventory.

## Mirror outbox and durability levels

Every versioned commit log entry lands with a durable `OutboxEntry`
row — commit sequence plus entry digest — in the same write
transaction, at every write choke point (single assert/mutate/retract,
identified variants, multi-operation commit, and imported suffix
entries). The unshipped suffix is therefore complete by construction;
this is what forced storage layout 3. Layout 4 follows from the key
kind split in the versioned log and checkpoint view.

The outbox is the local half of the mirror protocol — the durable
queue a mirror daemon would drain — not the mirror itself. Mirror is
unshipped: in the psyche's words, "mirroring is a thing which we
haven't shipped yet. It's not a place, it's just the other daemon
running on another host." A store must never assume an outbox consumer
exists. Mirror activation — a durable mirror head recorded through
`acknowledge_mirror` — is a deliberate act, never a default; a store
that never recorded one carries no live outbox obligation, and its
outbox rows are vestige, inert under local-checkpoint compaction.

The typed API for the future mirror actor, library-only — no
transport, no actor, no network:

- `unshipped_outbox()` — the outbox rows past the durable shipped
  cursor; the mirror loads the matching versioned entries through
  `versioned_replay_from_sequence` and ships those.
- `acknowledge_mirror(MirrorHead)` — records a server-confirmed head
  (sequence + digest), advancing the durable shipped cursor.
  Idempotent: a head at or behind the cursor returns
  `MirrorAcknowledgement::Unchanged`; a head whose digest disagrees
  with the recorded outbox row is a typed `MirrorHeadForked`; a head
  with no outbox row is `MirrorHeadUnknown`.
- `durability_of(CommitSequence)` / `store_durability()` — the typed
  `Durability` level: `LocalCommitted` (no mirror queue position;
  stores without versioning never queue), `QueuedForMirror` (outbox
  row exists, unacknowledged), `ServerCommitted` (covered by the
  acknowledged head). Server-committed waiting belongs at the
  component request layer, after the local transaction closes.

## CommitSequence — durable high-water mark for handover

Every successful write transaction advances a per-database
`CommitSequence`. The counter is the workspace's mechanism for
zero-downtime component version handover: when a next-version daemon
starts beside a current-version daemon and needs to copy the current
database, it asks the current daemon for the high-water mark N, copies
state at N, then replays commits from N+1 forward. `MutationReceipt`,
`CommitReceipt`, and `CommitLogEntry` all carry `commit_sequence` so
peers can observe the same value the next successful commit will exceed.

`Engine::current_commit_sequence()` returns the high-water mark for use
in handover markers. `Engine::replay_from_sequence(start)` returns
commit-log entries by `CommitSequence` so a peer can drain deltas from
a known point. Failed commits do not advance the counter, so a crash
between transactions resumes cleanly at the previous high-water mark.

The sequence integrates with snapshots: `SnapshotIdentifier` still drives
subscription replay through the existing snapshot cursor;
`CommitSequence` is the boundary for cross-daemon handover. Both are
durable, monotonic, and per-database.

## Handover raw-payload storage discipline

`signal-version-handover`'s `Mirror` operation carries an unspecified
raw payload (raw bytes plus a `RecordKind` discriminant). When a
component daemon receives a mirrored write during the handover window
those raw bytes do **not** enter the sema-engine-managed typed tables
directly. The receiver persists them in a **separate raw-payload
container** outside the typed database — the engine's typed tables
only ever accept records produced by `version-projection`'s
reverse-projection step.

This keeps the typed database invariant intact: every record in
sema-engine's tables has gone through a typed shape known to the
receiver's signal-X library. Un-incorporated handover bytes live
beside the typed database, never inside it. Non-representable
payloads become typed `Divergence` operations on the handover wire,
again never silently landing as raw rows.

The container itself is the receiver daemon's responsibility — this
crate owns neither the container's on-disk shape nor its lifetime.
The discipline is the load-bearing rule: typed tables stay clean.
The typed-enum alternative for the `Mirror` payload is deferred per
`signal-version-handover/ARCHITECTURE.md` (Possible features), so the
raw-container discipline is the durable answer for the first
production handover.

## Non-Goals

- No schema-less storage open.
- No raw byte slot store. (Handover raw payloads live in a separate
  container outside this engine's typed tables — see the section
  above.)
- No raw redb access from component daemons.
- No second redb handle for a component database already opened by
  `Engine`.
- No actors in this crate.
- No text parser in this crate.
- No daemon process in this crate.

## Macro-pattern integration

**Status:** integrated into the brilliant macro library pattern per `reports/designer/326-v13-spirit-complete-schema-vision.md §3` (schemas as macro-pattern instance).

**Role:** this crate is the typed database engine. It owns the `Engine` handle, the typed-table machinery, the `CommitSequence` high-water-mark contract, and the redb storage adapter. Per-component daemons consume this crate by declaring their storage types in a dedicated per-component schema document — the `sema.schema` document kind (existing usage: `orchestrate/schema/sema.schema`, `spirit/schema/sema.schema`, each generated into `src/schema/sema.rs`) — from which the macro emits the redb table descriptors that bind those storage types into `Engine`.

**Integration target:** typed database engine; the macro emits sema-engine
table descriptors from the storage-type declarations in a component's
`sema.schema` document. Each declaration names one stored record type together
with the parts this engine consumes from it — its record type, its key or
identity style, and its indices and projections. The macro lowers these into
`TableDescriptor` or `IdentifiedTableDescriptor` constants the component daemon
registers with its `Engine` at startup. Every descriptor is generated from a
declaration: no daemon hand-constructs a descriptor.

The `sema.schema` document kind is the settled declaration surface; the
schema-language architecture records it as a distinct document kind whose file
kind fixes the storage-declaration root type (see that repository's "Schema
document kinds"). It supersedes two earlier framings of the same vision: the
schema-language storage block direction (per `/326-v13`), and before it the
retired `Family.(…)` namespace-head construct, which was the narrow,
mis-shaped first implementation. The exact entry shape — how an index or
projection declaration reads as surface syntax — is not yet designed; it is
reserved for a psyche design session. This crate therefore names the fields it
consumes from a declaration without depending on their surface form.

**Stored-record identity basis:** the family-closure hash that the
generated-descriptor chain used as its stored-record identity basis has been
retired from schema-language along with the families construct. The successor
identity basis for stored records is not yet chosen; it is pending the same
`sema.schema` document-kind design session. Schema-language's per-declaration
nominal-identifier and core-hash machinery is the available foundation and the
candidate basis, but the binding — how a stored record's identity derives from
it — is undesigned here and reserved for that session. That is the target; as
current state the engine still keys stored-record identity on the per-family
`SchemaHash` it persists in its catalog.

This is the target, not the current state. Component daemons still carry
hand-written descriptor paths — for example spirit's guardian journal and
production migration hand-construct their `TableDefinition` declarations. The
macro-pattern upgrade — named `schema-engine` here before that repo was
renamed `ethos-engine` on 2026-07-27 (S1R entry 7) — replaces those
hand-written declarations with macro-emitted descriptors derived from the
component's `sema.schema` document, and the surviving hand-written paths are
drift to be eliminated as each component converts.

**References:**
- `reports/designer/326-v13-spirit-complete-schema-vision.md` — schema language + macro pattern
- `reports/designer/324-migration-mvp-spirit-handover-re-specification.md` — migration MVP
- `reports/operator/174-schema-import-header-design-critique-2026-05-24.md` — lowering + AssembledSchema form
