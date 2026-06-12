# INTENT — sema-engine

`sema-engine` is the exclusive database-operation boundary for
state-bearing components. Component daemons do not open redb, define redb
tables, run redb transactions, or maintain their own database commit
ledger. They own domain validation, actors, sockets, authorization, and
schema-root dispatch; durable database work goes through `Engine`. Per
Spirit fosp (Correction): [Sema-engine is the exclusive interface to the
database. No component daemon may make direct redb calls.]

Per Spirit iir4 (Decision): [The versioned operation log is the
authoritative source of truth for component Sema state, and the redb
store becomes a rebuildable materialized view folded from the log.]
Every durable write goes through the engine's logged choke points; the
storage kernel hands components a read-only `StorageReader` with no
write affordance, so the commit log stays complete by construction.
Versioned log operations carry typed family identity — the family name
(the schema declaration name that survives table renames) plus a
per-family blake3 schema hash — and replay dispatches on that identity;
the table name is only the current storage coordinate. The store-level
schema hash is derived, domain-separated blake3 over the sorted
(family, schema hash) inventory in the engine catalog, never
hand-supplied. Per Spirit x0ja (Constraint): blake3 for all content
addressing. Per Spirit 29pb/j487: native version control is one
reusable library and state loss is unacceptable — components configure
a `VersioningPolicy` once instead of reimplementing component-local
durability journals.

`sema` is the storage kernel that hides redb behind typed rkyv tables and
schema-version-guarded `.sema` files. `sema-engine` is the reusable
database engine over that kernel: it registers component record families,
executes writes and reads, owns the durable `CommitSequence`, records the
commit log, and supplies replay/subscription surfaces for handover and
observation.

When a component opts in with a `VersioningPolicy`, `sema-engine` also
records a payload-bearing, hash-linked versioned commit log inside the
same `.sema` file and the same write transaction as the data write. That
log is the shared substrate for reusable SEMA-state versioning and
backup: components configure the store name once; family identity is
declared per registered family and the store-level schema hash derives
from the registered inventory. Remote transport, acknowledgement policy,
and server storage remain outside this library-only crate.

The engine surface should follow the architecture of its users instead of
forcing components to build compatibility shims. Domain-keyed record
families use `TableDescriptor` and either `EngineRecord::record_key` when
the record type is local, or explicit-key `KeyedAssertion` /
`KeyedMutation` when the component stores imported schema/contract record
types it cannot legally implement external traits for. Components whose
schema contract needs engine-assigned numeric identity use
`IdentifiedTableDescriptor`; the engine allocates `RecordIdentifier`,
persists the counter, and returns identified receipts and snapshots. Identified
families support assert, mutate, retract, and match without forcing components
to simulate mutation through retract-plus-assert or maintain their own
identifier-preserving write shim.

Read and write results expose a `DatabaseMarker`: the durable
`CommitSequence` plus the observed `SnapshotIdentifier`. Read snapshots obtain
that marker from the same closure-scoped storage read transaction as the rows
they return, giving component actors one compact handover/replay boundary
without leaking redb transactions or turning this crate into a runtime.

Consumers that still have component-local tables during migration receive
read-only storage-kernel access (`StorageReader`, `StorageReadTransaction`)
from `sema-engine` instead of depending on redb directly. The handoff is
transitional and read-only: durable writes have no path around the logged
choke points, so lifting local tables into engine record families is the
only way to write them.

Component database files use the `.sema` extension. redb remains an
implementation detail of the storage kernel, not the component-facing file
type or daemon API.

The crate remains library-only: no daemon binary, no socket listener, no
actor runtime, no NOTA parser, and no component-specific signal contract
dependencies. Schema and macro work may emit descriptors and dispatch code
that consume `sema-engine`; the engine itself stays reusable and
component-agnostic.
