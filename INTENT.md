# INTENT — sema-engine

`sema-engine` is the exclusive database-operation boundary for
state-bearing components. Component daemons do not open redb, define redb
tables, run redb transactions, or maintain their own database commit
ledger. They own domain validation, actors, sockets, authorization, and
schema-root dispatch; durable database work goes through `Engine`.

`sema` is the storage kernel that hides redb behind typed rkyv tables and
schema-version-guarded `.sema` files. `sema-engine` is the reusable
database engine over that kernel: it registers component record families,
executes writes and reads, owns the durable `CommitSequence`, records the
commit log, and supplies replay/subscription surfaces for handover and
observation.

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
storage-kernel transaction types from `sema-engine` instead of depending on
redb directly. The handoff is transitional: it keeps the daemon's dependency
surface on the SEMA boundary while those local tables are lifted into engine
record families.

Component database files use the `.sema` extension. redb remains an
implementation detail of the storage kernel, not the component-facing file
type or daemon API.

The crate remains library-only: no daemon binary, no socket listener, no
actor runtime, no NOTA parser, and no component-specific signal contract
dependencies. Schema and macro work may emit descriptors and dispatch code
that consume `sema-engine`; the engine itself stays reusable and
component-agnostic.
