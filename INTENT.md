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
families use `TableDescriptor` and `EngineRecord::record_key`. Components
whose schema contract needs engine-assigned numeric identity use
`IdentifiedTableDescriptor`; the engine allocates `RecordIdentifier`,
persists the counter, and returns identified receipts and snapshots.

Component database files use the `.sema` extension. redb remains an
implementation detail of the storage kernel, not the component-facing file
type or daemon API.

The crate remains library-only: no daemon binary, no socket listener, no
actor runtime, no NOTA parser, and no component-specific signal contract
dependencies. Schema and macro work may emit descriptors and dispatch code
that consume `sema-engine`; the engine itself stays reusable and
component-agnostic.
