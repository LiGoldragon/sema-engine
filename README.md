# sema-engine

`sema-engine` is the full database engine library over `sema` and
`signal-core`. It executes typed database verbs over component-owned
redb files.

It is library-only: no daemon, no actors, no sockets, no NOTA parser,
and no Persona-specific contract dependencies.

Current implemented surface: registered record families and the six
SignalVerb roots `Assert`, `Mutate`, `Retract`, `Match`, `Subscribe`,
`Validate`. Multi-operation atomicity is structural — `Engine::commit`
takes a typed `CommitRequest<RecordValue>` (a non-empty sequence of
`WriteOperation`s for one registered table) and lands it under a single
`SnapshotId` with one `CommitLogEntry`. Typed `ReadPlan` vocabulary,
operation-log replay, `list_tables`, and the first `Subscribe` primitive
(durable registration, initial snapshot, post-commit delta delivery) are
also implemented. Executable read plans cover all rows, exact key, and
key range. Query-algebra operators `Constrain`, `Project`, `Aggregate`,
`Infer`, and `Recurse` live here as typed plan nodes and return a typed
`UnsupportedReadPlan` error until execution semantics land. Subscription
sinks may use detached delivery for blocking consumers or inline delivery
for actor enqueue consumers that need deterministic post-commit ordering
without polling.

`Assert` rejects records whose key already exists (typed
`DuplicateAssertKey`); `Mutate` and `Retract` reject missing records
(typed `RecordNotFound`). `Engine` is a single-owner handle — callers
that need concurrent access must serialise through one owning actor.

Run the test surface:

```sh
nix run .#test
nix run .#test-subscriptions
nix flake check -L
```
