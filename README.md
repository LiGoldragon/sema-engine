# sema-engine

`sema-engine` is the full database engine library over `sema` and
`signal-core`. It executes typed database verbs over component-owned
redb files.

It is library-only: no daemon, no actors, no sockets, no NOTA parser,
and no Persona-specific contract dependencies.

Current implemented surface: registered record families, `Assert`,
`Mutate`, `Retract`, `Atomic`, `Match`, `Validate`, typed `ReadPlan`
vocabulary, `SnapshotId`, operation-log replay, `list_tables`, and the first
`Subscribe` primitive with durable registration, initial snapshot, and
post-commit delta delivery.
Executable read plans now cover all rows, exact key, and key range.
Query-algebra operators
`Constrain`, `Project`, `Aggregate`, `Infer`, and `Recurse` live here as typed
plan nodes, not as `signal-core` frame roots; operators that are not executable
yet return a typed `UnsupportedReadPlan` error. Schema and catalog operations
are data under the seven `SignalVerb` roots; there is no separate `Structure`
root. `AtomicBatch` is the first
executable write-bundle plan: it commits one registered table's assert, mutate,
and retract operations under one `SnapshotId`, writes one operation-log entry
for the bundle, and emits per-operation subscription deltas only after the
bundle commit succeeds. Subscription sinks may use detached delivery for
blocking consumers or inline delivery for actor enqueue consumers that need
deterministic post-commit ordering without polling.

Run the test surface:

```sh
nix run .#test
nix run .#test-subscriptions
nix flake check -L
```
