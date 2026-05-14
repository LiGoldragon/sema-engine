# sema-engine

`sema-engine` is the full database engine library over `sema` and
`signal-core`. It executes typed database verbs over component-owned
redb files.

It is library-only: no daemon, no actors, no sockets, no NOTA parser,
and no Persona-specific contract dependencies.

Current implemented surface: registered record families, `Assert`,
`Match`, `SnapshotId`, operation-log replay, `list_tables`, and the
first `Subscribe` primitive with durable registration, initial
snapshot, and post-commit delta delivery.

Run the test surface:

```sh
nix run .#test
nix run .#test-subscriptions
nix flake check -L
```
