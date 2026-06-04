# sema-engine Skills

This repository follows the workspace skills in `/home/li/primary/skills/`.

## Local Discipline

- Keep `Engine` as a data-bearing library object.
- Do not introduce Kameo here; components put `Engine` inside their
  own actors.
- Do not introduce tokio here; process runtimes live in daemons.
- Do not introduce NOTA here; text projection lives at CLI/debug
  boundaries.
- Do not introduce `signal-persona-*` dependencies here; component
  daemons lower their domain records into engine plans.
- Keep redb behind the `sema` kernel. Engine consumers open `.sema`
  files through `EngineOpen`; they do not import redb.
- Use domain-keyed tables for records with stable semantic keys and
  identified tables for schema contracts that need engine-assigned
  numeric record identifiers.
- Add a constraint test when adding an architectural rule.
- Subscription delivery is post-commit. Tests must prove a failing or
  blocking sink does not roll back or freeze the write path.
- Consumer crates own async routing and durable outbox policy; this
  library only exposes the engine primitive.

## Test Commands

```sh
nix run .#test
nix run .#test-dependency-boundary
nix run .#test-engine
nix run .#test-subscriptions
nix flake check -L
```
