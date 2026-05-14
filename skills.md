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
  contracts compile their domain records into engine plans.
- Add a constraint test when adding an architectural rule.

## Test Commands

```sh
nix run .#test
nix run .#test-dependency-boundary
nix run .#test-engine
nix flake check -L
```
