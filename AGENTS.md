# sema-engine — Agent Instructions

This repository is the workspace's full database engine library. It is
not a daemon, not an actor runtime, and not a command-line tool. Runtime
components hold an `Engine` inside their own actors when they need
Sema database-operation execution.

## Required Local Reading

1. `ARCHITECTURE.md`
2. `skills.md`

## Local Rules

- Keep this crate library-only. Do not add `src/main.rs`, daemon
  sockets, Kameo actors, tokio runtimes, NOTA parsing, or
  `signal-persona-*` dependencies.
- `Engine` composes `sema::Sema`; it does not replace the storage
  kernel and does not expose raw redb access.
- Component daemons consume this crate instead of opening redb directly.
  redb belongs behind `sema`; component-facing database files use
  `.sema`.
- Sema operations enter through `signal-sema` vocabulary. Component
  contracts own their domain-specific public operations and the daemon
  lowers them into engine calls.
- Prefer improving `sema-engine` when schema-derived components expose a
  reusable storage need. Do not force every component to hand-roll the
  same compatibility shim around a mismatched current API.
- Human text projection belongs at component CLI boundaries, not here.
- Tests must include architectural-truth witnesses for dependency and
  binary absence, not only behavior tests.
