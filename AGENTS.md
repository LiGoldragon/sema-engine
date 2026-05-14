# sema-engine — Agent Instructions

Read `/home/li/primary/AGENTS.md` first.

This repository is the workspace's full database engine library. It is
not a daemon, not an actor runtime, and not a command-line tool. Runtime
components hold an `Engine` inside their own actors when they need
Signal-verb database execution.

## Required Local Reading

1. `ARCHITECTURE.md`
2. `skills.md`
3. `/home/li/primary/skills/rust-discipline.md`
4. `/home/li/primary/skills/rust/storage-and-wire.md`
5. `/home/li/primary/skills/nix-discipline.md`
6. `/home/li/primary/skills/jj.md`

## Local Rules

- Keep this crate library-only. Do not add `src/main.rs`, daemon
  sockets, Kameo actors, tokio runtimes, NOTA parsing, or
  `signal-persona-*` dependencies.
- `Engine` composes `sema::Sema`; it does not replace the storage
  kernel and does not expose raw redb access.
- Signal verbs enter through `signal-core` vocabulary. Component
  contracts own their domain-specific request records and verb mapping.
- Human text projection belongs at component CLI boundaries, not here.
- Tests must include architectural-truth witnesses for dependency and
  binary absence, not only behavior tests.
