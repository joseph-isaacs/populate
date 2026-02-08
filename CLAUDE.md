# Claude Code Instructions

## Build & Test Commands

```bash
cargo build
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

## Code Quality

- Run `cargo fmt` before committing
- Run `cargo clippy -- -D warnings` and fix all warnings
- Treat all compiler warnings as errors (`-D warnings`)

## Three-Layer Architecture

The system has three layers. Values and Sinks are user-facing and must be flat, simple tuples. Distribution impls are internal and absorb all complexity.

- **Values** (`type Value`): flat tuples the user iterates over — `(i32, i64, bool)`, never HLists
- **Sinks** (`type Sink` / `SinkOf`): flat tuples of columns the user passes in and gets back — `(Vec<i32>, Vec<i64>)`
- **Distribution impls**: where tree sinks, HAppend, field splitting, SinkFlatten all live — the "complexity budget"

When making design changes: keep values and sinks as simple flat tuples for the user. If complexity must go somewhere, push it into Distribution impls, not into user-facing types.

## Import Style

- Import names directly (not module-qualified) when there are no clashes
- `use` statements should be at module scope, never function scope unless needed to avoid conflicts
