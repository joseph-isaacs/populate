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

## Import Style

- Import names directly (not module-qualified) when there are no clashes
- `use` statements should be at module scope, never function scope unless needed to avoid conflicts
