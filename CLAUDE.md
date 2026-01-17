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
