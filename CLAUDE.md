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

## Docs and comments

This repository is public. Write for a reader who has only the code, not
the history of how it was built.

- **Describe what is, not how it got here.** No "before this PR", "now
  faster than main", "after review", "PR #N", "used to", "since X landed"
  in READMEs, doc comments or code comments. That history belongs in the
  commit message and PR description.
- **No benchmark write-ups or result files in the tree.** Before/after
  tables, run logs, `*.csv`/`*.json` results and graphs (`*.png`, `*.svg`)
  go in the PR description, not the repository. A crate README may keep one
  short, current performance section with the command that reproduces it
  and the machine it ran on.
- **No planning docs.** Surveys, TODO lists, design proposals and
  "future work" notes go in GitHub issues or the PR. Design docs in the tree
  (`populate/DESIGN.md`) describe the code as it is; update them in the
  same change that alters the design, and delete what no longer applies.
- **Don't commit prompts, scratch notes or tool output** (pasted chat
  messages, `.cargo-ok`, `mutants.out`, local logs).
- **One home per fact.** The root README gives the overview and links to
  each crate's README; crate READMEs hold flags, verification and
  performance. Don't repeat the same section in both.
- **Comments say why, not what.** Skip comments that restate the code.
  Keep the ones that explain a non-obvious choice, an invariant, or a
  reference tool's quirk being reproduced (with where it comes from).
- **Keep licence files.** Vendored code (`populate-dsb/dsbgen`,
  `populate-dsb/dsbgen-arrow`) keeps its `LICENSE`, upstream README and
  `populate-dsb/NOTICE.md`; record new vendored code there.
- `scripts/check-tree.sh` (run in CI) enforces the file and wording rules
  above; run it before pushing.
- **Scripts must be referenced.** A script in `scripts/` or a crate is
  documented in a README or used by a test or CI; delete it when that stops
  being true.
