# Contributing

Issues and pull requests are welcome. If a generator's output differs from
its reference tool, open an issue with the command, the scale factor and
the first differing line or row.

## Build and test

What CI runs (`.github/workflows/ci.yml`):

```bash
scripts/check-tree.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Clippy's `--all-targets` checks that the benches compile; CI does not
build or run them in the optimised `bench` profile.

CI disables debug symbols for development and test builds with
`CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_PROFILE_TEST_DEBUG=0` to reduce
code generation and artifact size. Tests retain debug assertions and
overflow checks. Local builds keep their normal debug symbols.

CI uses `rust-cache` for Cargo dependencies and `sccache` with GitHub's
cache storage for Rust compilation during tests and Python extension
builds. Compiler results are keyed by their inputs; unchanged library
compiles can be reused across checkouts. Linking and test execution still
run. Incremental compilation is disabled for compatibility with sccache,
and the action reports cache statistics in each build job.

Plain `cargo build` / `cargo test` cover only the workspace
`default-members`, which leave out `populate-bench` and the `parquet`
feature.

The minimum supported Rust version is the workspace `rust-version`
(1.92); CI's `msrv` job checks the workspace builds with it.

## Release packages

Run the publication check from a clean checkout:

```bash
cargo publish --workspace --dry-run --all-features
```

Cargo packages and builds all publishable crates in dependency order using
an isolated staging registry. The dry run uploads nothing. It includes the
core libraries, all ten generators, and the two DSB support crates;
benchmark and Python binding crates remain unpublished.

The facade's book chapters live in `populate/book/` so its published
archive contains the documentation examples. `book/book.toml` points
mdBook at those same chapters. LDBC's external reference dictionaries are
not bundled; its README describes how to obtain them.

## Code Quality

- Run `cargo fmt` before committing
- Run clippy as CI does (above) and fix all warnings
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
  tables, run logs, benchmark result files and graphs (`*.png`, `*.svg`)
  go in the PR description, not the repository. A crate README may keep a
  current performance section with the command that reproduces it and the
  machine it ran on; replace its numbers rather than adding before/after
  comparisons.
- **No planning docs.** Surveys, TODO lists, design proposals and
  "future work" notes go in GitHub issues or the PR. Design docs in the tree
  (`DESIGN.md` in the `populate` crate) describe the code as it is; update them in the
  same change that alters the design, and delete what no longer applies.
- **Don't commit prompts, scratch notes or tool output** (pasted chat
  messages, `.cargo-ok`, `mutants.out`, local logs).
- **One home per fact.** The root README gives the overview and links to
  each crate's README; crate READMEs hold flags, verification and
  performance. Don't repeat the same section in both.
- **Comments say why, not what.** Skip comments that restate the code.
  Keep the ones that explain a non-obvious choice, an invariant, or a
  reference tool's quirk being reproduced (with where it comes from).
- **Keep licence files.** The vendored `dsbgen` and `dsbgen-arrow` crates
  keep their `LICENSE` and upstream README, and the DSB crate's `NOTICE.md`
  records what was vendored and under which licence. Each crate containing
  vendored or ported code keeps a `NOTICE.md` identifying the upstream
  sources, versions, adaptations and licences, plus required licence texts
  and upstream notices. Preserve source-file copyright and permission
  notices, mark modified Apache-2.0 files, and make the crate's SPDX
  `license` expression reflect all included code.
- **Scripts must be referenced.** A script in `scripts/` or a crate is
  documented in a README or used by a test or CI; delete it when that stops
  being true. Test fixtures (such as YCSB's `tests/data/*.tsv`) are fine.
- **Run `scripts/check-tree.sh` before pushing.** CI runs it too. It
  catches the common cases of the file and wording rules above (images,
  `TODO.md`, `.cargo-ok`, "PR #N", "before this PR", ...), not every one.
