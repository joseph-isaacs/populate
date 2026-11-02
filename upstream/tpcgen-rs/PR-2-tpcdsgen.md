# perf(tpcdsgen): O(1) end-of-row seed skip, cheaper modulus

Patch: `0002-perf-tpcdsgen-O-1-end-of-row-seed-skip-cheaper-modul.patch` (on `main` @ d2ccc58)

## What

Two drop-in changes to `RandomNumberStream`; generated values are unchanged.

- `consume_remaining_seeds_for_row` used to draw every unused seed one at a time (through `generate_uniform_random_int`), for every column stream on every row. The new `RandomNumberStream::finish_row` skips them with one multiply by a memoized `MULTIPLIER^k`. Correctness doesn't depend on rows leaving the same number unused; the multiplier is recomputed whenever `k` changes.
- `next_random` reduces mod 2^31−1 by folding instead of Schrage's method (a division and a modulo per draw). Schrage is kept as the test reference. It is exact for seeds above `MAX_INT` too, which initial seeds of high global column numbers can be.

## Correctness

- New unit tests: the fold against Schrage (1M random seeds up to 2^32 plus edge cases), and `finish_row` against drawing the remaining seeds (varying usage, including above `seeds_per_row`).
- All existing `tpcdsgen` and `tpcdsgen-arrow` tests pass.
- Differential test against released 4.0.0, rendering every row with the crate's own CSV formatter (every field):
  - every row of all 24 data tables at SF 1, plus 14–20 chunks per table at SF 100, built the way `tpcgen-cli` does (`chunk_number`/`total_chunks` + `set_source_row_range`). Small tables are compared in full through 20 chunks. **35.5M rows, 548M field values, all equal.**
  - a sweep over SF 0.01, 0.1, 1, 10, 100, 1k, 10k, 100k, 300k, with at least 12 chunks per table at each SF: all equal.
  - A planted bug (jump cache never invalidated) is caught at `store_sales` row 29.
  - `dbgen_version` is excluded: it is one row stamped with the wall-clock time.

**Found while testing (not touched by this PR):** released 4.0.0 cannot generate `store` at SF 300,000. `Address::make_address_for_column` indexes a 1000-entry street-name list at 1053 (`string_values_distribution.rs:124`). It works at SF 100,000 and 200,000. The patched crate panics identically, so this PR doesn't change it.

## Performance

Modest, mostly on the sales tables. Single-threaded SF 1, best of 3, three separate runs (released/patched, row generation only):

| table | speed-up per run |
|---|---|
| store_sales | 1.09× / 1.36× / 1.31× |
| catalog_sales | 1.18× / 1.01× / 1.78× |
| web_sales | 1.25× / 1.20× / 1.13× |
| others | 0.7–1.45×, mostly within noise |

With CSV formatting included, formatting dominates and differences are within noise.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
