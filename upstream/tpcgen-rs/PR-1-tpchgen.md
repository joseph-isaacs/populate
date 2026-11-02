# perf(tpchgen): O(1) row skips, cheaper modulus, no per-row color copy

Patch: `0001-perf-tpchgen-O-1-row-skips-cheaper-modulus-no-per-ro.patch` (on `main` @ d2ccc58)

## What

Three drop-in changes to `tpchgen/src/random.rs`; generated values are unchanged.

- `RowRandomInt::advance_seed` memoizes `MULTIPLIER^count`. `row_finished` nearly always skips the same number of unused seeds, so finishing a row is one multiply instead of a square-and-multiply loop. The cache is only used for `count > 0` and is excluded from `PartialEq`.
- `next_rand` reduces mod 2^31−1 by folding (`2^31 ≡ 1 mod M`) instead of the multiply-high sequence the compiler emits for `%`.
- `RandomStringSequence::next_value` (`p_name`) no longer copies all 92 colors into a `Vec` per row. A partial Fisher-Yates that records only the swapped positions gives the same draws and result. The original is kept as `next_value_full_shuffle` for sequences longer than 16.

## Correctness

- New unit tests compare each change with the original: the fold against `%` (1M random values plus edge cases), the memoized jump against square-and-multiply (with varying seed usage, including usage above `seeds_per_row`), and the sparse shuffle against the full shuffle (counts 1–16).
- The existing reference-data integration tests pass unchanged.
- Differential test against released 4.0.0, rendering every row to its `.tbl` line (every field):
  - every row of all 8 tables at SF 1, plus 14 partitions per table reached with `part`/`part_count` at SF 100 (SF 1000 for supplier): **17.1M rows, 209M field values, all equal**.
  - a sweep over SF 0.01, 0.1, 1, 10, 100, 1k, 10k, 30k (the 64-bit `RandomBoundedLong` switch), 100k, with at least 12 partitions per table at each SF: all equal.
  - A planted bug (jump cache never invalidated) is caught at `orders` row 2.

## Performance

**Within noise on a shared 4-vCPU VM.** Most columns use every seed they reserve, so the square-and-multiply loop this removes rarely ran. Single-threaded SF 1, best of 3, three separate runs (released/patched):

| table | rows only | rows + `.tbl` text |
|---|---|---|
| part | 1.15× / 1.17× / 0.79× | 0.98× / 1.10× / 1.14× |
| others | 0.8–1.2×, no consistent direction | 0.86–1.28× |

The main concrete win is avoiding a 92-element allocation and copy per `part` row. This is a cleanup more than a speed-up.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
