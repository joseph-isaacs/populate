# populate-h2o

The [H2O db-benchmark](https://github.com/h2oai/db-benchmark) data sets,
byte-identical to the CSV its R datagen scripts write
(`_data/groupby-datagen.R` and `_data/join-datagen.R`, `set.seed(108)`),
on all cores, as CSV, Arrow `RecordBatch`es or Parquet.

```sh
# G1_1e7_1e2_0_0.csv: 1e7 rows, K = 100, no NAs, unsorted
cargo run -p populate-h2o --release --bin h2o-gen -- groupby --n 1e7 --k 1e2

# 5% NAs, sorted by id1..id6
cargo run -p populate-h2o --release --bin h2o-gen -- groupby --n 1e7 --k 1e2 --nas 5 --sort 1

# J1_1e7_NA_0_0.csv and its right-hand tables J1_1e7_{1e1,1e4,1e7}_0_0.csv
cargo run -p populate-h2o --release --bin h2o-gen -- join --n 1e7

# the same tables as Parquet
cargo run -p populate-h2o --release --bin h2o-gen -- join --n 1e7 --format parquet
```

Flags: `--n`, `--k` (groupby), `--nas` (percent), `--sort 0|1`, `--out`
(default `.`), `--format csv|parquet|none`, `--threads` (default all
cores), `--chunk` (rows per chunk = Parquet row group, default 256Ki). File
names are the scripts' own.

From Rust:

```rust
use populate_h2o::{GroupBy, Join};

let g = GroupBy::new(10_000_000, 100, 0, false);
g.write_csv(std::fs::File::create(g.file_name())?, 0)?;
populate_table::chunks::for_each_batch(&g, 0, |batch| { /* in order */ });

for t in Join::new(10_000_000, 0, false).tables() {
    populate_table::write_parquet(&t, std::fs::File::create(t.file_name())?, 0, None)?;
}
```

Arrow types: groupby `id1`–`id3` `Utf8View`, `id4`–`id6`, `v1`, `v2`
`Int32`, `v3` `Float64`; join `id1`–`id3` `Int32`, `id4`–`id6` (`"id<n>"`)
`Utf8View`, `v1` / `v2` `Float64`. NAs are nulls.

## Matching R

The reference is R 4.3.3 with data.table 1.14.10. Every data set the
benchmark uses at 1e7 rows (groupby `1e2_0_0`, `1e1_0_0`, `2e0_0_0`,
`1e2_0_1`, `1e2_5_0`; join `0_0`, `5_0`, `0_1`, all four tables each) is
byte-identical to the scripts' output, and so are groupby
`G1_1e8_1e2_0_0` (5.2 GB) and `G1_1e8_1e1_5_1` (NAs and sorted, 5.0 GB). The tests check those hashes, small groupby
data sets covering every knob (NAs from 5 to 100%, sorting, K = 1, K =
1000 where `"id1000" < "id101"`), and that the output is the same for any
chunk size and thread count, as CSV and as Arrow.

What that takes:

- **R's Mersenne Twister**, seeded as `set.seed()` scrambles the seed,
  and `sample()`'s default rejection sampling (R >= 3.6): `ceil(log2(n))`
  bits from the top 16 bits of one or two draws, retried while `>= n`.
- **`sample()`'s algorithms**: with replacement; without, R's partial
  Fisher-Yates over an index vector, or, for `n > 1e7` and `size <= n/2`,
  draws with a hash set of the values so far (both happen in the NA step).
- **The scripts' quirks**: `unique()` order (first appearance) decides
  which values become NA; `setkey` is a stable sort in C-locale string
  order with NA first; `split_xlr`'s `n * 1.1` index arithmetic follows
  R's `:` on doubles.
- **`round(x, 6)`** (R 4's `fround`, which picks the nearer of the two
  candidates in double arithmetic) and **`fwrite`'s number format**
  (data.table's own 15-significant-digit algorithm and power tables). All
  values are `k / 1e6` for an integer `k <= 1e8`, so the crate formats `k`
  directly; a test checks that equals the port of `fwrite`'s algorithm for
  all 1e8 + 1 of them.

## How it runs on all cores

- **Groupby**: each column is one run of draws, so one sequential pass
  over the stream (draws only, no formatting) records the generator state
  at the start of each chunk of each column. Chunks then regenerate their
  rows from those snapshots, on any thread. NAs come from the same pass
  (each id column's distinct values in order of first appearance) and the
  draws after it. Sorted data sets are generated in parallel, then sorted
  by a stable counting sort per column, in memory.
- **Join**: the keys are permutations and resamples of permutations,
  inherently sequential, so they are built in memory first; formatting
  runs on all cores.

Both are `populate_table` `Chunks` (Arrow, Parquet) and `TextChunks` (CSV),
so they use its parallel ordered writers.

## Speed

4 cores, CSV to disk, vs `Rscript` (data.table's `fwrite` is multi-threaded
too):

| data set | R | h2o-gen |
|---|---|---|
| groupby `G1_1e7_1e2_0_0` (509 MB) | 5.6 s | 0.41 s |
| groupby `G1_1e7_1e2_0_1` (sorted) | 6.6 s | 0.93 s |
| groupby `G1_1e8_1e2_0_0` (5.2 GB) | 61.6 s | 3.7 s |
| groupby `G1_1e8_1e1_5_1` (NAs, sorted) | 156 s | 13.3 s |
| join `J1_1e7_NA_0_0` (4 tables, 923 MB) | 54 s | 1.4 s |
| join `J1_1e8_NA_5_0` (4 tables, 9 GB) | out of memory | 20 s |
| join `J1_1e8_NA_0_1` (sorted) | out of memory | 32 s |

What makes the sequential parts fast:

- **The stream pass** counts accepted draws over whole 624-word
  generator blocks in a vectorisable loop (no exit test) and only scans
  for the exact end in the block where a chunk ends.
- **Shuffles** (`sample.int(n)`): the draws depend only on the generator,
  so they are made first (branch-free, a block at a time) and the swaps,
  which are random accesses into a large array, are applied with
  prefetching on another thread while the next column's draws are made.
- **Sorting** (`setkey`): a parallel counting sort scatters `(key, row)`
  pairs into buckets of the leading key columns, then the buckets are
  sorted on all threads. Sorted groupby rows are kept row-major, so
  reading them in sorted order costs one random access per row.

## Limits

- `N` up to `2^31 - 1` (R's integer vectors). Join takes what the script
  takes: `N >= 1e7`, a multiple of 1e6, and `N <= 1e8` or `N = 1e9`.
- Join at 1e8 is not compared with R: `Rscript join-datagen.R 1e8` runs
  out of memory on the 15 GB test machine.
- Sorted groupby data and all join data are held in memory (about 36
  bytes a row for sorted groupby, 20 for join), as R holds them.
- At `N = 1e9` the join script writes in batches of 1e8 rows with one
  header; the output here is the same file, but has not been compared
  with R's at that size.
