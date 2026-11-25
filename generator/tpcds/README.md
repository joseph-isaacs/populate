# populate-tpcds

TPC-DS data byte-identical to the reference `dsdgen`, on all cores: all 24
tables as Arrow `RecordBatch`es, Parquet, or dsdgen's own `.dat` text.

```sh
# all tables at SF 1 as Parquet in ./tpcds (one file per table)
cargo run -p populate-tpcds --release --bin tpcds-gen -- --sf 1 --out tpcds

# dsdgen's .dat files, byte for byte
cargo run -p populate-tpcds --release --bin tpcds-gen -- --sf 1 --out tpcds --format dat

# a few tables, generation only (no output), 4 threads
cargo run -p populate-tpcds --release --bin tpcds-gen -- \
    --sf 10 --tables store_sales,store_returns --format none --threads 4
```

Flags: `--sf`, `--out`, `--format parquet|dat|none`, `--tables`,
`--threads` (default all cores), `--batch`, `--row-group` (rows per chunk,
default 256Ki), `--compression snappy|none`, `--no-dictionary`,
`--encoding latin1|utf8` (`.dat` only).

From Rust:

```rust
use populate_tpcds::{Encoding, TableName, Tpcds};

let ds = Tpcds::new(10.0);
let sales = ds.table(TableName::StoreSales);
sales.for_each_batch(0, |batch| { /* RecordBatch, in table order */ });
sales.write_parquet(std::fs::File::create("store_sales.parquet")?, 0, None)?;
sales.write_dat(std::fs::File::create("store_sales.dat")?, 0, Encoding::Latin1)?;

// first 1000 tickets only (the same rows, not a smaller scale factor)
let head = sales.limit_source_rows(1000).collect(0);
```

`write_parquet_many`, `write_dat_many` and `for_each_batch_many` run
several tables on one shared pool (what `tpcds-gen` does).

## Where the data comes from

The values are computed by [`tpcdsgen`](https://crates.io/crates/tpcdsgen),
the Apache-2.0 Rust port of dsdgen from the
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs) project, in its
default `CompatMode::C`, which reproduces C dsdgen bug for bug. This crate
adds the engine around it:

- **Chunks of source rows.** Each table is split into chunks of consecutive
  source rows (tickets for `store_sales`, orders for `catalog_sales`, rows
  elsewhere). tpcdsgen's random streams skip exactly to any row, so each
  chunk is generated from scratch, independently, and the chunks
  concatenate to the sequential output for any chunk size and thread count
  (tested for every table). dsdgen and `tpcgen-cli` only split a table into
  parts of at least 1M source rows, so at SF 1 `store_sales` (240K tickets)
  runs on one core there.
- **One pool for all tables.** Every chunk of every table is a job; small
  tables fill in around big ones.
- **Parquet encoded on the workers.** Each job generates *and* encodes its
  row group (`populate_table::write_parquet_many`); the main thread only
  appends finished row groups. Dictionary encoding is on for text, keys,
  dates and quantities and off for decimals and 64-bit ids, where it never
  pays: that cuts encoding time by ~40% and the files come out slightly
  smaller (`parquet_props`).
- **Columns encoded directly.** Keys, dates, quantities, decimals and
  `Utf8View` text are encoded by `populate-table` instead of
  arrow-rs's column writer: definition levels come straight from the null
  bitmap, values are copied 64 rows at a time, and the dictionary is a
  table indexed by value where the values span a small range. The chunks
  are what arrow-rs writes (same encodings, statistics and level
  histograms, plus page indexes). Checked with DuckDB on all 24 SF 1 tables: the same rows as arrow-rs's files and
  the same statistics in all 1,515 column chunks.
- **`.dat` written from Arrow.** Formatting tpcdsgen's rows with `Display`
  costs 10x their generation. `.dat` lines are instead written straight
  from the Arrow batches into a reserved buffer (~800 MB/s per core),
  matching tpcdsgen's text on every table (tested).
- **The workers write the `.dat` files.** SF 1 is 1.2 GB of text. Written
  by one thread it took ~1.6 s with the other cores busy, the whole run,
  and workers sat blocked on it. Now a finished chunk is queued on its
  file and whichever worker finds the file idle writes the queue in order,
  so files are written in parallel and every core generates. 4 threads:
  ~1.6 → ~1.25 s (interleaved runs, all 24 files `cmp`-identical).
- **Inventory and demographics into columns.** `inventory` (11.7M rows
  at SF 1) and `customer_demographics` (1.9M) are cross products of keys,
  plus one random quantity and a null bitmap for inventory. `src/direct.rs`
  fills their columns directly instead of building a tpcdsgen row struct
  per row and converting (both tested equal to tpcdsgen-arrow).
- **Cheaper `.dat` numbers.** Up to 8 digits are made at once in one
  `u64` (multiply-shift divisions on 4-, 2- and 1-digit lanes), the length
  is the count of leading zero bytes, and null bits are read straight from
  the bitmap (`NullBuffer::is_valid` was a quarter of formatting time).
  `--example datbench` times formatting alone.
  Together with the two tables above: 28% fewer instructions for a
  one-thread `.dat` run and 27% for Parquet (callgrind, SF 0.2), ~9% less
  wall time at 1 thread and 6–9% at 4 (interleaved, best of 3).
- **Cache-sized conversion.** tpcdsgen-arrow converts rows to columns one
  column at a time over the buffered rows; at 8Ki rows a `catalog_sales`
  batch (~3 MB) falls out of L2 and conversion is 5x slower. Batches are
  generated 2Ki rows at a time.

## Byte-identical: how it's checked

- All 24 SF 1 `.dat` files from `tpcds-gen --format dat --encoding utf8`
  are identical (`cmp`) to `tpcgen-cli tpcds dat -s 1`.
- The test suite checks, for every table: chunked and multi-threaded Arrow
  output equals one sequential generator; `.dat` from the Arrow path equals
  tpcdsgen's row `Display`; and the first `store_sales` line is dsdgen's
  (`2451813|65495|3617|67006|591617|...`).

**Encoding.** dsdgen copies its distribution files' bytes, which are
ISO-8859-1, so its `customer.dat` has one-byte `Ô` and `É`
(`CÔTE D'IVOIRE`, `RÉUNION` in `c_birth_country`, the only non-ASCII
values). `tpcgen-cli` writes them as UTF-8. `--encoding latin1` (the default)
writes dsdgen's bytes; `--encoding utf8` writes tpcgen-cli's. The two differ
only in those characters (checked with `iconv`). The Latin-1 default
follows the distribution files' encoding; it has not been diffed against
dsdgen's own output.

**Reason.** The spec lists 35 `reason` rows at SF 1; dsdgen (and this
crate) writes all 75.

`dbgen_version` is not generated: its contents are the generation time and
command line.

## Performance

SF 1 (19.6M rows, 24 tables) and SF 10 (192M rows) on a 4-core cloud VM,
`cargo xtask bench --generators tpcds --scale 1,10` (see
[BENCHMARKS.md](../../BENCHMARKS.md)) against `tpcgen-cli tpcds`. The
VM is noisy (±10% run to run, and it drifts), so both tools run
interleaved and each keeps its best time: best of 3 at SF 1 (output to
tmpfs), best of 2 at SF 10 (output to disk). `.dat` runs use
`--encoding utf8`, so the files are `cmp`-identical to tpcgen-cli's.

| SF | output           | threads | tpcgen-cli | populate-tpcds | speedup |
|---:|------------------|--------:|-----------:|---------------:|--------:|
| 1  | `.dat`           | 1       | 9.51 s     | 3.04 s         | 3.1x    |
| 1  | `.dat`           | 4       | 3.05 s     | 0.92 s         | 3.3x    |
| 1  | Parquet (snappy) | 1       | 10.50 s    | 4.47 s         | 2.3x    |
| 1  | Parquet (snappy) | 4       | 3.38 s     | 1.21 s         | 2.8x    |
| 10 | `.dat`           | 1       | 92.4 s     | 30.1 s         | 3.1x    |
| 10 | `.dat`           | 4       | 28.6 s     | 11.5 s         | 2.5x    |
| 10 | Parquet (snappy) | 1       | 102.0 s    | 43.5 s         | 2.3x    |
| 10 | Parquet (snappy) | 4       | 27.9 s     | 11.6 s         | 2.4x    |

Parquet files: 393 MB vs tpcgen-cli's 418 MB at SF 1, 4,087 MB vs
4,199 MB at SF 10.

### Where the time goes

`cargo run -p populate-tpcds --release --example stages` times each stage
per table on one thread. Summed over the tables, at SF 1:

| stage                                                     | time    |
|-----------------------------------------------------------|--------:|
| the six sales and returns tables, fused, into columns     | ~1.1 s  |
| the other 18 tables: tpcdsgen row generation              | ~0.55 s |
| the other 18 tables: rows → Arrow (tpcdsgen-arrow)        | ~0.3 s  |
| Arrow → `.dat` text                                       | ~1.3 s  |
| Arrow → Parquet                                           | ~2.2 s  |

tpcdsgen is rows all the way down: each generator builds a row struct
(text as owned `String`s), and tpcdsgen-arrow buffers 2Ki of them and
walks the buffer once per column. Building columns directly is not much
cheaper by itself: the store port fills `store_sales`' columns in about the
time tpcdsgen-arrow converts them (~150 ms over 216 ms of generation). The
fused channels win by not replaying sales for returns and by skipping
seeds.

tpcgen-cli runs the same row generation, so the difference is in the
rest:

- **`.dat`:** tpcgen-cli formats each row with `Display`, ~10x the cost of
  generating it (`store_sales`: 3.4 s vs 1.0 s here).
- **Parquet:** its 8Ki-row batches fall out of cache during conversion
  (`catalog_sales` converts 5x slower), and it dictionary-encodes decimals
  and ids (~40% of encoding time, no smaller files).
- **Cores:** it doesn't split tables under 1M source rows.

### The sales channels, fused

Each sales table and its returns (`store_`, `catalog_`, `web_`) come from
one loop (`src/fused.rs`): ports of tpcdsgen's sales and returns
generators (Apache-2.0) built on its public stream and key helpers,
pushing values straight into Arrow columns. tpcdsgen's returns generators
replay every sales line item; the fused loop does the sales work once.
The loops push each row's values, in schema order, into a `Row`: the
Arrow `Builder` appends them to columns, and for `.dat` output `Text`
formats them straight into the chunk's text, so the sales and returns
tables (~75% of the `.dat` bytes) never become Arrow batches.
Each channel is tested equal to tpcdsgen-arrow for both tables, together
and alone, and its text equal to the text of its Arrow batches. When
only one table is asked for, the loop skips the other's work.

The loops also burn dsdgen's end-of-row seeds faster
(`src/seeds.rs`). Every column gets a fixed budget of random draws per row,
and tpcdsgen burns the unused ones one draw at a time: ~100 wasted draws per
ticket. The stream is a Lehmer generator, so burning `k` draws is one
multiply by `16807^k mod (2^31 - 1)`.
