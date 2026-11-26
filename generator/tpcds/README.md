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
- **`.dat` written from the columns.** Formatting tpcdsgen's rows with
  `Display` costs 10x their generation. `.dat` lines are instead written
  straight from the generated columns into a reserved buffer
  (`populate_table::text`), matching tpcdsgen's text on every table
  (tested).
- **The workers write the `.dat` files.** SF 1 is 1.2 GB of text. Written
  by one thread it took ~1.6 s with the other cores busy, the whole run,
  and workers sat blocked on it. Now a finished chunk is queued on its
  file and whichever worker finds the file idle writes the queue in order,
  so files are written in parallel and every core generates. 4 threads:
  ~1.6 → ~1.25 s (interleaved runs, all 24 files `cmp`-identical).
- **Every table is a `Distribution`.** Each table is one
  `populate_core::Distribution` of rows built from `populate-builtins`
  combinators, so its schema is `schema_of` its rows and `populate-table`
  turns it into Arrow, Parquet or text; no Arrow array or schema is built
  by hand. Dimensions are tpcdsgen's generators wrapped as distributions
  (`src/dims.rs`). `inventory` and `customer_demographics` (11.7M and 1.9M
  rows at SF 1) are cross products of keys counted up per column, plus one
  random quantity for inventory (`src/inventory.rs`).
- **One bitmap per row for NULLs.** dsdgen decides all of a sales row's
  NULLs with one draw. The sales and returns rows keep that bitmap
  (`src/masked.rs`) instead of a validity bit per column: it is pushed once
  per row, transposed 64 rows at a time into each column's validity for
  Arrow, and read per field when a row is printed.
- **Cheaper `.dat` numbers.** Up to 8 digits are made at once in one
  `u64` (multiply-shift divisions on 4-, 2- and 1-digit lanes), and the
  length is the count of leading zero bytes. `--example datbench` times
  formatting alone.
- **Parquet pages as if batches were even.** A ticket has 8 to 16 lines,
  so generated batches vary in length; the Parquet writer pages each
  column as if its rows came in batches of `--batch` rows
  (`write_parquet_many_in_batches`), so the files are those of even
  batches without re-cutting any array.

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
per table on one thread, in ms, at SF 1: tpcdsgen's own row generation,
then this crate's Arrow, `.dat` and Parquet output (each includes the
stages before it). A returns table is generated with its sales, so its
columns include the sales work.

| table                 | tpcdsgen rows | +Arrow | +`.dat` | +Parquet |
|-----------------------|--------------:|-------:|--------:|---------:|
| store_sales           | 259           | 354    | 591     | 960      |
| catalog_sales         | 218           | 283    | 532     | 720      |
| web_sales             | 96            | 130    | 271     | 324      |
| store_returns         | 386           | 362    | 671     | 427      |
| inventory             | 215           | 69     | 240     | 263      |
| customer_demographics | 150           | 22     | 91      | 140      |
| customer              | 56            | 72     | 84      | 111      |

tpcgen-cli runs the same row generation, so the difference is in the
rest:

- **`.dat`:** tpcgen-cli formats each row with `Display`, ~10x the cost of
  generating it.
- **Parquet:** its 8Ki-row batches fall out of cache during conversion,
  and it dictionary-encodes decimals and ids (~40% of encoding time, no
  smaller files).
- **Cores:** it doesn't split tables under 1M source rows.

### The sales channels

Each sales table and its returns (`store_`, `catalog_`, `web_`) come from
one distribution (`src/channels.rs`): ports of tpcdsgen's sales and
returns generators (Apache-2.0) built on its public stream and key
helpers, as one `flat_map` over tickets or orders that pushes each line to
the sales columns and, when it is returned, a row to the returns columns.
tpcdsgen's returns generators replay every sales line item; here the sales
work is done once. Each channel is tested equal to tpcdsgen-arrow for both
tables.

The channels also burn dsdgen's end-of-row seeds faster
(`src/seeds.rs`). Every column gets a fixed budget of random draws per row,
and tpcdsgen burns the unused ones one draw at a time: ~100 wasted draws per
ticket. The stream is a Lehmer generator, so burning `k` draws is one
multiply by `16807^k mod (2^31 - 1)`.
