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
[tpchgen-rs](https://github.com/datafusion-contrib/tpcgen-rs) project, in its
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
- **`.dat` written from Arrow.** Formatting tpcdsgen's rows with `Display`
  costs 10x their generation. `.dat` lines are instead written straight
  from the Arrow batches into a reserved buffer (~800 MB/s per core),
  matching tpcdsgen's text on every table (tested).
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

SF 1 (19.6M rows, 24 tables) on a 4-core container, output to tmpfs. The
container is noisy (±10% run to run), so these are bests of 2–3:

| output                   | threads | tpcgen-cli | populate-tpcds | speedup |
|--------------------------|--------:|-----------:|---------------:|--------:|
| `.dat`                   | 4       | 2.60 s     | 1.45 s         | 1.8x    |
| Parquet (snappy)         | 4       | 2.51 s     | 1.36 s         | 1.8x    |
| `.dat`                   | 1       | 8.25 s     | 4.18 s         | 2.0x    |
| Parquet (snappy)         | 1       | 8.03 s     | 4.58 s         | 1.8x    |
| Arrow only (`--format none`) | 4   |            | 0.71 s         |         |

Parquet files: 385 MB vs tpcgen-cli's 399 MB. SF 10 (192M rows), 4
threads, output to disk, before the fused store channel: `.dat` 15.7 s vs
23.3 s, Parquet 12.9 s vs 23.5 s.

### Where the time goes

`cargo run -p populate-tpcds --release --example stages` times each stage
per table on one thread. Summed over the tables, at SF 1:

| stage                        | time   |
|------------------------------|-------:|
| tpcdsgen row generation      | ~1.7 s |
| rows → Arrow                 | ~0.9 s |
| Arrow → `.dat` text          | ~1.3 s |
| Arrow → Parquet (arrow-rs)   | ~2.4 s |

tpcgen-cli runs the same row generation, so the difference is in the
rest:

- **`.dat`:** tpcgen-cli formats each row with `Display`, ~10x the cost of
  generating it (`store_sales`: 3.4 s vs 1.0 s here).
- **Parquet:** its 8Ki-row batches fall out of cache during conversion
  (`catalog_sales` converts 5x slower), and it dictionary-encodes decimals
  and ids (~40% of encoding time, no smaller files).
- **Cores:** it doesn't split tables under 1M source rows.

### The store channel, fused

`store_sales` and `store_returns` come from one loop (`src/fused.rs`): a
port of tpcdsgen's store sales and returns generators (Apache-2.0) built on
its public stream and key helpers, pushing values straight into Arrow
columns. tpcdsgen's returns generator replays every sales line item, so the
two tables cost the sales work twice. Together they now take ~0.6x the time
(tested equal to tpcdsgen-arrow for both tables). When only one is asked
for, the same loop skips the other's work.

That loop also skips dsdgen's end-of-row seed burning faster
(`src/seeds.rs`). Every column gets a fixed budget of random draws per row,
and tpcdsgen burns the unused ones one draw at a time: ~100 wasted draws per
ticket. The stream is a Lehmer generator, so burning `k` draws is one
multiply by `16807^k mod (2^31 - 1)`.

### What's left

- **The catalog and web channels.** They could be fused the same way:
  `catalog_returns` and `web_returns` cost ~0.35 s of generation on one
  thread.
- **tpcdsgen's other generators.** They still burn seeds draw by draw.
- **Parquet encoding.** This is arrow-rs, now the largest stage.
