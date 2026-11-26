# populate-gen-tpch

TPC-H data byte-identical to `dbgen` (and `tpchgen-cli`), on all cores: all
8 tables, every column, at any scale factor up to 10,000, as Arrow
`RecordBatch`es, Parquet, or dbgen's `.tbl` text.

```sh
# all tables at SF 1 as Parquet in ./tpch (one file per table)
cargo run -p populate-gen-tpch --release --bin tpch-gen -- --sf 1 --out tpch

# dbgen's .tbl files, byte for byte
cargo run -p populate-gen-tpch --release --bin tpch-gen -- --sf 1 --out tpch --format tbl

# a few tables, generation only (no output), 4 threads
cargo run -p populate-gen-tpch --release --bin tpch-gen -- \
    --sf 10 --tables lineitem,orders --format none --threads 4
```

Flags: `--sf`, `--out`, `--format parquet|tbl|none`, `--tables`,
`--threads` (default all cores), `--batch`, `--row-group` (rows per chunk,
default 256Ki) or `--row-group-bytes` (uncompressed Parquet bytes per row
group, as `tpcgen-cli --row-group-bytes` counts them, measured on a sample
of each table), `--compression snappy|none`, `--no-dictionary`.

From Rust:

```rust
use populate_gen_tpch::{TableName, Tpch};

let h = Tpch::new(10.0);
let lineitem = h.table(TableName::LineItem);
lineitem.for_each_batch(0, |batch| { /* RecordBatch, in table order */ });
lineitem.write_parquet(std::fs::File::create("lineitem.parquet")?, 0, None)?;
lineitem.write_tbl(std::fs::File::create("lineitem.tbl")?, 0)?;

// the first 1000 orders' line items (the same rows, not a smaller scale factor)
let head = lineitem.limit_source_rows(1000).collect(0);
```

`write_parquet_many`, `write_tbl_many` and `for_each_batch_many` run
several tables on one shared pool (what `tpch-gen` does). The Arrow
schemas are tpchgen-arrow's: `Int64` keys, `Decimal128(15, 2)` money,
`Date32` dates and `Utf8View` text.

## Where the data comes from

The values are dbgen's, as ported by
[`tpchgen`](https://crates.io/crates/tpchgen) (Apache-2.0, from
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs)): this crate
uses its distributions, word lists and text grammar, and is tested equal to
it. The generators are this crate's own:

- **Each table is one populate distribution.** A table is a tuple of
  named columns built from `populate-builtins` combinators (`draws`,
  `derive`, `children`, `flat_map`, `format`, `named`) over dbgen's
  streams (`populate_combinators::lehmer`), run by
  `populate_table::DynTable`. Its Arrow schema is read off that
  distribution: the names it carries and the types of the columns it
  collects into (`Date32`, `Decimal<15, 2>`, `Utf8View` text). Nothing is
  declared twice, and no rows are generated to get it.
- **Chunks of source rows.** Each table is split into chunks of
  consecutive source rows (orders for `lineitem`, parts for `partsupp`,
  rows elsewhere). dbgen's streams are Lehmer generators
  (`x' = 16807 x mod 2^31 - 1`) that reserve a fixed number of draws per
  row, so a stream jumps to row `k` with one multiply by `16807^k` (the
  stream is a `SeekRng`, and `Distribution::skip` is that jump), each
  chunk is generated from scratch, and the chunks concatenate to the
  sequential output for any chunk size and thread count (tested for every
  table). `tpchgen-cli` splits a table into at most `--num-threads` parts.
- **Columns, not rows.** Each stream's draws for a block of rows are made
  at once by 16 independent LCG lanes that vectorise, then mapped to
  ranges with integer arithmetic (proven equal to dbgen's floating-point
  formula for every range under 2^21; floating point above it). Columns are
  filled straight from the draws.
- **Text is never copied.** Comments are cut from dbgen's 300 MiB text
  pool, and distribution values (colours, ship modes, nations, ...) from
  one dictionary of all of them. Both are shared Arrow buffers, so every
  comment and value is a `Utf8View` into them: 16 bytes per row, no string
  copied, whatever the comment's length.
- **The text pool, fast and on all cores.** dbgen builds the pool by
  parsing one long stream of draws into sentences from a small grammar,
  which is sequential: where a sentence starts depends on every earlier
  one. Here:
  - The grammar is compiled: each sentence, noun-phrase and verb-phrase
    shape becomes a list of word draws, and each word is one 32-byte copy
    of the word with what follows it (a space, `", "`, `" the "`). No
    syntax is read per word, and there is no mispredicted jump per word.
    One thread builds the pool in about 0.38 s.
  - `src/pool_index.rs` lists 512 sentence starts (draw and byte), evenly
    spaced through the pool. The parts between them are generated
    independently, each straight into its place in the pool. A test
    checks the list against the serial parse and can regenerate it.
    4 threads build the pool in about 0.10 s.

  The pool is built once per process (`Tpch::prepare`), and is tested
  equal to tpchgen's byte for byte.
- **Line items by order.** `lineitem` is a `flat_map` over orders: its
  streams reserve 7 slots per order and an order with `k` lines writes its
  first `k`; the return flag of the k-th line past its receipt date uses
  the k-th slot, as in dbgen. `partsupp` is `children` of parts, 4 per
  part. Comment views are made in their own pass when the column becomes
  an array, so their (random) pool reads overlap.
- **Shared engine.** Chunks run on `populate-table`'s pool: Parquet row
  groups are encoded on the workers (directly for integer, date, decimal
  and string columns), and `.tbl` text is formatted from the Arrow
  batches by the writer shared with TPC-DS (`populate_table::text`) and
  written by whichever worker finds the file idle. Dictionary encoding is
  off for 64-bit keys, the high-cardinality text (comments, names,
  addresses, phones) and the decimals with many distinct values (prices,
  balances), where it never pays (`parquet_props`).
- **Huge pages for the pool.** On Linux the pool asks for 2 MiB pages
  (`madvise`), so the random comment reads don't also miss the TLB:
  about 6% off a `.tbl` run on one thread, 10% on four.

The largest supported scale factor is 10,000: above ~10,737 dbgen's part
keys overflow 32 bits, and from 30,000 tpchgen switches to a 64-bit
generator whose partitioned output differs from its sequential output.

## Byte-identical: how it's checked

- All 8 SF 1 `.tbl` files from `tpch-gen --format tbl` are identical
  (`cmp`) to `tpcgen-cli tpch tbl -s 1`.
- The test suite checks, for every table:
  - Arrow output equal to tpchgen-arrow at SF 0.01;
  - `.tbl` equal to tpchgen's row `Display`;
  - the same output for any chunk size, batch size and thread count;
  - Parquet round-trips.
  At SF 10 and SF 1000 it also compares a chunk deep in each table (part
  78 of 100) with tpchgen's partitioned generators, which covers keys and
  ranges past 2^21. The text pool and every distribution are tested equal
  to tpchgen's.

## Performance

On a 4-core cloud VM, `cargo xtask bench --generators tpch --scale 1,10`
(see [BENCHMARKS.md](../../BENCHMARKS.md)) against
`tpcgen-cli tpch` 4.0 (the CLI of tpcgen-rs), both with the same thread
count, and for Parquet both with Snappy and `--row-group-bytes 7340032`
(tpcgen-cli's default). The VM is noisy, so the two tools run interleaved
and each keeps its best time: best of 3 at SF 1 (output to tmpfs), best of
2 at SF 10 (output to disk; 11 GB of text does not fit in the VM's tmpfs).

| SF | output           | threads | tpcgen-cli | populate-gen-tpch | speedup |
|---:|------------------|--------:|-----------:|--------------:|--------:|
| 1  | `.tbl`           | 1       | 8.24 s     | 3.16 s        | 2.6x    |
| 1  | `.tbl`           | 4       | 3.26 s     | 0.87 s        | 3.7x    |
| 1  | Parquet (snappy) | 1       | 8.95 s     | 4.19 s        | 2.1x    |
| 1  | Parquet (snappy) | 4       | 4.11 s     | 1.28 s        | 3.2x    |
| 10 | `.tbl`           | 1       | 74.4 s     | 32.5 s        | 2.3x    |
| 10 | `.tbl`           | 4       | 29.7 s     | 11.3 s        | 2.6x    |
| 10 | Parquet (snappy) | 1       | 82.0 s     | 40.2 s        | 2.0x    |
| 10 | Parquet (snappy) | 4       | 28.7 s     | 11.4 s        | 2.5x    |

The `.tbl` files are identical (1,101 MB at SF 1; the bench checks the
bytes). Parquet files have the same schema and rows; with each tool's own
dictionary choices they come to 337 MB vs tpcgen-cli's 362 MB at SF 1,
3,452 MB vs 3,915 MB at SF 10.

### Where the time goes

`cargo run -p populate-gen-tpch --release --example tpch_stages -- 1` times each
stage per table on one thread, and `--example tpch_columns` the Parquet encoding
of each column. At SF 1, summed over the tables:

| stage                       | time    |
|-----------------------------|--------:|
| the text pool               | ~0.4 s  |
| generation into Arrow       | ~0.95 s |
| Arrow → `.tbl` text         | ~1.5 s  |
| Arrow → Parquet (snappy)    | ~2.9 s  |

Formatting text and encoding Parquet cost more than making the data.
In Parquet, `lineitem`'s comment column alone is ~0.6 s: 6M unique
strings, read from random places in the pool and compressed.
