# populate-tpch

TPC-H data byte-identical to `dbgen` (and `tpchgen-cli`), on all cores: all
8 tables, every column, at any scale factor up to 10,000, as Arrow
`RecordBatch`es, Parquet, or dbgen's `.tbl` text.

```sh
# all tables at SF 1 as Parquet in ./tpch (one file per table)
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch

# dbgen's .tbl files, byte for byte
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch --format tbl

# a few tables, generation only (no output), 4 threads
cargo run -p populate-tpch --release --bin tpch-gen -- \
    --sf 10 --tables lineitem,orders --format none --threads 4
```

Flags: `--sf`, `--out`, `--format parquet|tbl|none`, `--tables`,
`--threads` (default all cores), `--batch`, `--row-group` (rows per chunk,
default 256Ki), `--compression snappy|none`, `--no-dictionary`.

From Rust:

```rust
use populate_tpch::{TableName, Tpch};

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
[tpchgen-rs](https://github.com/datafusion-contrib/tpcgen-rs)): this crate
uses its distributions, word lists and text grammar, and is tested equal to
it. The generators are this crate's own:

- **Chunks of source rows.** Each table is split into chunks of
  consecutive source rows (orders for `lineitem`, parts for `partsupp`,
  rows elsewhere). dbgen's streams are Lehmer generators
  (`x' = 16807 x mod 2^31 - 1`) that reserve a fixed number of draws per
  row, so a stream jumps to row `k` with one multiply by `16807^k`, each
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
  one. Two changes:
  - The grammar is compiled: each sentence, noun-phrase and verb-phrase
    shape becomes a list of word draws, and each word is one 32-byte copy
    of the word with what follows it (a space, `", "`, `" the "`). No
    syntax is read per word, and there is no mispredicted jump per word.
    One thread: 0.50 → 0.38 s.
  - `src/pool_index.rs` lists 512 sentence starts (draw and byte), evenly
    spaced through the pool. The parts between them are generated
    independently, each straight into its place in the pool. A test
    checks the list against the serial parse and can regenerate it.
    4 threads: 0.23 → 0.10 s.

  The pool is built once per process (`Tpch::prepare`), and is tested
  equal to tpchgen's byte for byte.
- **Line items by order.** `lineitem` streams reserve 7 slots per order;
  the return flag of the k-th line past its receipt date uses the k-th
  slot, as in dbgen. Comment views are made in their own pass after the
  other columns, so their (random) pool reads overlap.
- **Shared engine.** Chunks run on `populate-table`'s pool: Parquet row
  groups are encoded on the workers (directly for integer, date, decimal
  and string columns), and `.tbl` text is formatted from the Arrow
  batches by the writer shared with TPC-DS (`populate_table::text`) and
  written by whichever worker finds the file idle. Dictionary encoding is
  off for decimals, 64-bit keys and the high-cardinality text (comments,
  names, addresses, phones), where it never pays (`parquet_props`).

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

On a 4-core container, `scripts/e2e-bench.sh --suite tpch` against
`tpcgen-cli tpch` (the CLI of tpchgen-rs). The container is noisy, so the
two tools run interleaved and each keeps its best time: best of 3 at SF 1
(output to tmpfs), best of 2 at SF 10 (output to disk; 11 GB of text does
not fit in this container's tmpfs).

| SF | output           | threads | tpcgen-cli | populate-tpch | speedup |
|---:|------------------|--------:|-----------:|--------------:|--------:|
| 1  | `.tbl`           | 1       | 6.44 s     | 3.47 s        | 1.9x    |
| 1  | `.tbl`           | 4       | 2.61 s     | 1.02 s        | 2.6x    |
| 1  | Parquet (snappy) | 1       | 6.74 s     | 3.95 s        | 1.7x    |
| 1  | Parquet (snappy) | 4       | 2.50 s     | 1.15 s        | 2.2x    |
| 10 | `.tbl`           | 1       | 60.4 s     | 35.4 s        | 1.7x    |
| 10 | `.tbl`           | 4       | 20.3 s     | 9.21 s        | 2.2x    |
| 10 | Parquet (snappy) | 1       | 64.3 s     | 45.6 s        | 1.4x    |
| 10 | Parquet (snappy) | 4       | 19.7 s     | 11.4 s        | 1.7x    |

The SF 1 rows are after the faster text pool; the SF 10 rows are from
before it (about 0.4 s more per run on one thread). The `.tbl` files are
identical (1,100 MB at SF 1). Parquet files: 358 MB
vs tpcgen-cli's 361 MB at SF 1, 3,658 MB vs 3,915 MB at SF 10.

### Where the time goes

`cargo run -p populate-tpch --release --example tpch_stages -- 1` times each
stage per table on one thread, and `--example tpch_columns` the Parquet encoding
of each column. At SF 1, summed over the tables:

| stage                       | time    |
|-----------------------------|--------:|
| the text pool               | ~0.4 s  |
| generation into Arrow       | ~0.95 s |
| Arrow → `.tbl` text         | ~1.5 s  |
| Arrow → Parquet (snappy)    | ~2.9 s  |

Formatting text and encoding Parquet now cost more than making the data.
In Parquet, `lineitem`'s comment column alone is ~0.6 s: 6M unique
strings, read from random places in the pool and compressed.
