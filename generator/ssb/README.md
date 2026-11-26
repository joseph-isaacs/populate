# populate-gen-ssb

Star Schema Benchmark (SSB) data byte-identical to
[`ssb-dbgen`](https://github.com/eyalroz/ssb-dbgen), on all cores: the five
SSB tables (`part`, `supplier`, `customer`, `date`, `lineorder`) and the
denormalised `lineorder_flat`, as Arrow `RecordBatch`es, Parquet, or
ssb-dbgen's `.tbl` text.

```sh
# all five tables at SF 1 as Parquet in ./ssb (one file per table)
cargo run -p populate-gen-ssb --release --bin ssb-gen -- --sf 1 --out ssb

# ssb-dbgen's .tbl files, byte for byte
cargo run -p populate-gen-ssb --release --bin ssb-gen -- --sf 1 --out ssb --format tbl

# SSB-flat (lineorder joined with customer, supplier and part)
cargo run -p populate-gen-ssb --release --bin ssb-gen -- --sf 10 --tables lineorder_flat
```

Flags are `tpch-gen`'s: `--sf`, `--out`, `--format parquet|tbl|none`,
`--tables`, `--threads` (default all cores), `--batch`, `--row-group`,
`--compression snappy|none`, `--no-dictionary`.

From Rust:

```rust
use populate_gen_ssb::{Ssb, TableName};

let s = Ssb::new(10.0);
let lo = s.table(TableName::LineOrder);
lo.for_each_batch(0, |batch| { /* RecordBatch, in table order */ });
lo.write_parquet(std::fs::File::create("lineorder.parquet")?, 0, None)?;
lo.write_tbl(std::fs::File::create("lineorder.tbl")?, 0)?;
```

Column names are ssb-dbgen's `ssb.ddl`. Integers are `Int32`
(`lo_orderkey` is `Int64`), dates are `Int32` `YYYYMMDD` as ssb-dbgen
prints them, and text is `Utf8View`. `lineorder_flat` has `lineorder`'s 17
columns and then the customer, supplier and part columns of ClickHouse's
`lineorder_flat` (no keys repeated, no date columns).

## What ssb-dbgen does, and what this matches

ssb-dbgen is an old fork of TPC-H's `dbgen`: the same Park–Miller streams
(one per column, a fixed number of draws reserved per row) and the same
distributions (its `dists.dss` values equal TPC-H's, so this crate takes
them from [`tpchgen`](https://crates.io/crates/tpchgen)). This crate is a
clean reimplementation of its output; no ssb-dbgen code is used. It
reproduces ssb-dbgen's quirks, because they are in the data:

- from SF 1 `part` has `200,000 * floor(1 + log2(SF))` rows, but
  `lo_partkey` is drawn up to `200,000 * (floor(ln(SF)) + 1)`;
- below SF 1 every table is scaled, including `date`; from SF 1 the scale is
  truncated to an integer (SF 2.5 is SF 2);
- supplier and customer share the city and phone streams (customer `c`
  continues where the suppliers stopped), and each `date` row skips a row
  of the order-level streams, so `lineorder` starts them at row 2557 (below
  SF 1, at the scaled `date` row count).
  These hold for the default run of all five tables; `dbgen -T c` alone
  gives other customer cities and phone numbers;
- `d_dayofweek` is a day late (1992-01-01, a Wednesday, is `Thursday`),
  and `d_daynuminweek` with it.

## Byte-identical: how it's checked

- All five `.tbl` files from `ssb-gen --format tbl` are identical (`cmp`)
  to `dbgen -s SF` (default CMake build) at SF 0.0001, 0.003, 0.01, 0.1,
  0.37, 1, 1.5, 2, 3, 8 and 10, and the first 100+ MB of every file at SF 100
  and 1100 (where key ranges pass 2^21 and the floating-point mapping is
  used). `scripts/ssb-check.sh` builds ssb-dbgen and repeats the check.
- The tests check FNV hashes of ssb-dbgen's files: every table at SF 0.01,
  the dimension tables at SF 1 and 8, and ranges of `lineorder` and
  `customer` deep in SF 1, 8, 100 and 1100. With `SSB_DBGEN=/path/to/dbgen`, an ignored test runs
  dbgen and compares whole files at four more scale factors.
- Chunked, multi-threaded output equals one sequential chunk for every
  table and many chunk / batch sizes; Parquet round-trips;
  `lineorder_flat` equals the join.

## How it is generated

On `populate`'s combinators, `populate-combinators`' Lehmer streams and
`populate-table`'s engine:

- **Each table is one `Distribution`**: a tuple of named columns, each
  a distribution over source rows, run by `populate_table::DistTable`.
  The Arrow schema is read off it (`populate_sink_arrow::schema_of`):
  names from `named`, types from the column types the values collect
  into. Every dbgen stream is a seekable leaf (`draws`, `Ints`), computed
  columns are `derive`s of the streams they read, and `lineorder` is the
  children of each block of orders, written a column at a time.
- **Chunks of source rows** (orders for `lineorder`). Every stream jumps to
  a chunk's first row with one multiply by `16807^k`, so chunks are
  independent and concatenate to the sequential output for any chunk size
  and thread count.
- **Columns, not rows.** Each stream's draws for a block of rows are made
  at once by 16 LCG lanes (`populate_combinators::lehmer::Lcg`) and mapped to
  ranges with integer arithmetic.
- **A shared dictionary.** SSB has no comments: apart from names,
  addresses and phone numbers, every string is from a small set
  (nations, cities, the 92² part names, the 1,000 brands, the date table's
  names). They are built once into one buffer, and those columns are
  `Utf8View`s into it, 16 bytes per row and no string copied.
- **`lineorder_flat`** draws `customer`, `supplier` and `part` once, kept as
  a few bytes of codes per row plus their name, address and phone columns,
  then joins each `lineorder` batch by key: dictionary views from the
  codes, and views into the dimension columns for the rest (no copy).
- Parquet row groups and `.tbl` text are encoded on the workers by
  `populate-table`, as for TPC-H and TPC-DS.

## Performance

4-core cloud VM, `.tbl` output to tmpfs, SF 1 (600 MB, 6.0 M line
orders); ssb-dbgen is single-threaded.

| output            | threads | ssb-dbgen | populate-gen-ssb |
|-------------------|--------:|----------:|-------------:|
| `.tbl`            | 1       | 8.2 s     | 1.41 s       |
| `.tbl`            | 4       | —         | 0.61 s       |
| Parquet (snappy)  | 4       | —         | 0.54 s       |
| Arrow only        | 4       | —         | 0.20 s       |
