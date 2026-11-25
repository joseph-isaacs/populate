# populate

Composable distribution combinators for generating tabular test data,
column by column, straight into Arrow (and optionally Parquet).

```rust
use populate::arrow::DistributionArrow;
use populate::dist::{constant, list, map, range};

// id: i64, tags: list<i64> with lengths cycling 0..8
let mut table = (
    range(0i64, 1),
    list(map(range(0usize, 1), |i| i % 8), range(0i64, 1)),
);
let batch = table.collect_record_batch(&mut rand::rng(), 1_000)?;
```

Every numeric column, including list offsets and values, is generated into
one exactly-sized buffer that becomes the Arrow array without a copy. The optional `parquet` feature writes those buffers to Parquet
in place (`collect_parquet`).

## Tables

For real schemas, declare a `Table`: named, typed columns of any width, each
a distribution, a function of the row number, or derived from earlier
columns. Batches are seeded by index, so they generate on all cores with the
same output as one thread.

```rust
use populate::dist::{choose, uniform};
use populate::table::Table;

let sales = Table::new("sales", 10_000_000)
    .key("id")
    .col("store", choose(&["north", "south", "east"]))
    .col("sold", uniform(10_000i32, 10_365)).date()
    .col("qty", uniform(1i64, 10))
    .col("price", uniform(100i64, 9_999)).decimal(7, 2)
    .derive2("total", ["qty", "price"], |q: i64, p: i64| q * p).decimal(9, 2)
    .col("note", choose(&["", "gift"])).nullable(0.1);

sales.for_each_batch(64 * 1024, 0, |batch| { /* RecordBatch, in order */ });
sales.write_parquet(std::fs::File::create("sales.parquet")?, 64 * 1024, 0, None)?;
```

Any generator that can produce its rows in independent chunks can use the
same engine: implement `populate::table::Chunks` (schema, chunk count,
"generate chunk `i`") and `chunks::for_each_batch` / `write_parquet` run it
on all cores, Parquet row groups encoded on the workers.

## TPC-DS

`populate-tpcds` generates all 24 TPC-DS tables byte-identical to the
reference `dsdgen`, as Arrow, Parquet or dsdgen's own `.dat` text, on all
cores. The values come from `tpcdsgen` (the Apache-2.0 Rust port of dsdgen
from tpchgen-rs); populate-tpcds splits every table into small chunks and
runs them through the `Chunks` engine.

```sh
cargo run -p populate-tpcds --release --bin tpcds-gen -- --sf 1 --out tpcds              # Parquet
cargo run -p populate-tpcds --release --bin tpcds-gen -- --sf 1 --out tpcds --format dat # dsdgen text
```

See [`populate-tpcds/README.md`](populate-tpcds/README.md).

## DSB

`populate-dsb` generates all 24 tables of Microsoft's
[DSB](https://github.com/microsoft/dsb) (TPC-DS with skewed, correlated
data) byte-identical to DSB's `dsdgen`, on all cores. Its generator is
tpcdsgen with DSB's changes to dsdgen ported in, run through the same
`Chunks` engine as populate-tpcds.

```sh
cargo run -p populate-dsb --release --bin dsb-gen -- --sf 1 --out dsb --format dat # DSB dsdgen text
```

See [`populate-dsb/README.md`](populate-dsb/README.md).

## TPC-H

`populate-tpch` generates all 8 TPC-H tables byte-identical to `dbgen` (and
`tpchgen-cli`), as Arrow, Parquet or dbgen's `.tbl` text, on all cores, at
any scale factor up to 10,000. Its generators are its own, column at a
time, with dbgen's random streams skipped straight to any row.

```sh
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch              # Parquet
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch --format tbl # dbgen text
```

See [`populate-tpch/README.md`](populate-tpch/README.md).

## SSB

`populate-ssb` generates the five Star Schema Benchmark tables
byte-identical to `ssb-dbgen`, plus the denormalised `lineorder_flat`, as
Arrow, Parquet or `.tbl` text, on all cores, on TPC-H's random streams.

```sh
cargo run -p populate-ssb --release --bin ssb-gen -- --sf 1 --out ssb              # Parquet
cargo run -p populate-ssb --release --bin ssb-gen -- --sf 1 --out ssb --format tbl # ssb-dbgen text
```

See [`populate-ssb/README.md`](populate-ssb/README.md).

## Crates

| crate                   | contents                                            |
|-------------------------|-----------------------------------------------------|
| `populate`              | facade: re-exports everything below                 |
| `populate-core`         | `Distribution`, `Columnar`, `Sink`, `StrCol`, `ListCol`, tuples |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `with_column`, `uniform`, `choose`, `weighted`, `text`, `chars` |
| `populate-sink-arrow`   | Arrow arrays and `RecordBatch` output               |
| `populate-sink-parquet` | zero-copy Parquet writer (`parquet` feature)        |
| `populate-table`        | `Table`: named, typed, any-width tables; `Chunks`: parallel Arrow/Parquet output for any chunked generator |
| `populate-tpcds`        | TPC-DS byte-identical to dsdgen, on all cores; the `tpcds-gen` CLI |
| `populate-dsb`          | DSB byte-identical to DSB's dsdgen, on all cores; the `dsb-gen` CLI |
| `populate-tpch`         | TPC-H byte-identical to dbgen, on all cores; the `tpch-gen` CLI |
| `populate-ssb`          | SSB byte-identical to ssb-dbgen, plus SSB-flat; the `ssb-gen` CLI |
| `populate-h2o`          | H2O db-benchmark groupby and join data, byte-identical to its R datagen; the `h2o-gen` CLI |
| `populate-tpcc`         | the TPC-C initial database, to the spec's population rules; the `tpcc-gen` CLI |
| `populate-ycsb`         | YCSB load and run operations, byte-identical to YCSB 0.17.0; the `ycsb-gen` CLI |
| `populate-ldbc`         | LDBC SNB, identical to the LDBC Spark datagen; the `ldbc-gen` CLI |
| `populate-synth`        | synthetic data sets (ClickBench-style hits, taxi trips, embeddings, features); the `synth-gen` CLI |

See `populate/DESIGN.md` for the design.

## H2O db-benchmark

`populate-h2o` generates the H2O db-benchmark groupby and join data sets,
byte-identical to the CSV its R datagen scripts write (R's Mersenne
Twister, `sample()`, `round()` and data.table `fwrite` reproduced), as CSV,
Arrow or Parquet, on all cores.

```sh
cargo run -p populate-h2o --release --bin h2o-gen -- groupby --n 1e7 --k 1e2 --nas 0 --sort 0
cargo run -p populate-h2o --release --bin h2o-gen -- join --n 1e7 --format parquet
```

See [`populate-h2o/README.md`](populate-h2o/README.md).
