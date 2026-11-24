# populate

Composable distribution combinators for generating tabular test data,
column by column, straight into Arrow (and optionally Parquet).

```rust
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{constant, list, map, range};

// id: i64, tags: list<i64> with lengths cycling 0..8
let mut table = (
    range(0i64, 1),
    list(map(range(0usize, 1), |i| i % 8), range(0i64, 1)),
);
let batch = DistributionArrow::<ArrowBackend>::collect_record_batch(&mut table, &mut rand::rng(), 1_000)?;
```

With `ArrowBackend` every numeric column, including list offsets and values,
is generated into one exactly-sized buffer that becomes the Arrow array
without a copy. The optional `parquet` feature writes those buffers to Parquet
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

## TSBS

`populate-tsbs` generates TSBS's devops and IoT data byte-identical to
`tsbs_generate_data` for any seed and flags (Go's `math/rand`, its amd64
`math.Exp`/`math.Log` and the per-host simulation, ported), as influx,
timescaledb, cratedb, cassandra and the other text formats, or as Arrow and
Parquet, one table per measurement, on all cores.

```sh
cargo run -p populate-tsbs --release --bin tsbs-gen -- --use-case devops --scale 100 --seed 123 --format influx > devops.influx
cargo run -p populate-tsbs --release --bin tsbs-gen -- --use-case iot --scale 1000 --seed 123 --format parquet --out tsbs
```

See [`populate-tsbs/README.md`](populate-tsbs/README.md).

## Crates

| crate                   | contents                                            |
|-------------------------|-----------------------------------------------------|
| `populate`              | facade: re-exports everything below                 |
| `populate-core`         | `Distribution`, `SampleIter`, `Sink`, `Backend`, HLists, tuples |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `with_column`, `flatten`, `uniform`, `choose`, `weighted`, `text`, `chars` |
| `populate-sink-arrow`   | `ArrowBackend`, `RecordBatch` output                |
| `populate-sink-parquet` | zero-copy Parquet writer (`parquet` feature)        |
| `populate-table`        | `Table`: named, typed, any-width tables; `Chunks`: parallel Arrow/Parquet output for any chunked generator |
| `populate-tpcds`        | TPC-DS byte-identical to dsdgen, on all cores; the `tpcds-gen` CLI |
| `populate-tpch`         | TPC-H byte-identical to dbgen, on all cores; the `tpch-gen` CLI |
| `populate-tsbs`         | TSBS devops/IoT byte-identical to `tsbs_generate_data`, on all cores; the `tsbs-gen` CLI |

See `populate/DESIGN.md` for the design.
