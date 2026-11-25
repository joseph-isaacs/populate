# populate

[![CI](https://github.com/joseph-isaacs/data-comb/actions/workflows/ci.yml/badge.svg)](https://github.com/joseph-isaacs/data-comb/actions/workflows/ci.yml)
[![Rust 1.92+](https://img.shields.io/badge/rust-1.92%2B-blue)](https://www.rust-lang.org/tools/install)

Composable distribution combinators for generating tabular test data,
column by column, straight into Arrow (and optionally Parquet).

## Install and use

The minimum supported Rust version (MSRV) is **1.92**. Add the library to
an application's `Cargo.toml`:

```toml
[dependencies]
populate = { git = "https://github.com/joseph-isaacs/data-comb.git", features = ["parquet"] }
rand = "0.9"
```

Arrow and table generation are enabled by default. The `parquet` feature
also enables Parquet output. Generate a batch in `src/main.rs`:

```rust
use populate::arrow::DistributionArrow;
use populate::dist::{list, map, range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // id: i64, tags: list<i64> with lengths cycling 0..8
    let mut table = (
        range(0i64, 1),
        list(map(range(0usize, 1), |i| i % 8), range(0i64, 1)),
    );
    let batch = table.collect_record_batch(&mut rand::rng(), 1_000)?;
    assert_eq!(batch.num_rows(), 1_000);
    Ok(())
}
```

Every numeric column, including list offsets and values, is generated into
one exactly-sized buffer that becomes the Arrow array without a copy. The optional `parquet` feature writes those buffers to Parquet
in place (`collect_parquet`).

For dependent columns, see the [realistic orders example](populate/examples/realistic.rs):
shipping and receipt dates, a late flag, and countries selected by region.
Run it from a checkout:

```sh
cargo run -p populate --example realistic
```

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
// with the `parquet` feature:
sales.write_parquet(std::fs::File::create("sales.parquet")?, 64 * 1024, 0, None)?;
```

Any generator that can produce its rows in independent chunks can use the
same engine: implement `populate::table::Chunks` (schema, chunk count,
"generate chunk `i`") and `chunks::for_each_batch` / `write_parquet` run it
on all cores, Parquet row groups encoded on the workers.

## Benchmark generators

Each generator is a crate with its own CLI, built on the `Chunks` engine:
Arrow, Parquet or the reference tool's own text format, on all cores, with
output that does not depend on the thread count. Each crate's README covers
its flags, how it was verified and its performance.

| benchmark | crate | CLI | matches |
|---|---|---|---|
| TPC-H | [`populate-tpch`](generator/tpch/README.md) | `tpch-gen` | `dbgen` / `tpchgen-cli`, byte for byte |
| TPC-DS | [`populate-tpcds`](generator/tpcds/README.md) | `tpcds-gen` | `dsdgen`, byte for byte |
| DSB | [`populate-dsb`](generator/dsb/README.md) | `dsb-gen` | DSB's `dsdgen`, byte for byte |
| SSB (and SSB-flat) | [`populate-ssb`](generator/ssb/README.md) | `ssb-gen` | `ssb-dbgen`, byte for byte |
| H2O db-benchmark | [`populate-h2o`](generator/h2o/README.md) | `h2o-gen` | its R datagen, byte for byte |
| TSBS devops / IoT | [`populate-tsbs`](generator/tsbs/README.md) | `tsbs-gen` | `tsbs_generate_data`, byte for byte |
| LDBC SNB | [`populate-ldbc`](generator/ldbc/README.md) | `ldbc-gen` | the LDBC Spark datagen |
| YCSB | [`populate-ycsb`](generator/ycsb/README.md) | `ycsb-gen` | YCSB 0.17.0, byte for byte |
| TPC-C | [`populate-tpcc`](generator/tpcc/README.md) | `tpcc-gen` | the spec's population rules |
| synthetic shapes | [`populate-synth`](generator/synth/README.md) | `synth-gen` | (no reference) |

Install the TPC-H command from Git:

```sh
cargo install --git https://github.com/joseph-isaacs/data-comb.git populate-tpch
```

The installed executable is `tpch-gen`:

```sh
tpch-gen --sf 1 --out tpch --format parquet
```

From a checkout, run the same generator with Cargo:

```sh
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch              # Parquet
cargo run -p populate-tpch --release --bin tpch-gen -- --sf 1 --out tpch --format tbl # dbgen text
```

TPC-H, TPC-DS and TPC-C are trademarks of the Transaction Processing
Performance Council. Data generated here is not valid for TPC result
publication.

## Crates

The core crates live at the top level; the output sinks are under `sink/`
and the data-set generators under `generator/`.

| crate                   | contents                                            |
|-------------------------|-----------------------------------------------------|
| `populate`              | facade: re-exports everything below                 |
| `populate-core`         | `Distribution`, `Columnar`, `Sink`, `StrCol`, `ListCol`, tuples |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `with_column`, `uniform`, `choose`, `weighted`, `text`, `chars` |
| `populate-sink-arrow`   | Arrow arrays and `RecordBatch` output               |
| `populate-sink-parquet` | zero-copy Parquet writer (`parquet` feature)        |
| `populate-table`        | `Table`: named, typed, any-width tables; `Chunks`: parallel Arrow/Parquet output for any chunked generator |
| `populate-bench`        | [benchmarks](populate-bench/README.md) against tpchgen and arrow-rs |

The generator crates are listed [above](#benchmark-generators). See
[`populate/DESIGN.md`](populate/DESIGN.md) for the design.

## Licence

MIT (see [`LICENSE`](LICENSE)). `populate-dsb` vendors code under
Apache-2.0 and MIT; see [`generator/dsb/NOTICE.md`](generator/dsb/NOTICE.md).
