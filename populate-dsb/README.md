# populate-dsb

[DSB](https://github.com/microsoft/dsb) data (Microsoft's TPC-DS with skewed
distributions and cross-column correlations) byte-identical to DSB's own
`dsdgen`, on all cores: all 24 tables as Arrow `RecordBatch`es, Parquet, or
dsdgen's `.dat` text.

```sh
# all tables at SF 1 as Parquet in ./dsb (one file per table)
cargo run -p populate-dsb --release --bin dsb-gen -- --sf 1 --out dsb

# DSB dsdgen's .dat files, byte for byte
cargo run -p populate-dsb --release --bin dsb-gen -- --sf 1 --out dsb --format dat
```

Flags as `tpcds-gen` (see [`populate-tpcds`](../populate-tpcds/README.md)):
`--sf` (at least 1: dsdgen crashes below), `--out`,
`--format parquet|dat|none`, `--tables`, `--threads`, `--batch`,
`--row-group`, `--compression snappy|none`, `--no-dictionary`,
`--encoding utf8|latin1`. `.dat` output is UTF-8 by default, as dsdgen
writes it.

From Rust:

```rust
use populate_dsb::{Dsb, Encoding, TableName};

let ds = Dsb::new(1.0);
let sales = ds.table(TableName::StoreSales);
sales.for_each_batch(0, |batch| { /* RecordBatch, in table order */ });
sales.write_dat(std::fs::File::create("store_sales.dat")?, 0, Encoding::Utf8)?;
```

## Where the data comes from

`dsbgen` is tpcdsgen 4.0 (the Apache-2.0 Rust port of TPC-DS dsdgen from
tpchgen-rs) with DSB's changes to dsdgen ported in:

- exponentially skewed ranks (`genrand_exp_rank`) through permutations of
  each value domain, for most dimension columns, sales keys and prices
  (bucketed for prices and dates);
- attribute key maps (dsdgen's `.meta` files): keys grouped by attribute,
  so the sales tables can draw an item category and a customer state
  jointly, and customers' demographics correlate with their state;
- hierarchical keys for the demographics joins and DSB's sales date
  scaling.

Each DSB random stream is the dsdgen stream of the same column, burnt to
the same per-row budget, so each chunk of source rows is generated
independently and the chunks concatenate to dsdgen's sequential output.
Where DSB's draws overrun a budget (variable-length draws, gift orders,
returns), the streams are moved past the earlier rows' overruns from
per-order prefix sums.

## Checked against dsdgen

All 24 `.dat` files `cmp` equal to DSB dsdgen (version 2.11.0, built from
the DSB repository) at SF 1 and SF 2, with any chunk size and thread
count. One caveat: dsdgen's first `item` row reads an uninitialised flag
word, so that row's `i_brand_id` and `i_class_id` change from run to run
with address space randomisation. `populate-dsb` matches dsdgen run with
randomisation off (`setarch -R dsdgen ...`).

See [NOTICE.md](NOTICE.md) for licences.
