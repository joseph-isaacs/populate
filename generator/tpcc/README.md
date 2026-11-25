# populate-tpcc

The TPC-C initial database, on all cores, as Arrow `RecordBatch`es,
Parquet, or CSV, on `populate-table`'s `Chunks` engine.

```text
cargo run --release -p populate-tpcc --bin tpcc-gen -- --warehouses 100 --out tpcc
```

| Tables | Reference | How it is tested |
|---|---|---|
| all 9, any number of warehouses | TPC-C 5.11 clause 4.3.3.1 (no reference bytes exist) | row counts, keys, ranges and distributions against the spec |

Output is identical for any thread count and chunk (row group) size: every
random value is a function of its row, so each chunk is generated on its own.

## Tables

`Tpcc::new(w)`: `warehouse` (w rows), `district` (10w), `customer` and
`history` (30,000w), `orders` (30,000w), `new_order` (9,000w), `order_line`
(5 to 15 per order, ~300,000w), `item` (100,000), `stock` (100,000w).
Columns and types follow the spec (`Int32` ids, `Decimal128` money with the
spec's precision, `Timestamp(µs)` dates, `Utf8View` text). Each table is
one populate `Distribution` (`src/tables.rs`): a tuple of named columns,
each a function of its row number, with `order_line` a `flat_map` over
orders. The Arrow schema is read off it (`schema_of`). Text output is
CSV (`,`, `NULL`, no header), the layout `go-tpc tpcc prepare --output-type
csv` writes.

The population rules implemented, by clause:

- random a-strings are letters and digits of uniform length in `[x .. y]`,
  n-strings digits, states two upper-case letters, zips 4 digits + `11111`
  (4.3.2.2, 4.3.2.7);
- `i_data` and `s_data` hold `ORIGINAL` at a random position in 10% of
  rows chosen at random;
- `c_last` is the syllable name of `c_id - 1` for the first 1,000
  customers of a district and of `NURand(255, 0, 999)` for the rest, with
  one `C` per load (`Tpcc::c_load()`; a run must choose `C_run` with
  `|C_run - C_load|` in `65 ..= 119`, not 96 or 112);
- `c_credit` is `BC` for 10% of customers chosen at random;
- `o_c_id` is a random permutation of `1 ..= 3,000` per district; orders
  `2,101 ..= 3,000` have no carrier, `new_order` rows, and order lines with no
  delivery date and a random amount;
- the "current date/time" columns hold one load time (`Tpcc::load_time`,
  default 2026-01-01 00:00:00 UTC), so data is reproducible from the seed.

**Why there is no byte-for-byte reference.** go-tpc, BenchBase and
tpcc-mysql all draw from clock-seeded generators (go-tpc's `rand.go` seeds
its NURand constants from `time.Now()`, and loads through one sequential
stream per worker), so no two runs agree and no fixed seed reproduces them.
Instead `tests/tpcc.rs` checks every table against the spec: exact row counts
(1 and 3 warehouses); unique, complete primary keys; every column's range,
character set and length bounds (every length used); `ORIGINAL` and `BC`
shares within 10% ± 0.5%; `c_last` decoded back to numbers, the first 1,000
exactly and the other 2,000 per district against the exact `NURand(255, 0,
999)` distribution for the load's `C` (chi-squared, and visibly non-uniform);
`o_c_id` a permutation per district; `o_ol_cnt` uniform over 5..15; one
`new_order` row per undelivered order; order lines numbered `1 ..= o_ol_cnt`
for every order; and chunked, multi-threaded output equal to one sequential
batch for every table.

## Speed

4 cores, `--release`, best of 5, output to tmpfs (see also `cargo bench -p
populate-tpcc`):

| Workload | Generate (Arrow) | Parquet (snappy) | CSV |
|---|---|---|---|
| 10 warehouses (5.1M rows) | 0.17 s | 0.52 s, 556 MiB | 0.37 s, 685 MiB |

For comparison, on the same machine: `go-tpc tpcc prepare --warehouses 10
--threads 4 --output-type csv` takes 3.6 s. `tpcc-gen` uses mimalloc
(feature `mimalloc`, on by default; the library does not).

Parquet uses per-table writer properties (`tpcc::parquet_props`) that skip
dictionaries for columns of distinct values. Most of the Parquet time is in
populate-table's encoder.
