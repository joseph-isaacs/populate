# populate-oltp

The TPC-C initial database and the YCSB load, on all cores, as Arrow
`RecordBatch`es, Parquet, or text, on `populate-table`'s `Chunks` engine.

```text
cargo run --release -p populate-oltp --bin oltp-gen -- tpcc --warehouses 100 --out tpcc
cargo run --release -p populate-oltp --bin oltp-gen -- ycsb --records 10000000 --out ycsb
```

| | Tables | Reference | How it is tested |
|---|---|---|---|
| TPC-C | all 9, any number of warehouses | TPC-C 5.11 clause 4.3.3.1 (no reference bytes exist) | row counts, keys, ranges and distributions against the spec |
| YCSB | `usertable` of `ycsb load` (`CoreWorkload`) | YCSB 0.17.0 | byte-identical to YCSB's own output |

Output is identical for any thread count and chunk (row group) size: every
random value is a function of its row, so each chunk is generated on its own.

## TPC-C

`Tpcc::new(w)`: `warehouse` (w rows), `district` (10w), `customer` and
`history` (30,000w), `orders` (30,000w), `new_order` (9,000w), `order_line`
(5 to 15 per order, ~300,000w), `item` (100,000), `stock` (100,000w).
Columns and types follow the spec (`Int32` ids, `Decimal128` money with the
spec's precision, `Timestamp(µs)` dates, `Utf8View` text). Text output is
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

## YCSB

`Ycsb::new(recordcount)` with YCSB's properties as builder methods:
`fieldcount`, `fieldlength` (constant), `fieldnameprefix`, `insertorder`,
`zeropadding`, `insertstart` / `insertcount`, `dataintegrity`. The schema is
`ycsb_key` then the fields, all `Utf8View`; text output is TSV
(`key\tfield0\t...\n`, fields in name order).

- Keys are YCSB's `buildKeyName`: `user` + `Utils.fnvhash64` of the insert
  sequence number (taken as an `int`, as YCSB does), zero-padded.
- `dataintegrity=true` values are `buildDeterministicValue`: `key:field`
  then `:` + `String.hashCode` of the string so far, until the field length.
- Random values: YCSB fills them from the client thread's
  `ThreadLocalRandom`, which it never seeds. That generator is a counter
  (`seed + n * gamma`, `gamma` = golden gamma + 2 × thread id, on OpenJDK
  17+), and with a constant field length every record draws a fixed number
  of ints (one per field when `buildValues` constructs its
  `RandomByteIterator`s, then `ceil(fieldlength / 6)` per field read, in the
  `HashMap`'s iteration order). So any record's values are computed
  directly, and they are the bytes YCSB writes when its loading thread starts
  from seed `--java-seed` (`JavaTlr`).

**Tested byte for byte against YCSB 0.17.0.** `ycsb-ref/run.sh` builds YCSB's
core from its Maven Central sources and runs its real `Client -load` with a
binding (`RefDB.java`) that writes each insert as TSV, in the order the
binding receives the values, after setting the client thread's random seed
by reflection. `tests/ycsb.rs` holds fixtures and FNV hashes of its output
for: 100,000 default records with random values; 20,000 with
`dataintegrity=true`; 20 fields of 37 bytes with another prefix;
`insertstart` with and without `insertcount`, with `insertorder=ordered` and
`zeropadding=12`; and short deterministic fields; each on 1 and 4 threads
with odd chunk and batch sizes. Before merging, a wider matrix (default and
`dataintegrity` loads of 20,000; 1, 3, 20 and 50 fields; field lengths 1, 5,
6, 7, 10, 13 and 1,000; prefixes, padding, ordered keys, slices) was compared
whole with `cmp`: all identical.

Not supported: `fieldlengthdistribution` other than `constant` (they draw a
variable number of values per record, so records could not be generated
independently), and multi-threaded YCSB loads (each YCSB thread interleaves
its own key range; generate one `Ycsb` per thread with
`insert_start`/`insert_count` and that thread's seed instead).

## Speed

4 cores, `--release`, best of 5, output to tmpfs (see also `cargo bench -p
populate-oltp`):

| Workload | Generate (Arrow) | Parquet (snappy) | Text |
|---|---|---|---|
| TPC-C, 10 warehouses (5.1M rows) | 0.20 s | 0.59 s, 556 MiB | 0.37 s, 685 MiB CSV |
| YCSB, 1M records (1 GiB) | 0.19 s | 0.69 s, 1,010 MiB | 0.53 s, 986 MiB TSV |

For comparison, on the same machine: `go-tpc tpcc prepare --warehouses 10
--threads 4 --output-type csv` takes 3.6 s, and YCSB 0.17.0's own load of 1M
records (one client thread, the `ycsb-ref` binding writing TSV) 3.4 s.

YCSB text is written straight from the generators (no Arrow arrays), each
random 6-byte group built in one word from YCSB's int; Parquet uses
per-table writer properties (`tpcc::parquet_props`, `Ycsb::parquet_props`)
that skip dictionaries for columns of distinct values. Most of the Parquet
time is in populate-table's encoder.
