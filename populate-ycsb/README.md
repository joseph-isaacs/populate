# populate-ycsb

The YCSB load and the YCSB run (its request stream), byte-identical to
YCSB 0.17.0, on all cores, as Arrow `RecordBatch`es, Parquet, or TSV, on
`populate-table`'s `Chunks` engine.

```text
cargo run --release -p populate-ycsb --bin ycsb-gen -- load --records 10000000 --out ycsb
cargo run --release -p populate-ycsb --bin ycsb-gen -- run --records 10000000 \
    --operations 10000000 --workload a --out ycsb-run
```

| | Tables | Reference | How it is tested |
|---|---|---|---|
| YCSB load | `usertable` of `ycsb load` (`CoreWorkload`) | YCSB 0.17.0 | byte-identical to YCSB's own output |
| YCSB run | the operations of `ycsb run`, workloads A to F or any proportions | YCSB 0.17.0 | byte-identical to YCSB's own output |

Output is identical for any thread count and chunk (row group) size: every
random value is a function of its row, so each chunk is generated on its own.

## YCSB load

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
with odd chunk and batch sizes. A wider matrix run by hand (default and
`dataintegrity` loads of 20,000; 1, 3, 20 and 50 fields; field lengths 1, 5,
6, 7, 10, 13 and 1,000; prefixes, padding, ordered keys, slices) was compared
whole with `cmp`: all identical.

Not supported: `fieldlengthdistribution` other than `constant` (they draw a
variable number of values per record, so records could not be generated
independently), and multi-threaded YCSB loads (each YCSB thread interleaves
its own key range; generate one `Ycsb` per thread with
`insert_start`/`insert_count` and that thread's seed instead).

## YCSB run

`YcsbRun::new(load, operationcount)` or `YcsbRun::workload('a'..='f', load,
operationcount)` (the proportions and distributions of YCSB's
`workloads/workloada` ... `workloadf`), with `proportions` (read, update,
insert, scan, read-modify-write), `distribution` (`Uniform`, `Zipfian`,
`Latest`, `Hotspot`), `read_all_fields`, `write_all_fields` and
`scan_length(min, max, zipfian)`. Each row is one call YCSB makes into the
database binding: `op` (`READ`, `UPDATE`, `INSERT`, `SCAN`; a
read-modify-write is a `READ` row then an `UPDATE` row), `ycsb_key`,
`scan_length`, `read_fields` (comma-separated; NULL for all fields) and the
values written (NULL where not written). Text output is that, tab-separated.

Every choice draws a fixed number of values from the client thread's
`ThreadLocalRandom`: the operation one `nextDouble` (two draws on OpenJDK
21, which keeps `Random`'s `next(26) << 27 | next(27)`), a uniform key one
`nextLong`, a zipfian key one `nextDouble` (YCSB's scrambled zipfian over
10^10 items, then FNV), `latest` one `nextDouble` (a zipfian back from the
newest key, whose zeta grows with the inserts), `hotspot` a `nextDouble`
and a `nextLong`, and values as in the load. Only rejected keys (past the
last insert) and inserts make an operation depend on the ones before it, so
a quick pass that only advances the generators finds each chunk's start
state (draw position, next insert, `latest`'s zeta), one chunk at a time by
whichever thread first needs it, and chunks are generated in parallel. The
zipfian's `pow(x, 1 / (1 - 0.99))` is computed as `x^100` by squaring, and
only falls back to `pow` when the product is within 1e-12 of an integer.

**Tested byte for byte against YCSB 0.17.0's `ycsb run`** (`YCSB_MODE=-t
ycsb-ref/run.sh OUT -p refdb.run=true ...`, where `RefDB` writes each call
it receives): `tests/ycsb_run.rs` holds a fixture and the FNV hashes of 22
runs (workloads A to F; uniform, zipfian, latest and hotspot keys, with and
without inserts; uniform and zipfian scan lengths; one field read, all
fields written; `dataintegrity`; other field counts, lengths and prefixes;
ordered keys with padding; `insertstart` / `insertcount`), each on 1 and 4
threads with odd chunk and batch sizes; 1M-operation runs of
workload A, latest with 50% inserts, workload E and zipfian scans of up to
100,000 records were also compared whole with `cmp`.

Not supported: the `exponential` and `sequential` request distributions,
and `fieldlengthdistribution` other than `constant`. Java's `Math.pow` and
glibc's `pow` differ by an ulp on a few inputs (e.g. `pow(2.0 / 888, 0.01)`,
in zipfian setup); no run tested diverged (2e8 zipfian draws checked), but
one could in principle.

## Speed

4 cores, `--release`, best of 5, output to tmpfs (see also `cargo bench -p
populate-ycsb`):

| Workload | Generate (Arrow) | Parquet (snappy) | TSV |
|---|---|---|---|
| YCSB load, 1M records (1 GiB) | 0.14 s | 0.55 s, 1,010 MiB | 0.37 s, 986 MiB |
| YCSB run, workload A, 2M operations | 0.12 s | 0.25 s, 131 MiB | 0.11 s, 175 MiB |

For comparison, on the same machine: YCSB 0.17.0's own load of 1M records
(one client thread, the `ycsb-ref` binding writing TSV) takes 3.4 s, and
its run of those 2M workload A operations 2.0 s. `ycsb-gen` uses mimalloc
(feature `mimalloc`, on by default; the library does not).

YCSB text is written straight from the generators (no Arrow arrays), each
random 6-byte group built in one word from YCSB's int; Parquet uses
writer properties (`Ycsb::parquet_props`, `YcsbRun::parquet_props`) that
skip dictionaries for columns of distinct values. Most of the Parquet time
is in populate-table's encoder.
