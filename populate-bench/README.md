# populate vs tpchgen

Single-thread generation throughput of populate (`ArrowBackend`, zero-copy
Arrow and Parquet output) against [tpchgen](https://crates.io/crates/tpchgen)
4.0 / tpchgen-arrow 4.0, on TPC-H `lineitem`-shaped data.

```bash
cargo bench -p populate-bench            # divan: time, rows/s, bytes/s, allocs, max live heap
cargo run -p populate-bench --release    # Markdown table incl. peak heap (args: [rows] [repeats])
cargo test -p populate-bench             # sanity checks: shapes/sizes match on both sides
cargo run -p populate-bench --release --bin scale -- <tpch-full|tpch-numeric|tpch-direct|tpch-lanes|populate> <sf> [threads]
```

The crate is not in the workspace `default-members`, so root `cargo build` /
`cargo test` skip the tpchgen comparison. (It was added when populate was on
arrow 57 and tpchgen on 60; both are on arrow/parquet 60 now.)

## Summary

After an adversarial review, the honest summary is:

- **Getting these columns through tpchgen's public API: populate is ~3x faster.**
  About 40% of that is tpchgen work the benchmark throws away. tpchgen always
  draws the 4 string columns' random values and builds a `LineItem` struct per
  row, which then gets copied into columns.
- **Like-for-like, populate is ~1.5–2.5x faster.** The baseline here is
  tpchgen's own random generators and row logic for just the numeric columns
  ([`fair::tpch_direct_numeric`](src/fair.rs)). Its output is identical to
  tpchgen's (tested).
- **All of that gap is the random number generator, not populate's engine.**
  populate's side uses xoshiro (`SmallRng`) plus rand's integer range
  reduction. tpchgen must use the TPC-H generator: a multiply, a modulo by
  2^31 - 1 and an f64 divide per value, plus a per-order skip-ahead that makes
  partitions reproducible. Give populate tpchgen's generator
  ([`fair::p2_uniform_tpch_rng`](src/fair.rs)) and it is no faster than
  tpchgen.
- **populate adds a little overhead over a hand-written loop.** It is about 7%
  slower than a hand-written column loop and about 30% slower than a
  hand-written row loop into Arrow builders, using the same random generator.
- **Lists: ~2.7x like-for-like, not 16x.** The 16x compared generating full
  lineitem rows against one column. Generating only the 3 tpchgen streams the
  list needs is far cheaper.
- **What the populate side doesn't do:** tpchgen's values are spec-correct and
  correlated (for example `extendedprice` is derived from the part price, and
  the dates from the order date). populate's side draws independent uniform
  values over the same ranges, with populate's own `uniform` leaf. That leaf
  takes several small-range values from each RNG call (see
  `populate-builtins/src/bounded.rs`), so it makes about half the RNG calls
  of the hand-written baselines, which draw one rand `Uniform` per value.

## Vectorised TPC-H RNG: same bytes, ~1.8x faster

[`lanes::tpch_lanes_numeric`](src/lanes.rs) generates the 11 numeric
lineitem columns **byte-identical to tpchgen** (tested against
`LineItemGenerator`), column by column. Every lineitem stream reserves 7
draws per order, so a value's draw index depends only on (order, line), and
the TPC-H LCG can jump to any index. The draws are split over 16 independent
LCG chains that the compiler vectorises, with the modulo and the bounded-int
mapping done with shifts and adds instead of divides. The integer mapping
equals tpchgen's f64 one for every seed and every range used: checked
exhaustively (`exhaustive_scale_matches_f64`, `--ignored`).

Same VM, `numeric_arrow` group, median of 2 interleaved runs (ms):

| workload | x86-64 (default) | x86-64-v3 (AVX2) |
|---|---:|---:|
| tpchgen rows -> Arrow (public API) | 91.9 | 79.0 |
| tpchgen RNGs + row logic, numeric only | 45.5 | 41.8–67.3 |
| **lane-parallel tpchgen RNG, same output** | **25.2** | **13.8–18.0** |
| populate, xoshiro uniforms (not TPC-H values) | 24.6 | 24.2 |
| hand-written row loop, xoshiro | 29.9 | 27.5 |

So the RNG gap from the summary below can be closed without giving up
spec-correct output: like-for-like it is now ~1.8x at the default target
and ~2.5–3x with AVX2. This is where column-at-a-time generation actually
pays: a row loop has to step each stream serially.

### Full table: SF10 `lineitem`, byte-identical, streamed

The `tpch-lanes` workload of `scale` generates the whole SF10 table this
way. Orders are cut into fixed chunks (65,536 orders, ~262k rows) dealt out
to the threads; each chunk starts with `TpchLanes::at_order`, which jumps
every stream ahead (`16807^k`) to the chunk's first draw. With
`SCALE_CHECK=1`, `tpch-lanes` and `tpch-numeric` print an order-independent
per-row checksum. Both give `5818b09aeaf9e04d` at SF10 on 1 and 4 threads,
with 59,986,052 rows, so the full table matches tpchgen exactly.

Same VM, best of 3 interleaved runs (s):

| workload | 1 thread | 4 threads |
|---|---:|---:|
| tpchgen rows -> numeric cols (`tpch-numeric`) | 3.95 | 1.10 |
| tpchgen RNGs + row logic (`tpch-direct`) | 2.10 | n/a |
| **lane-parallel tpchgen RNG (`tpch-lanes`)** | **1.58** | **0.54** |
| populate, xoshiro uniforms (not TPC-H values) | 0.72 | 0.20 |

Not done yet: the 4 string streams (their draw counts vary per row, so they
need a prefix pass first), and it is a bench-crate prototype hard-wired to
SF 10, not a populate API.

## Workloads

All workloads live in `src/lib.rs`; each builds ~1M rows (`1 << 20`) from scratch.

- **numeric**: the 11 non-string `lineitem` columns (7 x `i64`, 4 x `i32`;
  decimals as `i64` cents, dates as `i32` days). populate draws them from
  uniform distributions over the TPC-H value ranges. tpchgen generates its
  rows and the columns are copied into exactly-sized `Vec`s and moved into
  Arrow, in 8192-row batches (tpchgen-arrow's own default is 8000).
- **list**: `(orderkey, list<extendedprice>)` with 1..=7 lines per order
  (mean 4), so ~262k orders and ~1M list values. On the tpchgen side the
  lineitem rows are grouped by orderkey into a `ListArray`, since tpchgen has
  no nested output.
- **tpchgen full**: `LineItemArrow` with all 16 columns, for reference. This is
  what `tpchgen-cli` does. populate has no string columns yet, so there's no
  populate counterpart.

Parquet is written to a byte-counting `Write` (no disk I/O), uncompressed and
without dictionary encoding on both sides. Both sides then write the same
plain-encoded data, so the files come out within 0.1% of each other in size.
populate uses its zero-copy `collect_parquet` writer, with the generic
`RecordBatch -> ArrowWriter` path shown for comparison. tpchgen uses
`ArrowWriter` from parquet 60.

tpchgen builds a 300 MiB text pool once per process (~1 s). Both harnesses
build it before timing anything.

## Results

Intel Xeon @ 2.10 GHz (4 vCPU cloud VM), rustc 1.97, `cargo bench` (divan,
10 samples, median). "Peak" is divan's max live heap during a sample. The VM
is noisy: the same benchmark moves by ±20–30% between runs. Treat the ratios
as the result, not the absolute numbers.

**lineitem, 11 numeric columns -> Arrow** (1,048,576 rows, 72 MiB, 8192-row
batches unless noted)

| workload | median | rows/s |
|---|---:|---:|
| hand-written row loop -> Arrow builders, no populate | 19.6 ms | 53.5 M |
| hand-written column loop -> `Vec` -> Arrow, no populate | 24.0 ms | 43.8 M |
| **populate** | 25.6 ms | 40.9 M |
| populate, one 1M-row batch | 59.4 ms | 17.7 M |
| populate, every draw from tpchgen's RNG | 59.8 ms | 17.5 M |
| populate, tpchgen's RNG as lane-parallel leaves (same values) | see ¹ | |
| **tpchgen RNGs + row logic, numeric columns only** (same output as below) | 63.7 ms | 16.5 M |
| tpchgen RNGs + row logic, plus the 4 string streams (values discarded) | 82.3 ms | 12.7 M |
| tpchgen rows only, no output | 105.6 ms | 9.9 M |
| **tpchgen rows -> `Vec` -> Arrow** (as tpchgen-arrow does it) | 98.7 ms | 10.6 M |

¹ Measured later, on a separate run of the same machine: lane-parallel
leaves 20.8 ms, against 40.3 ms for the serial leaves and 71.1 ms for
tpchgen (`cargo bench -- numeric_arrow`). `LaneLcg` in `src/fair.rs` runs
16 chains stepping by `16807^16`, and is tested value-for-value against the
serial leaf. Batches after the first also reuse the first one's schema
(`collect_record_batch_with`), which brings populate down to the
hand-written loop's allocation count (4,376 against 7,424 before).

"tpchgen rows only" is not a floor: it black-boxes a ~200-byte `LineItem` by
value per row, and comes out no faster than the full path.

**lineitem, 11 numeric columns -> Parquet** (72 MiB file)

| workload | median | rows/s | peak heap |
|---|---:|---:|---:|
| populate, zero-copy writer | 63.2 ms | 16.6 M | 76.0 MB |
| populate, RecordBatch -> ArrowWriter | 108.5 ms | 9.7 M | 160.2 MB |
| tpchgen -> ArrowWriter | 139.6 ms | 7.5 M | 78.3 MB |

**orders with list\<line price\> -> Arrow** (262k orders, ~1M values, 11 MiB)

| workload | median | orders/s |
|---|---:|---:|
| populate, independent uniform prices | 5.6 ms | 47.1 M |
| **populate, `price(partkey) * quantity`** | 6.2 ms | 42.2 M |
| **tpchgen, only the 3 RNG streams needed** (same output as below) | 16.6 ms | 15.8 M |
| tpchgen full rows grouped by orderkey | 75.9 ms | 3.5 M |

**orders with list\<line price\> -> Parquet** (10.1 MiB file)

| workload | median | orders/s | peak heap |
|---|---:|---:|---:|
| populate, zero-copy writer | 25.2 ms | 10.4 M | 13.8 MB |
| tpchgen full rows grouped -> ArrowWriter | 93.1 ms | 2.8 M | 33.7 MB |

**tpchgen full lineitem** (16 columns, strings included)

| workload | median | rows/s | peak heap |
|---|---:|---:|---:|
| tpchgen-arrow `LineItemArrow` -> Arrow | 219.0 ms | 4.8 M | 3.1 MB |
| tpchgen-arrow `LineItemArrow` -> ArrowWriter | 440.0 ms | 2.4 M | 147.3 MB |

## Full scale: SF10 and SF100, streamed

`src/bin/scale.rs` generates the whole `lineitem` table as 8192-row Arrow
batches and drops each batch once it's made, so memory stays flat at any
scale factor. With several threads, each thread generates one partition.
tpchgen uses its `part`/`part_count` support, which skips its RNGs ahead to
the partition's first row. populate cuts the rows into fixed 2^18-row
chunks, each with an RNG seeded by its index, and deals them out to the
threads, so its output is the same at any thread count (`SCALE_CHECK=1`
prints a checksum: SF1 gives the same one on 1, 2 and 4 threads). populate generates exactly as many rows as tpchgen. tpchgen's
partitioned runs return exactly the spec row counts (59,986,052 at SF10,
600,037,902 at SF100 and 5,999,989,709 at SF1000).

Same VM (4 vCPU, 15 GiB). Each row below is one process, run once. "Peak RSS"
is the process's `VmHWM`, which for tpchgen includes its 300 MiB text pool.
Building the pool (~1.1 s) is excluded from the time.

| workload | SF | threads | rows | time | rows/s | output GiB | MiB/s | peak RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| populate (11 numeric cols) | 10 | 1 | 59,986,052 | 1.58 s | 37.9 M | 4.03 | 2605 | 3 |
| tpchgen -> 11 numeric cols | 10 | 1 | 59,986,052 | 4.78 s | 12.6 M | 4.03 | 864 | 304 |
| tpchgen-arrow full lineitem | 10 | 1 | 59,986,052 | 14.74 s | 4.1 M | 12.75 | 886 | 305 |
| populate (11 numeric cols) | 10 | 4 | 59,986,052 | 0.49 s | 121.3 M | 4.03 | 8346 | 5 |
| tpchgen -> 11 numeric cols | 10 | 4 | 59,986,052 | 1.21 s | 49.4 M | 4.03 | 3397 | 309 |
| tpchgen-arrow full lineitem | 10 | 4 | 59,986,052 | 3.79 s | 15.8 M | 12.75 | 3442 | 314 |
| populate (11 numeric cols) | 100 | 4 | 600,037,902 | 4.24 s | 141.7 M | 40.31 | 9744 | 5 |
| tpchgen -> 11 numeric cols | 100 | 4 | 600,037,902 | 12.33 s | 48.7 M | 40.31 | 3347 | 310 |
| tpchgen-arrow full lineitem | 100 | 4 | 600,037,902 | 40.93 s | 14.7 M | 127.52 | 3191 | 314 |
| populate (11 numeric cols) | 1000 | 4 | 5,999,989,709 | 41.62 s | 144.2 M | 403.05 | 9917 | 5 |
| tpchgen -> 11 numeric cols | 1000 | 4 | 5,999,989,709 | 127.45 s | 47.1 M | 403.05 | 3238 | 310 |
| tpchgen-arrow full lineitem | 1000 | 4 | 5,999,989,709 | 426.62 s | 14.1 M | 1275.15 | 3061 | 314 |

The populate rows in this table were recorded before a bench bug was fixed.
`collect_into` restarts a `range` on every call, so each 8192-row batch
restarted the orderkeys at the partition's first key. Performance is
unaffected, but the keys repeated. Batches now rebuild the distribution at the
next key (`p2_numeric_batches`, tested).

A rerun of SF10 on one thread after the fix, 3 interleaved runs, was on a
slower day for the VM:

| workload | time | rows/s |
|---|---:|---:|
| populate | 2.11–2.35 s | 25.5–28.4 M |
| tpchgen RNGs + row logic, numeric only (`tpch-direct`) | 3.21–3.85 s | 15.6–18.7 M |
| tpchgen rows -> numeric cols (`tpch-numeric`) | 5.63–6.41 s | 9.4–10.6 M |

That is ~2.7x against the tpchgen public-API path and ~1.6x like-for-like,
matching the 1M-row results. Both sides scale almost linearly to 4 threads.
Peak memory doesn't depend on SF for either: about one batch per thread, plus
tpchgen's text pool.

## Other observations

- **One big batch is slower than streamed batches** (59 ms vs 26 ms). The
  generation work is the same, so the gap is most likely page faults on the
  first touch of ~72 MB of freshly mapped memory each run (not profiled).
- **The zero-copy Parquet writer is ~1.7x faster than `ArrowWriter` on the same
  populate data, and peaks at half the memory** (76 MB, which is the generated
  data itself, vs 160 MB). `collect_parquet` still holds all `n` rows and
  writes a single row group. A streaming version that generates one row group
  at a time would cut the peak to about one row group.
- **Parquet versions differed** when these numbers were taken: populate
  wrote with parquet 57 and tpchgen with parquet 60 (both are on 60 now). The settings match (uncompressed, no dictionary, default
  page and row-group sizes).
- **Expressiveness gaps found while building fair baselines:**
  - A flat table can't share per-order state across lines.
  - `list` elements can't see their parent's values.
  - `map` output is limited to flat tuples of up to 4, and falls back to the
    row-at-a-time path.
  - Leaves restart on every `collect_into`, so streaming in batches needs
    care.
