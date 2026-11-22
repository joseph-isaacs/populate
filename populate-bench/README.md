# populate vs tpchgen

Single-thread generation throughput of populate (`ArrowBackend`, zero-copy
Arrow and Parquet output) against [tpchgen](https://crates.io/crates/tpchgen)
4.0 / tpchgen-arrow 4.0, on TPC-H `lineitem`-shaped data.

```bash
cargo bench -p populate-bench            # divan: time, rows/s, bytes/s, allocs, max live heap
cargo run -p populate-bench --release    # Markdown table incl. peak heap (args: [rows] [repeats])
cargo test -p populate-bench             # sanity checks: shapes/sizes match on both sides
```

The crate is not in the workspace `default-members` because tpchgen 4 pulls in
arrow/parquet 60 next to populate's arrow 57.

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

**lineitem, 11 numeric columns -> Arrow** (1,048,576 rows, 72 MiB)

| workload | median | rows/s | MiB/s | peak heap |
|---|---:|---:|---:|---:|
| populate, one batch | 58.0 ms | 18.1 M | 1240 | 75.5 MB |
| populate, 8192-row batches | 30.8 ms | 34.0 M | 2340 | 0.6 MB |
| tpchgen rows -> Arrow, 8192-row batches | 96.8 ms | 10.8 M | 744 | 1.8 MB |
| tpchgen rows only, no output (floor) | 75.8 ms | 13.8 M | – | 0 |

**lineitem, 11 numeric columns -> Parquet** (72 MiB file)

| workload | median | rows/s | MiB/s | peak heap |
|---|---:|---:|---:|---:|
| populate, zero-copy writer | 55.0 ms | 19.1 M | 1310 | 76.0 MB |
| populate, RecordBatch -> ArrowWriter | 98.7 ms | 10.6 M | 729 | 160.2 MB |
| tpchgen -> ArrowWriter | 127.3 ms | 8.2 M | 566 | 78.3 MB |

**orders with list\<line price\> -> Arrow** (262k orders, ~1M values, 11 MiB)

| workload | median | orders/s | MiB/s | peak heap |
|---|---:|---:|---:|---:|
| populate | 4.4 ms | 59.6 M | 2500 | 11.5 MB |
| tpchgen rows grouped by orderkey | 72.2 ms | 3.6 M | 152 | 11.5 MB |

**orders with list\<line price\> -> Parquet** (10.1 MiB file)

| workload | median | orders/s | MiB/s | peak heap |
|---|---:|---:|---:|---:|
| populate, zero-copy writer | 16.6 ms | 15.8 M | 608 | 13.8 MB |
| tpchgen grouped -> ArrowWriter | 91.2 ms | 2.9 M | 111 | 33.7 MB |

**tpchgen full lineitem** (16 columns, strings included)

| workload | median | rows/s | MiB/s | peak heap |
|---|---:|---:|---:|---:|
| tpchgen-arrow `LineItemArrow` -> Arrow | 221.8 ms | 4.7 M | 1030 (228 MiB) | 3.1 MB |
| tpchgen-arrow `LineItemArrow` -> ArrowWriter | 418.6 ms | 2.5 M | 327 (137 MiB file) | 147.3 MB |

## Reading the numbers

- **Numeric columns: about 3x faster to Arrow and 2.3x faster to Parquet.**
  tpchgen's row generator is the limit on its side: just producing the rows
  (75.8 ms) takes longer than populate takes to make the Arrow batches.
  tpchgen also builds every column, strings included, even when you only
  need the numeric ones.
- **Lists: about 16x faster to Arrow and 5.5x faster to Parquet.** populate
  writes offsets and values straight into the two final buffers. Parquet
  then spends most of its time on the def/rep levels and encoding.
- **One big batch is slower than streamed batches** (58 ms vs 31 ms). The
  generation work is the same, so the gap is most likely page faults on the
  first touch of ~72 MB of freshly mapped memory each run (not profiled).
  Streamed 8192-row batches keep reusing the same warm memory.
- **The zero-copy Parquet writer is ~1.8x faster than `ArrowWriter` on the same
  populate data and peaks at half the memory** (76 MB, which is the
  generated data itself, vs 160 MB). `collect_parquet` still holds all `n`
  rows before writing. A streaming version that generates one row group at a
  time would cut the peak to about one row group.
- **Not like-for-like:** tpchgen produces spec-correct correlated values
  (for example `extendedprice` derived from the part price, dates derived from
  the order date). These populate distributions are independent uniform
  draws over the same ranges. The comparison is generation-engine throughput
  for the same output shape, not a TPC-H-compliant populate generator.
