# populate-bench

Benchmarks for populate's column engine and Parquet writers, mostly against
[tpchgen](https://crates.io/crates/tpchgen) 4.0 / tpchgen-arrow 4.0 on TPC-H
`lineitem`-shaped data.

```bash
cargo bench -p populate-bench            # divan: time, rows/s, bytes/s, allocs, max live heap
cargo run -p populate-bench --release    # Markdown table incl. peak heap (args: [rows] [repeats])
cargo test -p populate-bench             # sanity checks: shapes/sizes match on both sides
cargo run -p populate-bench --release --bin scale -- <tpch-full|tpch-numeric|tpch-direct|tpch-lanes|populate> <sf> [threads]
cargo run -p populate-bench --release --bin direct_parquet -- --rows 1000000,4000000,16000000 --threads 1,2,4 --reps 3 [--plain] [--snappy]
```

The crate is not in the workspace `default-members` (it pulls in
benchmark-only dependencies), so root `cargo build` / `cargo test` skip it.

## What is here

- `src/lib.rs`: the workloads, each about 1M rows built from scratch.
  **numeric** is the 11 non-string `lineitem` columns, **list** is
  `(orderkey, list<extendedprice>)` with 1 to 7 lines per order, and
  **tpchgen full** is `LineItemArrow` with all 16 columns, for reference.
  Parquet goes to a byte-counting writer, uncompressed and without
  dictionaries on both sides.
- `src/fair.rs`: like-for-like baselines. tpchgen's own RNG streams and row
  logic written straight into columns, and the populate workloads with every
  draw taken from tpchgen's RNG, to separate engine cost from RNG cost.
- `src/lanes.rs`: the 11 numeric `lineitem` columns byte-identical to
  tpchgen, with the TPC-H LCG split over 16 independent chains that the
  compiler vectorises.
- `src/bin/scale.rs`: whole `lineitem` tables at any scale factor, streamed
  in 8192-row batches on any number of threads. `SCALE_CHECK=1` prints an
  order-independent checksum.
- `src/bin/direct_parquet.rs`: a distribution to Parquet three ways (the
  direct writer, `Table::write_parquet`, and arrow-rs alone).

## Findings

- Like for like, populate is about 1.5 to 2.5x faster than tpchgen on the
  numeric columns. All of that gap is the random number generator: given
  tpchgen's generator, populate is no faster.
- The vectorised TPC-H RNG in `lanes.rs` closes most of it while keeping
  tpchgen's exact output: about 1.8x at the default target and 2.5 to 3x
  with AVX2. This is the approach `populate-tpch` is built on.
- populate is about 7% slower than a hand-written column loop and about 30%
  slower than a hand-written row loop into Arrow builders, on the same RNG.
- Memory stays flat at any scale factor when streaming: about one batch per
  thread.

Cloud VMs are noisy (±20 to 30% run to run), so treat ratios as the result,
not absolute numbers.
