# TODO: generators to build

One build task per generator from the survey in
[`docs/benchmarks.md`](docs/benchmarks.md), in recommended order. Each should
follow the `populate-tpcds` pattern: a crate on top of `populate-table`'s
`Chunks` engine, Arrow / Parquet / text output, and tests that chunked,
multi-threaded output equals one sequential run.

- [x] **TPC-H, byte-identical to dbgen** (`populate-tpch`)
  - Done with its own column-at-a-time generators on dbgen's skip-ahead
    streams (tpchgen 4.0 for distributions and tests). All 8 SF 1 `.tbl`
    files equal `tpchgen-cli`'s; 1.4–2.6x faster than `tpchgen-cli` (see
    [`populate-tpch/README.md`](populate-tpch/README.md)). `tpchgen-cli`
    splits a table into at most `--num-threads` parts.
  - [ ] Switch `populate-bench` to real TPC-H data.
- [x] **SSB** (`populate-ssb`)
  - All five tables plus `lineorder_flat`; every `.tbl` file equals
    `ssb-dbgen`'s (`cmp`, SF 0.01 to 10), on TPC-H's streams (see
    [`populate-ssb/README.md`](populate-ssb/README.md)). Clean
    re-implementation, no ssb-dbgen code.
- [x] **H2O db-benchmark groupby and join** (`populate-h2o`)
  - Matches R's `set.seed(108)` output byte for byte (R's Mersenne
    Twister, `sample()`, `round()`, data.table `fwrite`): every 1e7 data
    set the benchmark uses and groupby `1e8_1e2_0_0` / `1e8_1e1_5_1`.
    12–40x faster than `Rscript` (see
    [`populate-h2o/README.md`](populate-h2o/README.md)).
  - [ ] Parallelise the join key shuffles themselves (draws are
    sequential; only the swaps overlap).
- [x] **DSB** (`populate-dsb`): DSB's dsdgen is a fork of TPC-DS dsdgen;
  its changes are ported into a copy of tpcdsgen. All 24 tables match DSB
  dsdgen byte for byte at SF 1, 2 and 5, chunked on all cores.
  - Next: build the sales columns directly (no row structs), as
    populate-tpcds' fused channels do.
- [ ] **TSBS devops / IoT** (`populate-tsbs`)
  - Reproduce Go `math/rand` and the per-host simulation for `--seed`
    determinism.
  - Output Arrow / Parquet. Test against `tsbs_generate_data` with a fixed
    seed.
- [x] **TPC-C and YCSB initial load** (`populate-tpcc`, `populate-ycsb`)
  - All 9 TPC-C tables with NURand and random strings, tested against the
    spec (no loader has reproducible output: all seed from the clock).
  - YCSB `usertable`, byte-identical to YCSB 0.17.0's own `ycsb load`
    (keys, `dataintegrity` values, and random values given the client
    thread's seed). See [`populate-tpcc/README.md`](populate-tpcc/README.md) and
    [`populate-ycsb/README.md`](populate-ycsb/README.md).
- [x] **LDBC SNB, identical to the LDBC Spark datagen** (`populate-ldbc`)
  - All 14 dynamic and 4 static tables of `--mode raw`. Byte-identical CSV
    at SF 0.1 and 1; value-identical to the datagen's raw Parquet, row by
    row, at SF 3 and 10 (its CSV row order past one block is a Spark
    file-scan artefact). 13–39x faster than the Spark datagen on 4 cores
    (see [`populate-ldbc/README.md`](populate-ldbc/README.md)).
  - `--mode bi` (initial snapshot, insert and delete batches) is
    byte-identical at SF 0.1 and 1. So are the exploded-edge,
    exploded-attribute and epoch-millis layouts at SF 0.1. The datagen has
    no separate interactive mode.
  - The 34 factor tables (`--generate-factors`) are identical at SF 0.1.
    At SF 1, 33 match; `people2Hops` samples over Spark's shuffle
    partitions.
  - Persons and friendships are a pre-pass; a chunk is 1,000 persons of an
    activity block, resumed from the block's RNG state. The state-skipping
    pass is ~40% of generation, down from ~60%.
  - [ ] Reproduce Spark's partitioning of large sampled factor tables
    (`people2Hops` from SF 1).
- [x] **Synthetic shapes** (examples, not benchmarks) (`populate-synth`)
  - NYC-taxi-like trips, ClickBench-like web hits, ML feature tables, and
    vector embeddings as `FixedSizeList<f32>`. Every value is a function of
    `(seed, column, row)`, so chunked output equals sequential and Parquet
    is byte-identical for any thread count (see
    [`populate-synth/README.md`](populate-synth/README.md)).
