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
- [ ] **SSB** (`populate-ssb`)
  - `lineorder`, `customer`, `supplier`, `part`, `date`, plus an optional
    flattened SSB-flat table.
  - Reuse the TPC-H streams. Clean re-implementation only, no code copied from
    `ssb-dbgen` until its licence is confirmed.
  - Test against `ssb-dbgen` output.
- [ ] **H2O db-benchmark groupby and join** (`populate-h2o`)
  - groupby: `id1`–`id6`, `v1`–`v3`, with N, K, NA % and sort knobs.
  - join: LHS of N rows, RHS small / medium / big (N/1e6, N/1e3, N).
  - Decide the match target: R's `set.seed(108)` output (needs R's Mersenne
    Twister and `sample()`), `falsa`, or statistics only.
- [ ] **DSB** (skewed TPC-DS, on `populate-tpcds`)
  - Find out whether DSB's code forks tpcds-kit's dsdgen.
  - Port its skew and correlation changes into the `tpcdsgen` path, and
    generate tables in dependency order.
  - Test against DSB's own generator output.
- [ ] **TSBS devops / IoT** (`populate-tsbs`)
  - Reproduce Go `math/rand` and the per-host simulation for `--seed`
    determinism.
  - Output Arrow / Parquet. Test against `tsbs_generate_data` with a fixed
    seed.
- [x] **TPC-C and YCSB initial load** (`populate-oltp`)
  - All 9 TPC-C tables with NURand and random strings, tested against the
    spec (no loader has reproducible output: all seed from the clock).
  - YCSB `usertable`, byte-identical to YCSB 0.17.0's own `ycsb load`
    (keys, `dataintegrity` values, and random values given the client
    thread's seed). See [`populate-oltp/README.md`](populate-oltp/README.md).
- [x] **LDBC SNB, identical to the LDBC Spark datagen** (`populate-ldbc`)
  - All 14 dynamic and 4 static tables of `--mode raw`. Byte-identical CSV
    at SF 0.1 and 1; value-identical to the datagen's raw Parquet, row by
    row, at SF 3 and 10 (its CSV row order past one block is a Spark
    file-scan artefact). 9.5–32x faster than the Spark datagen on 4 cores
    (see [`populate-ldbc/README.md`](populate-ldbc/README.md)).
  - Persons and friendships are a pre-pass; a chunk is 1,000 persons of an
    activity block, resumed from the block's RNG state.
  - [ ] Cheaper state-skipping pass (now ~60% of generation), and the
    interactive / BI modes (`--mode bi`: factor tables, update streams).
- [x] **Synthetic shapes** (examples, not benchmarks) (`populate-synth`)
  - NYC-taxi-like trips, ClickBench-like web hits, ML feature tables, and
    vector embeddings as `FixedSizeList<f32>`. Every value is a function of
    `(seed, column, row)`, so chunked output equals sequential and Parquet
    is byte-identical for any thread count (see
    [`populate-synth/README.md`](populate-synth/README.md)).
