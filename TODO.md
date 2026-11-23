# TODO: generators to build

One build task per generator from the survey in
[`docs/benchmarks.md`](docs/benchmarks.md), in recommended order. Each should
follow the `populate-tpcds` pattern: a crate on top of `populate-table`'s
`Chunks` engine, Arrow / Parquet / text output, and tests that chunked,
multi-threaded output equals one sequential run.

- [ ] **TPC-H, byte-identical to dbgen** (`populate-tpch`)
  - Wrap `tpchgen` 4.0's per-table generators (`part`/`part_count`,
    `advance_rows`) in `Chunks`.
  - Test: `.tbl` output equals `tpchgen-cli` byte for byte at SF 1.
  - Benchmark against `tpchgen-cli` (`.tbl` and Parquet, 1 and N threads), and
    switch `populate-bench` to real TPC-H data.
  - First check whether `tpchgen-cli` already splits one table across cores.
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
- [ ] **TPC-C and YCSB initial load** (`populate-oltp`)
  - TPC-C tables with NURand and random strings, and YCSB `usertable`.
  - No canonical bytes: test row counts, key ranges and distributions against
    the spec.
- [ ] **LDBC SNB** (spike only)
  - Prototype `Person` and `knows` to see whether the multi-pass, sorted
    generation fits `Chunks` at all, before committing to the full schema.
- [ ] **Synthetic shapes** (examples, not benchmarks)
  - NYC-taxi-like trips, ClickBench-like web hits, ML feature tables, and
    vector embeddings as `FixedSizeList<f32>`.
