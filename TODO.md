# TODO: generators to build

One build task per generator from the survey in
[`docs/benchmarks.md`](docs/benchmarks.md), in recommended order. Each should
follow the `populate-tpcds` pattern: a crate on top of `populate-table`'s
`Chunks` engine, Arrow / Parquet / text output, and tests that chunked,
multi-threaded output equals one sequential run.

- [x] **TPC-H, byte-identical to dbgen** (`populate-tpch`)
  - Done with its own column-at-a-time generators on dbgen's skip-ahead
    streams (tpchgen 4.0 for distributions and tests). All 8 SF 1 `.tbl`
    files equal `tpchgen-cli`'s; 1.4–2.6x faster than `tpcgen-cli` (see
    [`populate-tpch/README.md`](populate-tpch/README.md)). `tpcgen-cli`
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
- [x] **TSBS devops / IoT** (`populate-tsbs`)
  - All five use cases, byte-identical to `tsbs_generate_data` for any seed
    and flags in the eight text formats (Go `math/rand`, amd64
    `math.Exp`/`math.Log`, the per-host and batch simulation, ported);
    Arrow / Parquet per measurement. 3.5x Go on one thread, 12-15x on 4
    cores (see [`populate-tsbs/README.md`](populate-tsbs/README.md)).
  - [ ] The binary formats: `siridb` (qpack), `prometheus` (protobuf),
    `mongo` (flatbuffers).
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
