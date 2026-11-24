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
- [ ] **DSB** (skewed TPC-DS, on `populate-tpcds`)
  - Find out whether DSB's code forks tpcds-kit's dsdgen.
  - Port its skew and correlation changes into the `tpcdsgen` path, and
    generate tables in dependency order.
  - Test against DSB's own generator output.
- [x] **TSBS devops / IoT** (`populate-tsbs`)
  - All five use cases, byte-identical to `tsbs_generate_data` for any seed
    and flags in the eight text and three binary formats (Go `math/rand`, amd64
    `math.Exp`/`math.Log`, the per-host and batch simulation, ported);
    Arrow / Parquet per measurement. 6x Go on one thread, about 20x on 4
    cores (see [`populate-tsbs/README.md`](populate-tsbs/README.md)).
  - [x] The binary formats: `siridb` (qpack), `prometheus` (protobuf),
    `mongo` (flatbuffers).
- [x] **TPC-C and YCSB initial load** (`populate-oltp`)
  - All 9 TPC-C tables with NURand and random strings, tested against the
    spec (no loader has reproducible output: all seed from the clock).
  - YCSB `usertable`, byte-identical to YCSB 0.17.0's own `ycsb load`
    (keys, `dataintegrity` values, and random values given the client
    thread's seed). See [`populate-oltp/README.md`](populate-oltp/README.md).
- [ ] **LDBC SNB** (spike only)
  - Prototype `Person` and `knows` to see whether the multi-pass, sorted
    generation fits `Chunks` at all, before committing to the full schema.
- [x] **Synthetic shapes** (examples, not benchmarks) (`populate-synth`)
  - NYC-taxi-like trips, ClickBench-like web hits, ML feature tables, and
    vector embeddings as `FixedSizeList<f32>`. Every value is a function of
    `(seed, column, row)`, so chunked output equals sequential and Parquet
    is byte-identical for any thread count (see
    [`populate-synth/README.md`](populate-synth/README.md)).
