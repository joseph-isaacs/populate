# Benchmarks

Every benchmark data set populate generates, what it matches, and how fast
it is against the reference tool. Each crate's README has the full tables,
the exact commands and how the output was verified. Speedups are wall time
on a 4-core x86-64 cloud VM; see each README for the settings.

## Generators

| benchmark | crate | matches | speed vs the reference |
|---|---|---|---|
| TPC-H | [`populate-tpch`](generator/tpch/README.md) | `dbgen` / `tpchgen-cli`, byte for byte, all 8 tables | 1.4–2.6x `tpcgen-cli` (SF 1 and 10, `.tbl` and Parquet, 1 and 4 threads) |
| TPC-DS | [`populate-tpcds`](generator/tpcds/README.md) | `dsdgen`, byte for byte, all 24 tables | 2.3–3.3x `tpcgen-cli` (SF 1 and 10, `.dat` and Parquet) |
| DSB | [`populate-dsb`](generator/dsb/README.md) | DSB's `dsdgen`, byte for byte, all 24 tables (SF 1 and 2) | SF 1 `.dat` in ~2 s on 4 cores; `dsdgen` takes 68 s on one |
| SSB and SSB-flat | [`populate-ssb`](generator/ssb/README.md) | `ssb-dbgen`, byte for byte, all 5 tables (SF 0.0001 to 10) | 5.8x `ssb-dbgen` on 1 thread, 13x on 4 (SF 1 `.tbl`) |
| H2O db-benchmark | [`populate-h2o`](generator/h2o/README.md) | its R datagen (`set.seed(108)`), byte for byte | 7–39x `Rscript`; 1e8-row joins that R runs out of memory on take ~17–20 s |
| TSBS devops / IoT | [`populate-tsbs`](generator/tsbs/README.md) | `tsbs_generate_data`, byte for byte, all 5 use cases, 11 formats | 3–21x Go on 1 thread, 11–56x on 4 |
| LDBC SNB | [`populate-ldbc`](generator/ldbc/README.md) | the LDBC Spark datagen: byte-identical CSV at SF 0.1 and 1, value-identical Parquet at SF 3 and 10 | 9–39x the Spark datagen on 4 cores (SF 1 to 10) |
| YCSB | [`populate-ycsb`](generator/ycsb/README.md) | YCSB 0.17.0 `load` and `run`, byte for byte | ~9x YCSB's own load (1M records, TSV), ~18x its run (2M ops) |
| TPC-C | [`populate-tpcc`](generator/tpcc/README.md) | the spec's population rules (no loader has reproducible output) | ~10x `go-tpc tpcc prepare` (10 warehouses, CSV) |
| synthetic shapes | [`populate-synth`](generator/synth/README.md) | no reference: taxi trips, ClickBench-style hits, ML features, embeddings | taxi at 34.6 M rows/s on 4 threads |

All of them write Arrow, Parquet or the reference tool's own text format,
on all cores, with output that does not depend on the thread count.

## Not built

- **TPC-E:** EGen is behind a TPC request form and the TPC EULA, so there
  is no open reference to match.
- **JOB / IMDB:** real IMDb data, licensed for personal and
  non-commercial use, with no generator.
- **ClickBench:** real, anonymised web-analytics data with no generator,
  under CC BY-NC-SA 4.0. `populate-synth` has a ClickBench-shaped table
  instead.

## Keeping this current

Update a row when the crate's README changes: replace the numbers rather
than adding before/after comparisons. Open work on a generator goes in a
GitHub issue, not here.
