# Benchmarks

Every benchmark data set populate generates, what it matches, and how fast
it is against the reference tool. Each crate's README has the full tables,
the exact commands and how the output was verified. Speedups are wall time
on a 4-core x86-64 cloud VM; see each README for the settings.

## Generators

| benchmark | crate | matches | speed vs the reference |
|---|---|---|---|
| TPC-H | [`populate-tpch`](generator/tpch/README.md) | `dbgen` / `tpchgen-cli`, byte for byte, all 8 tables | 1.3–2.6x `tpcgen-cli` (SF 1 and 10, `.tbl` and Parquet, 1 and 4 threads) |
| TPC-DS | [`populate-tpcds`](generator/tpcds/README.md) | `dsdgen`, byte for byte, all 24 tables | 2.0–3.3x `tpcgen-cli` (SF 1 and 10, `.dat` and Parquet) |
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

## Running them

```bash
cargo xtask bench
```

builds every generator in release mode, runs each one in each output
format at `--scale 1` on 1 thread and on every core, and prints a Markdown
table of wall time and output size. Where a reference tool is on `PATH` it
runs alternately with ours and the table gets a speedup column:
`tpcgen-cli` (`cargo install tpcgen-cli`) for TPC-H and TPC-DS, and
`tsbs_generate_data` for TSBS. Missing tools are skipped.

A reference runs like for like. `tpcgen-cli` writes the same tables, and
for Parquet it gets the settings read off our own output: the same codec
(Snappy), no dictionary on the columns we write without one, and
`--row-group-bytes` set to our largest table's row-group size, so that
table has about as many row groups on both sides. After the first run of
each, the two outputs are compared, text files byte for byte and Parquet
files by schema, values, codecs and dictionary use. The `output` column
says `same`; a difference fails the run. Tables whose row-group counts still
differ by more than 2x are listed under the table. The default run
takes about 10 minutes on 4 cores; `--scale 0.1 --runs 1` takes under a
minute.

| flag | default | |
|---|---|---|
| `--scale` | `1` | comma-separated; each generator maps it to its own size (`--list`) |
| `--threads` | `1,N` (N = cores) | comma-separated thread counts |
| `--generators` | all | names from `--list`, or a family: `h2o`, `ycsb`, `synth` |
| `--formats` | `text,parquet,arrow` | `text` is the reference tool's own format (`.tbl`, `.dat`, CSV, influx line protocol); `arrow` generates batches and drops them |
| `--runs` | `3` | runs per configuration; the best wall time is kept |
| `--out` | `/dev/shm`, else the temp dir | output goes to `DIR/populate-bench` and is deleted after each run; against a reference one output of each is kept to compare, so allow twice the output size; use a disk path when it will not fit in memory |
| `--tpcgen-cli`, `--tsbs-generate-data` | found on `PATH` | reference binaries |
| `--no-reference` | | skip reference tools |

At `--scale 1`, DSB and the H2O join run at SF 1 and 1e7 rows (their
smallest sizes), TPC-C at 10 warehouses, YCSB at 1M records and 10M
operations, synth at 10M rows (1M for embeddings) and TSBS devops at 100
hosts for one day. LDBC runs when its datagen resources are present
(`scripts/fetch-ldbc-resources.sh`). For example, the TPC-H rows of the
table above:

```bash
cargo xtask bench --generators tpch --scale 1,10 --formats text,parquet --out /var/tmp
```

The numbers are printed, not committed: a crate README keeps its current
performance section, and run results go in the PR description.

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
