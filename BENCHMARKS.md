# Benchmarks

Every benchmark data set populate generates, what it matches, and how fast
it is against the reference tool. Each crate's README has the full tables,
the exact commands and how the output was verified. Speedups are wall time
on a 4-core x86-64 cloud VM; see each README for the settings.

## Generators

| benchmark | crate | matches | speed vs the reference |
|---|---|---|---|
| TPC-H | [`populate-gen-tpch`](generator/tpch/README.md) | `dbgen` / `tpchgen-cli`, byte for byte, all 8 tables | 1.3–2.6x `tpcgen-cli` (SF 1 and 10, `.tbl` and Parquet, 1 and 4 threads) |
| TPC-DS | [`populate-gen-tpcds`](generator/tpcds/README.md) | `dsdgen`, byte for byte, all 24 tables | 2.0–3.3x `tpcgen-cli` (SF 1 and 10, `.dat` and Parquet) |
| DSB | [`populate-gen-dsb`](generator/dsb/README.md) | DSB's `dsdgen`, byte for byte, all 24 tables (SF 1 and 2) | SF 1 `.dat` in ~2 s on 4 cores; `dsdgen` takes 68 s on one |
| SSB and SSB-flat | [`populate-gen-ssb`](generator/ssb/README.md) | `ssb-dbgen`, byte for byte, all 5 tables (SF 0.0001 to 10) | 5.8x `ssb-dbgen` on 1 thread, 13x on 4 (SF 1 `.tbl`) |
| H2O db-benchmark | [`populate-gen-h2o`](generator/h2o/README.md) | its R datagen (`set.seed(108)`), byte for byte | 7–39x `Rscript`; 1e8-row joins that R runs out of memory on take ~17–20 s |
| TSBS devops / IoT | [`populate-gen-tsbs`](generator/tsbs/README.md) | `tsbs_generate_data`, byte for byte, all 5 use cases, 11 formats | 3–21x Go on 1 thread, 11–56x on 4 |
| LDBC SNB | [`populate-gen-ldbc`](generator/ldbc/README.md) | the LDBC Spark datagen: byte-identical CSV at SF 0.1 and 1, value-identical Parquet at SF 3 and 10 | 9–39x the Spark datagen on 4 cores (SF 1 to 10) |
| YCSB | [`populate-gen-ycsb`](generator/ycsb/README.md) | YCSB 0.17.0 `load` and `run`, byte for byte | ~9x YCSB's own load (1M records, TSV), ~18x its run (2M ops) |
| TPC-C | [`populate-gen-tpcc`](generator/tpcc/README.md) | the spec's population rules (no loader has reproducible output) | ~10x `go-tpc tpcc prepare` (10 warehouses, CSV) |
| synthetic shapes | [`populate-gen-synth`](generator/synth/README.md) | no reference: taxi trips, ClickBench-style hits, ML features, embeddings | taxi at 34.6 M rows/s on 4 threads |

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
`tsbs_generate_data` for TSBS. SSB's single-threaded C `dbgen` can be supplied
with `--ssb-dbgen` (or named `ssb-dbgen` on `PATH`). Missing tools are
skipped.

A reference runs like for like. `tpcgen-cli` writes the same tables, and
for Parquet it gets the settings read off our own output: the same codec
(Snappy), no dictionary on the columns we write without one, and
`--row-group-bytes` set to our largest table's row-group size, so that
table has about as many row groups on both sides. After the first run of
each, the two outputs are compared, text files byte for byte and Parquet
files by schema, values, codecs and dictionary use. The `output` column
says `same`; a difference fails the run. Tables whose row-group counts still
differ by more than 2x are listed under the table. Go's
`tsbs_generate_data` and SSB's `dbgen` are single-threaded, so each one's
time is the reference for every thread count, and the table says so. The
default run takes about 10 minutes on 4 cores; `--scale 0.1 --runs 1` takes
under a minute.

| flag | default | |
|---|---|---|
| `--scale` | `1` | comma-separated; each generator maps it to its own size (`--list`) |
| `--threads` | `1,N` (N = cores) | comma-separated thread counts |
| `--generators` | all | names from `--list`, or a family: `h2o`, `ycsb`, `synth` |
| `--formats` | `text,parquet,arrow` | `text` is the reference tool's own format (`.tbl`, `.dat`, CSV, influx line protocol); `arrow` generates batches and drops them |
| `--runs` | `3` | minimum attempts per configuration; median and range are reported |
| `--max-runs` | `max(5, runs)` | cap for noisy configurations; set equal to `--runs` for a fixed count |
| `--noise-threshold` | `0.10` | extend each engine independently while `(max - min) / median` exceeds this fraction |
| `--sync-output` | off | sync regular output files after process timing; record flush and total times separately |
| `--out` | `/dev/shm`, else the temp dir | output goes to `DIR/populate-bench` and is deleted after each run; against a reference one output of each is kept to compare, so allow twice the output size; use a disk path when it will not fit in memory |
| `--tpcgen-cli`, `--tsbs-generate-data` | found on `PATH` | reference binaries |
| `--ssb-dbgen` | `ssb-dbgen` on `PATH` | SSB reference binary; text output only, single-threaded |
| `--ssb-dists` | beside the SSB binary or one directory above it | SSB reference's `dists.dss` file |
| `--no-reference` | | skip reference tools |
| `--json` | | save a versioned JSON report to a path outside the temporary output directory |

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

### JSON reports

```bash
cargo xtask bench --json /tmp/benchmark.json
```

The Markdown table is still printed. The report (`schema_version: 1`) is
compact JSON, saved again after each configuration, so `complete: false`
marks an interrupted run. Build time is not measured. Its JSON Schema
comes from the Rust types that write it:

```bash
cargo xtask bench-schema > benchmark-report.schema.json
```

The report holds:

- `engines`: each implementation's ID, version and source revision
  (`null` when unknown).
- `measurements`: one per engine and configuration, with the exact
  command and environment. `samples` keeps every timed attempt in order,
  as wall time in seconds and output size in bytes, or an error string.
  With `--sync-output` a sample also has its flush time. `summary` is the
  median wall time with its minimum and maximum, and is `null` if any
  attempt failed. `validation_status` is `passed` or `mismatch` from the
  output comparison above, else `not_run`. Single-threaded references are
  recorded once per command.
- `source`, `environment`, `configuration` and `invocation`: revision,
  host, compiler settings and the requested matrix. `skipped` lists
  missing tools and unsupported formats.

Each trial reverses the engine order, and only an engine whose spread
exceeds `--noise-threshold` gets more runs, up to `--max-runs`. `arrow`
means batches generated and dropped in memory, so its output size is 0.
Compare reports only from the same machine and invocation.

## Not built

- **TPC-E:** EGen is behind a TPC request form and the TPC EULA, so there
  is no open reference to match.
- **JOB / IMDB:** real IMDb data, licensed for personal and
  non-commercial use, with no generator.
- **ClickBench:** real, anonymised web-analytics data with no generator,
  under CC BY-NC-SA 4.0. `populate-gen-synth` has a ClickBench-shaped table
  instead.

## Keeping this current

Update a row when the crate's README changes: replace the numbers rather
than adding before/after comparisons. Open work on a generator goes in a
GitHub issue, not here.
