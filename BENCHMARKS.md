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
with `--ssb-dbgen` (or named `ssb-dbgen` on `PATH`); its text output is
compared with populate at each requested thread count. Missing tools are
skipped. The default run takes about 10 minutes on 4 cores;
`--scale 0.1 --runs 1` takes under a minute.

| flag | default | |
|---|---|---|
| `--scale` | `1` | comma-separated; each generator maps it to its own size (`--list`) |
| `--threads` | `1,N` (N = cores) | comma-separated thread counts |
| `--generators` | all | names from `--list`, or a family: `h2o`, `ycsb`, `synth` |
| `--formats` | `text,parquet,arrow` | `text` is the reference tool's own format (`.tbl`, `.dat`, CSV, influx line protocol); `arrow` generates batches and drops them |
| `--runs` | `3` | runs per configuration; the best wall time is kept |
| `--out` | `/dev/shm`, else the temp dir | output goes to `DIR/populate-bench` and is deleted after each run; use a disk path when the output will not fit in memory |
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

### JSON reports

```bash
cargo xtask bench --json /tmp/benchmark.json
```

The Markdown table is still printed. JSON follows
[`xtask/benchmark-report.schema.json`](xtask/benchmark-report.schema.json)
(JSON Schema draft 2020-12). The report is checkpointed atomically after
each configuration; `complete: false` identifies an interrupted run.
`complete: true` means the matrix finished, even if samples failed or
unavailable tools were skipped. Failed samples still make the command exit
with a nonzero status. Build time is excluded from measurements.

Version 1 uses these conventions:

- `engines` identifies implementations by an open string ID, version and
  source revision. Unknown versions/revisions are `null`. Add an engine
  here and append its `measurements`; the format has no fixed engine list.
- Each measurement identifies the engine, workload, requested scale,
  resolved size, output format and actual thread argument. `size` is a
  display label; `command.args` records the exact resolved parameters,
  including seeds when explicitly passed. Optional `command.env` records
  environment overrides (such as SSB's `DSS_PATH` output directory); an
  absent object means no overrides. Generator defaults are pinned
  by the source revision. Single-threaded Go and C references are recorded
  once per unique command, even when compared to several populate thread
  counts in the Markdown table.
- `samples` retains every timed attempt in execution order for that
  measurement. Successful samples store process wall time in **seconds**
  and output-file size in **bytes**, without display rounding. This
  includes process startup and writing, but excludes directory setup,
  size collection and cleanup; writes are not explicitly synced to disk.
  Failed samples carry an error string, never a fabricated zero timing.
- `summary` selects the minimum wall time and the bytes from that same
  sample. It is `null` if any attempt failed. There are no separate
  warmups (`warmup_runs: 0`); filesystem caches are not flushed.
- `arrow` means batches generated and dropped in memory, not Arrow IPC
  files. Its `output_bytes: 0` is a file count, not a memory or throughput
  measurement. No peak-memory or generated-row-count metric is inferred.
- `source`, `environment`, `configuration` and `invocation` record
  provenance, host details, compiler settings and the requested matrix.
  Unavailable metadata is `null`. Timestamps are Unix seconds in UTC.
  `skipped` distinguishes unavailable resources/tools and unsupported
  formats from successful measurements.

Compare engines only when workload parameters, output semantics, hardware
and thread counts match; this runner does not verify output equivalence.
Keep separate reports for different machines or invocations, rather than
appending measurements under another run's environment. Consumers should
reject unknown `schema_version` values and check that engine IDs are unique,
every measurement references a declared engine, and each sample count
matches `configuration.runs`. Additional engines can emit this format from
their own harness; running them through `xtask` requires a command adapter.

The [Apple M3 Max baseline](benchmarks/results/2026-11-25-m3-max.json)
records the default matrix at the source revision named in the file.
The [reference comparison](benchmarks/results/2026-11-25-m3-max-references.json)
runs populate interleaved with tpcgen-rs 4.0.0 (TPC-H and TPC-DS), Go TSBS,
and C SSB. Reference versions/revisions are annotated from installation
and build metadata. Both TPC Parquet writers use Snappy with their own
default layouts; this measures the complete tools, including their
writer settings, rather than isolating generator kernels.
The TPC-DS reference also writes a small, timestamped `dbgen_version` file;
populate writes the 24 data tables. That metadata file is included in the
reference's measured time and bytes, and excluded from data-equivalence checks.
Validate reports with a draft 2020-12 validator, for example:

```bash
uv run --with jsonschema python - <<'PY'
import json
from pathlib import Path
from jsonschema import Draft202012Validator

schema = json.loads(Path("xtask/benchmark-report.schema.json").read_text())
Draft202012Validator.check_schema(schema)
validator = Draft202012Validator(schema)
for path in Path("benchmarks/results").glob("*.json"):
    validator.validate(json.loads(path.read_text()))
    print(f"valid: {path}")
PY
```

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
