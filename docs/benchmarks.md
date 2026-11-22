# Benchmarks to build on populate

Survey from 22 Nov 2026. Build tasks are tracked in [`TODO.md`](../TODO.md).

## Recommendation

Build **spec-correct TPC-H, byte-identical to dbgen**, next. It is the TPC-DS
playbook from PR #16 applied again, with an Apache-2.0 reference (`tpchgen`)
that already has skip-ahead streams. It also fixes the weak spot in today's
`populate-bench`, where populate draws uniform values instead of TPC-H's
correlated ones. After that, **SSB** comes almost for free on the same
streams. If you want something outside the TPC family, **H2O db-benchmark**
(groupby/join) is the best small win.

## What fits this framework

populate is strongest when three things hold:

- the data is wide, flat tables (or simple lists) that land in Arrow, Parquet or `.dat`;
- the reference generator can skip to any row, so `populate-table`'s `Chunks`
  engine can cut tables into independent jobs;
- there is a licence-clean reference to be byte-identical against, which makes
  "faster and correct" a provable claim.

Workloads that are mostly about transactions, graph structure, or real (not
generated) data fit poorly.

## Ranked shortlist

| # | System | Columnar fit | Licence-clean reference | Effort | Usefulness |
|---|---|---|---|---|---|
| 1 | TPC-H (spec-correct) | Excellent | Yes: `tpchgen` / `tpchgen-cli`, Apache-2.0, already a dependency | Small | High |
| 2 | SSB (Star Schema Benchmark) | Excellent | Partly: `ssb-dbgen` (C, forked from TPC-H dbgen, licence unverified) | Small to medium after TPC-H | High for OLAP engines |
| 3 | H2O db-benchmark groupby/join | Excellent | Yes: R datagen scripts, MPL-2.0; `falsa` (Rust, unofficial) | Small | Medium-high |
| 4 | DSB (skewed TPC-DS) | Excellent | Yes: Microsoft, MIT | Medium-large | Medium |
| 5 | TSBS (time series) | Good | Yes: Go, MIT, seeded | Medium | Medium |
| 6 | TPC-C / YCSB initial load | Fair | go-tpc, BenchBase, YCSB (Apache-2.0), but no canonical bytes | Small | Low |
| 7 | LDBC SNB | Poor | Spark datagen, Apache-2.0 | Large | Medium (graph only) |
| 8 | Synthetic shapes: taxi-like, ClickBench-like, ML/vector | Good | None to match | Small each | Medium as examples |
| — | TPC-E, JOB/IMDB, ClickBench itself | — | No | — | Skip |

## Notes on each

### 1. TPC-H, byte-identical to dbgen

- **How:** wrap `tpchgen`'s per-table generators in `Chunks`, the way
  `populate-tpcds` wraps `tpcdsgen`. Every `tpchgen` generator takes
  `(scale_factor, part, part_count)` and its streams have `advance_rows`, so
  chunks can start at any row.
- **Proof:** `cmp` against `tpchgen-cli` `.tbl` output (which tpcgen-rs tests
  against the reference implementation), plus the same
  chunked-equals-sequential tests as TPC-DS.
- **Risk:** `tpchgen-cli` may already spread tables across cores (unverified),
  so the speedup may be smaller than the 1.5–2x on TPC-DS. The win would come
  mainly from the shared job pool and encoding on the workers.
- **Bonus:** the existing benchmark can then compare real TPC-H data on both sides.

### 2. SSB

- Five tables: `lineorder` (orders and lineitem joined), `customer`,
  `supplier`, `part`, `date`. ClickHouse, Doris, StarRocks and Kylin all
  publish SSB numbers, and many run a flattened "SSB-flat" single table, which
  suits a columnar generator well.
- `ssb-dbgen` "was forked from an older, now out-of-date version of TPC-H
  dbgen", so it probably shares dbgen's random streams and can reuse most of
  the TPC-H work. Its README doesn't say so, so that needs checking against
  its output. Matching it byte for byte means porting its few changes.
- **Licence caveat:** no licence file was found in `eyalroz/ssb-dbgen`. Since
  it derives from TPC's dbgen, the safest route is a clean re-implementation
  tested against its output, without copying code.

### 3. H2O db-benchmark (groupby and join)

- Small schema: `id1`–`id6` (K-level strings, ints in 1..K, and N/K-level
  ids), `v1`–`v3` (small ints and a float), with knobs for N, K, NA % and
  sortedness. The join set has right-hand tables of N/1e6, N/1e3 and N rows,
  and needs N ≥ 1e7.
- Used by DuckDB Labs' running results page and by Polars, so a fast
  generator for it is directly useful.
- **Matching:** the official generator is R with `set.seed(108)`.
  Byte-identical would mean re-implementing R's Mersenne Twister and
  `sample()`, which is doable but fiddly. `falsa` (Rust, ChaCha8) is
  unofficial and not identical to R. Matching the statistics, or matching
  `falsa`, is the cheap option.

### 4. DSB

- Microsoft's TPC-DS variant with skewed and correlated distributions, MIT
  licence. Popular for optimizer and learned-cardinality research.
- Builds directly on PR #16. The work is porting DSB's distribution changes
  into the `tpcdsgen` path, which is a real porting job, not a wrapper. Its
  tables must also be generated in dependency order.

### 5. TSBS

- Timescale's Time Series Benchmark Suite (Go, MIT): devops and IoT data, as
  many hosts emitting metrics on fixed intervals. Very columnar.
- Generation takes `--seed`, so output is deterministic. Matching it means
  reproducing Go's `math/rand` stream and its per-host simulation logic. Its
  default output is per-database insert formats, so the value here is a fast
  Parquet/Arrow version.

### 6. TPC-C and YCSB

- Their initial data loads are simple (TPC-C's NURand and random strings;
  YCSB's keys and random fields), but these benchmarks measure transactions,
  not bulk generation. Each driver has its own generator, so there is nothing
  canonical to be byte-identical with. Low value.

### 7. LDBC SNB

- A social-network graph with heavy correlations (friendships based on
  similarity, activity driven by graph structure), built in several sorted
  passes on Spark, with CSV and Parquet output. It does not cut into
  independent row chunks, so it fights the `Chunks` model. Large effort, and
  matching the reference would be very hard.

### 8. Synthetic shapes with no reference

- NYC-taxi-like trips, ClickBench-like web hits, ML feature tables, and vector
  embeddings (`FixedSizeList<f32>`) all suit populate's combinators and list
  support.
- With no reference to match, these work best as showcase examples or
  fixtures, not as benchmarks.

### Skip

- **TPC-E:** EGen is a download behind a TPC request form, and the TPC EULA
  very likely covers it, so there is no clean open reference.
- **JOB / IMDB:** real IMDb data for "personal and non-commercial use" only,
  with no generator.
- **ClickBench:** real, anonymised web-analytics data with no generator. The
  repo is CC BY-NC-SA 4.0.

## Verification

Each claim was re-checked against its source on 22 Nov 2026.

**Confirmed**

- **tpchgen 4.0** (read from the crate source): Apache-2.0, part-based
  generators, `advance_rows` skip-ahead. The tpcgen-rs README says its
  generators "produce the same exact bytes as the reference implementations".
- **populate-bench** draws independent uniform values (its README says so).
- **H2O db-benchmark:** MPL-2.0; both R scripts call `set.seed(108)`; columns
  and join table sizes as above.
- **falsa:** unofficial Rust generator using ChaCha8.
- **DSB:** MIT. **TSBS:** MIT, Go, `--seed`. **LDBC SNB datagen:** Apache-2.0.
  **go-tpc, BenchBase, YCSB:** Apache-2.0. **ClickBench repo:** CC BY-NC-SA 4.0.
  **IMDb datasets:** personal and non-commercial use only.

**Still unverified**

- The `ssb-dbgen` licence (GitHub rate-limited the LICENSE file; the README
  has no licence section).
- Whether DSB's code is a fork of tpcds-kit's C. Its README only says
  "adapted from" TPC-DS. The answer decides how much of the `tpcdsgen` path
  DSB could reuse.
- Whether `tpchgen-cli` already spreads one table across cores.
- TPC-E's licence is an inference: the TPC page doesn't name EGen's licence.

## Sources

- [datafusion-contrib/tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs)
- [eyalroz/ssb-dbgen](https://github.com/eyalroz/ssb-dbgen/)
- [h2oai/db-benchmark](https://github.com/h2oai/db-benchmark), [duckdblabs/db-benchmark](https://github.com/duckdblabs/db-benchmark), [DuckDB Labs results](https://duckdblabs.github.io/db-benchmark)
- [falsa: data generation in Rust](https://semyonsinchenko.github.io/ssinchenko/post/data-generation-rust)
- [microsoft/dsb](https://github.com/microsoft/dsb)
- [timescale/tsbs](https://github.com/timescale/tsbs)
- [ldbc/ldbc_snb_datagen_spark](https://github.com/ldbc/ldbc_snb_datagen_spark)
- [pingcap/go-tpc](https://github.com/pingcap/go-tpc), [cmu-db/benchbase](https://github.com/cmu-db/benchbase), [brianfrankcooper/YCSB](https://github.com/brianfrankcooper/YCSB)
- [ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench/)
- [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/)
- [TPC current specifications](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
