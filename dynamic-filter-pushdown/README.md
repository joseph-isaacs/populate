# Dynamic filter pushdown: DuckDB vs DataFusion, and a DataFusion patch

This directory audits whether runtime ("dynamic") filters reach the table scans on TPC-H, TPC-DS and
ClickBench. It covers DuckDB 2.0.0, DataFusion 55.0.0 (Python) and DataFusion `main` (57.0.0-dev,
commit `d3ae55e`). It also has a DataFusion patch that closes most of the gaps found.

A dynamic filter is built while a query runs. A hash join builds one from its build side, a TopK
from its current cut-off row, and an aggregate from its running MIN/MAX. To skip data, the filter
has to travel down the plan to the scan. A filter counts as **pushed** only if it shows up in
`EXPLAIN ANALYZE` on a scan below the operator that made it.

## Results

Equi hash joins whose filter reaches a scan. Joins with an empty build side at runtime are left out.

| engine | TPC-H | TPC-DS | TopK pushed (TPC-H / TPC-DS / ClickBench) |
|---|---|---|---|
| DuckDB 2.0.0 | 55/63 (87%) | 430/533 (81%) | 0/5, 8/80, 4/30 |
| DataFusion `main` | 60/62 (97%) | 704/734 (96%) | 1/5, 27/73, 4/31 |
| DataFusion `main` + patch | 61/62 (98%) | 719/734 (98%) | 2/5, 39/73, 4/31 |

The DuckDB numbers are probably a little low. The harness matches filters to joins by column name
and doesn't follow renames, which also hid about 40 DataFusion pushes until fixed for DataFusion.

### Why filters don't reach the scan

* **Correct by design (both engines).**
  * Outer joins, on the side that must keep all its rows.
  * FULL joins.
  * Anti joins (`NOT IN`, `NOT EXISTS`).
  * DuckDB MARK joins.
  * Aggregates, for filters on aggregated values.
  * A TopK sorted by an aggregate (`ORDER BY count(*) DESC`), which covers most ClickBench TopKs.
* **DuckDB gaps.** These come from `join_filter_pushdown_optimizer.cpp` in v2.0.0.
  * Materialised CTEs and decorrelated subqueries: no filter can enter a `CTE_SCAN`.
  * `IS NOT DISTINCT FROM` joins are skipped (`// TODO: Need OR predicates`).
  * Only the probe side of a lower join is searched.
  * Windows block filters.
  * No MIN/MAX aggregate filter.
* **DataFusion gaps, before the patch.**
  1. The TopK filter covers *all* ORDER BY keys as one lexicographic predicate. It only goes to an
     input that produces every key, so `ORDER BY item_col, store_col` couldn't pass a join.
  2. A join on several keys builds one filter over all of them. When the keys come from different
     scans (TPC-H q05; TPC-DS q38, q39, q59), the filter reaches none of them.
  3. `BoundedWindowAggExec` and `WindowAggExec` don't implement filter pushdown, so they block
     everything. That includes filters on `PARTITION BY` columns, which would be safe
     (TPC-DS q47, q57, q70).
  4. `datafusion.execution.parquet.pushdown_filters` is off by default. Dynamic filters reaching a
     Parquet scan only skip row groups using statistics; they don't drop individual rows.

### Vortex

Vortex already does the scan-side part. Its V2 scans evaluate DataFusion's dynamic filters row by
row and skip unselective ones (`vortex-datafusion/src/persistent/{source,opener}.rs`). Gaps 1–3
are decided in DataFusion's operators, so fixing them in DataFusion also helps Vortex scans.

## The patch: `datafusion-dynamic-filter-split.patch`

The patch applies to DataFusion `main` at `d3ae55e`.

* **TopK first-key filter.** With several sort keys, TopK also maintains a filter on the first key
  alone: `first_key <= boundary`, `>=` for a descending key, with the sort's NULL ordering. Any row
  that can still enter the TopK passes it. It is pushed only when something between the TopK and
  the scan could stop the full filter: a join, union, aggregate or window. Elsewhere it would just
  repeat the full filter. Code: `topk/mod.rs`, `sorts/sort.rs`.
* **Per-key join filters.** A join on several keys also pushes one range filter per probe key,
  `key >= min AND key <= max`. The bounds cover every build partition. A filter stays `true` for a
  canceled partition, a key with no non-null values, or an empty build. It keeps probe NULLs for a
  null-equal join and is never used for null-aware (`NOT IN`) joins. Only filters that reach a
  consumer are kept. Code: `joins/hash_join/{exec,shared_bounds}.rs`.
* **Windows pass PARTITION BY filters.** A filter that only references columns every window
  expression partitions by is pushed to the window's input. It keeps or drops whole partitions, so
  window values don't change. Code: `windows/`.
* **Serialization.** The new filters round-trip through datafusion-proto. A new
  `SortExecNode.topk_prefix_filter` and `HashJoinExecNode.key_filters`, with generated code
  regenerated, keep producer and scan sharing one filter after decoding.
* **Tests.**
  * Unit tests: first-key filter NULL ordering; per-key bounds across partitions; a canceled
    partition staying permissive.
  * `dynamic_filter_split_pushdown.slt`: plans and results for each change.

Item 4 (row-level dynamic filters in Parquet) is **not** in the patch. Turning on
`pushdown_filters` made the patched build 6% slower on TPC-H and 3% slower on TPC-DS (geometric
mean), with more queries slowing than speeding up. A switch for dynamic filters only would need
benchmarking at a larger scale first. An adaptive approach like Vortex's (skip unselective
dynamic filters) looks like the better design.

### Verification

* DataFusion test suites, with git submodules initialised:
  * physical-plan: 3,071 unit tests passed.
  * datafusion-proto: 305 round-trip tests and the rest of its tests passed.
  * core `physical_optimizer` and `sql::explain`: 702 tests passed.
  * The full sqllogictest suite passed.
  * `cargo clippy -D warnings` and `cargo fmt --check` are clean.
* Query results, unpatched vs patched release builds, every benchmark query, sorted output:
  * 154 of 164 queries are identical.
  * The other 10 (`results/result_comparison.log`, marked `TIES`) disagree between two runs of the
    *unpatched* build. They have ties at the LIMIT cut-off, so any build can return a different
    row set.
* Runtime: the patched build is 4–5% faster by geometric mean (`results/timing_*.json`). Queries
  take 0.05–0.25 s here, with roughly ±20% run-to-run noise, so read this as "no regression".

## Caveats

* **Synthetic data.** The network blocks the official TPC-DS/TPC-H generators and the ClickBench
  dataset, so `harness/gen.py` writes synthetic Parquet. It uses the official schemas, SF1 row
  counts and key ranges. String columns use the literals the benchmark queries filter on. The hits
  table is sorted like the real file.
  * Whether a filter gets pushed depends on plan shape, so these results should hold on real data.
  * How many rows a filter prunes depends on the data, so don't read pruning numbers from these
    runs.
* The harness matches filters to joins by column name. Filters pushed through UNION branches
  (TPC-DS q02, q05) are written with branch column names. The harness misses them; checked by hand,
  they are pushed.
* TPC-DS q90 fails on both builds with a divide-by-zero caused by the synthetic data.

## Reproducing

The harness scripts in `harness/` use their own directory as the workspace and expect these
checkouts next to them:
* `ddb`: DuckDB queries and schemas
* `dfr`: DataFusion queries
* `cb`: ClickBench queries
* `dfcli-base` and `dfcli-new`: datafusion-cli binaries, unpatched and patched

Steps:
1. Run `python3 gen.py tpch tpcds clickbench` to write the data.
2. Run `./drive.sh <engine> <bench>` for each engine and benchmark. Engines are `duckdb`,
   `datafusion` (Python), `dfcli-base` and `dfcli-new`.
3. Run `python3 final.py` for the summary table and `python3 why2.py results/*.jsonl` for the
   reason for each unpushed filter.
4. Run `./cmp.sh` to compare query results and `python3 timing.py` for timings.
