# populate-ldbc

LDBC SNB (Social Network Benchmark) data, identical to the
[LDBC Spark datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)'s
output, on all cores: the 14 dynamic tables (persons, friendships, forums,
posts, comments, tags, likes, ...) and the 4 static ones, as CSV, Arrow
`RecordBatch`es or Parquet. It covers both of the datagen's modes. `raw`
can take any of its layouts (exploded edges and attributes, epoch
milliseconds). `bi` writes the initial snapshot plus insert and delete
batches. It also writes the 34 factor tables (`--generate-factors`).

```sh
# the datagen's dictionaries (~72 MB) into target/ldbc_snb_datagen_spark
scripts/fetch-ldbc-resources.sh

# SF 1 as the datagen's CSV, in ./ldbc-out/{dynamic,static}/<Table>.csv
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 1 --out ldbc-out

# Parquet, one file per table; or generate only (counts rows)
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 10 --format parquet
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 10 --format none
```

Flags:

- `--sf` (the datagen's scale factors: 0.003, 0.1, 0.3, 1, 3, 10, 30,
  100, ...) or `--persons N`.
- `--resources DIR` (default `$LDBC_DATAGEN_RESOURCES`, then the fetch
  script's directory).
- `--out`, `--format csv|parquet|none`, `--threads` (default all cores).
- The datagen's own: `--mode raw|bi`, `--bulkload-portion` (default 0.97),
  `--batch-period year|month|day|hour|minute` (default day),
  `--explode-edges`, `--explode-attrs`, `--epoch-millis`,
  `--generate-factors`, `--factor-format csv|parquet` (default parquet).

```sh
# BI: OUT/initial_snapshot/{dynamic,static}/<Table>.csv and
# OUT/{inserts,deletes}/dynamic/<Table>/batch_id=<day>/part-<k>.csv (a
# new part once a string column of the batch passes 1 GiB)
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 1 --mode bi --out bi-out

# factor tables: OUT/factors/csv/raw/composite-merged-fk/<name>/part-0.csv
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 1 --generate-factors --factor-format csv
```

From Rust:

```rust
use populate_ldbc::{Ldbc, default_resources, persons_for_scale_factor};

let g = Ldbc::new(&default_resources().unwrap(), persons_for_scale_factor("1").unwrap(), 0)?;
// every dynamic table as Parquet, one shared pool
populate_table::write_parquet_many(&[&g.raw()], files, 0, None)?;
// or the datagen's CSV text
populate_table::text::write_text_many(&[&g.csv()], files, 0)?;
```

The 14 dynamic tables are one populate `Distribution`, `g.raw_dist()`: a
`flat_map` to `Tables` whose source rows are the chunks and whose
children are each table's rows of a chunk. Each table's row type is a tuple in `rows.rs`
(`Timestamp`, `Date32`, `Option<String>`, ...), so its Arrow schema is
read off the distribution (`schemas_of`), and `g.raw()` runs it as a
`populate_table::DistTables`. The 4 static tables are `g.static_dist()`,
one source row. `g.csv()` is a `TextChunks` source, so they share a pool
with other generators' tables. The other layouts are `layout::LayoutRun`,
and BI is `bi::BiRun`: its snapshot is a chunked source, then `finish`
gives the insert and delete batches. They work on the same columns held
by kind (`rows::Col`), with the types the row types give them. For factor
tables, call `g.collect_factors()` before generating, then
`g.factors(threads)`.

## How it matches the datagen

The port follows the datagen at commit `a2210c5` (the one the fetch script
checks out) and reads its own dictionary files. The datagen is a Spark job,
but every random draw comes from `java.util.Random`s reseeded per block of
10,000 persons, so its output does not depend on Spark's parallelism. The
port reproduces:

- `java.util.Random` and the datagen's 72-generator farms, the ssj
  distributions it uses (geometric via fdlibm's `log1p`, power law), Java's
  `String.split`, `trim`, `HashMap<Long, _>` iteration order,
  `Arrays.binarySearch` on floats, `Normalizer` (NFD, for e-mail
  addresses), UTF-16 string lengths and substrings, and the lazy Java
  streams' draw order, written out eagerly;
- the datagen's own bugs (three parameters that all read `maxEmails`, ...);
- Spark's CSV writer: `|` separator, header line, timestamps through
  `ms / 1000.0` seconds truncated to microseconds, trimmed strings, `""` for
  empty ones.

The generation has three passes. Persons are generated per block. Three
friendship passes each rank all persons by a key, cut the ranking into
blocks and connect persons within a block; a person's friendships are
merged. Activity (forums, posts, comments, photos, likes) and the person
tables are generated per block of the third ranking. The first two are a
pre-pass (`Ldbc::new`, blocks in parallel); activity is the chunked
source.

### Modes and layouts

These match the datagen's Spark transformations:

- **Layouts.** `--explode-edges` turns multi-valued foreign keys (a
  forum's container posts, a person's e-mails or languages, ...) into
  tables of their own. `--explode-attrs` does the same for multi-valued
  attributes: Spark's `split` keeps empty pieces, and a null gives no
  rows. `--epoch-millis` writes dates as integers.
- **BI.** The cut-off is `end - (end - start) * (1 - portion)`. The
  initial snapshot holds the rows created before it, without the deletion
  columns. Inserts are the later rows, deletes the rows explicitly
  deleted later (their deletion date and key). Both are stably sorted by
  date and cut into `date_format(date_trunc(period))` batches. The hour
  and minute batches use the datagen's `hh` pattern, a 12-hour clock.
- **Factors.** These are `FactorGenerationStage`'s 34 tables. Frequency
  tables are ordered by frequency (descending, nulls last), then by their
  keys. Left outer joins count 0 and sum to null. The datagen's quirks are
  kept: the n-hop tables repeat person 1's dates as person 2's, and a
  person is two hops from itself. The 2- and 4-hop tables sample with
  Spark's `XORShiftRandom` (`sample(f, 42)`), checked against Spark's own
  class.

### Chunks

An activity block is one stream of draws, person after person. A chunk is
1,000 persons of a block, started from the state (the farm's 72 seeds and
two id counters) its block reaches at the chunk's first person. That
state is found by a cheaper pass over the block's earlier persons that
makes the same draws without building rows or text (about 40% of the
generation's cost). It reads packed lengths and bitsets rather than
dictionary text, and it skips the author lookups that only the output
uses. With at least as many blocks as threads, every
chunk's state is found up front, blocks in parallel; with fewer, as
chunks need them, one pass per block overlapping the chunks it unblocks.
So a block's chunks run in parallel, and a chunk is ~200 MB of CSV rather
than a whole block's ~2 GB.

### Tested

- All 18 tables at SF 0.1 and SF 1 are byte-identical to the Spark
  datagen's CSV (`--mode raw --format csv`).
- At SF 3 and SF 10 (166 million rows) all 18 tables are value-identical,
  row by row, to the datagen's raw Parquet (`--format parquet`) part files
  in partition order. The datagen's CSV at these sizes holds the same rows,
  but its row order is a Spark artefact: it converts the raw Parquet to CSV
  with Spark's file scan, which orders files by size (and splits or packs
  them), so blocks come out in order of their Parquet files' compressed
  size.
- BI mode is byte-identical to the datagen's on every snapshot table and
  every insert and delete batch, at SF 0.1 (day, hour and month batches)
  and SF 1 (day).
- The exploded layouts with epoch milliseconds (36 tables, raw and BI)
  are byte-identical at SF 0.1.
- All 34 factor tables are identical to the datagen's at SF 0.1, as CSV
  and as Parquet values. Three have no defined row order (a `distinct`
  and two connected-component tables) and are compared as sets. At SF 1,
  33 of 34 match. The exception is `people2Hops`: its ~2 million pairs are
  identical, but Spark samples them over four partitions whose boundaries
  come from its shuffle sizes, so a different ~10,000 rows are kept. The
  reference's rows are exactly that sample over those partitions.
- `tests/output.rs`: the same bytes for any thread count, with chunk
  states found up front or lazily, for chunks generated in any order, and
  (with `LDBC_REFERENCE_SF0_1=<datagen output dir>`) byte-equality with a
  Spark SF 0.1 run.
- `tests/modes.rs`: BI output and factor tables are the same for any
  thread count. Given datagen output directories
  (`LDBC_REFERENCE_BI_SF0_1`, `LDBC_REFERENCE_LAYOUT_SF0_1` and
  `LDBC_REFERENCE_FACTORS_SF0_1`), it also checks byte-equality with
  Spark's SF 0.1 BI, exploded-layout and factor output.

## Speed

4 cores (the same machine for both), wall time including dictionary
loading:

| SF | persons | rows | Spark datagen (local[4], CSV + Parquet) | ldbc-gen CSV | ldbc-gen Parquet |
|---:|--------:|-----:|---------------------:|-------------:|-----------------:|
| 1 | 10,620 | 13.9 M | 85.3 s | 2.8 s (30x) | 2.2 s (39x) |
| 3 | 25,870 | 42.1 M | 114.3 s | 9.0 s (13x) | 5.6 s (20x) |
| 10 | 70,800 | 142.0 M | 270.3 s | 28.9 s (9.4x) | 15.9 s (17x) |

The Spark datagen writes raw Parquet and then converts it to CSV in the same
run; its time is for both (`--mode raw --format csv`, Spark 3.2.1, Java 11).
Peak memory of `ldbc-gen` at SF 10: 5.5 GB for CSV, 2.3 GB for Parquet.

Factor tables add 0.7 s at SF 1 (`--generate-factors --factor-format csv`
with the raw CSV: 3.4 s in all). The datagen takes 379 s for the same
run, against 85 s without factors.

`generator/ldbc/scripts/compare-parquet.py REF_OUT MINE_DIR` (needs `pyarrow`) is the
value-by-value comparison with a datagen run's raw Parquet.
