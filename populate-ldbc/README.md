# populate-ldbc

LDBC SNB (Social Network Benchmark) data, identical to the
[LDBC Spark datagen](https://github.com/ldbc/ldbc_snb_datagen_spark)'s
`--mode raw` output, on all cores: the 14 dynamic tables (persons,
friendships, forums, posts, comments, tags, likes, ...) and the 4 static
ones, as CSV, Arrow `RecordBatch`es or Parquet.

```sh
# the datagen's dictionaries (~72 MB) into target/ldbc_snb_datagen_spark
scripts/fetch-ldbc-resources.sh

# SF 1 as the datagen's CSV, in ./ldbc-out/{dynamic,static}/<Table>.csv
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 1 --out ldbc-out

# Parquet, one file per table; or generate only (counts rows)
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 10 --format parquet
cargo run -p populate-ldbc --release --bin ldbc-gen -- --sf 10 --format none
```

Flags: `--sf` (the datagen's scale factors: 0.003, 0.1, 0.3, 1, 3, 10, 30,
100, ...) or `--persons N`, `--resources DIR` (default
`$LDBC_DATAGEN_RESOURCES`, then the fetch script's directory), `--out`,
`--format csv|parquet|none`, `--threads` (default all cores).

From Rust:

```rust
use populate_ldbc::{Ldbc, default_resources, persons_for_scale_factor};

let g = Ldbc::new(&default_resources().unwrap(), persons_for_scale_factor("1").unwrap(), 0)?;
// every dynamic table as Parquet, one shared pool
populate_table::write_parquet_many(&[&g], files, 0, None)?;
// or the datagen's CSV text
populate_table::text::write_text_many(&[&g.csv()], files, 0)?;
```

`Ldbc` is a `populate_table::MultiChunks` source (one output per table in
`rows::DYNAMIC`) and `g.csv()` a `TextChunks` one, so they share a pool with
other generators' tables.

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

### Chunks

An activity block is one stream of draws, person after person. A chunk is
1,000 persons of a block, started from the state (the farm's 72 seeds and
two id counters) its block reaches at the chunk's first person. That
state is found by a cheaper pass over the block's earlier persons that
makes the same draws without building rows or text (about 60% of the
generation's cost). With at least as many blocks as threads, every
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
- `tests/output.rs`: the same bytes for any thread count, with chunk
  states found up front or lazily, for chunks generated in any order, and
  (with `LDBC_REFERENCE_SF0_1=<datagen output dir>`) byte-equality with a
  Spark SF 0.1 run.

## Speed

4 cores (the same machine for both), wall time including dictionary
loading:

| SF | persons | rows | Spark datagen (local[4], CSV + Parquet) | ldbc-gen CSV | ldbc-gen Parquet |
|---:|--------:|-----:|---------------------:|-------------:|-----------------:|
| 1 | 10,620 | 13.9 M | 85.3 s | 3.4 s (25x) | 2.7 s (32x) |
| 3 | 25,870 | 42.1 M | 114.3 s | 10.3 s (11x) | 6.6 s (17x) |
| 10 | 70,800 | 142.0 M | 270.3 s | 28.6 s (9.5x) | 18.3 s (15x) |

The Spark datagen writes raw Parquet and then converts it to CSV in the same
run; its time is for both (`--mode raw --format csv`, Spark 3.2.1, Java 11).
Peak memory of `ldbc-gen` at SF 10: 5.5 GB for CSV, 2.3 GB for Parquet.

`populate-ldbc/scripts/compare-parquet.py REF_OUT MINE_DIR` (needs `pyarrow`) is the
value-by-value comparison with a datagen run's raw Parquet.
