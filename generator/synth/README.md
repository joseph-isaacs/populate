# populate-synth

Synthetic tables shaped like data people benchmark on, on
`populate-table`'s `Chunks` engine: Arrow `RecordBatch`es or Parquet, on all
cores, deterministic. Each shape is one populate `Distribution`, made a
`DistTable`, its Arrow schema read off the distribution's names and column
types. There is no reference generator to match, so these are
examples and fixtures, not benchmarks.

| Shape | Looks like | Columns |
|---|---|---|
| `Taxi` | NYC TLC yellow-cab trips | 19: sorted pickup times with an hourly profile, log-normal distances, fares, tips and totals that add up, correlated missing fields |
| `Hits` | ClickBench `hits` | 30 of its 105, same names and types: skewed sites and users, per-user devices, generated URLs, referers and search phrases |
| `Features` | an ML feature table | id, time, split, 32 `Float32` features (normal, log-normal, correlated, uniform; 2% NULL), 8 Zipf categoricals, a learnable label and target |
| `Embeddings` | a vector-search corpus | id, cluster, `FixedSizeList<Float32>[384]`: clustered, unit-length vectors |

```sh
cargo run -p populate-synth --release --bin synth-gen -- taxi --rows 10000000 --out taxi.parquet
cargo run -p populate-synth --release --bin synth-gen -- embeddings --rows 1000000 --dim 768
```

Flags: `--rows`, `--seed`, `--threads` (default all cores), `--batch-size`,
`--chunk-rows` (rows per row group), `--dim` (embeddings: 16, 32, 64, 128,
256, 384, 512, 768, 1024, 1536, 2048, 3072 or 4096), `--out`. Without
`--out` the batches are generated and dropped, to time generation alone.

```rust
use populate_synth::{Hits, Shape};

let hits = Hits::new(42).table(100_000_000);
hits.for_each_batch(0, |batch| { /* in order */ });
hits.write_parquet(std::fs::File::create("hits.parquet")?, 0, None)?;
```

## Determinism

Every value is a pure function of `(seed, column, row)`: a `Stream` hashes
the row number to 64 bits, and a kernel turns the bits into a value with no
state (an inverse-CDF table lookup for continuous distributions, a
branch-free threshold count for weighted picks, a Zipf quantile table for
skewed ids). Values that must agree across rows, such as everything about a
user or a URL, hash the user or page instead of the row. So:

- any slice of rows generated on its own equals those rows of the whole
  table, and chunked, multi-threaded output equals one sequential run for
  any batch size, chunk size and thread count;
- Parquet files are byte-identical for any thread count (row groups are
  chunks, so the file layout, not the data, depends on `chunk_rows`);
- the same seed gives the same data, pinned in the tests by a hash of the
  values for seed 7.

`tests/determinism.rs` checks all of that, plus each shape's invariants
(sorted times, totals that add up, consistent per-user attributes,
unit-length vectors, cluster similarity, the label's dependence on the
features).

## Speed

`cargo bench -p populate-synth` (256Ki rows, 64Ki for embeddings, one
thread, 4-core cloud VM):

| Shape | Arrow | Parquet |
|---|---|---|
| taxi | 12.0 M rows/s | 3.4 M rows/s |
| hits | 6.0 M rows/s | 1.33 M rows/s |
| features | 3.7 M rows/s | 1.84 M rows/s |
| embeddings (384-d) | 0.87 M rows/s | 0.29 M rows/s |

The benches use the system allocator; `synth-gen` uses mimalloc (feature
`mimalloc`, on by default), which makes generation 20-65% faster here
because glibc hands each batch's memory back to the OS and faults it in
again. Taxi scales to 34.6 M rows/s on 4 threads. Columns whose values are
nearly all distinct within a chunk (times, ids, continuous features,
vectors) are written without a dictionary by default, and random floats
without compression (`Synth::parquet_props`), which is faster and smaller.
