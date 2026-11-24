# populate-synth

Synthetic tables shaped like data people benchmark on, on
`populate-table`'s `Chunks` engine: Arrow `RecordBatch`es or Parquet, on all
cores, deterministic. There is no reference generator to match, so these are
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
`--chunk-rows` (rows per row group), `--dim` (embeddings), `--out`. Without
`--out` the batches are generated and dropped, to time generation alone.

```rust
use populate_synth::{Hits, Synth};

let hits = Synth::new(Hits::new(42), 100_000_000);
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
thread, 4-core container):

| Shape | Arrow | Parquet |
|---|---|---|
| taxi | 12.4 M rows/s | 3.1 M rows/s |
| hits | 4.6 M rows/s | 1.16 M rows/s |
| features | 3.9 M rows/s | 1.36 M rows/s |
| embeddings (384-d) | 0.54 M rows/s | 0.19 M rows/s |

Taxi scales to 34.6 M rows/s on 4 threads. Columns whose values are
nearly all distinct within a chunk (times, ids, continuous features,
vectors) are written without a dictionary by default
(`Synth::parquet_props`), which is both faster and smaller.
