# populate

Composable distribution combinators for generating tabular test data,
column by column, straight into Arrow (and optionally Parquet).

```rust
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{constant, list, map, range};

// id: i64, tags: list<i64> with lengths cycling 0..8
let mut table = (
    range(0i64, 1),
    list(map(range(0usize, 1), |i| i % 8), range(0i64, 1)),
);
let batch = DistributionArrow::<ArrowBackend>::collect_record_batch(&mut table, &mut rand::rng(), 1_000)?;
```

With `ArrowBackend` every numeric column, including list offsets and values,
is generated into one exactly-sized buffer that becomes the Arrow array
without a copy. The optional `parquet` feature writes those buffers to Parquet
in place (`collect_parquet`).

## Crates

| crate                   | contents                                            |
|-------------------------|-----------------------------------------------------|
| `populate`              | facade: re-exports everything below                 |
| `populate-core`         | `Distribution`, `SampleIter`, `Sink`, `Backend`, HLists, tuples |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `derive`, `flatten` |
| `populate-sink-arrow`   | `ArrowBackend`, `RecordBatch` output                |
| `populate-sink-parquet` | zero-copy Parquet writer (`parquet` feature)        |

See `populate/DESIGN.md` for the design.
