# populate

Composable distribution combinators for generating test data, designed for columnar data structures.

Small distribution primitives are combined into larger ones that describe a whole table. Collection is
column by column: each column is generated in full into its own buffer before the next, and with
`ArrowBackend` those buffers are moved into Arrow arrays without copying.

## Example

```rust
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{constant, list, range};

// id: i64, tags: list<i64> (every row has 3 tags)
let mut dist = (range(0i64, 1), list(constant(3usize), range(0i64, 1)));

let batch = DistributionArrow::<ArrowBackend>::collect_record_batch(&mut dist, &mut rand::rng(), 1_000).unwrap();
assert_eq!(batch.num_rows(), 1_000);
```

## Crates

| crate                   | contents                                                       |
|-------------------------|----------------------------------------------------------------|
| `populate`              | facade: re-exports everything below (`arrow` default, `parquet` optional features) |
| `populate-core`         | `Distribution`, `SampleIter`, `Sink`, `Backend`, HLists, tuple combinators |
| `populate-builtins`     | `constant`, `range`, `map`, `list`, `flatten`                  |
| `populate-sink-arrow`   | `ArrowBackend`, `RecordBatch` conversion                       |
| `populate-sink-parquet` | zero-copy Parquet writer                                       |

`populate/tests/zero_copy.rs` checks that generation really is columnar and zero-copy: it counts
allocations during generation and checks that Arrow (and Parquet) output reuses the generated buffers.

## License

MIT

## Documentation

- [populate/DESIGN.md](populate/DESIGN.md) - Core design
