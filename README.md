# populate

Composable distribution combinators for generating test data, designed for columnar data structures.

Inspired by parser combinators (like nom), populate provides small, focused distribution primitives that can be combined to generate complex, realistic datasets with controllable statistical properties—from simple uniform ranges to clustered runs, nullable columns, and weighted selections.

## Example

```rust
use populate::{Range, OneOf, Constant};

let mut dist = (
    Range::new(0i32, 100),           // id
    OneOf::new(vec!["a", "b", "c"]), // category
    Constant::new(true),             // active
);

let (ids, categories, actives): (Vec<_>, Vec<_>, Vec<_>) =
    dist.sample().take(1000).collect();
```

## Arrow Integration

Generate Arrow `RecordBatch` directly:

```rust
use populate::{Range, Bernoulli};

let mut dist = (
    Range::new(0i64, 1_000_000),  // id
    Range::new(0.0f64, 100.0),    // price
    Bernoulli::new(0.9),          // active
);

let batch: RecordBatch = dist.collect(&mut rng, 10_000);
// batch.num_rows() == 10_000
// batch.num_columns() == 3
```

## License

MIT

## Documentation

- [DESIGN.md](DESIGN.md) - Core design and API
- [EXAMPLES.md](EXAMPLES.md) - Real world examples (TPC-H tables)
- [FUTURE.md](FUTURE.md) - Planned features
