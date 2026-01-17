# Sampling Combinators for Columnar Data

A trait-based system for generating test data with composable distributions.

## Core Design

The design separates three orthogonal concerns:

| Layer | Trait | Responsibility |
|-------|-------|----------------|
| Generation | `Distribution<T>` | Produce values of type `T` |
| Composition | Combinators | Transform/wrap distributions |
| Materialization | `Rows` / `Columns` | Pack values into row or columnar format |

This separation means:
- Distributions know nothing about storage
- Storage formats know nothing about statistics
- Combinators work with any distribution AND any storage format
- Same definition, different materialization (rows vs columns)

## Core Traits

### `Distribution<T>`

```rust
pub trait Distribution<T>: Sized {
    fn sample(&mut self, rng: &mut impl Rng) -> T;

    fn map<U, F: Fn(T) -> U>(self, f: F) -> Map<Self, F, T> {
        Map::new(self, f)
    }
}

// Tuples of distributions implement Distribution
impl<D1, D2, T1, T2> Distribution<(T1, T2)> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    fn sample(&mut self, rng: &mut impl Rng) -> (T1, T2) {
        (self.0.sample(rng), self.1.sample(rng))
    }
}
// ... impl for tuples up to 12 via macro
```

## Base Distributions

| Distribution | Parameters | Description |
|--------------|------------|-------------|
| `Bernoulli` | `true_prob: f64` | Biased boolean |
| `Range<T>` | `min, max` | Bounded uniform `[min, max)` |
| `OneOf<T>` | `values: Vec<T>` | Uniform from fixed set |
| `Constant<T>` | `value: T` | Always same value |
| `Weighted<T>` | `values, weights` | Non-uniform selection |

## Combinators

| Combinator | Transform | Description |
|------------|-----------|-------------|
| `WithNulls<D>` | `T` → `Option<T>` | Adds null probability |
| `Runs<D>` | `T` → `T` | Run-length friendly data (requires `T: Copy`) |
| `Map<D, F>` | `T` → `U` | Transform output |
| `Monotonic<D>` | `i64` → `i64` | Increasing values (accumulates deltas) |
| `(D1, D2, ...)` | → `(T1, T2, ...)` | Tuple composition (built-in) |

## Materialization

Define data once, materialize as rows or columns:

### `Rows::generate` - Row-oriented output

```rust
pub struct Rows;

impl Rows {
    pub fn generate<T, D: Distribution<T>>(
        dist: &mut D,
        rng: &mut impl Rng,
        len: usize,
    ) -> Vec<T> {
        (0..len).map(|_| dist.sample(rng)).collect()
    }
}
```

### `Columns::generate` - Column-oriented output

```rust
pub struct Columns;

// Fast path: independent tuple of distributions (column-at-a-time)
impl<D1, D2, D3, T1, T2, T3> GenerateColumns<(Vec<T1>, Vec<T2>, Vec<T3>)>
    for (D1, D2, D3)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    fn generate_columns(&mut self, rng: &mut impl Rng, len: usize)
        -> (Vec<T1>, Vec<T2>, Vec<T3>)
    {
        let c1 = (0..len).map(|_| self.0.sample(rng)).collect();
        let c2 = (0..len).map(|_| self.1.sample(rng)).collect();
        let c3 = (0..len).map(|_| self.2.sample(rng)).collect();
        (c1, c2, c3)
    }
}

// Slow path: dependent columns (via Map) - rows then transpose
impl Columns {
    pub fn from_rows_3<D, T1, T2, T3>(
        dist: &mut D,
        rng: &mut impl Rng,
        len: usize,
    ) -> (Vec<T1>, Vec<T2>, Vec<T3>)
    where
        D: Distribution<(T1, T2, T3)>,
    {
        let rows: Vec<_> = Rows::generate(dist, rng, len);
        // Transpose to columns...
    }
}
```

### When each path is used

| Distribution | Path | Performance |
|--------------|------|-------------|
| `(D1, D2, D3)` tuple | `Columns::generate` (fast) | Column-at-a-time |
| `Map<D, F>` → tuple | `Columns::from_rows_N` (slow) | Row then transpose |

## Usage Examples

```rust
// Define once
let mut dist = (
    Runs::new(Range::new(1u64, 10_000), 0.1),  // user_id (clustered)
    Weighted::new(
        vec!["click", "view", "purchase"],
        vec![0.5, 0.3, 0.2],
    ),
    WithNulls::new(Range::new(1i32, 10000), 0.5),  // amount (nullable)
);

let mut rng = StdRng::seed_from_u64(42);

// Materialize as rows
let rows: Vec<(u64, &str, Option<i32>)> = Rows::generate(&mut dist, &mut rng, 10_000);

// Materialize as columns (same definition!)
let (user_ids, events, amounts): (Vec<u64>, Vec<&str>, Vec<Option<i32>>) =
    Columns::generate(&mut dist, &mut rng, 10_000);
```

### Dependent columns (derived values)

```rust
// extended_price = qty * price
let mut dist = (Range::new(1i32, 100), Range::new(100i64, 1000))
    .map(|(qty, price)| (qty, price, qty as i64 * price));

// Must use from_rows since columns depend on each other
let (qty, price, extended): (Vec<i32>, Vec<i64>, Vec<i64>) =
    Columns::from_rows_3(&mut dist, &mut rng, 1000);
```

## Key Properties

- **Deterministic**: Same seed produces same output
- **Zero coupling**: Only depends on `rand`, not vortex types
- **Composable**: Combinators stack arbitrarily deep
- **Extensible**: New distributions and storage formats added independently
- **Stateful**: `&mut self` enables combinators like `Runs` and `Monotonic`
- **Dual materialization**: Same definition → rows or columns

## See Also

- [EXAMPLES.md](EXAMPLES.md) - Real world examples (TPC-H tables)
- [FUTURE.md](FUTURE.md) - Planned features and complex patterns
