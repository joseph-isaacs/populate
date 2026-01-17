# Sampling Combinators for Columnar Data

A trait-based system for generating test data with composable distributions.

## Core Design

The design separates three orthogonal concerns:

| Layer | Trait | Responsibility |
|-------|-------|----------------|
| Generation | `Distribution<T>` | Produce values of type `T` |
| Composition | Combinators | Transform/wrap distributions |
| Materialization | `FromDistribution<T, D>` | Pack values into storage format |

This separation means:
- Distributions know nothing about storage
- Storage formats know nothing about statistics
- Combinators work with any distribution AND any storage format

## Core Traits

### `Distribution<T>`

```rust
pub trait Distribution<T>: Sized {
    fn sample(&mut self, rng: &mut impl Rng) -> T;

    fn map<U, F: Fn(T) -> U>(self, f: F) -> Map<Self, F> {
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

### `FromDistribution<T, D>`

```rust
pub trait FromDistribution<T, D: Distribution<T>>: Sized {
    fn from_distribution(dist: &mut D, rng: &mut impl Rng, len: usize) -> Self;
}

// Nullable types handled via bounds, e.g.:
// impl<T, D: Distribution<Option<T>>> FromDistribution<Option<T>, D> for MyColumn<T>
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
| `Runs<D>` | `T` → `T` | Run-length friendly data |
| `Map<D, F>` | `T` → `U` | Transform output |
| `Repeat<D>` | `T` → `Vec<T>` | Fixed-length collection |
| `RepeatN<D, L>` | `T` → `Vec<T>` | Variable-length collection |
| `Monotonic<D>` | `T` → `T` | Increasing values |
| `(D1, D2, ...)` | → `(T1, T2, ...)` | Tuple composition (built-in) |

## Composition Examples

```rust
// Combinators via nesting
let dist = WithNulls::new(Range::new(0, 100), 0.2);
let dist = Runs::new(OneOf::new(vec![1, 2, 3]), 0.1);
let dist = Range::new(0, 100).map(|x| x * 2);

// Tuples (no wrapper needed)
let mut row = (
    Range::new(0, 100),
    Range::new(0.0, 1.0),
    Bernoulli::new(0.5),
);
let (i, f, b): (i32, f64, bool) = row.sample(&mut rng);

// Collect into Vec
let mut rows = Repeat::new(
    (
        Runs::new(Range::new(1u64, 10_000), 0.1),
        Weighted::new(
            vec!["click", "view", "purchase"],
            vec![0.5, 0.3, 0.2],
        ),
        WithNulls::new(Range::new(1i32, 10000), 0.85),
    ),
    10_000,
);
let data: Vec<(u64, &str, Option<i32>)> = rows.sample(&mut rng);
```

## Key Properties

- **Deterministic**: Same seed produces same output
- **Zero coupling**: Only depends on `rand`, not vortex types
- **Composable**: Combinators stack arbitrarily deep
- **Extensible**: New distributions and storage formats added independently
- **Stateful**: `&mut self` enables combinators like `Runs` and `Monotonic`

## See Also

- [EXAMPLES.md](EXAMPLES.md) - Real world examples (TPC-H tables)
- [FUTURE.md](FUTURE.md) - Planned features and complex patterns
