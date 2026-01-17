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
pub trait Distribution<T> {
    fn sample(&self, rng: &mut impl Rng) -> T;
}
```

### `FromDistribution<T, D>`

```rust
pub trait FromDistribution<T, D: Distribution<T>>: Sized {
    fn from_distribution(dist: &D, rng: &mut impl Rng, len: usize) -> Self;
}

// Nullable types handled via bounds, e.g.:
// impl<T, D: Distribution<Option<T>>> FromDistribution<Option<T>, D> for MyColumn<T>
```

## Base Distributions

| Distribution | Parameters | Description |
|--------------|------------|-------------|
| `Uniform` | none | Full-range uniform random |
| `Bernoulli` | `true_prob: f64` | Biased boolean |
| `Range<T>` | `min, max` | Bounded uniform `[min, max)` |
| `OneOf<T>` | `values: Vec<T>` | Uniform from fixed set |
| `Constant<T>` | `value: T` | Always same value |
| `Normal` | `mean, std_dev` | Gaussian distribution |
| `Exponential` | `lambda` | Exponential distribution |
| `Weighted<T>` | `values, weights` | Non-uniform selection |

## Combinators

| Combinator | Transform | Description |
|------------|-----------|-------------|
| `WithNulls<D>` | `T` → `Option<T>` | Adds null probability |
| `Runs<D>` | `T` → `T` | Run-length friendly data |
| `Map<D, F>` | `T` → `U` | Transform output |
| `Zip<D1, D2>` | → `(T1, T2)` | Pair values |
| `Either<D1, D2, B>` | → `T` | Conditional selection |
| `Tuple2..5` | → `(T0, T1, ...)` | Struct-like composition |
| `ListDist<D, L>` | → `Vec<T>` | Variable-length lists |
| `StringDist<L>` | → `String` | Random strings |
| `Monotonic<D>` | → `i64` | Increasing values |
| `RandomWalk<D>` | → `f64` | Cumulative deltas |

## Composition Examples

```rust
// Combinators via nesting
let dist = WithNulls::new(Range::new(0, 100), 0.2);
let dist = Runs::new(OneOf::new(vec![1, 2, 3]), 0.1);
let dist = Map::new(Range::new(0, 100), |x| x * 2);

// Struct-like
Zip::new(Range::new(0, 100), Range::new(0.0, 1.0))

// Lists
ListDist::new(Range::new(0i32, 100), Range::new(1usize, 10))

// Strings
StringDist::alphanumeric(Range::new(5usize, 15))
```

## Custom Type Integration

```rust
struct MyColumn<T> { data: Vec<T> }

impl<T, D: Distribution<T>> FromDistribution<T, D> for MyColumn<T> {
    fn from_distribution(dist: &D, rng: &mut impl Rng, len: usize) -> Self {
        MyColumn { data: (0..len).map(|_| dist.sample(rng)).collect() }
    }
}

// Works with any distribution
let mut rng = StdRng::seed_from_u64(42);
MyColumn::from_distribution(&Range::new(0u8, 100), &mut rng, 1000)
```

## Key Properties

- **Deterministic**: Same seed produces same output
- **Zero coupling**: Only depends on `rand`, not vortex types
- **Composable**: Combinators stack arbitrarily deep
- **Extensible**: New distributions and storage formats added independently
