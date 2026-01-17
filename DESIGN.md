# Sampling Combinators for Columnar Data

A trait-based system for generating test data with composable distributions.

## Core Design

The design is built around infinite iterators that produce values on demand. Distributions produce forever; consumers decide when to stop.

| Layer | Trait | Responsibility |
|-------|-------|----------------|
| Iteration | `SampleIter` | Pull-based value generation with RNG |
| Distribution | `Distribution<T>` | Factory for sample iterators |
| Composition | Combinators | Wrap iterators, zero intermediate allocation |
| Collection | `collect`, `collect_column`, etc. | Materialize into row or column format |

This separation means:
- Distributions are factories for infinite iterators
- Combinators wrap iterators, not values
- Row vs column is a collection concern, not a generation concern
- Same iterator, different materialization

## Core Traits

### `SampleIter`

The fundamental primitive - an infinite iterator that takes RNG on each `next()` call:

```rust
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}
```

Unlike `std::iter::Iterator`:
- RNG is passed to `next()` rather than captured
- Always returns a value (no `Option`) - streams are infinite
- Zero allocation between combinators
- Stateful iteration (for `Runs`, `Monotonic`, etc.)

### `Distribution<T>`

A factory that produces sample iterators:

```rust
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;

    fn sample(&mut self) -> Self::Iter;
}
```

Single method, minimal interface. No length parameter - iterators are infinite.

### Collection

Collectors decide how many values to take and how to materialize:

```rust
// Scalar collection: take n values
fn collect<I: SampleIter>(mut iter: I, rng: &mut impl Rng, n: usize) -> Vec<I::Item> {
    (0..n).map(|_| iter.next(rng)).collect()
}

// Row: single value
let value = dist.sample().next(&mut rng);

// Column: n values
let column: Vec<T> = collect(dist.sample(), &mut rng, 1000);
```

### Tuple Composition

Tuples of distributions implement `Distribution`, producing tuple iterators:

```rust
impl<D1, D2, T1, T2> Distribution<(T1, T2)> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Iter = TupleIter2<D1::Iter, D2::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter2(self.0.sample(), self.1.sample())
    }
}

struct TupleIter2<I1, I2>(I1, I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
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

Combinators wrap iterators, not values. No intermediate allocation:

| Combinator | Transform | Description |
|------------|-----------|-------------|
| `WithNulls<D>` | `T` → `Option<T>` | Adds null probability |
| `Runs<D>` | `T` → `T` | Run-length friendly data (stateful iterator) |
| `Map<D, F>` | `T` → `U` | Transform output element-wise |
| `Monotonic<D>` | `i64` → `i64` | Increasing values (accumulates deltas) |
| `List<L, E>` | `(usize, T)` → lists | Variable-length lists |
| `(D1, D2, ...)` | → `(T1, T2, ...)` | Tuple composition (built-in) |

### How Combinators Wrap Iterators

```rust
// Map wraps the inner iterator, applies F on each next()
struct MapIter<I, F> {
    inner: I,
    f: F,
}

impl<I: SampleIter, F: Fn(I::Item) -> U, U> SampleIter for MapIter<I, F> {
    type Item = U;

    fn next(&mut self, rng: &mut impl Rng) -> U {
        (self.f)(self.inner.next(rng))
    }
}

// Runs maintains state across next() calls
struct RunsIter<I: SampleIter> where I::Item: Copy {
    inner: I,
    current: Option<I::Item>,
    repeat_prob: f64,
}

impl<I: SampleIter> SampleIter for RunsIter<I> where I::Item: Copy {
    type Item = I::Item;

    fn next(&mut self, rng: &mut impl Rng) -> I::Item {
        if let Some(val) = self.current {
            if rng.gen::<f64>() < self.repeat_prob {
                return val;
            }
        }
        let val = self.inner.next(rng);
        self.current = Some(val);
        val
    }
}
```

## List Combinator

Lists combine a length distribution with an element distribution:

```rust
struct List<L, E> {
    lengths: L,   // Distribution<usize>
    elements: E,  // Distribution<T>
}

struct ListIter<L, E> {
    lengths: L,   // SampleIter<Item = usize>
    elements: E,  // SampleIter<Item = T>
}

impl<L: Distribution<usize>, E: Distribution<T>> Distribution<???> for List<L, E> {
    type Iter = ListIter<L::Iter, E::Iter>;

    fn sample(&mut self) -> Self::Iter {
        ListIter {
            lengths: self.lengths.sample(),
            elements: self.elements.sample(),
        }
    }
}
```

The `ListIter` exposes two infinite streams. Collection determines materialization:

```rust
// Column: flat values + offsets
fn collect_list_column<L, E>(
    mut iter: ListIter<L, E>,
    rng: &mut impl Rng,
    n: usize,
) -> (Vec<E::Item>, Vec<usize>)
where
    L: SampleIter<Item = usize>,
    E: SampleIter,
{
    let mut values = Vec::new();
    let mut offsets = vec![0];
    for _ in 0..n {
        let len = iter.lengths.next(rng);
        for _ in 0..len {
            values.push(iter.elements.next(rng));
        }
        offsets.push(values.len());
    }
    (values, offsets)
}

// Row: Vec<Vec<T>>
fn collect_list_row<L, E>(
    mut iter: ListIter<L, E>,
    rng: &mut impl Rng,
    n: usize,
) -> Vec<Vec<E::Item>>
where
    L: SampleIter<Item = usize>,
    E: SampleIter,
{
    (0..n).map(|_| {
        let len = iter.lengths.next(rng);
        (0..len).map(|_| iter.elements.next(rng)).collect()
    }).collect()
}
```

Same combinator, same iterator, different collector.

## Usage Examples

```rust
let mut dist = (
    Runs::new(Range::new(1u64, 10_000), 0.1),  // user_id (clustered)
    Weighted::new(
        vec!["click", "view", "purchase"],
        vec![0.5, 0.3, 0.2],
    ),
    WithNulls::new(Range::new(1i32, 10000), 0.5),  // amount (nullable)
);

let mut rng = StdRng::seed_from_u64(42);

// Row iteration - take values one at a time
let mut iter = dist.sample();
for _ in 0..10_000 {
    let (user_id, event, amount) = iter.next(&mut rng);
    process_row(user_id, event, amount);
}

// Column collection - take 10,000 rows
let rows: Vec<(u64, &str, Option<i32>)> = collect(dist.sample(), &mut rng, 10_000);
```

### Derived columns

```rust
// extended_price = qty * price
let mut dist = (Range::new(1i32, 100), Range::new(100i64, 1000))
    .map(|(qty, price)| (qty, price, qty as i64 * price));

// Works the same - Map wraps the tuple iterator
let data: Vec<(i32, i64, i64)> = collect(dist.sample(), &mut rng, 1000);
```

### Lists

```rust
// Lists of random integers
let mut dist = List::new(
    Range::new(1usize, 10),   // 1-10 elements per list
    Range::new(0i32, 100),    // element values
);

let mut rng = StdRng::seed_from_u64(42);

// Column format: (values, offsets)
let (values, offsets) = collect_list_column(dist.sample(), &mut rng, 1000);

// Row format: Vec<Vec<i32>>
let rows = collect_list_row(dist.sample(), &mut rng, 1000);
```

## Key Properties

- **Deterministic**: Same seed produces same output
- **Infinite streams**: Distributions produce forever, consumers stop when satisfied
- **Zero intermediate allocation**: Combinators wrap iterators, not collected values
- **Lazy**: Values generated on `next()`, not upfront
- **Composable**: Combinators stack arbitrarily deep
- **Stateful**: Iterator holds state for `Runs`, `Monotonic`, etc.
- **Unified**: Row and column are collection strategies, not separate implementations

## See Also

- [EXAMPLES.md](EXAMPLES.md) - Real world examples (TPC-H tables)
- [FUTURE.md](FUTURE.md) - Planned features and complex patterns
