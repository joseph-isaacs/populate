# Future Work

## Complex Boolean Patterns

Beyond the basic `Bernoulli` + `Runs`, these patterns could be useful:

| Pattern | Description | Use case |
|---------|-------------|----------|
| `Periodic<N>` | Repeat every N: `[T,T,F,T,T,F,...]` | Predictable patterns |
| `Burst<D>` | Mostly one value, occasional bursts of other | Sparse events, validity masks |
| `Gradient` | Probability changes over sequence (0%→100%) | Testing boundary conditions |
| `Markov` | P(next) depends on current value | Realistic clustering |
| `Segments` | Different densities per segment | Mixed workloads |
| `ZipfRuns` | Run lengths follow Zipf (few long, many short) | Realistic run distributions |

### Examples

```rust
// Bursts: 95% false, but true values come in clusters of 5-10
Burst::new(false, 0.05, Range::new(5, 10))

// Gradient: starts 0% true, ends 100% true
Gradient::new(0.0, 1.0)

// Markov: 90% chance to stay same, 10% to flip
Markov::new(0.9)

// Segments: 3 segments with different densities
Segments::new(vec![
    (0.1, 1000),  // 10% true for 1000 values
    (0.9, 1000),  // 90% true for 1000 values
    (0.5, 1000),  // 50% true for 1000 values
])
```

## Nested Tuple Support

Currently, `Columns::generate` only works with flat tuples. Nested tuples like `(D1, (D2, D3))` require additional support.

### Current Limitation

```rust
let mut dist = (
    Range::new(0i32, 100),
    (Bernoulli::new(0.5), Range::new(0.0f64, 1.0)),
);

// Rows: works ✓
let rows: Vec<(i32, (bool, f64))> = Rows::generate(&mut dist, &mut rng, 100);

// Columns: needs specific impl - does NOT work yet
let cols: (Vec<i32>, (Vec<bool>, Vec<f64>)) = Columns::generate(&mut dist, ...); // ✗
```

### Proposed Solution: Recursive Flattening Trait

```rust
// Trait that knows how to flatten to columns
trait IntoColumns {
    type Columns;  // The flattened column types

    fn into_columns(rows: Vec<Self>) -> Self::Columns;
}

// Base case: primitives stay as Vec
impl IntoColumns for i32 {
    type Columns = Vec<i32>;

    fn into_columns(rows: Vec<Self>) -> Self::Columns {
        rows  // Already a Vec
    }
}

// Recursive case: tuples flatten their elements
impl<A, B> IntoColumns for (A, B)
where
    A: IntoColumns,
    B: IntoColumns,
{
    type Columns = (A::Columns, B::Columns);

    fn into_columns(rows: Vec<Self>) -> Self::Columns {
        let (a_rows, b_rows): (Vec<A>, Vec<B>) = rows.into_iter().unzip();
        (A::into_columns(a_rows), B::into_columns(b_rows))
    }
}
```

### How It Flattens

```rust
// (i32, (bool, f64))
// A = i32       → Columns = Vec<i32>
// B = (bool, f64)
//     B::A = bool  → Columns = Vec<bool>
//     B::B = f64   → Columns = Vec<f64>
//     B::Columns = (Vec<bool>, Vec<f64>)
// Final: (Vec<i32>, (Vec<bool>, Vec<f64>))
```

### Full Flattening (Optional)

For fully flat output `(Vec<i32>, Vec<bool>, Vec<f64>)` instead of nested `(Vec<i32>, (Vec<bool>, Vec<f64>))`:

```rust
trait FlatColumns {
    type Flat;  // Fully flattened tuple of Vecs
}

impl FlatColumns for Vec<i32> {
    type Flat = Vec<i32>;
}

impl<A, B> FlatColumns for (A, B)
where
    A: FlatColumns,
    B: FlatColumns,
    (A::Flat, B::Flat): ConcatTuples,  // Trait to merge tuples
{
    type Flat = <(A::Flat, B::Flat) as ConcatTuples>::Output;
}
```

The `ConcatTuples` trait requires either:
- Macros for all combinations
- Nightly features (variadic generics)
- A crate like `frunk` for HLists

### Simpler Alternative

Generate more tuple impls via macro for common nested patterns:

```rust
impl GenerateColumns for (D1, (D2, D3)) { ... }
impl GenerateColumns for ((D1, D2), D3) { ... }
impl GenerateColumns for (D1, (D2, (D3, D4))) { ... }
// etc.
```

Limited depth but covers practical cases.

## Additional Future Features

- **FlatMap combinator** - Output determines next distribution
- **ForeignKey combinator** - Reference values from another generated column
- **Grammar-based text** - Template-driven string generation (like TPC-H comments)
- **Arrow/Parquet integration** - Direct `FromDistribution` impls for Arrow arrays
