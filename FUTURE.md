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

## Type Erasure for Dynamic Schemas

**Open Question:** How to support heterogeneous collections of distributions for JSON, Arrow, etc.?

### The Problem

```rust
// Can't do this - different T in Distribution<T>
let columns: Vec<Box<dyn Distribution<?>>> = vec![
    Box::new(Range::new(0i32, 100)),     // Distribution<i32>
    Box::new(Bernoulli::new(0.5)),        // Distribution<bool>
    Box::new(OneOf::new(vec!["a", "b"])), // Distribution<&str>
];
```

### Option 1: Value Enum

```rust
enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Null,
}

trait DynDistribution {
    fn sample_dyn(&mut self, rng: &mut dyn Rng) -> Value;
}

// Blanket impl for any Distribution<T> where T converts to Value
impl<T: Into<Value>, D: Distribution<T>> DynDistribution for D {
    fn sample_dyn(&mut self, rng: &mut dyn Rng) -> Value {
        self.sample(rng).into()
    }
}

// Now heterogeneous collections work
let columns: Vec<Box<dyn DynDistribution>> = vec![
    Box::new(Range::new(0i64, 100)),
    Box::new(Bernoulli::new(0.5)),
];
```

### Option 2: Write to Trait Object (zero-copy for Arrow)

```rust
trait ColumnWriter {
    fn write_i64(&mut self, val: i64);
    fn write_bool(&mut self, val: bool);
    fn write_str(&mut self, val: &str);
    fn write_null(&mut self);
}

trait DynDistribution {
    fn sample_to(&mut self, rng: &mut dyn Rng, writer: &mut dyn ColumnWriter);
}

// Arrow impl
struct Int64Builder { inner: arrow::array::Int64Builder }
impl ColumnWriter for Int64Builder {
    fn write_i64(&mut self, val: i64) { self.inner.append_value(val); }
    // others panic - type mismatch
}
```

### Option 3: Schema-Driven Generation

```rust
enum ColumnDef {
    Int { min: i64, max: i64 },
    Bool { true_prob: f64 },
    String { values: Vec<String> },
    Nullable { inner: Box<ColumnDef>, null_prob: f64 },
}

struct Schema(Vec<(String, ColumnDef)>);

impl Schema {
    fn generate_json(&self, rng: &mut impl Rng, len: usize) -> Vec<serde_json::Value>;
    fn generate_arrow(&self, rng: &mut impl Rng, len: usize) -> RecordBatch;
}
```

### Tradeoffs

| Approach | Type safety | Performance | Flexibility |
|----------|-------------|-------------|-------------|
| Value enum | Runtime | Boxing overhead | High |
| ColumnWriter | Runtime | Zero-copy possible | Medium |
| Schema enum | Runtime | Good | Limited to enum variants |

**Decision needed:** Which approach best balances ergonomics, performance, and flexibility?

## Additional Future Features

- **FlatMap combinator** - Output determines next distribution
- **ForeignKey combinator** - Reference values from another generated column
- **Grammar-based text** - Template-driven string generation (like TPC-H comments)
- **Arrow/Parquet integration** - Direct `FromDistribution` impls for Arrow arrays

## Benchmark Generators

Ports of existing benchmark data generators onto populate combinators, in the
same spirit as tpchgen-rs over TPC-H `dbgen`: in-process, Arrow output, any
scale, seeded, and with the knobs the original can't express (skew, clustered
nulls, partial sortedness).

| # | Benchmark | Reference generator | Status |
|---|-----------|---------------------|--------|
| 1 | H2O.ai db-benchmark (groupby + join) | `groupby-datagen.R`, `join-datagen.R` (R + data.table) | `populate-h2o` |
| 2 | Star Schema Benchmark | `ssb-dbgen` (C fork of TPC-H dbgen) | planned |
| 3 | TSBS (Time Series Benchmark Suite) | `tsbs_generate_data` (Go) | `populate-tsbs` |
| 4 | YCSB | YCSB core workloads (Java) | planned |

Shared building blocks (in populate-2, used by both ports):

| module | what | used by |
|--------|------|---------|
| `sampler` | `Sampler` dialects: `FastSampler`, `RSampler` + `RRng` (R), `GoSampler` + `GoRand` (Go) | both |
| `dist::Leaf` | any clonable `SampleIter` as a column | both |
| `random` | `choose`, `uniform_int`, `uniform` leaves | H2O |
| `process` | random / clamped / monotonic walks, `precision`, `lazy`, `series` | TSBS |
| `table` | NA injection, covering samples, key splits, stable multi-key sort | H2O |
| `panel` | entity × time panels with ramp-up | TSBS |
| `fixed` + `arrow::flatten_named` | wide same-typed column groups, named flat batches | TSBS |

A new port (SSB's `dbgen` RNG, YCSB's Java `Random`) adds a `Sampler` dialect
and reuses the rest.
