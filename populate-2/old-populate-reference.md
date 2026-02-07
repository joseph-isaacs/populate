# Old Populate Implementation Reference

Everything from the v1 populate implementation (populate-core, populate-builtins, populate-builtins-tpch, tpch), preserved before removal.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Traits (populate-core)](#core-traits)
3. [Distribution Implementations (populate-builtins)](#distribution-implementations)
4. [TPC-H Extensions (populate-builtins-tpch)](#tpch-extensions)
5. [TPC-H Usage (tpch crate)](#tpch-usage)
6. [Columnar Collection System](#columnar-collection-system)
7. [Design Evolution & Insights](#design-evolution)
8. [Key Differences from populate-2](#key-differences)

---

## Architecture Overview

The v1 system has three layers:

```
populate-core          Core traits: Distribution<T>, SampleIter, Sink, CollectColumnar
populate-builtins      Leaf distributions + combinators: Constant, Range, Map, List, Pick, Join, Merge, etc.
populate-builtins-tpch TPC-H specific: TpchRng, TpcHRange, TpchPickN, PickNBuffered
tpch                   Actual TPC-H table definitions using the above
```

**Key design**: `Distribution<T>` is parameterized by its *output type* T, not by backend/sink. This means a single `Constant<i32>` implements `Distribution<i32>` and works with any sink. The sink is chosen at collection time, not at distribution definition time.

---

## Core Traits

### SampleIter

```rust
pub trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;

    /// Zero-allocation bulk generation. Used for col-by-col collection.
    fn next_n_with<F>(&mut self, n: usize, rng: &mut impl Rng, mut f: F)
    where F: FnMut(Self::Item);
}
```

**Key insight**: `Clone` is required. Clone semantics: cloned iterator resets accumulated state (produces same sequence given same RNG). Used by List to give each list independent element generation.

### Distribution<T>

```rust
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}
```

Minimal factory trait. No length parameter — iterators are infinite. `&mut self` on `sample()` allows distributions to hold mutable state (though most are stateless).

### Sink<T>

```rust
pub trait Sink<T> {
    type Output;
    fn push(&mut self, value: T);
    fn extend(&mut self, iter: impl Iterator<Item = T>); // bulk with default
    fn finish(self) -> Self::Output;
}
```

Push-based value consumer with `finish()` to produce final output. `Output` type allows transformations (e.g., Arrow builder → ArrayRef).

### ListSink<T>

```rust
pub trait ListSink<T> {
    type Output;
    fn start_list(&mut self);
    fn push_element(&mut self, value: T);
    fn end_list(&mut self);
    fn finish(self) -> Self::Output;
}
```

Specialized for variable-length list columns. Tracks list boundaries (offsets).

### ColumnSchema (derive macro)

```rust
pub trait ColumnSchema {
    type Row;
    fn field_names() -> &'static [&'static str];
    fn field_count() -> usize;
}
```

Used by `#[derive(ColumnSchema)]` on structs like TPC-H `Part` to get field metadata.

### Collector<D, T>

```rust
pub trait Collector<D, T>: Sized {
    type Output;
    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output;
}
```

Turbofish-style collection: `dist.collect::<Arrow>(&mut rng, n)`. Allows different output formats from same distribution.

---

## Tuple Machinery

### Tuple Distribution Impls

Tuples of distributions implement `Distribution` producing tuple iterators:

```rust
impl<D1: Distribution<T1>, D2: Distribution<T2>> Distribution<(T1, T2)> for (D1, D2)
// ... up to 8-tuple
```

Each has a corresponding `TupleIterN` struct.

### Unnest — Compile-time Tuple Flattening

```rust
pub struct Unnest<D>(pub D);
```

When used in a tuple like `(D1, Unnest<D2>)` where D2 produces `(B, C)`, the result is flattened to `(A, B, C)` instead of `(A, (B, C))`.

**Used by**: nations table — `(inc(0i64), unnest(cycle(NATIONS)))` where NATIONS is `[(&str, i64); 25]`.

---

## Distribution Implementations

### Constant<T>
- Always returns `value.clone()`
- `IS_COLUMNAR: true` (leaf)

### Range<T, S = ()>
- Random values in `[min, max)` via `RangeSampler<T>` strategy
- Default strategy `()` delegates to `rng.random_range()`
- Custom strategy `TpcHRange` for TPC-H specific formula
- Supports i8..i64, u8..u64, usize, f32, f64

### Inc<T>
- Incrementing values: 0, 1, 2, 3, ...
- Uses macro to impl for all integer types (can't use generic Add due to += requirement)
- **Insight**: populate-2's `Range { start, step }` with step=1 subsumes this

### Cycle<T, const N: usize>
- Cycles through a fixed array: ["A", "B", "C", "A", "B", ...]
- Const-generic N avoids allocation
- **Use case**: Deterministic sequences (nation names, region names)

### Map<D, F, T>
- Transforms `D::Item` via closure `F`
- `IS_COLUMNAR: false` — map breaks columnar since function can combine columns
- Row-by-row fallback in `collect_columnar`

### List<L, E>
- Length distribution `L: Distribution<usize>` + element distribution `E`
- `ListIter` exposes both sub-iterators (`.lengths`, `.elements`)
- Row-by-row: `next()` produces `Vec<E::Item>` by sampling length, then that many elements
- Columnar: uses `ListSink` with start_list/push_element/end_list protocol

### PickOne<T, I>
- Pick one element from a slice by index: `values[indices.next(rng)]`
- Index distribution is explicit (usually `Range<i64>`)
- Clones the picked value

### PickOneRef<T, I>
- Like PickOne but returns `&'a T` (no clone)
- Has `with_sampler` constructor that auto-creates range index

### Pick<T, I, const N: usize>
- Pick N elements with replacement (duplicates allowed)
- Returns `[T; N]` fixed-size array
- Uses `std::array::from_fn`

### PickN<T, S, const N: usize>
- Pick N unique elements using a `PickStrategy`
- Default strategy `Unique` uses Fisher-Yates partial shuffle
- Returns `[T; N]`

### Seeded<R, D>
- Wraps a distribution with its own RNG, ignoring the passed-in one
- **Use case**: TPC-H requires specific RNG seeds per column for reproducibility
- `IS_COLUMNAR: false` — can't delegate columnar since own RNG changes order

### Print<D>
- Debug wrapper that prints a label on each `next()`
- `IS_COLUMNAR: false` (interleaved output would be confusing)

### Log<D> (test-only)
- Like Print but logs to `Rc<RefCell<Vec<&str>>>` instead of stdout
- **Key test tool**: Verifies col-by-col vs row-by-row execution order

### Join<D, T, const N: usize>
- Joins `[T; N]` array elements into String with separator
- `pick_n(&COLORS).join(" ")` → "red green blue"

### JoinVec<D, T>
- Runtime version of Join for `Vec<T>` elements
- `list(constant(3), pick_one(&COLORS)).join_vec(" ")`

### JoinTuple2/JoinTuple3
- Join 2-tuple or 3-tuple elements into String
- Uses `format!()` instead of collect+join

### Combine
- Precomputes cartesian product of string slices: `combine3(&A, &B, &C, " ")`
- Stores `Vec<String>`, derefs to `[String]`
- **Use case**: TPC-H part types = syllable3 × syllable2 × syllable1

### Merge<D1, D2, T1, T2>
- Concatenates two tuple distributions: `(A, B)` + `(C, D)` → `(A, B, C, D)`
- Uses `AppendColumnsTo` trait for type-level tuple concatenation
- `IS_COLUMNAR: false` (TODO: could be columnar)

### ExtendColumn / AppendColumnsTo
- Type-level tuple extension: `(A, B).extend_column(C)` → `(A, B, C)`
- `(A, B, C).append_columns_to((X, Y))` → `(X, Y, A, B, C)`
- Impls up to 7-element base + 6-element append

---

## Columnar Collection System

### SinkTuple<Row>

Row-oriented push into tuple of sinks:

```rust
pub trait SinkTuple<Row>: Sized {
    type Output;
    fn push_row(&mut self, row: Row);
    fn finish(self) -> Self::Output;
}
```

Impls for `(S1,)` through `(S1..S8)`. Destructures the row tuple and pushes to individual sinks.

### SinkTupleN — Column-wise Access

```rust
pub trait SinkTuple2<T1, T2>: SinkTuple<(T1, T2)> {
    fn push0(&mut self, v: T1);
    fn push1(&mut self, v: T2);
}
// ... up to SinkTuple8
```

Column-wise push for true col-by-col collection. Each `pushN` pushes to the Nth sink only.

### CollectColumnar<Sinks>

```rust
pub trait CollectColumnar<Sinks>: SampleIter + Sized {
    const IS_COLUMNAR: bool;  // can this collect column-by-column?
    type Output;
    fn collect_columnar(self, rng: &mut impl Rng, n: usize, sinks: Sinks) -> Self::Output;
}
```

True col-by-col for tuple iterators (each column generated fully before next). Row-by-row fallback for Map, Seeded, etc.

### JoinOutputs — Tuple Output Concatenation

```rust
pub trait JoinOutputs<Other> {
    type Output;
    fn join(self, other: Other) -> Self::Output;
}
```

All partition combinations for output tuples up to 8 elements: `(A,) + (B, C)` → `(A, B, C)`, etc.

### Free Collection Functions

```rust
fn collect_into<I, S>(iter, rng, n, sink) -> S::Output;         // row-oriented
fn collect_into_column<I, S>(iter, rng, n, sink) -> S::Output;  // bulk extend
fn collect_struct2_into(iter, rng, n, s1, s2) -> (O1, O2);      // 2-col columnar
fn collect_struct3_into(...)                                      // 3-col columnar
// ... up to collect_struct7_into
fn collect_list_into(lengths, elements, rng, n, sink) -> Output; // list columnar
```

The `collect_structN_into` functions use `next_n_with` for zero-allocation col-by-col:
```rust
iter.0.next_n_with(n, rng, |v| sink1.push(v));  // all of col 0
iter.1.next_n_with(n, rng, |v| sink2.push(v));  // all of col 1
```

---

## TPC-H Extensions

### TpchRng — Linear Congruential Generator

```rust
pub struct TpchRng { seed: i64 }
// Formula: seed = (seed * 16807) % 2147483647
```

- Implements exact TPC-H spec RNG (MINSTD)
- `bounded_int(min, max)` for TPC-H range formula: `floor((seed / MODULUS) * range) + min`
- `skip(n)` — O(log n) using binary exponentiation of the multiplier
  - Key optimization: `seed' = seed * multiplier^n mod modulus`

### TpcHRange — RangeSampler Strategy

Plugs into `Range<T, TpcHRange>` to use TPC-H's specific scaling formula instead of standard rand.

### TpchUnique — PickStrategy

Fisher-Yates with TPC-H seed accounting: after picking N, advance RNG by (len - N) to match TPC-H's seeds_per_row.

### TpchPickN — Optimized Pick

- Owns its TpchRng (not seeded wrapper)
- Uses `rng.skip(len - N)` for O(log n) seed accounting instead of O(n) linear calls
- Reusable u8 shuffle buffer (max 255 elements)
- Tracks swaps for buffer restoration instead of full reset

### PickNBuffered — Legacy Pick

- Same algorithm but O(n) RNG advancement per row
- Still used where seeded wrapper provides the RNG

---

## TPC-H Usage

### Regions (static, 5 rows)

```rust
pub fn regions() -> impl Distribution<(i64, &'static str)> {
    (inc(0i64), cycle(["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]))
}
```

### Nations (static, 25 rows)

```rust
pub fn nations() -> impl Distribution<(i64, &'static str, i64)> {
    (inc(0i64), unnest(cycle(NATIONS)))  // NATIONS: [(&str, i64); 25]
}
```

**Insight**: `unnest(cycle(NATIONS))` — cycle produces `(&str, i64)` tuples, unnest flattens `(i64, (&str, i64))` → `(i64, &str, i64)`.

### Parts (200k × scale_factor, 8 columns)

```rust
pub fn parts() -> impl Distribution<PartRow> {
    merge(
        merge(
            inc(1i64).map(|key| (key, calculate_retail_price(key))),
            (
                seeded(rng(709314158), pick_n::<_, 5>(&COLORS).join(" ")),  // p_name
                seeded(rng(1841581359), pick_one(&PART_TYPES)),              // p_type
                seeded(rng(1193163244), range(1, 51)),                       // p_size
                seeded(rng(727633698), pick_one(&PART_CONTAINERS)),          // p_container
            ),
        ),
        (seeded(rng(1), range(1i32, 6)), seeded(rng(46831694), range(1i32, 6)))
            .map(|(mfgr, brand_raw)| (mfgr, mfgr * 10 + brand_raw)),
    )
}
```

**Pattern**: `merge` concatenates sub-tuples. Each column gets its own `seeded(rng(SPECIFIC_SEED), ...)` for TPC-H reproducibility.

**Derived columns**: `retail_price` computed from `partkey`, `brand` computed from `mfgr` — both use `map`.

### Verification

Tests compare output against `tpchgen` reference implementation row-by-row and column-by-column, asserting exact value matches for all 8 part columns.

---

## Design Evolution

### Original Vision (DESIGN.md, PLAN.md)

- Distributions produce forever, consumers decide when to stop
- Row vs column is a *collection concern*, not a generation concern
- Same iterator, different materialization
- Planned: Arrow collectors, schema-driven generation, grammar-based text

### Planned But Not Built

From FUTURE.md:

| Feature | Description |
|---------|-------------|
| Bernoulli | Biased boolean |
| Weighted<T> | Non-uniform selection from set |
| WithNulls<D> | Adds null probability |
| Runs<D> | Run-length friendly data (stateful) |
| Monotonic<D> | Accumulating deltas |
| FlatMap | Output determines next distribution |
| ForeignKey | Reference values from another column |
| Grammar text | Template-driven string generation |
| Arrow collectors | Direct ArrayRef output |
| Parquet integration | File output |
| Dynamic schemas | `Vec<Box<dyn DynDistribution>>` |
| Value enum | Type-erased `Value { Int, Float, Bool, String, Null }` |

### Boolean Pattern Ideas

- `Periodic<N>` — repeat every N
- `Burst<D>` — mostly one value, occasional bursts
- `Gradient` — probability changes over sequence
- `Markov` — P(next) depends on current
- `Segments` — different densities per segment
- `ZipfRuns` — run lengths follow Zipf distribution

### Design Questions Explored

1. **Where does col-by-col logic live?** → On Distribution via `collect_into` with associated sink type
2. **One sink type or many?** → v1 uses generic sinks via CollectColumnar trait; v2 uses associated type via Backend
3. **Item type flattening** → v1 uses flat tuples with Unnest for manual flattening; v2 uses HList with automatic flattening
4. **Map with col-by-col** → Must fall back to row-by-row (can't separate dependency)

### Boilerplate Problem

The v1 approach requires massive boilerplate:
- `TupleIterN` structs: 2 through 8 (7 structs)
- `SinkTupleN` traits: 2 through 8 (7 traits)
- `CollectColumnar` impls per TupleIterN (7 impls)
- `SinkTuple` impls per arity (7 impls)
- `JoinOutputs` impls: all partitions up to 8 (~28 impls)
- `collect_structN_into` functions: 2 through 7 (6 functions)

This is why v2 moved to HList-based approach — one recursive definition replaces all arity-specific impls.

---

## Key Differences from populate-2

| Aspect | v1 (old) | v2 (populate-2) |
|--------|----------|-----------------|
| Distribution param | `Distribution<T>` (output type) | `Distribution<B: Backend>` (backend) |
| Sink selection | At collection time via generics | Via `DefaultSink<B>` associated type |
| Tuple sinks | Flat tuple of sinks | HList (`HCons<S, HCons<S, HNil>>`) |
| Col-by-col | `CollectColumnar` trait + `SinkTupleN` | `collect_into` on Distribution |
| Flattening | `Unnest<D>` (manual, compile-time) | `Flatten<D>` (automatic via HList) |
| Tuple concat | `Merge` + `AppendColumnsTo` | `HAppend` (type-level) |
| Ergonomic API | `collect_structN_into()` free functions | `collect_flat()` with tuple↔HList conversion |
| `SampleIter::Clone` | Required | Not required |
| Max arity | 8-tuple | 5-tuple (extensible) |

### What v1 Has That v2 Doesn't (Yet)

- `Cycle<T, const N: usize>` — deterministic repeating sequences
- `Inc<T>` — incrementing (v2 has Range which subsumes this)
- `PickOne`, `PickOneRef`, `Pick`, `PickN` — slice sampling family
- `Join`, `JoinVec`, `JoinTuple2/3` — string joining combinators
- `Merge` — tuple concatenation combinator
- `Combine` — cartesian product of string slices
- `Seeded<R, D>` — per-column RNG override
- `Print<D>`, `Log<D>` — debug wrappers (v2 has equivalent Log in tests)
- `Unnest<D>` — manual tuple flattening (v2 has Flatten)
- `TpchRng` with O(log n) skip
- `ColumnSchema` derive macro
- Full TPC-H part table generation with reference test verification
