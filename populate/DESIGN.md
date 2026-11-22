# Data-Comb Design Document

## Overview

A combinator library for generating tabular datasets. Distributions compose to describe table schemas, and data can be collected in different orders depending on the output format.

## Core Concepts

### Distribution

A **Distribution** describes how to generate values for one or more columns. Distributions compose:

- **Scalar**: Single column (`Constant<T>`, `Range`, `Uniform<T>`, `Map<D, F>`)
- **Tuple**: Multiple columns (`(D1, D2)`, `(D1, D2, D3)`, nested `((D1, D2), D3)`)

The distribution structure mirrors the table schema:

```rust
// Schema: id (i64), name (String), age (i32)
let dist = (range(0, 1), constant("Alice"), uniform(18..65));
```

### Collection Modes

**Row-by-row**: Produce complete rows one at a time.

```
Row 0: (id_0, name_0, age_0)
Row 1: (id_1, name_1, age_1)
Row 2: (id_2, name_2, age_2)
```

Use case: JSON, CSV, row-oriented databases, streaming.

**Col-by-col**: Fill each column completely before moving to the next.

```
Column 0: id_0, id_1, id_2
Column 1: name_0, name_1, name_2
Column 2: age_0, age_1, age_2
```

Use case: Arrow, Parquet, columnar databases. More cache-friendly for columnar storage.

### Sinks

A **Sink** receives generated values. Different sink types for different use cases:

- `Vec<T>` - simple collection
- Arrow array builders - columnar format
- Custom collectors - streaming, aggregation, etc.

Sink structure must match distribution structure:

```
Distribution          Sink
-----------           ----
Constant<i32>    →    Vec<i32>
(D1, D2)         →    (S1, S2)
((D1, D2), D3)   →    ((S1, S2), S3)
```

## Requirements

### Functional

1. **Row iteration**: Any distribution can produce rows via an iterator
2. **Col collection**: Tuple distributions can fill columns independently
3. **Composability**: Distributions nest arbitrarily: `((D1, D2), (D3, D4))`
4. **Multiple sinks**: Same distribution works with Vec, Arrow, custom sinks

### Ergonomic

5. **Simple trait bounds**: Users write `D: Distribution`, not complex parameterized bounds
6. **Minimal distribution boilerplate**: New scalar distributions need minimal code
7. **Minimal sink boilerplate**: New sink types need minimal code

### Technical

8. **Rust coherence**: No unstable features (specialization, negative impls)
9. **Type safety**: Sink/distribution structure mismatch is a compile error

## Design Questions

### Q1: What is the minimal Distribution trait?

Row iteration is straightforward:

```rust
trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;
}
```

But how do we support col-by-col collection?

### Q2: Where does col-by-col logic live?

Options:

**A. Method on Distribution**
```rust
trait Distribution {
    fn collect_into<S: ???>(&mut self, sink: &mut S, n: usize, rng: &mut impl Rng);
}
```
Problem: What bound on S? Varies by distribution structure.

**B. Method on Sink**
```rust
trait Sink<D: Distribution> {
    fn collect_from(&mut self, dist: &mut D, n: usize, rng: &mut impl Rng);
}
```
Problem: Users work with sinks, not distributions. Less discoverable.

**C. Free function / external**
```rust
fn collect<D, S>(dist: &mut D, sink: &mut S, n: usize, rng: &mut impl Rng)
where
    // constraints
```
Problem: Same constraint problem, just moved.

**D. Distribution controls its own collection**
```rust
trait Distribution {
    type Sink;
    fn collect_into(&mut self, sink: &mut Self::Sink, n: usize, rng: &mut impl Rng);
}
```
Problem: Only one sink type per distribution. Loses flexibility.

### Q3: How do we express "sink matches distribution"?

The constraint is recursive:

- Scalar `D` with `Item = T` needs `S: Sink<T>`
- Tuple `(D1, D2)` needs `S` splittable into `(S1, S2)` where:
  - `S1` matches `D1`
  - `S2` matches `D2`

Rust can't express this as a trait bound without:
- Per-distribution sink impls (boilerplate)
- Coherence conflicts (blanket impls overlap)
- Unstable features

### Q4: Is splitting necessary?

Col-by-col requires iterating each sub-distribution independently. For `(D1, D2)`:

```rust
// Conceptually:
for _ in 0..n { sink.0.push(d1.next(rng)); }
for _ in 0..n { sink.1.push(d2.next(rng)); }
```

This requires:
1. Accessing sub-distributions (`d1`, `d2`)
2. Accessing sub-sinks (`sink.0`, `sink.1`)

"Splitting" is one way to express this. Are there others?

## Approaches Under Consideration

### Approach 1: SinkFor<D> trait

```rust
trait SinkFor<D: Distribution> {
    fn accept(&mut self, dist: &mut D, n: usize, rng: &mut impl Rng);
}

trait Distribution {
    fn collect_into<S: SinkFor<Self>>(&mut self, sink: &mut S, ...);
}
```

Pros: Clean method on Distribution, bound references Self.

Cons: Need impl `SinkFor<D>` for each (sink, distribution) pair. Blanket impls conflict.

### Approach 2: Associated Sink type

```rust
trait Distribution {
    type Sink;
    fn collect_into(&mut self, sink: &mut Self::Sink, ...);
}
```

Pros: Simple, no complex bounds.

Cons: Only one sink type per distribution.

### Approach 3: External collection

Keep Distribution minimal. Collection logic lives outside:

```rust
trait Distribution {
    type Item;
    fn row_iter(&mut self) -> impl SampleIter<Item = Self::Item>;
}

// Separate functions/traits for collection
fn collect_scalar<D: Distribution, S: Sink<D::Item>>(...);
fn collect_tuple<D1, D2, S1, S2>(...);
```

Pros: Distribution stays simple. No coherence issues.

Cons: Less unified API. Users must choose the right function.

### Approach 4: Macro-based

Use macros to generate the boilerplate:

```rust
scalar_distribution!(Constant<T: Copy>, T);
scalar_distribution!(Range, i64);
tuple_distribution!((D1, D2));
```

Pros: Reduces visible boilerplate.

Cons: Magic. Harder to understand. Still boilerplate under the hood.

## Next Steps

1. Decide: Does `collect_into` belong on `Distribution`?
2. Decide: One sink type (associated) vs many (generic with constraints)?
3. Prototype the most promising approach
4. Validate with real use cases (Arrow integration)
