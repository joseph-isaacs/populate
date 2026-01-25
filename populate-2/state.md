# Current State

## Project Goal

A combinator library for generating tabular datasets with two collection modes:
- **Row-by-row**: Iterate columns together (for JSON, row formats) - uses `row_iter()` with manual loop
- **Col-by-col**: Iterate each column fully before next (for Arrow, columnar formats) - uses `collect_into()`

## Current File Structure

```
populate-2/
├── src/
│   ├── lib.rs      # Module declarations + tests
│   ├── traits.rs   # Core traits: SampleIter, Sink, SplitSink, Distribution
│   └── dist.rs     # Implementations: Constant, Range, Map, Tuple
├── DESIGN.md       # Design document with cons-list sink proposal
└── state.md        # This file
```

## Current Traits

### traits.rs

```rust
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

pub trait Sink<T> {
    fn push(&mut self, item: T);
}

pub trait SplitSink<T1, T2> {
    type Left: Sink<T1>;
    type Right: Sink<T2>;
    fn split(&mut self) -> (&mut Self::Left, &mut Self::Right);
}

pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    fn collect_into<S: SplitSink<Self::Item>>(
        &mut self, sink: &mut S, n: usize, rng: &mut impl Rng,
    );
}
```

### dist.rs Implementations

- `Constant<T>` - returns same value every time
- `Range` - returns incrementing i64 values (start + step*i)
- `Map<D, F>` - transforms values from source distribution
- `(D1, D2)` - tuple of two distributions

## Current Problem: Trait Bound Mismatch

The `Distribution` trait has `collect_into<S: SplitSink<Self::Item>>` but:
- Scalar distributions (Constant, Range, Map) want `S: Sink<T>`
- Tuple distributions want `S: SplitSink<T1, T2>`

Current workaround: Each impl specifies its own bound:
- `Constant::collect_into<S: Sink<T>>`
- `(D1, D2)::collect_into<S: SplitSink<D1::Item, D2::Item>>`

This works but the trait definition doesn't match the impls.

## Active Design Discussions

### 1. SplitSink Redesign

DESIGN.md proposes cons-list sinks:
```rust
pub struct Nil(());
pub struct Cons<H, T>(pub H, pub T);

pub trait SplitSink: Sized {
    type Item;
    type Head;
    type Tail: SplitSink;

    fn head(&mut self) -> &mut Self::Head;
    fn tail(&mut self) -> &mut Self::Tail;
}
```

**Benefit**: Uniform recursive structure for any column count.

**Alternative under discussion**: Simple impls for tuple types:
```rust
impl<T1, T2> SplitSink for (Vec<T1>, Vec<T2>) {
    type Head = Vec<T1>;
    type Tail = Vec<T2>;
    fn head(&mut self) -> &mut Vec<T1> { &mut self.0 }
    fn tail(&mut self) -> &mut Vec<T2> { &mut self.1 }
}
```

### 2. Named Fields

Both Arrow and JSON need column names. Proposals:
- `Field<D>` wrapper: `Field { name: &'static str, dist: D }`
- `StructDistribution` trait for named field handling

## Open Questions (from DESIGN.md)

1. **Scalar collect_into**: How does a scalar distribution collect into `SplitSink`?
   - Does `Vec<T>` implement `SplitSink`?
   - Or use `sink.head()` which returns `&mut Vec<T>`?

2. **Item type flattening**: Use cons-style `(T1, (T2, ()))` or flat `(T1, T2)`?

3. **Nested distributions**: For `((D1, D2), D3)`, is sink structure:
   - Tree: `Cons<Cons<Vec, Cons<Vec, Nil>>, Cons<Vec, Nil>>`
   - Flat: `Cons<Vec, Cons<Vec, Cons<Vec, Nil>>>`

4. **Map/dependent columns**: Must fall back to row-by-row (can't separate dependency)

## Tests

Tests use helper functions returning `impl Distribution`:
```rust
fn make_constant() -> impl Distribution<Item = i32> { constant(42) }
fn make_tuple() -> impl Distribution<Item = (i32, i64)> { (constant(1), range(0, 10)) }
```

Iteration order tests verify:
- Row-by-row: `COL1, COL2, COL1, COL2, ...`
- Col-by-col: `COL1, COL1, COL1, COL2, COL2, COL2`

## Next Steps

1. Decide on SplitSink design (cons-list vs tuple impls)
2. Design named field support
3. Reconcile trait bounds for `collect_into`
