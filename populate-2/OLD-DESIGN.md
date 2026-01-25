# Distribution & Sink Design (Old)

## Goal

A combinator library for generating tabular data with two collection modes:
- **Row-by-row**: Iterate columns together (for JSON, row formats)
- **Col-by-col**: Iterate each column fully before the next (for Arrow, columnar formats)

## Core Traits

### SampleIter

Lazy iterator that produces samples:

```rust
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}
```

### Sink

Accepts items (push interface):

```rust
pub trait Sink<T> {
    fn push(&mut self, item: T);
}

impl<T> Sink<T> for Vec<T> { ... }
```

### Distribution

Produces values with row iteration and col-by-col collection:

```rust
pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    fn collect_into<S: SplitSink<Item = Self::Item>>(
        &mut self,
        sink: &mut S,
        n: usize,
        rng: &mut impl Rng,
    );
}
```

- `row_iter()` - for row-by-row iteration (caller loops and pushes)
- `collect_into()` - col-by-col collection (distribution drives the iteration)

## Sink Structure: Cons Lists

Sinks use explicit cons-list types:

```rust
pub struct Nil;
pub struct Cons<H, T>(pub H, pub T);
```

Examples:
- Single column: `Cons<Vec<i32>, Nil>`
- Two columns: `Cons<Vec<i32>, Cons<Vec<i64>, Nil>>`
- Three columns: `Cons<Vec<i32>, Cons<Vec<i64>, Cons<Vec<String>, Nil>>>`

### SplitSink Trait

```rust
pub trait SplitSink: Sized {
    type Item;
    type Head;
    type Tail: SplitSink;

    fn head(&mut self) -> &mut Self::Head;
    fn tail(&mut self) -> &mut Self::Tail;
}
```

### SplitSink Impls

Terminal (Nil stores a `()` so we can return `&mut ()`):

```rust
pub struct Nil(());

impl SplitSink for Nil {
    type Item = ();
    type Head = ();
    type Tail = Nil;

    fn head(&mut self) -> &mut () { &mut self.0 }
    fn tail(&mut self) -> &mut Nil { self }
}
```

Cons cell:

```rust
impl<T, Tail: SplitSink> SplitSink for Cons<Vec<T>, Tail> {
    type Item = (T, Tail::Item);
    type Head = Vec<T>;
    type Tail = Tail;

    fn head(&mut self) -> &mut Vec<T> { &mut self.0 }
    fn tail(&mut self) -> &mut Tail { &mut self.1 }
}
```

### Sink Impls for Cons

```rust
impl Sink<()> for Nil {
    fn push(&mut self, _: ()) {}
}

impl<T, Tail: SplitSink> Sink<(T, Tail::Item)> for Cons<Vec<T>, Tail>
where
    Tail: Sink<Tail::Item>,
{
    fn push(&mut self, (head, tail): (T, Tail::Item)) {
        self.0.push(head);
        self.1.push(tail);
    }
}
```

## Distribution Impls

### Scalar (Constant, Range, etc.)

```rust
impl<T: Copy> Distribution for Constant<T> {
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter { ... }

    fn collect_into<S: SplitSink<Item = T>>(
        &mut self,
        sink: &mut S,
        n: usize,
        rng: &mut impl Rng,
    ) {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.head().push(iter.next(rng));
        }
    }
}
```

### Tuple (D1, D2)

```rust
impl<D1: Distribution, D2: Distribution> Distribution for (D1, D2) {
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter { ... }

    fn collect_into<S: SplitSink<Item = (D1::Item, D2::Item)>>(
        &mut self,
        sink: &mut S,
        n: usize,
        rng: &mut impl Rng,
    ) {
        // Col-by-col: fully collect D1, then D2
        self.0.collect_into(sink.head(), n, rng);  // ???
        self.1.collect_into(sink.tail(), n, rng);
    }
}
```

**Open question**: The head is `Vec<T>`, not a `SplitSink`. Need to reconcile:
- Option A: `Vec<T>` also implements `SplitSink` (with `Tail = Nil`)
- Option B: Scalar distributions take `Sink<T>` not `SplitSink`
- Option C: Different method signatures for scalar vs tuple distributions

## Ergonomic Conversion

Convert from flat tuples to cons-lists:

```rust
pub trait IntoSink {
    type Sink: SplitSink;
    fn into_sink(self) -> Self::Sink;
}

impl<T> IntoSink for Vec<T> {
    type Sink = Cons<Vec<T>, Nil>;
    fn into_sink(self) -> Self::Sink { Cons(self, Nil(())) }
}

impl<T1, T2> IntoSink for (Vec<T1>, Vec<T2>) {
    type Sink = Cons<Vec<T1>, Cons<Vec<T2>, Nil>>;
    fn into_sink(self) -> Self::Sink {
        Cons(self.0, Cons(self.1, Nil(())))
    }
}

impl<T1, T2, T3> IntoSink for (Vec<T1>, Vec<T2>, Vec<T3>) {
    type Sink = Cons<Vec<T1>, Cons<Vec<T2>, Cons<Vec<T3>, Nil>>>;
    fn into_sink(self) -> Self::Sink {
        Cons(self.0, Cons(self.1, Cons(self.2, Nil(()))))
    }
}
```

## Item Type Structure

Item types mirror the cons structure:
- `Cons<Vec<T>, Nil>` has `Item = (T, ()) ` or just `T`?
- `Cons<Vec<T1>, Cons<Vec<T2>, Nil>>` has `Item = (T1, (T2, ()))`

**Open question**: Should we flatten the item type for ergonomics?
- Cons item: `(T1, (T2, (T3, ())))`
- Flat item: `(T1, T2, T3)`

If flat, need separate traits or conversion.

## Usage Example

```rust
// Build distribution
let mut dist = (constant(1i32), range(0, 10));

// Row-by-row (for JSON)
let mut iter = dist.row_iter();
for _ in 0..n {
    let (a, b) = iter.next(&mut rng);
    // write row
}

// Col-by-col (for Arrow)
let sink = (Vec::new(), Vec::new()).into_sink();
dist.collect_into(&mut sink, n, &mut rng);
// sink.0.0 has all values from constant
// sink.0.1.0 has all values from range
```

## Open Questions

1. **Scalar collect_into**: How does a scalar distribution collect into `SplitSink`?
   - Does `Vec<T>` implement `SplitSink`?
   - Or use `sink.head()` which returns `&mut Vec<T>`?

2. **Item type flattening**: Use cons-style `(T1, (T2, ()))` or flat `(T1, T2)`?

3. **Nested distributions**: For `((D1, D2), D3)`, is sink structure:
   - Tree: `Cons<Cons<Vec, Cons<Vec, Nil>>, Cons<Vec, Nil>>`
   - Flat: `Cons<Vec, Cons<Vec, Cons<Vec, Nil>>>`

4. **Map/dependent columns**: How do mapped distributions work with col-by-col?
   - They must use row-by-row (can't separate the dependency)
   - Use default `collect_into` that falls back to `row_iter`
