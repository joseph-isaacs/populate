# Trait Split Design: RowDistribution + Distribution

## Problem

The current single `Distribution` trait requires `Item: DefaultSink<B>` for all types, even those that only do row-by-row iteration (Map, Flatten, wrappers). This drags `DefaultSink`, `HAppend`, `Backend`, and HList machinery into every bound, increasing compile time and coupling.

A default `collect_into` impl doesn't work because it needs `Sink<Self::Item>` on the sink, which fails for nested tuple items like `(i32, (i64, i64), i32)` — the sink is a 4-element HList but the item is a 3-tuple with no matching `Sink` impl. The bound propagates to generic callers like `collect_flat`, breaking nested tuple usage.

## Design

```rust
// Core: no DefaultSink, no Backend, no HList bounds
pub trait RowDistribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;
    fn row_iter(&mut self) -> Self::RowIter;
}

// Columnar: adds collect_into, needs Backend + DefaultSink
pub trait Distribution<B: Backend>: RowDistribution
where
    Self::Item: DefaultSink<B>,
{
    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink<B>>::Sink;
}
```

## How types impl both

### Leaf types (Constant, Range)

```rust
impl<T: Copy> RowDistribution for Constant<T> {
    type Item = T;
    type RowIter = ConstantIter<T>;
    fn row_iter(&mut self) -> Self::RowIter { ConstantIter { value: self.value } }
}

impl<B: Backend, T: Copy + DefaultSink<B>> Distribution<B> for Constant<T>
where B::Prim<T>: Sink<T> {
    fn collect_into(&mut self, mut sink: ..., rng, n) -> ... {
        let mut iter = self.row_iter();
        for _ in 0..n { sink.push(iter.next(rng)); }
        sink
    }
}
```

### Map — key win: inner D only needs RowDistribution

```rust
impl<D: RowDistribution<Item = In>, F: Fn(In) -> Out + Clone, In, Out>
    RowDistribution for Map<D, F, In, Out>
{
    type Item = Out;  // no DefaultSink bound!
    type RowIter = MapIter<D::RowIter, F>;
    fn row_iter(&mut self) -> Self::RowIter { ... }
}

impl<B: Backend, D: RowDistribution<Item = In>, F, In, Out: DefaultSink<B>>
    Distribution<B> for Map<D, F, In, Out>
where F: Fn(In) -> Out + Clone, <Out as DefaultSink<B>>::Sink: Sink<Out>
{
    fn collect_into(&mut self, mut sink: ..., rng, n) -> ... {
        let mut iter = self.row_iter();
        for _ in 0..n { sink.push(iter.next(rng)); }
        sink
    }
}
```

### Tuples — col-by-col needs inner Distribution

```rust
impl<D1: RowDistribution, D2: RowDistribution> RowDistribution for (D1, D2) {
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;
    fn row_iter(&mut self) -> Self::RowIter { ... }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
{
    fn collect_into(&mut self, sink: ..., rng, n) -> ... {
        let (s1, s2) = unappend(sink);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        s1.append(s2)
    }
}
```

### Flatten — RowDistribution has no columnar bounds

```rust
impl<D: RowDistribution> RowDistribution for Flatten<D>
where
    D::Item: ValueHList,
    <D::Item as ValueHList>::HList: IntoTuple,
{
    type Item = <<D::Item as ValueHList>::HList as IntoTuple>::Tuple;
    type RowIter = FlattenIter<D::RowIter>;
    fn row_iter(&mut self) -> Self::RowIter { FlattenIter(self.0.row_iter()) }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for Flatten<D>
where
    D::Item: ValueHList,
    <D::Item as ValueHList>::HList: IntoTuple,
    <<D::Item as ValueHList>::HList as IntoTuple>::Tuple:
        DefaultSink<B, Sink = <D::Item as DefaultSink<B>>::Sink>,
{
    fn collect_into(&mut self, sink: ..., rng, n) -> ... {
        self.0.collect_into(sink, rng, n)  // delegate
    }
}
```

## Ext traits

`.flatten()` lives on `RowDistributionExt` — no Backend needed:

```rust
pub trait RowDistributionExt: RowDistribution {
    fn flatten(self) -> Flatten<Self>
    where Self: Sized, Self::Item: ValueHList, ... {
        Flatten(self)
    }
}

pub trait DistributionExt<B: Backend>: Distribution<B> {
    fn collect_flat<T>(&mut self, sink: T, rng: &mut impl Rng, n: usize) -> T
    where ... {
        self.collect_into(sink.into_hlist(), rng, n).into_tuple()
    }
}
```

## User-facing

```rust
// Row-only consumer — no columnar code compiled
fn gen_rows() -> impl RowDistribution<Item = (i16, i64, i64, i32)> {
    (range(0i16, 1), map(...), range(0i32, 5)).flatten()
}

// Full columnar consumer
fn gen_cols() -> impl Distribution<VecBackend, Item = (i16, i64, i64, i32)> {
    (range(0i16, 1), map(...), range(0i32, 5)).flatten()
}
```

## Benefits

- `Map`, `Flatten`, and custom wrappers only need `D: RowDistribution` — no DefaultSink/HAppend/Backend bounds in most code
- Feature-gatable: columnar support behind a feature flag
- Less monomorphization, faster compile for row-only consumers
- Clearer separation of concerns

## Why not default collect_into

Default impl needs `Sink<Self::Item>` which fails for nested tuple items like `(i32, (i64, i64), i32)` where the sink HList length doesn't match the tuple arity. The bound propagates to generic callers like `collect_flat`, breaking nested tuple usage. No clean workaround without specialization.
