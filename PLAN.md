# Implementation Plan

Minimal implementation: core traits, three combinators, two output formats.

## Phase 1: Core Traits

```rust
// src/lib.rs
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}
```

## Phase 2: Constant Combinator

Simplest base distribution - always returns the same value.

```rust
// src/constant.rs
pub struct Constant<T: Clone>(T);

pub struct ConstantIter<T: Clone>(T);

impl<T: Clone> Distribution<T> for Constant<T> {
    type Iter = ConstantIter<T>;
    fn sample(&mut self) -> Self::Iter { ConstantIter(self.0.clone()) }
}

impl<T: Clone> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> T { self.0.clone() }
}
```

## Phase 3: Tuple/Struct Combinator

Tuples of distributions produce tuple iterators.

```rust
// src/tuple.rs
pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

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
```

## Phase 4: List Combinator

Variable-length lists with separate length and element distributions.

```rust
// src/list.rs
pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

pub struct ListIter<L, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L, E, T> Distribution<()> for List<L, E>  // () placeholder, List uses custom collection
where
    L: Distribution<usize>,
    E: Distribution<T>,
{
    type Iter = ListIter<L::Iter, E::Iter>;
    fn sample(&mut self) -> Self::Iter {
        ListIter {
            lengths: self.lengths.sample(),
            elements: self.elements.sample(),
        }
    }
}
```

## Phase 5: Collector Traits

Three traits, one per shape.

```rust
// src/collect.rs
pub trait ElementCollector {
    type Output<T>;
    fn collect<I: SampleIter>(iter: I, rng: &mut impl Rng, n: usize) -> Self::Output<I::Item>;
}

pub trait StructCollector {
    type Output<T1, T2>;
    fn collect<I1: SampleIter, I2: SampleIter>(
        iter: TupleIter2<I1, I2>,
        rng: &mut impl Rng,
        n: usize
    ) -> Self::Output<I1::Item, I2::Item>;
}

pub trait ListCollector {
    type Output<T>;
    fn collect<L: SampleIter<Item = usize>, E: SampleIter>(
        iter: ListIter<L, E>,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Output<E::Item>;
}
```

## Phase 6: Vec Collectors

```rust
// src/collect/vec.rs
pub struct Vecs;

impl ElementCollector for Vecs {
    type Output<T> = Vec<T>;
    fn collect<I: SampleIter>(mut iter: I, rng: &mut impl Rng, n: usize) -> Vec<I::Item> {
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

impl StructCollector for Vecs {
    type Output<T1, T2> = (Vec<T1>, Vec<T2>);
    fn collect<I1: SampleIter, I2: SampleIter>(
        mut iter: TupleIter2<I1, I2>,
        rng: &mut impl Rng,
        n: usize,
    ) -> (Vec<I1::Item>, Vec<I2::Item>) {
        let (mut c1, mut c2) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for _ in 0..n {
            c1.push(iter.0.next(rng));
            c2.push(iter.1.next(rng));
        }
        (c1, c2)
    }
}

impl ListCollector for Vecs {
    type Output<T> = (Vec<T>, Vec<usize>);
    fn collect<L: SampleIter<Item = usize>, E: SampleIter>(
        mut iter: ListIter<L, E>,
        rng: &mut impl Rng,
        n: usize,
    ) -> (Vec<E::Item>, Vec<usize>) {
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
}
```

## Phase 7: Arrow Collectors

```rust
// src/collect/arrow.rs
pub struct Arrow;

pub trait ArrowType {
    type Builder: ArrowBuilder<Self>;
}

impl ArrowType for i32 { type Builder = Int32Builder; }
impl ArrowType for i64 { type Builder = Int64Builder; }
// ... etc

impl ElementCollector for Arrow {
    type Output<T> = ArrayRef;
    fn collect<I: SampleIter>(mut iter: I, rng: &mut impl Rng, n: usize) -> ArrayRef
    where
        I::Item: ArrowType,
    {
        let mut builder = <I::Item as ArrowType>::Builder::with_capacity(n);
        for _ in 0..n {
            builder.append_value(iter.next(rng));
        }
        Arc::new(builder.finish())
    }
}

impl StructCollector for Arrow {
    type Output<T1, T2> = RecordBatch;
    // ...
}

impl ListCollector for Arrow {
    type Output<T> = ArrayRef;
    // ...
}
```

## File Structure

```
src/
├── lib.rs           # Core traits: SampleIter, Distribution
├── constant.rs      # Constant combinator
├── tuple.rs         # Tuple impl + TupleIter2
├── list.rs          # List combinator + ListIter
└── collect/
    ├── mod.rs       # Collector traits
    ├── vec.rs       # Vecs impl
    └── arrow.rs     # Arrow impl (feature-gated)
```

## Cargo.toml

```toml
[package]
name = "data-comb"
version = "0.1.0"
edition = "2021"

[dependencies]
rand = "0.8"

[dependencies.arrow]
version = "53"
optional = true

[features]
default = []
arrow = ["dep:arrow"]
```

## Example Usage

```rust
use data_comb::*;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // Constant
    let mut dist = Constant(42i32);
    let data: Vec<i32> = Vecs::collect(dist.sample(), &mut rng, 10);

    // Tuple
    let mut dist = (Constant(1i32), Constant(2i64));
    let (c1, c2): (Vec<i32>, Vec<i64>) = Vecs::collect(dist.sample(), &mut rng, 10);

    // List
    let mut dist = List::new(Constant(3usize), Constant(7i32));
    let (values, offsets): (Vec<i32>, Vec<usize>) = Vecs::collect(dist.sample(), &mut rng, 10);

    // Arrow (with feature)
    #[cfg(feature = "arrow")]
    {
        let array: ArrayRef = Arrow::collect(Constant(42i32).sample(), &mut rng, 10);
    }
}
```
