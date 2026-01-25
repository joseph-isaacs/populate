use crate::traits::{Distribution, SampleIter, Sink};
use rand::Rng;
use std::marker::PhantomData;

// ============================================================================
// Constant
// ============================================================================

pub struct Constant<T> {
    pub value: T,
}

pub fn constant<T>(value: T) -> Constant<T> {
    Constant { value }
}

pub struct ConstantIter<T> {
    value: T,
}

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<T: Copy> Distribution for Constant<T> {
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter { value: self.value }
    }

    fn collect_into<S: Sink<T>>(&mut self, rng: &mut impl Rng, mut sink: S, n: usize) -> S {
        for _ in 0..n {
            sink.push(self.row_iter().next(rng))
        }
        sink
    }
}

// ============================================================================
// Range
// ============================================================================

pub struct Range {
    pub start: i64,
    pub step: i64,
}

pub fn range(start: i64, step: i64) -> Range {
    Range { start, step }
}

pub struct RangeIter {
    pub current: i64,
    pub step: i64,
}

impl SampleIter for RangeIter {
    type Item = i64;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        let v = self.current;
        self.current += self.step;
        v
    }
}

impl Distribution for Range {
    type Item = i64;
    type RowIter = RangeIter;

    fn row_iter(&mut self) -> Self::RowIter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }
}

// ============================================================================
// Tuple (D1, D2)
// ============================================================================

pub struct TupleIter<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<D1: Distribution, D2: Distribution> Distribution for (D1, D2) {
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter(self.0.row_iter(), self.1.row_iter())
    }

    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, sink: S, n: usize) -> S {
        let (s1, s2) = sink.split();
        let (s2, s3) = s2.split()
    }
}

// ============================================================================
// Map
// ============================================================================

pub struct Map<D, F, In, Out> {
    pub source: D,
    pub f: F,
    pub _phantom: PhantomData<(In, Out)>,
}

pub fn map<D, F, In, Out>(source: D, f: F) -> Map<D, F, In, Out>
where
    F: Fn(In) -> Out,
{
    Map {
        source,
        f,
        _phantom: PhantomData,
    }
}

pub struct MapIter<I, F> {
    source: I,
    f: F,
}

impl<I, F, Out> SampleIter for MapIter<I, F>
where
    I: SampleIter,
    F: Fn(I::Item) -> Out,
{
    type Item = Out;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.f)(self.source.next(rng))
    }
}

impl<D, F, In, Out> Distribution for Map<D, F, In, Out>
where
    D: Distribution<Item = In>,
    F: Fn(In) -> Out + Clone,
{
    type Item = Out;
    type RowIter = MapIter<D::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }
}
