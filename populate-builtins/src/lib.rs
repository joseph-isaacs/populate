//! Built-in distributions: [`constant`], [`range`], [`map`], [`list`],
//! [`with_column`], [`flatten`], and the random leaves in [`random`] ([`uniform`], [`choose`],
//! [`weighted`], [`text`], [`chars`]). Tuples of distributions are implemented in `populate-core`
//! (orphan rule); their iterator types are re-exported here.

use populate_core::traits::{
    Backend, DefaultSink, Distribution, Flatten, HCons, HNil, ListCol, SampleIter, Sink,
    collect_rows,
};
use rand::Rng;
use std::marker::PhantomData;
use std::ops::Add;

mod with_column;
pub use with_column::{
    Column, ColumnRng, ReadRows, RowAppend, WithColumn, WithColumnIter, with_column,
};

pub use populate_core::tuple::{TupleIter2, TupleIter3, TupleIter4};

pub mod random;
pub use random::{Chars, Choose, Text, Uniform, Weighted, chars, choose, text, uniform, weighted};

// ============================================================================
// Constant
// ============================================================================

#[derive(Clone)]
pub struct Constant<T> {
    pub value: T,
}

pub fn constant<T>(value: T) -> Constant<T> {
    Constant { value }
}

#[derive(Clone)]
pub struct ConstantIter<T> {
    value: T,
}

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.value
    }
}

impl<B: Backend, T: Copy + DefaultSink<B>> Distribution<B> for Constant<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        ConstantIter { value: self.value }
    }

    fn collect_into(
        &mut self,
        sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

// ============================================================================
// Range
// ============================================================================

#[derive(Clone)]
pub struct Range<T> {
    pub start: T,
    pub step: T,
}

pub fn range<T: Add>(start: T, step: T) -> Range<T> {
    Range { start, step }
}

#[derive(Clone)]
pub struct RangeIter<T> {
    pub current: T,
    pub step: T,
}

impl<T: Add<Output = T> + Copy> SampleIter for RangeIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        let v = self.current;
        self.current = self.current + self.step;
        v
    }
}

impl<B: Backend, T: DefaultSink<B> + Add<Output = T> + Copy> Distribution<B> for Range<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = RangeIter<T>;

    fn row_iter(&mut self) -> Self::RowIter {
        RangeIter {
            current: self.start,
            step: self.step,
        }
    }

    fn collect_into(
        &mut self,
        sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

// ============================================================================
// Map — row-by-row collection via Sink<Out>::push
// ============================================================================

pub struct Map<D, F, In, Out> {
    pub source: D,
    pub f: F,
    pub _phantom: PhantomData<(In, Out)>,
}

impl<D: Clone, F: Clone, In, Out> Clone for Map<D, F, In, Out> {
    fn clone(&self) -> Self {
        Map {
            source: self.source.clone(),
            f: self.f.clone(),
            _phantom: PhantomData,
        }
    }
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

impl<B: Backend, D, F, In, Out: DefaultSink<B>> Distribution<B> for Map<D, F, In, Out>
where
    D: Distribution<B, Item = In>,
    F: Fn(In) -> Out + Clone,
    <Out as DefaultSink<B>>::Sink: Sink<Out>,
{
    type Item = Out;
    type RowIter = MapIter<D::RowIter, F>;

    fn row_iter(&mut self) -> Self::RowIter {
        MapIter {
            source: self.source.row_iter(),
            f: self.f.clone(),
        }
    }

    fn collect_into(
        &mut self,
        sink: <Out as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Out as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

// ============================================================================
// List<L, E> — list/array column distribution
// ============================================================================

#[derive(Clone)]
pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

pub fn list<L, E>(lengths: L, elements: E) -> List<L, E> {
    List { lengths, elements }
}

pub struct ListIter<L, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L, E> SampleIter for ListIter<L, E>
where
    L: SampleIter<Item = usize>,
    E: SampleIter,
{
    type Item = Vec<E::Item>;

    // Element stream continues across rows (same semantics as columnar path).
    fn next(&mut self, rng: &mut impl Rng) -> Vec<E::Item> {
        let len = self.lengths.next(rng);
        (0..len).map(|_| self.elements.next(rng)).collect()
    }
}

impl<B: Backend, L, E> Distribution<B> for List<L, E>
where
    L: Distribution<B, Item = usize>,
    E: Distribution<B>,
{
    type Item = Vec<E::Item>;
    type RowIter = ListIter<L::RowIter, E::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        ListIter {
            lengths: self.lengths.row_iter(),
            elements: self.elements.row_iter(),
        }
    }

    // Fully columnar:
    //   pass 1: n lengths -> offsets column (+ total element count)
    //   pass 2: hand the *whole* child sink to the element distribution for
    //           `total` rows, so tuples/lists inside recurse col-by-col.
    fn collect_into(
        &mut self,
        sink: <Vec<E::Item> as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Vec<E::Item> as DefaultSink<B>>::Sink {
        let HCons(
            ListCol {
                mut offsets,
                values,
                mut len,
            },
            HNil,
        ) = sink;
        offsets.reserve(n);
        let mut lengths = self.lengths.row_iter();
        let start = len;
        for _ in 0..n {
            len += lengths.next(rng);
            offsets.push(len);
        }
        let values = self.elements.collect_into(values, rng, len - start);
        HCons(
            ListCol {
                offsets,
                values,
                len,
            },
            HNil,
        )
    }
}

// ============================================================================
// Flatten — free function wrapper (avoids UFCS for generic-B contexts)
// ============================================================================

pub fn flatten<D>(d: D) -> Flatten<D> {
    Flatten(d)
}
