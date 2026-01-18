use std::marker::PhantomData;

use populate_core::{Distribution, SampleIter};

// === ExtendColumn: append one element to a tuple (N impls) ===

/// Extend a tuple by appending one element.
pub trait ExtendColumn<T> {
    type Output;
    fn extend_column(self, t: T) -> Self::Output;
}

impl<A, T> ExtendColumn<T> for (A,) {
    type Output = (A, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, t)
    }
}

impl<A, B, T> ExtendColumn<T> for (A, B) {
    type Output = (A, B, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, t)
    }
}

impl<A, B, C, T> ExtendColumn<T> for (A, B, C) {
    type Output = (A, B, C, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, self.2, t)
    }
}

impl<A, B, C, D, T> ExtendColumn<T> for (A, B, C, D) {
    type Output = (A, B, C, D, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, self.2, self.3, t)
    }
}

impl<A, B, C, D, E, T> ExtendColumn<T> for (A, B, C, D, E) {
    type Output = (A, B, C, D, E, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, self.2, self.3, self.4, t)
    }
}

impl<A, B, C, D, E, F, T> ExtendColumn<T> for (A, B, C, D, E, F) {
    type Output = (A, B, C, D, E, F, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, self.2, self.3, self.4, self.5, t)
    }
}

impl<A, B, C, D, E, F, G, T> ExtendColumn<T> for (A, B, C, D, E, F, G) {
    type Output = (A, B, C, D, E, F, G, T);
    fn extend_column(self, t: T) -> Self::Output {
        (self.0, self.1, self.2, self.3, self.4, self.5, self.6, t)
    }
}

// === AppendColumnsTo: unpack self and extend base element-by-element (M impls) ===

/// Append all elements of self to a base tuple.
pub trait AppendColumnsTo<Base> {
    type Output;
    fn append_columns_to(self, base: Base) -> Self::Output;
}

impl<Base: ExtendColumn<A>, A> AppendColumnsTo<Base> for (A,) {
    type Output = Base::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0)
    }
}

impl<Base, A, B> AppendColumnsTo<Base> for (A, B)
where
    Base: ExtendColumn<A>,
    Base::Output: ExtendColumn<B>,
{
    type Output = <Base::Output as ExtendColumn<B>>::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0).extend_column(self.1)
    }
}

impl<Base, A, B, C> AppendColumnsTo<Base> for (A, B, C)
where
    Base: ExtendColumn<A>,
    Base::Output: ExtendColumn<B>,
    <Base::Output as ExtendColumn<B>>::Output: ExtendColumn<C>,
{
    type Output = <<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0)
            .extend_column(self.1)
            .extend_column(self.2)
    }
}

impl<Base, A, B, C, D> AppendColumnsTo<Base> for (A, B, C, D)
where
    Base: ExtendColumn<A>,
    Base::Output: ExtendColumn<B>,
    <Base::Output as ExtendColumn<B>>::Output: ExtendColumn<C>,
    <<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output: ExtendColumn<D>,
{
    type Output =
        <<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0)
            .extend_column(self.1)
            .extend_column(self.2)
            .extend_column(self.3)
    }
}

impl<Base, A, B, C, D, E> AppendColumnsTo<Base> for (A, B, C, D, E)
where
    Base: ExtendColumn<A>,
    Base::Output: ExtendColumn<B>,
    <Base::Output as ExtendColumn<B>>::Output: ExtendColumn<C>,
    <<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output: ExtendColumn<D>,
    <<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output:
        ExtendColumn<E>,
{
    type Output = <<<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output as ExtendColumn<E>>::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0)
            .extend_column(self.1)
            .extend_column(self.2)
            .extend_column(self.3)
            .extend_column(self.4)
    }
}

impl<Base, A, B, C, D, E, F> AppendColumnsTo<Base> for (A, B, C, D, E, F)
where
    Base: ExtendColumn<A>,
    Base::Output: ExtendColumn<B>,
    <Base::Output as ExtendColumn<B>>::Output: ExtendColumn<C>,
    <<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output: ExtendColumn<D>,
    <<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output:
        ExtendColumn<E>,
    <<<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output as ExtendColumn<E>>::Output:
        ExtendColumn<F>,
{
    type Output = <<<<<Base::Output as ExtendColumn<B>>::Output as ExtendColumn<C>>::Output as ExtendColumn<D>>::Output as ExtendColumn<E>>::Output as ExtendColumn<F>>::Output;
    fn append_columns_to(self, base: Base) -> Self::Output {
        base.extend_column(self.0)
            .extend_column(self.1)
            .extend_column(self.2)
            .extend_column(self.3)
            .extend_column(self.4)
            .extend_column(self.5)
    }
}

// === Merge: combine two distributions by concatenating their output tuples ===

/// Merge two distributions by concatenating their output tuples.
pub struct Merge<D1, D2, T1, T2> {
    first: D1,
    second: D2,
    _phantom: PhantomData<(T1, T2)>,
}

impl<D1, D2, T1, T2> Merge<D1, D2, T1, T2> {
    pub fn new(first: D1, second: D2) -> Self {
        Self {
            first,
            second,
            _phantom: PhantomData,
        }
    }
}

pub struct MergeIter<I1, I2, T1, T2> {
    first: I1,
    second: I2,
    _phantom: PhantomData<(T1, T2)>,
}

impl<I1: Clone, I2: Clone, T1, T2> Clone for MergeIter<I1, I2, T1, T2> {
    fn clone(&self) -> Self {
        Self {
            first: self.first.clone(),
            second: self.second.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I1, I2, T1, T2> SampleIter for MergeIter<I1, I2, T1, T2>
where
    I1: SampleIter<Item = T1>,
    I2: SampleIter<Item = T2>,
    T2: AppendColumnsTo<T1>,
{
    type Item = T2::Output;

    fn next(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
        let first = self.first.next(rng);
        let second = self.second.next(rng);
        second.append_columns_to(first)
    }
}

impl<D1, D2, T1, T2> Distribution<T2::Output> for Merge<D1, D2, T1, T2>
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    T2: AppendColumnsTo<T1>,
{
    type Iter = MergeIter<D1::Iter, D2::Iter, T1, T2>;

    fn sample(&mut self) -> Self::Iter {
        MergeIter {
            first: self.first.sample(),
            second: self.second.sample(),
            _phantom: PhantomData,
        }
    }
}

/// Merge two distributions by concatenating their output tuples.
pub fn merge<D1, D2, T1, T2>(first: D1, second: D2) -> Merge<D1, D2, T1, T2>
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    Merge::new(first, second)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constant, inc};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_merge_tuples() {
        let mut dist = merge((inc(1i64), constant("a")), (constant(3.14f64), inc(100u32)));

        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let row1 = iter.next(&mut rng);
        assert_eq!(row1, (1i64, "a", 3.14f64, 100u32));

        let row2 = iter.next(&mut rng);
        assert_eq!(row2, (2i64, "a", 3.14f64, 101u32));
    }

    #[test]
    fn test_extend_column() {
        let t = (1, 2);
        let extended = t.extend_column(3);
        assert_eq!(extended, (1, 2, 3));

        let extended2 = extended.extend_column(4);
        assert_eq!(extended2, (1, 2, 3, 4));
    }

    #[test]
    fn test_append_columns_to() {
        let base = (1, 2);
        let to_append = (3, 4, 5);
        let result = to_append.append_columns_to(base);
        assert_eq!(result, (1, 2, 3, 4, 5));
    }
}
