mod vec;

#[cfg(feature = "arrow")]
pub mod arrow;

pub use vec::Vecs;

#[cfg(feature = "arrow")]
pub use self::arrow::Arrow;

use crate::list::ListIter;
use crate::tuple::{TupleIter2, TupleIter3};
use crate::SampleIter;
use rand::Rng;

/// Collector for scalar/element distributions.
pub trait ElementCollector {
    type Output<T>;

    fn collect_element<I: SampleIter>(iter: I, rng: &mut impl Rng, n: usize) -> Self::Output<I::Item>;
}

/// Collector for tuple/struct distributions.
pub trait StructCollector {
    type Output2<T1, T2>;
    type Output3<T1, T2, T3>;

    fn collect2<I1: SampleIter, I2: SampleIter>(
        iter: TupleIter2<I1, I2>,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Output2<I1::Item, I2::Item>;

    fn collect3<I1: SampleIter, I2: SampleIter, I3: SampleIter>(
        iter: TupleIter3<I1, I2, I3>,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Output3<I1::Item, I2::Item, I3::Item>;
}

/// Collector for list distributions.
pub trait ListCollector {
    type Output<T>;

    fn collect_list<L: SampleIter<Item = usize>, E: SampleIter>(
        iter: ListIter<L, E>,
        rng: &mut impl Rng,
        n: usize,
    ) -> Self::Output<E::Item>;
}
