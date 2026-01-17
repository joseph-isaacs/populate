use crate::{Distribution, SampleIter};
use rand::Rng;

/// Iterator for tuple distributions.
#[derive(Clone)]
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

/// Iterator for 3-tuples.
#[derive(Clone)]
pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }
}

impl<D1, D2, D3, T1, T2, T3> Distribution<(T1, T2, T3)> for (D1, D2, D3)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Iter = TupleIter3<D1::Iter, D2::Iter, D3::Iter>;

    fn sample(&mut self) -> Self::Iter {
        TupleIter3(self.0.sample(), self.1.sample(), self.2.sample())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_tuple2() {
        let mut dist = (Constant::new(1i32), Constant::new(2i64));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), (1, 2));
        assert_eq!(iter.next(&mut rng), (1, 2));
    }

    #[test]
    fn test_tuple3() {
        let mut dist = (Constant::new(1i32), Constant::new(2i64), Constant::new(3u8));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), (1, 2, 3));
    }
}
