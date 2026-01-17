use crate::collect::{ElementCollector, ListCollector, StructCollector};
use crate::list::ListIter;
use crate::tuple::{TupleIter2, TupleIter3};
use crate::SampleIter;
use rand::Rng;

/// Collector that outputs Vec types.
pub struct Vecs;

impl ElementCollector for Vecs {
    type Output<T> = Vec<T>;

    fn collect_element<I: SampleIter>(mut iter: I, rng: &mut impl Rng, n: usize) -> Vec<I::Item> {
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

impl StructCollector for Vecs {
    type Output2<T1, T2> = (Vec<T1>, Vec<T2>);
    type Output3<T1, T2, T3> = (Vec<T1>, Vec<T2>, Vec<T3>);

    fn collect2<I1: SampleIter, I2: SampleIter>(
        mut iter: TupleIter2<I1, I2>,
        rng: &mut impl Rng,
        n: usize,
    ) -> (Vec<I1::Item>, Vec<I2::Item>) {
        let mut c1 = Vec::with_capacity(n);
        let mut c2 = Vec::with_capacity(n);
        for _ in 0..n {
            c1.push(iter.0.next(rng));
            c2.push(iter.1.next(rng));
        }
        (c1, c2)
    }

    fn collect3<I1: SampleIter, I2: SampleIter, I3: SampleIter>(
        mut iter: TupleIter3<I1, I2, I3>,
        rng: &mut impl Rng,
        n: usize,
    ) -> (Vec<I1::Item>, Vec<I2::Item>, Vec<I3::Item>) {
        let mut c1 = Vec::with_capacity(n);
        let mut c2 = Vec::with_capacity(n);
        let mut c3 = Vec::with_capacity(n);
        for _ in 0..n {
            c1.push(iter.0.next(rng));
            c2.push(iter.1.next(rng));
            c3.push(iter.2.next(rng));
        }
        (c1, c2, c3)
    }
}

impl ListCollector for Vecs {
    type Output<T> = (Vec<T>, Vec<usize>);

    fn collect_list<L: SampleIter<Item = usize>, E: SampleIter>(
        mut iter: ListIter<L, E>,
        rng: &mut impl Rng,
        n: usize,
    ) -> (Vec<E::Item>, Vec<usize>) {
        let mut values = Vec::new();
        let mut offsets = vec![0usize];
        for _ in 0..n {
            let mut elem_iter = iter.elements.clone(); // Fresh state for each list
            let len = iter.lengths.next(rng);
            for _ in 0..len {
                values.push(elem_iter.next(rng));
            }
            offsets.push(values.len());
        }
        (values, offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Distribution, List};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_element_collect() {
        let mut dist = Constant::new(42i32);
        let mut rng = StdRng::seed_from_u64(0);

        let data: Vec<i32> = Vecs::collect_element(dist.sample(), &mut rng, 5);
        assert_eq!(data, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_struct_collect2() {
        let mut dist = (Constant::new(1i32), Constant::new(2i64));
        let mut rng = StdRng::seed_from_u64(0);

        let (c1, c2) = Vecs::collect2(dist.sample(), &mut rng, 3);
        assert_eq!(c1, vec![1, 1, 1]);
        assert_eq!(c2, vec![2, 2, 2]);
    }

    #[test]
    fn test_list_collect() {
        let mut dist = List::new(Constant::new(2usize), Constant::new(7i32));
        let mut rng = StdRng::seed_from_u64(0);

        let (values, offsets) = Vecs::collect_list(dist.sample(), &mut rng, 3);
        assert_eq!(values, vec![7, 7, 7, 7, 7, 7]); // 3 lists * 2 elements
        assert_eq!(offsets, vec![0, 2, 4, 6]);
    }
}
