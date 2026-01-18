use populate_core::Sampler;
use populate_core::{Distribution, SampleIter};

/// A distribution for lists with a length distribution and element distribution.
pub struct List<L: Distribution<usize>, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L: Distribution<usize>, E> List<L, E> {
    pub fn new(lengths: L, elements: E) -> Self {
        Self { lengths, elements }
    }
}

/// Iterator for list values - bundles length and element iterators.
///
/// For AoS (row-oriented): call `next()` to produce `Vec<E::Item>`
/// For SoA (columnar): access `.lengths` and `.elements` directly
#[derive(Clone)]
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

    fn next(&mut self, rng: &mut impl Sampler) -> Vec<E::Item> {
        let len = self.lengths.next(rng);
        (0..len).map(|_| self.elements.next(rng)).collect()
    }
}

impl<T, L, E> Distribution<Vec<T>> for List<L, E>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_list_aos() {
        let mut dist = List::new(Constant::new(3usize), Constant::new(42i32));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        // AoS: next() produces complete Vec
        let row = iter.next(&mut rng);
        assert_eq!(row, vec![42, 42, 42]);
    }

    #[test]
    fn test_list_soa() {
        let mut dist = List::new(Constant::new(3usize), Constant::new(42i32));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        // SoA: access structure directly
        let len = iter.lengths.next(&mut rng);
        assert_eq!(len, 3);

        for _ in 0..len {
            assert_eq!(iter.elements.next(&mut rng), 42);
        }
    }
}
