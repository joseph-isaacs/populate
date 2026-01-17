use crate::Distribution;

/// A combinator that produces variable-length lists.
///
/// Unlike scalar distributions, List does not implement Distribution<T>
/// because it produces a special two-stream iterator (lengths + elements).
/// Use ListCollector to materialize into the desired format.
pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L, E> List<L, E> {
    pub fn new(lengths: L, elements: E) -> Self {
        List { lengths, elements }
    }
}

impl<L, E> List<L, E> {
    pub fn sample<T>(&mut self) -> ListIter<L::Iter, E::Iter>
    where
        L: Distribution<usize>,
        E: Distribution<T>,
    {
        ListIter {
            lengths: self.lengths.sample(),
            elements: self.elements.sample(),
        }
    }
}

/// Iterator for List combinator.
///
/// Exposes two streams: lengths and elements.
/// Use a ListCollector to materialize into the desired format.
pub struct ListIter<L, E> {
    pub lengths: L,
    pub elements: E,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Vecs, ListCollector};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_list() {
        let mut dist = List::new(Constant::new(3usize), Constant::new(7i32));
        let mut rng = StdRng::seed_from_u64(0);

        let (values, offsets) = Vecs::collect_list(dist.sample(), &mut rng, 2);

        // 2 lists of 3 elements each = 6 values
        assert_eq!(values, vec![7, 7, 7, 7, 7, 7]);
        assert_eq!(offsets, vec![0, 3, 6]);
    }
}
