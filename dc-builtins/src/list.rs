use dc_core::Distribution;

/// A distribution for lists with a length distribution and element distribution.
pub struct List<L, E> {
    pub lengths: L,
    pub elements: E,
}

impl<L, E> List<L, E> {
    pub fn new(lengths: L, elements: E) -> Self {
        Self { lengths, elements }
    }

    /// Sample a list iterator from this distribution.
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

/// Iterator for list values - bundles length and element iterators.
#[derive(Clone)]
pub struct ListIter<L, E> {
    pub lengths: L,
    pub elements: E,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;
    use dc_core::SampleIter;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_list() {
        let mut dist = List::new(Constant::new(3usize), Constant::new(42i32));
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        // Get a length
        let len = iter.lengths.next(&mut rng);
        assert_eq!(len, 3);

        // Get elements
        for _ in 0..len {
            assert_eq!(iter.elements.next(&mut rng), 42);
        }
    }
}
