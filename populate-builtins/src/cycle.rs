use populate_core::Rng;
use populate_core::{Distribution, SampleIter};

/// A distribution that cycles through a fixed list of values.
pub struct Cycle<T, const N: usize> {
    values: [T; N],
}

impl<T: Clone, const N: usize> Cycle<T, N> {
    pub fn new(values: [T; N]) -> Self {
        Self { values }
    }
}

#[derive(Clone)]
pub struct CycleIter<T, const N: usize> {
    values: [T; N],
    index: usize,
}

impl<T: Clone, const N: usize> Distribution<T> for Cycle<T, N> {
    type Iter = CycleIter<T, N>;

    fn sample(&mut self) -> Self::Iter {
        CycleIter {
            values: self.values.clone(),
            index: 0,
        }
    }
}

impl<T: Clone, const N: usize> SampleIter for CycleIter<T, N> {
    type Item = T;

    fn next(&mut self, _rng: &mut impl Rng) -> T {
        let v = self.values[self.index].clone();
        self.index = (self.index + 1) % N;
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_cycle() {
        let mut dist = Cycle::new(["A", "B", "C"]);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), "A");
        assert_eq!(iter.next(&mut rng), "B");
        assert_eq!(iter.next(&mut rng), "C");
        assert_eq!(iter.next(&mut rng), "A");
        assert_eq!(iter.next(&mut rng), "B");
    }

    #[test]
    fn test_cycle_integers() {
        let mut dist = Cycle::new([10, 20, 30]);
        let mut rng = StdRng::seed_from_u64(0);
        let mut iter = dist.sample();

        assert_eq!(iter.next(&mut rng), 10);
        assert_eq!(iter.next(&mut rng), 20);
        assert_eq!(iter.next(&mut rng), 30);
        assert_eq!(iter.next(&mut rng), 10);
    }
}
