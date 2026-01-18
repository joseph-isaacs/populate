use populate_core::Sampler;
use populate_core::{Distribution, SampleIter};

/// Pick one random element from a slice.
pub struct PickOne<'a, T> {
    values: &'a [T],
}

impl<'a, T> PickOne<'a, T> {
    pub fn new(values: &'a [T]) -> Self {
        Self { values }
    }
}

#[derive(Clone)]
pub struct PickOneIter<'a, T> {
    values: &'a [T],
}

impl<T: Clone> SampleIter for PickOneIter<'_, T> {
    type Item = T;

    fn next(&mut self, rng: &mut impl Sampler) -> T {
        let idx = rng.bounded_int(0, self.values.len() as i64 - 1) as usize;
        self.values[idx].clone()
    }
}

impl<'a, T: Clone> Distribution<T> for PickOne<'a, T> {
    type Iter = PickOneIter<'a, T>;

    fn sample(&mut self) -> Self::Iter {
        PickOneIter {
            values: self.values,
        }
    }
}

/// Pick N random elements from a slice (compile-time N).
pub struct Pick<'a, T, const N: usize> {
    values: &'a [T],
}

impl<'a, T, const N: usize> Pick<'a, T, N> {
    pub fn new(values: &'a [T]) -> Self {
        Self { values }
    }
}

#[derive(Clone)]
pub struct PickIter<'a, T, const N: usize> {
    values: &'a [T],
}

impl<T: Clone, const N: usize> SampleIter for PickIter<'_, T, N> {
    type Item = [T; N];

    fn next(&mut self, rng: &mut impl Sampler) -> [T; N] {
        std::array::from_fn(|_| {
            let idx = rng.bounded_int(0, self.values.len() as i64 - 1) as usize;
            self.values[idx].clone()
        })
    }
}

impl<'a, T: Clone, const N: usize> Distribution<[T; N]> for Pick<'a, T, N> {
    type Iter = PickIter<'a, T, N>;

    fn sample(&mut self) -> Self::Iter {
        PickIter {
            values: self.values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const COLORS: [&str; 5] = ["red", "green", "blue", "yellow", "purple"];

    #[test]
    fn test_pick_one() {
        let mut dist = PickOne::new(&COLORS);
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        for _ in 0..10 {
            let color = iter.next(&mut rng);
            assert!(COLORS.contains(&color));
        }
    }

    #[test]
    fn test_pick_n() {
        let mut dist = Pick::<_, 3>::new(&COLORS);
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let picked: [&str; 3] = iter.next(&mut rng);
        assert_eq!(picked.len(), 3);
        for color in picked {
            assert!(COLORS.contains(&color));
        }
    }
}
