//! OneOf combinator for selecting from multiple distributions.

use rand::Rng;

use crate::Distribution;

/// A combinator that uniformly selects from multiple distributions.
///
/// Each inner distribution has an equal probability of being selected.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Constant, OneOf};
/// use rand::thread_rng;
///
/// let dist = OneOf::new(vec![
///     Constant::new(1),
///     Constant::new(2),
///     Constant::new(3),
/// ]);
/// let mut rng = thread_rng();
/// let value = dist.sample(&mut rng);
/// assert!(value >= 1 && value <= 3);
/// ```
#[derive(Debug, Clone)]
pub struct OneOf<D> {
    distributions: Vec<D>,
}

impl<D> OneOf<D> {
    /// Create a new OneOf distribution from a list of distributions.
    ///
    /// # Panics
    ///
    /// Panics if `distributions` is empty.
    pub fn new(distributions: Vec<D>) -> Self {
        assert!(!distributions.is_empty(), "OneOf requires at least one distribution");
        OneOf { distributions }
    }

    /// Get a reference to the inner distributions.
    pub fn distributions(&self) -> &[D] {
        &self.distributions
    }
}

impl<T, D: Distribution<T>> Distribution<T> for OneOf<D> {
    fn sample(&self, rng: &mut impl Rng) -> T {
        let idx = rng.gen_range(0..self.distributions.len());
        self.distributions[idx].sample(rng)
    }
}

/// A wrapper for distributions with associated weights.
#[derive(Debug, Clone)]
pub struct WeightedDistribution<D> {
    /// The distribution.
    pub distribution: D,
    /// The weight (relative probability).
    pub weight: f64,
}

impl<D> WeightedDistribution<D> {
    /// Create a new weighted distribution.
    pub fn new(distribution: D, weight: f64) -> Self {
        WeightedDistribution { distribution, weight }
    }
}

/// A combinator that selects from multiple distributions with weighted probabilities.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Constant, OneOfWeighted, WeightedDistribution};
/// use rand::thread_rng;
///
/// let dist = OneOfWeighted::new(vec![
///     WeightedDistribution::new(Constant::new(1), 0.1),  // 10% probability
///     WeightedDistribution::new(Constant::new(2), 0.9),  // 90% probability
/// ]);
/// let mut rng = thread_rng();
/// let value = dist.sample(&mut rng);
/// ```
#[derive(Debug, Clone)]
pub struct OneOfWeighted<D> {
    distributions: Vec<WeightedDistribution<D>>,
    cumulative_weights: Vec<f64>,
    total_weight: f64,
}

impl<D> OneOfWeighted<D> {
    /// Create a new weighted OneOf distribution.
    ///
    /// # Panics
    ///
    /// Panics if `distributions` is empty or if any weight is negative.
    pub fn new(distributions: Vec<WeightedDistribution<D>>) -> Self {
        assert!(
            !distributions.is_empty(),
            "OneOfWeighted requires at least one distribution"
        );

        let mut cumulative_weights = Vec::with_capacity(distributions.len());
        let mut total = 0.0;

        for wd in &distributions {
            assert!(
                wd.weight >= 0.0,
                "Weights must be non-negative, got {}",
                wd.weight
            );
            total += wd.weight;
            cumulative_weights.push(total);
        }

        assert!(
            total > 0.0,
            "Total weight must be positive"
        );

        OneOfWeighted {
            distributions,
            cumulative_weights,
            total_weight: total,
        }
    }

    /// Get a reference to the inner weighted distributions.
    pub fn distributions(&self) -> &[WeightedDistribution<D>] {
        &self.distributions
    }
}

impl<T, D: Distribution<T>> Distribution<T> for OneOfWeighted<D> {
    fn sample(&self, rng: &mut impl Rng) -> T {
        let target = rng.gen::<f64>() * self.total_weight;

        // Binary search for the selected distribution
        let idx = self
            .cumulative_weights
            .partition_point(|&w| w <= target)
            .min(self.distributions.len() - 1);

        self.distributions[idx].distribution.sample(rng)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Constant;

    #[test]
    fn test_one_of_uniform() {
        let dist = OneOf::new(vec![
            Constant::new(1),
            Constant::new(2),
            Constant::new(3),
        ]);
        let mut rng = rand::thread_rng();

        let mut counts = [0; 3];
        for _ in 0..3000 {
            let v = dist.sample(&mut rng);
            counts[(v - 1) as usize] += 1;
        }

        // Each should be roughly 1000 (allow wide margin)
        for count in counts {
            assert!(count > 700 && count < 1300);
        }
    }

    #[test]
    fn test_one_of_weighted() {
        let dist = OneOfWeighted::new(vec![
            WeightedDistribution::new(Constant::new(1), 0.1),
            WeightedDistribution::new(Constant::new(2), 0.9),
        ]);
        let mut rng = rand::thread_rng();

        let mut counts = [0; 2];
        for _ in 0..1000 {
            let v = dist.sample(&mut rng);
            counts[(v - 1) as usize] += 1;
        }

        // First should be ~100, second ~900
        assert!(counts[0] < 200);
        assert!(counts[1] > 800);
    }

    #[test]
    #[should_panic(expected = "requires at least one distribution")]
    fn test_one_of_empty() {
        let _: OneOf<Constant<i32>> = OneOf::new(vec![]);
    }
}
