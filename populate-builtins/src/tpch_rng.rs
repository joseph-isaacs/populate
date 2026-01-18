use populate_core::{Distribution, SampleIter, Sampler};

/// TPC-H specification Linear Congruential Generator (MINSTD).
///
/// This implements the exact RNG algorithm used by the TPC-H benchmark spec:
/// - Multiplier: 16807
/// - Modulus: 2^31 - 1 (2147483647)
/// - Formula: seed = (seed * 16807) % 2147483647
#[derive(Clone, Debug)]
pub struct TpchRng {
    pub(crate) seed: i64,
}

impl TpchRng {
    pub const MULTIPLIER: i64 = 16807;
    pub const MODULUS: i64 = 2147483647;

    pub fn new(seed: i64) -> Self {
        Self { seed }
    }

    /// Advance the seed and return the new value.
    pub(crate) fn next_rand(&mut self) -> i64 {
        self.seed = (self.seed * Self::MULTIPLIER) % Self::MODULUS;
        self.seed
    }
}

impl Sampler for TpchRng {
    fn bounded_int(&mut self, min: i64, max: i64) -> i64 {
        let _ = self.next_rand();
        let range = (max - min).wrapping_add(1) as f64;
        let value = ((self.seed as f64 / Self::MODULUS as f64) * range) as i64;
        min + value
    }
}

/// A distribution that produces bounded integers using the TPC-H RNG algorithm.
///
/// This matches tpchgen-rs `RandomBoundedInt` exactly.
pub struct TpchBoundedInt {
    seed: i64,
    lower: i64,
    upper: i64,
}

impl TpchBoundedInt {
    pub fn new(seed: i64, lower: i64, upper: i64) -> Self {
        Self { seed, lower, upper }
    }
}

impl Distribution<i64> for TpchBoundedInt {
    type Iter = TpchBoundedIntIter;

    fn sample(&mut self) -> Self::Iter {
        TpchBoundedIntIter {
            rng: TpchRng::new(self.seed),
            lower: self.lower,
            upper: self.upper,
        }
    }
}

#[derive(Clone)]
pub struct TpchBoundedIntIter {
    rng: TpchRng,
    lower: i64,
    upper: i64,
}

impl SampleIter for TpchBoundedIntIter {
    type Item = i64;

    fn next(&mut self, _rng: &mut impl Sampler) -> i64 {
        self.rng.bounded_int(self.lower, self.upper)
    }
}

/// Convenience function to create a TPC-H bounded integer distribution.
pub fn tpch_bounded_int(seed: i64, lower: i64, upper: i64) -> TpchBoundedInt {
    TpchBoundedInt::new(seed, lower, upper)
}

#[cfg(test)]
mod tests {
    use super::*;
    use populate_core::next_double;

    #[test]
    fn test_tpch_rng_sequence() {
        // Verify the LCG produces expected sequence
        let mut rng = TpchRng::new(1193163244);

        // First value for p_size should be 7 (matching tpchgen-rs row 0)
        let v1 = rng.bounded_int(1, 50);
        assert_eq!(v1, 7, "First p_size value should be 7");
    }

    #[test]
    fn test_tpch_rng_deterministic() {
        let mut rng1 = TpchRng::new(12345);
        let mut rng2 = TpchRng::new(12345);

        for _ in 0..100 {
            assert_eq!(rng1.bounded_int(1, 100), rng2.bounded_int(1, 100));
        }
    }

    #[test]
    fn test_next_double_exact() {
        // Verify that next_double via bounded_int gives exact results
        // next_double should equal seed / MODULUS after advancing
        const MODULUS: i64 = 2147483647;

        for seed in [12345i64, 1193163244, 99999, 1, MODULUS - 1] {
            let mut rng1 = TpchRng::new(seed);
            let mut rng2 = TpchRng::new(seed);

            // Direct calculation: advance seed, then seed / MODULUS
            rng1.next_rand();
            let expected = rng1.seed as f64 / MODULUS as f64;

            // Via bounded_int
            let actual = next_double(&mut rng2);

            assert_eq!(
                actual, expected,
                "next_double mismatch for seed {}: got {}, expected {}",
                seed, actual, expected
            );
        }
    }
}
