#[derive(Debug, Clone)]
pub struct RandomNumberStream {
    // Constants matching Java implementation exactly
    seed: i64,
    initial_seed: i64,
    seeds_used: u32,
    seeds_per_row: u32,
}

impl RandomNumberStream {
    const DEFAULT_SEED_BASE: i32 = 19620718;
    const MULTIPLIER: i64 = 16807;
    const QUOTIENT: i64 = 127773; // the quotient MAX_INT / MULTIPLIER
    const REMAINDER: i64 = 2836; // the remainder MAX_INT % MULTIPLIER

    pub fn new(seeds_per_row: u32) -> Self {
        RandomNumberStream {
            initial_seed: 3,
            seed: 3,
            seeds_used: 0,
            seeds_per_row,
        }
    }

    pub fn new_with_column(global_column_number: i32, seeds_per_row: u32) -> Self {
        Self::new_with_base(global_column_number, Self::DEFAULT_SEED_BASE, seeds_per_row)
    }

    pub fn new_with_base(global_column_number: i32, seed_base: i32, seeds_per_row: u32) -> Self {
        let initial_seed = seed_base as i64 + global_column_number as i64 * (i32::MAX as i64 / 799);
        RandomNumberStream {
            initial_seed,
            seed: initial_seed,
            seeds_used: 0,
            seeds_per_row,
        }
    }
}

/// `16807^k mod (2^31 - 1)`: `k` Lehmer steps as one multiplication.
static POW: [i64; 256] = {
    let mut p = [1i64; 256];
    let mut k = 1;
    while k < 256 {
        p[k] = p[k - 1] * 16807 % i32::MAX as i64;
        k += 1;
    }
    p
};

impl RandomNumberStream {
    // https://en.wikipedia.org/wiki/Lehmer_random_number_generator
    #[inline]
    pub fn next_random(&mut self) -> i64 {
        let seed = self.seed;
        let next_seed = if (1..i32::MAX as i64).contains(&seed) {
            // `seed * 16807 mod (2^31 - 1)` by folding the high bits onto
            // the low ones (`2^31 = 1 mod m`): Schrage's method below gives
            // the same residue for a seed in range, with two divisions
            let p = seed as u64 * Self::MULTIPLIER as u64;
            let r = (p & i32::MAX as u64) + (p >> 31);
            ((r & i32::MAX as u64) + (r >> 31)) as i64
        } else {
            // a seed dsdgen never reduced (some start above the modulus)
            let division_result = seed / Self::QUOTIENT;
            let mod_result = seed % Self::QUOTIENT;
            let next = Self::MULTIPLIER * mod_result - division_result * Self::REMAINDER;
            if next < 0 {
                next + i32::MAX as i64
            } else {
                next
            }
        };

        self.seed = next_seed;
        // dsdgen's count is a C int: it can wrap on a stream that is never
        // reset (WS_PRICING before web_sales).
        self.seeds_used = self.seeds_used.wrapping_add(1);
        self.seed
    }

    pub fn next_random_double(&mut self) -> f64 {
        self.next_random() as f64 / i32::MAX as f64
    }

    pub fn skip_rows(&mut self, number_of_rows: u64) {
        let mut number_of_values_to_skip = number_of_rows * self.seeds_per_row as u64;
        let mut next_seed = self.initial_seed;
        let mut multiplier = Self::MULTIPLIER;

        while number_of_values_to_skip > 0 {
            if !number_of_values_to_skip.is_multiple_of(2) {
                // n is odd
                next_seed = (multiplier * next_seed) % i32::MAX as i64;
            }
            number_of_values_to_skip /= 2;
            multiplier = (multiplier * multiplier) % i32::MAX as i64;
        }

        self.seed = next_seed;
        self.seeds_used = 0;
    }

    /// Burn the rest of this row's budget, as drawing until `seeds_used`
    /// reaches `seeds_per_row` would (nothing if the row overran), and
    /// start the next row's count.
    #[inline]
    pub fn finish_row(&mut self) {
        if self.seeds_used < self.seeds_per_row {
            let left = (self.seeds_per_row - self.seeds_used) as usize;
            if left < POW.len() {
                self.seed = self.seed * POW[left] % i32::MAX as i64;
            } else {
                self.advance(left as u64);
            }
        }
        self.seeds_used = 0;
    }

    /// Advance the stream by `n` draws from where it is now (dsdgen's
    /// `skip_random`, but relative), leaving `seeds_used` alone.
    pub fn advance(&mut self, mut n: u64) {
        let mut next_seed = self.seed;
        let mut multiplier = Self::MULTIPLIER;
        while n > 0 {
            if n % 2 == 1 {
                next_seed = (multiplier * next_seed) % i32::MAX as i64;
            }
            n /= 2;
            multiplier = (multiplier * multiplier) % i32::MAX as i64;
        }
        self.seed = next_seed;
    }

    /// The current seed (the last value drawn).
    pub fn seed(&self) -> i64 {
        self.seed
    }

    /// Put the stream at `seed`, as if it had just drawn it.
    pub fn set_seed(&mut self, seed: i64) {
        self.seed = seed;
        self.seeds_used = 0;
    }

    /// The stream as it starts, before any draw.
    pub fn initial(&self) -> Self {
        let mut s = self.clone();
        s.seed = s.initial_seed;
        s.seeds_used = 0;
        s
    }

    /// Change the per-row budget (for columns whose every row draws a fixed
    /// number above dsdgen's budget, so skipping stays exact).
    pub fn set_seeds_per_row(&mut self, n: u32) {
        self.seeds_per_row = n;
    }

    /// Set the draws counted in this row (restoring a snapshot).
    pub fn set_seeds_used(&mut self, n: u32) {
        self.seeds_used = n;
    }

    pub fn reset_seed(&mut self) {
        self.seed = self.initial_seed;
        self.seeds_used = 0;
    }

    pub fn get_seeds_used(&self) -> u32 {
        self.seeds_used
    }

    pub fn reset_seeds_used(&mut self) {
        self.seeds_used = 0;
    }

    pub fn get_seeds_per_row(&self) -> u32 {
        self.seeds_per_row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The folded step is Schrage's method for every seed in range.
    #[test]
    fn folded_step_matches_schrage() {
        let schrage = |seed: i64| {
            let q = RandomNumberStream::QUOTIENT;
            let next = RandomNumberStream::MULTIPLIER * (seed % q)
                - seed / q * RandomNumberStream::REMAINDER;
            if next < 0 {
                next + i32::MAX as i64
            } else {
                next
            }
        };
        let seeds = (1..i32::MAX as i64).step_by(4_093).chain([
            1,
            2,
            127_772,
            127_773,
            127_774,
            i32::MAX as i64 - 1,
        ]);
        for seed in seeds {
            let mut s = RandomNumberStream::new_with_column(1, 1);
            s.seed = seed;
            assert_eq!(s.next_random(), schrage(seed), "seed {seed}");
        }
    }

    #[test]
    fn finish_row_burns_like_drawing() {
        for per in [1u32, 2, 7, 100, 255, 256, 300, 1000] {
            for used in [0u32, 1, 5, 99, 254, 255, 299, 1001] {
                let mut fast = RandomNumberStream::new_with_column(5, per);
                let mut slow = fast.clone();
                for _ in 0..used {
                    fast.next_random();
                    slow.next_random();
                }
                fast.finish_row();
                while slow.get_seeds_used() < slow.get_seeds_per_row() {
                    slow.next_random();
                }
                slow.reset_seeds_used();
                assert_eq!(fast.seed(), slow.seed(), "per {per} used {used}");
                assert_eq!(fast.get_seeds_used(), 0);
            }
        }
    }

    #[test]
    fn test_random_stream_creation() {
        let stream = RandomNumberStream::new(1);
        assert_eq!(stream.get_seeds_per_row(), 1);
        assert_eq!(stream.get_seeds_used(), 0);
    }

    #[test]
    fn test_random_stream_with_column() {
        let stream = RandomNumberStream::new_with_column(1, 1);
        assert_eq!(stream.get_seeds_per_row(), 1);

        // Initial seed should be computed based on column number
        assert_ne!(stream.initial_seed, 3); // Should be different from default
    }

    #[test]
    fn test_next_random() {
        let mut stream = RandomNumberStream::new(1);
        let first = stream.next_random();
        let second = stream.next_random();

        // Should generate different values
        assert_ne!(first, second);
        assert_eq!(stream.get_seeds_used(), 2);
    }

    #[test]
    fn test_random_double() {
        let mut stream = RandomNumberStream::new(1);
        let random_double = stream.next_random_double();

        // Should be between 0 and 1
        assert!((0.0..=1.0).contains(&random_double));
    }

    #[test]
    fn test_reset_seed() {
        let mut stream = RandomNumberStream::new(1);
        let initial = stream.next_random();
        stream.next_random(); // Generate another

        stream.reset_seed();
        let after_reset = stream.next_random();

        assert_eq!(initial, after_reset);
        assert_eq!(stream.get_seeds_used(), 1);
    }

    #[test]
    fn test_skip_rows() {
        let mut stream1 = RandomNumberStream::new(2);
        let mut stream2 = RandomNumberStream::new(2);

        // Generate 2 rows manually on stream1
        stream1.next_random();
        stream1.next_random();
        stream1.next_random();
        stream1.next_random();

        // Skip 2 rows on stream2
        stream2.skip_rows(2);

        // Both should now generate the same next value
        let manual = stream1.next_random();
        let skipped = stream2.next_random();
        assert_eq!(manual, skipped);
    }
}
