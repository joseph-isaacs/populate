//! Java's `ThreadLocalRandom`, as YCSB's client threads draw from it.

/// Java's `GOLDEN_GAMMA` (SplitMix64's increment).
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// The int stream of one thread's `java.util.concurrent.ThreadLocalRandom`
/// (OpenJDK 17+): the `n`th `nextInt()` after the thread's seed is `seed`
/// is the high half of `mix64(seed + (n + 1) * gamma)`, where `gamma` is
/// `GOLDEN_GAMMA + 2 * thread id`. A counter, so any position is one hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JavaTlr {
    /// The thread's `threadLocalRandomSeed` before its first draw.
    pub seed: u64,
    /// The thread's `Thread.getId()`.
    pub thread_id: u64,
}

impl JavaTlr {
    #[inline(always)]
    pub(crate) fn gamma(self) -> u64 {
        GOLDEN_GAMMA.wrapping_add(self.thread_id << 1)
    }

    /// The seed state that the `n`th (0-based) `nextInt()` mixes.
    #[inline(always)]
    pub(crate) fn state_at(self, n: u64) -> u64 {
        self.seed
            .wrapping_add(n.wrapping_add(1).wrapping_mul(self.gamma()))
    }

    /// `nextLong()` of seed state `s`.
    #[inline(always)]
    pub(crate) fn long_of(s: u64) -> i64 {
        murmur_mix(s) as i64
    }

    /// `nextDouble()` from draw `n`: `ThreadLocalRandom` keeps `Random`'s
    /// two-draw `nextDouble` (`next(26) << 27 | next(27)`), so it takes
    /// draws `n` and `n + 1`.
    #[inline(always)]
    pub(crate) fn double_at(self, n: u64) -> f64 {
        let a = Self::int_of(self.state_at(n)) as u32 >> 6;
        let b = Self::int_of(self.state_at(n + 1)) as u32 >> 5;
        (((a as u64) << 27) + b as u64) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// `nextInt()` of seed state `s`.
    #[inline(always)]
    pub(crate) fn int_of(s: u64) -> i32 {
        (murmur_mix(s) >> 32) as i32
    }

    /// The `n`th (0-based) `nextInt()`.
    #[cfg(test)]
    pub(crate) fn int_at(self, n: u64) -> i32 {
        Self::int_of(self.state_at(n))
    }
}

/// `ThreadLocalRandom.mix64`: MurmurHash3's 64-bit finaliser.
#[inline(always)]
fn murmur_mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    z = (z ^ (z >> 33)).wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    z ^ (z >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_tlr_matches_openjdk_21() {
        // `ThreadLocalRandom.current().nextInt()` x4 on thread 1 after its
        // seed was set to 0, on OpenJDK 21
        let t = JavaTlr {
            seed: 0,
            thread_id: 1,
        };
        let got: Vec<i32> = (0..4).map(|n| t.int_at(n)).collect();
        assert_eq!(got, [-118004909, 1594776516, 1244138524, -2098499193]);
    }

    #[test]
    fn java_tlr_double_and_long() {
        // OpenJDK 21: seed 1, thread 1: nextDouble, nextLong, nextInt, nextDouble
        let t = JavaTlr {
            seed: 1,
            thread_id: 1,
        };
        assert_eq!(t.double_at(0), 0.3744649462336701);
        assert_eq!(JavaTlr::long_of(t.state_at(2)), -5173039928888015872);
        assert_eq!(JavaTlr::int_of(t.state_at(3)), 1275880045);
        assert_eq!(t.double_at(4), 0.27109419679061686);
    }
}
