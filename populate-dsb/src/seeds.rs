//! Lehmer steps on raw seeds, for the direct paths that keep a stream's
//! seed in a register instead of a [`dsbgen::random::RandomNumberStream`].
//!
//! The stream is `seed' = 16807 * seed mod (2^31 - 1)`.

const M: u64 = (1 << 31) - 1;
const A: u64 = 16807;

#[inline]
fn mulmod(a: u64, b: u64) -> u64 {
    let p = a * b;
    let x = (p & M) + (p >> 31);
    if x >= M { x - M } else { x }
}

/// Two Lehmer steps.
#[inline]
pub(crate) fn lehmer2(seed: u64) -> u64 {
    mulmod(seed, A * A % M)
}
