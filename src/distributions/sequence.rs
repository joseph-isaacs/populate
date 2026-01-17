//! Sequential distributions that produce incrementing values.

use std::cell::Cell;

use rand::Rng;

use crate::Distribution;

/// A distribution that produces sequential values starting from a given value.
///
/// This is useful for generating unique IDs or monotonically increasing values.
/// Note: This distribution uses interior mutability (`Cell`) to maintain state.
///
/// # Example
///
/// ```
/// use data_comb::{Distribution, Sequence};
/// use rand::thread_rng;
///
/// let dist = Sequence::new(0i64);
/// let mut rng = thread_rng();
/// assert_eq!(dist.sample(&mut rng), 0);
/// assert_eq!(dist.sample(&mut rng), 1);
/// assert_eq!(dist.sample(&mut rng), 2);
/// ```
#[derive(Debug)]
pub struct Sequence<T> {
    current: Cell<T>,
}

impl<T> Sequence<T> {
    /// Create a new sequence starting at `start`.
    pub fn new(start: T) -> Self {
        Sequence {
            current: Cell::new(start),
        }
    }
}

impl<T: Clone> Clone for Sequence<T> {
    fn clone(&self) -> Self {
        Sequence {
            current: Cell::new(self.current.take()),
        }
    }
}

/// Trait for types that can be incremented.
pub trait Incrementable {
    /// Return the next value after `self`.
    fn increment(self) -> Self;
}

macro_rules! impl_incrementable {
    ($($t:ty),*) => {
        $(
            impl Incrementable for $t {
                fn increment(self) -> Self {
                    self + 1
                }
            }
        )*
    };
}

impl_incrementable!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);

impl<T: Copy + Incrementable> Distribution<T> for Sequence<T> {
    fn sample(&self, _rng: &mut impl Rng) -> T {
        let value = self.current.get();
        self.current.set(value.increment());
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_i64() {
        let dist = Sequence::new(0i64);
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), 0);
        assert_eq!(dist.sample(&mut rng), 1);
        assert_eq!(dist.sample(&mut rng), 2);
        assert_eq!(dist.sample(&mut rng), 3);
    }

    #[test]
    fn test_sequence_from_offset() {
        let dist = Sequence::new(100u32);
        let mut rng = rand::thread_rng();
        assert_eq!(dist.sample(&mut rng), 100);
        assert_eq!(dist.sample(&mut rng), 101);
    }
}
