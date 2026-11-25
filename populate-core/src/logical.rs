//! Row types that carry their logical type, so a schema can be read off a
//! distribution's `Item` without generating anything.
//!
//! Each is a `#[repr(transparent)]` wrapper of its storage value, and its
//! columns are a plain `Vec` of it: the buffer is the storage buffer, and an
//! Arrow sink moves it into a typed array without a copy.

use crate::columnar::Columnar;

/// A date: days since 1970-01-01 (Arrow `Date32`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Date32(pub i32);

/// A decimal of precision `P` and scale `S`: an integer count of `10^-S`
/// (cents for `S = 2`), stored as Arrow `Decimal128(P, S)` stores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Decimal<const P: u8, const S: i8>(pub i128);

impl Columnar for Date32 {
    type Columns = Vec<Date32>;
}

impl<const P: u8, const S: i8> Columnar for Decimal<P, S> {
    type Columns = Vec<Decimal<P, S>>;
}

/// A fixed number of values per row, stored end to end: `n` rows of
/// `[T; N]` are `n * N` values in one buffer. The row type of a stream that
/// reserves `N` draws per row (see [`SeekRng`](crate::SeekRng)).
impl<T, const N: usize> Columnar for [T; N] {
    type Columns = Vec<[T; N]>;
}
