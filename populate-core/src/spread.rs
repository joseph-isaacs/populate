//! [`Spread`]: `N` values of a row as `N` columns of their own.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;
use std::array::from_fn;

/// `N` values per row stored as `N` separate columns (a table's `f_0` to
/// `f_31`, say), where `[T; N]` keeps them end to end in one column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Spread<T, const N: usize>(pub [T; N]);

/// The columns of [`Spread`]: one set of `T`'s columns per position.
#[derive(Debug, Clone, PartialEq)]
pub struct SpreadCols<C, const N: usize>(pub [C; N]);

impl<C: Default, const N: usize> Default for SpreadCols<C, N> {
    fn default() -> Self {
        SpreadCols(from_fn(|_| C::default()))
    }
}

impl<T: Columnar, const N: usize> Columnar for Spread<T, N> {
    type Columns = SpreadCols<Columns<T>, N>;
}

impl<T, C: Sink<T>, const N: usize> Sink<Spread<T, N>> for SpreadCols<C, N> {
    #[inline]
    fn push(&mut self, value: Spread<T, N>) {
        for (c, v) in self.0.iter_mut().zip(value.0) {
            c.push(v);
        }
    }

    fn reserve(&mut self, additional: usize) {
        for c in &mut self.0 {
            c.reserve(additional);
        }
    }
}
