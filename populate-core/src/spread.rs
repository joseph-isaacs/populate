//! [`Spread`]: `N` values of a row as `N` columns of their own.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;
use std::array::from_fn;

/// `N` values per row stored as `N` separate columns (a table's `f_0` to
/// `f_31`, say), where `[T; N]` keeps them end to end in one column.
///
/// ```
/// use populate_core::{Columns, Sink, Spread};
///
/// let mut f: Columns<Spread<i32, 3>> = Default::default();
/// f.push(Spread([1, 2, 3]));
/// f.push(Spread([4, 5, 6]));
/// assert_eq!(f.0, [vec![1, 4], vec![2, 5], vec![3, 6]]);
///
/// let mut packed: Columns<[i32; 3]> = Default::default();
/// packed.push([1, 2, 3]);
/// packed.push([4, 5, 6]);
/// assert_eq!(packed, [[1, 2, 3], [4, 5, 6]]);
/// ```
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
