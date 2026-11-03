//! Fixed-width groups of same-typed columns.
//!
//! Tuples stop at 4 columns; wide tables (a TSBS measurement has up to 12 tags
//! and 31 fields) need more. [`Fixed<T, N>`] is one value made of `N`
//! same-typed columns (all the tags of a host, all the fields of a
//! measurement), collected column-wise into [`FixedCols`] (`N` sinks). In
//! Arrow it becomes a `StructArray`, which
//! [`flatten_named`](crate::arrow::flatten_named) splices back into
//! top-level columns.

use crate::traits::{Backend, DefaultSink, HCons, HNil, Sink};
use std::array::from_fn;

/// `N` values of the same type, one per column.
#[derive(Clone, Debug, PartialEq)]
pub struct Fixed<T, const N: usize>(pub [T; N]);

/// `N` column sinks, one per element of a [`Fixed`].
#[derive(Debug, PartialEq)]
pub struct FixedCols<S, const N: usize>(pub [S; N]);

impl<S: Default, const N: usize> Default for FixedCols<S, N> {
    fn default() -> Self {
        FixedCols(from_fn(|_| S::default()))
    }
}

/// Each element goes into `T`'s own column (`StrCol` for `String`,
/// `NullableCol` for `Option`, ...).
impl<B: Backend, T, S, const N: usize> DefaultSink<B> for Fixed<T, N>
where
    T: DefaultSink<B, Sink = HCons<S, HNil>>,
{
    type Sink = HCons<FixedCols<S, N>, HNil>;
}

impl<T, S: Sink<T>, const N: usize> Sink<Fixed<T, N>> for FixedCols<S, N> {
    fn push(&mut self, value: Fixed<T, N>) {
        for (sink, v) in self.0.iter_mut().zip(value.0) {
            sink.push(v);
        }
    }
}
