//! `Option<T>` rows: a column with missing values.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;

/// The columns of `Option<T>`: `T`'s columns with a value in every row (a
/// missing one is `T::default()`), and whether each row is present.
///
/// A writer that fills `values` itself (a string written in place, say)
/// pushes one `valid` flag per row it writes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Nullable<C> {
    pub values: C,
    /// `true` where the row has a value.
    pub valid: Vec<bool>,
}

impl<C> Nullable<C> {
    /// Rows written so far.
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }
}

impl<T: Columnar + Default> Columnar for Option<T> {
    type Columns = Nullable<Columns<T>>;
}

impl<T: Default, C: Sink<T>> Sink<Option<T>> for Nullable<C> {
    #[inline]
    fn push(&mut self, value: Option<T>) {
        self.valid.push(value.is_some());
        self.values.push(value.unwrap_or_default());
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
        self.valid.reserve(additional);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_rows_hold_the_default() {
        let mut c = Columns::<Option<i32>>::default();
        c.push_n([Some(3), None, Some(5)].into_iter());
        assert_eq!(c.values, [3, 0, 5]);
        assert_eq!(c.valid, [true, false, true]);
        assert_eq!(c.len(), 3);
    }
}
