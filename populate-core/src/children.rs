//! [`Children`]: a row that stands for any number of rows of another table.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;

/// The rows one source row expands to: the line items of an order, the
/// suppliers of a part. Its columns hold only the children, end to end, so
/// a distribution of `Children<T>` is a table of `T` rows driven by source
/// rows (`populate_builtins::flat_map` makes one).
///
/// A table built from it has the children as its rows; it cannot be a
/// column next to others, which would have one row per source row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Children<T>(pub Vec<T>);

/// The columns of [`Children`]: the children's own columns, end to end,
/// and how many source rows they came from.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChildCols<V> {
    /// Every child, in order.
    pub values: V,
    /// Source rows pushed so far.
    pub sources: usize,
}

impl<T: Columnar> Columnar for Children<T> {
    type Columns = ChildCols<Columns<T>>;
}

impl<T, V: Sink<T>> Sink<Children<T>> for ChildCols<V> {
    fn push(&mut self, value: Children<T>) {
        self.values.push_n(value.0.into_iter());
        self.sources += 1;
    }
}
