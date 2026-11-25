//! [`Children`]: a row that stands for any number of rows of another
//! table, and [`Tables`]: several such tables made together.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;
use std::ops::{Deref, DerefMut};

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

/// Several child tables of the same source rows, made in one pass: `T` is
/// a tuple of [`Children`], one per table (nested tuples for more than
/// twelve). A plain tuple of [`Children`] is one child table whose columns
/// are its fields; this wrapper is what makes each field its own table.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Tables<T>(pub T);

/// The columns of [`Tables`]: one [`ChildCols`] per table, in `tables`.
/// Field access reaches through to the tuple, so `out.0.values` is the
/// first table's columns.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableCols<C> {
    pub tables: C,
}

impl<C> Deref for TableCols<C> {
    type Target = C;
    fn deref(&self) -> &C {
        &self.tables
    }
}

impl<C> DerefMut for TableCols<C> {
    fn deref_mut(&mut self) -> &mut C {
        &mut self.tables
    }
}

impl<T: Columnar> Columnar for Tables<T> {
    type Columns = TableCols<Columns<T>>;
}

impl<T, C: Sink<T>> Sink<Tables<T>> for TableCols<C> {
    fn push(&mut self, value: Tables<T>) {
        self.tables.push(value.0);
    }
}
