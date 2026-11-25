//! [`RowCount`]: how many rows a set of columns holds.

use crate::children::{ChildCols, TableCols};
use crate::list::ListCol;
use crate::nullable::Nullable;
use crate::spread::SpreadCols;
use crate::string::StrCol;
use crate::wide::WideCols;

/// The number of rows written to a set of columns: one per value pushed or
/// collected. For a child table ([`ChildCols`]) a row is a source row, so it
/// counts the rows of the distribution that wrote it, not its children.
pub trait RowCount {
    fn rows(&self) -> usize;
}

impl<T> RowCount for Vec<T> {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl RowCount for StrCol {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl<V> RowCount for ListCol<V> {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl<C> RowCount for Nullable<C> {
    fn rows(&self) -> usize {
        self.valid.len()
    }
}

impl<C: RowCount, const N: usize> RowCount for SpreadCols<C, N> {
    fn rows(&self) -> usize {
        self.0.first().map_or(0, RowCount::rows)
    }
}

impl<C: RowCount> RowCount for WideCols<C> {
    fn rows(&self) -> usize {
        self.0.first().map_or(0, RowCount::rows)
    }
}

impl<V> RowCount for ChildCols<V> {
    fn rows(&self) -> usize {
        self.sources
    }
}

impl<C: RowCount> RowCount for TableCols<C> {
    fn rows(&self) -> usize {
        self.tables.rows()
    }
}
