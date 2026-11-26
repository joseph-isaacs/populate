//! [`RowCount`]: how many rows a set of columns holds, and [`Append`]:
//! moving one set of columns onto the end of another.

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
    /// Rows written so far.
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

/// Columns another set of the same columns can be moved onto the end of:
/// its rows after this one's, column by column. Moving onto empty columns
/// takes the other's buffers as they are.
pub trait Append {
    /// Move `other`'s rows onto the end of these columns.
    fn append_rows(&mut self, other: Self);
}

impl<T> Append for Vec<T> {
    fn append_rows(&mut self, mut other: Self) {
        if self.is_empty() {
            *self = other;
        } else {
            Vec::append(self, &mut other);
        }
    }
}

impl<C: Append> Append for Nullable<C> {
    fn append_rows(&mut self, other: Self) {
        self.values.append_rows(other.values);
        self.valid.append(&other.valid);
    }
}
