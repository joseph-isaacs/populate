//! Native H2O column encodings. Values equal to [`NA`] represent missing data.

pub use crate::cols::NA;
use crate::values::Table;
use populate_core::{Columns, Distribution, Wide, WideCols};
use rand::Rng;
use std::sync::Arc;

/// Meaning of a column's native `u32` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// The string `id` followed by the value, zero-padded to `width` digits.
    Id { width: usize },
    /// Integer values.
    Int,
    /// Fixed-point values with six fractional digits (`value / 1_000_000`).
    Micros,
}

/// A native column's logical type and whether it permits [`NA`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnType {
    pub kind: ColumnKind,
    pub nullable: bool,
}

/// A native group-by or join distribution, retaining the generator's exact
/// `u32` storage instead of materializing strings and floating-point arrays.
#[derive(Clone)]
pub struct Native {
    table: Arc<dyn Table>,
    next: u64,
}

impl Native {
    pub(crate) fn new(table: Arc<dyn Table>) -> Self {
        Self { table, next: 0 }
    }
    pub fn names(&self) -> Vec<String> {
        self.table.names()
    }
    pub fn column_types(&self) -> Vec<ColumnType> {
        self.table.column_types()
    }
}

impl Distribution for Native {
    type Item = Wide<u32>;
    fn columns(&self) -> Columns<Self::Item> {
        WideCols(self.names().iter().map(|_| Vec::new()).collect())
    }
    fn column_names(&self, out: &mut Vec<String>) {
        out.extend(self.names());
    }
    fn collect_into(&mut self, out: &mut Columns<Self::Item>, _rng: &mut impl Rng, n: usize) {
        let values = self.table.raw(self.next, n);
        assert_eq!(out.0.len(), values.len(), "one buffer per native column");
        for (dst, src) in out.0.iter_mut().zip(values) {
            if dst.is_empty() {
                *dst = src;
            } else {
                dst.extend(src);
            }
        }
        self.next += n as u64;
    }
    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}
