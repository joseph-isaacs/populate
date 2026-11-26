//! Row numbers as a source: for columns whose value at row `r` is a
//! function of `r` alone. A block of them is held as the range it is, so
//! collecting one costs nothing and loops over it count in a register.

use crate::RowRefs;
use populate_core::{Columnar, Distribution, RowCount, Sink};
use rand::Rng;
use std::ops::Range;

/// A row number.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Row(pub u64);

/// Consecutive row numbers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rows(pub Range<u64>);

impl Rows {
    /// How many row numbers.
    pub fn len(&self) -> usize {
        (self.0.end - self.0.start) as usize
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The row numbers, in order.
    pub fn iter(&self) -> Range<u64> {
        self.0.clone()
    }
}

impl Columnar for Row {
    type Columns = Rows;
}

impl Sink<Row> for Rows {
    fn push(&mut self, Row(r): Row) {
        if self.0.is_empty() {
            self.0 = r..r + 1;
        } else {
            assert_eq!(r, self.0.end, "row numbers are consecutive");
            self.0.end += 1;
        }
    }
}

impl RowCount for Rows {
    fn rows(&self) -> usize {
        self.len()
    }
}

/// A row is its number, so `derive(row_numbers(), |r: u64| ..)`.
impl RowRefs for Rows {
    type Ref<'a> = u64;

    fn row_refs(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.clone()
    }

    fn clear(&mut self) {
        self.0 = 0..0;
    }
}

/// The row numbers. See [`row_numbers`].
#[derive(Debug, Clone, Default)]
pub struct RowNumbers {
    next: u64,
}

/// Each row's number, from 0: the source of [`derive`](crate::derive) or
/// [`derive_columns`](crate::derive_columns) for a column that is a
/// function of its row. Skipping moves the counter.
pub fn row_numbers() -> RowNumbers {
    RowNumbers::default()
}

impl Distribution for RowNumbers {
    type Item = Row;

    fn collect_into(&mut self, cols: &mut Rows, _: &mut impl Rng, n: usize) {
        let end = self.next + n as u64;
        if cols.0.is_empty() {
            cols.0 = self.next..end;
        } else {
            assert_eq!(cols.0.end, self.next, "row numbers are consecutive");
            cols.0.end = end;
        }
        self.next = end;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}

/// The bounds a column of a table needs: a [`Distribution`] of `T` that
/// tables can clone into each worker thread.
pub trait Col<T>: Distribution<Item = T> + Clone + Send + Sync + 'static {}

impl<T, D: Distribution<Item = T> + Clone + Send + Sync + 'static> Col<T> for D {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{derive, derive_columns};
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    #[test]
    fn rows_skip_and_derive() {
        let rng = &mut SmallRng::seed_from_u64(0);
        let mut d = derive(row_numbers(), |r: u64| r * 10);
        d.skip(3);
        assert_eq!(d.collect(rng, 3), vec![30, 40, 50]);
        assert_eq!(d.collect(rng, 1), vec![60]);
        let mut w =
            derive_columns::<_, _, u64>(row_numbers(), |rows: &Rows, out: &mut Vec<u64>| {
                out.extend(rows.iter().map(|r| r + 1))
            });
        assert_eq!(w.collect(rng, 2), vec![1, 2]);
        assert_eq!(w.collect(rng, 2), vec![3, 4]);
    }
}
