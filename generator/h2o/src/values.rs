//! The data sets' row types, their columns, and a table that is one
//! populate [`Distribution`], as Arrow (a [`DistTable`]) and as the
//! scripts' CSV.
//!
//! Every value is a `u32` with [`NA`] for R's `NA`: an [`Id`] is
//! `"id"` and the value (zero-padded to `W` digits), an [`Int`] the value,
//! [`Micros`] the value over 1e6 (`round(x, 6)` makes every double one).
//! The column types say what they become in Arrow, and whether they hold
//! nulls (`NULL`), so a table's schema is read off its distribution; and
//! they write themselves as `fwrite` does, straight from the `u32`s.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use populate_builtins::ReadRows;
use populate_core::{Columnar, Columns, Distribution, Sink};
use populate_sink_arrow::{IntoArrowArray, IntoArrowColumns};
use populate_table::DistTable;
use rand::{Rng, RngCore};
use std::sync::Arc;

use crate::cols::{Bits, Cursor, NA, id_strings, int32, micros, write_with};

/// A row type whose columns are a `Vec<u32>` of its values, NA as [`NA`].
pub trait Value: Copy + From<u32> + Columnar<Columns = Self::Col> {
    type Col: Sink<Self> + Default + CsvColumn + Values;
}

/// Columns whose values are a `Vec<u32>`.
pub trait Values {
    fn values(&self) -> &Vec<u32>;
    fn values_mut(&mut self) -> &mut Vec<u32>;
}

/// A column that writes its values as `fwrite` does.
///
/// # Safety
/// [`write`](Self::write) writes at most [`MAX_BYTES`](Self::MAX_BYTES)
/// bytes, and reads only rows below [`rows`](Self::rows): the CSV writer
/// reserves room by them.
pub unsafe trait CsvColumn {
    /// Most bytes one value takes.
    const MAX_BYTES: usize;

    /// Rows.
    fn rows(&self) -> usize;

    /// Write value `r` (nothing for NA).
    ///
    /// # Safety
    /// Room for [`MAX_BYTES`](Self::MAX_BYTES) bytes, `r` in bounds.
    unsafe fn write(&self, r: usize, w: &mut Cursor);
}

macro_rules! value_type {
    ($(#[$m:meta])* $row:ident, $col:ident, [$($g:tt)*], [$($a:tt)*], $dt:expr,
     $max:expr, |$v:ident, $w:ident| $write:expr, |$c:ident| $array:expr) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Default)]
        pub struct $row<$($g)*>(pub u32);

        /// The columns of the row type of the same name.
        #[derive(Debug, Clone, PartialEq, Default)]
        pub struct $col<$($g)*>(pub Vec<u32>);

        impl<$($g)*> From<u32> for $row<$($a)*> {
            #[inline(always)]
            fn from(v: u32) -> Self {
                $row(v)
            }
        }

        impl<$($g)*> Columnar for $row<$($a)*> {
            type Columns = $col<$($a)*>;
        }

        impl<$($g)*> Value for $row<$($a)*> {
            type Col = $col<$($a)*>;
        }

        impl<$($g)*> Sink<$row<$($a)*>> for $col<$($a)*> {
            #[inline(always)]
            fn push(&mut self, v: $row<$($a)*>) {
                self.0.push(v.0);
            }

            fn reserve(&mut self, n: usize) {
                self.0.reserve(n);
            }

            fn push_n(&mut self, values: impl Iterator<Item = $row<$($a)*>>) {
                self.0.extend(values.map(|v| v.0));
            }
        }

        impl<$($g)*> Values for $col<$($a)*> {
            fn values(&self) -> &Vec<u32> {
                &self.0
            }
            fn values_mut(&mut self) -> &mut Vec<u32> {
                &mut self.0
            }
        }

        impl<$($g)*> ReadRows for $col<$($a)*> {
            type Row = $row<$($a)*>;
            fn rows(&self) -> usize {
                self.0.len()
            }
            fn rows_from(&self, start: usize) -> impl Iterator<Item = Self::Row> + '_ {
                self.0[start..].iter().map(|&v| $row(v))
            }
        }

        impl<$($g)*> IntoArrowArray for $col<$($a)*> {
            fn data_type() -> DataType {
                $dt
            }
            fn nullable() -> bool {
                NULL
            }
            fn into_array(self) -> ArrayRef {
                let $c = self.0;
                $array
            }
        }

        // SAFETY: `$max` bounds every value's text; `write` reads row `r < rows`.
        unsafe impl<$($g)*> CsvColumn for $col<$($a)*> {
            const MAX_BYTES: usize = $max;
            fn rows(&self) -> usize {
                self.0.len()
            }
            #[inline(always)]
            unsafe fn write(&self, r: usize, $w: &mut Cursor) {
                let $v = unsafe { *self.0.get_unchecked(r) };
                if $v != NA {
                    unsafe { $write }
                }
            }
        }
    };
}

value_type!(
    /// `"id"` and the value, zero-padded to `W <= 10` digits (`W = 0`: as
    /// is): Arrow `Utf8View`, every value inline in its view.
    Id, Ids, [const W: usize, const NULL: bool], [W, NULL], DataType::Utf8View,
    {
        const { assert!(W <= 10, "an id longer than an inline view") };
        12
    },
    |v, w| {
        w.bytes(b"id");
        if W == 0 { w.uint(v) } else { w.padded(v, W) }
    },
    |c| id_strings(&c, b"id", W)
);

value_type!(
    /// The value: Arrow `Int32`.
    Int, Ints, [const NULL: bool], [NULL], DataType::Int32,
    10,
    |v, w| w.uint(v),
    |c| int32(c)
);

value_type!(
    /// The value over 1e6: Arrow `Float64`.
    Micros, MicrosCol, [const NULL: bool], [NULL], DataType::Float64,
    16,
    |v, w| w.micros(v),
    |c| micros(&c)
);

/// Columns that write their rows as CSV lines.
pub trait CsvRows {
    /// Append every row, one line each.
    fn write_csv(&self, out: &mut Vec<u8>);
}

macro_rules! csv_rows {
    ($first:ident $f:tt $(, $C:ident $idx:tt)*) => {
        impl<$first: CsvColumn, $($C: CsvColumn),*> CsvRows for ($first, $($C,)*) {
            fn write_csv(&self, out: &mut Vec<u8>) {
                let rows = self.$f.rows();
                $(assert_eq!(self.$idx.rows(), rows, "columns of different lengths");)*
                // each value and its `,` (the first's: the `\n`)
                let line = $first::MAX_BYTES + 1 $(+ $C::MAX_BYTES + 1)*;
                write_with(out, rows * line, |w| unsafe {
                    for r in 0..rows {
                        self.$f.write(r, w);
                        $(
                            w.byte(b',');
                            self.$idx.write(r, w);
                        )*
                        w.byte(b'\n');
                    }
                });
            }
        }
    };
}

csv_rows!(A 0, B 1, C 2);
csv_rows!(A 0, B 1, C 2, D 3, E 4);
csv_rows!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
csv_rows!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);

/// Values in `na` become NA (by value for an id column, by row for a v
/// column).
pub(crate) type NaSet = Option<Arc<Bits>>;

/// `inner` with the values (or the rows) in `na` NA, in place.
#[derive(Clone)]
pub(crate) struct Na<D> {
    inner: D,
    na: NaSet,
    by_row: bool,
    /// Rows `inner` has moved past.
    at: u64,
}

pub(crate) fn na_values<D>(inner: D, na: NaSet) -> Na<D> {
    Na {
        inner,
        na,
        by_row: false,
        at: 0,
    }
}

pub(crate) fn na_rows<D>(inner: D, na: NaSet) -> Na<D> {
    Na {
        inner,
        na,
        by_row: true,
        at: 0,
    }
}

impl<D> Distribution for Na<D>
where
    D: Distribution,
    D::Item: Value,
    Columns<D::Item>: Values,
{
    type Item = D::Item;

    fn sample(&mut self, rng: &mut impl Rng) -> D::Item {
        let mut c = Columns::<D::Item>::default();
        self.collect_into(&mut c, rng, 1);
        D::Item::from(c.values()[0])
    }

    fn collect_into(&mut self, cols: &mut Columns<D::Item>, rng: &mut impl Rng, n: usize) {
        let start = cols.values().len();
        self.inner.collect_into(cols, rng, n);
        if let Some(b) = &self.na {
            let vals = &mut cols.values_mut()[start..];
            if self.by_row {
                for (r, v) in (self.at..).zip(vals) {
                    if b.get(r) {
                        *v = NA;
                    }
                }
            } else {
                for v in vals {
                    if b.get(*v as u64) {
                        *v = NA;
                    }
                }
            }
        }
        self.at += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n);
        self.at += n;
    }
}

/// A table, generated from any range of its rows, as Arrow or as CSV.
pub(crate) trait Table: Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// The same table in Arrow batches of `n` rows.
    fn with_batch_rows(&self, n: u64) -> Arc<dyn Table>;

    /// Rows `first..end` as batches.
    fn arrow(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch));

    /// Rows `first..end` as CSV lines, appended to `out`.
    fn csv(&self, first: u64, end: u64, out: &mut Vec<u8>);
}

impl<D> Table for DistTable<D>
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns + CsvRows,
{
    fn schema(&self) -> SchemaRef {
        DistTable::schema(self).clone()
    }

    fn with_batch_rows(&self, n: u64) -> Arc<dyn Table> {
        Arc::new(self.clone().batch_rows(n))
    }

    fn arrow(&self, first: u64, end: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.range(first, end, emit)
    }

    fn csv(&self, first: u64, end: u64, out: &mut Vec<u8>) {
        let mut d = self.dist().clone();
        d.skip(first);
        let mut rng = NoRng;
        let n = usize::try_from(end - first).expect("chunk fits in memory");
        d.collect(&mut rng, n).write_csv(out);
    }
}

/// `dist` over `rows` rows as a [`Table`].
pub(crate) fn table<D>(name: &str, rows: u64, batch_rows: usize, dist: D) -> Arc<dyn Table>
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns + CsvRows,
{
    Arc::new(DistTable::new(name, rows, dist).batch_rows(batch_rows as u64))
}

/// The CSV header: the schema's names.
pub(crate) fn header(schema: &SchemaRef, out: &mut Vec<u8>) {
    for (i, f) in schema.fields().iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(f.name().as_bytes());
    }
    out.push(b'\n');
}

/// The caller RNG for the tables' distributions, which draw from their own
/// streams (R's), never the caller's.
pub(crate) struct NoRng;

impl RngCore for NoRng {
    fn next_u32(&mut self) -> u32 {
        unreachable!("the tables draw from their own streams")
    }
    fn next_u64(&mut self) -> u64 {
        unreachable!("the tables draw from their own streams")
    }
    fn fill_bytes(&mut self, _: &mut [u8]) {
        unreachable!("the tables draw from their own streams")
    }
}
