//! A column type particular to dsdgen's tables (TPC-DS, DSB). Their text
//! columns are [`views`](crate::views) columns.

#[cfg(feature = "formats")]
use arrow_array::ArrayRef;
#[cfg(feature = "formats")]
use arrow_schema::DataType;
use populate_core::{Columnar, Decimal as Fixed, RowCount, Sink};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowArray;
#[cfg(feature = "formats")]
use populate_table::text::{Cursor, Encoding, TextField};

/// A GMT offset in whole hours, kept as `decimal(5,2)` and printed in
/// `.dat` files with no decimals (`-5`), as dsdgen does.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Gmt(pub i32);

/// The column of [`Gmt`]: the offsets as `decimal(5,2)` units.
#[derive(Debug, Clone, Default)]
pub struct GmtCol(pub Vec<Fixed<5, 2>>);

impl Columnar for Gmt {
    type Columns = GmtCol;
}

impl Sink<Gmt> for GmtCol {
    #[inline]
    fn push(&mut self, g: Gmt) {
        self.0.push(Fixed(i128::from(g.0) * 100));
    }

    fn reserve(&mut self, n: usize) {
        self.0.reserve(n);
    }
}

impl RowCount for GmtCol {
    fn rows(&self) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "formats")]
impl IntoArrowArray for GmtCol {
    fn data_type() -> DataType {
        <Vec<Fixed<5, 2>>>::data_type()
    }

    fn into_array(self) -> ArrayRef {
        IntoArrowArray::into_array(self.0)
    }
}

#[cfg(feature = "formats")]
impl TextField for GmtCol {
    fn text_rows(&self) -> usize {
        self.0.len()
    }

    fn text_bytes(&self) -> usize {
        TextField::text_bytes(&self.0)
    }

    #[inline]
    unsafe fn write_field(&self, r: usize, w: &mut Cursor, _: Encoding) {
        // SAFETY: as the caller's
        unsafe { w.int((self.0.get_unchecked(r).0 / 100) as i64) }
    }
}
