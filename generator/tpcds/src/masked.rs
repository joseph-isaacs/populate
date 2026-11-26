//! [`Masked`] rows: a row's NULLs as one bitmap, dsdgen's null bitmap,
//! rather than a validity bit pushed per nullable column.
//!
//! The sales channels' rows have 20 to 34 columns, nearly all nullable,
//! and dsdgen decides all of a row's NULLs with one draw. Pushing that
//! bitmap once per row and the values unconditionally costs a fraction of
//! a validity bit per column; each column's validity is read off the
//! bitmaps when the columns become Arrow arrays, and each field's when a
//! row is printed.

#[cfg(feature = "formats")]
use arrow_array::{ArrayRef, make_array};
#[cfg(feature = "formats")]
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
#[cfg(feature = "formats")]
use arrow_schema::DataType;
use populate_core::RowCount;
use populate_core::{Columnar, Columns, Sink};
#[cfg(feature = "formats")]
use populate_sink_arrow::IntoArrowColumns;
#[cfg(feature = "formats")]
use populate_table::text::{Cursor, Encoding, TextColumns};
use std::marker::PhantomData;

/// The null bitmap bit of each column of a table, in column order: the bit
/// that makes it NULL, or [`NOT_NULL`]. A bit may NULL several columns
/// (`store_sales` prints its coupon amount twice).
pub trait NullBits {
    const BITS: &'static [i8];
}

/// A column no bit makes NULL (a NOT NULL field).
pub const NOT_NULL: i8 = -1;

/// Whether bit `bit` of `nbm` makes its column NULL.
#[inline(always)]
fn is_null(nbm: i64, bit: i8) -> bool {
    bit >= 0 && (nbm >> bit) & 1 == 1
}

/// A row of values `T` and its null bitmap; a column's value is ignored
/// where [`NullBits::BITS`] of `M` says the bitmap makes it NULL.
#[derive(Debug, Clone, PartialEq)]
pub struct Masked<T, M> {
    pub nbm: i64,
    pub row: T,
    bits: PhantomData<M>,
}

impl<T: Columnar, M: NullBits> Columnar for Masked<T, M> {
    type Columns = MaskedCols<Columns<T>, M>;
}

/// The columns of [`Masked`] rows: the values, and a bitmap per row.
#[derive(Debug, Clone, PartialEq)]
pub struct MaskedCols<C, M> {
    pub nbm: Vec<i64>,
    pub values: C,
    bits: PhantomData<M>,
}

impl<C: Default, M> Default for MaskedCols<C, M> {
    fn default() -> Self {
        MaskedCols {
            nbm: Vec::new(),
            values: C::default(),
            bits: PhantomData,
        }
    }
}

impl<T, C: Sink<T>, M> Sink<Masked<T, M>> for MaskedCols<C, M> {
    fn push(&mut self, m: Masked<T, M>) {
        self.nbm.push(m.nbm);
        self.values.push(m.row);
    }

    fn reserve(&mut self, n: usize) {
        self.nbm.reserve(n);
        self.values.reserve(n);
    }
}

impl<C, M> RowCount for MaskedCols<C, M> {
    fn rows(&self) -> usize {
        self.nbm.len()
    }
}

#[cfg(feature = "formats")]
impl<C: IntoArrowColumns, M: NullBits> IntoArrowColumns for MaskedCols<C, M> {
    const LEN: usize = C::LEN;

    fn data_types(out: &mut Vec<DataType>) {
        C::data_types(out);
    }

    fn nullables(out: &mut Vec<bool>) {
        out.extend(M::BITS.iter().map(|&b| b != NOT_NULL));
    }

    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        assert_eq!(M::BITS.len(), C::LEN, "a null bit per column");
        let start = out.len();
        self.values.into_arrays(out);
        let valid = validity(&self.nbm, M::BITS);
        for (a, &bit) in out[start..].iter_mut().zip(M::BITS) {
            let Some(valid) = usize::try_from(bit).ok().and_then(|b| valid[b].clone()) else {
                continue;
            };
            let nulls = NullBuffer::new(valid);
            if nulls.null_count() > 0 {
                let data = a.to_data().into_builder().nulls(Some(nulls));
                // SAFETY: the array was valid, and only a null buffer of
                // its own length is added
                *a = make_array(unsafe { data.build_unchecked() });
            }
        }
    }
}

/// For each bit `b` some column uses, whether it leaves each row present:
/// the bitmaps transposed 64 rows at a time, so every bit's validity costs
/// a few operations per 64 rows rather than a few per row.
#[cfg(feature = "formats")]
fn validity(nbm: &[i64], bits: &[i8]) -> Vec<Option<BooleanBuffer>> {
    let used = bits
        .iter()
        .filter(|&&b| b >= 0)
        .fold(0u64, |u, &b| u | 1 << b);
    let mut words: Vec<Vec<u64>> = (0..64)
        .map(|b| match used >> b & 1 {
            1 => Vec::with_capacity(nbm.len().div_ceil(64)),
            _ => Vec::new(),
        })
        .collect();
    for rows in nbm.chunks(64) {
        // rows past the end are NULL in every column, so their bits are 0
        let mut m = [u64::MAX; 64];
        for (m, &r) in m.iter_mut().zip(rows) {
            *m = r as u64;
        }
        transpose(&mut m);
        for (b, w) in words.iter_mut().enumerate() {
            if used >> b & 1 == 1 {
                w.push(!m[b]);
            }
        }
    }
    words
        .into_iter()
        .enumerate()
        .map(|(b, w)| {
            (used >> b & 1 == 1).then(|| BooleanBuffer::new(Buffer::from_vec(w), 0, nbm.len()))
        })
        .collect()
}

/// Transpose a 64 by 64 bit matrix: bit `k` of `m[b]` becomes bit `b` of
/// `m[k]`. Swaps ever smaller blocks across the diagonal (Hacker's
/// Delight, 7-3).
#[cfg(feature = "formats")]
fn transpose(m: &mut [u64; 64]) {
    const MASKS: [u64; 6] = [
        0x0000_0000_FFFF_FFFF,
        0x0000_FFFF_0000_FFFF,
        0x00FF_00FF_00FF_00FF,
        0x0F0F_0F0F_0F0F_0F0F,
        0x3333_3333_3333_3333,
        0x5555_5555_5555_5555,
    ];
    for (i, mask) in MASKS.into_iter().enumerate() {
        let j = 32 >> i;
        for k in 0..64 {
            if k & j == 0 {
                let t = ((m[k] >> j) ^ m[k | j]) & mask;
                m[k] ^= t << j;
                m[k | j] ^= t;
            }
        }
    }
}

#[cfg(feature = "formats")]
impl<C: TextColumns, M: NullBits> TextColumns for MaskedCols<C, M> {
    const WIDTH: usize = C::WIDTH;

    fn text_rows(&self) -> usize {
        self.nbm.len()
    }

    fn text_bytes(&self) -> usize {
        self.values.text_bytes()
    }

    #[inline(always)]
    unsafe fn write_row(&self, r: usize, w: &mut Cursor, encoding: Encoding) {
        // SAFETY: as the caller's, and `r` is a row of `nbm` too
        unsafe {
            let nbm = *self.nbm.get_unchecked(r);
            let null = |j: usize| is_null(nbm, M::BITS[j]);
            self.values.write_row_masked(r, w, encoding, 0, &null);
        }
    }

    #[inline(always)]
    unsafe fn write_row_masked(
        &self,
        r: usize,
        w: &mut Cursor,
        encoding: Encoding,
        first: usize,
        null: &impl Fn(usize) -> bool,
    ) {
        // SAFETY: as the caller's, and `r` is a row of `nbm` too
        unsafe {
            let nbm = *self.nbm.get_unchecked(r);
            let null = |j: usize| null(first + j) || is_null(nbm, M::BITS[j]);
            self.values.write_row_masked(r, w, encoding, 0, &null);
        }
    }
}

#[cfg(all(test, feature = "formats"))]
mod tests {
    use super::*;

    #[test]
    fn transpose_moves_each_bit_across_the_diagonal() {
        let mut m: [u64; 64] =
            std::array::from_fn(|i| (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let want: [u64; 64] =
            std::array::from_fn(|b| (0..64).fold(0, |w, k| w | (m[k] >> b & 1) << k));
        transpose(&mut m);
        assert_eq!(m, want);
    }

    #[test]
    fn validity_is_each_bit_clear() {
        let nbm: Vec<i64> = (0..150).map(|i: i64| i.wrapping_mul(0x2545_F491)).collect();
        let v = validity(&nbm, &[0, NOT_NULL, 5, 0]);
        for b in 0..64 {
            match &v[b] {
                Some(v) => {
                    let want: Vec<bool> = nbm.iter().map(|m| m >> b & 1 == 0).collect();
                    assert_eq!(v.iter().collect::<Vec<_>>(), want, "bit {b}");
                }
                None => assert!(b != 0 && b != 5),
            }
        }
    }
}

impl<C, M: NullBits> MaskedCols<C, M> {
    /// Whether output column `column` is NULL at row `row`.
    pub fn is_null(&self, row: usize, column: usize) -> bool {
        is_null(self.nbm[row], M::BITS[column])
    }
}
