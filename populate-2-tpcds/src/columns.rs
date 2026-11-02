//! TPC-DS column building blocks: integer-modulo draws, per-table null
//! bitmaps, and a nullable `Int32` sink.

use arrow_array::{ArrayRef, Int32Array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use populate_2::arrow::IntoArrowArray;
use populate_2::hlist::{HCons, HNil};
use populate_2::traits::{Backend, DefaultSink, Sink};
use populate_2_tpch::rng::{MODULUS, Rand, Stream};
use std::sync::Arc;
use tpcdsgen::config::Table as DsTable;

/// dsdgen's per-column seed: `19620718 + column * (i32::MAX / 799)`.
pub fn column_seed(global_column_number: i32) -> i64 {
    let seed = 19620718 + global_column_number as i64 * (i32::MAX as i64 / 799);
    assert!(seed < MODULUS, "seed must be a valid stream state");
    seed
}

/// A column's stream starting at 0-based row `start`.
pub fn stream(global_column_number: i32, seeds_per_row: i32, start: i64) -> Stream {
    Stream::ungrouped(column_seed(global_column_number), seeds_per_row, start)
}

/// `generate_uniform_random_int`: `next % (max - min + 1) + min` (no floats).
#[inline(always)]
pub fn uniform_int(r: &mut Rand, min: i32, max: i32) -> i32 {
    (r.next_rand() as i32) % (max - min + 1) + min
}

/// A table's per-row null bitmap (`create_null_bit_map`), from the table's
/// nulls stream (2 draws per row).
#[derive(Clone, Copy)]
pub struct NullBitmap {
    basis_points: i32,
    not_null: i64,
}

impl NullBitmap {
    pub fn of(table: DsTable) -> Self {
        Self {
            basis_points: table.get_null_basis_points(),
            not_null: table.get_not_null_bit_map(),
        }
    }

    /// The full bitmap (both draws). Columnar code should prefer
    /// [`Self::is_null`], which skips the second draw for most rows.
    #[inline(always)]
    pub fn draw(&self, r: &mut Rand) -> i64 {
        let threshold = uniform_int(r, 0, 9999);
        // generate_uniform_random_key(1, i32::MAX): `next % i32::MAX + 1`
        let bit_map = (r.next_rand() as i32 % i32::MAX + 1) as i64;
        if threshold < self.basis_points {
            bit_map & !self.not_null
        } else {
            0
        }
    }

    /// Whether column `pos` is null in this row. The bitmap draw only matters
    /// when the threshold selects the row, and the stream jumps to the next
    /// row's block either way, so it's skipped otherwise (90%+ of rows).
    #[inline(always)]
    pub fn is_null(&self, r: &mut Rand, pos: u32) -> bool {
        if uniform_int(r, 0, 9999) >= self.basis_points {
            return false;
        }
        let bit_map = (r.next_rand() as i32 % i32::MAX + 1) as i64;
        ((bit_map & !self.not_null) >> pos) & 1 != 0
    }
}

// ============================================================================
// Nullable Int32
// ============================================================================

/// A nullable `Int32` value (row mode).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NullI32(pub Option<i32>);

impl<B: Backend> DefaultSink<B> for NullI32 {
    type Sink = HCons<NullI32Col, HNil>;
}

/// Nullable `Int32` sink. Validity is packed 64 rows at a time into a `u64`
/// (no per-row builder call); the null buffer is only kept if a null appears.
#[derive(Default, Debug)]
pub struct NullI32Col {
    values: Vec<i32>,
    words: Vec<u64>,
    word: u64,
    any_null: bool,
}

impl NullI32Col {
    pub fn reserve(&mut self, n: usize) {
        self.values.reserve(n);
        self.words.reserve(n / 64 + 1);
    }

    #[inline(always)]
    fn push_bit(&mut self, valid: bool) {
        let i = self.values.len() % 64;
        self.word |= (valid as u64) << i;
        if i == 63 {
            self.words.push(self.word);
            self.word = 0;
        }
    }

    #[inline(always)]
    pub fn push_value(&mut self, v: i32) {
        self.push_bit(true);
        self.values.push(v);
    }

    #[inline(always)]
    pub fn push_null(&mut self) {
        self.any_null = true;
        self.push_bit(false);
        self.values.push(0);
    }
}

impl Sink<NullI32> for NullI32Col {
    fn push(&mut self, value: NullI32) {
        match value.0 {
            Some(v) => self.push_value(v),
            None => self.push_null(),
        }
    }
}

impl IntoArrowArray for NullI32Col {
    fn into_array(mut self) -> ArrayRef {
        let len = self.values.len();
        let nulls = self.any_null.then(|| {
            if !len.is_multiple_of(64) {
                self.words.push(self.word);
            }
            let bits = BooleanBuffer::new(Buffer::from_vec(self.words), 0, len);
            NullBuffer::new(bits)
        });
        Arc::new(Int32Array::new(ScalarBuffer::from(self.values), nulls))
    }
}
