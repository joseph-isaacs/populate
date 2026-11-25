//! Arrow output: collected columns become Arrow arrays.
//!
//! Collection is unchanged: data is generated column by column into plain
//! `Vec`s (which is all an Arrow `PrimitiveBuilder` is internally), already
//! in Arrow's memory layout. At the end the columns are moved into Arrow
//! arrays:
//!
//! | columns                  | Arrow array                                  |
//! |--------------------------|----------------------------------------------|
//! | `Vec<T>` (numeric)       | `PrimitiveArray`, buffer moved, no copy      |
//! | `Vec<bool>`              | `BooleanArray` (bit-packed)                  |
//! | `Vec<usize>`             | `UInt64Array`                                |
//! | `StrCol` (strings)       | `StringArray`, both buffers moved, no copy   |
//! | `ListCol<V>`             | `ListArray`, offsets moved, with V's columns |
//! |                          | as its values; several columns become a      |
//! |                          | `StructArray`                                |
//! | a tuple of columns       | each field's arrays, in order (nested tuples |
//! |                          | are flattened)                               |
//! | top level                | `RecordBatch` (columns `c0`, `c1`, ...)      |
//!
//! So `list<(i32, list<i64>)>` becomes `List<Struct<c0: Int32, c1: List<Int64>>>`.
//! Nothing goes through a per-value `dyn ArrayBuilder` call. With exact
//! up-front `reserve`s, a `list<numeric>` column is written into exactly two
//! allocations (offsets and values) and both are moved into the `ListArray`
//! as they are. `Vec<bool>` and `Vec<usize>` columns are converted
//! (bit-packing / widening).

use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, ListArray, PrimitiveArray, RecordBatch,
    StringArray, StructArray, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, Field, Fields, Schema, SchemaRef};
use populate_core::{Columns, Distribution, ListCol, StrCol};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A column that becomes exactly one Arrow array.
pub trait IntoArrowArray {
    fn into_array(self) -> ArrayRef;
}

/// Rust native type -> Arrow primitive type (zero-copy buffer move).
pub trait ArrowNative: ArrowNativeType {
    type Arrow: ArrowPrimitiveType<Native = Self>;
}

macro_rules! impl_arrow_native {
    ($($t:ty => $a:ty),* $(,)?) => {
        $(impl ArrowNative for $t { type Arrow = $a; })*
    };
}

impl_arrow_native!(
    i8 => Int8Type, i16 => Int16Type, i32 => Int32Type, i64 => Int64Type,
    u8 => UInt8Type, u16 => UInt16Type, u32 => UInt32Type, u64 => UInt64Type,
    f32 => Float32Type, f64 => Float64Type,
);

impl<T: ArrowNative> IntoArrowArray for Vec<T> {
    fn into_array(self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T::Arrow>::new(self.into(), None))
    }
}

impl IntoArrowArray for Vec<bool> {
    fn into_array(self) -> ArrayRef {
        Arc::new(BooleanArray::from(self))
    }
}

impl IntoArrowArray for Vec<usize> {
    fn into_array(self) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(
            self.into_iter().map(|v| v as u64),
        ))
    }
}

/// `StrCol` -> `StringArray`: offsets and bytes are moved, not copied.
impl IntoArrowArray for StrCol {
    fn into_array(self) -> ArrayRef {
        let (offsets, data) = self.into_parts();
        // SAFETY: `StrCol` only accepts UTF-8 (`&str`, `char`, checked ASCII)
        // and its offsets start at 0 and never decrease.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let array = unsafe { StringArray::new_unchecked(offsets, Buffer::from_vec(data), None) };
        Arc::new(array)
    }
}

/// `ListCol` -> `ListArray`: the offsets buffer is moved into the array, no
/// copy.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<V> {
    fn into_array(self) -> ArrayRef {
        let (offsets, values) = self.into_parts();
        let values = columns_to_value_array(values);
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), false));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    }
}

/// Columns as one array: a single column is returned as is; several
/// columns (a tuple) become a `StructArray` with fields `c0, c1, ...`.
pub fn columns_to_value_array<V: IntoArrowColumns>(values: V) -> ArrayRef {
    let mut cols = Vec::with_capacity(V::LEN);
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    Arc::new(StructArray::new(column_fields(&cols), cols, None))
}

/// Non-nullable fields `c0, c1, ...` typed after `cols`.
fn column_fields(cols: &[ArrayRef]) -> Fields {
    cols.iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
        .collect()
}

// ============================================================================
// Columns -> Vec<ArrayRef>
// ============================================================================

/// Columns that become a sequence of Arrow arrays: one array for a single
/// column, and for a tuple the arrays of each field in order.
pub trait IntoArrowColumns {
    /// The number of arrays, so the `Vec` of them is allocated once.
    const LEN: usize;
    fn into_arrays(self, out: &mut Vec<ArrayRef>);
}

impl<C: IntoArrowArray> IntoArrowColumns for C {
    const LEN: usize = 1;
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        out.push(self.into_array());
    }
}

macro_rules! tuple_columns {
    ($($C:ident $idx:tt),+) => {
        impl<$($C: IntoArrowColumns),+> IntoArrowColumns for ($($C,)+) {
            const LEN: usize = 0 $(+ $C::LEN)+;
            fn into_arrays(self, out: &mut Vec<ArrayRef>) {
                $(self.$idx.into_arrays(out);)+
            }
        }
    };
}

tuple_columns!(A 0, B 1);
tuple_columns!(A 0, B 1, C 2);
tuple_columns!(A 0, B 1, C 2, D 3);
tuple_columns!(A 0, B 1, C 2, D 3, E 4);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Top-level columns -> `RecordBatch` with columns `c0, c1, ...`.
pub fn into_record_batch<S: IntoArrowColumns>(sink: S) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::with_capacity(S::LEN);
    sink.into_arrays(&mut cols);
    let schema = Schema::new(column_fields(&cols));
    RecordBatch::try_new(Arc::new(schema), cols)
}

/// [`into_record_batch`] against a schema made once (for example the first
/// batch's `schema()`), so a stream of batches shares one `Arc<Schema>`
/// instead of building names and fields per batch. The columns are checked
/// against it as by `RecordBatch::try_new`.
pub fn into_record_batch_with<S: IntoArrowColumns>(
    schema: &SchemaRef,
    sink: S,
) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::with_capacity(S::LEN);
    sink.into_arrays(&mut cols);
    RecordBatch::try_new(schema.clone(), cols)
}

// ============================================================================
// DistributionArrow — collect straight to a RecordBatch
// ============================================================================

pub trait DistributionArrow: Distribution {
    /// Collect `n` rows column by column and return them as a `RecordBatch`.
    fn collect_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        Columns<Self::Item>: IntoArrowColumns,
    {
        into_record_batch(self.collect(rng, n))
    }

    /// [`collect_record_batch`](Self::collect_record_batch) with a schema
    /// reused across batches: see [`into_record_batch_with`].
    fn collect_record_batch_with(
        &mut self,
        schema: &SchemaRef,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        Columns<Self::Item>: IntoArrowColumns,
    {
        into_record_batch_with(schema, self.collect(rng, n))
    }
}

impl<D: Distribution> DistributionArrow for D {}
