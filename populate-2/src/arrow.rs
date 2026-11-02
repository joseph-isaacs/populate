//! Arrow output for the `VecBackend`.
//!
//! Collection is unchanged: data is generated column by column into plain
//! `Vec`s (which is all an Arrow `PrimitiveBuilder` is internally). At the end
//! the sink HList is converted into Arrow arrays:
//!
//! | sink                     | Arrow array                                  |
//! |--------------------------|----------------------------------------------|
//! | `Vec<T>` (numeric)       | `PrimitiveArray`, buffer moved, no copy      |
//! | `Vec<bool>`              | `BooleanArray` (bit-packed)                  |
//! | `Vec<usize>`             | `UInt64Array`                                |
//! | `ListCol<offsets, V>`    | `ListArray` with V's columns as its values;  |
//! |                          | several columns become a `StructArray`       |
//! | top-level HList          | `RecordBatch` (columns `c0`, `c1`, ...)      |
//!
//! So `list<(i32, list<i64>)>` becomes `List<Struct<c0: Int32, c1: List<Int64>>>`.
//! Nothing goes through a per-value `dyn ArrayBuilder` call.

use crate::hlist::{HCons, HNil};
use crate::traits::{DefaultSink, Distribution, ListCol, VecBackend};
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, ListArray, PrimitiveArray, RecordBatch,
    StructArray, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::{ArrowError, Field, Fields, Schema};
use rand::Rng;
use std::sync::Arc;

// ============================================================================
// Single column -> ArrayRef
// ============================================================================

/// A sink that becomes exactly one Arrow array.
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

/// `ListCol` -> `ListArray`. Offsets are stored as end offsets (`n` entries);
/// Arrow wants `n + 1` starting at 0, as i32.
impl<V: IntoArrowColumns> IntoArrowArray for ListCol<Vec<usize>, V> {
    fn into_array(self) -> ArrayRef {
        let mut offsets = Vec::with_capacity(self.offsets.len() + 1);
        offsets.push(0i32);
        offsets.extend(self.offsets.iter().map(|&o| {
            i32::try_from(o).expect("list values exceed i32 offsets; LargeList not supported yet")
        }));

        let values = columns_to_value_array(self.values);
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), false));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        ))
    }
}

/// One column is the list's values directly; several columns (a tuple element)
/// become a `StructArray`.
fn columns_to_value_array<V: IntoArrowColumns>(values: V) -> ArrayRef {
    let mut cols = Vec::new();
    values.into_arrays(&mut cols);
    if cols.len() == 1 {
        return cols.pop().expect("one column");
    }
    let fields: Fields = cols
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
        .collect();
    Arc::new(StructArray::new(fields, cols, None))
}

// ============================================================================
// HList of sinks -> Vec<ArrayRef>
// ============================================================================

/// A sink HList that becomes a sequence of Arrow arrays, one per column.
pub trait IntoArrowColumns {
    fn into_arrays(self, out: &mut Vec<ArrayRef>);
}

impl IntoArrowColumns for HNil {
    fn into_arrays(self, _out: &mut Vec<ArrayRef>) {}
}

impl<H: IntoArrowArray, T: IntoArrowColumns> IntoArrowColumns for HCons<H, T> {
    fn into_arrays(self, out: &mut Vec<ArrayRef>) {
        out.push(self.0.into_array());
        self.1.into_arrays(out);
    }
}

/// Top-level sink HList -> `RecordBatch` with columns `c0, c1, ...`.
pub fn into_record_batch<S: IntoArrowColumns>(sink: S) -> Result<RecordBatch, ArrowError> {
    let mut cols = Vec::new();
    sink.into_arrays(&mut cols);
    let fields: Vec<Field> = cols
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), false))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
}

// ============================================================================
// DistributionArrow — collect straight to a RecordBatch
// ============================================================================

pub trait DistributionArrow: Distribution<VecBackend> {
    /// Collect `n` rows column by column and return them as a `RecordBatch`.
    /// No sink type to spell out: it's derived from `Self::Item`.
    fn collect_record_batch(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
    ) -> Result<RecordBatch, ArrowError>
    where
        <Self::Item as DefaultSink<VecBackend>>::Sink: Default + IntoArrowColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        into_record_batch(sink)
    }
}

impl<D: Distribution<VecBackend>> DistributionArrow for D {}
