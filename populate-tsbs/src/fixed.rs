//! Fixed-width groups of same-typed columns, and naming/flattening them into
//! a flat Arrow `RecordBatch`.
//!
//! populate-2 tuples stop at 4 columns; a TSBS measurement has up to 12 tags
//! and 31 fields. [`Fixed<T, N>`] is one value made of `N` same-typed columns
//! (all the tags of a host, all the fields of a measurement), collected
//! column-wise into [`FixedCols`] (`N` sinks). In Arrow it first becomes a
//! `StructArray`, which [`flatten_named`] splices back into top-level columns.

use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, StructArray, TimestampNanosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use populate_2::arrow::{IntoArrowArray, IntoArrowColumns, into_record_batch};
use populate_2::traits::{Backend, DefaultSink, HCons, HNil, Sink};
use std::array::from_fn;
use std::sync::Arc;

/// `N` values of the same type, one per column.
#[derive(Clone, Debug, PartialEq)]
pub struct Fixed<T, const N: usize>(pub [T; N]);

/// `N` column sinks, one per element of a [`Fixed`].
#[derive(Debug, PartialEq)]
pub struct FixedCols<S, const N: usize>(pub [S; N]);

impl<S: Default, const N: usize> Default for FixedCols<S, N> {
    fn default() -> Self {
        FixedCols(from_fn(|_| S::default()))
    }
}

impl<B: Backend, T, const N: usize> DefaultSink<B> for Fixed<T, N> {
    type Sink = HCons<FixedCols<B::Prim<T>, N>, HNil>;
}

impl<T, S: Sink<T>, const N: usize> Sink<Fixed<T, N>> for FixedCols<S, N> {
    fn push(&mut self, value: Fixed<T, N>) {
        for (sink, v) in self.0.iter_mut().zip(value.0) {
            sink.push(v);
        }
    }
}

/// Becomes a `StructArray` (children `f0..`); see [`flatten_named`].
impl<S: IntoArrowArray, const N: usize> IntoArrowArray for FixedCols<S, N> {
    fn into_array(self) -> ArrayRef {
        let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = self
            .0
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let a = s.into_array();
                (
                    Field::new(format!("f{i}"), a.data_type().clone(), S::NULLABLE),
                    a,
                )
            })
            .unzip();
        Arc::new(StructArray::new(Fields::from(fields), arrays, None))
    }
}

/// Collect a sink into a flat, named `RecordBatch`: struct columns (from
/// [`Fixed`] groups) are spliced into top-level columns, and `names` are
/// applied to the flattened columns in order. If `timestamp_col` is given,
/// that (Int64, nanoseconds) column becomes `Timestamp(ns, "UTC")`.
pub fn flatten_named<S: IntoArrowColumns>(
    sink: S,
    names: &[&str],
    timestamp_col: Option<usize>,
) -> Result<RecordBatch, ArrowError> {
    let batch = into_record_batch(sink)?;
    let mut cols: Vec<(ArrayRef, bool)> = Vec::new();
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        match col.as_any().downcast_ref::<StructArray>() {
            Some(s) => cols.extend(
                s.fields()
                    .iter()
                    .zip(s.columns())
                    .map(|(f, c)| (Arc::clone(c), f.is_nullable())),
            ),
            None => cols.push((Arc::clone(col), field.is_nullable())),
        }
    }
    if cols.len() != names.len() {
        return Err(ArrowError::SchemaError(format!(
            "{} columns but {} names",
            cols.len(),
            names.len()
        )));
    }
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols
        .into_iter()
        .zip(names)
        .enumerate()
        .map(|(i, ((a, nullable), name))| {
            let a = match timestamp_col {
                Some(t) if t == i && a.data_type() == &DataType::Int64 => {
                    let values = a
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Int64")
                        .values()
                        .clone();
                    Arc::new(TimestampNanosecondArray::new(values, None).with_timezone("UTC"))
                        as ArrayRef
                }
                _ => a,
            };
            (Field::new(*name, a.data_type().clone(), nullable), a)
        })
        .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}
