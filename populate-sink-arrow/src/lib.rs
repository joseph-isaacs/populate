use arrow_array::{
    ArrayRef, ArrowPrimitiveType, ListArray, RecordBatch,
    builder::{BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder},
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use populate_builtins::List;
use populate_core::{Collector, Distribution, ListSink, Rng, SampleIter, Sink};
use std::sync::Arc;

// === RecordBatchBuilder for wide columns ===

/// Builder for creating RecordBatch with arbitrary number of columns.
///
/// This provides an alternative to tuple-based collection for tables with many columns.
/// Each column is generated independently and combined into a RecordBatch.
///
/// # Example
///
/// ```ignore
/// use populate_sink_arrow::RecordBatchBuilder;
/// use populate_builtins::{Inc, Range};
/// use rand::SeedableRng;
/// use rand::rngs::StdRng;
///
/// let mut rng = StdRng::seed_from_u64(42);
/// let batch = RecordBatchBuilder::new(1000, &mut rng)
///     .column::<i64, _>("id", Inc::new(1))
///     .column::<f64, _>("price", Range::new(0.0, 100.0))
///     .column::<i32, _>("quantity", Range::new(1, 100))
///     .build()
///     .unwrap();
/// ```
pub struct RecordBatchBuilder<'a, R: Rng> {
    n: usize,
    rng: &'a mut R,
    columns: Vec<ArrayRef>,
    fields: Vec<Field>,
}

impl<'a, R: Rng> RecordBatchBuilder<'a, R> {
    /// Create a new builder for a RecordBatch with `n` rows.
    pub fn new(n: usize, rng: &'a mut R) -> Self {
        Self {
            n,
            rng,
            columns: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// Add a column with the given name and distribution.
    ///
    /// The type parameter `T` specifies the Rust type that maps to an Arrow type.
    /// Supported types: i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, String.
    pub fn column<T, D>(mut self, name: &str, mut dist: D) -> Self
    where
        T: IntoArrowArray,
        D: Distribution<T>,
    {
        let mut iter = dist.sample();
        let array = T::collect_array(&mut iter, self.rng, self.n);
        self.fields.push(Field::new(name, T::data_type(), false));
        self.columns.push(array);
        self
    }

    /// Add a nullable column with the given name and distribution.
    pub fn nullable_column<T, D>(mut self, name: &str, mut dist: D) -> Self
    where
        T: IntoArrowArray,
        D: Distribution<T>,
    {
        let mut iter = dist.sample();
        let array = T::collect_array(&mut iter, self.rng, self.n);
        self.fields.push(Field::new(name, T::data_type(), true));
        self.columns.push(array);
        self
    }

    /// Add a pre-built ArrayRef as a column.
    ///
    /// Useful for adding columns that were built separately or with custom logic.
    pub fn column_array(mut self, name: &str, data_type: DataType, array: ArrayRef) -> Self {
        self.fields.push(Field::new(name, data_type, false));
        self.columns.push(array);
        self
    }

    /// Get the number of columns added so far.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows this builder will produce.
    pub fn num_rows(&self) -> usize {
        self.n
    }

    /// Build the RecordBatch from the added columns.
    pub fn build(self) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(self.fields));
        RecordBatch::try_new(schema, self.columns)
    }
}

/// Extension trait for Distribution to enable fluent column building.
pub trait DistributionColumnExt<T>: Distribution<T> + Sized {
    /// Collect this distribution into an ArrayRef.
    fn into_array(mut self, rng: &mut impl Rng, n: usize) -> ArrayRef
    where
        T: IntoArrowArray,
    {
        let mut iter = self.sample();
        T::collect_array(&mut iter, rng, n)
    }
}

impl<T, D: Distribution<T>> DistributionColumnExt<T> for D {}

/// Trait for creating Arrow sinks for a type.
pub trait ArrowSinkType: Sized {
    type Sink: Sink<Self, Output = ArrayRef>;
    fn new_sink(capacity: usize) -> Self::Sink;
    fn data_type() -> DataType;
}

/// Primitive sink wrapper.
pub struct PrimitiveSink<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
}

impl<T: ArrowPrimitiveType> PrimitiveSink<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
        }
    }
}

impl<T: ArrowPrimitiveType> Sink<T::Native> for PrimitiveSink<T> {
    type Output = ArrayRef;

    fn push(&mut self, value: T::Native) {
        self.builder.append_value(value);
    }

    fn extend(&mut self, iter: impl Iterator<Item = T::Native>) {
        for value in iter {
            self.builder.append_value(value);
        }
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

macro_rules! impl_arrow_sink_type {
    ($rust_type:ty, $arrow_type:ty, $data_type:expr) => {
        impl ArrowSinkType for $rust_type {
            type Sink = PrimitiveSink<$arrow_type>;

            fn new_sink(capacity: usize) -> Self::Sink {
                PrimitiveSink::new(capacity)
            }

            fn data_type() -> DataType {
                $data_type
            }
        }
    };
}

impl_arrow_sink_type!(i8, Int8Type, DataType::Int8);
impl_arrow_sink_type!(i16, Int16Type, DataType::Int16);
impl_arrow_sink_type!(i32, Int32Type, DataType::Int32);
impl_arrow_sink_type!(i64, Int64Type, DataType::Int64);
impl_arrow_sink_type!(u8, UInt8Type, DataType::UInt8);
impl_arrow_sink_type!(u16, UInt16Type, DataType::UInt16);
impl_arrow_sink_type!(u32, UInt32Type, DataType::UInt32);
impl_arrow_sink_type!(u64, UInt64Type, DataType::UInt64);
impl_arrow_sink_type!(f32, Float32Type, DataType::Float32);
impl_arrow_sink_type!(f64, Float64Type, DataType::Float64);

// Boolean sink
pub struct BooleanSink {
    builder: BooleanBuilder,
}

impl BooleanSink {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(capacity),
        }
    }
}

impl Sink<bool> for BooleanSink {
    type Output = ArrayRef;

    fn push(&mut self, value: bool) {
        self.builder.append_value(value);
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

impl ArrowSinkType for bool {
    type Sink = BooleanSink;

    fn new_sink(capacity: usize) -> Self::Sink {
        BooleanSink::new(capacity)
    }

    fn data_type() -> DataType {
        DataType::Boolean
    }
}

// String sink
pub struct StringSink {
    builder: StringBuilder,
}

impl StringSink {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }
}

impl Sink<String> for StringSink {
    type Output = ArrayRef;

    fn push(&mut self, value: String) {
        self.builder.append_value(value);
    }

    fn finish(mut self) -> Self::Output {
        Arc::new(self.builder.finish())
    }
}

impl ArrowSinkType for String {
    type Sink = StringSink;

    fn new_sink(capacity: usize) -> Self::Sink {
        StringSink::new(capacity)
    }

    fn data_type() -> DataType {
        DataType::Utf8
    }
}

// List sink for Arrow
pub struct ArrowListSink<T: ArrowPrimitiveType> {
    builder: ListBuilder<PrimitiveBuilder<T>>,
}

impl<T: ArrowPrimitiveType> ArrowListSink<T> {
    pub fn new() -> Self {
        Self {
            builder: ListBuilder::new(PrimitiveBuilder::<T>::new()),
        }
    }
}

impl<T: ArrowPrimitiveType> Default for ArrowListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType> ListSink<T::Native> for ArrowListSink<T> {
    type Output = ListArray;

    fn start_list(&mut self) {
        // Nothing to do at start
    }

    fn push_element(&mut self, value: T::Native) {
        self.builder.values().append_value(value);
    }

    fn end_list(&mut self) {
        self.builder.append(true);
    }

    fn finish(mut self) -> Self::Output {
        self.builder.finish()
    }
}

/// Trait for types that can be used in Arrow list sinks.
pub trait ArrowListSinkType: Sized {
    type Sink: ListSink<Self, Output = ListArray>;
    fn new_list_sink() -> Self::Sink;
}

macro_rules! impl_arrow_list_sink_type {
    ($rust_type:ty, $arrow_type:ty) => {
        impl ArrowListSinkType for $rust_type {
            type Sink = ArrowListSink<$arrow_type>;

            fn new_list_sink() -> Self::Sink {
                ArrowListSink::new()
            }
        }
    };
}

impl_arrow_list_sink_type!(i8, Int8Type);
impl_arrow_list_sink_type!(i16, Int16Type);
impl_arrow_list_sink_type!(i32, Int32Type);
impl_arrow_list_sink_type!(i64, Int64Type);
impl_arrow_list_sink_type!(u8, UInt8Type);
impl_arrow_list_sink_type!(u16, UInt16Type);
impl_arrow_list_sink_type!(u32, UInt32Type);
impl_arrow_list_sink_type!(u64, UInt64Type);
impl_arrow_list_sink_type!(f32, Float32Type);
impl_arrow_list_sink_type!(f64, Float64Type);

// === IntoArrowArray trait for unified array collection ===

/// Trait for types that can be collected into Arrow arrays.
/// This unifies scalar types and Vec<T> (lists) for use in RecordBatch tuples.
pub trait IntoArrowArray: Sized {
    /// Collect n samples from an iterator into an ArrayRef.
    fn collect_array<I: SampleIter<Item = Self>>(
        iter: &mut I,
        rng: &mut impl Rng,
        n: usize,
    ) -> ArrayRef;

    /// Get the Arrow DataType for this type.
    fn data_type() -> DataType;
}

// Implement IntoArrowArray for all scalar types via ArrowSinkType
impl<T: ArrowSinkType> IntoArrowArray for T {
    fn collect_array<I: SampleIter<Item = Self>>(
        iter: &mut I,
        rng: &mut impl Rng,
        n: usize,
    ) -> ArrayRef {
        let mut sink = T::new_sink(n);
        sink.extend((0..n).map(|_| iter.next(rng)));
        sink.finish()
    }

    fn data_type() -> DataType {
        T::data_type()
    }
}

// Implement IntoArrowArray for Vec<T> where T can be stored in Arrow lists
impl<T: ArrowListSinkType + ArrowSinkType> IntoArrowArray for Vec<T> {
    fn collect_array<I: SampleIter<Item = Self>>(
        iter: &mut I,
        rng: &mut impl Rng,
        n: usize,
    ) -> ArrayRef {
        let mut sink = T::new_list_sink();
        for _ in 0..n {
            let list = iter.next(rng);
            sink.start_list();
            for elem in list {
                sink.push_element(elem);
            }
            sink.end_list();
        }
        Arc::new(sink.finish())
    }

    fn data_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", T::data_type(), true)))
    }
}

// === Marker types for Arrow collection ===

/// Marker for collecting scalar distributions into ArrayRef.
pub struct ArrayRefW;

/// Marker for collecting tuple distributions into RecordBatch (columnar).
pub struct RecordBatchW;

/// Marker for collecting List distributions into ArrayRef (ListArray).
pub struct ListArrayW;

// === Collector implementations ===

/// Collect scalar Distribution into ArrayRef
impl<T, D> Collector<D, T> for ArrayRefW
where
    T: ArrowSinkType,
    D: Distribution<T>,
{
    type Output = ArrayRef;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut sink = T::new_sink(n);
        sink.extend((0..n).map(|_| iter.next(rng)));
        sink.finish()
    }
}

/// Collect 2-tuple distribution into RecordBatch
impl<T1, T2, D1, D2> Collector<(D1, D2), (T1, T2)> for RecordBatchW
where
    T1: IntoArrowArray,
    T2: IntoArrowArray,
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();

        let a1 = T1::collect_array(&mut iter.0, rng, n);
        let a2 = T2::collect_array(&mut iter.1, rng, n);

        let schema = Schema::new(vec![
            Field::new("c0", T1::data_type(), false),
            Field::new("c1", T2::data_type(), false),
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![a1, a2]).unwrap()
    }
}

/// Collect 3-tuple distribution into RecordBatch
impl<T1, T2, T3, D1, D2, D3> Collector<(D1, D2, D3), (T1, T2, T3)> for RecordBatchW
where
    T1: IntoArrowArray,
    T2: IntoArrowArray,
    T3: IntoArrowArray,
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();

        let a1 = T1::collect_array(&mut iter.0, rng, n);
        let a2 = T2::collect_array(&mut iter.1, rng, n);
        let a3 = T3::collect_array(&mut iter.2, rng, n);

        let schema = Schema::new(vec![
            Field::new("c0", T1::data_type(), false),
            Field::new("c1", T2::data_type(), false),
            Field::new("c2", T3::data_type(), false),
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![a1, a2, a3]).unwrap()
    }
}

/// Collect List distribution into ListArray
impl<T, L, E> Collector<List<L, E>, Vec<T>> for ListArrayW
where
    T: ArrowListSinkType,
    L: Distribution<usize>,
    E: Distribution<T>,
{
    type Output = ListArray;

    fn collect(dist: &mut List<L, E>, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        populate_core::collect_list_into(iter.lengths, iter.elements, rng, n, T::new_list_sink())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use populate_builtins::{Constant, Inc, Range};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_primitive_sink() {
        let mut sink = i32::new_sink(5);
        sink.push(1);
        sink.push(2);
        sink.push(3);
        let array = sink.finish();

        assert_eq!(array.len(), 3);
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(1), 2);
        assert_eq!(int_array.value(2), 3);
    }

    #[test]
    fn test_list_sink() {
        let mut sink = ArrowListSink::<Int32Type>::new();
        sink.start_list();
        sink.push_element(1);
        sink.push_element(2);
        sink.end_list();
        sink.start_list();
        sink.push_element(3);
        sink.end_list();

        let array = sink.finish();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_record_batch_builder_basic() {
        let mut rng = StdRng::seed_from_u64(42);
        let batch = RecordBatchBuilder::new(10, &mut rng)
            .column::<i64, _>("id", Inc::new(1i64))
            .column::<i32, _>("value", Range::new(0, 100))
            .build()
            .unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 10);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(9), 10);
    }

    #[test]
    fn test_record_batch_builder_many_columns() {
        let mut rng = StdRng::seed_from_u64(42);
        let batch = RecordBatchBuilder::new(100, &mut rng)
            .column::<i64, _>("c0", Inc::new(0i64))
            .column::<i32, _>("c1", Range::new(0, 100))
            .column::<f64, _>("c2", Range::new(0.0, 1.0))
            .column::<i64, _>("c3", Constant::new(42i64))
            .column::<i32, _>("c4", Range::new(-50, 50))
            .column::<f64, _>("c5", Range::new(100.0, 200.0))
            .column::<i64, _>("c6", Inc::new(1000i64))
            .column::<i32, _>("c7", Range::new(0, 10))
            .column::<f64, _>("c8", Range::new(-1.0, 1.0))
            .column::<i64, _>("c9", Constant::new(0i64))
            .build()
            .unwrap();

        assert_eq!(batch.num_columns(), 10);
        assert_eq!(batch.num_rows(), 100);

        // Verify column names
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "c0");
        assert_eq!(schema.field(5).name(), "c5");
        assert_eq!(schema.field(9).name(), "c9");
    }

    #[test]
    fn test_record_batch_builder_with_strings() {
        let mut rng = StdRng::seed_from_u64(42);
        let batch = RecordBatchBuilder::new(5, &mut rng)
            .column::<i64, _>("id", Inc::new(1i64))
            .column::<String, _>("name", Constant::new("test".to_string()))
            .column::<f64, _>("score", Range::new(0.0, 100.0))
            .build()
            .unwrap();

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 5);

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "test");
    }

    #[test]
    fn test_record_batch_builder_empty() {
        let mut rng = StdRng::seed_from_u64(42);
        let batch = RecordBatchBuilder::new(0, &mut rng)
            .column::<i64, _>("id", Inc::new(1i64))
            .build()
            .unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_record_batch_builder_deterministic() {
        // Same seed should produce same results
        let mut rng1 = StdRng::seed_from_u64(123);
        let batch1 = RecordBatchBuilder::new(10, &mut rng1)
            .column::<f64, _>("value", Range::new(0.0, 100.0))
            .build()
            .unwrap();

        let mut rng2 = StdRng::seed_from_u64(123);
        let batch2 = RecordBatchBuilder::new(10, &mut rng2)
            .column::<f64, _>("value", Range::new(0.0, 100.0))
            .build()
            .unwrap();

        let col1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let col2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..10 {
            assert_eq!(col1.value(i), col2.value(i));
        }
    }

    #[test]
    fn test_distribution_column_ext() {
        let mut rng = StdRng::seed_from_u64(42);
        let array = Inc::new(1i64).into_array(&mut rng, 5);

        assert_eq!(array.len(), 5);
        let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(4), 5);
    }
}
