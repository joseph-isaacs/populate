use arrow_array::{
    ArrayRef, ArrowPrimitiveType, ListArray, RecordBatch,
    builder::{BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder},
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field, Schema};
use populate_builtins::List;
use populate_core::{Collector, Distribution, ListSink, Rng, SampleIter, Sink};
use std::sync::Arc;

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
/// Generates each column fully before moving to the next column.
pub struct RecordBatchW;

/// Marker for collecting tuple distributions into RecordBatch row-by-row.
/// Iterates through rows, pushing values to all column builders simultaneously.
pub struct RecordBatchRowW;

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

/// Collect 2-tuple distribution into RecordBatch row-by-row
impl<T1, T2, D1, D2> Collector<(D1, D2), (T1, T2)> for RecordBatchRowW
where
    T1: ArrowSinkType,
    T2: ArrowSinkType,
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut sink1 = T1::new_sink(n);
        let mut sink2 = T2::new_sink(n);

        // Row-by-row: interleaved writes to all columns
        for _ in 0..n {
            sink1.push(iter.0.next(rng));
            sink2.push(iter.1.next(rng));
        }

        let a1 = sink1.finish();
        let a2 = sink2.finish();

        let schema = Schema::new(vec![
            Field::new("c0", T1::data_type(), false),
            Field::new("c1", T2::data_type(), false),
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![a1, a2]).unwrap()
    }
}

/// Collect 3-tuple distribution into RecordBatch row-by-row
impl<T1, T2, T3, D1, D2, D3> Collector<(D1, D2, D3), (T1, T2, T3)> for RecordBatchRowW
where
    T1: ArrowSinkType,
    T2: ArrowSinkType,
    T3: ArrowSinkType,
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Output = RecordBatch;

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut sink1 = T1::new_sink(n);
        let mut sink2 = T2::new_sink(n);
        let mut sink3 = T3::new_sink(n);

        // Row-by-row: interleaved writes to all columns
        for _ in 0..n {
            sink1.push(iter.0.next(rng));
            sink2.push(iter.1.next(rng));
            sink3.push(iter.2.next(rng));
        }

        let a1 = sink1.finish();
        let a2 = sink2.finish();
        let a3 = sink3.finish();

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
    use arrow_array::{Array, Int32Array};

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
}
