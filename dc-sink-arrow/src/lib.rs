use arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder};
use arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use dc_core::{ListSink, SampleIter, Sink, TupleIter2, TupleIter3};
use rand::Rng;
use std::sync::Arc;

/// Trait for creating Arrow sinks for a type.
pub trait ArrowSinkType: Sized {
    type Sink: Sink<Self, Output = ArrayRef>;
    fn new_sink(capacity: usize) -> Self::Sink;
    fn data_type() -> DataType;
}

/// Primitive sink wrapper.
pub struct PrimitiveSink<T: arrow::datatypes::ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> PrimitiveSink<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
        }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Sink<T::Native> for PrimitiveSink<T> {
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
pub struct ArrowListSink<T: arrow::datatypes::ArrowPrimitiveType> {
    builder: ListBuilder<PrimitiveBuilder<T>>,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> ArrowListSink<T> {
    pub fn new() -> Self {
        Self {
            builder: ListBuilder::new(PrimitiveBuilder::<T>::new()),
        }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Default for ArrowListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> ListSink<T::Native> for ArrowListSink<T> {
    type Output = ArrayRef;

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
        Arc::new(self.builder.finish())
    }
}

/// Trait for types that can be used in Arrow list sinks.
pub trait ArrowListSinkType: Sized {
    type Sink: ListSink<Self, Output = ArrayRef>;
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

// === RecordBatch collection functions ===

/// Collect a 2-tuple iterator into a RecordBatch.
pub fn collect_record_batch2<I1, I2>(
    mut iter: TupleIter2<I1, I2>,
    rng: &mut impl Rng,
    n: usize,
) -> RecordBatch
where
    I1: SampleIter,
    I2: SampleIter,
    I1::Item: ArrowSinkType,
    I2::Item: ArrowSinkType,
{
    let mut s1 = I1::Item::new_sink(n);
    let mut s2 = I2::Item::new_sink(n);

    s1.extend((0..n).map(|_| iter.0.next(rng)));
    s2.extend((0..n).map(|_| iter.1.next(rng)));

    let schema = Schema::new(vec![
        Field::new("c0", I1::Item::data_type(), false),
        Field::new("c1", I2::Item::data_type(), false),
    ]);

    RecordBatch::try_new(Arc::new(schema), vec![s1.finish(), s2.finish()]).unwrap()
}

/// Collect a 3-tuple iterator into a RecordBatch.
pub fn collect_record_batch3<I1, I2, I3>(
    mut iter: TupleIter3<I1, I2, I3>,
    rng: &mut impl Rng,
    n: usize,
) -> RecordBatch
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I1::Item: ArrowSinkType,
    I2::Item: ArrowSinkType,
    I3::Item: ArrowSinkType,
{
    let mut s1 = I1::Item::new_sink(n);
    let mut s2 = I2::Item::new_sink(n);
    let mut s3 = I3::Item::new_sink(n);

    s1.extend((0..n).map(|_| iter.0.next(rng)));
    s2.extend((0..n).map(|_| iter.1.next(rng)));
    s3.extend((0..n).map(|_| iter.2.next(rng)));

    let schema = Schema::new(vec![
        Field::new("c0", I1::Item::data_type(), false),
        Field::new("c1", I2::Item::data_type(), false),
        Field::new("c2", I3::Item::data_type(), false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![s1.finish(), s2.finish(), s3.finish()],
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};

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
