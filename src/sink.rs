use rand::Rng;

use crate::list::ListIter;
use crate::tuple::{TupleIter2, TupleIter3};
use crate::SampleIter;

/// A sink that consumes values and produces an output.
pub trait Sink<T> {
    type Output;

    fn push(&mut self, value: T);

    /// Bulk append. Default uses push, but implementations can override
    /// with optimized bulk operations.
    fn extend(&mut self, iter: impl Iterator<Item = T>) {
        for value in iter {
            self.push(value);
        }
    }

    fn finish(self) -> Self::Output;
}

/// A sink for list values that tracks offsets.
pub trait ListSink<T> {
    type Output;

    fn start_list(&mut self);
    fn push_element(&mut self, value: T);
    fn end_list(&mut self);
    fn finish(self) -> Self::Output;
}

// === Collection functions using Sink ===

/// Collect elements into a sink.
pub fn collect_into<I, S>(mut iter: I, rng: &mut impl Rng, n: usize, mut sink: S) -> S::Output
where
    I: SampleIter,
    S: Sink<I::Item>,
{
    for _ in 0..n {
        sink.push(iter.next(rng));
    }
    sink.finish()
}

/// Collect elements using column-oriented bulk extend.
pub fn collect_into_column<I, S>(mut iter: I, rng: &mut impl Rng, n: usize, mut sink: S) -> S::Output
where
    I: SampleIter,
    S: Sink<I::Item>,
{
    sink.extend((0..n).map(|_| iter.next(rng)));
    sink.finish()
}

/// Collect 2-tuple into separate sinks (column-oriented).
pub fn collect_struct2_into<I1, I2, S1, S2>(
    mut iter: TupleIter2<I1, I2>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
) -> (S1::Output, S2::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
{
    sink1.extend((0..n).map(|_| iter.0.next(rng)));
    sink2.extend((0..n).map(|_| iter.1.next(rng)));
    (sink1.finish(), sink2.finish())
}

/// Collect 3-tuple into separate sinks (column-oriented).
pub fn collect_struct3_into<I1, I2, I3, S1, S2, S3>(
    mut iter: TupleIter3<I1, I2, I3>,
    rng: &mut impl Rng,
    n: usize,
    mut sink1: S1,
    mut sink2: S2,
    mut sink3: S3,
) -> (S1::Output, S2::Output, S3::Output)
where
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    S1: Sink<I1::Item>,
    S2: Sink<I2::Item>,
    S3: Sink<I3::Item>,
{
    sink1.extend((0..n).map(|_| iter.0.next(rng)));
    sink2.extend((0..n).map(|_| iter.1.next(rng)));
    sink3.extend((0..n).map(|_| iter.2.next(rng)));
    (sink1.finish(), sink2.finish(), sink3.finish())
}

/// Collect list into a list sink.
pub fn collect_list_into<L, E, S>(
    mut iter: ListIter<L, E>,
    rng: &mut impl Rng,
    n: usize,
    mut sink: S,
) -> S::Output
where
    L: SampleIter<Item = usize>,
    E: SampleIter,
    S: ListSink<E::Item>,
{
    for _ in 0..n {
        sink.start_list();
        let mut elem_iter = iter.elements.clone();
        let len = iter.lengths.next(rng);
        for _ in 0..len {
            sink.push_element(elem_iter.next(rng));
        }
        sink.end_list();
    }
    sink.finish()
}

// === Vec implementations ===

/// A simple Vec sink.
pub struct VecSink<T> {
    data: Vec<T>,
}

impl<T> VecSink<T> {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }
}

impl<T> Default for VecSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Sink<T> for VecSink<T> {
    type Output = Vec<T>;

    fn push(&mut self, value: T) {
        self.data.push(value);
    }

    fn extend(&mut self, iter: impl Iterator<Item = T>) {
        self.data.extend(iter);
    }

    fn finish(self) -> Self::Output {
        self.data
    }
}

/// A Vec-based list sink that produces (values, offsets).
pub struct VecListSink<T> {
    values: Vec<T>,
    offsets: Vec<usize>,
}

impl<T> VecListSink<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }
}

impl<T> Default for VecListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ListSink<T> for VecListSink<T> {
    type Output = (Vec<T>, Vec<usize>);

    fn start_list(&mut self) {
        // Nothing to do at start
    }

    fn push_element(&mut self, value: T) {
        self.values.push(value);
    }

    fn end_list(&mut self) {
        self.offsets.push(self.values.len());
    }

    fn finish(self) -> Self::Output {
        (self.values, self.offsets)
    }
}

// === Arrow implementations ===

#[cfg(feature = "arrow")]
mod arrow_sinks {
    use super::{ListSink, Sink};
    use arrow::array::{
        ArrayRef, BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder,
    };
    use arrow::datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    };
    use std::sync::Arc;

    /// Trait for creating Arrow sinks for a type.
    pub trait ArrowSinkType: Sized {
        type Sink: Sink<Self, Output = ArrayRef>;
        fn new_sink(capacity: usize) -> Self::Sink;
    }

    // Primitive sink wrapper
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
        ($rust_type:ty, $arrow_type:ty) => {
            impl ArrowSinkType for $rust_type {
                type Sink = PrimitiveSink<$arrow_type>;

                fn new_sink(capacity: usize) -> Self::Sink {
                    PrimitiveSink::new(capacity)
                }
            }
        };
    }

    impl_arrow_sink_type!(i8, Int8Type);
    impl_arrow_sink_type!(i16, Int16Type);
    impl_arrow_sink_type!(i32, Int32Type);
    impl_arrow_sink_type!(i64, Int64Type);
    impl_arrow_sink_type!(u8, UInt8Type);
    impl_arrow_sink_type!(u16, UInt16Type);
    impl_arrow_sink_type!(u32, UInt32Type);
    impl_arrow_sink_type!(u64, UInt64Type);
    impl_arrow_sink_type!(f32, Float32Type);
    impl_arrow_sink_type!(f64, Float64Type);

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
}

#[cfg(feature = "arrow")]
pub use arrow_sinks::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Distribution, List, Range};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_vec_sink() {
        let mut dist = Constant::new(42i32);
        let mut rng = StdRng::seed_from_u64(0);

        let result: Vec<i32> = collect_into(dist.sample(), &mut rng, 5, VecSink::new());
        assert_eq!(result, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_vec_sink_column() {
        let mut dist = Range::new(0i32, 10);
        let mut rng = StdRng::seed_from_u64(0);

        let result: Vec<i32> =
            collect_into_column(dist.sample(), &mut rng, 5, VecSink::with_capacity(5));
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_vec_list_sink() {
        let mut dist = List::new(Constant::new(2usize), Constant::new(7i32));
        let mut rng = StdRng::seed_from_u64(0);

        let (values, offsets) = collect_list_into(dist.sample(), &mut rng, 3, VecListSink::new());
        assert_eq!(values, vec![7, 7, 7, 7, 7, 7]); // 3 lists * 2 elements
        assert_eq!(offsets, vec![0, 2, 4, 6]);
    }

    #[test]
    fn test_struct2_sink() {
        let mut dist = (Constant::new(1i32), Constant::new(2i64));
        let mut rng = StdRng::seed_from_u64(0);

        let (v1, v2) = collect_struct2_into(
            dist.sample(),
            &mut rng,
            3,
            VecSink::new(),
            VecSink::new(),
        );
        assert_eq!(v1, vec![1, 1, 1]);
        assert_eq!(v2, vec![2, 2, 2]);
    }
}

#[cfg(all(test, feature = "arrow"))]
mod arrow_tests {
    use super::*;
    use crate::{Constant, Distribution, List};
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::Int32Type;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_arrow_sink() {
        let mut dist = Constant::new(42i32);
        let mut rng = StdRng::seed_from_u64(0);

        let array = collect_into(dist.sample(), &mut rng, 5, i32::new_sink(5));
        assert_eq!(array.len(), 5);

        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(4), 42);
    }

    #[test]
    fn test_arrow_list_sink() {
        let mut dist = List::new(Constant::new(2usize), Constant::new(7i32));
        let mut rng = StdRng::seed_from_u64(0);

        let array = collect_list_into(
            dist.sample(),
            &mut rng,
            3,
            ArrowListSink::<Int32Type>::new(),
        );
        assert_eq!(array.len(), 3); // 3 lists
    }
}
