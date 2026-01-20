//! Sink traits - push-based value accumulators.
//!
//! Sinks receive generated values and accumulate them into output structures.
//! Two variants:
//! - `Sink<S>`: Fully typed sink (Tier 1)
//! - `ErasedSink`: Type-erased sink (Tier 2)

#![allow(dead_code)]

use crate::Schema;
use crate::value::{ErasedValue, FromErased, TypeTag};

/// A typed sink that accumulates values of schema type S.
pub trait Sink<S: Schema> {
    /// The output type produced when the sink is finished.
    type Output;

    /// Reserve space for n additional values.
    fn reserve(&mut self, n: usize);

    /// Push a single value into the sink.
    fn push(&mut self, value: S::Value);

    /// Finish accumulation and produce the output.
    fn finish(self) -> Self::Output;
}

/// A type-erased sink for dynamic/Tier 2 operations.
pub trait ErasedSink: Send {
    /// Get the type tag for values this sink accepts.
    fn type_tag(&self) -> TypeTag;

    /// Reserve space for n additional values.
    fn reserve(&mut self, n: usize);

    /// Push an erased value into the sink.
    ///
    /// Returns false if the value type doesn't match.
    fn push_erased(&mut self, value: ErasedValue) -> bool;
}

/// A Vec-based sink for accumulating values.
pub struct VecSink<T> {
    values: Vec<T>,
}

impl<T> VecSink<T> {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    /// Reserve space for n additional values.
    pub fn reserve_capacity(&mut self, n: usize) {
        self.values.reserve(n);
    }

    /// Push a value into the sink.
    pub fn push_value(&mut self, value: T) {
        self.values.push(value);
    }

    /// Finish accumulation and return the collected values.
    pub fn into_vec(self) -> Vec<T> {
        self.values
    }
}

impl<T> Default for VecSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Schema> Sink<S> for VecSink<S::Value> {
    type Output = Vec<S::Value>;

    fn reserve(&mut self, n: usize) {
        self.values.reserve(n);
    }

    fn push(&mut self, value: S::Value) {
        self.values.push(value);
    }

    fn finish(self) -> Self::Output {
        self.values
    }
}

/// Wrapper to make a typed VecSink work as an ErasedSink.
pub struct ErasedVecSink<S: Schema> {
    sink: VecSink<S::Value>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Schema> ErasedVecSink<S>
where
    S::Value: FromErased,
{
    pub fn new() -> Self {
        Self {
            sink: VecSink::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn finish(self) -> Vec<S::Value> {
        self.sink.into_vec()
    }
}

impl<S: Schema> Default for ErasedVecSink<S>
where
    S::Value: FromErased,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> ErasedSink for ErasedVecSink<S>
where
    S: Schema + Send + 'static,
    S::Value: FromErased + Send,
{
    fn type_tag(&self) -> TypeTag {
        S::type_tag()
    }

    fn reserve(&mut self, n: usize) {
        self.sink.reserve_capacity(n);
    }

    fn push_erased(&mut self, value: ErasedValue) -> bool {
        match S::Value::from_erased(value) {
            Some(v) => {
                self.sink.push_value(v);
                true
            }
            None => false,
        }
    }
}

/// A sink for lists that tracks offsets (columnar representation).
pub struct ListSink<T> {
    /// Flat buffer of all elements across all lists.
    values: Vec<T>,
    /// Offsets marking the start of each list. Length is num_lists + 1.
    offsets: Vec<usize>,
}

impl<T> ListSink<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }

    /// Start a new list.
    pub fn start_list(&mut self) {
        // Nothing to do - current position is implicit
    }

    /// Push an element to the current list.
    pub fn push_element(&mut self, value: T) {
        self.values.push(value);
    }

    /// End the current list.
    pub fn end_list(&mut self) {
        self.offsets.push(self.values.len());
    }

    /// Finish and return (values, offsets).
    pub fn finish(self) -> (Vec<T>, Vec<usize>) {
        (self.values, self.offsets)
    }
}

impl<T> Default for ListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A null sink that discards values (useful for benchmarking).
pub struct NullSink<S: Schema> {
    count: usize,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Schema> NullSink<S> {
    pub fn new() -> Self {
        Self {
            count: 0,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

impl<S: Schema> Default for NullSink<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Schema> Sink<S> for NullSink<S> {
    type Output = usize;

    fn reserve(&mut self, _n: usize) {
        // No-op
    }

    fn push(&mut self, _value: S::Value) {
        self.count += 1;
    }

    fn finish(self) -> Self::Output {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SInt64;

    #[test]
    fn test_vec_sink() {
        let mut sink: VecSink<i64> = VecSink::new();
        <VecSink<i64> as Sink<SInt64>>::push(&mut sink, 1);
        <VecSink<i64> as Sink<SInt64>>::push(&mut sink, 2);
        <VecSink<i64> as Sink<SInt64>>::push(&mut sink, 3);
        let result: Vec<i64> = <VecSink<i64> as Sink<SInt64>>::finish(sink);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_list_sink() {
        let mut sink: ListSink<i64> = ListSink::new();

        sink.start_list();
        sink.push_element(1);
        sink.push_element(2);
        sink.end_list();

        sink.start_list();
        sink.push_element(3);
        sink.end_list();

        let (values, offsets) = sink.finish();
        assert_eq!(values, vec![1, 2, 3]);
        assert_eq!(offsets, vec![0, 2, 3]);
    }
}
