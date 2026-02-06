use rand::Rng;

/// Iterator that produces samples
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Distribution trait - basic interface for data generators
pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;
}

// ============================================================================
// Sink<T> — accepts values via push. Works for both primitives and tuples.
//
// - Primitive sink: Vec<f64> impl Sink<f64>
// - Tuple sink: (S1, S2) impl Sink<(T1, T2)> — destructures and pushes
// - List sink: see ListSink<T> below
// ============================================================================

pub trait Sink<T> {
    fn push(&mut self, value: T);
}

// Vec<T> is a primitive sink
impl<T> Sink<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value);
    }
}

// Tuple sinks — destructure row and push to sub-sinks (for row-by-row / Map)

impl<T1, T2, S1: Sink<T1>, S2: Sink<T2>> Sink<(T1, T2)> for (S1, S2) {
    fn push(&mut self, (a, b): (T1, T2)) {
        self.0.push(a);
        self.1.push(b);
    }
}

impl<T1, T2, T3, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>> Sink<(T1, T2, T3)> for (S1, S2, S3) {
    fn push(&mut self, (a, b, c): (T1, T2, T3)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
    }
}

impl<T1, T2, T3, T4, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>, S4: Sink<T4>> Sink<(T1, T2, T3, T4)>
    for (S1, S2, S3, S4)
{
    fn push(&mut self, (a, b, c, d): (T1, T2, T3, T4)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
    }
}

// ============================================================================
// ListSink<T> — sink for list/array columns.
//
// Accepts fixed-size element iterators. Zero allocation — elements flow
// directly from distribution iterator to the underlying storage.
//
// Works for both:
// - Vec-based: (Vec<T>, Vec<usize>) storing values + offsets
// - Arrow-based: ListBuilder<PrimitiveBuilder<T>>
// ============================================================================

pub trait ListSink<T> {
    /// Push one list of `len` elements from the iterator.
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>);
}

/// Vec-based list sink: flat values buffer + offsets.
pub struct VecListSink<T> {
    pub values: Vec<T>,
    pub offsets: Vec<usize>,
}

impl<T> VecListSink<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }

    pub fn into_parts(self) -> (Vec<T>, Vec<usize>) {
        (self.values, self.offsets)
    }
}

impl<T> Default for VecListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ListSink<T> for VecListSink<T> {
    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.values.extend(elems);
        self.offsets.push(self.values.len());
    }
}

// ============================================================================
// CollectInto<S> — distribution-driven collection into a typed sink.
//
// The distribution knows its structure and fills the sink accordingly:
// - Scalar dists: iterate and push
// - Tuple dists (independent): split sink, fill each column, reassemble
// - Map (dependent): iterate row-by-row, push via Sink<T>
// - List dists: iterate, push_list with element iterators
// - Unnest: regroup middle sinks, delegate to inner dist
//
// Sink is owned (taken and returned) so tuple/unnest impls can freely
// destructure and reassemble the sink structure.
// ============================================================================

pub trait CollectInto<S> {
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S;
}
