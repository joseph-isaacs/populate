use populate_builtins::List;
use populate_core::{Collector, Distribution, ListSink, SampleIter, Sink};
use rand::Rng;

// === Marker types for Vec collection ===

/// Marker for collecting scalar distributions into Vec<T>.
pub struct Col;

/// Marker for collecting tuple distributions into tuple of Vecs (columnar).
pub struct Cols;

/// Marker for collecting List distributions into (Vec<T>, Vec<usize>).
pub struct ListCol;

// === Collector implementations ===

/// Collect any Distribution into Vec<D::Item>
impl<D> Collector<D> for Col
where
    D: Distribution,
{
    type Output = Vec<D::Item>;

    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        (0..n).map(|_| iter.next(rng)).collect()
    }
}

/// Collect 2-tuple distribution into (Vec<T1>, Vec<T2>)
impl<D1, D2> Collector<(D1, D2)> for Cols
where
    D1: Distribution,
    D2: Distribution,
{
    type Output = (Vec<D1::Item>, Vec<D2::Item>);

    fn collect(dist: &mut (D1, D2), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| iter.0.next(rng)));
        v2.extend((0..n).map(|_| iter.1.next(rng)));
        (v1, v2)
    }
}

/// Collect 3-tuple distribution into (Vec<T1>, Vec<T2>, Vec<T3>)
impl<D1, D2, D3> Collector<(D1, D2, D3)> for Cols
where
    D1: Distribution,
    D2: Distribution,
    D3: Distribution,
{
    type Output = (Vec<D1::Item>, Vec<D2::Item>, Vec<D3::Item>);

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Rng, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let mut v1 = Vec::with_capacity(n);
        let mut v2 = Vec::with_capacity(n);
        let mut v3 = Vec::with_capacity(n);
        v1.extend((0..n).map(|_| iter.0.next(rng)));
        v2.extend((0..n).map(|_| iter.1.next(rng)));
        v3.extend((0..n).map(|_| iter.2.next(rng)));
        (v1, v2, v3)
    }
}

/// Collect List distribution into (Vec<T>, Vec<usize>) - values and offsets
impl<L, E> Collector<List<L, E>> for ListCol
where
    L: Distribution<Item = usize>,
    E: Distribution,
{
    type Output = (Vec<E::Item>, Vec<usize>);

    fn collect(dist: &mut List<L, E>, rng: &mut impl Rng, n: usize) -> Self::Output {
        let iter = dist.sample();
        populate_core::collect_list_into(iter.lengths, iter.elements, rng, n, VecListSink::new())
    }
}

// === Sink types ===

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_sink() {
        let mut sink = VecSink::new();
        sink.push(1);
        sink.push(2);
        sink.push(3);
        assert_eq!(sink.finish(), vec![1, 2, 3]);
    }

    #[test]
    fn test_vec_sink_extend() {
        let mut sink = VecSink::new();
        sink.extend(1..=5);
        assert_eq!(sink.finish(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_vec_list_sink() {
        let mut sink = VecListSink::new();
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
