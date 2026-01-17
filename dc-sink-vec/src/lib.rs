use dc_core::{ListSink, Sink};

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
