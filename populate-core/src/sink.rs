/// Something values of type `T` are appended to: a column buffer, or a tuple
/// of them taking tuples.
pub trait Sink<T> {
    /// Append one value.
    fn push(&mut self, value: T);

    /// Hint that `additional` more values are about to be pushed. Columnar
    /// collection always knows the exact count, so buffers are allocated once
    /// at their final size and never grown (no realloc copies).
    fn reserve(&mut self, _additional: usize) {}

    /// Push every value of `values`.
    fn push_n(&mut self, values: impl Iterator<Item = T>) {
        values.for_each(|v| self.push(v));
    }
}

impl<T> Sink<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value);
    }

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional);
    }

    fn push_n(&mut self, values: impl Iterator<Item = T>) {
        Vec::extend(self, values)
    }
}
