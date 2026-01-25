use rand::Rng;

/// Iterator that produces samples
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Sink that accepts items
pub trait Sink<T> {
    fn push(&mut self, item: T);
}

impl<T> Sink<T> for Vec<T> {
    fn push(&mut self, item: T) {
        Vec::push(self, item);
    }
}

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

// ============================================================================
// IntoNested / IntoFlat - convert flat tuples to nested head-tail style
// ============================================================================

pub trait IntoNested {
    type Nested;
    fn into_nested(self) -> Self::Nested;
}

pub trait IntoFlat {
    type Flat;
    fn into_flat(self) -> Self::Flat;
}

// Identity for 2-tuples (already nested)
impl<S1, S2> IntoNested for (S1, S2) {
    type Nested = (S1, S2);
    fn into_nested(self) -> Self::Nested {
        self
    }
}

impl<S1, S2> IntoFlat for (S1, S2) {
    type Flat = (S1, S2);
    fn into_flat(self) -> Self::Flat {
        self
    }
}

// 3-tuple: (S1, S2, S3) <-> (S1, (S2, S3))
impl<S1, S2, S3> IntoNested for (S1, S2, S3) {
    type Nested = (S1, (S2, S3));
    fn into_nested(self) -> Self::Nested {
        (self.0, (self.1, self.2))
    }
}

impl<S1, S2, S3> IntoFlat for (S1, (S2, S3)) {
    type Flat = (S1, S2, S3);
    fn into_flat(self) -> Self::Flat {
        (self.0, self.1 .0, self.1 .1)
    }
}

// 4-tuple: (S1, S2, S3, S4) <-> (S1, (S2, (S3, S4)))
impl<S1, S2, S3, S4> IntoNested for (S1, S2, S3, S4) {
    type Nested = (S1, (S2, (S3, S4)));
    fn into_nested(self) -> Self::Nested {
        (self.0, (self.1, (self.2, self.3)))
    }
}

impl<S1, S2, S3, S4> IntoFlat for (S1, (S2, (S3, S4))) {
    type Flat = (S1, S2, S3, S4);
    fn into_flat(self) -> Self::Flat {
        (self.0, self.1 .0, self.1 .1 .0, self.1 .1 .1)
    }
}

// Vec is already "flat" (scalar)
impl<T> IntoNested for Vec<T> {
    type Nested = Vec<T>;
    fn into_nested(self) -> Self::Nested {
        self
    }
}

impl<T> IntoFlat for Vec<T> {
    type Flat = Vec<T>;
    fn into_flat(self) -> Self::Flat {
        self
    }
}

/// Distribution trait
pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    /// Collect n items into the sink, returning the sink.
    /// Default implementation uses row_iter (row-by-row).
    fn collect_into<S: Sink<Self::Item>>(
        &mut self,
        rng: &mut impl Rng,
        mut sink: S,
        n: usize,
    ) -> S {
        let mut sample_iter = self.row_iter();
        for _ in 0..n {
            sink.push(sample_iter.next(rng));
        }
        sink
    }
}
