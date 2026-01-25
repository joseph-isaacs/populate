use rand::Rng;

/// Iterator that produces samples
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Trait for types that can be split into head and tail.
/// Scalars split to (Self, ()), tuples split to (Head, Tail).
pub trait Splittable: 'static {
    type Head: Splittable;
    type Tail: Splittable;
}

// Base case: () splits to ((), ())
impl Splittable for () {
    type Head = ();
    type Tail = ();
}

// Scalar implementations
impl Splittable for i8 {
    type Head = i8;
    type Tail = ();
}

impl Splittable for i16 {
    type Head = i16;
    type Tail = ();
}

impl Splittable for i32 {
    type Head = i32;
    type Tail = ();
}

impl Splittable for i64 {
    type Head = i64;
    type Tail = ();
}

impl Splittable for u8 {
    type Head = u8;
    type Tail = ();
}

impl Splittable for u16 {
    type Head = u16;
    type Tail = ();
}

impl Splittable for u32 {
    type Head = u32;
    type Tail = ();
}

impl Splittable for u64 {
    type Head = u64;
    type Tail = ();
}

impl Splittable for f32 {
    type Head = f32;
    type Tail = ();
}

impl Splittable for f64 {
    type Head = f64;
    type Tail = ();
}

impl Splittable for bool {
    type Head = bool;
    type Tail = ();
}

impl Splittable for String {
    type Head = String;
    type Tail = ();
}

// Tuple implementations - split into head and tail
impl<T1: Splittable, T2: Splittable> Splittable for (T1, T2) {
    type Head = T1;
    type Tail = T2;
}

impl<T1: Splittable, T2: Splittable, T3: Splittable> Splittable for (T1, T2, T3) {
    type Head = T1;
    type Tail = (T2, T3);
}

impl<T1: Splittable, T2: Splittable, T3: Splittable, T4: Splittable> Splittable
    for (T1, T2, T3, T4)
{
    type Head = T1;
    type Tail = (T2, T3, T4);
}

/// Sink that accepts items and can be split into sub-sinks
pub trait Sink<T: Splittable>: Sized + 'static {
    type HeadSink: Sink<T::Head>;
    type TailSink: Sink<T::Tail>;

    fn push(&mut self, item: T);

    /// Split this sink into head and tail sinks (consumes self)
    fn split(self) -> (Self::HeadSink, Self::TailSink);

    /// Reconstruct sink from head and tail sinks
    fn unsplit(head: Self::HeadSink, tail: Self::TailSink) -> Self;
}

/// Unit sink - does nothing, used as tail for scalar sinks
impl Sink<()> for () {
    type HeadSink = ();
    type TailSink = ();

    fn push(&mut self, _: ()) {}

    fn split(self) -> ((), ()) {
        ((), ())
    }

    fn unsplit(_head: (), _tail: ()) -> Self {}
}

// Vec<T> as sink for scalar T (where T::Tail = ())
impl<T: Splittable<Head = T, Tail = ()>> Sink<T> for Vec<T> {
    type HeadSink = Vec<T>;
    type TailSink = ();

    fn push(&mut self, item: T) {
        Vec::push(self, item);
    }

    fn split(self) -> (Vec<T>, ()) {
        (self, ())
    }

    fn unsplit(head: Vec<T>, _tail: ()) -> Self {
        head
    }
}

// Tuple sink (S1, S2) for tuple item (T1, T2)
impl<T1: Splittable, T2: Splittable, S1: Sink<T1>, S2: Sink<T2>> Sink<(T1, T2)> for (S1, S2) {
    type HeadSink = S1;
    type TailSink = S2;

    fn push(&mut self, (a, b): (T1, T2)) {
        self.0.push(a);
        self.1.push(b);
    }

    fn split(self) -> (S1, S2) {
        (self.0, self.1)
    }

    fn unsplit(head: S1, tail: S2) -> Self {
        (head, tail)
    }
}

// Tuple sink (S1, S2, S3) for tuple item (T1, T2, T3)
// TailSink is (S2, S3) to match Tail = (T2, T3)
impl<T1: Splittable, T2: Splittable, T3: Splittable, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>>
    Sink<(T1, T2, T3)> for (S1, S2, S3)
{
    type HeadSink = S1;
    type TailSink = (S2, S3);

    fn push(&mut self, (a, b, c): (T1, T2, T3)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
    }

    fn split(self) -> (S1, (S2, S3)) {
        (self.0, (self.1, self.2))
    }

    fn unsplit(head: S1, tail: (S2, S3)) -> Self {
        (head, tail.0, tail.1)
    }
}

// Tuple sink (S1, S2, S3, S4) for tuple item (T1, T2, T3, T4)
// TailSink is (S2, S3, S4) to match Tail = (T2, T3, T4)
impl<
    T1: Splittable,
    T2: Splittable,
    T3: Splittable,
    T4: Splittable,
    S1: Sink<T1>,
    S2: Sink<T2>,
    S3: Sink<T3>,
    S4: Sink<T4>,
> Sink<(T1, T2, T3, T4)> for (S1, S2, S3, S4)
{
    type HeadSink = S1;
    type TailSink = (S2, S3, S4);

    fn push(&mut self, (a, b, c, d): (T1, T2, T3, T4)) {
        self.0.push(a);
        self.1.push(b);
        self.2.push(c);
        self.3.push(d);
    }

    fn split(self) -> (S1, (S2, S3, S4)) {
        (self.0, (self.1, self.2, self.3))
    }

    fn unsplit(head: S1, tail: (S2, S3, S4)) -> Self {
        (head, tail.0, tail.1, tail.2)
    }
}

/// Distribution trait
pub trait Distribution {
    type Item: Splittable;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    /// Collect n items into the sink, returning the sink.
    /// Default implementation uses row_iter (row-by-row).
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, mut sink: S, n: usize) -> S
    where
        Self: Sized,
    {
        let mut sample_iter = self.row_iter();
        for _ in 0..n {
            sink.push(sample_iter.next(rng));
        }
        sink
    }
}
