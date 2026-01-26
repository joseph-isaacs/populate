use rand::Rng;

// ============================================================================
// Sink Mode markers
// ============================================================================

/// Marker for scalar sinks - supports push() one item at a time
pub struct Scalar;

/// Marker for column sinks - supports push_list() for batched collection
pub struct Column;

/// Iterator that produces samples
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Trait for types that can be split into head and tail.
/// Scalars split to (Self, ()), tuples split to (Head, Tail).
pub trait Splittable {
    type Head: Splittable;
    type Tail: Splittable;
}

/// Extension trait for splitting values into head and tail components
pub trait SplittableExt: Splittable + Sized {
    fn split_head_tail(self) -> (Self::Head, Self::Tail);
}

// Base case: () splits to ((), ())
impl Splittable for () {
    type Head = ();
    type Tail = ();
}

impl SplittableExt for () {
    fn split_head_tail(self) -> ((), ()) {
        ((), ())
    }
}

// Macro to implement Splittable for scalar types
macro_rules! impl_splittable_scalar {
    ($($t:ty),*) => {
        $(
            impl Splittable for $t {
                type Head = $t;
                type Tail = ();
            }
            impl SplittableExt for $t {
                fn split_head_tail(self) -> ($t, ()) {
                    (self, ())
                }
            }
        )*
    };
}

impl_splittable_scalar!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, String);

// Tuple implementations - split into head and tail
impl<T1: Splittable, T2: Splittable> Splittable for (T1, T2) {
    type Head = T1;
    type Tail = T2;
}

impl<T1: Splittable, T2: Splittable> SplittableExt for (T1, T2) {
    fn split_head_tail(self) -> (T1, T2) {
        (self.0, self.1)
    }
}

impl<T1: Splittable, T2: Splittable, T3: Splittable> Splittable for (T1, T2, T3) {
    type Head = T1;
    type Tail = (T2, T3);
}

impl<T1: Splittable, T2: Splittable, T3: Splittable> SplittableExt for (T1, T2, T3) {
    fn split_head_tail(self) -> (T1, (T2, T3)) {
        (self.0, (self.1, self.2))
    }
}

impl<T1: Splittable, T2: Splittable, T3: Splittable, T4: Splittable> Splittable
    for (T1, T2, T3, T4)
{
    type Head = T1;
    type Tail = (T2, T3, T4);
}

impl<T1: Splittable, T2: Splittable, T3: Splittable, T4: Splittable> SplittableExt
    for (T1, T2, T3, T4)
{
    fn split_head_tail(self) -> (T1, (T2, T3, T4)) {
        (self.0, (self.1, self.2, self.3))
    }
}

/// Sink that accepts items and can be split into sub-sinks
pub trait Sink<T: Splittable>: Sized {
    type HeadSink: Sink<T::Head, Mode = Self::Mode>;
    type TailSink: Sink<T::Tail, Mode = Self::Mode>;
    /// Mode marker: Scalar or Column
    type Mode;

    fn push(&mut self, item: T);

    /// Push multiple items at once. Default implementation loops over push.
    /// Column-mode sinks can override for efficiency.
    fn push_list(&mut self, items: impl IntoIterator<Item = T>) {
        for item in items {
            self.push(item);
        }
    }

    /// Split this sink into head and tail sinks (consumes self)
    fn split(self) -> (Self::HeadSink, Self::TailSink);

    /// Reconstruct sink from head and tail sinks
    fn unsplit(head: Self::HeadSink, tail: Self::TailSink) -> Self;
}

/// Unit sink - does nothing, used as tail for scalar sinks
impl Sink<()> for () {
    type HeadSink = ();
    type TailSink = ();
    type Mode = Column; // Unit supports column mode (no-op)

    fn push(&mut self, _: ()) {}

    fn push_list(&mut self, _items: impl IntoIterator<Item = ()>) {}

    fn split(self) -> ((), ()) {
        ((), ())
    }

    fn unsplit(_head: (), _tail: ()) -> Self {}
}

// Vec<T> as sink for scalar T (where T::Tail = ())
impl<T: Splittable<Head = T, Tail = ()>> Sink<T> for Vec<T> {
    type HeadSink = Vec<T>;
    type TailSink = ();
    type Mode = Column; // Vec supports column mode with push_list

    fn push(&mut self, item: T) {
        Vec::push(self, item);
    }

    fn push_list(&mut self, items: impl IntoIterator<Item = T>) {
        self.extend(items);
    }

    fn split(self) -> (Vec<T>, ()) {
        (self, ())
    }

    fn unsplit(head: Vec<T>, _tail: ()) -> Self {
        head
    }
}

// Tuple sink (S1, S2) for tuple item (T1, T2)
// Mode is inherited from sub-sinks (must match)
impl<T1: Splittable, T2: Splittable, S1: Sink<T1>, S2: Sink<T2, Mode = S1::Mode>> Sink<(T1, T2)>
    for (S1, S2)
{
    type HeadSink = S1;
    type TailSink = S2;
    type Mode = S1::Mode;

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
impl<
    T1: Splittable,
    T2: Splittable,
    T3: Splittable,
    S1: Sink<T1>,
    S2: Sink<T2, Mode = S1::Mode>,
    S3: Sink<T3, Mode = S1::Mode>,
> Sink<(T1, T2, T3)> for (S1, S2, S3)
{
    type HeadSink = S1;
    type TailSink = (S2, S3);
    type Mode = S1::Mode;

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
    S2: Sink<T2, Mode = S1::Mode>,
    S3: Sink<T3, Mode = S1::Mode>,
    S4: Sink<T4, Mode = S1::Mode>,
> Sink<(T1, T2, T3, T4)> for (S1, S2, S3, S4)
{
    type HeadSink = S1;
    type TailSink = (S2, S3, S4);
    type Mode = S1::Mode;

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

/// Distribution trait - basic interface for data generators
pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;
}

/// ColumnDistribution trait - distributions that support column-by-column collection
/// Generic over Mode: Scalar uses push(), Column uses push_list()
pub trait ColumnDistribution<M = Column>: Distribution
where
    Self::Item: Splittable,
{
    /// Collect n items into the sink, returning the sink.
    fn collect_into<S>(&mut self, rng: &mut impl Rng, sink: S, n: usize) -> S
    where
        S: Sink<Self::Item, Mode = M>,
        Self: Sized;
}
