use rand::Rng;

// ============================================================================
// Sink Mode markers
// ============================================================================

/// Marker for scalar sinks - supports push() one item at a time
pub struct Scalar;

/// Marker for list sinks - supports push_list() with list-specific semantics
pub struct List;

// ============================================================================
// HasMode - derive Mode from item type
// ============================================================================

/// Trait to determine the sink Mode from the item type.
/// Scalars have Scalar mode, Vec/List types have List mode.
pub trait HasMode {
    type Mode;
}

// Scalars have Scalar mode
macro_rules! impl_has_mode_scalar {
    ($($t:ty),*) => {
        $(
            impl HasMode for $t {
                type Mode = Scalar;
            }
        )*
    };
}

impl_has_mode_scalar!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, String);

// Unit has Scalar mode
impl HasMode for () {
    type Mode = Scalar;
}

// Vec<T> items have List mode (we're pushing lists)
impl<T> HasMode for Vec<T> {
    type Mode = List;
}

// Tuples - mode is Scalar (we push tuple values, not lists of tuples)
impl<T1: HasMode, T2: HasMode> HasMode for (T1, T2) {
    type Mode = Scalar;
}

impl<T1: HasMode, T2: HasMode, T3: HasMode> HasMode for (T1, T2, T3) {
    type Mode = Scalar;
}

impl<T1: HasMode, T2: HasMode, T3: HasMode, T4: HasMode> HasMode for (T1, T2, T3, T4) {
    type Mode = Scalar;
}

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

/// Sink that accepts items and can be split into sub-sinks.
/// Each sink has its own Mode (Scalar or List) determined by its item type.
/// When split, sub-sinks may have different Modes.
pub trait Sink<T: Splittable>: Sized {
    type HeadSink: Sink<T::Head>;
    type TailSink: Sink<T::Tail>;
    /// Mode marker: Scalar or List, derived from item type via HasMode
    type Mode;

    fn push(&mut self, item: T);

    /// Split this sink into head and tail sinks (consumes self)
    fn split(self) -> (Self::HeadSink, Self::TailSink);

    /// Reconstruct sink from head and tail sinks
    fn unsplit(head: Self::HeadSink, tail: Self::TailSink) -> Self;
}

/// Extension trait for List-mode sinks that support push_list
pub trait SinkExt<T: Splittable>: Sink<T, Mode = List> {
    fn push_list(&mut self, items: impl IntoIterator<Item = T>);
}

/// Unit sink - does nothing, used as tail for scalar sinks
impl Sink<()> for () {
    type HeadSink = ();
    type TailSink = ();
    type Mode = Scalar;

    fn push(&mut self, _: ()) {}

    fn split(self) -> ((), ()) {
        ((), ())
    }

    fn unsplit(_head: (), _tail: ()) -> Self {}
}

// Vec<T> as sink for scalar T (where T::Tail = ())
// Mode is derived from T via HasMode
impl<T: Splittable<Head = T, Tail = ()> + HasMode> Sink<T> for Vec<T> {
    type HeadSink = Vec<T>;
    type TailSink = ();
    type Mode = <T as HasMode>::Mode;

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

// SinkExt for Vec<T> when T has List mode (i.e., T is Vec<U>)
impl<T: Splittable<Head = T, Tail = ()> + HasMode<Mode = List>> SinkExt<T> for Vec<T> {
    fn push_list(&mut self, items: impl IntoIterator<Item = T>) {
        self.extend(items);
    }
}

// Tuple sink (S1, S2) for tuple item (T1, T2)
// Each sub-sink has its own Mode - they don't need to match!
impl<T1: Splittable, T2: Splittable, S1: Sink<T1>, S2: Sink<T2>> Sink<(T1, T2)> for (S1, S2)
where
    (T1, T2): HasMode,
{
    type HeadSink = S1;
    type TailSink = S2;
    type Mode = <(T1, T2) as HasMode>::Mode;

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
// Each sub-sink has its own Mode
impl<T1: Splittable, T2: Splittable, T3: Splittable, S1: Sink<T1>, S2: Sink<T2>, S3: Sink<T3>>
    Sink<(T1, T2, T3)> for (S1, S2, S3)
where
    (T1, T2, T3): HasMode,
    (T2, T3): HasMode,
{
    type HeadSink = S1;
    type TailSink = (S2, S3);
    type Mode = <(T1, T2, T3) as HasMode>::Mode;

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
// Each sub-sink has its own Mode
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
where
    (T1, T2, T3, T4): HasMode,
    (T2, T3, T4): HasMode,
    (T3, T4): HasMode,
{
    type HeadSink = S1;
    type TailSink = (S2, S3, S4);
    type Mode = <(T1, T2, T3, T4) as HasMode>::Mode;

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

/// ColumnDistribution trait - distributions that support column-by-column collection.
/// Each sink has its own Mode (Scalar or List) determined by its item type.
pub trait ColumnDistribution: Distribution
where
    Self::Item: Splittable,
{
    /// Collect n items into the sink, returning the sink.
    fn collect_into<S: Sink<Self::Item>>(&mut self, rng: &mut impl Rng, sink: S, n: usize) -> S
    where
        Self: Sized;
}
