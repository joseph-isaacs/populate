// Re-export rand traits
pub use rand::{Rng, RngCore};

// === Collection traits ===

/// Trait for a collector that can collect a distribution D into an output.
/// The collector type C is a marker (e.g., `Arrow`, `Vecs`) that determines
/// the output family. The associated `Output` type is the actual result.
/// T is the item type produced by the distribution.
pub trait Collector<D, T>: Sized {
    type Output;
    fn collect(dist: &mut D, rng: &mut impl Rng, n: usize) -> Self::Output;
}

/// Extension trait that adds `collect` method to all distributions.
pub trait CollectExt<T>: Distribution<T> {
    fn collect<C>(&mut self, rng: &mut impl Rng, n: usize) -> C::Output
    where
        C: Collector<Self, T>,
        Self: Sized,
    {
        C::collect(self, rng, n)
    }
}

// Blanket impl for all Distribution<T>
impl<T, D: Distribution<T>> CollectExt<T> for D {}

// === Core traits ===

/// An infinite iterator that produces values using an RNG.
///
/// # Reset semantics
///
/// Iterators must implement `reset()` to restore themselves to their initial state.
/// After calling `reset()`, the iterator must produce the same sequence
/// (given the same RNG state) as a freshly created iterator.
///
/// This invariant is used by list collectors to ensure each list gets
/// independent element generation - the element iterator is reset before
/// generating each list's values.
///
/// For stateless iterators (e.g., `Range`, `Constant`), `reset()` is a no-op.
/// For stateful iterators (e.g., `Cycle`, `Inc`), `reset()` must restore
/// any accumulated state to its initial condition.
pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
    /// Reset the iterator to its initial state.
    fn reset(&mut self);
}

/// A factory that produces sample iterators.
///
/// Uses GATs to allow iterators to borrow from the distribution,
/// avoiding allocations when creating iterators.
pub trait Distribution<T> {
    type Iter<'a>: SampleIter<Item = T>
    where
        Self: 'a;
    fn sample(&mut self) -> Self::Iter<'_>;
}

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

// === Tuple iterators and Distribution impls ===
// These must be in the same crate as Distribution due to orphan rules.

/// Iterator for 2-tuple distributions.
pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
    }
}

impl<T1, T2, D1, D2> Distribution<(T1, T2)> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Iter<'a>
        = TupleIter2<D1::Iter<'a>, D2::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter2(self.0.sample(), self.1.sample())
    }
}

/// Iterator for 3-tuple distributions.
pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
        self.2.reset();
    }
}

impl<T1, T2, T3, D1, D2, D3> Distribution<(T1, T2, T3)> for (D1, D2, D3)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
{
    type Iter<'a>
        = TupleIter3<D1::Iter<'a>, D2::Iter<'a>, D3::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter3(self.0.sample(), self.1.sample(), self.2.sample())
    }
}

/// Iterator for 4-tuple distributions.
pub struct TupleIter4<I1, I2, I3, I4>(pub I1, pub I2, pub I3, pub I4);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter> SampleIter
    for TupleIter4<I1, I2, I3, I4>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
        )
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
        self.2.reset();
        self.3.reset();
    }
}

impl<T1, T2, T3, T4, D1, D2, D3, D4> Distribution<(T1, T2, T3, T4)> for (D1, D2, D3, D4)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
{
    type Iter<'a>
        = TupleIter4<D1::Iter<'a>, D2::Iter<'a>, D3::Iter<'a>, D4::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter4(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
        )
    }
}

/// Iterator for 5-tuple distributions.
pub struct TupleIter5<I1, I2, I3, I4, I5>(pub I1, pub I2, pub I3, pub I4, pub I5);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter, I5: SampleIter> SampleIter
    for TupleIter5<I1, I2, I3, I4, I5>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item, I5::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
        )
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
        self.2.reset();
        self.3.reset();
        self.4.reset();
    }
}

impl<T1, T2, T3, T4, T5, D1, D2, D3, D4, D5> Distribution<(T1, T2, T3, T4, T5)>
    for (D1, D2, D3, D4, D5)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
{
    type Iter<'a>
        = TupleIter5<D1::Iter<'a>, D2::Iter<'a>, D3::Iter<'a>, D4::Iter<'a>, D5::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter5(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
        )
    }
}

/// Iterator for 6-tuple distributions.
pub struct TupleIter6<I1, I2, I3, I4, I5, I6>(pub I1, pub I2, pub I3, pub I4, pub I5, pub I6);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter, I5: SampleIter, I6: SampleIter>
    SampleIter for TupleIter6<I1, I2, I3, I4, I5, I6>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item, I5::Item, I6::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
            self.5.next(rng),
        )
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
        self.2.reset();
        self.3.reset();
        self.4.reset();
        self.5.reset();
    }
}

impl<T1, T2, T3, T4, T5, T6, D1, D2, D3, D4, D5, D6> Distribution<(T1, T2, T3, T4, T5, T6)>
    for (D1, D2, D3, D4, D5, D6)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<T6>,
{
    type Iter<'a>
        = TupleIter6<
        D1::Iter<'a>,
        D2::Iter<'a>,
        D3::Iter<'a>,
        D4::Iter<'a>,
        D5::Iter<'a>,
        D6::Iter<'a>,
    >
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter6(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.sample(),
        )
    }
}

/// Iterator for 7-tuple distributions.
pub struct TupleIter7<I1, I2, I3, I4, I5, I6, I7>(
    pub I1,
    pub I2,
    pub I3,
    pub I4,
    pub I5,
    pub I6,
    pub I7,
);

impl<
    I1: SampleIter,
    I2: SampleIter,
    I3: SampleIter,
    I4: SampleIter,
    I5: SampleIter,
    I6: SampleIter,
    I7: SampleIter,
> SampleIter for TupleIter7<I1, I2, I3, I4, I5, I6, I7>
{
    type Item = (
        I1::Item,
        I2::Item,
        I3::Item,
        I4::Item,
        I5::Item,
        I6::Item,
        I7::Item,
    );

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
            self.4.next(rng),
            self.5.next(rng),
            self.6.next(rng),
        )
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
        self.2.reset();
        self.3.reset();
        self.4.reset();
        self.5.reset();
        self.6.reset();
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, D1, D2, D3, D4, D5, D6, D7>
    Distribution<(T1, T2, T3, T4, T5, T6, T7)> for (D1, D2, D3, D4, D5, D6, D7)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    D4: Distribution<T4>,
    D5: Distribution<T5>,
    D6: Distribution<T6>,
    D7: Distribution<T7>,
{
    type Iter<'a>
        = TupleIter7<
        D1::Iter<'a>,
        D2::Iter<'a>,
        D3::Iter<'a>,
        D4::Iter<'a>,
        D5::Iter<'a>,
        D6::Iter<'a>,
        D7::Iter<'a>,
    >
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleIter7(
            self.0.sample(),
            self.1.sample(),
            self.2.sample(),
            self.3.sample(),
            self.4.sample(),
            self.5.sample(),
            self.6.sample(),
        )
    }
}

// === Unnest marker for tuple spreading ===

/// Marker wrapper that tells tuple combinators to spread the inner distribution's output.
///
/// When used in a tuple like `(D1, Unnest<D2>)` where D2 produces (B, C),
/// the result is flattened to (A, B, C) instead of (A, (B, C)).
pub struct Unnest<D>(pub D);

/// Convenience function to create an Unnest marker.
pub fn unnest<D>(inner: D) -> Unnest<D> {
    Unnest(inner)
}

/// Iterator for (A, Unnest<(B, C)>) -> (A, B, C).
pub struct TupleUnnest2Iter<I1, I2>(pub I1, pub I2);

impl<I1, I2, A, B, C> SampleIter for TupleUnnest2Iter<I1, I2>
where
    I1: SampleIter<Item = A>,
    I2: SampleIter<Item = (B, C)>,
{
    type Item = (A, B, C);

    fn next(&mut self, rng: &mut impl Rng) -> (A, B, C) {
        let a = self.0.next(rng);
        let (b, c) = self.1.next(rng);
        (a, b, c)
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
    }
}

impl<D1, D2, A, B, C> Distribution<(A, B, C)> for (D1, Unnest<D2>)
where
    D1: Distribution<A>,
    D2: Distribution<(B, C)>,
{
    type Iter<'a>
        = TupleUnnest2Iter<D1::Iter<'a>, D2::Iter<'a>>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        TupleUnnest2Iter(self.0.sample(), self.1.0.sample())
    }
}

// === Collection functions using Sink ===

/// Collect elements into a sink (row-oriented).
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
pub fn collect_into_column<I, S>(
    mut iter: I,
    rng: &mut impl Rng,
    n: usize,
    mut sink: S,
) -> S::Output
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
    mut lengths: L,
    mut elements: E,
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
        elements.reset();
        let len = lengths.next(rng);
        for _ in 0..len {
            sink.push_element(elements.next(rng));
        }
        sink.end_list();
    }
    sink.finish()
}

// === Dynamic dispatch support ===
//
// The GAT-based `Distribution` trait is not object-safe. These types provide
// an opt-in escape hatch for cases requiring runtime polymorphism (e.g.,
// schema defined by config file, heterogeneous collections).
//
// Usage:
// ```
// let dyn_dist: DynDist<i32> = DynDist::new(range(1, 100));
// let mut table = (dyn_dist, constant(42));  // mixes dynamic and static
// ```

/// Object-safe version of `SampleIter` using dynamic dispatch for RNG.
///
/// Uses `RngCore` instead of `Rng` because `Rng` has generic methods
/// that make it not object-safe.
///
/// Unlike `SampleIter`, this trait cannot have a blanket impl because
/// `SampleIter::next` takes `impl Rng` (Sized), but `DynSampleIter::next_dyn`
/// receives `&mut dyn RngCore` (unsized). Implement this trait for specific
/// iterator types that need dynamic dispatch support.
pub trait DynSampleIter {
    type Item;
    fn next_dyn(&mut self, rng: &mut dyn RngCore) -> Self::Item;
    fn reset_dyn(&mut self);
}

/// Wrapper that turns `&mut dyn RngCore` into a sized `impl Rng`.
///
/// This allows passing dynamically-dispatched RNGs to functions that
/// expect `impl Rng` (which requires Sized).
pub struct DynRng<'a>(pub &'a mut dyn RngCore);

impl RngCore for DynRng<'_> {
    fn next_u32(&mut self) -> u32 {
        self.0.next_u32()
    }
    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }
}

// DynRng implements Rng via the blanket impl: impl<R: RngCore + ?Sized> Rng for R

/// Object-safe trait for distributions that produce boxed iterators.
///
/// This trait enables runtime polymorphism at the cost of allocation.
/// Implement this for types that need to be stored in `dyn` contexts.
pub trait DynDistribution<T> {
    fn sample_boxed(&mut self) -> Box<dyn DynSampleIter<Item = T> + '_>;
}

/// Wrapper that adapts a `DynDistribution` into the GAT-based `Distribution`.
///
/// This allows dynamic distributions to compose with static ones:
/// ```ignore
/// let dynamic = DynDist::new(range(1, 100));
/// let static_dist = constant(42);
/// let mut combined = (dynamic, static_dist);
/// ```
pub struct DynDist<T>(Box<dyn DynDistribution<T>>);

impl<T> DynDist<T> {
    pub fn new<D>(dist: D) -> Self
    where
        D: DynDistribution<T> + 'static,
    {
        DynDist(Box::new(dist))
    }
}

/// Boxed iterator wrapper for `DynDist`.
pub struct BoxedIter<'a, T>(Box<dyn DynSampleIter<Item = T> + 'a>);

impl<T> SampleIter for BoxedIter<'_, T> {
    type Item = T;

    fn next(&mut self, rng: &mut impl Rng) -> T {
        // `impl Rng` is Sized and implements RngCore, so we can coerce to &mut dyn RngCore
        self.0.next_dyn(rng)
    }

    fn reset(&mut self) {
        self.0.reset_dyn();
    }
}

impl<T> Distribution<T> for DynDist<T> {
    type Iter<'a>
        = BoxedIter<'a, T>
    where
        Self: 'a;

    fn sample(&mut self) -> Self::Iter<'_> {
        BoxedIter(self.0.sample_boxed())
    }
}
