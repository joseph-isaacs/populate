use rand::Rng;
use std::marker::PhantomData;

// ============================================================================
// Type-level natural numbers
// ============================================================================

pub struct Z;
pub struct Succ<N>(PhantomData<N>);

pub type N0 = Z;
pub type N1 = Succ<N0>;
pub type N2 = Succ<N1>;
pub type N3 = Succ<N2>;
pub type N4 = Succ<N3>;

pub trait NAdd<R> {
    type Sum;
}

impl<R> NAdd<R> for Z {
    type Sum = R;
}

impl<N: NAdd<R>, R> NAdd<R> for Succ<N> {
    type Sum = Succ<N::Sum>;
}

// ============================================================================
// HList
// ============================================================================

pub struct HNil;
pub struct HCons<H, T>(pub H, pub T);

// ============================================================================
// HSplitAt<N> — split an HList at type-level position N
// ============================================================================

pub trait HSplitAt<N> {
    type Left;
    type Right;
    fn split(self) -> (Self::Left, Self::Right);
    fn unsplit(left: Self::Left, right: Self::Right) -> Self;
}

impl<L> HSplitAt<Z> for L {
    type Left = HNil;
    type Right = L;

    fn split(self) -> (HNil, L) {
        (HNil, self)
    }

    fn unsplit(_: HNil, right: L) -> L {
        right
    }
}

impl<H, T: HSplitAt<N>, N> HSplitAt<Succ<N>> for HCons<H, T> {
    type Left = HCons<H, T::Left>;
    type Right = T::Right;

    fn split(self) -> (Self::Left, Self::Right) {
        let (left, right) = self.1.split();
        (HCons(self.0, left), right)
    }

    fn unsplit(left: HCons<H, T::Left>, right: T::Right) -> Self {
        HCons(left.0, T::unsplit(left.1, right))
    }
}

// ============================================================================
// HasWidth — how many sink columns a distribution contributes
// ============================================================================

pub trait HasWidth {
    type Width;
}

// Width for primitive value types (needed for Map<D, F, In, Out>)
impl HasWidth for i32 {
    type Width = N1;
}
impl HasWidth for i64 {
    type Width = N1;
}
impl HasWidth for f64 {
    type Width = N1;
}
impl HasWidth for bool {
    type Width = N1;
}
impl HasWidth for usize {
    type Width = N1;
}

// Width for tuple value types — sum of component widths
impl<A: HasWidth, B: HasWidth> HasWidth for (A, B)
where
    A::Width: NAdd<B::Width>,
{
    type Width = <A::Width as NAdd<B::Width>>::Sum;
}

impl<A: HasWidth, B: HasWidth, C: HasWidth> HasWidth for (A, B, C)
where
    A::Width: NAdd<B::Width>,
    <A::Width as NAdd<B::Width>>::Sum: NAdd<C::Width>,
{
    type Width = <<A::Width as NAdd<B::Width>>::Sum as NAdd<C::Width>>::Sum;
}

impl<A: HasWidth, B: HasWidth, C: HasWidth, D: HasWidth> HasWidth for (A, B, C, D)
where
    A::Width: NAdd<B::Width>,
    <A::Width as NAdd<B::Width>>::Sum: NAdd<C::Width>,
    <<A::Width as NAdd<B::Width>>::Sum as NAdd<C::Width>>::Sum: NAdd<D::Width>,
{
    type Width =
        <<<A::Width as NAdd<B::Width>>::Sum as NAdd<C::Width>>::Sum as NAdd<D::Width>>::Sum;
}

// Width for Vec<T> — a list column is 1 column
impl<T> HasWidth for Vec<T> {
    type Width = N1;
}

// ============================================================================
// SampleIter and Distribution
// ============================================================================

pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

pub trait Distribution {
    type Item;
    type RowIter: SampleIter<Item = Self::Item>;
    fn row_iter(&mut self) -> Self::RowIter;
}

// ============================================================================
// Sink<T> — accepts values via push
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

// HList Sink impls — bridge between leaf distributions and HList sinks

// Scalar into 1-element HList
impl<T, S: Sink<T>> Sink<T> for HCons<S, HNil> {
    fn push(&mut self, value: T) {
        self.0.push(value);
    }
}

// 2-tuple into 2-element HList
impl<A, B, SA: Sink<A>, SB: Sink<B>> Sink<(A, B)> for HCons<SA, HCons<SB, HNil>> {
    fn push(&mut self, (a, b): (A, B)) {
        self.0.push(a);
        self.1.0.push(b);
    }
}

// 3-tuple into 3-element HList
impl<A, B, C, SA: Sink<A>, SB: Sink<B>, SC: Sink<C>> Sink<(A, B, C)>
    for HCons<SA, HCons<SB, HCons<SC, HNil>>>
{
    fn push(&mut self, (a, b, c): (A, B, C)) {
        self.0.push(a);
        self.1.0.push(b);
        self.1.1.0.push(c);
    }
}

// 4-tuple into 4-element HList
impl<A, B, C, D, SA: Sink<A>, SB: Sink<B>, SC: Sink<C>, SD: Sink<D>> Sink<(A, B, C, D)>
    for HCons<SA, HCons<SB, HCons<SC, HCons<SD, HNil>>>>
{
    fn push(&mut self, (a, b, c, d): (A, B, C, D)) {
        self.0.push(a);
        self.1.0.push(b);
        self.1.1.0.push(c);
        self.1.1.1.0.push(d);
    }
}

// Tuple sinks — for row-by-row collection (Map uses these)
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
// ListSink<T> — sink for list/array columns
// ============================================================================

pub trait ListSink<T> {
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>);
}

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

// HList ListSink — wrap a ListSink in a 1-element HList
impl<T, S: ListSink<T>> ListSink<T> for HCons<S, HNil> {
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>) {
        self.0.push_list(len, elems);
    }
}

// ============================================================================
// CollectInto<S> — distribution-driven collection into a typed sink
// ============================================================================

pub trait CollectInto<S> {
    fn collect_into(&mut self, sink: S, rng: &mut impl Rng, n: usize) -> S;
}

// ============================================================================
// IntoHList / IntoTuple — conversion between flat tuples and HLists
// ============================================================================

pub trait IntoHList {
    type HList;
    fn into_hlist(self) -> Self::HList;
}

pub trait IntoTuple {
    type Tuple;
    fn into_tuple(self) -> Self::Tuple;
}

// 2-tuple
impl<A, B> IntoHList for (A, B) {
    type HList = HCons<A, HCons<B, HNil>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HNil))
    }
}

impl<A, B> IntoTuple for HCons<A, HCons<B, HNil>> {
    type Tuple = (A, B);

    fn into_tuple(self) -> (A, B) {
        (self.0, self.1.0)
    }
}

// 3-tuple
impl<A, B, C> IntoHList for (A, B, C) {
    type HList = HCons<A, HCons<B, HCons<C, HNil>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HCons(self.2, HNil)))
    }
}

impl<A, B, C> IntoTuple for HCons<A, HCons<B, HCons<C, HNil>>> {
    type Tuple = (A, B, C);

    fn into_tuple(self) -> (A, B, C) {
        (self.0, self.1.0, self.1.1.0)
    }
}

// 4-tuple
impl<A, B, C, D> IntoHList for (A, B, C, D) {
    type HList = HCons<A, HCons<B, HCons<C, HCons<D, HNil>>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HCons(self.1, HCons(self.2, HCons(self.3, HNil))))
    }
}

impl<A, B, C, D> IntoTuple for HCons<A, HCons<B, HCons<C, HCons<D, HNil>>>> {
    type Tuple = (A, B, C, D);

    fn into_tuple(self) -> (A, B, C, D) {
        (self.0, self.1.0, self.1.1.0, self.1.1.1.0)
    }
}

// 5-tuple (for unnest cases producing 5 columns)
impl<A, B, C, D, E> IntoHList for (A, B, C, D, E) {
    type HList = HCons<A, HCons<B, HCons<C, HCons<D, HCons<E, HNil>>>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(
            self.0,
            HCons(self.1, HCons(self.2, HCons(self.3, HCons(self.4, HNil)))),
        )
    }
}

impl<A, B, C, D, E> IntoTuple for HCons<A, HCons<B, HCons<C, HCons<D, HCons<E, HNil>>>>> {
    type Tuple = (A, B, C, D, E);

    fn into_tuple(self) -> (A, B, C, D, E) {
        (self.0, self.1.0, self.1.1.0, self.1.1.1.0, self.1.1.1.1.0)
    }
}
