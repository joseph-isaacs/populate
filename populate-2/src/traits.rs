use rand::Rng;

// ============================================================================
// HList
// ============================================================================

pub struct HNil;
pub struct HCons<H, T>(pub H, pub T);

// ============================================================================
// HAppend — concatenate two HLists, and split them back apart
// ============================================================================

pub trait HAppend<R> {
    type Output;
    fn append(self, right: R) -> Self::Output;
    fn unappend(combined: Self::Output) -> (Self, R)
    where
        Self: Sized;
}

impl<R> HAppend<R> for HNil {
    type Output = R;

    fn append(self, right: R) -> R {
        right
    }

    fn unappend(combined: R) -> (HNil, R) {
        (HNil, combined)
    }
}

impl<H, T: HAppend<R>, R> HAppend<R> for HCons<H, T> {
    type Output = HCons<H, T::Output>;

    fn append(self, right: R) -> HCons<H, T::Output> {
        HCons(self.0, self.1.append(right))
    }

    fn unappend(combined: HCons<H, T::Output>) -> (HCons<H, T>, R) {
        let (tail, right) = T::unappend(combined.1);
        (HCons(combined.0, tail), right)
    }
}

// ============================================================================
// DefaultSink — maps a value type to its HList-of-Vecs sink type
// ============================================================================

pub trait DefaultSink {
    type Sink;
}

impl DefaultSink for i16 {
    type Sink = HCons<Vec<i16>, HNil>;
}

impl DefaultSink for i32 {
    type Sink = HCons<Vec<i32>, HNil>;
}
impl DefaultSink for i64 {
    type Sink = HCons<Vec<i64>, HNil>;
}
impl DefaultSink for f64 {
    type Sink = HCons<Vec<f64>, HNil>;
}
impl DefaultSink for bool {
    type Sink = HCons<Vec<bool>, HNil>;
}
impl DefaultSink for usize {
    type Sink = HCons<Vec<usize>, HNil>;
}

impl<T> DefaultSink for Vec<T> {
    type Sink = HCons<VecListSink<T>, HNil>;
}

impl<A: DefaultSink, B: DefaultSink> DefaultSink for (A, B)
where
    A::Sink: HAppend<B::Sink>,
{
    type Sink = <A::Sink as HAppend<B::Sink>>::Output;
}

impl<A: DefaultSink, B: DefaultSink, C: DefaultSink> DefaultSink for (A, B, C)
where
    A::Sink: HAppend<B::Sink>,
    <A::Sink as HAppend<B::Sink>>::Output: HAppend<C::Sink>,
{
    type Sink = <<A::Sink as HAppend<B::Sink>>::Output as HAppend<C::Sink>>::Output;
}

impl<A: DefaultSink, B: DefaultSink, C: DefaultSink, D: DefaultSink> DefaultSink for (A, B, C, D)
where
    A::Sink: HAppend<B::Sink>,
    <A::Sink as HAppend<B::Sink>>::Output: HAppend<C::Sink>,
    <<A::Sink as HAppend<B::Sink>>::Output as HAppend<C::Sink>>::Output: HAppend<D::Sink>,
{
    type Sink = <<<A::Sink as HAppend<B::Sink>>::Output as HAppend<C::Sink>>::Output as HAppend<
        D::Sink,
    >>::Output;
}

// ============================================================================
// SampleIter and Distribution
// ============================================================================

pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// Sink type is always `<Self::Item as DefaultSink>::Sink` — no separate
/// associated type needed. Users write `impl Distribution<Item = (i32, i64)>`
/// and the sink shape is fully determined by the Item.
pub trait Distribution {
    type Item: DefaultSink;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink>::Sink;

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

// Tuple sinks — for row-by-row collection (collect helper in tests uses these)
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
// VecListSink<T> — columnar sink for list/array columns
// ============================================================================

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

    pub fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.values.extend(elems);
        self.offsets.push(self.values.len());
    }
}

impl<T> Default for VecListSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Sink<Vec<T>> for VecListSink<T> {
    fn push(&mut self, value: Vec<T>) {
        let len = value.len();
        self.push_list(len, value.into_iter());
    }
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

// 1-tuple
impl<A> IntoHList for (A,) {
    type HList = HCons<A, HNil>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.0, HNil)
    }
}

impl<A> IntoTuple for HCons<A, HNil> {
    type Tuple = (A,);

    fn into_tuple(self) -> (A,) {
        (self.0,)
    }
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

// 5-tuple
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

// ============================================================================
// ValueHList — recursive value-to-HList (mirrors DefaultSink but for values)
// ============================================================================

pub trait ValueHList {
    type HList;
    fn into_hlist(self) -> Self::HList;
}

macro_rules! impl_value_hlist_scalar {
    ($($t:ty),*) => {
        $(impl ValueHList for $t {
            type HList = HCons<$t, HNil>;
            fn into_hlist(self) -> Self::HList { HCons(self, HNil) }
        })*
    }
}

impl_value_hlist_scalar!(i16, i32, i64, f64, bool, usize);

impl<T> ValueHList for Vec<T> {
    type HList = HCons<Vec<T>, HNil>;
    fn into_hlist(self) -> Self::HList {
        HCons(self, HNil)
    }
}

impl<A: ValueHList, B: ValueHList> ValueHList for (A, B)
where
    A::HList: HAppend<B::HList>,
{
    type HList = <A::HList as HAppend<B::HList>>::Output;
    fn into_hlist(self) -> Self::HList {
        self.0.into_hlist().append(self.1.into_hlist())
    }
}

impl<A: ValueHList, B: ValueHList, C: ValueHList> ValueHList for (A, B, C)
where
    A::HList: HAppend<B::HList>,
    <A::HList as HAppend<B::HList>>::Output: HAppend<C::HList>,
{
    type HList = <<A::HList as HAppend<B::HList>>::Output as HAppend<C::HList>>::Output;
    fn into_hlist(self) -> Self::HList {
        self.0
            .into_hlist()
            .append(self.1.into_hlist())
            .append(self.2.into_hlist())
    }
}

impl<A: ValueHList, B: ValueHList, C: ValueHList, D: ValueHList> ValueHList for (A, B, C, D)
where
    A::HList: HAppend<B::HList>,
    <A::HList as HAppend<B::HList>>::Output: HAppend<C::HList>,
    <<A::HList as HAppend<B::HList>>::Output as HAppend<C::HList>>::Output: HAppend<D::HList>,
{
    type HList = <<<A::HList as HAppend<B::HList>>::Output as HAppend<C::HList>>::Output as HAppend<
        D::HList,
    >>::Output;
    fn into_hlist(self) -> Self::HList {
        self.0
            .into_hlist()
            .append(self.1.into_hlist())
            .append(self.2.into_hlist())
            .append(self.3.into_hlist())
    }
}

// ============================================================================
// Flatten<D> — distribution wrapper that flattens nested Item types
// ============================================================================

pub trait DistributionExt: Distribution {
    fn collect_flat<T>(&mut self, sink: T, rng: &mut impl Rng, n: usize) -> T
    where
        T: IntoHList<HList = <Self::Item as DefaultSink>::Sink>,
        <Self::Item as DefaultSink>::Sink: IntoTuple<Tuple = T>,
    {
        self.collect_into(sink.into_hlist(), rng, n).into_tuple()
    }

    fn flatten(self) -> Flatten<Self>
    where
        Self: Sized,
        Self::Item: ValueHList,
        <Self::Item as ValueHList>::HList: IntoTuple,
        <<Self::Item as ValueHList>::HList as IntoTuple>::Tuple:
            DefaultSink<Sink = <Self::Item as DefaultSink>::Sink>,
    {
        Flatten(self)
    }
}

impl<D: Distribution> DistributionExt for D {}

pub struct Flatten<D>(pub D);

pub struct FlattenIter<I>(pub I);

impl<I: SampleIter> SampleIter for FlattenIter<I>
where
    I::Item: ValueHList,
    <I::Item as ValueHList>::HList: IntoTuple,
{
    type Item = <<I::Item as ValueHList>::HList as IntoTuple>::Tuple;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0.next(rng).into_hlist().into_tuple()
    }
}

impl<D: Distribution> Distribution for Flatten<D>
where
    D::Item: ValueHList,
    <D::Item as ValueHList>::HList: IntoTuple,
    <<D::Item as ValueHList>::HList as IntoTuple>::Tuple:
        DefaultSink<Sink = <D::Item as DefaultSink>::Sink>,
{
    type Item = <<D::Item as ValueHList>::HList as IntoTuple>::Tuple;
    type RowIter = FlattenIter<D::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        FlattenIter(self.0.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink>::Sink {
        self.0.collect_into(sink, rng, n)
    }
}
