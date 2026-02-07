# Distribution Trait Redesign

Supersedes `named-columns-design.md`.

## Motivation

The current `Distribution` trait derives sink types indirectly through `DefaultSink<B>`.
This means every generic function needs verbose where clauses like
`<T as DefaultSink<B>>::Sink: Sink<T>`, and auto-flattening requires an explicit
`Flatten<D>` wrapper.

The redesigned `Distribution` trait has:
1. Explicit `type Sink` associated type (not derived from `DefaultSink`)
2. `default_sink(&self)` — distribution constructs its own sink
3. Auto-flattening built into tuple impls — no `Flatten` wrapper needed

---

## New Trait

```rust
pub trait Distribution<B: Backend> {
    type Value;
    type Sink;
    type Iter: SampleIter<Item = Self::Value>;

    fn row_iter(&mut self) -> Self::Iter;
    fn default_sink(&self) -> Self::Sink;
    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink;
}
```

Differences from current:

| Current | New |
|---|---|
| `type Item: DefaultSink<B>` | `type Value` (no trait bound) |
| Sink = `<Item as DefaultSink<B>>::Sink` | `type Sink` — explicit |
| No `default_sink` | `default_sink(&self) -> Self::Sink` |
| Flatten wrapper for auto-flatten | Flattening built into tuple impls |
| `type RowIter` | `type Iter` |

---

## Backend

```rust
pub trait Backend: Sized {
    type Prim<T>: Sink<T> + SinkLen + Default;
    type Column<S>: GroupSink<Inner = S>;
}
```

`Default` on Prim enables `default_sink()` to construct sinks.
`SinkLen` on Prim enables `ColumnarList` to query offset count (via `B::Prim<usize>`).
`Column<S>` wraps an inner sink HList for named/nested columns.
Distributions that don't use named columns never reference `B::Column`.

`Backend::List<T>` is removed — the list sink type is determined by
`ColumnarList<E::Sink, B::Prim<usize>>` in the List Distribution impl (see list-sink-redesign.md).

### GroupSink — wrap/unwrap for column groups

```rust
pub trait GroupSink: Sized {
    type Inner;
    fn new(name: &'static str, inner: Self::Inner) -> Self;
    fn into_inner(self) -> Self::Inner;
}
```

### Transparent — Vec ignores names

```rust
pub struct Transparent<S>(pub S);

impl<S> GroupSink for Transparent<S> {
    type Inner = S;
    fn new(_name: &'static str, inner: S) -> Self { Transparent(inner) }
    fn into_inner(self) -> S { self.0 }
}

impl<S: HAppend<R>, R> HAppend<R> for Transparent<S> {
    type Output = <S as HAppend<R>>::Output;
    fn append(self, right: R) -> Self::Output { self.0.append(right) }
    fn unappend(combined: Self::Output) -> (Self, R) {
        let (s, r) = S::unappend(combined);
        (Transparent(s), r)
    }
}
```

Transparent delegates HAppend to the inner HList, so column sinks flatten just like tuples.

### Backend implementations

```rust
impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type Column<S> = Transparent<S>;    // names ignored, flattens
}

impl Backend for DequeBackend {
    type Prim<T> = VecDeque<T>;
    type Column<S> = Transparent<S>;
}
```

Arrow (future):

```rust
impl Backend for ArrowBackend {
    type Prim<T> = ArrowBuilder<T>;
    type Column<S> = HCons<ArrowNamed<S>, HNil>;  // 1-element HList, preserves nesting
}
```

Arrow list sinks use `ColumnarList<E::Sink, ArrowBuilder<usize>>` — same pattern, different
offset storage.

---

## SinkFor — replaces DefaultSink

`DefaultSink` is replaced by `SinkFor`. Same type-level mapping (value type → sink type),
but with `Sink<Self> + Default` bounds, and **not** a bound on `Distribution::Value`.

```rust
pub trait SinkFor<B: Backend>: Sized {
    type Sink: Sink<Self> + Default;
}
```

`SinkFor` is only required by `Map` (to resolve sink type from arbitrary output type).
Leaf distributions and tuple distributions derive their Sink types directly.

### Scalar impls (macro)

```rust
macro_rules! impl_sink_for_scalar {
    ($($t:ty),*) => {
        $(impl<B: Backend> SinkFor<B> for $t {
            type Sink = HCons<B::Prim<$t>, HNil>;
        })*
    };
}
impl_sink_for_scalar!(i16, i32, i64, f64, bool, usize);
```

Scalar sinks remain HCons-wrapped — required for `HAppend` composition in tuple impls.

### Vec

```rust
impl<B: Backend, T: SinkFor<B>> SinkFor<B> for Vec<T>
where
    ColumnarList<<T as SinkFor<B>>::Sink, B::Prim<usize>>: Sink<Vec<T>>,
{
    type Sink = HCons<ColumnarList<<T as SinkFor<B>>::Sink, B::Prim<usize>>, HNil>;
}
```

The inner type of `ColumnarList` is `T`'s own SinkFor sink — so `Vec<(i32, u32)>` gets columnar
inner storage `HCons<Vec<i32>, HCons<Vec<u32>, HNil>>`. See list-sink-redesign.md for details.

### Tuples (for Map output)

```rust
impl<B: Backend, A: SinkFor<B>, C: SinkFor<B>> SinkFor<B> for (A, C)
where
    <A as SinkFor<B>>::Sink: HAppend<<C as SinkFor<B>>::Sink>,
    <<A as SinkFor<B>>::Sink as HAppend<<C as SinkFor<B>>::Sink>>::Output: Sink<(A, C)> + Default,
{
    type Sink = <<A as SinkFor<B>>::Sink as HAppend<<C as SinkFor<B>>::Sink>>::Output;
}
// 3-tuple, 4-tuple follow same pattern
```

---

## Distribution Implementations

### Constant

```rust
impl<B: Backend, T: Copy + SinkFor<B>> Distribution<B> for Constant<T> {
    type Value = T;
    type Sink = <T as SinkFor<B>>::Sink;
    type Iter = ConstantIter<T>;

    fn row_iter(&mut self) -> Self::Iter {
        ConstantIter { value: self.value }
    }

    fn default_sink(&self) -> Self::Sink { Default::default() }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let mut iter = self.row_iter();
        for _ in 0..n { sink.push(iter.next(rng)); }
        sink
    }
}
```

For VecBackend with T=i32: `Sink = HCons<Vec<i32>, HNil>`.

### Range

Same pattern, with `T: Add<Output = T> + Copy + SinkFor<B>`.

### Tuple (D1, D2) — auto-flattening

```rust
impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    D1::Value: ValueHList,
    D2::Value: ValueHList,
    D1::Sink: HAppend<D2::Sink>,
    <D1::Value as ValueHList>::HList: HAppend<<D2::Value as ValueHList>::HList>,
    <<D1::Value as ValueHList>::HList as HAppend<<D2::Value as ValueHList>::HList>>::Output:
        IntoTuple,
{
    type Value = <<<D1::Value as ValueHList>::HList as HAppend<
        <D2::Value as ValueHList>::HList,
    >>::Output as IntoTuple>::Tuple;

    type Sink = <D1::Sink as HAppend<D2::Sink>>::Output;

    type Iter = TupleIter2<D1::Iter, D2::Iter>;

    fn default_sink(&self) -> Self::Sink {
        self.0.default_sink().append(self.1.default_sink())
    }

    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let (s1, s2) = <D1::Sink as HAppend<D2::Sink>>::unappend(sink);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        s1.append(s2)
    }

    fn row_iter(&mut self) -> Self::Iter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }
}
```

**Auto-flatten in row_iter:** `TupleIter2` flattens values via ValueHList:

```rust
impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2>
where
    I1::Item: ValueHList,
    I2::Item: ValueHList,
    <I1::Item as ValueHList>::HList: HAppend<<I2::Item as ValueHList>::HList>,
    <<I1::Item as ValueHList>::HList as HAppend<<I2::Item as ValueHList>::HList>>::Output:
        IntoTuple,
{
    type Item = <<...>>::Tuple;  // flat tuple

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        self.0.next(rng).into_hlist()
            .append(self.1.next(rng).into_hlist())
            .into_tuple()
    }
}
```

For the simple case (both scalar): `i32.into_hlist()` = `HCons(42, HNil)`,
append gives `HCons(42, HCons(7, HNil))`, `into_tuple()` = `(42, 7)`. Same as today.

For the mixed case (D2 produces `(B, C)`): `(b, c).into_hlist()` = `HCons(b, HCons(c, HNil))`,
append gives `HCons(a, HCons(b, HCons(c, HNil)))`, `into_tuple()` = `(a, b, c)`. Auto-flattened!

3-tuple and 4-tuple follow the same pattern via macro.

### Map

```rust
impl<B: Backend, D, F, In, Out: SinkFor<B>> Distribution<B> for Map<D, F, In, Out>
where
    D: Distribution<B, Value = In>,
    F: Fn(In) -> Out + Clone,
{
    type Value = Out;
    type Sink = <Out as SinkFor<B>>::Sink;
    type Iter = MapIter<D::Iter, F>;

    fn default_sink(&self) -> Self::Sink { Default::default() }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let mut iter = self.row_iter();
        for _ in 0..n { sink.push(iter.next(rng)); }
        sink
    }

    fn row_iter(&mut self) -> Self::Iter {
        MapIter { source: self.source.row_iter(), f: self.f.clone() }
    }
}
```

Map always collects row-by-row. `SinkFor` resolves the correct sink for any output type.

When Map is inside a tuple `(D1, Map<.., (B,C)>)`, the outer tuple's auto-flatten
handles composing Map's 2-element HList sink with D1's 1-element HList sink via HAppend.

### List

```rust
impl<B: Backend, L: Distribution<B, Value = usize>, E: Distribution<B>> Distribution<B>
    for List<L, E>
where
    E::Iter: Clone,
    ColumnarList<E::Sink, B::Prim<usize>>: ListSink<E::Value>,
{
    type Value = Vec<E::Value>;
    type Sink = HCons<ColumnarList<E::Sink, B::Prim<usize>>, HNil>;
    type Iter = ListIter<L::Iter, E::Iter>;

    fn default_sink(&self) -> Self::Sink { Default::default() }

    fn collect_into(&mut self, mut sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let mut len_iter = self.lengths.row_iter();
        let elem_base = self.elements.row_iter();
        for _ in 0..n {
            let len = len_iter.next(rng);
            let mut elems = elem_base.clone();
            sink.push_list(len, (0..len).map(|_| elems.next(rng)));
        }
        sink
    }
}
```

List sink is HCons-wrapped (like scalars) so it composes via HAppend in tuple impls.
The inner type `E::Sink` gives columnar storage for struct elements — see list-sink-redesign.md.
`push_list` goes through the HCons ListSink delegator to ColumnarList.

---

## Extension Traits

```rust
pub trait DistributionExt<B: Backend>: Distribution<B> {
    /// Collect using default_sink, return native Sink type
    fn collect(&mut self, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let sink = self.default_sink();
        self.collect_into(sink, rng, n)
    }

    /// Collect using default_sink, return flat tuple
    fn collect_flat(&mut self, rng: &mut impl Rng, n: usize) -> <Self::Sink as IntoTuple>::Tuple
    where
        Self::Sink: IntoTuple,
    {
        self.collect(rng, n).into_tuple()
    }

    /// Collect with explicit sink tuple, return flat tuple
    fn collect_flat_into<T>(&mut self, sink: T, rng: &mut impl Rng, n: usize) -> T
    where
        T: IntoHList<HList = Self::Sink>,
        Self::Sink: IntoTuple<Tuple = T>,
    {
        self.collect_into(sink.into_hlist(), rng, n).into_tuple()
    }
}

impl<B: Backend, D: Distribution<B>> DistributionExt<B> for D {}
```

### DistributionScalar — unwrap HCons for single-column dists

```rust
pub trait DistributionScalar<B: Backend>: Distribution<B> {
    fn collect_col(&mut self, rng: &mut impl Rng, n: usize) -> S
    where
        Self::Sink = HCons<S, HNil>,
    {
        self.collect(rng, n).0
    }
}
```

(Exact syntax TBD — may need a helper trait to extract the inner type from HCons<S, HNil>.)

---

## Named Columns

Named columns use `B::Column<S>` to wrap sub-sinks with backend-specific metadata.

### Column<T> — value wrapper preventing auto-flatten

```rust
pub struct Column<T>(pub T);

impl<T> ValueHList for Column<T> {
    type HList = HCons<Column<T>, HNil>;
    fn into_hlist(self) -> Self::HList { HCons(self, HNil) }
}
```

When a Columns distribution is inside a tuple, `Column<T>` acts as one opaque element —
the tuple auto-flatten does not flatten through struct boundaries.

### Columns distribution

```rust
struct Columns2<D1, D2> {
    name1: &'static str,
    dist1: D1,
    name2: &'static str,
    dist2: D2,
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B>
    for Columns2<D1, D2>
where
    B::Column<D1::Sink>: HAppend<B::Column<D2::Sink>>,
{
    type Value = Column<(D1::Value, D2::Value)>;
    type Sink = <B::Column<D1::Sink> as HAppend<B::Column<D2::Sink>>>::Output;
    type Iter = ColumnsIter2<D1::Iter, D2::Iter>;

    fn default_sink(&self) -> Self::Sink {
        let s1 = B::Column::new(self.name1, self.dist1.default_sink());
        let s2 = B::Column::new(self.name2, self.dist2.default_sink());
        s1.append(s2)
    }

    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let (cs1, cs2) = <B::Column<D1::Sink> as HAppend<B::Column<D2::Sink>>>::unappend(sink);
        let s1 = self.dist1.collect_into(cs1.into_inner(), rng, n);
        let s2 = self.dist2.collect_into(cs2.into_inner(), rng, n);
        let cs1 = B::Column::new(self.name1, s1);
        let cs2 = B::Column::new(self.name2, s2);
        cs1.append(cs2)
    }

    fn row_iter(&mut self) -> Self::Iter {
        ColumnsIter2(self.dist1.row_iter(), self.dist2.row_iter())
    }
}
```

### Type check: VecBackend (flat, names ignored)

```
D1 = Range<i32>    → Sink = HCons<Vec<i32>, HNil>
D2 = (Range<u16>, Range<u32>) → Sink = HCons<Vec<u16>, HCons<Vec<u32>, HNil>>

B::Column<D1::Sink> = Transparent<HCons<Vec<i32>, HNil>>
B::Column<D2::Sink> = Transparent<HCons<Vec<u16>, HCons<Vec<u32>, HNil>>>

Transparent delegates HAppend to inner:
  HCons<Vec<i32>, HNil>.append(HCons<Vec<u16>, HCons<Vec<u32>, HNil>>)
  = HCons<Vec<i32>, HCons<Vec<u16>, HCons<Vec<u32>, HNil>>>

Flat 3-element HList. Names ignored. ✓
```

### Type check: ArrowBackend (nested, names preserved)

```
B::Column<D1::Sink> = HCons<ArrowNamed<HCons<ArrowBuilder<i32>, HNil>>, HNil>
B::Column<D2::Sink> = HCons<ArrowNamed<HCons<ArrowBuilder<u16>, HCons<ArrowBuilder<u32>, HNil>>>, HNil>

HAppend:
  HCons<ArrowNamed("a", ...), HCons<ArrowNamed("b", ...), HNil>>

2-element HList of named groups. Arrow can reconstruct schema. ✓
```

### Builder API (future)

```rust
columns()
    .column("a", range(0i32, 1))
    .column("b", columns()
        .column("x", range(0u16, 1))
        .column("y", range(0u32, 1))
    )
```

Accumulates an HList of (name, dist) pairs, producing Columns2/3/4 or a generic
HList-based Columns type.

---

## What Gets Eliminated

| Eliminated | Reason |
|---|---|
| `DefaultSink<B>` trait + all impls | Replaced by `SinkFor` (not a bound on Value) |
| `DefaultSink` scalar/tuple macros | Replaced by `SinkFor` macros |
| `Flatten<D>` / `FlattenIter<I>` | Flattening built into tuple Distribution impls |
| `flatten()` combinator in dist.rs | Not needed |
| `type Item: DefaultSink<B>` bound | `type Value` has no bound |
| `VecListSink<T>` / `DequeListSink<T>` | Replaced by `ColumnarList<Inner, Offsets>` |
| `Backend::List<T>` | List sink = `ColumnarList<E::Sink, B::Prim<usize>>` in List impl |

## What Stays

| Kept | Why |
|---|---|
| `HCons` / `HNil` | Tuple sinks are HLists |
| `HAppend` | Sink composition in tuple collect_into + default_sink |
| `ValueHList` | Value flattening in tuple row_iter |
| `IntoHList` / `IntoTuple` | Tuple ↔ HList conversion for ergonomic API |
| `Sink<T>` | Core push trait, unchanged |
| `ListSink<T>` | Expanded: `inner()`, `end_list()`, `reserve_lists()` (see list-sink-redesign.md) |
| `Sink<(A,B)> for HCons<..>` | Row-by-row push into HList sinks (used by Map) |
| `Sink<(A,B)> for (S1, S2)` | Row-by-row push into tuple sinks (used in tests) |
| `Backend` | Parameterizes sink types (minus `List<T>`, plus `SinkLen` on Prim) |
| `SampleIter` | Row iteration, unchanged |
| `DistributionScalar` | Unwraps HCons for scalar collect |

## What's New

| New | Purpose |
|---|---|
| `SinkFor<B>` | Like DefaultSink but with `Sink<Self> + Default` bounds |
| `Distribution::default_sink(&self)` | Distribution constructs its own sink |
| `DistributionExt::collect` | Convenience: default_sink + collect_into |
| `DistributionExt::collect_flat` | Convenience: collect + IntoTuple |
| `Backend::Column<S>` | Wraps inner sinks for named groups |
| `GroupSink` | Wrap/unwrap trait for Column sinks |
| `Transparent<S>` | Vec's Column impl — delegates, ignores names |
| `Column<T>` | Value wrapper preventing auto-flatten through struct boundaries |
| `ColumnarList<Inner, Offsets>` | Flat list storage, replaces VecListSink/DequeListSink |
| `SinkLen` | Query inner position for end_list (see list-sink-redesign.md) |
| `Default` for `HCons`/`HNil` | Construct empty HList sinks for default_sink |

---

## Worked Example: Auto-Flatten

`(range(0i16, 1), map((range(0i64, 1), range(0i64, 1)), |(a,b)| (a+b, a*b)), range(0i32, 5))`

### Value type

```
D1 = Range<i16>         → Value = i16
D2 = Map<.., (i64,i64)> → Value = (i64, i64)
D3 = Range<i32>         → Value = i32

ValueHList:
  i16         → HCons<i16, HNil>
  (i64, i64)  → HCons<i64, HCons<i64, HNil>>
  i32         → HCons<i32, HNil>

Appended: HCons<i16, HCons<i64, HCons<i64, HCons<i32, HNil>>>>
IntoTuple: (i16, i64, i64, i32)
```

### Sink type

```
D1::Sink = HCons<Vec<i16>, HNil>
D2::Sink = HCons<Vec<i64>, HCons<Vec<i64>, HNil>>   (from SinkFor for (i64,i64))
D3::Sink = HCons<Vec<i32>, HNil>

Appended: HCons<Vec<i16>, HCons<Vec<i64>, HCons<Vec<i64>, HCons<Vec<i32>, HNil>>>>
```

### collect_into trace

```
outer 3-tuple collect_into(sink, rng, 3):
  (s12, s3) = S12::unappend(sink)       // split off D3's sink
  (s1, s2)  = S1::unappend(s12)         // split off D2's sink

  s1 = Range<i16>.collect_into(s1, rng, 3)   // col-by-col
  s2 = Map.collect_into(s2, rng, 3)          // row-by-row
  s3 = Range<i32>.collect_into(s3, rng, 3)   // col-by-col

  s1.append(s2).append(s3)
```

### default_sink trace

```
outer 3-tuple default_sink():
  self.0.default_sink()                       // HCons(Vec::<i16>::new(), HNil)
    .append(self.1.default_sink())            // HCons(Vec::<i64>::new(), HCons(Vec::<i64>::new(), HNil))
    .append(self.2.default_sink())            // HCons(Vec::<i32>::new(), HNil)
  = HCons(Vec::new(), HCons(Vec::new(), HCons(Vec::new(), HCons(Vec::new(), HNil))))
```

### row_iter trace

```
TupleIter3.next(rng):
  v1 = 0i16
  v2 = (100i64, 200i64)
  v3 = 0i32

  v1.into_hlist() = HCons(0i16, HNil)
  v2.into_hlist() = HCons(100, HCons(200, HNil))
  v3.into_hlist() = HCons(0i32, HNil)

  appended = HCons(0i16, HCons(100, HCons(200, HCons(0i32, HNil))))
  .into_tuple() = (0i16, 100i64, 200i64, 0i32)
```

No `flatten()` wrapper needed — `Value = (i16, i64, i64, i32)` directly from the tuple impl.

---

## Worked Example: List in Tuple

`(constant(42i32), list(constant(2usize), (range(0i32, 1), range(0u32, 1))))` on VecBackend:

### Value type

```
D1 = Constant<i32>               → Value = i32
D2 = List<.., (Range<i32>, Range<u32>)>
  E = (Range<i32>, Range<u32>)   → E::Value = (i32, u32)
                                  → Value = Vec<(i32, u32)>

ValueHList:
  i32          → HCons<i32, HNil>
  Vec<(i32,u32)> → HCons<Vec<(i32,u32)>, HNil>

Appended: HCons<i32, HCons<Vec<(i32,u32)>, HNil>>
IntoTuple: (i32, Vec<(i32, u32)>)
```

### Sink type

```
D1::Sink = HCons<Vec<i32>, HNil>
D2::Sink:
  E::Sink = HCons<Vec<i32>, HCons<Vec<u32>, HNil>>   (from tuple impl)
  List::Sink = HCons<ColumnarList<HCons<Vec<i32>, HCons<Vec<u32>, HNil>>, Vec<usize>>, HNil>

Tuple Sink = D1::Sink.append(D2::Sink)
  = HCons<Vec<i32>, HCons<ColumnarList<HCons<Vec<i32>, HCons<Vec<u32>, HNil>>, Vec<usize>>, HNil>>
```

Physical layout: `Vec<i32>` (scalar col) + offsets `Vec<usize>` + `Vec<i32>` (list inner col 0) + `Vec<u32>` (list inner col 1). Four contiguous buffers.

### collect_into trace

```
outer 2-tuple collect_into(sink, rng, 3):
  (s1, s2) = S1::unappend(sink)
  s1: HCons<Vec<i32>, HNil>
  s2: HCons<ColumnarList<.., ..>, HNil>

  s1 = Constant<i32>.collect_into(s1, rng, 3)   // pushes 42, 42, 42
  s2 = List.collect_into(s2, rng, 3)            // push_list for each row
    // push_list(2, iter) → inner.push_n([(0,0), (1,1)]) → destructures to columns
    //                     → end_list() records offset

  s1.append(s2)
```

---

## Test Changes

### Helper functions — where clauses simplify

```rust
// BEFORE
pub(crate) fn make_constant<B: Backend>() -> impl Distribution<B, Item = i32>
where
    <i32 as DefaultSink<B>>::Sink: Sink<i32>,
{ constant(42) }

pub(crate) fn make_tuple<B: Backend>() -> impl Distribution<B, Item = (i32, i64)>
where
    <i32 as DefaultSink<B>>::Sink: Sink<i32>,
    <i64 as DefaultSink<B>>::Sink: Sink<i64>,
    <i32 as DefaultSink<B>>::Sink: HAppend<<i64 as DefaultSink<B>>::Sink>,
{ (constant(1), range(0, 10)) }

// AFTER — no DefaultSink bounds
pub(crate) fn make_constant<B: Backend>() -> impl Distribution<B, Value = i32> {
    constant(42)
}

pub(crate) fn make_tuple<B: Backend>() -> impl Distribution<B, Value = (i32, i64)> {
    (constant(1), range(0, 10))
}
```

### Scalar collection — uses collect then unwrap

```rust
// BEFORE
let col = dist.collect_col(Vec::<i32>::new(), &mut rng(), 4);

// AFTER
let col = dist.collect_col(&mut rng(), 4);
```

### Tuple collection — uses default_sink

```rust
// BEFORE
let (c1, c2) = dist.collect_flat((Vec::<i32>::new(), Vec::<i64>::new()), &mut rng(), 4);

// AFTER
let (c1, c2) = dist.collect_flat(&mut rng(), 4);
```

### Flatten — eliminated, auto-flatten is built in

```rust
// BEFORE — needs explicit flatten() wrapper
pub(crate) fn gen_col4<B: Backend>(log: &Log)
    -> impl Distribution<B, Item = (i16, i64, i64, i32)>
{
    flatten(( tracked("COL1", ...), map((...), |(a,b)| (a+b, a*b)), tracked("COL4", ...) ))
}

// AFTER — tuple impl auto-flattens
pub(crate) fn gen_col4<B: Backend>(log: &Log)
    -> impl Distribution<B, Value = (i16, i64, i64, i32)>
{
    ( tracked("COL1", ...), map((...), |(a,b)| (a+b, a*b)), tracked("COL4", ...) )
}
```

### Tracked wrapper — simpler bounds

```rust
// BEFORE
impl<B: Backend, D: Distribution<B>> Distribution<B> for Tracked<D>
where
    <D::Item as DefaultSink<B>>::Sink: Sink<D::Item>,
{ ... }

// AFTER
impl<B: Backend, D: Distribution<B>> Distribution<B> for Tracked<D>
where
    D::Sink: Sink<D::Value>,
{
    type Value = D::Value;
    type Sink = D::Sink;
    type Iter = TrackedIter<D::Iter>;

    fn default_sink(&self) -> Self::Sink { self.inner.default_sink() }
    // row_iter, collect_into — same body as before
}
```
