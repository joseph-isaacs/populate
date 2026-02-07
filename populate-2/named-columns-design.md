# Named & Nested Column Sinks — Design

## Motivation

The current sink system is positional: tuples of `Vec<T>` / `VecListSink<T>` with no
column names and no way to group columns into logical units. We want:

1. **Named columns** — sinks carry a `&'static str` name
2. **Nested columns** — a group of columns forms a single logical unit (like Arrow's Struct type)
3. **Backend-controlled** — Vec ignores names, Arrow uses them
4. **Distribution as schema** — the distribution tree defines column names; `default_sink()` constructs correctly-named sinks

Target:
```rust
let mut dist = (
    range("a", 0i32, 1),
    nested("b", (
        range("a", 0u16, 1),
        range("b", 0u32, 1),
    )),
);

// Vec — names ignored, flat storage
let sink = dist.collect_default(&mut rng, 100);

// Arrow — names embedded in builders
let sink = dist.collect_default(&mut rng, 100);
// sink has: field "a" (Int32), field "b" (Struct { "a": UInt16, "b": UInt32 })
```

---

## New Types

### `Column<T>` — value wrapper for grouped/nested columns

```rust
pub struct Column<T>(pub T);
```

`Column<(u16, u32)>` means "a struct column whose row is `(u16, u32)`."
Prevents HList flattening — the outer tuple sees one element, not two.

### `Named<S>` — sink wrapper carrying a name

```rust
pub struct Named<S> {
    pub name: &'static str,
    pub inner: S,
}
```

Delegates all sink operations:

```rust
impl<T, S: Sink<T>> Sink<T> for Named<S> {
    fn push(&mut self, value: T) { self.inner.push(value); }
}

impl<T, S: ListSink<T>> ListSink<T> for Named<S> {
    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>) {
        self.inner.push_list(len, elems);
    }
}

impl<T, S: Sink<T>> Sink<Column<T>> for Named<S> {
    fn push(&mut self, Column(v): Column<T>) { self.inner.push(v); }
}
```

Used by Arrow backend for all column types (prim, list, and group).

### `Transparent<S>` — zero-cost group wrapper (no name)

```rust
pub struct Transparent<S>(pub S);

impl<T, S: Sink<T>> Sink<T> for Transparent<S> {
    fn push(&mut self, value: T) { self.0.push(value); }
}

impl<T, S: Sink<T>> Sink<Column<T>> for Transparent<S> {
    fn push(&mut self, Column(v): Column<T>) { self.0.push(v); }
}
```

Used by Vec backend for groups. Newtype pattern — zero overhead.

### `GroupSink` — trait for wrapping/unwrapping group sinks

```rust
pub trait GroupSink: Sized {
    type Inner;
    fn into_inner(self) -> Self::Inner;
}

impl<S> GroupSink for Transparent<S> {
    type Inner = S;
    fn into_inner(self) -> S { self.0 }
}

impl<S> GroupSink for Named<S> {
    type Inner = S;
    fn into_inner(self) -> S { self.inner }
}
```

Used by `Nested` distribution to unwrap → delegate → re-wrap during `collect_into`.

### `Nested<D>` — distribution wrapper for grouped columns

```rust
pub struct Nested<D> {
    pub name: &'static str,
    pub inner: D,
}

pub fn nested<D>(name: &'static str, inner: D) -> Nested<D> {
    Nested { name, inner }
}
```

---

## Changes to Existing Traits

### Backend — add `Group` GAT + construction methods

```rust
pub trait Backend: Sized {
    type Prim<T>: Sink<T>;
    type List<T>: ListSink<T>;
    type Group<S>: GroupSink<Inner = S> + Sink<Column<???>>;  // see note below

    fn new_prim<T>(name: &'static str) -> Self::Prim<T>;
    fn new_list<T>(name: &'static str) -> Self::List<T>;
    fn new_group<S>(name: &'static str, inner: S) -> Self::Group<S>;
}
```

**Note on `Group<S>` bounds:** We need `Group<S>: Sink<Column<T>>` when `S: Sink<T>`,
but we can't express that conditional in the GAT bound. Instead, the bound is satisfied
by the concrete impls (`Transparent`, `Named`) and required where needed via where clauses
on Distribution impls.

Simplified:

```rust
pub trait Backend: Sized {
    type Prim<T>: Sink<T>;
    type List<T>: ListSink<T>;
    type Group<S>: GroupSink<Inner = S>;

    fn new_prim<T>(name: &'static str) -> Self::Prim<T>;
    fn new_list<T>(name: &'static str) -> Self::List<T>;
    fn new_group<S>(name: &'static str, inner: S) -> Self::Group<S>;
}
```

### VecBackend

```rust
impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type List<T> = VecListSink<T>;
    type Group<S> = Transparent<S>;

    fn new_prim<T>(_name: &'static str) -> Vec<T> { Vec::new() }
    fn new_list<T>(_name: &'static str) -> VecListSink<T> { VecListSink::new() }
    fn new_group<S>(_name: &'static str, inner: S) -> Transparent<S> { Transparent(inner) }
}
```

### ArrowBackend (sketch)

```rust
impl Backend for ArrowBackend {
    type Prim<T> = Named<ArrowPrimBuilder<T>>;
    type List<T> = Named<ArrowListBuilder<T>>;
    type Group<S> = Named<S>;

    fn new_prim<T>(name: &'static str) -> Named<ArrowPrimBuilder<T>> {
        Named { name, inner: ArrowPrimBuilder::new() }
    }
    fn new_list<T>(name: &'static str) -> Named<ArrowListBuilder<T>> {
        Named { name, inner: ArrowListBuilder::new() }
    }
    fn new_group<S>(name: &'static str, inner: S) -> Named<S> {
        Named { name, inner }
    }
}
```

### DefaultSink — add `Column<T>` impl

```rust
impl<B: Backend, T: DefaultSink<B>> DefaultSink<B> for Column<T> {
    type Sink = HCons<B::Group<<T as DefaultSink<B>>::Sink>, HNil>;
}
```

Single-element HList wrapping the group. The group wraps the inner sink HList.

### Distribution — add `default_sink`

```rust
pub trait Distribution<B: Backend> {
    type Item: DefaultSink<B>;
    type RowIter: SampleIter<Item = Self::Item>;

    fn row_iter(&mut self) -> Self::RowIter;

    fn collect_into(
        &mut self,
        sink: <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink<B>>::Sink;

    fn default_sink(&self) -> <Self::Item as DefaultSink<B>>::Sink;  // NEW
}
```

---

## Leaf Distribution Changes

### Range

```rust
pub struct Range<T> {
    pub name: &'static str,   // NEW
    pub start: T,
    pub step: T,
}

pub fn range<T: Add>(name: &'static str, start: T, step: T) -> Range<T> {
    Range { name, start, step }
}

impl<B: Backend, T: DefaultSink<B> + Add<Output = T> + Copy> Distribution<B> for Range<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    // ... row_iter, collect_into unchanged ...

    fn default_sink(&self) -> HCons<B::Prim<T>, HNil> {
        HCons(B::new_prim::<T>(self.name), HNil)
    }
}
```

### Constant

```rust
pub struct Constant<T> {
    pub name: &'static str,   // NEW
    pub value: T,
}

impl<B: Backend, T: Copy + DefaultSink<B>> Distribution<B> for Constant<T>
where
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    fn default_sink(&self) -> HCons<B::Prim<T>, HNil> {
        HCons(B::new_prim::<T>(self.name), HNil)
    }
}
```

### List

```rust
pub struct List<L, E> {
    pub name: &'static str,   // NEW
    pub lengths: L,
    pub elements: E,
}

impl<B: Backend, L, E> Distribution<B> for List<L, E>
where
    L: Distribution<B, Item = usize>,
    E: Distribution<B>,
    E::RowIter: Clone,
    <Vec<E::Item> as DefaultSink<B>>::Sink: ListSink<E::Item>,
{
    fn default_sink(&self) -> HCons<B::List<E::Item>, HNil> {
        HCons(B::new_list::<E::Item>(self.name), HNil)
    }
}
```

---

## Tuple Distribution — `default_sink` composes via HAppend

### 2-tuple

```rust
impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
{
    fn default_sink(&self) -> <(D1::Item, D2::Item) as DefaultSink<B>>::Sink {
        self.0.default_sink().append(self.1.default_sink())
    }
}
```

**Type check:**
- `self.0.default_sink()` : `S1` = `<D1::Item as DefaultSink<B>>::Sink`
- `self.1.default_sink()` : `S2` = `<D2::Item as DefaultSink<B>>::Sink`
- `S1: HAppend<S2>` ← from where clause
- `s1.append(s2)` : `<S1 as HAppend<S2>>::Output`
- `<(D1::Item, D2::Item) as DefaultSink<B>>::Sink` = `<S1 as HAppend<S2>>::Output` ← from DefaultSink tuple impl
- **Types match.** ✓

3-tuple and 4-tuple follow identically: chain `.append()` calls matching the existing pattern.

---

## Nested Distribution

```rust
pub struct NestedIter<I>(pub I);

impl<I: SampleIter> SampleIter for NestedIter<I> {
    type Item = Column<I::Item>;
    fn next(&mut self, rng: &mut impl Rng) -> Column<I::Item> {
        Column(self.0.next(rng))
    }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for Nested<D>
where
    Column<D::Item>: DefaultSink<B>,
    B::Group<<D::Item as DefaultSink<B>>::Sink>: Sink<Column<D::Item>>,
{
    type Item = Column<D::Item>;
    type RowIter = NestedIter<D::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        NestedIter(self.inner.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <Column<D::Item> as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Column<D::Item> as DefaultSink<B>>::Sink {
        // sink : HCons<B::Group<inner_sink>, HNil>
        let HCons(group, HNil) = sink;
        let inner = group.into_inner();
        let inner = self.inner.collect_into(inner, rng, n);
        HCons(B::new_group(self.name, inner), HNil)
    }

    fn default_sink(&self) -> <Column<D::Item> as DefaultSink<B>>::Sink {
        let inner = self.inner.default_sink();
        HCons(B::new_group(self.name, inner), HNil)
    }
}
```

**Type check for `collect_into`:**
- `sink` : `HCons<B::Group<<D::Item as DefaultSink<B>>::Sink>, HNil>` (from `DefaultSink for Column<T>`)
- `group.into_inner()` : `<D::Item as DefaultSink<B>>::Sink` (from `GroupSink`)
- `self.inner.collect_into(inner, ...)` : `<D::Item as DefaultSink<B>>::Sink`
- `B::new_group(name, inner)` : `B::Group<<D::Item as DefaultSink<B>>::Sink>`
- `HCons(that, HNil)` : `HCons<B::Group<...>, HNil>` = `<Column<D::Item> as DefaultSink<B>>::Sink`
- **Types match.** ✓

**Type check for `default_sink`:**
- `self.inner.default_sink()` : `<D::Item as DefaultSink<B>>::Sink`
- `B::new_group(name, inner)` : `B::Group<...>`
- Same return type as above. ✓

---

## Map — `default_sink` limitation

Map transforms values with an arbitrary `F: Fn(In) -> Out`. It cannot know the column
structure of `Out`, so it cannot construct a meaningful `default_sink`.

Options:
1. **Name on Map** — `Map` holds a name, constructs an unnamed fallback sink. Only works when `Out` is scalar.
2. **Require `DefaultSinkNew` bound** — a separate trait for constructing default sinks given no name.
3. **`default_sink` returns `Option`** — or is a separate trait, not on `Distribution`.

Recommended: **Move `default_sink` to a separate trait** so Map doesn't need it:

```rust
pub trait HasDefaultSink<B: Backend>: Distribution<B> {
    fn default_sink(&self) -> <Self::Item as DefaultSink<B>>::Sink;
}
```

Leaf distributions, tuples, and Nested impl `HasDefaultSink`. Map does not (unless
wrapped with name metadata). This keeps Distribution clean.

Alternative: keep `default_sink` on Distribution with a default impl that panics. Map
overrides nothing and panics if called. Less type-safe but simpler.

---

## ValueHList for Column<T>

For Flatten support, Column values should not flatten — they stay as one element:

```rust
impl<T> ValueHList for Column<T> {
    type HList = HCons<Column<T>, HNil>;
    fn into_hlist(self) -> Self::HList { HCons(self, HNil) }
}
```

---

## DistributionExt — `collect_default`

```rust
fn collect_default(&mut self, rng: &mut impl Rng, n: usize) -> <Self::Item as DefaultSink<B>>::Sink
where
    Self: HasDefaultSink<B>,
{
    let sink = self.default_sink();
    self.collect_into(sink, rng, n)
}
```

---

## Worked Example: `(a: i32, b: struct(a: u16, b: u32))`

### Distribution construction

```rust
let mut dist = (
    range("a", 0i32, 1),
    nested("b", (
        range("a", 0u16, 1),
        range("b", 0u32, 1),
    )),
);
```

### Type resolution

```
dist : (Range<i32>, Nested<(Range<u16>, Range<u32>)>)

D1 = Range<i32>          → Item = i32
D2 = Nested<(Range<u16>, Range<u32>)>
   inner = (Range<u16>, Range<u32>) → Item = (u16, u32)
   → Item = Column<(u16, u32)>

Outer tuple → Item = (i32, Column<(u16, u32)>)
```

### Sink type resolution (VecBackend)

```
DefaultSink<VecBackend> for i32
  = HCons<Vec<i32>, HNil>

DefaultSink<VecBackend> for (u16, u32)
  = HCons<Vec<u16>, HCons<Vec<u32>, HNil>>

DefaultSink<VecBackend> for Column<(u16, u32)>
  = HCons<Transparent<HCons<Vec<u16>, HCons<Vec<u32>, HNil>>>, HNil>

DefaultSink<VecBackend> for (i32, Column<(u16, u32)>)
  = S1.append(S2)
  = HCons<Vec<i32>, HNil>.append(HCons<Transparent<...>, HNil>)
  = HCons<Vec<i32>, HCons<Transparent<HCons<Vec<u16>, HCons<Vec<u32>, HNil>>>, HNil>>
```

**2-element HList:** `[Vec<i32>, Transparent<[Vec<u16>, Vec<u32>]>]`

The inner two Vecs are grouped inside Transparent — not flattened into the outer HList.

### Sink type resolution (ArrowBackend)

```
DefaultSink<ArrowBackend> for i32
  = HCons<Named<ArrowBuilder<i32>>, HNil>

DefaultSink<ArrowBackend> for (u16, u32)
  = HCons<Named<ArrowBuilder<u16>>, HCons<Named<ArrowBuilder<u32>>, HNil>>

DefaultSink<ArrowBackend> for Column<(u16, u32)>
  = HCons<Named<HCons<Named<ArrowBuilder<u16>>, HCons<Named<ArrowBuilder<u32>>, HNil>>>, HNil>

DefaultSink<ArrowBackend> for (i32, Column<(u16, u32)>)
  = HCons<Named<ArrowBuilder<i32>>,
      HCons<Named<HCons<Named<ArrowBuilder<u16>>, HCons<Named<ArrowBuilder<u32>>, HNil>>>,
        HNil>>
```

**2-element HList:** `[Named("a", ArrowBuilder<i32>), Named("b", [Named("a", ArrowBuilder<u16>), Named("b", ArrowBuilder<u32>)])]`

Every column and group carries its name. Arrow can reconstruct the full schema by inspecting the sink tree.

### `default_sink` trace (VecBackend)

```
dist.default_sink()
  = self.0.default_sink().append(self.1.default_sink())

self.0.default_sink()  [Range<i32>, name="a"]
  = HCons(VecBackend::new_prim::<i32>("a"), HNil)
  = HCons(Vec::new(), HNil)

self.1.default_sink()  [Nested, name="b"]
  inner = (Range<u16>, Range<u32>)
  inner.default_sink()
    = inner.0.default_sink().append(inner.1.default_sink())
    = HCons(Vec::<u16>::new(), HNil).append(HCons(Vec::<u32>::new(), HNil))
    = HCons(Vec::<u16>::new(), HCons(Vec::<u32>::new(), HNil))
  = HCons(VecBackend::new_group("b", inner_sink), HNil)
  = HCons(Transparent(HCons(Vec::<u16>::new(), HCons(Vec::<u32>::new(), HNil))), HNil)

result = HCons(Vec::<i32>::new(), HCons(Transparent(...), HNil))
```

**Names "a" and "b" are passed to Backend but VecBackend ignores them.** ✓

### `collect_into` trace

```
outer tuple collect_into(sink, rng, 3):
  (s1, s2) = unappend(sink)
  s1 = HCons(Vec::<i32>::new(), HNil)
  s2 = HCons(Transparent(inner), HNil)

  s1 = Range<i32>.collect_into(s1, rng, 3)
  → pushes 0, 1, 2 into Vec<i32>

  s2 = Nested.collect_into(s2, rng, 3)
  → let HCons(group, HNil) = s2
  → inner = group.into_inner()   [Transparent unwraps]
  → inner = (Range<u16>, Range<u32>).collect_into(inner, rng, 3)
    → (s_a, s_b) = unappend(inner)
    → Range<u16>.collect_into(s_a, rng, 3)  [pushes 0, 1, 2]
    → Range<u32>.collect_into(s_b, rng, 3)  [pushes 0, 1, 2]
    → s_a.append(s_b)
  → HCons(Transparent(inner), HNil)

  s1.append(s2)
```

**Column-by-column order preserved through nesting.** ✓

---

## Summary of All Changes

| What | Type | Purpose |
|------|------|---------|
| `Column<T>` | New struct | Value wrapper — prevents flattening |
| `Named<S>` | New struct | Sink wrapper — carries name |
| `Transparent<S>` | New struct | Sink wrapper — no name, zero-cost |
| `GroupSink` | New trait | `into_inner()` for unwrapping groups |
| `Nested<D>` | New struct | Distribution wrapper for struct columns |
| `Backend::Group<S>` | New GAT | Backend selects group wrapper type |
| `Backend::new_prim/list/group` | New methods | Construct sinks with names |
| `DefaultSink for Column<T>` | New impl | Maps Column to grouped HList sink |
| `Distribution::default_sink` | New method | Distribution constructs its own sinks |
| `HasDefaultSink` | New trait (optional) | Separates default_sink from Distribution |
| `Range/Constant/List.name` | Field addition | Leaf distributions carry column names |

---

## Open Questions

1. **`default_sink` on Distribution vs separate trait:** Putting it on Distribution means
   Map must provide it (awkward). A separate `HasDefaultSink` trait is cleaner but adds
   complexity. Alternative: default impl that panics.

2. **Arrow backend `new_prim<T>` for all T:** `Backend::new_prim<T>` must work for all T.
   Vec is fine (`Vec::new()`). Arrow only supports specific types. Options:
   - Runtime panic for unsupported T
   - Separate `ArrowSinkNew` trait with per-type bounds
   - Don't put construction on Backend; use a separate `SinkNew<T>` trait

3. **`IntoTuple` for sinks containing `Transparent`:** Currently `IntoTuple` is implemented
   for HLists of 1-5 elements. An HList containing `Transparent<...>` would convert to a
   tuple with `Transparent` inside: `(Vec<i32>, Transparent<HCons<Vec<u16>, HCons<Vec<u32>, HNil>>>)`.
   Users would need `.0` to unwrap. Acceptable ergonomic cost or add `IntoTuple` for Transparent?

4. **Naming for `collect_flat`:** If using `collect_flat` which converts HList → tuple, the
   names are inside the tuple elements (for Arrow) or absent (for Vec). The naming is
   preserved in the sink types, not lost during conversion.

5. **Deep nesting:** `nested("a", nested("b", range("c", ...)))` — should this work?
   Type: `Column<Column<i32>>`. DefaultSink: `HCons<Group<HCons<Group<HCons<Prim, HNil>>, HNil>>, HNil>`.
   Should work mechanically but is unusual. Worth supporting or disallowing?
