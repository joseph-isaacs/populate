# ListSink Redesign

## Goals

1. **Reserve capacity** — pre-allocate all sink buffers so collection doesn't reallocate
2. **Access inner storage** — expose the inner buffer so callers can push directly / reserve it
3. **Signal list boundaries** — `end_list()` records where each list ends
4. **Depth-N composition** — the same two operations (`inner()` + `end_list()`) compose to any depth
5. **Work for both flat and non-flat** — reserve should do the right thing regardless of representation

## Core Insight

At each list level, the reserve information is: **how many lists, and the size of each**.

That's `&[usize]` — one length per list. From this:
- **Flat `ColumnarList`**: `offsets.reserve(n)`, `inner.reserve(sum)` — one shared buffer, sum is all we need
- **Non-flat `Vec<Vec<T>>`**: `self.reserve(n)` for Vec object slots, **store the sizes** so each inner Vec can be pre-allocated with the right capacity when created

The non-flat case needs **extra state**: a queue of upcoming sizes consumed during push.

## Changes to Sink\<T\>

```rust
pub trait Sink<T> {
    fn push(&mut self, value: T);

    fn push_n(&mut self, value: impl Iterator<Item = T>) {
        value.for_each(|v| self.push(v));
    }

    fn reserve(&mut self, _n: usize) {}
}
```

Overrides for Vec, VecDeque — call their native `reserve`.

HList sinks delegate reserve to each column (every column gets the same n since they all receive the same number of rows).

## Changes to ListSink\<T\>

```rust
pub trait ListSink<T>: Sink<Vec<T>> {
    type Inner: Sink<T>;

    fn inner(&mut self) -> &mut Self::Inner;
    fn end_list(&mut self);

    fn reserve_lists(&mut self, lengths: &[usize]) {
        // Default: reserve offsets only
        self.reserve(lengths.len());
    }

    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.inner().push_n(elems);
        self.end_list();
    }
}
```

`reserve_lists` takes per-list sizes. Each implementation decides how to use them.

## ColumnarList\<Inner\> (flat representation)

Replaces `VecListSink<T>` and `DequeListSink<T>`.

```rust
pub struct ColumnarList<Inner> {
    pub offsets: Vec<usize>,
    pub inner: Inner,
}
```

### SinkLen (for end_list)

`end_list` needs to know the inner's current position. One small trait:

```rust
pub trait SinkLen {
    fn sink_len(&self) -> usize;
}

impl<T> SinkLen for Vec<T>          { fn sink_len(&self) -> usize { self.len() } }
impl<T> SinkLen for VecDeque<T>     { fn sink_len(&self) -> usize { self.len() } }
impl<I> SinkLen for ColumnarList<I> { fn sink_len(&self) -> usize { self.offsets.len() - 1 } }
```

### ListSink impl

```rust
impl<T, Inner: Sink<T> + SinkLen> ListSink<T> for ColumnarList<Inner> {
    type Inner = Inner;

    fn inner(&mut self) -> &mut Inner { &mut self.inner }

    fn end_list(&mut self) {
        self.offsets.push(self.inner.sink_len());
    }

    fn reserve_lists(&mut self, lengths: &[usize]) {
        self.offsets.reserve(lengths.len());
        self.inner.reserve(lengths.iter().sum());
    }
}
```

`reserve_lists` reserves offsets for `n` lists, and the inner buffer for the **sum** of all lengths. For depth 1, inner is `Vec<T>` so this reserves the flat values buffer. For depth 2, inner is another `ColumnarList` — `reserve(sum)` reserves its offset buffer (via `Sink<Vec<T>>::reserve`).

### Sink\<Vec\<T\>\> bridge

```rust
impl<T, Inner: Sink<T> + SinkLen> Sink<Vec<T>> for ColumnarList<Inner> {
    fn push(&mut self, value: Vec<T>) {
        self.push_list(value.len(), value.into_iter());
    }

    fn reserve(&mut self, n: usize) {
        self.offsets.reserve(n);
    }
}
```

## Non-flat representation (Vec\<Vec\<T\>\>)

For the default Backend, `List<T> = ColumnarList<Vec<T>>` and depth-2 gives `ColumnarList<Vec<Vec<T>>>`. The inner `Vec<Vec<T>>` stores heap-allocated Vecs.

### The problem

`reserve_lists(&[usize])` on `ColumnarList<Vec<Vec<T>>>` calls `self.inner.reserve(sum)` which reserves `sum` Vec object slots in `Vec<Vec<T>>`. But each inner `Vec<T>` still allocates separately when created.

### Solution: store upcoming sizes

We can make `Vec<Vec<T>>`'s Sink impl size-aware by wrapping it:

```rust
pub struct SizedVecSink<T> {
    data: Vec<Vec<T>>,
    upcoming_sizes: VecDeque<usize>,
}

impl<T> Sink<Vec<T>> for SizedVecSink<T> {
    fn push(&mut self, value: Vec<T>) {
        self.data.push(value);
    }

    fn reserve(&mut self, n: usize) {
        self.data.reserve(n);
    }
}
```

But this only helps if the caller creates Vecs using the stored sizes. The actual benefit comes from `push_list`:

```rust
impl<T> ListSink<T> for ColumnarList<SizedVecSink<T>> {
    fn reserve_lists(&mut self, lengths: &[usize]) {
        self.offsets.reserve(lengths.len());
        self.inner.data.reserve(lengths.len());
        self.inner.upcoming_sizes.extend(lengths);
    }

    fn push_list(&mut self, len: usize, elems: impl Iterator<Item = T>) {
        // Use stored size if available, otherwise use len
        let cap = self.inner.upcoming_sizes.pop_front().unwrap_or(len);
        let mut v = Vec::with_capacity(cap);
        v.extend(elems);
        self.inner.data.push(v);
        self.end_list();
    }
}
```

Each `push_list` pops the next pre-stored size and creates a `Vec::with_capacity`. No reallocation inside the inner Vec during extend (assuming size was correct).

**Trade-off**: This avoids per-list *reallocation* but not per-list *allocation*. Each inner Vec is still a separate heap allocation. To avoid that entirely, use the flat `ColumnarList<ColumnarList<Vec<T>>>`.

### Is SizedVecSink worth it?

Probably not for most cases. `.collect()` with a `(0..len).map(...)` iterator already uses size hints to allocate correctly. The main win of `SizedVecSink` is when you call `push_list` with an iterator whose `size_hint` is imprecise.

**Recommendation**: Start with flat `ColumnarList` only. Add `SizedVecSink` later if profiling shows non-flat inner-list allocation is a bottleneck.

## Pre-generating lengths (the "other state")

To call `reserve_lists`, we need the lengths before pushing. This means pre-generating them from the length distribution.

### Depth 1

One `Vec<usize>`:

```rust
fn collect_into(&mut self, mut sink: ..., rng: &mut impl Rng, n: usize) -> ... {
    let mut len_iter = self.lengths.row_iter();
    let elem_base = self.elements.row_iter();

    let lengths: Vec<usize> = (0..n).map(|_| len_iter.next(rng)).collect();
    sink.reserve_lists(&lengths);

    for &len in &lengths {
        let mut elems = elem_base.clone();
        sink.push_list(len, (0..len).map(|_| elems.next(rng)));
    }
    sink
}
```

Cost: `Vec<usize>` of n entries (8 bytes each). Saves reallocations of potentially much larger buffers.

### Depth 2 (flat)

Two `Vec<usize>` — outer lengths and all inner lengths flattened:

```rust
// Outer List::collect_into
let outer_lengths: Vec<usize> = (0..n).map(|_| outer_len_iter.next(rng)).collect();
sink.reserve_lists(&outer_lengths);
// → outer offsets.reserve(n), inner ColumnarList.reserve(total_inner_lists)

// Pre-generate ALL inner lengths
let mut inner_len_iter = self.elements.lengths.row_iter();
let total_inner: usize = outer_lengths.iter().sum();
let inner_lengths: Vec<usize> = (0..total_inner).map(|_| inner_len_iter.next(rng)).collect();
sink.inner().reserve_lists(&inner_lengths);
// → inner offsets.reserve(total_inner), leaf values.reserve(total_elements)
```

This requires the outer `List::collect_into` to reach into `self.elements` (the inner List distribution) to access its length distribution. Currently `List::collect_into` treats `self.elements` as an opaque `Distribution`. To pre-generate inner lengths, it needs to know `self.elements` is also a `List`.

**Options:**
1. **Trait method on Distribution**: `fn pre_reserve(&mut self, sink: &mut ..., rng, lengths: &[usize])` that recursively pre-generates and reserves. Scalar distributions ignore it. List distributions generate their lengths and recurse.
2. **Manual**: Only depth-1 reserve is automatic. Depth-2+ callers construct sinks and reserve manually before calling `collect_into`.
3. **Lazy**: Each `List::collect_into` reserves its own level only. Depth-1 reserves offsets + values. Depth-2 outer reserves offsets + inner offsets, but leaf values grow incrementally.

Option 3 is the simplest and still eliminates most reallocation (offsets at every level are reserved; only the leaf values buffer grows incrementally at depth 2+).

Option 1 is the most complete. Sketch:

```rust
pub trait Distribution<B: Backend> {
    // ... existing methods ...

    /// Pre-generate reserve information and reserve sink buffers.
    /// Called before collect_into. Default: reserve sink for n items.
    fn pre_reserve(
        &mut self,
        sink: &mut <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> PreReserveState;
}
```

Where `PreReserveState` carries the pre-generated lengths so `collect_into` can reuse them instead of regenerating:

```rust
pub enum PreReserveState {
    None,
    Lengths(Vec<usize>),
}
```

For `List`:
```rust
fn pre_reserve(&mut self, sink: &mut ..., rng, n) -> PreReserveState {
    let mut len_iter = self.lengths.row_iter();
    let lengths: Vec<usize> = (0..n).map(|_| len_iter.next(rng)).collect();
    sink.reserve_lists(&lengths);

    // Recurse into element distribution for inner levels
    let total: usize = lengths.iter().sum();
    self.elements.pre_reserve(sink.inner(), rng, total);

    PreReserveState::Lengths(lengths)
}
```

Each List level generates its lengths, reserves its sink level, then asks the element distribution to do the same for the inner level. Scalar distributions are no-ops. Composes to any depth.

Then `collect_into` receives the pre-generated lengths:

```rust
fn collect_into_with(
    &mut self,
    mut sink: ...,
    rng: &mut impl Rng,
    state: PreReserveState,
) -> ... {
    let lengths = match state {
        PreReserveState::Lengths(l) => l,
        PreReserveState::None => {
            // fallback: generate on the fly
            (0..n).map(|_| self.lengths.row_iter().next(rng)).collect()
        }
    };
    // ... push using pre-generated lengths
}
```

**Recommendation**: Start with option 3 (each level reserves itself). Add `pre_reserve` later when depth-2+ reserve matters.

## How reserve composes with everything

### Tuples — no change

Tuple `collect_into` splits sinks, delegates to sub-distributions. Each sub-distribution calls `reserve_lists` on its own sink piece. Works naturally.

### Map — no change

Map uses `Sink<Out>::push`. When Out = Vec<T>, calls `Sink<Vec<T>>::push`. Map doesn't call `reserve_lists` (it doesn't know list structure). This is fine — Map's collect_into is row-by-row.

### Flatten — no change

Delegates to inner distribution. If inner is a tuple containing a List, the List's collect_into handles reserve.

### Tracked — no change

Row-by-row via `Sink<D::Item>::push`. Doesn't interact with reserve.

## Backend

```rust
pub trait Backend {
    type Prim<T>: Sink<T>;
    type List<T>: ListSink<T>;
}

impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
    type List<T> = ColumnarList<Vec<T>>;
}
```

DefaultSink for Vec<T> unchanged: `HCons<B::List<T>, HNil>`.

For flat depth-2+, manually construct `ColumnarList<ColumnarList<Vec<T>>>` and pass to `collect_into` directly. Future: `FlatVecBackend` that recursively nests ColumnarList.

## Columnar struct-inside-list

### Problem

`list((i32, u32))` currently stores inner values as `Vec<(i32, u32)>` — one interleaved buffer. We want two contiguous buffers (`Vec<i32>` + `Vec<u32>`) with shared offsets.

Root cause: `DefaultSink<B> for Vec<T>` uses `B::List<T>` which treats `T` as opaque. It can't decompose `(i32, u32)` into separate columns.

### Solution: recursive DefaultSink + SinkLen for HList

Two changes that work together:

**1. SinkLen for HCons** — All columns in an HList sink have the same count (each `push` adds one element to every column), so delegate to the head:

```rust
impl<S: SinkLen, Tail> SinkLen for HCons<S, Tail> {
    fn sink_len(&self) -> usize { self.0.sink_len() }
}
```

This lets `ColumnarList` use an HList of Vecs as its inner storage.

**2. DefaultSink for Vec\<T\> recurses into T** — The inner storage of a list sink becomes the DefaultSink of the element type itself:

```rust
impl<B: Backend, T: DefaultSink<B>> DefaultSink<B> for Vec<T>
where
    <T as DefaultSink<B>>::Sink: SinkLen,
{
    type Sink = HCons<ColumnarList<<T as DefaultSink<B>>::Sink, B::Prim<usize>>, HNil>;
}
```

### ColumnarList gains an Offsets parameter

Offsets can't be hardcoded to `Vec<usize>` — DequeBackend needs `VecDeque<usize>`, array backends need something else. The offset type comes from `B::Prim<usize>`:

```rust
pub struct ColumnarList<Inner, Offsets> {
    pub inner: Inner,
    pub offsets: Offsets,
}
```

### Backend simplifies

`Backend::List<T>` is removed — the list sink type is now fully determined by `ColumnarList` + recursive `DefaultSink`:

```rust
pub trait Backend {
    type Prim<T>: Sink<T> + SinkLen + Default;
}

impl Backend for VecBackend {
    type Prim<T> = Vec<T>;
}

impl Backend for DequeBackend {
    type Prim<T> = VecDeque<T>;
}
```

### Type resolution examples

**`list(i64)` on VecBackend:**
```
T = i64
<i64 as DefaultSink>::Sink = HCons<Vec<i64>, HNil>
→ HCons<ColumnarList<HCons<Vec<i64>, HNil>, Vec<usize>>, HNil>
```
Layout: offsets + one Vec\<i64\> (wrapped in HCons, zero-cost since HNil is ZST).

**`list((i32, u32))` on VecBackend:**
```
T = (i32, u32)
<(i32, u32) as DefaultSink>::Sink = HCons<Vec<i32>, HCons<Vec<u32>, HNil>>
→ HCons<ColumnarList<HCons<Vec<i32>, HCons<Vec<u32>, HNil>>, Vec<usize>>, HNil>
```
Layout: one shared offsets + Vec\<i32\> + Vec\<u32\>. Columnar struct-inside-list.

**`list(list(i64))` on VecBackend:**
```
T = Vec<i64>
<Vec<i64> as DefaultSink>::Sink = HCons<ColumnarList<HCons<Vec<i64>, HNil>, Vec<usize>>, HNil>
→ HCons<ColumnarList<HCons<ColumnarList<HCons<Vec<i64>, HNil>, Vec<usize>>, HNil>, Vec<usize>>, HNil>
```
Layout: outer offsets + inner offsets + Vec\<i64\>. Flat depth-2 comes for free.

**`list(list((i32, u32)))` on VecBackend:**
```
→ outer offsets + inner offsets + Vec<i32> + Vec<u32>
```
Fully columnar at every level, zero heap allocation per element.

### Verified: existing code is unaffected

- **List::collect\_into** — `sink.push_list(len, iter)` compiles; the `ListSink<E::Item>` bound carries all needed information
- **Tuple distributions with lists** — `(constant(42i32), list(...))` splits via `HAppend::unappend`, each sub-distribution gets its own sink slice. ColumnarList ends up in the right HList position
- **Flatten with lists** — HAppend associativity still holds; nested and flat tuple sinks resolve to the same type, including when ColumnarList appears in the HList
- **collect\_flat / collect\_col** — `IntoHList`/`IntoTuple` for 1-tuples works for any type, including ColumnarList
- **Map producing Vec\<T\>** — uses `Sink<Vec<T>>::push` on the HList wrapper, which delegates to `ColumnarList::push`
- **ValueHList for Vec\<T\>** — unchanged, still `HCons<Vec<T>, HNil>`. Value side, not sink side

### Known issues

**1. Missing Default for HCons/HNil** — `ColumnarList::new()` needs `Inner: Default`. When Inner is an HList like `HCons<Vec<i32>, HCons<Vec<u32>, HNil>>`, there's no Default impl. Need to add:
```rust
impl Default for HNil { fn default() -> Self { HNil } }
impl<H: Default, T: Default> Default for HCons<H, T> {
    fn default() -> Self { HCons(H::default(), T::default()) }
}
```

**2. Sink\<Vec\<T\>\>::push can't call push\_list** — The `Sink<Vec<T>>` impl for ColumnarList can't call `self.push_list()` because that requires `Self: ListSink<T>` which isn't in the Sink impl's bounds. Must inline the logic:
```rust
impl<T, Inner: Sink<T> + SinkLen, Offsets: Sink<usize>> Sink<Vec<T>>
    for ColumnarList<Inner, Offsets>
{
    fn push(&mut self, value: Vec<T>) {
        self.inner.push_n(value.into_iter());
        self.offsets.push(self.inner.sink_len());
    }
}
```

**3. No extend optimization for struct-inside-list** — Current `VecListSink<T>::push_list` calls `self.values.extend(elems)` — one extend call. New `ColumnarList<HCons<Vec<i32>, HCons<Vec<u32>, HNil>>>`: the default `push_list` calls `push` per element to destructure each `(i32, u32)` into columns. No way to `extend` two Vecs simultaneously from one iterator of tuples. Inherent cost of columnar layout — trade batch writes for better memory layout. For scalar lists (`list(i64)`), override `push_n` on `HCons<S, HNil>` to delegate to `S::push_n`, preserving the `Vec::extend` path.

**4. DefaultSink for Vec\<T\> becomes narrower** — Old: `impl<B, T> DefaultSink<B> for Vec<T>` (any T). New: requires `T: DefaultSink<B>`. No practical breakage — list elements are always types that already have DefaultSink.

**5. Backend::Prim\<T\> gains bounds** — `Sink<T> + SinkLen + Default` instead of just `Sink<T>`. Both Vec and VecDeque satisfy these. Reasonable constraint for any columnar storage.

## Summary

| What | Addition | Purpose |
|------|----------|---------|
| `Sink<T>` | `reserve(&mut self, n: usize)` | Pre-allocate capacity |
| `ListSink<T>` | `inner(&mut self) -> &mut Inner` | Access inner storage |
| `ListSink<T>` | `end_list(&mut self)` | Record list boundary |
| `ListSink<T>` | `reserve_lists(&mut self, &[usize])` | Reserve with per-list sizes |
| `ListSink<T>` | `push_list` becomes default | Built on inner + end_list |
| `SinkLen` | `sink_len(&self) -> usize` | Inner position for end_list |
| `SinkLen` | impl for `HCons<S, Tail>` | Enables HList inner in ColumnarList |
| `ColumnarList<Inner, Offsets>` | New type | Flat list storage, nests to any depth |
| `DefaultSink for Vec<T>` | Recurses into T's DefaultSink | Columnar struct-inside-list |
| `Backend` | Remove `List<T>`, add bounds on `Prim<T>` | Offsets via `B::Prim<usize>` |
| `HCons`/`HNil` | `Default` impls | Construct empty HList sinks |

State needed for reserve:
- Depth 1: `Vec<usize>` of lengths (pre-generated from length distribution)
- Depth 2: outer lengths + inner lengths (requires reaching into inner List's length distribution)
- Depth N: one `Vec<usize>` per level, generated recursively via `pre_reserve` or manually
