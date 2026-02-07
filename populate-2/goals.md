# Data Comb Goals & Constraints

Consolidated from: code, `dist-redesign.md`, `list-sink-redesign.md`, `named-columns-design.md`, `old-populate-reference.md`, `trait-split-design.md`.

---

## 1. Collection Strategies

### 1a. Col-by-col collection (primary)

Independent columns are generated one at a time — all N values of column 1, then all N values of column 2, etc. This is the default for tuple distributions.

**Why:** Cache-friendly writes to contiguous buffers. Each column's sink receives a dense stream of pushes with no interleaving. Enables `Vec::extend`-style bulk paths.

**Current:** Tuple `collect_into` splits the HList sink via `HAppend::unappend`, delegates to each sub-distribution sequentially, then re-appends.

**Verified by:** Tracked wrapper tests asserting `["COL1","COL1","COL1","COL2","COL2","COL2"]` order.

### 1b. Row-by-row collection (fallback)

All columns of one row are generated together before moving to the next row.

**When required:**
- `Map` — the mapping function consumes the full row, so all input columns must be generated together
- `Seeded` — per-column RNG means generation order matters
- Any combinator where columns are not independent

**Current:** `row_iter()` returns a `SampleIter` producing full row tuples. `Sink<(A,B)>` impls destructure and push to individual sinks. Tests verify `["COL1","COL2","COL1","COL2"]` order.

### 1c. Mixed col/row within a tuple

When a tuple contains both independent columns and a `Map`, the independent columns collect col-by-col and the Map collects row-by-row. The outer tuple orchestrates: col-by-col for leaf distributions, row-by-row for Map.

**Verified by:** `test_map_in_tuple_order` asserting `["COL1"x3, "MAP_SRC"x3, "COL3"x3]`.

---

## 2. Auto-flattening

Nested tuple distributions like `(D1, (D2, D3), D4)` should produce a flat value type `(T1, T2, T3, T4)` and a flat sink type, without manual wrappers.

**Current approach:** `Flatten<D>` wrapper + `ValueHList` trait for value flattening. The sink types are already flat due to `HAppend` associativity — `Flatten::collect_into` just delegates to the inner distribution.

**Redesign goal:** Eliminate `Flatten<D>` entirely. Build auto-flattening into the tuple Distribution impls via `ValueHList` + `IntoTuple` on `TupleIterN`. The tuple `collect_into` already works (sinks are identical) — only `row_iter` and `type Item` need the flatten logic.

---

## 3. Map with dependent columns

`Map<D, F>` transforms input columns into output columns. The output can be:
- Scalar: `map(range(0,1), |x| x * 2)` — one column in, one column out
- Tuple: `map((d1, d2), |(a,b)| (a+b, a*b))` — two columns in, two columns out

Map always collects row-by-row (the function creates a data dependency between input and output columns). When Map is inside a tuple, the outer tuple splits the sink: Map gets its own sink slice and fills it row-by-row, while sibling independent columns fill theirs col-by-col.

**Constraint:** Map's output type needs a `SinkFor`/`DefaultSink` impl to resolve its sink type, since the type comes from an arbitrary closure, not from the distribution tree.

---

## 4. List/array columns

Variable-length list columns: each row is a `Vec<T>`.

### 4a. Columnar list storage

Flat representation: one contiguous values buffer + one offsets buffer (`ColumnarList<Inner, Offsets>`). No per-list heap allocation.

**Composes to any depth:** `list(list(i64))` → nested `ColumnarList` (outer offsets + inner offsets + flat values).

### 4b. Struct-inside-list

`list((i32, u32))` should store two contiguous buffers (`Vec<i32>` + `Vec<u32>`) with shared offsets, not one interleaved `Vec<(i32, u32)>`.

**Solution:** `DefaultSink for Vec<T>` recurses into `T`'s `DefaultSink`. Inner storage of `ColumnarList` becomes an HList of sinks. Requires `SinkLen` for HList (delegate to head element).

### 4c. Pre-allocation / reserve

**Goal:** Pre-allocate all sink buffers so collection doesn't reallocate.

- `Sink<T>::reserve(n)` — reserve capacity for n pushes
- `ListSink<T>::reserve_lists(&[usize])` — reserve with per-list sizes. Flat: `offsets.reserve(n)`, `inner.reserve(sum)`. Non-flat: store upcoming sizes for per-Vec pre-allocation.
- HList sinks delegate reserve to each column (same n for all)

**Pre-generating lengths:** Call length distribution ahead of time to get all list sizes, then reserve, then push. Cost: one `Vec<usize>` per list level. Saves potentially many reallocations.

**Depth composition:** Each level reserves itself (option 3 — simplest). Or recursive `pre_reserve` method for full depth-N reservation (option 1 — most complete).

### 4d. push_list optimization

`ListSink::push_list(len, iter)` enables batch element insertion. For scalar lists, `push_n` on `Vec<T>` delegates to `Vec::extend`. For struct-inside-list with columnar storage, must destructure per-element (inherent cost of columnar layout).

---

## 5. Backend abstraction

`Backend` parameterizes concrete sink types. Different backends serve different consumers:

| Backend | Prim<T> | List<T> | Use case |
|---------|---------|---------|----------|
| VecBackend | Vec<T> | Vec<Vec<T>> / ColumnarList | In-memory, tests |
| DequeBackend | VecDeque<T> | DequeListSink<T> | Streaming / ring-buffer |
| ArrowBackend (future) | ArrowBuilder<T> | ArrowListBuilder<T> | Arrow columnar format |

**Constraint:** All backend primitive types must impl `Sink<T>`. After redesign, also `SinkLen + Default` for capacity reservation and `default_sink()` construction.

**Backend simplification goal:** Remove `Backend::List<T>`. List sink type becomes fully determined by `ColumnarList` + recursive `DefaultSink`. Backend reduces to just `type Prim<T>`.

---

## 6. Ergonomic API

### 6a. Minimal where clauses

Current generic functions need verbose bounds like `<T as DefaultSink<B>>::Sink: Sink<T>` and `<S1 as HAppend<S2>>`.

**Goal (dist-redesign):** `Distribution` has explicit `type Sink` associated type. Helper functions reduce to `impl Distribution<B, Value = (i32, i64)>` with no extra bounds.

### 6b. default_sink — distribution constructs its own sink

**Goal:** `default_sink(&self) -> Self::Sink` on Distribution (or extension trait). Eliminates manual sink construction at call sites.

**Before:** `dist.collect_flat((Vec::<i32>::new(), Vec::<i64>::new()), &mut rng(), n)`
**After:** `dist.collect_flat(&mut rng(), n)`

Leaf distributions construct their sinks from the Backend. Tuple distributions compose via `append`. Names flow through for backends that care (Arrow).

### 6c. default_sink_with_capacity — pre-allocated sink construction

**Goal:** `default_sink_with_capacity(&self, n: usize) -> Self::Sink` constructs a sink tree with all buffers pre-allocated for `n` rows.

```rust
fn default_sink_with_capacity(&self, n: usize) -> Self::Sink;
```

**How it flows down the tree:**

| Distribution | Behaviour |
|-------------|-----------|
| Leaf (Constant, Range) | `Vec::with_capacity(n)` (or backend equivalent) |
| Tuple (D1, D2) | `self.0.default_sink_with_capacity(n).append(self.1.default_sink_with_capacity(n))` |
| Map | `Vec::with_capacity(n)` per output column via `SinkFor` |
| List (depth 1) | Offsets: `with_capacity(n)`. Inner values: **cannot pre-size** without knowing lengths — leave at 0 or accept an estimate. Use `reserve_lists` after pre-generating lengths for exact sizing. |
| List (depth 2+) | Same — each level's offsets get `n`, inner defers to `reserve_lists` |
| Nested | Delegates to inner, wraps in Group |

**Relationship to reserve:** `default_sink_with_capacity(n)` handles the simple case (scalar columns all get `n`). For list columns where inner buffer size depends on generated lengths, `reserve_lists` is still needed after length pre-generation. The two are complementary:

```rust
// Simple: scalar columns only
let sink = dist.default_sink_with_capacity(1_000_000);
let result = dist.collect_into(sink, &mut rng, 1_000_000);

// List columns: capacity + reserve
let mut sink = dist.default_sink_with_capacity(1_000_000);
dist.pre_reserve(&mut sink, &mut rng, 1_000_000); // generates lengths, reserves inner buffers
let result = dist.collect_into(sink, &mut rng, 1_000_000);
```

**Backend requirement:** `Backend::Prim<T>` needs a capacity constructor. Options:
- Add `fn with_capacity(n: usize) -> Self` to a trait bound on `Prim<T>` (Vec and VecDeque both support this)
- Add `Backend::new_prim_with_capacity<T>(name, n)` alongside `new_prim<T>(name)`
- Add a `ReservableSink` trait with `fn with_capacity(n: usize) -> Self`

### 6d. Scalar collection convenience

`collect_col` unwraps the HCons for single-column distributions:

**Before:** `dist.collect_into(HCons(Vec::new(), HNil), rng, n).0`
**After:** `dist.collect_col(rng, n)` → `Vec<T>` directly

### 6e. Flat tuple collection

`collect_flat` converts HList sink ↔ tuple via `IntoHList`/`IntoTuple`:

User passes/receives `(Vec<i32>, Vec<i64>)` instead of `HCons<Vec<i32>, HCons<Vec<i64>, HNil>>`.

---

## 7. Named columns

### 7a. Column names in distributions

Each leaf distribution carries a `name: &'static str`. Names flow to sink construction.

### 7b. Backend-controlled naming

- VecBackend ignores names (zero cost via `Transparent<S>` wrapper)
- ArrowBackend embeds names in builders via `Named<S>` wrapper

### 7c. Nested/grouped columns

`nested("b", (range("a", ..), range("b", ..)))` produces a `Column<(u16, u32)>` value. The group wrapper (`Transparent` or `Named`) prevents HList flattening — the group stays as one logical unit in the outer sink HList.

`GroupSink` trait provides `into_inner()` for unwrapping during `collect_into`.

### 7d. Distribution as schema

The distribution tree defines the full schema. `default_sink()` constructs a correctly-named, correctly-typed sink tree. Arrow backend can reconstruct field metadata from the sink tree.

---

## 8. Trait design

### 8a. RowDistribution + Distribution split

**Goal:** Separate row iteration (no Backend/HList/DefaultSink bounds) from columnar collection.

- `RowDistribution` — `type Item`, `type RowIter`, `fn row_iter()`. No backend parameter.
- `Distribution<B>: RowDistribution` — adds `collect_into`. Needs Backend + sink bounds.

**Benefits:**
- `Map`, `Flatten`, custom wrappers only need `D: RowDistribution` — lighter bounds
- Feature-gatable columnar support
- Less monomorphization for row-only consumers

### 8b. SinkFor replaces DefaultSink

`SinkFor<B>` — same type-level mapping (value type → sink type) but with `Sink<Self> + Default` bounds. **Not** a bound on `Distribution::Value` — only required by `Map` (to resolve sink for arbitrary output type).

### 8c. No default collect_into

A default `collect_into` using `row_iter` + `Sink::push` doesn't work for nested tuple items. `(i32, (i64, i64), i32)` has a 4-element HList sink but a 3-element tuple value — no `Sink<(A, (B,C), D)>` impl for 4-element HLists. Each Distribution impl must provide its own `collect_into`.

---

## 9. Performance constraints

### 9a. Zero-allocation collection path

Col-by-col collection should not allocate beyond the sink buffers themselves. No intermediate `Vec<Row>`, no boxing, no allocation per push.

### 9b. Bulk push paths

`push_n` / `Vec::extend` for scalar columns. Avoid per-element function call overhead where possible.

### 9c. Pre-allocated buffers

`reserve` on sinks before collection begins. For lists, `reserve_lists` with pre-generated lengths to size all buffers upfront.

### 9d. HList is zero-cost

`HCons<H, T>` and `HNil` compile down to flat structs. `HNil` is ZST. `HCons<Vec<i32>, HCons<Vec<i64>, HNil>>` has the same layout as `(Vec<i32>, Vec<i64>)`. `append`/`unappend` should compile to no-ops.

### 9e. No overhead for unused features

VecBackend ignores column names — `Transparent<S>` is a zero-cost newtype. Backend parameterization is monomorphized away. `RowDistribution` without columnar doesn't pull in HList/sink machinery.

---

## 10. Parallel generation via skip

### 10a. Goal

Partition N total rows across T threads. Thread `i` generates rows `[i*chunk, (i+1)*chunk)`. Each thread gets its own clone of the distribution + its own sink, calls `skip(offset)` to jump ahead, then `collect_into` for its chunk. Results are concatenated (or written to disjoint buffer regions).

```rust
let handles: Vec<_> = (0..num_threads).map(|i| {
    let mut dist = dist.clone();
    let chunk = total / num_threads;
    let offset = i * chunk;
    thread::spawn(move || {
        let mut rng = make_rng(seed + i);
        dist.skip(offset, &mut rng);  // advance to this thread's starting point
        let sink = dist.default_sink_with_capacity(chunk);
        dist.collect_into(sink, &mut rng, chunk)
    })
}).collect();
```

### 10b. What skip means per distribution

`skip(n, rng)` advances the distribution's internal state as if `n` rows had been generated, without producing output.

| Distribution | Skip behaviour | Cost |
|-------------|---------------|------|
| Constant | No-op (stateless) | O(1) |
| Range/Inc | `self.start += n * self.step` | O(1) |
| Random (uniform, etc.) | Advance RNG by `n` calls | O(n) or O(log n) with special RNG |
| TpchRng | `rng.skip(n)` via binary exponentiation | O(log n) |
| Map<D, F> | `self.source.skip(n, rng)` — skip the source, function is stateless | Same as source |
| Tuple (D1, D2) | `self.0.skip(n, rng); self.1.skip(n, rng)` — skip each independently | Sum of children |
| Flatten<D> | Delegates to inner | Same as inner |
| Nested<D> | Delegates to inner | Same as inner |
| Tracked<D> | Delegates to inner (don't log) | Same as inner |

### 10c. List — the hard case

List columns are fundamentally difficult to skip because the number of *elements* to skip depends on the *generated lengths* of the skipped rows.

To skip `n` list rows:
1. Generate all `n` lengths from the length distribution (advancing its RNG)
2. Sum them to get total elements to skip
3. Skip the element distribution by that sum

```rust
impl List<L, E> {
    fn skip(&mut self, n: usize, rng: &mut impl Rng) {
        let mut len_iter = self.lengths.row_iter();
        let total_elements: usize = (0..n).map(|_| len_iter.next(rng)).sum();
        self.lengths.skip(n, rng);
        self.elements.skip(total_elements, rng);
    }
}
```

**Cost:** O(n) for the length generation (can't skip without knowing lengths). If the length distribution is Constant, this could be optimized to O(1): `total = n * constant_len`.

For depth-2 lists (`list(list(i64))`), this recurses — skip outer lengths, generate them to compute inner lengths, skip inner lengths, generate them to compute leaf element count, skip leaf elements. Each level is O(rows_at_that_level).

### 10d. Seeded distributions

`Seeded<R, D>` wraps a distribution with its own RNG. Skip must advance both the seeded RNG and the inner distribution:

```rust
fn skip(&mut self, n: usize) {
    // Use own RNG, not the passed-in one
    self.inner.skip(n, &mut self.rng);
}
```

Each seeded column has an independent RNG that can be skipped independently. This is a natural fit for parallel generation — each thread clones the distribution and skips its own seeded RNGs.

### 10e. Graph walk — how to know skip is complete

**Question:** When a distribution tree has complex structure (tuples of maps of tuples with lists), how do we know every node has been correctly skipped?

**Answer:** `skip` is a recursive tree walk, same structure as `collect_into`. Each node is responsible for skipping itself and its children. The trait method signature ensures every implementor handles it:

```rust
pub trait Distribution<B: Backend> {
    // ...existing methods...
    fn skip(&mut self, n: usize, rng: &mut impl Rng);
}
```

**Completeness is structural:** If every `Distribution` impl correctly skips its children, the full tree is skipped. This is the same guarantee as `collect_into` — if every impl correctly collects its children, the full tree is collected. No separate "walk complete" check is needed.

**Where state lives:** On the distribution structs themselves (`&mut self`). Each distribution holds whatever state it needs:
- `Range` holds `start: T` — skip mutates it
- `List` holds `lengths: L, elements: E` — skip advances both
- `Seeded` holds `rng: R` — skip advances it
- `Constant` holds `value: T` — skip is a no-op

### 10f. Requirements for parallel skip

For parallel generation to work, distributions must be **`Clone`**. Each thread gets its own clone, skips independently. This mirrors v1's `SampleIter: Clone` requirement.

**Clone semantics:** A cloned distribution produces the same sequence given the same RNG state. Skip + generate on a clone is equivalent to generate-all on the original and taking a slice.

**Determinism constraint:** For reproducible output, the RNG seed scheme must be deterministic. Options:
- Single seed → derive per-thread seeds: `seed + thread_id`
- Per-column seeds (like TPC-H) → each thread clones the per-column seed and skips

### 10g. Open questions

1. **RNG skip cost:** Standard `rand` RNGs (ChaCha, etc.) don't have O(log n) skip. Options: (a) accept O(n) skip cost (fine if skip is done once per thread), (b) use a skippable RNG like TpchRng or PCG, (c) derive independent seeds per thread instead of skipping.

2. **Skip without RNG:** Some distributions don't need the RNG at all (Constant, Range/Inc). Should `skip` take `Option<&mut Rng>` or always require it? Always requiring it is simpler — callers always have one, and no-op impls just ignore it.

3. **Skip for Map:** Map's closure is stateless, but the *source* distribution may have state. `Map::skip` = `self.source.skip(n, rng)`. But if Map's source is a tuple that collects col-by-col, skip must advance each sub-distribution by `n`. This is handled by the recursive walk — Map skips its source, source (tuple) skips its children.

4. **Verification:** How to test that skip produces correct output? Generate N rows normally, compare against skip(K) + generate(N-K). For lists, verify that skipped length generation matches.

5. **Concatenation:** After parallel generation, thread sinks must be merged. For Vec sinks this is `Vec::append` or writing to pre-partitioned slices. For Arrow builders, this is `concat_batches`. Should the framework provide merge utilities?

---

## 11. Extensibility


### 11a. Custom distributions

Users can implement `Distribution<B>` for custom types. The trait surface is minimal: `row_iter()`, `collect_into()`, (optionally `default_sink()`).

### 11b. Custom backends

Implement `Backend` with custom `Prim<T>` (and possibly `List<T>`, `Group<S>`) to target different storage formats.

### 11c. Arity extensibility

Current: 2, 3, 4-tuple Distribution impls, 1-5-tuple IntoHList/IntoTuple. Extendable via macros. HList-based approach means one recursive definition handles composition regardless of arity.

### 11d. Planned distributions (from v1 FUTURE.md)

| Distribution | Description |
|-------------|-------------|
| Cycle<T, N> | Deterministic repeating sequences |
| PickOne/Pick/PickN | Slice sampling family |
| Bernoulli | Biased boolean |
| Weighted<T> | Non-uniform selection |
| WithNulls<D> | Nullable column |
| Runs<D> | Run-length friendly data |
| Monotonic<D> | Accumulating deltas |
| FlatMap | Output determines next distribution |
| ForeignKey | Reference values from another column |
| Seeded<R, D> | Per-column RNG override |
| Join/JoinVec | String joining combinators |

---

## 12. Composition rules summary

| Combinator | Value type | Sink type | Collection | Flattens? |
|-----------|-----------|-----------|------------|-----------|
| Leaf (Constant, Range) | T | HCons<B::Prim<T>, HNil> | col-by-col | N/A |
| (D1, D2) tuple | (T1, T2) or flat | S1.append(S2) | col-by-col (splits sink) | Yes (redesign) |
| Map<D, F> | F::Output | SinkFor<Out> | row-by-row | No |
| List<L, E> | Vec<E::Item> | HCons<ListSink, HNil> | row-by-row (per list) | No |
| Flatten<D> (current) | flat tuple | same as inner | delegates | Yes |
| Nested<D> (future) | Column<T> | HCons<Group<inner_sink>, HNil> | col-by-col (unwraps) | No (prevents) |
| Tracked<D> (test) | D::Item | D::Sink | row-by-row + logging | No |

---

## 13. Key invariants

1. **HAppend associativity:** Nested and flat tuple `DefaultSink::Sink` resolve to the same HList type. This is why `Flatten::collect_into` can just delegate — the sinks are identical.

2. **Sink type = column count:** Each scalar column contributes one HList element. A 4-column distribution has a 4-element HList sink, regardless of how the columns are grouped in the distribution tree.

3. **Value type can differ from sink arity:** `(i32, (i64, i64))` is a 2-element value tuple but has a 3-element HList sink. This is why `Sink<Value>` can't be a blanket bound — the arities don't match for nested tuples.

4. **Collection order is per-distribution:** Each distribution decides its own order (col-by-col or row-by-row). The outer tuple doesn't override — it just splits the sink and delegates.

5. **Backend is compile-time:** No runtime dispatch. All sink types resolved at compile time via associated types and monomorphization.
