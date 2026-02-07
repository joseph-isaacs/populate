# Plan: Remove `Backend::List<T>`, introduce `ColumnarList<Inner, Offsets>`

## Context

Currently `Backend::List<T>` is an associated type that maps scalar element types to list sinks (e.g., `Vec<Vec<T>>` for VecBackend, `DequeListSink<T>` for DequeBackend). This only works for scalar list elements — `list((i32, u32))` would store `Vec<Vec<(i32, u32)>>` rather than decomposing into two contiguous buffers with shared offsets.

The fix: replace `Backend::List<T>` with a generic `ColumnarList<Inner, Offsets>` that uses the element distribution's own sink type as its inner storage. For `list((i32, u32))`, the inner becomes `HCons<Vec<i32>, HCons<Vec<u32>, HNil>>` — true columnar struct-inside-list.

## Files to modify

1. **`populate-2/src/traits.rs`** — bulk of the changes
2. **`populate-2/src/test/list.rs`** — update test assertions
3. **`populate-2/src/test/named.rs`** — update named list test assertion
4. **`populate-2/src/dist.rs`** — no code changes expected (bounds resolve through `SinkOf`)

## Steps

### 1. Add `SinkLen` trait + impls (traits.rs, after Sink impls ~line 273)

```rust
pub trait SinkLen {
    fn sink_len(&self) -> usize;
}
```

Impls for: `Vec<T>`, `VecDeque<T>`, `HCons<S: SinkLen, Tail>` (delegates to head).

### 2. Add `ColumnarList<Inner, Offsets>` (traits.rs, after SinkLen)

```rust
pub struct ColumnarList<Inner, Offsets> {
    pub inner: Inner,
    pub offsets: Offsets,
}
```

- Custom `Default` where `Inner: Default, Offsets: Default + Sink<usize>`: initializes offsets with `[0]` via `Sink::push`
- `SinkLen` impl where `Offsets: SinkLen`: `self.offsets.sink_len() - 1`
- `Sink<Vec<T>>` impl where `Inner: Sink<T> + SinkLen, Offsets: Sink<usize>`:
  ```rust
  fn push(&mut self, value: Vec<T>) {
      self.inner.push_n(value.into_iter());
      self.offsets.push(self.inner.sink_len());
  }
  ```
- `ListSink<T>` impl where `Inner: Sink<T> + SinkLen, Offsets: Sink<usize>`:
  - `type Inner = Inner`
  - `inner()` returns `&mut self.inner`
  - `end_list()` pushes `self.inner.sink_len()` into `self.offsets`

### 3. Expand `ListSink<T>` trait (traits.rs:279-281)

Add `type Inner`, `inner()`, `end_list()`; make `push_list` a default method:

```rust
pub trait ListSink<T> {
    type Inner: Sink<T>;
    fn inner(&mut self) -> &mut Self::Inner;
    fn end_list(&mut self);
    fn push_list(&mut self, _len: usize, elems: impl Iterator<Item = T>) {
        self.inner().push_n(elems);
        self.end_list();
    }
}
```

Update existing `ListSink` implementors:

- **`ColumnarList`** — `type Inner = Inner`, `inner()` → `&mut self.inner`, `end_list()` → push offset
- **`HCons<S, HNil>`** — delegates all three to `self.0`
- **`NamedCol<S>`** — delegates all three to `self.inner`

Remove `ListSink` impls for `Vec<Vec<T>>`, `VecListSink`, and `DequeListSink` (all dead after the Backend change).

### 4. Rename `Backend::Col<T>` → `Prim<T>`, replace `ListCol<T>` with `Column<S>` (traits.rs:11-35)

Current Backend trait has `Col<T>` and `ListCol<T>`. Rename and restructure:

```rust
pub trait Backend: Sized {
    type Prim<T>: Sink<T> + SinkLen + Default;
    type Column<S>;
}
```

Update all three Backend impls:
- **VecBackend**: `Prim<T> = Vec<T>`, `Column<S> = S`
- **DequeBackend**: `Prim<T> = VecDeque<T>`, `Column<S> = S`
- **NamedVecBackend**: `Prim<T> = NamedCol<Vec<T>>`, `Column<S> = NamedCol<S>`

Also update the scalar `SinkFor` macro (line 134) to use `B::Prim<$t>` instead of `B::Col<$t>`.

### 5. Update `SinkFor<B> for Vec<T>` (traits.rs:91-93)

Currently: `impl<B: Backend, T> SinkFor<B> for Vec<T> { type Sink = HCons<B::ListCol<T>, HNil>; }`

Replace with:
```rust
impl<B: Backend, T: SinkFor<B>> SinkFor<B> for Vec<T>
where
    B::Column<ColumnarList<<T as SinkFor<B>>::Sink, B::Prim<usize>>>: Sink<Vec<T>> + Default,
{
    type Sink = HCons<B::Column<ColumnarList<<T as SinkFor<B>>::Sink, B::Prim<usize>>>, HNil>;
}
```

Key change: `T` now requires `SinkFor<B>` so inner storage decomposes recursively. The `B::Column<...>` wrapper allows NamedVecBackend to wrap with `NamedCol`.

### 6. Remove dead list sink types and impls (traits.rs)

Delete:
- `VecListSink<T>` struct and all impls (lines 284-320)
- `Vec<Vec<T>>` ListSink + Sink impls (lines 322-327) — no longer used by any Backend
- `DequeListSink<T>` struct and all impls (lines 340-378)

`ColumnarList<Inner, Offsets>` replaces all of them.

### 7. Update tests

**test/list.rs** — 3 tests change:

- `test_list_collect_col`: replace `Vec::<Vec<i64>>::new()` with `ColumnarList::default()`, assert on `.inner.0` (flat values) and `.offsets`
- `test_list_in_tuple`: replace `Vec::<Vec<i64>>::new()` with `ColumnarList::default()` in `collect_flat_into`, assert columnar layout
- `test_tracked_list_in_tuple`: same pattern

Add `ColumnarList` to imports.

**test/named.rs** — `test_named_list_in_tuple` (line 74-81):

`c2.inner` changes from `Vec<Vec<i64>>` to `ColumnarList<HCons<Vec<i64>, HNil>, Vec<usize>>`. Assert on `c2.inner.inner.0` and `c2.inner.offsets`.

### 8. dist.rs — verify no changes needed

`List`'s Distribution impl bounds (`Vec<E::Value>: SinkFor<B>`, `SinkOf<B, Vec<E::Value>>: ListSink<E::Value>`) resolve correctly through the updated `SinkFor` impl. `collect_into` body calls `sink.push_list(len, iter)` — unchanged. May need to add `ColumnarList` to imports if the compiler requires it for bound resolution (unlikely since bounds use `SinkOf`).

## Key type resolutions after change

| Expression | VecBackend resolves to |
|---|---|
| `SinkOf<B, Vec<i64>>` | `HCons<ColumnarList<HCons<Vec<i64>, HNil>, Vec<usize>>, HNil>` |
| `SinkOf<B, Vec<(i32, i64)>>` | `HCons<ColumnarList<HCons<Vec<i32>, HCons<Vec<i64>, HNil>>, Vec<usize>>, HNil>` |

## Verification

```bash
cargo test -p populate-2
cargo fmt --check -p populate-2
cargo clippy -p populate-2 -- -D warnings
```
