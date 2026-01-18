# Base Trait Rename Plan

## Analysis of Current Traits vs Rust Ecosystem

### Conflicts and Confusion with Standard Crates

| Current Name | Conflicts With | Issue |
|-------------|----------------|-------|
| `Distribution<T>` | `rand::Distribution<T>` | **Semantic mismatch**: rand's Distribution is *stateless* with `fn sample(&self, rng) -> T`. Ours is a *mutable factory* with `fn sample(&mut self) -> Iter` |
| `SampleIter` | `rand::DistIter` | Similar concept but different API signature |
| `Sink<T>` | `futures::Sink<T>` | Same concept (value consumer) - **good alignment** |
| `Collector<D, T>` | `std::iter::FromIterator` | Confusing since `Iterator::collect()` exists in std |

### Trait Semantics Summary

| Trait | What It Actually Does |
|-------|----------------------|
| `SampleIter` | Infinite iterator producing values via `next(&mut self, rng) -> Item` |
| `Distribution<T>` | Factory that creates `SampleIter` instances via `sample(&mut self) -> Iter` |
| `Sink<T>` | Consumer that accumulates values via `push()` and finalizes via `finish()` |
| `ListSink<T>` | Nested-aware sink with `start_list()`, `push_element()`, `end_list()` |
| `Collector<D, T>` | Format adapter that collects a distribution into a specific output type |

---

## Rename Options

### Option A: Avoid `rand` Conflict (Recommended)

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `SampleIter` | **`Sampler`** | Noun form, "the thing that samples". Cleaner, no `Iter` suffix since it's not a std Iterator |
| `Distribution<T>` | **`Generator<T>`** | Common programming term for "thing that generates values". Avoids rand conflict |
| `Sink<T>` | `Sink<T>` | Keep as-is, aligns with futures crate |
| `ListSink<T>` | `ListSink<T>` | Keep as-is, consistent with Sink |
| `Collector<D, T>` | **`Materializer<D, T>`** | "Materializes" abstract generator into concrete output. Avoids confusion with `Iterator::collect()` |
| `CollectExt<T>` | **`MaterializeExt<T>`** | Follows Materializer rename |

**Pros:**
- No naming conflicts with rand or std
- `Generator` is intuitive for programmers
- `Sampler` clearly indicates "thing that samples"
- `Materializer` accurately describes the operation

**Cons:**
- `Generator` might evoke Python/JS generators (different concept)

---

### Option B: Domain-Specific Naming

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `SampleIter` | **`DataStream`** | Emphasizes streaming/infinite nature |
| `Distribution<T>` | **`Schema<T>`** | Describes "how to generate" data |
| `Sink<T>` | **`Builder<T>`** | Builder pattern terminology |
| `ListSink<T>` | **`ListBuilder<T>`** | Consistent with Builder |
| `Collector<D, T>` | **`Format<D, T>`** | Represents an output format |
| `CollectExt<T>` | **`FormatExt<T>`** | Follows Format rename |

**Pros:**
- Unique to this domain
- `Schema` captures the declarative nature

**Cons:**
- `Schema` might suggest validation/typing rather than generation
- `Builder` conflicts with common pattern meaning

---

### Option C: Minimal Changes (Conservative)

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `SampleIter` | **`Sampler`** | Just drop the `Iter` suffix |
| `Distribution<T>` | `Distribution<T>` | Keep, document the difference from rand |
| `Sink<T>` | `Sink<T>` | Keep as-is |
| `ListSink<T>` | `ListSink<T>` | Keep as-is |
| `Collector<D, T>` | **`OutputCollector<D, T>`** | Disambiguate from Iterator::collect |
| `CollectExt<T>` | `CollectExt<T>` | Keep as-is |

**Pros:**
- Minimal churn
- Familiar names

**Cons:**
- `Distribution` still conflicts conceptually with rand

---

## Recommendation: Option A

The recommended renames are:

```
SampleIter      → Sampler
Distribution<T> → Generator<T>
Sink<T>         → Sink<T>         (no change)
ListSink<T>     → ListSink<T>     (no change)
Collector<D, T> → Materializer<D, T>
CollectExt<T>   → MaterializeExt<T>
```

### Associated Type/Struct Renames

With Option A, these would also change:

| Current | Proposed |
|---------|----------|
| `Distribution::Iter` | `Generator::Sampler` |
| `TupleIter2`, `TupleIter3`, etc. | `TupleSampler2`, `TupleSampler3`, etc. |
| `TupleUnnest2Iter` | `TupleUnnest2Sampler` |

### Method Renames

| Current | Proposed |
|---------|----------|
| `Distribution::sample()` | `Generator::sampler()` or `Generator::make_sampler()` |
| `CollectExt::collect()` | `MaterializeExt::materialize()` |

---

## Alternative: Keep `sample()` method name

If `Generator::sample()` feels right (since generators "sample" their output space), keep the method name:

```rust
pub trait Generator<T> {
    type Sampler: Sampler<Item = T>;
    fn sample(&mut self) -> Self::Sampler;  // keep as sample()
}
```

This reads naturally: "a generator produces a sampler when you call sample()".

---

## Sources

- [rand::Distribution trait](https://docs.rs/rand/latest/rand/distributions/trait.Distribution.html) - stateless, `sample(&self, rng) -> T`
- [futures::Sink trait](https://docs.rs/futures/latest/futures/sink/trait.Sink.html) - async value consumer
- [Rust API Guidelines - Naming](https://rust-lang.github.io/api-guidelines/naming.html) - extension trait `*Ext` convention
- [RFC 0344 - Conventions](https://rust-lang.github.io/rfcs/0344-conventions-galore.html) - iterator naming patterns
