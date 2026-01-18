# Base Trait Rename Plan

## Final Decision

| Current | New Name | Rationale |
|---------|----------|-----------|
| `SampleIter` | **`Sampler`** | Noun form, "the thing that samples" |
| `Distribution<T>` | **`ValueStream<T>`** | Emphasizes infinite streaming nature, avoids `rand::Distribution` conflict |
| `Sink<T>` | `Sink<T>` | Keep - aligns with `futures::Sink` |
| `ListSink<T>` | `ListSink<T>` | Keep - consistent with Sink |
| `Collector<D, T>` | **`Materializer<D, T>`** | "Materializes" abstract stream into concrete output |
| `CollectExt<T>` | **`MaterializeExt<T>`** | Follows Materializer rename |

---

## Associated Renames

### Types and Structs

| Current | New |
|---------|-----|
| `Distribution::Iter` | `ValueStream::Sampler` |
| `TupleIter2`, `TupleIter3`, ... | `TupleSampler2`, `TupleSampler3`, ... |
| `TupleUnnest2Iter` | `TupleUnnest2Sampler` |

### Methods

| Current | New |
|---------|-----|
| `Distribution::sample()` | `ValueStream::sampler()` |
| `CollectExt::collect()` | `MaterializeExt::materialize()` |

---

## New Trait Definitions

```rust
/// An infinite sampler that produces values using an RNG.
pub trait Sampler: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// A stream that produces samplers for generating values.
pub trait ValueStream<T> {
    type Sampler: Sampler<Item = T>;
    fn sampler(&mut self) -> Self::Sampler;
}

/// A sink that consumes values and produces an output.
pub trait Sink<T> {
    type Output;
    fn push(&mut self, value: T);
    fn extend(&mut self, iter: impl Iterator<Item = T>);
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

/// Materializes a value stream into a concrete output format.
pub trait Materializer<S, T>: Sized {
    type Output;
    fn materialize(stream: &mut S, rng: &mut impl Rng, n: usize) -> Self::Output;
}

/// Extension trait that adds `materialize` method to all value streams.
pub trait MaterializeExt<T>: ValueStream<T> {
    fn materialize<M>(&mut self, rng: &mut impl Rng, n: usize) -> M::Output
    where
        M: Materializer<Self, T>,
        Self: Sized;
}
```

---

## Analysis of Current Traits vs Rust Ecosystem

### Conflicts Resolved

| Current Name | Conflicted With | Resolution |
|-------------|-----------------|------------|
| `Distribution<T>` | `rand::Distribution<T>` | Renamed to `ValueStream<T>` |
| `SampleIter` | `rand::DistIter` | Renamed to `Sampler` |
| `Collector<D, T>` | `std::iter::FromIterator` | Renamed to `Materializer<S, T>` |

### Good Alignments Kept

| Trait | Aligns With |
|-------|-------------|
| `Sink<T>` | `futures::Sink<T>` - same value consumer concept |

---

## Sources

- [rand::Distribution trait](https://docs.rs/rand/latest/rand/distributions/trait.Distribution.html) - stateless, `sample(&self, rng) -> T`
- [futures::Sink trait](https://docs.rs/futures/latest/futures/sink/trait.Sink.html) - async value consumer
- [Rust API Guidelines - Naming](https://rust-lang.github.io/api-guidelines/naming.html) - extension trait `*Ext` convention
- [RFC 0344 - Conventions](https://rust-lang.github.io/rfcs/0344-conventions-galore.html) - iterator naming patterns
