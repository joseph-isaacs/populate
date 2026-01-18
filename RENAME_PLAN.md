# Base Trait Rename Plan

## Final Decisions

| Current | New Name | Status |
|---------|----------|--------|
| `SampleIter` | **`ValueStream`** | ✅ Decided |
| `Distribution<T>` | **TBD** | 🔄 See options below |
| `Sink<T>` | `Sink<T>` | ✅ Keep |
| `ListSink<T>` | `ListSink<T>` | ✅ Keep |
| `Collector<D, T>` | **`Materializer<D, T>`** | ✅ Decided |
| `CollectExt<T>` | **`MaterializeExt<T>`** | ✅ Decided |

---

## Distribution Rename Options

### From Statistics Literature

| Name | Origin | Rationale |
|------|--------|-----------|
| **`Variate<T>`** | Stats: "variate generation" | Precise statistical term for generating random values |
| **`RandVar<T>`** | Stats: "random variable" | Common abbreviation, maps outcomes to values |
| **`Population<T>`** | Stats: sampling theory | We draw samples from a population |
| **`Ensemble<T>`** | Stats: statistical mechanics | Collection of possible states |

### From Programming

| Name | Rationale |
|------|-----------|
| **`Sampler<T>`** | Now available since `SampleIter` → `ValueStream`. "Thing that samples" |
| **`Generator<T>`** | Common programming term for value producer |
| **`Source<T>`** | Simple - "data source" |
| **`Producer<T>`** | Factory-like, produces streams |

### Top Candidates

1. **`Variate<T>`** - statistically precise, pairs well: "a Variate yields a ValueStream"
2. **`Sampler<T>`** - intuitive: "a Sampler produces a ValueStream when you sample it"
3. **`Generator<T>`** - universally understood in programming

---

## Associated Renames

### Types and Structs

| Current | New |
|---------|-----|
| `Distribution::Iter` | `???::ValueStream` |
| `TupleIter2`, `TupleIter3`, ... | `TupleStream2`, `TupleStream3`, ... |
| `TupleUnnest2Iter` | `TupleUnnest2Stream` |

### Methods

| Current | New |
|---------|-----|
| `Distribution::sample()` | `???::stream()` or `???::value_stream()` |
| `CollectExt::collect()` | `MaterializeExt::materialize()` |

---

## New Trait Definitions (Draft)

```rust
/// An infinite stream that produces values using an RNG.
pub trait ValueStream: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// A [variate/sampler/generator] that produces value streams.
pub trait ???<T> {
    type Stream: ValueStream<Item = T>;
    fn stream(&mut self) -> Self::Stream;  // or value_stream()
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

/// Materializes a value stream source into a concrete output format.
pub trait Materializer<S, T>: Sized {
    type Output;
    fn materialize(source: &mut S, rng: &mut impl Rng, n: usize) -> Self::Output;
}
```

---

## Pairing Examples

How each option reads in practice:

**With `Variate`:**
```rust
impl Variate<i32> for MyType {
    type Stream = MyValueStream;
    fn stream(&mut self) -> Self::Stream { ... }
}
// "A Variate yields a ValueStream"
```

**With `Sampler`:**
```rust
impl Sampler<i32> for MyType {
    type Stream = MyValueStream;
    fn stream(&mut self) -> Self::Stream { ... }
}
// "A Sampler produces a ValueStream"
```

**With `Generator`:**
```rust
impl Generator<i32> for MyType {
    type Stream = MyValueStream;
    fn stream(&mut self) -> Self::Stream { ... }
}
// "A Generator creates a ValueStream"
```

---

## Analysis vs Rust Ecosystem

### Conflicts Resolved

| Current | Conflicted With | Resolution |
|---------|-----------------|------------|
| `Distribution<T>` | `rand::Distribution<T>` | Rename to `Variate`/`Sampler`/`Generator` |
| `SampleIter` | `rand::DistIter` | Renamed to `ValueStream` |
| `Collector<D, T>` | `std::iter::FromIterator` | Renamed to `Materializer<S, T>` |

### Alignments Kept

| Trait | Aligns With |
|-------|-------------|
| `Sink<T>` | `futures::Sink<T>` - same value consumer concept |

---

## Sources

- [Probability Theory 101](https://sangeetm.github.io/projects/2019/10/13/Probability-Theory-101-for-Dummies-like-Me/) - stats terminology
- [rand::Distribution](https://docs.rs/rand/latest/rand/distributions/trait.Distribution.html) - stateless `sample(&self, rng) -> T`
- [futures::Sink](https://docs.rs/futures/latest/futures/sink/trait.Sink.html) - async value consumer
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/naming.html) - naming conventions
