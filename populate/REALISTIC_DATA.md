# Realistic data: today's API vs a proposed one

Eight "interesting data" scenarios, each written twice:

- **Today**: runnable, in [`examples/realistic_today.rs`](examples/realistic_today.rs)
  (`cargo run -p populate --example realistic_today`).
- **Proposed**: the same scenario in the API sketched below. It doesn't
  compile yet; it's what we'd build.

Line counts are user-side code lines (no blanks, no comments), counted
between the `// == scenario` markers.

## Summary

| scenario | what it shows | today LOC | proposed LOC | today: what the reader has to understand |
|---|---|---:|---:|---|
| leaf | any random column | **40** | 0 (built in), 1 for a custom `rand` dist | `Distribution<B>`, `SampleIter`, `DefaultSink`, `Sink`, `collect_rows`, `PhantomData`, why `row_iter` needs `Clone` |
| skewed | Zipf ids, log-normal prices | 11 | 7 | `map` over an `f64` leaf to get an int column |
| correlated | weight depends on height + noise | 9 | 5 | `map` has no RNG, so noise is a hidden input column; the whole thing drops to row mode |
| dependent | receipt = ship + lag; late flag | 6 | 5 | same hidden-input trick |
| conditional | country drawn given region | 14 | 5 | hidden `u ~ U(0,1)` column inverted by hand; no strings, so countries are int codes |
| nulls | 20% null score | **25** | 6 | not expressible; generate a value and a `present` column, then rebuild a nullable Arrow array and schema by hand |
| nested | orders with list<struct<product, qty>> and `n_lines` | 21 (and `n_lines` missing) | 9 | a newtype because `map`'s *input* must be a column type (`u64` isn't); `n_lines` = list length can't be done columnar |
| names | real column names | 10 | 0 | columns are `c0, c1, ...`; rename by rebuilding the schema |
| random walk | stateful column | **43** | 3 | a full leaf type (struct, iterator, `Distribution` impl) |
| **total** | | **179** | **40** | |

The structural code (tuples, `range`, `list`) is already short and reads well.
Nearly all of today's cost is in three places: **there are no random leaves**,
**`map` can't see the RNG**, and **there are no nulls, strings or names**.

## Findings from writing the examples

1. **No random leaves.** `populate-builtins` has `constant`, `range`, `map`,
   `list`. The bench crate already hand-rolls `Uniform`; every user will write
   the same 40-line adapter. Writing a leaf means knowing `Backend`,
   `DefaultSink`, `Sink`, `SampleIter` and `collect_rows`.
2. **`map` is the only way to relate columns, and it's weak.** `Fn(In) -> Out`
   has no RNG, so "x plus noise" or "pick given parent" needs the noise drawn
   as an extra input column and mapped away. A `map` over a tuple also
   collects row by row, so the columnar fast path is lost for exactly the
   columns that make data interesting.
3. **Every intermediate must be a column type.** `Distribution::Item:
   DefaultSink<B>` means `map(sampled(Geometric), |k: u64| ..)` fails to
   compile because `u64` has no sink, even though it's never stored. Only
   `i16, i32, i64, f64, bool, usize` are column types today (no `f32`, `u8`,
   `u32`, `u64`, `i8`).
4. **`row_iter(&mut self) -> owned iterator`** forces leaves to be `Clone`.
   `rand`'s own `.map()` adapter isn't, so it can't be wrapped either.
5. **No `Option<T>`**: no validity bitmaps, so no nulls without
   post-processing. **No strings**: categorical data is int codes.
6. **No names**: every column is `c0, c1, ...`, struct fields too.
7. **Tuples stop at 4**: wider tables need nested tuples
   (`lineitem_numeric` in the bench is `((4), (4), (3))`).
8. **The backend leaks into every signature**: `impl Distribution<ArrowBackend,
   Item = ..>`, or a generic `B` plus sink bounds (see `src/test/dist.rs`).
9. **List lengths are internal.** A sibling column equal to each list's
   length (common: `n_lines`, `n_tags`) can only be made by mapping the
   list in row mode, copying every element through a `Vec`.

## Proposed API

### 1. Random leaves (builtins, no core change)

```rust
uniform(lo, hi)            // inclusive, any SampleUniform
normal(mean, sd)  lognormal(mu, sigma)  exponential(rate)
zipf(n, s)                 // i64 in 1..=n
geometric(p)               // usize
bernoulli(p)               // bool
choose(&["a", "b"])        weighted(&[("a", 0.7), ("b", 0.3)])
walk(start, step_dist)     // running sum: time series, monotone ids with gaps
sample(d)                  // any rand::Distribution<T>
```

Each fills its column with a tight loop in `collect_into`, like `uniform` in
`populate-bench`.

### 2. `derive`: dependent columns that stay columnar

```rust
uniform(D1992, D1998_END).named("ship_day")
    .derive("receipt_day", |ship, rng| ship + rng.random_range(1..=30))
    .derive("late", |(ship, receipt), _| receipt - ship > 14)
```

`a.derive(name, f)` yields `(A, U)`: keep `a`'s columns and add one computed
per row from `a`'s row **and the RNG**. Columnar: fill `a`'s columns first,
then walk those buffers to fill the new one. No hidden noise columns, no row
mode, and a chained `derive` sees all earlier columns. One concept covers the
correlated, dependent and conditional scenarios.

Implementation note: this needs read access to what the parent just wrote (a
"row view" over its sink). Easy for `Vec` sinks; for multi-column parents it
zips the column slices.

### 3. `.list(name, elements)`: list lengths from a column

```rust
geometric(0.3).map(|k| k + 1).named("n_lines")
    .list("lines", (zipf(10_000, 1.1).named("product_id"), uniform(1i64, 10).named("qty")))
```

The parent column *is* the lengths column, so `n_lines` and `lines` agree by
construction and both stay zero-copy. Today's `list(lengths, elems)` stays as
the shorthand when you don't want the lengths kept.

### 4. `.nullable(p)` and `.null_if(f)`

`Item = Option<T>`, with a `DefaultSink` of values buffer + validity bitmap,
mapped straight to an Arrow `NullBuffer`. Values under nulls are still
generated, which keeps the stream deterministic and the loop branch-free.

### 5. `.named("x")`

Names travel with the distribution and become the Arrow field names,
including struct fields inside lists. Unnamed columns keep `c0, c1, ...`.

### 6. Smaller fixes

- Tuples up to 12 (the macros already generate per arity).
- Default the backend, e.g. `trait Distribution<B: Backend = VecBackend>`, so
  user code writes `impl Distribution<Item = ..>`.
- Move `Item: DefaultSink<B>` from the trait to the collect methods, so
  intermediates like `u64` work inside `map`. This is the one structural
  change in `populate-core`.
- More column types: `f32`, `u8..u64`, `i8`, and `&'static str` / `String`
  (Utf8, or Dictionary for `choose`/`weighted`).

## Proposed versions of each scenario

```rust
// skewed (7)
fn skewed() -> impl Distribution<Item = (i64, i64, i64)> {
    (
        range(1i64, 1).named("id"),
        zipf(10_000, 1.1).named("product_id"),
        lognormal(7.0, 1.0).map(|x| x as i64).named("price_cents"),
    )
}

// correlated (5)
fn correlated() -> impl Distribution<Item = (f64, f64)> {
    normal(170.0, 10.0).named("height_cm").derive("weight_kg", |h, rng| {
        0.9 * h - 85.0 + 8.0 * rng.sample::<f64, _>(StandardNormal)
    })
}

// dependent (5)
fn dependent() -> impl Distribution<Item = (i32, i32, bool)> {
    uniform(D1992, D1998_END).named("ship_day")
        .derive("receipt_day", |ship, rng| ship + rng.random_range(1..=30))
        .derive("late", |(ship, receipt), _| receipt - ship > 14)
}

// conditional (5, now with real strings)
const COUNTRIES: [&[&str]; 4] = [&["UK", "FR", /* .. */], &["US", "CA", /* .. */], /* .. */];

fn conditional() -> impl Distribution<Item = (i32, &'static str)> {
    zipf(4, 1.0).map(|r| r - 1).named("region")
        .derive("country", |r, rng| *COUNTRIES[r as usize].choose(rng).unwrap())
}

// nulls (6)
fn nulls() -> impl Distribution<Item = (i64, Option<i64>)> {
    (
        range(1i64, 1).named("id"),
        uniform(0i64, 1_000).nullable(0.2).named("score"),
    )
}

// nested (9, including n_lines)
fn nested() -> impl Distribution<Item = (i64, usize, Vec<(i64, i64)>)> {
    (
        range(1i64, 1).named("order_id"),
        geometric(0.3).map(|k| k + 1).named("n_lines").list(
            "lines",
            (zipf(10_000, 1.1).named("product_id"), uniform(1i64, 10).named("qty")),
        ),
    )
}

// random walk (3)
fn random_walk() -> impl Distribution<Item = (i64, f64)> {
    (range(0i64, 1).named("t"), walk(20.0, normal(0.0, 1.0)).named("reading"))
}
```

## Suggested order

1. Random leaves + `sample()` (builtins only, small).
2. `.named()`, wider tuples, default backend (small).
3. `.derive()` and `.list()` (needs a row view over sinks; medium).
4. `.nullable()` (new sink + Arrow validity; medium).
5. Move `Item: DefaultSink` off the trait (core change; larger, touches every impl).
6. Strings / dictionary columns.

Steps 1 and 2 alone remove the 40-line leaf adapter, the 43-line random walk
and the 10-line rename (179 to about 90 lines); 3 and 4 get it to about 40.

## Open questions

- `derive` closures get values by copy for scalars. For list parents, a slice?
  Or disallow list parents at first?
- Should names live on distributions (`.named`) or in a `table!` macro that
  also removes the tuple-arity limit?
- Should there also be a vectorised form, `f(parent: &[T], rng) -> Vec<U>`?
