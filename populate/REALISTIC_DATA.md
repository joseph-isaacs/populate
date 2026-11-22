# Realistic data: today's API vs a proposed one

Eight "interesting data" scenarios, each written three ways:

- **Today, as shipped**: [`examples/realistic_today.rs`](examples/realistic_today.rs).
  Only what's on `main`; the user writes every leaf and helper.
- **Today + general combinators**: [`examples/realistic_combinators.rs`](examples/realistic_combinators.rs).
  Today's `populate-core` unchanged, plus every general-purpose combinator
  we'd add anyway (`uniform`, `normal`, `zipf`, `geometric`, `bernoulli`,
  `map_rng`, `walk`, `list_with_len`, and `named` / `with_nulls`
  post-processing). Those live in `mod general` and **aren't counted**. All of
  them compile against today's traits.
- **Proposed**: the API sketched below. Doesn't compile yet.

Both examples run: `cargo run -p populate --example realistic_combinators`.

Line counts are scenario code lines only (no blanks, comments, or general
combinators), between the `// == scenario` markers. Column names count on
both sides (a `const NAMES` line today, `.named()` calls proposed).

## Summary

| scenario | as shipped | + combinators | proposed | what's still awkward with combinators |
|---|---:|---:|---:|---|
| skewed (Zipf ids, log-normal prices) | 11 | 8 | 7 | names live apart from the columns |
| correlated (weight from height + noise) | 9 | 9 | 5 | `map_rng` is row-at-a-time; must re-emit the parent in the closure |
| dependent (receipt = ship + lag; late) | 6 | 7 | 5 | can't chain two dependent steps (see below); one closure emits every column |
| conditional (country given region) | 14 | 11 | 5 | no strings: countries are int codes + side tables |
| nulls (20% null score) | 25 | 7 | 6 | a `present` column plus batch post-processing; top level only |
| nested (list<struct> + `n_lines`) | 21* | 11 | 9 | struct fields stay `c0, c1`; nested `Item` type needs an alias for clippy |
| random walk | 43 | 4 | 3 | nothing |
| leaf / names (setup) | 50 | 0 | 0 | |
| **total** | **179** | **57** | **40** | |

\* `n_lines` isn't possible as shipped.

**Most of the gap is missing combinators.** With them,
today's core gets to 57 lines vs 40 proposed, and the remaining 17 lines
aren't the real issue. What combinators **can't** fix, because it's in
`populate-core`:

1. **Dependent columns don't compose.** A `map_rng` that returns
   `((ship, receipt), late)` doesn't compile: a nested row tuple can't be
   pushed into the flat column HList (`HCons<Vec<i32>, HCons<Vec<i32>,
   HCons<Vec<bool>, HNil>>>: Sink<((i32, i32), bool)>` is not satisfied).
   So `derive(..).derive(..)` can't be built; everything dependent goes into
   one closure. Fix: flattening tuple sinks in the core.
2. **Dependent columns are row-at-a-time.** `map_rng` collects with
   `collect_rows`. A columnar `derive` (fill the parent, then walk its
   buffer) needs a way to read back what a sink just wrote. Fix: a row/slice
   view on sinks.
3. **No nullable columns.** Needs an `Option<T>` `DefaultSink` (values +
   validity) so nulls work anywhere, including inside lists and structs.
   Post-processing only reaches top-level columns.
4. **No names.** Post-hoc renaming can't reach struct fields inside lists,
   and the names live away from the columns they describe. Fix: names carried
   by the distribution into the Arrow schema.
5. **No strings.** No `String`/`&str` `DefaultSink`, so no Utf8 or Dictionary
   columns.
6. **Every intermediate must be a column type.** `map(.., |k: u64| ..)`
   fails because `u64` has no sink, even when it's never stored. The
   combinators work around it by converting inside the leaf.
7. **Backend in every signature**: `impl Distribution<B, Item = ..>`.

Ease of understanding: with combinators, today's scenario code reads fine.
The proposed version mostly reads better because names, nulls and dependent
columns sit next to the column they describe (`uniform(..).nullable(0.2).named("score")`)
instead of being split between the distribution, a `const` and a
post-processing call.

The rest of this note is the original analysis of the as-shipped API,
followed by the proposal.

## Findings: the API as shipped

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

1. Ship the general combinators from `realistic_combinators.rs` in
   `populate-builtins` (no core change; they already compile).
2. Core: flattening tuple sinks, so dependent steps compose (`derive` chains).
3. Core: `Option<T>` sinks with Arrow validity (`nullable`).
4. Core: names carried into the schema (`named`), including struct fields.
5. Core: read-back on sinks, so `derive` stays columnar.
6. Core: string columns; move `Item: DefaultSink` off the trait.

## Open questions

- `derive` closures get values by copy for scalars. For list parents, a slice?
  Or disallow list parents at first?
- Should names live on distributions (`.named`) or in a `table!` macro that
  also removes the tuple-arity limit?
- Should there also be a vectorised form, `f(parent: &[T], rng) -> Vec<U>`?
