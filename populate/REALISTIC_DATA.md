# Realistic data: today's API vs a proposed one

Eight "interesting data" scenarios, each written three ways:

- **Today, as shipped**: [`examples/realistic_today.rs`](examples/realistic_today.rs).
  Only what's on `main`; the user writes every leaf and helper.
- **Today + general combinators**: [`examples/realistic_combinators.rs`](examples/realistic_combinators.rs).
  Today's traits plus general-purpose combinators: `derive` (now in
  `populate-builtins`), and `uniform`, `normal`, `zipf`, `geometric`,
  `bernoulli`, `walk`, `list_with_len`, `named` / `with_nulls`
  post-processing (in the example's `mod general`). None of these are
  counted.
- **Proposed**: the API sketched below. Doesn't compile yet.

Both examples run: `cargo run -p populate --example realistic_combinators`.

Line counts are scenario code lines only (no blanks, comments, or general
combinators), between the `// == scenario` markers. Column names count on
both sides (a `const NAMES` line today, `.named()` calls proposed).

## Summary

| scenario | as shipped | + combinators | proposed | what's still awkward with combinators |
|---|---:|---:|---:|---|
| skewed (Zipf ids, log-normal prices) | 11 | 8 | 7 | names live apart from the columns |
| correlated (weight from height + noise) | 9 | 6 | 5 | names live apart from the columns |
| dependent (receipt = ship + lag; late) | 6 | 7 | 5 | nested `derive(derive(..))` calls instead of a method chain |
| conditional (country given region) | 14 | 9 | 5 | no strings: countries are int codes + side tables |
| nulls (20% null score) | 25 | 7 | 6 | a `present` column plus batch post-processing; top level only |
| nested (list<struct> + `n_lines`) | 21* | 11 | 9 | struct fields stay `c0, c1`; nested `Item` type needs an alias for clippy |
| random walk | 43 | 4 | 3 | nothing |
| leaf / names (setup) | 50 | 0 | 0 | |
| **total** | **179** | **52** | **40** | |

\* `n_lines` isn't possible as shipped.

**Most of the gap is missing combinators.** With them, today's core gets
to 52 lines vs 40 proposed.

### Dependent columns: `derive` (implemented)

`derive(parent, |row, rng| value)` adds one column computed from the
parent's row and the RNG. It's in `populate-builtins` and is **columnar**:
the parent fills its own columns first, then the new column is computed in
one pass over the buffers the parent just wrote (zipped slice iterators into
`Vec::extend`). The output is flat, so derives chain:

```rust
let receipt = derive(uniform(D1992, D1998), |ship, rng| ship + rng.random_range(1..=30));
derive(receipt, |(ship, receipt), _| receipt - ship > 14) // Item = (i32, i32, bool)
```

Closures need no type annotations. The closure's RNG is a concrete
`DeriveRng` (`SmallRng`) seeded once per `collect_into` from the caller's
RNG, so draws inline instead of costing a `dyn RngCore` call each. Limit for
now: the parent must be up to 3 scalar columns (list parents can't be read
back yet).

A `map` returning nested rows such as `((ship, receipt), late)` still
doesn't compile, because nested row tuples can't be pushed into the flat
column HList. `derive` avoids that by keeping rows flat.

`cargo bench -p populate --bench derive`, 65,536 rows, sinks pre-allocated
(median, one run on a shared machine; compare within a column set):

| case | `derive` columnar | same, row by row | `map` to a tuple | hand-written loop |
|---|---:|---:|---:|---:|
| 2 columns, `b = 3a + 1` | 22.5 µs | 89.0 µs | 88.1 µs | |
| 3 columns, chained | 49.9 µs | 133.0 µs | 129.3 µs | |
| 2 columns, `b = a + rng.random_range(0..8)` | 80.1 µs | 127.7 µs | n/a | 79.9 µs (`SmallRng`), 148.8 µs (`dyn RngCore`) |

`derive` is 2.5 to 4x faster than row by row, and with RNG draws it matches
a hand-written loop. Three things got it there:

1. Reading the parent back as zipped slice iterators into `Vec::extend`
   (a first version that indexed and `push`ed per row was slower than row
   mode).
2. A concrete `DeriveRng` for the closure instead of `&mut dyn RngCore`
   (noisy case 208 µs -> 97 µs).
3. Core: `collect_rows` now feeds `push_n` from a `0..n` iterator, and
   `HCons<S, HNil>` / `VecDeque` forward `push_n`, so `Vec` sinks `extend`
   with a known length instead of a capacity check per `push`. This speeds
   up every leaf, not just `derive` (`collect` bench, 65,536 rows):

   | bench | before | after |
   |---|---:|---:|
   | `scalar/columnar` | 42.6 µs | 9.8 µs |
   | `tuple4/columnar` | 285.5 µs | 55.9 µs |
   | `list_i64/columnar` | 428.7 µs | 246.6 µs |
   | `list_tuple3/columnar` | 1.212 ms | 373.8 µs |
   | `nested/columnar` | 1.016 ms | 583.3 µs |

   Row-mode benches are unchanged (they don't go through `collect_rows`).

### Still needs core changes

1. **No nullable columns.** Needs an `Option<T>` `DefaultSink` (values +
   validity) so nulls work anywhere, including inside lists and structs.
   Post-processing only reaches top-level columns.
2. **No names.** Post-hoc renaming can't reach struct fields inside lists,
   and the names live away from the columns they describe. Fix: names carried
   by the distribution into the Arrow schema.
3. **No strings.** No `String`/`&str` `DefaultSink`, so no Utf8 or Dictionary
   columns.
4. **Every intermediate must be a column type.** `map(.., |k: u64| ..)`
   fails because `u64` has no sink, even when it's never stored. The
   combinators work around it by converting inside the leaf.
5. **Backend in every signature**: `impl Distribution<B, Item = ..>`.

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
2. ~~Columnar, chainable dependent columns~~: done, `derive`.
3. Core: `Option<T>` sinks with Arrow validity (`nullable`).
4. Core: names carried into the schema (`named`), including struct fields.
5. Core: string columns; move `Item: DefaultSink` off the trait.

## Open questions

- `derive` closures get values by copy for scalars. For list parents, a slice?
  Or disallow list parents at first?
- Should names live on distributions (`.named`) or in a `table!` macro that
  also removes the tuple-arity limit?
- Should there also be a vectorised form, `f(parent: &[T], rng) -> Vec<U>`?
