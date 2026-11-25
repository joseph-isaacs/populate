//! Columnar (`collect_into`) vs row-by-row (`sample` + `Sink::push`) collection.
//!
//! Measurement hygiene:
//! - Leaf parameters go through `black_box`, so LLVM can't constant-fold a
//!   `range(0, 1)` into a closed form or memset a `constant`. It can still
//!   vectorise a loop over runtime values, and that's a real columnar win.
//! - `n` also goes through `black_box`, so loops aren't specialised to a
//!   known trip count.
//! - The distribution, empty sink and RNG are built in `with_inputs` (not
//!   timed). `bench_local_refs` passes them by `&mut` and drops them *after*
//!   timing, so freeing big Vecs isn't counted. Sinks are moved out with
//!   `mem::take` and written back, so the result stays observable (no DCE).
//! - `bench_local_*` because `ThreadRng` is `!Send`.
//! - Benches use `impl Distribution` helpers (like the tests), so we measure
//!   the trait path users get, monomorphised into this crate.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate::dist::{constant, list, map, range};
use populate::{Distribution, ListCol, Sink, in_local};
use rand::rngs::ThreadRng;

fn main() {
    divan::main();
}

const NS: &[usize] = &[1 << 10, 1 << 16];

type Rows4 = (Vec<i64>, Vec<i32>, Vec<bool>, Vec<f64>);

// ---------------------------------------------------------------------------
// Distributions (black-boxed params)
// ---------------------------------------------------------------------------

fn d_scalar() -> impl Distribution<Item = i64> {
    range(black_box(0i64), black_box(3))
}

fn d_tuple4() -> impl Distribution<Item = (i64, i32, bool, f64)> {
    (
        range(black_box(0i64), black_box(3)),
        range(black_box(0i32), black_box(1)),
        constant(black_box(true)),
        range(black_box(0.0f64), black_box(0.5)),
    )
}

/// lengths cycle 0..16 (mean 7.5 elements per row)
fn d_list() -> impl Distribution<Item = Vec<i64>> {
    list(
        map(range(black_box(0usize), 1), |i: usize| i & 15),
        range(black_box(0i64), black_box(3)),
    )
}

fn d_list_tuple3() -> impl Distribution<Item = Vec<(i64, i32, bool)>> {
    list(
        map(range(black_box(0usize), 1), |i: usize| i & 15),
        (
            range(black_box(0i64), black_box(3)),
            range(black_box(0i32), black_box(1)),
            constant(black_box(true)),
        ),
    )
}

/// 4 inner lists per row, inner lengths cycle 0..8 => ~14 elements per row
fn d_nested() -> impl Distribution<Item = Vec<Vec<i64>>> {
    list(
        constant(black_box(4usize)),
        list(
            map(range(black_box(0usize), 1), |i: usize| i & 7),
            range(black_box(0i64), black_box(3)),
        ),
    )
}

// ---------------------------------------------------------------------------
// Row-mode driver: what you get without the columnar unwinding
// ---------------------------------------------------------------------------

fn collect_rows<D: Distribution, S: Sink<D::Item>>(
    d: &mut D,
    sink: &mut S,
    rng: &mut ThreadRng,
    n: usize,
) {
    // the distribution in a local, as a caller that owns it has it
    in_local(d, |d| {
        for _ in 0..n {
            sink.push(d.sample(rng));
        }
    });
}

// ---------------------------------------------------------------------------
// scalar: one i64 column
// ---------------------------------------------------------------------------

mod scalar {
    use super::*;

    #[divan::bench(args = NS)]
    fn handwritten(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (Vec::<i64>::new(), black_box(0i64), black_box(3i64)))
            .bench_local_refs(|(out, start, step)| {
                let mut v = *start;
                for _ in 0..black_box(n) {
                    out.push(v);
                    v += *step;
                }
            });
    }

    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_scalar(), Vec::<i64>::new(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn row(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_scalar(), Vec::<i64>::new(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }
}

// ---------------------------------------------------------------------------
// tuple4: (i64, i32, bool, f64), i.e. four columns
// ---------------------------------------------------------------------------

mod tuple4 {
    use super::*;

    #[divan::bench(args = NS)]
    fn handwritten(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(Rows4::default)
            .bench_local_refs(|(a, bb, c, dd)| {
                let n = black_box(n);
                let (mut x, s) = (black_box(0i64), black_box(3i64));
                for _ in 0..n {
                    a.push(x);
                    x += s;
                }
                let (mut x, s) = (black_box(0i32), black_box(1i32));
                for _ in 0..n {
                    bb.push(x);
                    x += s;
                }
                let t = black_box(true);
                for _ in 0..n {
                    c.push(t);
                }
                let (mut x, s) = (black_box(0.0f64), black_box(0.5f64));
                for _ in 0..n {
                    dd.push(x);
                    x += s;
                }
            });
    }

    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_tuple4(), Rows4::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn row(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_tuple4(), Rows4::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }
}

// ---------------------------------------------------------------------------
// list<i64>: counter is rows; ~7.5 elements per row
// ---------------------------------------------------------------------------

mod list_i64 {
    use super::*;

    /// offsets + one flat values column, both filled column-at-a-time
    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list(), ListCol::<Vec<i64>>::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    /// row mode into Vec<Vec<i64>>: one heap allocation per row
    #[divan::bench(args = NS)]
    fn row_vec_vec(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list(), Vec::<Vec<i64>>::new(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }

    /// row mode into the columnar sink (the `map -> Vec` fallback path):
    /// same output layout as `columnar`, but builds a temp Vec per row
    #[divan::bench(args = NS)]
    fn row_into_listcol(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list(), ListCol::<Vec<i64>>::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }
}

// ---------------------------------------------------------------------------
// list<(i64, i32, bool)>: struct-of-arrays vs Vec<Vec<tuple>>
// ---------------------------------------------------------------------------

mod list_tuple3 {
    use super::*;

    type Soa = ListCol<(Vec<i64>, Vec<i32>, Vec<bool>)>;

    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list_tuple3(), Soa::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn row_vec_vec(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| {
                (
                    d_list_tuple3(),
                    Vec::<Vec<(i64, i32, bool)>>::new(),
                    rand::rng(),
                )
            })
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn row_into_listcol(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_list_tuple3(), Soa::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }
}

// ---------------------------------------------------------------------------
// list<list<i64>>
// ---------------------------------------------------------------------------

mod nested {
    use super::*;

    type Nested = ListCol<ListCol<Vec<i64>>>;

    #[divan::bench(args = NS)]
    fn columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_nested(), Nested::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn row_vec_vec_vec(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_nested(), Vec::<Vec<Vec<i64>>>::new(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }
}

// ---------------------------------------------------------------------------
// Diagnostics: why is tuple4 row-mode faster than columnar?
// ---------------------------------------------------------------------------

mod diag {
    use super::*;

    fn d_f64() -> impl Distribution<Item = f64> {
        range(black_box(0.0f64), black_box(0.5))
    }

    fn d_int4() -> impl Distribution<Item = (i64, i64, i64, i64)> {
        (
            range(black_box(0i64), black_box(3)),
            range(black_box(0i64), black_box(1)),
            range(black_box(0i64), black_box(2)),
            range(black_box(0i64), black_box(5)),
        )
    }

    type Int4 = (Vec<i64>, Vec<i64>, Vec<i64>, Vec<i64>);

    /// f64 `x += step` is a serial FP-add chain (~4 cycle latency)
    #[divan::bench(args = NS)]
    fn scalar_f64_columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_f64(), Vec::<f64>::new(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    /// all-integer tuple: 1-cycle add chains, no FP latency to hide
    #[divan::bench(args = NS)]
    fn int4_columnar(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_int4(), Int4::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    #[divan::bench(args = NS)]
    fn int4_row(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_int4(), Int4::default(), rand::rng()))
            .bench_local_refs(|(d, out, rng)| collect_rows(d, out, rng, black_box(n)));
    }

    /// sink pre-reserved to n: removes Vec growth, isolates per-push cost
    #[divan::bench(args = NS)]
    fn scalar_columnar_prealloc(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| (d_scalar(), Vec::<i64>::with_capacity(n), rand::rng()))
            .bench_local_refs(|(d, out, rng)| d.collect_into(out, rng, black_box(n)));
    }

    /// upper bound if leaves extended in bulk (vectorisable, no per-push check)
    #[divan::bench(args = NS)]
    fn scalar_extend_bound(b: Bencher, n: usize) {
        b.counter(ItemsCount::new(n))
            .with_inputs(|| {
                (
                    Vec::<i64>::with_capacity(n),
                    black_box(0i64),
                    black_box(3i64),
                )
            })
            .bench_local_refs(|(out, start, step)| {
                let (s, k) = (*start, *step);
                out.extend((0..black_box(n) as i64).map(|i| s + i * k));
            });
    }
}
