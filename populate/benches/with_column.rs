//! `with_column` collected column by column (`collect_into`) vs the same
//! distribution row by row (`sample` + `Sink::push`), which is what a
//! `map` over a tuple does. Same values either way when the parent and the
//! closure don't draw from the RNG.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate::dist::{map, range, with_column};
use populate::{Columns, Distribution, Sink, in_local};
use rand::rngs::{SmallRng, StdRng};
use rand::{Rng, RngCore, SeedableRng};

fn main() {
    divan::main();
}

const N: usize = 1 << 16;

fn d_pair() -> impl Distribution<Item = (i64, i64)> {
    with_column(range(black_box(0i64), black_box(3)), |x: i64, _| 3 * x + 1)
}

fn d_map_pair() -> impl Distribution<Item = (i64, i64)> {
    map(range(black_box(0i64), black_box(3)), |x: i64| {
        (x, 3 * x + 1)
    })
}

fn d_chain3() -> impl Distribution<Item = (i64, i64, bool)> {
    with_column(
        with_column(range(black_box(0i64), black_box(3)), |x: i64, _| 3 * x + 1),
        |(a, b): (i64, i64), _| (a ^ b) & 1 == 0,
    )
}

fn d_map_chain3() -> impl Distribution<Item = (i64, i64, bool)> {
    map(range(black_box(0i64), black_box(3)), |x: i64| {
        let b = 3 * x + 1;
        (x, b, (x ^ b) & 1 == 0)
    })
}

fn d_noisy() -> impl Distribution<Item = (i64, i64)> {
    with_column(range(black_box(0i64), black_box(3)), |x: i64, rng| {
        x + rng.random_range(0..8)
    })
}

fn columnar<D>(bencher: Bencher, make: fn() -> D)
where
    D: Distribution,
    Columns<D::Item>: Clear,
{
    bencher
        .counter(ItemsCount::new(N))
        .with_inputs(|| (make(), warm(make), StdRng::seed_from_u64(1)))
        .bench_local_refs(|(d, cols, rng)| d.collect_into(cols, rng, black_box(N)));
}

fn rows<D>(bencher: Bencher, make: fn() -> D)
where
    D: Distribution,
    Columns<D::Item>: Clear,
{
    bencher
        .counter(ItemsCount::new(N))
        .with_inputs(|| (make(), warm(make), StdRng::seed_from_u64(1)))
        .bench_local_refs(|(d, cols, rng)| {
            let n = black_box(N);
            cols.reserve(n);
            // the distribution in a local, as a caller that owns it has it
            in_local(d, |d| {
                for _ in 0..n {
                    cols.push(d.sample(rng));
                }
            });
        });
}

/// Columns that already hold (and have touched) capacity for `N` rows, then
/// emptied: we time generation, not allocation and page faults.
fn warm<D>(make: fn() -> D) -> Columns<D::Item>
where
    D: Distribution,
    Columns<D::Item>: Clear,
{
    let mut cols = make().collect(&mut StdRng::seed_from_u64(0), N);
    cols.clear();
    cols
}

/// Empty the columns, keeping their capacity.
trait Clear {
    fn clear(&mut self);
}

type V3 = (Vec<i64>, Vec<i64>, Vec<bool>);

impl Clear for V2 {
    fn clear(&mut self) {
        self.0.clear();
        self.1.clear();
    }
}

impl Clear for V3 {
    fn clear(&mut self) {
        self.0.clear();
        self.1.clear();
        self.2.clear();
    }
}

type V2 = (Vec<i64>, Vec<i64>);

#[divan::bench]
fn pair_derive_columnar(b: Bencher) {
    columnar(b, d_pair);
}

#[divan::bench]
fn pair_derive_rows(b: Bencher) {
    rows(b, d_pair);
}

#[divan::bench]
fn pair_map_tuple(b: Bencher) {
    columnar(b, d_map_pair);
}

#[divan::bench]
fn chain3_derive_columnar(b: Bencher) {
    columnar(b, d_chain3);
}

#[divan::bench]
fn chain3_derive_rows(b: Bencher) {
    rows(b, d_chain3);
}

#[divan::bench]
fn chain3_map_tuple(b: Bencher) {
    columnar(b, d_map_chain3);
}

#[divan::bench]
fn noisy_derive_columnar(b: Bencher) {
    columnar(b, d_noisy);
}

#[divan::bench]
fn noisy_derive_rows(b: Bencher) {
    rows(b, d_noisy);
}

// Hand-written floors for the noisy case: one Vec pair, a plain loop.

#[divan::bench]
fn noisy_handwritten_concrete_rng(b: Bencher) {
    b.counter(ItemsCount::new(N))
        .with_inputs(|| (warm(d_noisy), SmallRng::seed_from_u64(1)))
        .bench_local_refs(|((xs, ys), rng)| {
            let n = black_box(N);
            xs.clear();
            ys.clear();
            xs.extend(0..n as i64);
            ys.extend(xs.iter().map(|x| x + rng.random_range(0..8)));
        });
}

#[divan::bench]
fn noisy_handwritten_dyn_rng(b: Bencher) {
    b.counter(ItemsCount::new(N))
        .with_inputs(|| (warm(d_noisy), SmallRng::seed_from_u64(1)))
        .bench_local_refs(|((xs, ys), rng)| {
            let n = black_box(N);
            let rng: &mut dyn RngCore = black_box(rng);
            xs.clear();
            ys.clear();
            xs.extend(0..n as i64);
            ys.extend(xs.iter().map(|x| x + rng.random_range(0..8)));
        });
}
