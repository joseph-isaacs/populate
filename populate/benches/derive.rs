//! `derive` collected column by column (`collect_into`) vs the same
//! distribution row by row (`row_iter` + `Sink::push`), which is what a
//! `map` over a tuple does. Same values either way when the parent and the
//! closure don't draw from the RNG.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate::dist::{derive, map, range};
use populate::traits::{Distribution, DistributionExt, SampleIter, Sink, VecBackend};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::mem::take;

fn main() {
    divan::main();
}

const N: usize = 1 << 16;

fn d_pair() -> impl Distribution<VecBackend, Item = (i64, i64)> {
    derive(range(black_box(0i64), black_box(3)), |x: i64, _| 3 * x + 1)
}

fn d_map_pair() -> impl Distribution<VecBackend, Item = (i64, i64)> {
    map(range(black_box(0i64), black_box(3)), |x: i64| {
        (x, 3 * x + 1)
    })
}

fn d_chain3() -> impl Distribution<VecBackend, Item = (i64, i64, bool)> {
    derive(
        derive(range(black_box(0i64), black_box(3)), |x: i64, _| 3 * x + 1),
        |(a, b): (i64, i64), _| (a ^ b) & 1 == 0,
    )
}

fn d_map_chain3() -> impl Distribution<VecBackend, Item = (i64, i64, bool)> {
    map(range(black_box(0i64), black_box(3)), |x: i64| {
        let b = 3 * x + 1;
        (x, b, (x ^ b) & 1 == 0)
    })
}

fn d_noisy() -> impl Distribution<VecBackend, Item = (i64, i64)> {
    derive(range(black_box(0i64), black_box(3)), |x: i64, rng| {
        x + rng.random_range(0..8)
    })
}

fn columnar<D, S>(bencher: Bencher, make: fn() -> D)
where
    D: Distribution<VecBackend>,
    S: Default,
    D: DistributionExt<VecBackend>,
    D: CollectFlat<S>,
{
    bencher
        .counter(ItemsCount::new(N))
        .with_inputs(|| (make(), warm::<D, S>(make), StdRng::seed_from_u64(1)))
        .bench_local_refs(|(d, sink, rng)| {
            *sink = d.flat(take(sink), rng, black_box(N));
        });
}

fn rows<D, S>(bencher: Bencher, make: fn() -> D)
where
    D: Distribution<VecBackend>,
    S: Default + Sink<D::Item>,
    D: CollectFlat<S>,
{
    bencher
        .counter(ItemsCount::new(N))
        .with_inputs(|| (make(), warm::<D, S>(make), StdRng::seed_from_u64(1)))
        .bench_local_refs(|(d, sink, rng)| {
            let mut it = d.row_iter();
            let n = black_box(N);
            sink.reserve(n);
            for _ in 0..n {
                sink.push(it.next(rng));
            }
        });
}

/// A sink that already holds (and has touched) capacity for `N` rows, then
/// emptied: we time generation, not allocation and page faults.
fn warm<D: CollectFlat<S>, S: Default>(make: fn() -> D) -> S {
    let mut s = make().flat(S::default(), &mut StdRng::seed_from_u64(0), N);
    D::clear(&mut s);
    s
}

/// `collect_flat` with the tuple-of-Vecs sink type spelled once per arity.
trait CollectFlat<S> {
    fn flat(&mut self, sink: S, rng: &mut StdRng, n: usize) -> S;
    fn clear(sink: &mut S);
}

impl<D> CollectFlat<V2> for D
where
    D: Distribution<VecBackend, Item = (i64, i64)>,
{
    fn flat(&mut self, s: V2, rng: &mut StdRng, n: usize) -> V2 {
        self.collect_flat(s, rng, n)
    }
    fn clear(s: &mut V2) {
        s.0.clear();
        s.1.clear();
    }
}

type V3 = (Vec<i64>, Vec<i64>, Vec<bool>);

impl<D> CollectFlat<V3> for D
where
    D: Distribution<VecBackend, Item = (i64, i64, bool)>,
{
    fn flat(&mut self, s: V3, rng: &mut StdRng, n: usize) -> V3 {
        self.collect_flat(s, rng, n)
    }
    fn clear(s: &mut V3) {
        s.0.clear();
        s.1.clear();
        s.2.clear();
    }
}

type V2 = (Vec<i64>, Vec<i64>);

#[divan::bench]
fn pair_derive_columnar(b: Bencher) {
    columnar::<_, V2>(b, d_pair);
}

#[divan::bench]
fn pair_derive_rows(b: Bencher) {
    rows::<_, V2>(b, d_pair);
}

#[divan::bench]
fn pair_map_tuple(b: Bencher) {
    columnar::<_, V2>(b, d_map_pair);
}

#[divan::bench]
fn chain3_derive_columnar(b: Bencher) {
    columnar::<_, V3>(b, d_chain3);
}

#[divan::bench]
fn chain3_derive_rows(b: Bencher) {
    rows::<_, V3>(b, d_chain3);
}

#[divan::bench]
fn chain3_map_tuple(b: Bencher) {
    columnar::<_, V3>(b, d_map_chain3);
}

#[divan::bench]
fn noisy_derive_columnar(b: Bencher) {
    columnar::<_, V2>(b, d_noisy);
}

#[divan::bench]
fn noisy_derive_rows(b: Bencher) {
    rows::<_, V2>(b, d_noisy);
}
