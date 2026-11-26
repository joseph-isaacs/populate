//! Per-leaf cost of columnar collection: each random leaf (and `range`,
//! `constant`, `derive`, `list`) into its columns, `N` rows,
//! one seeded `SmallRng`. The columns are passed to `black_box`, so the
//! result stays observable.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate::Distribution;
use populate::dist::{chars, choose, constant, derive, list, range, text, uniform, weighted};
use rand::SeedableRng;
use rand::rngs::SmallRng;

fn main() {
    divan::main();
}

const N: usize = 1 << 16;

static WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
static MODES: &[&str] = &["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];
static WEIGHTS: &[(i32, u32)] = &[(1, 10), (2, 30), (3, 5), (4, 25), (5, 20), (6, 9), (7, 1)];

fn run<D: Distribution>(b: Bencher, mut d: D) {
    let mut rng = SmallRng::seed_from_u64(7);
    b.counter(ItemsCount::new(N)).bench_local(|| {
        let sink = d.collect(&mut rng, black_box(N));
        black_box(sink);
    });
}

#[divan::bench]
fn uniform_i64(b: Bencher) {
    run(b, uniform(black_box(1i64), black_box(10_494_950)));
}

#[divan::bench]
fn uniform_i32(b: Bencher) {
    run(b, uniform(black_box(1i32), black_box(2_405)));
}

#[divan::bench]
fn uniform_f64(b: Bencher) {
    run(b, uniform(black_box(0.0f64), black_box(1.0)));
}

#[divan::bench]
fn range_i64(b: Bencher) {
    run(b, range(black_box(1i64), black_box(1)));
}

#[divan::bench]
fn constant_i64(b: Bencher) {
    run(b, constant(black_box(5i64)));
}

#[divan::bench]
fn choose_i32(b: Bencher) {
    static V: &[i32] = &[3, 1, 4, 1, 5, 9, 2];
    run(b, choose(V));
}

#[divan::bench]
fn choose_str(b: Bencher) {
    run(b, choose(MODES));
}

#[divan::bench]
fn weighted_i32(b: Bencher) {
    run(b, weighted(WEIGHTS));
}

#[divan::bench]
fn text_words(b: Bencher) {
    run(b, text(WORDS, 2, 6));
}

#[divan::bench]
fn chars_alnum(b: Bencher) {
    run(b, chars(b"abcdefghijklmnopqrstuvwxyz0123456789", 8, 16));
}

#[divan::bench]
fn derive_i64(b: Bencher) {
    run(
        b,
        derive(uniform(black_box(1i64), black_box(50)), |q: &i64| q * 100),
    );
}

#[divan::bench]
fn list_i64(b: Bencher) {
    run(
        b,
        list(
            uniform(1i32, 7),
            uniform(black_box(1i64), black_box(200_000)),
        ),
    );
}
