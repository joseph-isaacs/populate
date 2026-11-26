//! Run one built-in leaf over 20 x 64Ki rows, for an instruction-count
//! profiler: `valgrind --tool=callgrind leafprof <leaf>`.
use populate::Distribution;
use populate::dist::{chars, choose, constant, derive, list, range, text, uniform, weighted};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::hint::black_box;

static WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
static MODES: &[&str] = &["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];
static WEIGHTS: &[(i32, u32)] = &[(1, 10), (2, 30), (3, 5), (4, 25), (5, 20), (6, 9), (7, 1)];
static SMALL: &[i32] = &[3, 1, 4, 1, 5, 9, 2];

fn run<D: Distribution>(mut d: D) {
    let mut rng = SmallRng::seed_from_u64(7);
    for _ in 0..20 {
        black_box(d.collect(&mut rng, black_box(1 << 16)));
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("uniform_i64") => run(uniform(black_box(1i64), black_box(10_494_950))),
        Some("uniform_i32") => run(uniform(black_box(1i32), black_box(2_405))),
        Some("uniform_f64") => run(uniform(black_box(0.0f64), black_box(1.0))),
        Some("range") => run(range(black_box(1i64), black_box(1))),
        Some("constant") => run(constant(black_box(5i64))),
        Some("choose_i32") => run(choose(SMALL)),
        Some("choose_str") => run(choose(MODES)),
        Some("weighted") => run(weighted(WEIGHTS)),
        Some("text") => run(text(WORDS, 2, 6)),
        Some("chars") => run(chars(b"abcdefghijklmnopqrstuvwxyz0123456789", 8, 16)),
        Some("derive") => run(derive(
            uniform(black_box(1i64), black_box(50)),
            |q: &i64| q * 100,
        )),
        Some("list") => run(list(uniform(1i32, 7), uniform(1i64, 200_000))),
        _ => panic!("leaf?"),
    }
}
