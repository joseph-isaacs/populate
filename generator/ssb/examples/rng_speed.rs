//! Time dbgen stream draws and bounding (the core of every SSB column).
use populate_combinators::lehmer::{bound_all, draws};
use std::time::Instant;

fn main() {
    let n = 1 << 14;
    let reps = 5000;
    let mut v = vec![0u32; n];
    let t = Instant::now();
    for r in 0..reps {
        draws(1_808_217_256, (r * n) as u64, &mut v);
    }
    let a = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for _ in 0..reps {
        bound_all(&mut v, 1, 200_000);
        v.iter_mut().for_each(|x| *x |= 1);
    }
    let b = t.elapsed().as_secs_f64();
    let m = (n * reps) as f64;
    println!(
        "draws {:.2} ns/draw, bound {:.2} ns/value ({})",
        a / m * 1e9,
        b / m * 1e9,
        v[7]
    );
}
