use divan::{Bencher, black_box};
use populate_core::{Distribution, SampleIter};
use rand::SeedableRng;
use rand::rngs::StdRng;
use tpch::tables::part::parts;
use tpchgen::generators::PartGenerator;

fn main() {
    divan::main();
}

const ROWS: usize = 10_000;

#[divan::bench]
fn part_generator(bencher: Bencher) {
    bencher.bench(|| {
        PartGenerator::new(1.0, 1, 1)
            .into_iter()
            .take(black_box(ROWS))
            .for_each(drop)
    });
}

#[divan::bench]
fn parts_distribution(bencher: Bencher) {
    bencher.bench(|| {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = parts();
        let mut iter = dist.sample();
        for _ in 0..black_box(ROWS) {
            black_box(iter.next(&mut rng));
        }
    });
}
