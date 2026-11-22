//! Realistic data with the populate API as it is on `main` today.
//!
//! Each scenario is the code a user has to write themselves, between
//! `// == scenario: <name>` markers so line counts can be compared with the
//! proposed API in `REALISTIC_DATA.md`. Run with:
//!
//! ```sh
//! cargo run -p populate --example realistic_today
//! ```
//!
//! Gaps that can't be expressed at all today are marked `GAP:`.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{Field, Schema};
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{list, map, range};
use populate::traits::{DefaultSink, Distribution, SampleIter, Sink, collect_rows};
use rand::distr::{Distribution as RandDistribution, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Geometric, LogNormal, Normal, Zipf};
use std::marker::PhantomData;
use std::sync::Arc;

type B = ArrowBackend;

// == scenario: leaf (any rand distribution as a column)
// populate ships no random leaf, so every user starts by writing this adapter.

pub struct Sampled<D, T> {
    dist: D,
    _t: PhantomData<T>,
}

pub fn sampled<D: RandDistribution<T>, T>(dist: D) -> Sampled<D, T> {
    Sampled {
        dist,
        _t: PhantomData,
    }
}

pub struct SampledIter<D, T>(D, PhantomData<T>);

impl<D: RandDistribution<T>, T> SampleIter for SampledIter<D, T> {
    type Item = T;
    fn next(&mut self, rng: &mut impl Rng) -> T {
        self.0.sample(rng)
    }
}

impl<D, T> Distribution<B> for Sampled<D, T>
where
    D: RandDistribution<T> + Clone,
    T: DefaultSink<B>,
    <T as DefaultSink<B>>::Sink: Sink<T>,
{
    type Item = T;
    type RowIter = SampledIter<D, T>;

    fn row_iter(&mut self) -> Self::RowIter {
        SampledIter(self.dist.clone(), PhantomData)
    }

    fn collect_into(
        &mut self,
        sink: <T as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <T as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

pub fn uniform<T: rand::distr::uniform::SampleUniform>(lo: T, hi: T) -> Sampled<Uniform<T>, T> {
    sampled(Uniform::new_inclusive(lo, hi).expect("lo <= hi"))
}
// == end

// == scenario: skewed
// product_id: Zipf over 10k products (a few are very popular);
// price_cents: log-normal (long right tail).
pub fn skewed() -> impl Distribution<B, Item = (i64, i64, i64)> {
    (
        range(1i64, 1),
        map(sampled(Zipf::new(10_000.0, 1.1).unwrap()), |x: f64| {
            x as i64
        }),
        map(sampled(LogNormal::new(7.0, 1.0).unwrap()), |x: f64| {
            x as i64
        }),
    )
}
// == end

// == scenario: correlated
// height_cm ~ N(170, 10); weight_kg = 0.9 * height - 85 + N(0, 8).
// Map has no RNG, so the noise is drawn as its own (hidden) input column and
// the pair is mapped to the two output columns. Row-at-a-time from here.
pub fn correlated() -> impl Distribution<B, Item = (f64, f64)> {
    map(
        (
            sampled(Normal::new(170.0, 10.0).unwrap()),
            sampled(Normal::new(0.0, 8.0).unwrap()),
        ),
        |(h, noise): (f64, f64)| (h, 0.9 * h - 85.0 + noise),
    )
}
// == end

// == scenario: dependent
// ship_day uniform over 1992..1998, receipt_day = ship_day + 1..=30,
// late = receipt_day > ship_day + 14. Same trick: lag is a hidden input.
pub fn dependent() -> impl Distribution<B, Item = (i32, i32, bool)> {
    map(
        (uniform(8035i32, 10_591), uniform(1i32, 30)),
        |(ship, lag): (i32, i32)| (ship, ship + lag, lag > 14),
    )
}
// == end

// == scenario: conditional
// region (0..4, skewed toward 0), then country drawn *given* region.
// No RNG in map, so draw a uniform u in [0, 1) and invert by hand.
const COUNTRIES_PER_REGION: [i32; 4] = [12, 5, 3, 8];

pub fn conditional() -> impl Distribution<B, Item = (i32, i32)> {
    map(
        (
            map(sampled(Zipf::new(4.0, 1.0).unwrap()), |x: f64| x as i32 - 1),
            sampled(Uniform::new(0.0f64, 1.0).unwrap()),
        ),
        |(region, u): (i32, f64)| {
            let k = COUNTRIES_PER_REGION[region as usize];
            let first: i32 = COUNTRIES_PER_REGION[..region as usize].iter().sum();
            (region, first + (u * k as f64) as i32)
        },
    )
}
// == end

// == scenario: nulls
// GAP: no Option<T> column, so no Arrow validity. Generate a value column
// and a separate "present" column, then stitch a nullable array by hand.
pub fn nulls_raw() -> impl Distribution<B, Item = (i64, i64, bool)> {
    (
        range(1i64, 1),
        uniform(0i64, 1_000),
        map(sampled(Uniform::new(0.0f64, 1.0).unwrap()), |u: f64| {
            u >= 0.2
        }),
    )
}

pub fn nulls(rng: &mut SmallRng, n: usize) -> RecordBatch {
    let raw = nulls_raw().collect_record_batch(rng, n).unwrap();
    let values = raw.column(1).as_primitive::<Int64Type>();
    let present = raw.column(2).as_boolean();
    let col: Int64Array = values
        .iter()
        .zip(present.iter())
        .map(|(v, p)| if p.unwrap() { v } else { None })
        .collect();
    let schema = Schema::new(vec![
        Field::new("id", arrow_schema::DataType::Int64, false),
        Field::new("score", arrow_schema::DataType::Int64, true),
    ]);
    let cols: Vec<ArrayRef> = vec![raw.column(0).clone(), Arc::new(col)];
    RecordBatch::try_new(Arc::new(schema), cols).unwrap()
}
// == end

// == scenario: nested
// order_id, then a list of line items, each a struct (product_id Zipf,
// qty 1..=10). Lines per order are geometric (mean ~3). Lists of tuples
// become List<Struct<c0, c1>>.
// GAP: an `n_lines` column equal to each list's length can't be written
// columnar; it needs `map(list(..), |v| (v.len(), v))`, which is row mode
// and copies every element through a Vec.
//
// `map(sampled(Geometric), |k: u64| ..)` doesn't compile: Map's *input* must
// itself be a column type, and u64 isn't one. rand's own `.map()` isn't Clone,
// which `row_iter` needs. So: a newtype.
#[derive(Clone, Copy)]
pub struct LineCount(Geometric);

impl RandDistribution<usize> for LineCount {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> usize {
        self.0.sample(rng) as usize + 1
    }
}

pub fn nested() -> impl Distribution<B, Item = (i64, Vec<(i64, i64)>)> {
    (
        range(1i64, 1),
        list(
            sampled(LineCount(Geometric::new(0.3).unwrap())),
            (
                map(sampled(Zipf::new(10_000.0, 1.1).unwrap()), |x: f64| {
                    x as i64
                }),
                uniform(1i64, 10),
            ),
        ),
    )
}
// == end

// == scenario: names
// Columns come out as c0, c1, ...; naming them means rebuilding the schema.
pub fn named(batch: RecordBatch, names: &[&str]) -> RecordBatch {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .zip(names)
        .map(|(f, n)| f.as_ref().clone().with_name(*n))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
}
// == end

// == scenario: random_walk
// A stateful column (sensor reading = previous + N(0, 1)). Needs a new leaf
// type: struct, iterator, and a Distribution impl.
pub struct RandomWalk {
    start: f64,
    step: Normal<f64>,
}

pub struct RandomWalkIter {
    cur: f64,
    step: Normal<f64>,
}

impl SampleIter for RandomWalkIter {
    type Item = f64;
    fn next(&mut self, rng: &mut impl Rng) -> f64 {
        let v = self.cur;
        self.cur += self.step.sample(rng);
        v
    }
}

impl Distribution<B> for RandomWalk {
    type Item = f64;
    type RowIter = RandomWalkIter;

    fn row_iter(&mut self) -> RandomWalkIter {
        RandomWalkIter {
            cur: self.start,
            step: self.step,
        }
    }

    fn collect_into(
        &mut self,
        sink: <f64 as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <f64 as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

pub fn random_walk() -> impl Distribution<B, Item = (i64, f64)> {
    (
        range(0i64, 1),
        RandomWalk {
            start: 20.0,
            step: Normal::new(0.0, 1.0).unwrap(),
        },
    )
}
// == end

fn main() {
    let mut rng = SmallRng::seed_from_u64(42);
    let n = 8;
    let show = |name: &str, b: RecordBatch| {
        println!("== {name}\n{}", pretty_format_batches(&[b]).unwrap());
    };
    show(
        "skewed",
        named(
            skewed().collect_record_batch(&mut rng, n).unwrap(),
            &["id", "product_id", "price_cents"],
        ),
    );
    show(
        "correlated",
        correlated().collect_record_batch(&mut rng, n).unwrap(),
    );
    show(
        "dependent",
        dependent().collect_record_batch(&mut rng, n).unwrap(),
    );
    show(
        "conditional",
        conditional().collect_record_batch(&mut rng, n).unwrap(),
    );
    show("nulls", nulls(&mut rng, n));
    show(
        "nested",
        nested().collect_record_batch(&mut rng, 3).unwrap(),
    );
    show(
        "random_walk",
        random_walk().collect_record_batch(&mut rng, n).unwrap(),
    );
}
