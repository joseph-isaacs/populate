//! The same scenarios as `realistic_today.rs`, but assuming populate grows
//! every general-purpose combinator we'd want (`derive` is now in
//! builtins). The combinators live in `mod general` at the bottom and
//! aren't counted; only the code between `// == scenario` markers is.
//!
//! What's left after that is what the core traits themselves cost. Run with:
//!
//! ```sh
//! cargo run -p populate --example realistic_combinators
//! ```

use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use general::{
    B, bernoulli, geometric, list_with_len, lognormal, named, normal, uniform, walk, with_nulls,
    zipf,
};
use populate::arrow::DistributionArrow;
use populate::dist::{derive, map, range};
use populate::traits::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::StandardNormal;

// == scenario: skewed
pub const SKEWED: [&str; 3] = ["id", "product_id", "price_cents"];
pub fn skewed() -> impl Distribution<B, Item = (i64, i64, i64)> {
    (
        range(1i64, 1),
        zipf(10_000, 1.1),
        map(lognormal(7.0, 1.0), |x: f64| x as i64),
    )
}
// == end

// == scenario: correlated
pub const CORRELATED: [&str; 2] = ["height_cm", "weight_kg"];
pub fn correlated() -> impl Distribution<B, Item = (f64, f64)> {
    derive(normal(170.0, 10.0), |h, rng| {
        0.9 * h - 85.0 + 8.0 * rng.sample::<f64, _>(StandardNormal)
    })
}
// == end

// == scenario: dependent
pub const DEPENDENT: [&str; 3] = ["ship_day", "receipt_day", "late"];
pub fn dependent() -> impl Distribution<B, Item = (i32, i32, bool)> {
    let receipt = derive(uniform(8035i32, 10_591), |ship, rng| {
        ship + rng.random_range(1..=30)
    });
    derive(receipt, |(ship, receipt), _| receipt - ship > 14)
}
// == end

// == scenario: conditional
// CORE: no string columns, so countries are int codes into a side table.
const FIRST: [i32; 4] = [0, 12, 17, 20];
const COUNT: [i32; 4] = [12, 5, 3, 8];
pub const CONDITIONAL: [&str; 2] = ["region", "country"];
pub fn conditional() -> impl Distribution<B, Item = (i32, i32)> {
    let region = map(zipf(4, 1.0), |r: i64| r as i32 - 1);
    derive(region, |r, rng| {
        FIRST[r as usize] + rng.random_range(0..COUNT[r as usize])
    })
}
// == end

// == scenario: nulls
// CORE: no Option<T> column. Generate a `present` column and post-process
// the batch (top-level columns only; nulls inside lists/structs can't be done).
pub const NULLS: [&str; 2] = ["id", "score"];
pub fn nulls_raw() -> impl Distribution<B, Item = (i64, i64, bool)> {
    (range(1i64, 1), uniform(0i64, 1_000), bernoulli(0.8))
}
pub fn nulls(rng: &mut SmallRng, n: usize) -> RecordBatch {
    with_nulls(nulls_raw().collect_record_batch(rng, n).unwrap(), 1, 2)
}
// == end

// == scenario: nested
// CORE: struct fields inside the list stay c0, c1 (names are post-hoc and
// only reach top-level columns).
pub const NESTED: [&str; 3] = ["order_id", "n_lines", "lines"];
pub type Order = (i64, (usize, Vec<(i64, i64)>));
pub fn nested() -> impl Distribution<B, Item = Order> {
    (
        range(1i64, 1),
        list_with_len(
            map(geometric(0.3), |k: usize| k + 1),
            (zipf(10_000, 1.1), uniform(1i64, 10)),
        ),
    )
}
// == end

// == scenario: random_walk
pub const RANDOM_WALK: [&str; 2] = ["t", "reading"];
pub fn random_walk() -> impl Distribution<B, Item = (i64, f64)> {
    (range(0i64, 1), walk(20.0, normal(0.0, 1.0)))
}
// == end

fn main() {
    let mut rng = SmallRng::seed_from_u64(42);
    let n = 8;
    let show = |name: &str, b: RecordBatch, names: &[&str]| {
        let b = named(b, names);
        println!("== {name}\n{}", pretty_format_batches(&[b]).unwrap());
    };
    let skewed_batch = skewed().collect_record_batch(&mut rng, n).unwrap();
    show("skewed", skewed_batch, &SKEWED);
    let correlated_batch = correlated().collect_record_batch(&mut rng, n).unwrap();
    show("correlated", correlated_batch, &CORRELATED);
    let dependent_batch = dependent().collect_record_batch(&mut rng, n).unwrap();
    show("dependent", dependent_batch, &DEPENDENT);
    let conditional_batch = conditional().collect_record_batch(&mut rng, n).unwrap();
    show("conditional", conditional_batch, &CONDITIONAL);
    show("nulls", nulls(&mut rng, n), &NULLS);
    let nested_batch = nested().collect_record_batch(&mut rng, 3).unwrap();
    show("nested", nested_batch, &NESTED);
    let walk_batch = random_walk().collect_record_batch(&mut rng, n).unwrap();
    show("random_walk", walk_batch, &RANDOM_WALK);
}

/// General-purpose combinators written against today's `populate-core`.
/// None of this is counted.
mod general {
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, make_array};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{Field, Schema};
    use populate::arrow::{ArrowBackend, ArrowOffsets};
    use populate::traits::{
        DefaultSink, Distribution, HCons, HNil, ListCol, SampleIter, Sink, collect_rows,
    };
    use rand::Rng;
    use rand::distr::uniform::SampleUniform;
    use rand::distr::{Bernoulli, Distribution as RandDistribution, Uniform};
    use rand_distr::{Geometric, LogNormal, Normal, Zipf};
    use std::marker::PhantomData;
    use std::sync::Arc;

    pub type B = ArrowBackend;

    // ------------------------------------------------------------------
    // Leaves: any rand distribution, optionally mapped to a column type
    // ------------------------------------------------------------------

    pub struct Sampled<D, T, U, F> {
        dist: D,
        f: F,
        _t: PhantomData<fn(T) -> U>,
    }

    pub fn sampled_map<D, T, U, F>(dist: D, f: F) -> Sampled<D, T, U, F>
    where
        D: RandDistribution<T>,
        F: Fn(T) -> U,
    {
        Sampled {
            dist,
            f,
            _t: PhantomData,
        }
    }

    pub fn sampled<D: RandDistribution<T>, T>(dist: D) -> Sampled<D, T, T, fn(T) -> T> {
        sampled_map(dist, |x| x)
    }

    pub struct SampledIter<D, T, F>(D, F, PhantomData<fn() -> T>);

    impl<D, T, U, F> SampleIter for SampledIter<D, T, F>
    where
        D: RandDistribution<T>,
        F: Fn(T) -> U,
    {
        type Item = U;
        fn next(&mut self, rng: &mut impl Rng) -> U {
            (self.1)(self.0.sample(rng))
        }
    }

    impl<D, T, U, F> Distribution<B> for Sampled<D, T, U, F>
    where
        D: RandDistribution<T> + Clone,
        F: Fn(T) -> U + Clone,
        U: DefaultSink<B>,
        <U as DefaultSink<B>>::Sink: Sink<U>,
    {
        type Item = U;
        type RowIter = SampledIter<D, T, F>;

        fn row_iter(&mut self) -> Self::RowIter {
            SampledIter(self.dist.clone(), self.f.clone(), PhantomData)
        }

        fn collect_into(
            &mut self,
            mut sink: <U as DefaultSink<B>>::Sink,
            rng: &mut impl Rng,
            n: usize,
        ) -> <U as DefaultSink<B>>::Sink {
            sink.reserve(n);
            for _ in 0..n {
                sink.push((self.f)(self.dist.sample(rng)));
            }
            sink
        }
    }

    pub fn uniform<T>(lo: T, hi: T) -> impl Distribution<B, Item = T>
    where
        Uniform<T>: Clone,
        T: SampleUniform + DefaultSink<B>,
        <T as DefaultSink<B>>::Sink: Sink<T>,
    {
        sampled(Uniform::new_inclusive(lo, hi).expect("lo <= hi"))
    }

    pub fn normal(mean: f64, sd: f64) -> impl Distribution<B, Item = f64> {
        sampled(Normal::new(mean, sd).expect("sd >= 0"))
    }

    pub fn lognormal(mu: f64, sigma: f64) -> impl Distribution<B, Item = f64> {
        sampled(LogNormal::new(mu, sigma).expect("sigma >= 0"))
    }

    pub fn zipf(n: u64, s: f64) -> impl Distribution<B, Item = i64> {
        sampled_map(Zipf::new(n as f64, s).expect("n >= 1, s >= 0"), |x: f64| {
            x as i64
        })
    }

    pub fn geometric(p: f64) -> impl Distribution<B, Item = usize> {
        sampled_map(Geometric::new(p).expect("0 < p <= 1"), |k: u64| k as usize)
    }

    pub fn bernoulli(p: f64) -> impl Distribution<B, Item = bool> {
        sampled(Bernoulli::new(p).expect("0 <= p <= 1"))
    }

    // ------------------------------------------------------------------
    // walk: running sum of a step distribution
    // ------------------------------------------------------------------

    pub struct Walk<S> {
        start: f64,
        step: S,
    }

    pub fn walk<S: Distribution<B, Item = f64>>(start: f64, step: S) -> Walk<S> {
        Walk { start, step }
    }

    pub struct WalkIter<I> {
        cur: f64,
        step: I,
    }

    impl<I: SampleIter<Item = f64>> SampleIter for WalkIter<I> {
        type Item = f64;
        fn next(&mut self, rng: &mut impl Rng) -> f64 {
            let v = self.cur;
            self.cur += self.step.next(rng);
            v
        }
    }

    impl<S: Distribution<B, Item = f64>> Distribution<B> for Walk<S> {
        type Item = f64;
        type RowIter = WalkIter<S::RowIter>;

        fn row_iter(&mut self) -> Self::RowIter {
            WalkIter {
                cur: self.start,
                step: self.step.row_iter(),
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

    // ------------------------------------------------------------------
    // list_with_len: (length, list) where the length is also a column.
    // Columnar and zero-copy: lengths go to both buffers in one pass.
    // ------------------------------------------------------------------

    pub struct ListWithLen<L, E> {
        lengths: L,
        elements: E,
    }

    pub fn list_with_len<L, E>(lengths: L, elements: E) -> ListWithLen<L, E> {
        ListWithLen { lengths, elements }
    }

    pub struct ListWithLenIter<L, E> {
        lengths: L,
        elements: E,
    }

    impl<L: SampleIter<Item = usize>, E: SampleIter> SampleIter for ListWithLenIter<L, E> {
        type Item = (usize, Vec<E::Item>);
        fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
            let k = self.lengths.next(rng);
            (k, (0..k).map(|_| self.elements.next(rng)).collect())
        }
    }

    type ElemSink<E> = <<E as Distribution<B>>::Item as DefaultSink<B>>::Sink;
    type LenListSink<E> = HCons<Vec<usize>, HCons<ListCol<ArrowOffsets, ElemSink<E>>, HNil>>;

    impl<L, E> Distribution<B> for ListWithLen<L, E>
    where
        L: Distribution<B, Item = usize>,
        E: Distribution<B>,
        (usize, Vec<E::Item>): DefaultSink<B, Sink = LenListSink<E>>,
    {
        type Item = (usize, Vec<E::Item>);
        type RowIter = ListWithLenIter<L::RowIter, E::RowIter>;

        fn row_iter(&mut self) -> Self::RowIter {
            ListWithLenIter {
                lengths: self.lengths.row_iter(),
                elements: self.elements.row_iter(),
            }
        }

        fn collect_into(
            &mut self,
            sink: LenListSink<E>,
            rng: &mut impl Rng,
            n: usize,
        ) -> LenListSink<E> {
            let HCons(mut lens, HCons(list, HNil)) = sink;
            let ListCol {
                mut offsets,
                values,
                mut len,
            } = list;
            lens.reserve(n);
            offsets.reserve(n);
            let start = len;
            let mut lengths = self.lengths.row_iter();
            for _ in 0..n {
                let k = lengths.next(rng);
                lens.push(k);
                len += k;
                offsets.push(len);
            }
            let values = self.elements.collect_into(values, rng, len - start);
            HCons(
                lens,
                HCons(
                    ListCol {
                        offsets,
                        values,
                        len,
                    },
                    HNil,
                ),
            )
        }
    }

    // ------------------------------------------------------------------
    // Post-processing helpers (the core has no names or validity)
    // ------------------------------------------------------------------

    /// Rename top-level columns.
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

    /// Use boolean column `present` as the validity of column `value`, and
    /// drop `present`.
    pub fn with_nulls(batch: RecordBatch, value: usize, present: usize) -> RecordBatch {
        let mask: &BooleanArray = batch.column(present).as_boolean();
        let nulls = NullBuffer::new(mask.values().clone());
        let data = batch
            .column(value)
            .to_data()
            .into_builder()
            .nulls(Some(nulls));
        let col = make_array(data.build().unwrap());
        let mut fields = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for (i, (f, c)) in batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .enumerate()
        {
            if i == present {
                continue;
            }
            if i == value {
                fields.push(f.as_ref().clone().with_nullable(true));
                cols.push(col.clone());
            } else {
                fields.push(f.as_ref().clone());
                cols.push(c.clone());
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }
}
