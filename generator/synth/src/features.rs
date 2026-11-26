//! An ML feature table: an id, an event time, a train / valid / test
//! split, numeric and categorical features, and a label and a regression
//! target that depend on them.
//!
//! Numeric features `f_0..` are `Float32`, in four kinds by `i % 4`:
//! normal with a per-feature mean and scale, log-normal (skewed, like
//! amounts or counts), correlated with the feature before it (its normal
//! draw is `rho` times the previous feature's normal draw plus fresh noise,
//! `rho` from 0.5 to 0.95; the previous feature is log-normal, so the
//! Pearson correlation of the two columns is about `0.86 rho`), and uniform
//! scores in `[0, 1)`. Each has a share of
//! missing values (NULL). Categoricals `c_0..` are `Int32` codes, Zipf-
//! skewed, with cardinalities 10, 100, 1,000, 10,000, 100,000 in turn.
//!
//! `label` is 0/1, drawn with probability `sigmoid(bias + w . z)` over the
//! first eight features (in standard units) plus an effect of `c_0`;
//! `target` is another linear function of the same eight (without `c_0`)
//! plus noise. Both use a feature's value even where it is NULL, as if the
//! value were measured but lost. So a model can learn something real from
//! the table.

use crate::Shape;
use crate::kernel::{Pool, Quantiles, View, Views, Weighted, Zipf, threshold, unit};
use populate_builtins::{Rows, derive, derive_columns, named, named_all, range, row_numbers};
use populate_core::{Columnar, Distribution, Nullable, Spread, SpreadCols, Timestamp, time_unit};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::Stream;
use std::array::from_fn;
use std::mem::swap;
use std::sync::Arc;

/// 2025-01-01T00:00:00Z.
const DEFAULT_START_MS: i64 = 1_735_689_600_000;
/// Features that feed the label and target.
const SIGNAL: usize = 8;

type Millis = Timestamp<{ time_unit::MILLISECOND }>;

#[derive(Debug, Clone)]
struct Numeric {
    kind: Kind,
    values: Stream,
    nulls: Stream,
    null_t: u64,
    /// Output is `loc + scale * x` (normal and correlated kinds).
    loc: f64,
    scale: f64,
    /// Correlation with the previous feature (correlated kind).
    rho: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Normal,
    LogNormal,
    Correlated,
    Uniform,
}

#[derive(Debug, Clone)]
struct Categorical {
    values: Stream,
    zipf: Zipf,
}

/// The feature table, with `NUMERIC` numeric and `CATEGORICAL` categorical
/// features. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Features<const NUMERIC: usize = 32, const CATEGORICAL: usize = 8> {
    inner: Arc<Inner<NUMERIC, CATEGORICAL>>,
}

#[derive(Debug)]
struct Inner<const NUMERIC: usize, const CATEGORICAL: usize> {
    seed: u64,
    numeric: [Numeric; NUMERIC],
    categorical: [Categorical; CATEGORICAL],
    normal: Quantiles,
    split: Weighted<u32>,
    splits: Pool,
    s_time: Stream,
    s_split: Stream,
    s_label: Stream,
    s_target: Stream,
    bias: f64,
    w_label: [f64; SIGNAL],
    w_target: [f64; SIGNAL],
    /// Label effect of each `c_0` code.
    c0_effect: Vec<f64>,
}

impl Features {
    /// 32 numeric and 8 categorical features, 2% missing values.
    pub fn new(seed: u64) -> Self {
        Self::with_sizes(seed, 0.02)
    }
}

impl<const NUMERIC: usize, const CATEGORICAL: usize> Features<NUMERIC, CATEGORICAL> {
    /// A share `missing` of each numeric feature NULL.
    pub fn with_sizes(seed: u64, missing: f64) -> Self {
        let st = |n: &str| Stream::new(seed, n);
        let params = st("features.params");
        let numeric = from_fn(|i| {
            let b = params.at(i as u64);
            let kind = match i % 4 {
                0 => Kind::Normal,
                1 => Kind::LogNormal,
                2 => Kind::Correlated,
                _ => Kind::Uniform,
            };
            Numeric {
                kind,
                values: st("features.f").sub(i as u64),
                nulls: st("features.f_null").sub(i as u64),
                null_t: threshold(missing),
                loc: 100.0 * (unit(b) - 0.5),
                scale: 0.5 + 20.0 * unit(b << 16),
                rho: 0.5 + 0.45 * unit(b << 32),
            }
        });
        let categorical = from_fn(|j| Categorical {
            values: st("features.c").sub(j as u64),
            zipf: Zipf::new(10u64.pow(1 + (j % 5) as u32), 1.1),
        });
        let w = st("features.weights");
        let weight = |i: u64| 2.0 * unit(w.at(i)) - 1.0;
        let c0_card = if CATEGORICAL > 0 { 10 } else { 0 };
        Features {
            inner: Arc::new(Inner {
                seed,
                numeric,
                categorical,
                normal: Quantiles::normal(0.0, 1.0),
                split: Weighted::new(&[(0, 0.8), (1, 0.1), (2, 0.1)]),
                splits: Pool::new(["train", "valid", "test"]),
                s_time: st("features.time"),
                s_split: st("features.split"),
                s_label: st("features.label"),
                s_target: st("features.target"),
                bias: -0.5,
                w_label: from_fn(|i| weight(i as u64)),
                w_target: from_fn(|i| 3.0 * weight(100 + i as u64)),
                c0_effect: (0..c0_card).map(|k| weight(1000 + k)).collect(),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.inner.seed
    }
}

/// The features, the label and the target of a block of rows.
type Block<const N: usize, const C: usize> = (Spread<Option<f32>, N>, Spread<i32, C>, i32, f64);
type BlockCols<const N: usize, const C: usize> = (
    SpreadCols<Nullable<Vec<f32>>, N>,
    SpreadCols<Vec<i32>, C>,
    Vec<i32>,
    Vec<f64>,
);

impl<const NUMERIC: usize, const CATEGORICAL: usize> Inner<NUMERIC, CATEGORICAL> {
    fn write(&self, rows: &Rows, out: &mut BlockCols<NUMERIC, CATEGORICAL>) {
        let len = rows.len();
        let rows = rows.0.clone();
        let (features, cats, labels, targets) = out;
        // standard-normal draws of the signal features, for label / target
        let mut logit = vec![self.bias; len];
        let mut target = vec![0.0f64; len];
        let mut prev: Vec<f64> = vec![0.0; len];
        let mut z: Vec<f64> = vec![0.0; len];
        for (i, (f, col)) in self.numeric.iter().zip(&mut features.0).enumerate() {
            for (zk, r) in z.iter_mut().zip(rows.clone()) {
                *zk = self.normal.sample(f.values.at(r));
            }
            if f.kind == Kind::Correlated {
                let c = (1.0 - f.rho * f.rho).sqrt();
                for (zk, pk) in z.iter_mut().zip(&prev) {
                    *zk = f.rho * pk + c * *zk;
                }
            }
            match f.kind {
                Kind::Normal | Kind::Correlated => col
                    .values
                    .extend(z.iter().map(|&x| (f.loc + f.scale * x) as f32)),
                Kind::LogNormal => col
                    .values
                    .extend(z.iter().map(|&x| (0.75 * x).exp() as f32)),
                // uniform from the normal draw's own bits keeps one stream
                Kind::Uniform => col
                    .values
                    .extend(rows.clone().map(|r| unit(f.values.at(r)) as f32)),
            }
            if i < SIGNAL {
                let (wl, wt) = (self.w_label[i], self.w_target[i]);
                for k in 0..len {
                    logit[k] += wl * z[k];
                    target[k] += wt * z[k];
                }
            }
            col.valid
                .extend(rows.clone().map(|r| f.nulls.at(r) >= f.null_t));
            swap(&mut prev, &mut z);
        }

        for (j, (c, col)) in self.categorical.iter().zip(&mut cats.0).enumerate() {
            let first = col.len();
            col.extend(rows.clone().map(|r| c.zipf.sample(c.values.at(r)) as i32));
            if j == 0 {
                for (l, &code) in logit.iter_mut().zip(&col[first..]) {
                    *l += self.c0_effect[code as usize];
                }
            }
        }

        labels.extend(
            rows.clone()
                .zip(&logit)
                .map(|(r, &l)| (unit(self.s_label.at(r)) * (1.0 + (-l).exp()) < 1.0) as i32),
        );
        targets.extend(
            rows.zip(&target)
                .map(|(r, &t)| t + self.normal.sample(self.s_target.at(r))),
        );
    }
}

impl<const NUMERIC: usize, const CATEGORICAL: usize> Shape for Features<NUMERIC, CATEGORICAL> {
    fn name(&self) -> &'static str {
        "features"
    }

    fn is_plain(&self, column: &str) -> bool {
        matches!(column, "id" | "event_time" | "target") || column.starts_with("f_")
    }

    fn is_random(&self, column: &str) -> bool {
        column == "target" || column.starts_with("f_")
    }

    fn dist(
        &self,
    ) -> impl Distribution<Item: Columnar<Columns: IntoArrowColumns>>
    + Clone
    + Send
    + Sync
    + use<NUMERIC, CATEGORICAL> {
        let (time, split, block) = (self.inner.clone(), self.inner.clone(), self.inner.clone());
        let mut names: Vec<String> = (0..NUMERIC).map(|i| format!("f_{i}")).collect();
        names.extend((0..CATEGORICAL).map(|j| format!("c_{j}")));
        names.extend(["label".into(), "target".into()]);
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        (
            named("id", range(0i64, 1)),
            named(
                "event_time",
                derive(row_numbers(), move |r: u64| -> Millis {
                    Timestamp(DEFAULT_START_MS + r as i64 * 100 + (time.s_time.at(r) % 100) as i64)
                }),
            ),
            named(
                "split",
                derive_columns::<_, _, View>(row_numbers(), move |rows: &Rows, out: &mut Views| {
                    let idx: Vec<u32> = rows
                        .0
                        .clone()
                        .map(|r| split.split.sample(split.s_split.at(r)))
                        .collect();
                    split.splits.write(&idx, out);
                }),
            ),
            named_all(
                &names,
                derive_columns::<_, _, Block<NUMERIC, CATEGORICAL>>(
                    row_numbers(),
                    move |rows, out| block.write(rows, out),
                ),
            ),
        )
    }
}
