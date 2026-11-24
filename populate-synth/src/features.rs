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
use crate::kernel::{Pool, Quantiles, Weighted, Zipf, prim_ref, threshold, unit, validity};
use arrow_array::types::{
    Float32Type, Float64Type, Int32Type, Int64Type, TimestampMillisecondType,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use populate_table::Stream;
use std::array::from_fn;
use std::mem::swap;
use std::sync::Arc;

/// 2025-01-01T00:00:00Z.
const DEFAULT_START_MS: i64 = 1_735_689_600_000;
/// Features that feed the label and target.
const SIGNAL: usize = 8;

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

/// The feature table. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Features {
    seed: u64,
    schema: SchemaRef,
    numeric: Vec<Numeric>,
    categorical: Vec<Categorical>,
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
        Self::with_sizes(seed, 32, 8, 0.02)
    }

    /// `numeric` and `categorical` features, a share `missing` of each
    /// numeric feature NULL.
    pub fn with_sizes(seed: u64, numeric: usize, categorical: usize, missing: f64) -> Self {
        let st = |n: &str| Stream::new(seed, n);
        let params = st("features.params");
        let numeric: Vec<Numeric> = (0..numeric)
            .map(|i| {
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
            })
            .collect();
        let cats: Vec<Categorical> = (0..categorical)
            .map(|j| Categorical {
                values: st("features.c").sub(j as u64),
                zipf: Zipf::new(10u64.pow(1 + (j % 5) as u32), 1.1),
            })
            .collect();
        let w = st("features.weights");
        let weight = |i: u64| 2.0 * unit(w.at(i)) - 1.0;
        let c0_card = if categorical > 0 { 10 } else { 0 };
        Features {
            seed,
            schema: schema(numeric.len(), categorical),
            numeric,
            categorical: cats,
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
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
}

fn schema(numeric: usize, categorical: usize) -> SchemaRef {
    let mut f = vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("split", DataType::Utf8View, false),
    ];
    f.extend((0..numeric).map(|i| Field::new(format!("f_{i}"), DataType::Float32, true)));
    f.extend((0..categorical).map(|j| Field::new(format!("c_{j}"), DataType::Int32, false)));
    f.push(Field::new("label", DataType::Int32, false));
    f.push(Field::new("target", DataType::Float64, false));
    Arc::new(Schema::new(f))
}

impl Shape for Features {
    fn name(&self) -> &'static str {
        "features"
    }

    fn is_plain(&self, column: &str) -> bool {
        matches!(column, "id" | "event_time" | "target") || column.starts_with("f_")
    }

    fn is_random(&self, column: &str) -> bool {
        column == "target" || column.starts_with("f_")
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn batch(&self, start: u64, len: usize) -> RecordBatch {
        let rows = || start..start + len as u64;
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        cols.push(prim_ref::<Int64Type>(
            rows().map(|r| r as i64).collect(),
            None,
        ));
        cols.push(prim_ref::<TimestampMillisecondType>(
            rows()
                .map(|r| DEFAULT_START_MS + r as i64 * 100 + (self.s_time.at(r) % 100) as i64)
                .collect(),
            None,
        ));
        cols.push(
            self.splits.column(
                &rows()
                    .map(|r| self.split.sample(self.s_split.at(r)))
                    .collect::<Vec<u32>>(),
                None,
            ),
        );

        // standard-normal draws of the signal features, for label / target
        let mut logit = vec![self.bias; len];
        let mut target = vec![0.0f64; len];
        let mut prev: Vec<f64> = vec![0.0; len];
        let mut z: Vec<f64> = vec![0.0; len];
        for (i, f) in self.numeric.iter().enumerate() {
            for (k, r) in rows().enumerate() {
                z[k] = self.normal.sample(f.values.at(r));
            }
            if f.kind == Kind::Correlated {
                let c = (1.0 - f.rho * f.rho).sqrt();
                for (zk, pk) in z.iter_mut().zip(&prev) {
                    *zk = f.rho * pk + c * *zk;
                }
            }
            let out: Vec<f32> = match f.kind {
                Kind::Normal | Kind::Correlated => {
                    z.iter().map(|&x| (f.loc + f.scale * x) as f32).collect()
                }
                Kind::LogNormal => z.iter().map(|&x| (0.75 * x).exp() as f32).collect(),
                // uniform from the normal draw's own bits keeps one stream
                Kind::Uniform => rows().map(|r| unit(f.values.at(r)) as f32).collect(),
            };
            if i < SIGNAL {
                let (wl, wt) = (self.w_label[i], self.w_target[i]);
                for k in 0..len {
                    logit[k] += wl * z[k];
                    target[k] += wt * z[k];
                }
            }
            cols.push(prim_ref::<Float32Type>(
                out,
                validity(start, len, |r| f.nulls.at(r) >= f.null_t),
            ));
            swap(&mut prev, &mut z);
        }

        for (j, c) in self.categorical.iter().enumerate() {
            let codes: Vec<i32> = rows()
                .map(|r| c.zipf.sample(c.values.at(r)) as i32)
                .collect();
            if j == 0 {
                for (l, &code) in logit.iter_mut().zip(&codes) {
                    *l += self.c0_effect[code as usize];
                }
            }
            cols.push(prim_ref::<Int32Type>(codes, None));
        }

        let label: Vec<i32> = rows()
            .zip(&logit)
            .map(|(r, &l)| (unit(self.s_label.at(r)) * (1.0 + (-l).exp()) < 1.0) as i32)
            .collect();
        cols.push(prim_ref::<Int32Type>(label, None));
        let target: Vec<f64> = rows()
            .zip(&target)
            .map(|(r, &t)| t + self.normal.sample(self.s_target.at(r)))
            .collect();
        cols.push(prim_ref::<Float64Type>(target, None));
        RecordBatch::try_new(self.schema.clone(), cols).expect("columns match schema")
    }
}
