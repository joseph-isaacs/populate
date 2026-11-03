//! `groupby-datagen.R`: the G1 table of db-benchmark's groupby task.
//!
//! | col | R | values |
//! |-----|---|--------|
//! | id1, id2 | `sample(sprintf("id%03d", 1:K), N, TRUE)` | K string groups |
//! | id3 | `sample(sprintf("id%010d", 1:(N/K)), N, TRUE)` | N/K string groups |
//! | id4, id5 | `sample(K, N, TRUE)` | K int groups |
//! | id6 | `sample(N/K, N, TRUE)` | N/K int groups |
//! | v1, v2 | `sample(5, N, TRUE)`, `sample(15, N, TRUE)` | small ints |
//! | v3 | `round(runif(N, max = 100), 6)` | double |
//!
//! then NA injection (`nas` percent of each id column's distinct values,
//! `nas` percent of v1..v3's rows) and optionally a sort by id1..id6.

use crate::csv::{Column, Table};
use crate::dist::{choose, padded_labels, runif_rounded, sample_int};
use crate::ops::{
    SortKey, fraction_of, null_rows_with_sampled_values, null_sampled_rows, stable_order_by, take,
};
use crate::rng::RRng;
use crate::sampler::{RSampler, Sampler};
use populate_2::hlist::{HCons, HNil};
use populate_2::traits::{Distribution, VecBackend};
use rand::Rng;

/// Parameters of `Rscript groupby-datagen.R N K nas sort`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GroupByConfig {
    /// Rows.
    pub n: usize,
    /// Groups of id1, id2, id4, id5 (id3, id6 have `n / k`).
    pub k: usize,
    /// NA percentage, 0..=100.
    pub nas: u32,
    /// Sort by id1..id6.
    pub sort: bool,
}

impl GroupByConfig {
    pub fn new(n: usize, k: usize, nas: u32, sort: bool) -> Self {
        assert!(nas <= 100, "nas is a percentage");
        Self { n, k, nas, sort }
    }

    /// The reference's file name, e.g. `G1_1e7_1e2_0_0`.
    pub fn name(&self) -> String {
        format!(
            "G1_{}_{}_{}_{}",
            pretty_sci(self.n),
            pretty_sci(self.k),
            self.nas,
            u8::from(self.sort)
        )
    }
}

type Row = ((String, String, String, i32), (i32, i32, i32, i32), f64);

/// The row distribution before NA injection and sorting: nine independent
/// columns, collected column by column.
pub fn groupby_dist<S: Sampler>(
    sampler: S,
    k: usize,
    n_over_k: usize,
) -> impl Distribution<VecBackend, Item = Row> {
    let small = padded_labels("id", 3, k);
    let large = padded_labels("id", 10, n_over_k);
    (
        (
            choose(sampler, small.clone()),
            choose(sampler, small),
            choose(sampler, large),
            sample_int(sampler, k),
        ),
        (
            sample_int(sampler, k),
            sample_int(sampler, n_over_k),
            sample_int(sampler, 5),
            sample_int(sampler, 15),
        ),
        runif_rounded(sampler, 0.0, 100.0, 6),
    )
}

/// The G1 table.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupByTable {
    pub id1: Vec<Option<String>>,
    pub id2: Vec<Option<String>>,
    pub id3: Vec<Option<String>>,
    pub id4: Vec<Option<i32>>,
    pub id5: Vec<Option<i32>>,
    pub id6: Vec<Option<i32>>,
    pub v1: Vec<Option<i32>>,
    pub v2: Vec<Option<i32>>,
    pub v3: Vec<Option<f64>>,
}

fn some<T>(v: Vec<T>) -> Vec<Option<T>> {
    v.into_iter().map(Some).collect()
}

impl GroupByTable {
    /// Exactly what `Rscript groupby-datagen.R` produces (`set.seed(108)`).
    pub fn reference(cfg: GroupByConfig) -> Self {
        Self::generate(cfg, RSampler, &mut RRng::new(108))
    }

    pub fn generate<S: Sampler>(cfg: GroupByConfig, sampler: S, rng: &mut impl Rng) -> Self {
        let n_over_k = cfg.n / cfg.k;
        let sink =
            groupby_dist(sampler, cfg.k, n_over_k).collect_into(Default::default(), rng, cfg.n);
        let HCons(id1, HCons(id2, HCons(id3, HCons(id4, sink)))) = sink;
        let HCons(id5, HCons(id6, HCons(v1, HCons(v2, HCons(v3, HNil))))) = sink;
        let mut t = Self {
            id1: some(id1),
            id2: some(id2),
            id3: some(id3),
            id4: some(id4),
            id5: some(id5),
            id6: some(id6),
            v1: some(v1),
            v2: some(v2),
            v3: some(v3),
        };
        if cfg.nas > 0 {
            t.inject_nas(sampler, rng, f64::from(cfg.nas) / 100.0);
        }
        if cfg.sort {
            t.sort_by_ids();
        }
        t
    }

    fn inject_nas<S: Sampler>(&mut self, sampler: S, rng: &mut impl Rng, frac: f64) {
        null_rows_with_sampled_values(sampler, rng, &mut self.id1, frac);
        null_rows_with_sampled_values(sampler, rng, &mut self.id2, frac);
        null_rows_with_sampled_values(sampler, rng, &mut self.id3, frac);
        null_rows_with_sampled_values(sampler, rng, &mut self.id4, frac);
        null_rows_with_sampled_values(sampler, rng, &mut self.id5, frac);
        null_rows_with_sampled_values(sampler, rng, &mut self.id6, frac);
        let k = fraction_of(self.v1.len(), frac);
        if k > 0 {
            null_sampled_rows(sampler, rng, &mut self.v1, k);
            null_sampled_rows(sampler, rng, &mut self.v2, k);
            // f64 isn't Hash/Ord but null_sampled_rows doesn't need either.
            null_sampled_rows(sampler, rng, &mut self.v3, k);
        }
    }

    fn sort_by_ids(&mut self) {
        let keys: [&dyn SortKey; 6] = [
            &self.id1, &self.id2, &self.id3, &self.id4, &self.id5, &self.id6,
        ];
        let order = stable_order_by(self.id1.len(), &keys);
        self.id1 = take(&self.id1, &order);
        self.id2 = take(&self.id2, &order);
        self.id3 = take(&self.id3, &order);
        self.id4 = take(&self.id4, &order);
        self.id5 = take(&self.id5, &order);
        self.id6 = take(&self.id6, &order);
        self.v1 = take(&self.v1, &order);
        self.v2 = take(&self.v2, &order);
        self.v3 = take(&self.v3, &order);
    }

    pub fn into_table(self) -> Table {
        let cols = [
            ("id1", Column::Str(self.id1)),
            ("id2", Column::Str(self.id2)),
            ("id3", Column::Str(self.id3)),
            ("id4", Column::Int(self.id4)),
            ("id5", Column::Int(self.id5)),
            ("id6", Column::Int(self.id6)),
            ("v1", Column::Int(self.v1)),
            ("v2", Column::Int(self.v2)),
            ("v3", Column::Float(self.v3)),
        ];
        Table {
            columns: cols.into_iter().map(|(n, c)| (n.to_string(), c)).collect(),
        }
    }
}

/// The reference's `pretty_sci`: `10000000` -> `1e7`, `20000000` -> `2e7`.
pub fn pretty_sci(x: usize) -> String {
    let s = x.to_string();
    format!("{}e{}", &s[..1], s.len() - 1)
}
