//! `join-datagen.R`: the J1 tables of db-benchmark's join task.
//!
//! Keys for three join columns are split into matched / left-only /
//! right-only sets ([`split_keys`]), at three cardinalities: `N/1e6`,
//! `N/1e3` and `N`. The LHS table `x` has `N` rows covering all left keys of
//! each column; the RHS tables `small` (`N/1e6` rows), `medium` (`N/1e3`)
//! and `big` (`N`) cover the right keys. Each table also has the string
//! twins `id4..id6 = "id" + id1..id3` and one value column.

use crate::csv::{Column, Table};
use crate::dist::runif_rounded;
use crate::groupby::pretty_sci;
use crate::ops::{
    KeySplit, SortKey, covering_sample, fraction_of, null_rows_with_sampled_values,
    null_sampled_rows, split_keys, stable_order_by, take,
};
use crate::rng::RRng;
use crate::sampler::{RSampler, Sampler};
use populate_2::traits::{DistributionScalar, VecBackend};
use rand::Rng;

/// Fraction of each key set that only one side has.
const UNMATCHED: f64 = 0.1;

/// Parameters of `Rscript join-datagen.R N 0 nas sort`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JoinConfig {
    /// Rows of the LHS (and of the big RHS); a multiple of 1e6, at least 1e7
    /// (the reference's `stopifnot(N >= 1e7)`: smaller N leaves the small
    /// table's key set with fewer than one unmatched key).
    pub n: usize,
    /// NA percentage in the LHS, 0..=100.
    pub nas: u32,
    pub sort: bool,
    /// Rows of the small RHS (reference: `n / 1e6`).
    pub small: usize,
    /// Rows of the medium RHS (reference: `n / 1e3`).
    pub medium: usize,
}

impl JoinConfig {
    pub fn new(n: usize, nas: u32, sort: bool) -> Self {
        assert!(
            n >= 10_000_000 && n.is_multiple_of(1_000_000),
            "n must be a multiple of 1e6 and at least 1e7"
        );
        Self::scaled(n, n / 1_000_000, n / 1_000, nas, sort)
    }

    /// Like [`new`](Self::new) with explicit small/medium RHS sizes (an
    /// extension: the reference fixes them at `n / 1e6` and `n / 1e3`).
    /// Each size needs at least 10 rows so every key set has an unmatched key.
    pub fn scaled(n: usize, small: usize, medium: usize, nas: u32, sort: bool) -> Self {
        assert!(
            10 <= small && small <= medium && medium <= n,
            "need 10 <= small <= medium <= n"
        );
        assert!(nas <= 100, "nas is a percentage");
        Self {
            n,
            nas,
            sort,
            small,
            medium,
        }
    }

    /// File names of `[x, small, medium, big]`, e.g. `J1_1e7_NA_0_0`, `J1_1e7_1e1_0_0`.
    pub fn names(&self) -> [String; 4] {
        let name = |rhs: &str| {
            format!(
                "J1_{}_{}_{}_{}",
                pretty_sci(self.n),
                rhs,
                self.nas,
                u8::from(self.sort)
            )
        };
        [
            name("NA"),
            name(&pretty_sci(self.small)),
            name(&pretty_sci(self.medium)),
            name(&pretty_sci(self.n)),
        ]
    }
}

/// One join table: up to three integer keys plus a value column.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinTable {
    /// `id1..` (1 to 3 of them).
    pub ids: Vec<Vec<Option<i32>>>,
    /// Name of the value column (`v1` on the LHS, `v2` on the RHS).
    pub value_name: &'static str,
    pub value: Vec<Option<f64>>,
}

impl JoinTable {
    /// Each key column drawn with [`covering_sample`] from its key set.
    fn covering<S: Sampler>(
        sampler: S,
        rng: &mut impl Rng,
        keys: &[Vec<i32>],
        rows: usize,
    ) -> Vec<Vec<Option<i32>>> {
        keys.iter()
            .map(|k| {
                covering_sample(sampler, rng, k, rows)
                    .into_iter()
                    .map(Some)
                    .collect()
            })
            .collect()
    }

    fn sort_by(&mut self, key_cols: &[usize]) {
        let keys: Vec<&dyn SortKey> = key_cols
            .iter()
            .map(|&i| &self.ids[i] as &dyn SortKey)
            .collect();
        let order = stable_order_by(self.ids[0].len(), &keys);
        for col in &mut self.ids {
            *col = take(col, &order);
        }
    }

    fn add_value<S: Sampler>(&mut self, sampler: S, rng: &mut impl Rng, name: &'static str) {
        let rows = self.ids[0].len();
        let v: Vec<f64> = DistributionScalar::<VecBackend>::collect_col(
            &mut runif_rounded(sampler, 0.0, 100.0, 6),
            Vec::new(),
            rng,
            rows,
        );
        self.value_name = name;
        self.value = v.into_iter().map(Some).collect();
    }

    /// Columns in the reference's written order:
    /// `id1.., id4.. (= "id" + id1..), value`.
    pub fn into_table(self) -> Table {
        let mut columns = Vec::new();
        let twins: Vec<Vec<Option<String>>> = self
            .ids
            .iter()
            .map(|c| c.iter().map(|v| v.map(|i| format!("id{i}"))).collect())
            .collect();
        for (i, c) in self.ids.into_iter().enumerate() {
            columns.push((format!("id{}", i + 1), Column::Int(c)));
        }
        for (i, c) in twins.into_iter().enumerate() {
            columns.push((format!("id{}", i + 4), Column::Str(c)));
        }
        columns.push((self.value_name.to_string(), Column::Float(self.value)));
        Table { columns }
    }
}

/// The four J1 tables.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinTables {
    pub x: JoinTable,
    pub small: JoinTable,
    pub medium: JoinTable,
    pub big: JoinTable,
}

impl JoinTables {
    /// Exactly what `Rscript join-datagen.R` produces (`set.seed(108)`).
    pub fn reference(cfg: JoinConfig) -> Self {
        Self::generate(cfg, RSampler, &mut RRng::new(108))
    }

    pub fn generate<S: Sampler>(cfg: JoinConfig, sampler: S, rng: &mut impl Rng) -> Self {
        let n = cfg.n;
        let splits: [KeySplit; 3] =
            [cfg.small, cfg.medium, n].map(|size| split_keys(sampler, rng, size as f64, UNMATCHED));
        let left: Vec<Vec<i32>> = splits.iter().map(KeySplit::left).collect();
        let right: Vec<Vec<i32>> = splits.iter().map(KeySplit::right).collect();
        let empty = |ids| JoinTable {
            ids,
            value_name: "",
            value: Vec::new(),
        };

        // LHS: sort (before values and NAs, as the reference does), values, NAs.
        let mut x = empty(JoinTable::covering(sampler, rng, &left, n));
        if cfg.sort {
            x.sort_by(&[0, 1, 2]);
        }
        x.add_value(sampler, rng, "v1");
        if cfg.nas > 0 {
            let frac = f64::from(cfg.nas) / 100.0;
            for col in &mut x.ids {
                null_rows_with_sampled_values(sampler, rng, col, frac);
            }
            let k = fraction_of(n, frac);
            if k > 0 {
                null_sampled_rows(sampler, rng, &mut x.value, k);
            }
        }

        // RHS: keys, sort by the table's own finest key, values.
        let mut rhs = |rows: usize, n_keys: usize| {
            let mut t = empty(JoinTable::covering(sampler, rng, &right[..n_keys], rows));
            if cfg.sort {
                t.sort_by(&[n_keys - 1]);
            }
            t.add_value(sampler, rng, "v2");
            t
        };
        let small = rhs(cfg.small, 1);
        let medium = rhs(cfg.medium, 2);
        let big = rhs(n, 3);
        Self {
            x,
            small,
            medium,
            big,
        }
    }

    /// `[x, small, medium, big]`.
    pub fn into_tables(self) -> [Table; 4] {
        [
            self.x.into_table(),
            self.small.into_table(),
            self.medium.into_table(),
            self.big.into_table(),
        ]
    }
}
