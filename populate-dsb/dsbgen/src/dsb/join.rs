// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), join.c and scd.c. See populate-dsb/NOTICE.md.

//! DSB's `mk_join` for plain and history-keeping dimensions: the key is
//! drawn with exponential skew instead of uniformly, and optionally mapped
//! through a key permutation (`mk_join_permutation`).

use super::{exp_key, hierarchical_key, make_perm, map_key_perm};
use crate::config::{Scaling, Table};
use crate::distribution::DemographicsDistributions as Demo;
use crate::generator::{CatalogSalesGeneratorColumn, CustomerGeneratorColumn, GeneratorColumn};
use crate::random::RandomNumberStream;
use crate::slowly_changing_dimension_utils::match_surrogate_key;
use crate::types::Date;
use std::sync::OnceLock;

/// `mk_join` to a table that keeps no history (and is not one of the
/// specially joined tables). One draw.
pub fn join_key(
    to_table: Table,
    stream: &mut RandomNumberStream,
    scaling: &Scaling,
    perm: Option<&[i64]>,
) -> i64 {
    let rows = scaling.get_row_count(to_table) as i64;
    let key = exp_key(1, rows, stream);
    match perm {
        Some(p) => map_key_perm(key, p),
        None => key,
    }
}

/// `scd_join_permutation`: a join to a history-keeping dimension as of
/// `julian_date`. One draw.
pub fn scd_join_key(
    to_table: Table,
    stream: &mut RandomNumberStream,
    julian_date: i64,
    scaling: &Scaling,
    perm: Option<&[i64]>,
) -> i64 {
    let id_count = scaling.get_id_count(to_table) as i64;
    let mut id = exp_key(1, id_count, stream);
    if let Some(p) = perm {
        id = map_key_perm(id, p);
    }
    let key = match_surrogate_key(id, julian_date, to_table, scaling);
    if julian_date > Date::JULIAN_DATA_END_DATE || key > scaling.get_row_count(to_table) as i64 {
        -1
    } else {
        key
    }
}

/// The permutations of `household_demographics_join` or
/// `customer_demographics_join`: one per coordinate of the demographics
/// cross product, most significant first.
pub struct DemographicsJoin {
    pub sizes: Vec<i32>,
    pub perms: Vec<Vec<i32>>,
    /// Draws the permutations took.
    pub draws: u64,
}

impl DemographicsJoin {
    fn new(sizes: Vec<i32>, stream: &mut RandomNumberStream) -> Self {
        let perms: Vec<Vec<i32>> = sizes.iter().map(|&n| make_perm(n, stream)).collect();
        let draws = sizes.iter().map(|&n| n as u64).sum();
        DemographicsJoin {
            sizes,
            perms,
            draws,
        }
    }

    /// `genrand_hierarchical_key`: one exponential draw per coordinate.
    pub fn key(&self, stream: &mut RandomNumberStream) -> i64 {
        hierarchical_key(&self.sizes, &self.perms, stream)
    }

    /// `household_demographics_join`'s permutations. dsdgen draws them on
    /// the first hdemo join of a run: `customer`'s `C_CURRENT_HDEMO_SK` in
    /// its first row, after `customer`'s own key permutation of the
    /// household_demographics attributes.
    pub fn household() -> &'static DemographicsJoin {
        static JOIN: OnceLock<DemographicsJoin> = OnceLock::new();
        JOIN.get_or_init(|| {
            let c = CustomerGeneratorColumn::CCurrentHdemoSk;
            let mut s = RandomNumberStream::new_with_column(
                c.get_global_column_number(),
                c.get_seeds_per_row(),
            );
            s.advance(HDEMO_ATTRIBUTES);
            DemographicsJoin::new(
                vec![
                    Demo::get_vehicle_count_size() as i32,
                    Demo::get_dep_count_size() as i32,
                    Demo::get_buy_potential_size() as i32,
                    Demo::get_income_band_size() as i32,
                ],
                &mut s,
            )
        })
    }
}

/// Distinct buy potential and dependant count pairs: the attributes of
/// the `household_demographics` map (its size is fixed by the
/// distributions, not the scale).
pub const HDEMO_ATTRIBUTES: u64 = 60;
/// Distinct gender, education and marital status triples.
pub const CDEMO_ATTRIBUTES: u64 = 70;

impl DemographicsJoin {
    /// `customer_demographics_join`'s permutations: drawn on the first
    /// cdemo join of a run, `catalog_sales`' `CS_BILL_CDEMO_SK` in its
    /// first order.
    pub fn customer() -> &'static DemographicsJoin {
        static JOIN: OnceLock<DemographicsJoin> = OnceLock::new();
        JOIN.get_or_init(|| {
            let c = CatalogSalesGeneratorColumn::CsBillCdemoSk;
            let mut s = RandomNumberStream::new_with_column(
                c.get_global_column_number(),
                c.get_seeds_per_row(),
            );
            DemographicsJoin::new(
                vec![
                    CD_MAX_COLLEGE,
                    CD_MAX_EMPLOYED,
                    CD_MAX_CHILDREN,
                    Demo::get_credit_rating_size() as i32,
                    Demo::get_purchase_band_size() as i32,
                    Demo::get_education_size() as i32,
                    Demo::get_marital_status_size() as i32,
                    Demo::get_gender_size() as i32,
                ],
                &mut s,
            )
        })
    }
}

/// `CD_MAX_CHILDREN` and friends (join.c's copies).
const CD_MAX_CHILDREN: i32 = 7;
const CD_MAX_EMPLOYED: i32 = 7;
const CD_MAX_COLLEGE: i32 = 7;
