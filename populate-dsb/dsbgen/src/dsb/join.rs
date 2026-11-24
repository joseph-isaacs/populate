// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), join.c and scd.c. See NOTICE.md.

//! DSB's `mk_join` for plain and history-keeping dimensions: the key is
//! drawn with exponential skew instead of uniformly, and optionally mapped
//! through a key permutation (`mk_join_permutation`).

use super::{exp_key, map_key_perm};
use crate::config::{Scaling, Table};
use crate::random::RandomNumberStream;
use crate::slowly_changing_dimension_utils::match_surrogate_key;
use crate::types::Date;

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
