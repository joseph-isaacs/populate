//! Whole-table operations that can't be expressed row by row: they need to
//! see a complete column (its distinct values, its length) before deciding
//! what to do. All are generic over the column type and the [`Sampler`].
//!
//! | op | R idiom it ports |
//! |----|------------------|
//! | [`unique_in_order`] | `unique(x)` |
//! | [`null_rows_with_sampled_values`] | `set(DT, DT[.(sample(unique(x), k)), on=col, which=TRUE], col, NA)` |
//! | [`null_sampled_rows`] | `set(DT, sample(nrow(DT), k), col, NA)` |
//! | [`covering_sample`] | `sample(c(x, sample(x, size - length(x), TRUE)))` |
//! | [`split_keys`] | `split_xlr`: disjoint matched / left-only / right-only key sets |
//! | [`stable_order_by`] + [`take`] | `setkeyv(DT, cols)` (stable, NA first) |

use crate::sampler::{Sampler, permutation, sample_with_replacement, sample_without_replacement};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::Hash;

/// Distinct values in order of first appearance.
pub fn unique_in_order<T: Hash + Eq + Clone>(col: &[T]) -> Vec<T> {
    let mut seen = HashSet::new();
    col.iter().filter(|v| seen.insert(*v)).cloned().collect()
}

/// `trunc(len * frac)`, the way R's `as.integer(len * (nas / 100))` computes it.
pub fn fraction_of(len: usize, frac: f64) -> usize {
    (len as f64 * frac) as usize
}

/// Null out whole *values*: sample `trunc(distinct * frac)` of the column's
/// distinct values (without replacement) and set every row holding one of
/// them to `None`. Returns the number of distinct values nulled.
///
/// Unlike [`null_sampled_rows`] this removes entire groups/keys, which is how
/// db-benchmark makes NA groups.
pub fn null_rows_with_sampled_values<T: Hash + Eq + Clone>(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    col: &mut [Option<T>],
    frac: f64,
) -> usize {
    let distinct = unique_in_order(col);
    let k = fraction_of(distinct.len(), frac);
    if k == 0 {
        return 0;
    }
    let chosen: HashSet<Option<T>> = sample_without_replacement(sampler, rng, distinct.len(), k)
        .into_iter()
        .map(|i| distinct[i].clone())
        .collect();
    for v in col.iter_mut() {
        if chosen.contains(v) {
            *v = None;
        }
    }
    k
}

/// Set `k` distinct, uniformly chosen rows to `None`.
pub fn null_sampled_rows<T>(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    col: &mut [Option<T>],
    k: usize,
) {
    for i in sample_without_replacement(sampler, rng, col.len(), k) {
        col[i] = None;
    }
}

/// A shuffled column of `size` values drawn from `keys` that contains every
/// key at least once: all keys, topped up with `size - keys.len()` draws with
/// replacement, then permuted (R: `sample_all` in join-datagen.R).
pub fn covering_sample<T: Clone>(
    sampler: impl Sampler,
    rng: &mut impl Rng,
    keys: &[T],
    size: usize,
) -> Vec<T> {
    assert!(keys.len() <= size, "covering_sample: more keys than rows");
    let extra = sample_with_replacement(sampler, rng, keys.len(), size - keys.len());
    let pool: Vec<&T> = keys.iter().chain(extra.iter().map(|&i| &keys[i])).collect();
    permutation(sampler, rng, size)
        .into_iter()
        .map(|i| pool[i].clone())
        .collect()
}

/// Disjoint key sets for a join: `matched` keys appear on both sides,
/// `left_only` / `right_only` on one side each. Keys are a random
/// permutation of `1..=total`.
#[derive(Clone, Debug, PartialEq)]
pub struct KeySplit {
    pub matched: Vec<i32>,
    pub left_only: Vec<i32>,
    pub right_only: Vec<i32>,
}

impl KeySplit {
    /// Keys of the left table: matched followed by left-only.
    pub fn left(&self) -> Vec<i32> {
        [&self.matched[..], &self.left_only[..]].concat()
    }

    /// Keys of the right table: matched followed by right-only.
    pub fn right(&self) -> Vec<i32> {
        [&self.matched[..], &self.right_only[..]].concat()
    }
}

/// Split `n * (1 + unmatched)` shuffled keys into `n * (1 - unmatched)`
/// matched, `n * unmatched` left-only and `n * unmatched` right-only keys
/// (R: `split_xlr(n)` with `unmatched = 0.1`). Float arithmetic and R's `:` /
/// index truncation are reproduced so fractional sizes behave as in R.
pub fn split_keys(sampler: impl Sampler, rng: &mut impl Rng, n: f64, unmatched: f64) -> KeySplit {
    let total = n * (1.0 + unmatched);
    // sample.int(total): a permutation of trunc(total) keys.
    let pool = total as usize;
    let key: Vec<i32> = permutation(sampler, rng, pool)
        .into_iter()
        .map(|i| i as i32 + 1)
        .collect();
    let pick = |from: f64, to: f64| -> Vec<i32> {
        r_colon(from, to)
            .map(|i| key[i as usize - 1])
            .collect::<Vec<_>>()
    };
    let x_end = n * (1.0 - unmatched);
    KeySplit {
        matched: pick(1.0, x_end),
        left_only: pick(x_end + 1.0, n),
        right_only: pick(n + 1.0, total),
    }
}

/// R's `from:to` / `seq.int(from, to)`: `from, from ± 1, ...`, length
/// `trunc(|to - from| + 1 + FLT_EPSILON)`.
fn r_colon(from: f64, to: f64) -> impl Iterator<Item = f64> {
    let len = ((to - from).abs() + 1.0 + f64::from(f32::EPSILON)) as usize;
    let step = if from <= to { 1.0 } else { -1.0 };
    (0..len).map(move |i| from + step * i as f64)
}

// ============================================================================
// Sorting
// ============================================================================

/// A column that can compare two of its rows.
pub trait SortKey {
    fn compare_rows(&self, a: usize, b: usize) -> Ordering;
}

/// `None` sorts first, like data.table's `setkey`.
impl<T: Ord> SortKey for [Option<T>] {
    fn compare_rows(&self, a: usize, b: usize) -> Ordering {
        self[a].cmp(&self[b])
    }
}

impl<T: Ord> SortKey for Vec<Option<T>> {
    fn compare_rows(&self, a: usize, b: usize) -> Ordering {
        self[a].cmp(&self[b])
    }
}

/// Row order sorting by `keys` lexicographically; ties keep their original
/// order (stable).
pub fn stable_order_by(len: usize, keys: &[&dyn SortKey]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    order.sort_by(|&a, &b| {
        keys.iter()
            .map(|k| k.compare_rows(a, b))
            .find(|o| o.is_ne())
            .unwrap_or(Ordering::Equal)
    });
    order
}

/// Reorder `col` by `order` (`out[i] = col[order[i]]`).
pub fn take<T: Clone>(col: &[T], order: &[usize]) -> Vec<T> {
    order.iter().map(|&i| col[i].clone()).collect()
}
