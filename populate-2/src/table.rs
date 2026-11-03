//! Whole-table operations that can't be expressed row by row: they need to
//! see a complete column (its distinct values, its length) before deciding
//! what to do. All are generic over the column type and the [`Sampler`], so
//! with [`RSampler`](crate::sampler::RSampler) they reproduce the R idioms
//! below exactly.
//!
//! | op | R idiom it ports |
//! |----|------------------|
//! | [`unique_in_order`] | `unique(x)` |
//! | [`null_rows_with_sampled_values`] | `set(DT, DT[.(sample(unique(x), k)), on=col, which=TRUE], col, NA)` |
//! | [`null_sampled_rows`] | `set(DT, sample(nrow(DT), k), col, NA)` |
//! | [`covering_sample`] | `sample(c(x, sample(x, size - length(x), TRUE)))` |
//! | [`split_keys`] | `split_xlr`: disjoint matched / left-only / right-only key sets |
//! | [`radix_order_by`] + [`take`] | `setkeyv(DT, cols)` (stable, NA first), linear time |
//! | [`stable_order_by`] | same order by comparison sort; the reference [`radix_order_by`] is tested against |

use crate::sampler::Sampler;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::swap;

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
    let chosen: HashSet<Option<T>> = sampler
        .sample_without_replacement(rng, distinct.len(), k)
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
    for i in sampler.sample_without_replacement(rng, col.len(), k) {
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
    let extra = sampler.sample_with_replacement(rng, keys.len(), size - keys.len());
    let pool: Vec<&T> = keys.iter().chain(extra.iter().map(|&i| &keys[i])).collect();
    sampler
        .permutation(rng, size)
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
    let key: Vec<i32> = sampler
        .permutation(rng, pool)
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

// ============================================================================
// Radix sorting
// ============================================================================

/// A column that can describe its rows as dense ranks: equal values get equal
/// ranks, and rank order is the column's sort order (`None` first).
pub trait RankKey {
    /// Per-row ranks in `0..cardinality`, and that cardinality.
    fn ranks(&self) -> (Vec<u32>, usize);
}

/// `None` is rank 0; values are ranked `1..` in their `Ord` order, so ranks
/// sort exactly as [`SortKey`] compares.
///
/// One hash lookup per row: rows get provisional ids in order of first
/// appearance, then only the distinct values are sorted and the ids remapped.
impl<T: Ord + Hash> RankKey for [Option<T>] {
    fn ranks(&self) -> (Vec<u32>, usize) {
        let mut ids: HashMap<&T, u32, BuildHasherDefault<FxHasher>> = HashMap::default();
        let mut distinct: Vec<&T> = Vec::new();
        let provisional: Vec<u32> = self
            .iter()
            .map(|v| {
                v.as_ref().map_or(0, |x| {
                    *ids.entry(x).or_insert_with(|| {
                        distinct.push(x);
                        distinct.len() as u32
                    })
                })
            })
            .collect();
        // by_value[r] = provisional id (1-based) of the r-th smallest value.
        let mut by_value: Vec<u32> = (1..=distinct.len() as u32).collect();
        by_value.sort_unstable_by(|&a, &b| distinct[a as usize - 1].cmp(distinct[b as usize - 1]));
        let mut rank_of = vec![0u32; distinct.len() + 1];
        for (r, &id) in by_value.iter().enumerate() {
            rank_of[id as usize] = r as u32 + 1;
        }
        let ranks = provisional.iter().map(|&id| rank_of[id as usize]).collect();
        (ranks, distinct.len() + 1)
    }
}

/// FxHash (rustc's hasher): a fast non-cryptographic hasher for the rank
/// table, where SipHash's DoS resistance buys nothing.
#[derive(Default)]
pub struct FxHasher(u64);

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x51_7c_c1_b7_27_22_0a_95;
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let w = u64::from_le_bytes(c.try_into().expect("8 bytes"));
            self.0 = (self.0.rotate_left(5) ^ w).wrapping_mul(K);
        }
        for &b in chunks.remainder() {
            self.0 = (self.0.rotate_left(5) ^ u64::from(b)).wrapping_mul(K);
        }
    }

    fn write_u32(&mut self, i: u32) {
        self.write_u64(u64::from(i));
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

impl<T: Ord + Hash> RankKey for Vec<Option<T>> {
    fn ranks(&self) -> (Vec<u32>, usize) {
        self.as_slice().ranks()
    }
}

/// The same row order as [`stable_order_by`] (lexicographic by `keys`, ties in
/// original order, `None` first) without comparisons: each key is turned into
/// dense ranks and the rows are counting-sorted by one key at a time, last key
/// first (LSD radix sort). Each pass is stable, so earlier keys dominate and
/// full ties keep their original order. `O(keys × (rows + cardinality))`.
pub fn radix_order_by(len: usize, keys: &[&dyn RankKey]) -> Vec<usize> {
    assert!(
        u32::try_from(len).is_ok(),
        "radix_order_by: more than u32::MAX rows"
    );
    // u32 row ids halve the memory traffic of the scattered passes.
    let mut order: Vec<u32> = (0..len as u32).collect();
    let mut next = vec![0u32; len];
    for key in keys.iter().rev() {
        let (ranks, cardinality) = key.ranks();
        assert_eq!(ranks.len(), len, "key column length differs from table");
        if cardinality <= 1 {
            continue;
        }
        // starts[r] = next output slot for rank r.
        let mut starts = vec![0usize; cardinality + 1];
        for &r in &ranks {
            starts[r as usize + 1] += 1;
        }
        for r in 1..=cardinality {
            starts[r] += starts[r - 1];
        }
        for &row in &order {
            let r = ranks[row as usize] as usize;
            next[starts[r]] = row;
            starts[r] += 1;
        }
        swap(&mut order, &mut next);
    }
    order.into_iter().map(|r| r as usize).collect()
}

/// Reorder `col` by `order` (`out[i] = col[order[i]]`).
pub fn take<T: Clone>(col: &[T], order: &[usize]) -> Vec<T> {
    order.iter().map(|&i| col[i].clone()).collect()
}
