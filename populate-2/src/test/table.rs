use crate::column::{Bitmap, DictCol, DictValue, NullableCol, PushNull, StrCol, TakeRows};
use crate::sampler::{FastSampler, RRng, RSampler, Sampler};
use crate::table::{
    RankKey, SortKey, covering_sample, fraction_of, null_rows_with_sampled_values,
    null_sampled_rows, radix_order_by, split_keys, stable_order_by, take, unique_in_order,
};
use crate::traits::Sink;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

/// set.seed(108); k = split_xlr(10); sample_all(c(k$x, k$l), 15)   (R 4.3.3)
#[test]
fn split_and_covering_sample_match_r() {
    let rng = &mut RRng::new(108);
    let k = split_keys(RSampler, rng, 10.0, 0.1);
    assert_eq!(k.matched, vec![10, 7, 6, 5, 11, 8, 9, 3, 4]);
    assert_eq!(k.left_only, vec![2]);
    assert_eq!(k.right_only, vec![1]);
    let col = covering_sample(RSampler, rng, &k.left(), 15);
    assert_eq!(col, vec![5, 8, 3, 11, 2, 8, 6, 9, 4, 9, 9, 8, 10, 7, 4]);
}

#[test]
fn covering_sample_contains_every_key() {
    let rng = &mut StdRng::seed_from_u64(3);
    let keys: Vec<i32> = (0..40).collect();
    let col = covering_sample(FastSampler, rng, &keys, 100);
    assert_eq!(col.len(), 100);
    assert_eq!(col.iter().collect::<HashSet<_>>().len(), 40);
}

#[test]
fn null_ops() {
    let rng = &mut StdRng::seed_from_u64(4);
    // 10 distinct values, 5 rows each: nulling 20% of values nulls 2 whole groups.
    let mut col: Vec<Option<i32>> = (0..50).map(|i| Some(i % 10)).collect();
    assert_eq!(
        null_rows_with_sampled_values(FastSampler, rng, &mut col, 0.2),
        2
    );
    assert_eq!(col.iter().filter(|v| v.is_none()).count(), 10);
    assert_eq!(unique_in_order(&col).len(), 9); // 8 values + None

    let mut col: Vec<Option<f64>> = vec![Some(1.0); 50];
    null_sampled_rows(FastSampler, rng, &mut col, fraction_of(50, 0.1));
    assert_eq!(col.iter().filter(|v| v.is_none()).count(), 5);
}

#[test]
fn stable_sort_nulls_first() {
    let a = vec![Some(2), None, Some(1), Some(2), Some(1)];
    let b = vec![Some("x"), Some("y"), Some("z"), Some("a"), Some("z")];
    let order = stable_order_by(a.len(), &[&a as &dyn SortKey, &b]);
    assert_eq!(order, vec![1, 2, 4, 3, 0]);
    assert_eq!(
        take(&a, &order),
        vec![None, Some(1), Some(1), Some(2), Some(2)]
    );
}

#[test]
fn radix_sort_nulls_first() {
    let a = vec![Some(2), None, Some(1), Some(2), Some(1)];
    let b = vec![Some("x"), Some("y"), Some("z"), Some("a"), Some("z")];
    let order = radix_order_by(a.len(), &[&a as &dyn RankKey, &b]);
    assert_eq!(order, vec![1, 2, 4, 3, 0]);
}

#[test]
fn ranks_follow_ord_with_none_first() {
    // Strings rank lexicographically ("id10" < "id2"), not by length.
    let col = vec![Some("id2"), None, Some("id10"), Some("id2"), Some("id1")];
    let (ranks, cardinality) = col.ranks();
    assert_eq!(ranks, vec![3, 0, 2, 3, 1]);
    assert_eq!(cardinality, 4);
}

#[test]
fn radix_edge_cases_match_comparison_sort() {
    let empty: Vec<Option<i32>> = vec![];
    assert_eq!(
        radix_order_by(0, &[&empty as &dyn RankKey]),
        Vec::<usize>::new()
    );
    let all_null: Vec<Option<i32>> = vec![None; 5];
    let constant = vec![Some(7); 5];
    let keys_r: [&dyn RankKey; 2] = [&all_null, &constant];
    let keys_c: [&dyn SortKey; 2] = [&all_null, &constant];
    assert_eq!(radix_order_by(5, &keys_r), vec![0, 1, 2, 3, 4]);
    assert_eq!(stable_order_by(5, &keys_c), vec![0, 1, 2, 3, 4]);
}

/// Random table: a few columns of mixed type, cardinality and null rate,
/// with many full-key ties so stability is exercised.
struct RandomTable {
    s: Vec<Option<String>>,
    i: Vec<Option<i32>>,
    t: Vec<Option<i64>>,
}

fn random_table(rng: &mut StdRng) -> RandomTable {
    let len = rng.random_range(0..400);
    let s_card = rng.random_range(1..30);
    let i_card = rng.random_range(1..10);
    let t_card = rng.random_range(1..1000);
    let null_rate = [0.0, 0.05, 0.5, 1.0][rng.random_range(0..4)];
    let mut cell = |card: i64| -> Option<i64> {
        (!rng.random_bool(null_rate)).then(|| rng.random_range(-card..card))
    };
    let mut s = Vec::with_capacity(len);
    let mut i = Vec::with_capacity(len);
    let mut t = Vec::with_capacity(len);
    for _ in 0..len {
        // Unpadded labels so lexicographic order differs from numeric order.
        s.push(cell(s_card).map(|v| format!("id{v}")));
        i.push(cell(i_card).map(|v| v as i32));
        t.push(cell(t_card));
    }
    RandomTable { s, i, t }
}

/// The same key list as radix keys and as comparison keys.
type KeyPair<'a> = (Vec<&'a dyn RankKey>, Vec<&'a dyn SortKey>);

/// The radix sort must give exactly the comparison sort's order (including
/// the order of tied rows) for every key combination, on thousands of tables.
#[test]
fn radix_order_equals_comparison_order() {
    let rng = &mut StdRng::seed_from_u64(2024);
    for _ in 0..2_000 {
        let t = random_table(rng);
        let len = t.s.len();
        let combos: [KeyPair; 5] = [
            (vec![&t.s], vec![&t.s]),
            (vec![&t.i, &t.s], vec![&t.i, &t.s]),
            (vec![&t.s, &t.i, &t.t], vec![&t.s, &t.i, &t.t]),
            (vec![&t.t, &t.i], vec![&t.t, &t.i]),
            (vec![&t.i, &t.i, &t.s], vec![&t.i, &t.i, &t.s]),
        ];
        for (radix_keys, cmp_keys) in combos {
            assert_eq!(
                radix_order_by(len, &radix_keys),
                stable_order_by(len, &cmp_keys)
            );
        }
    }
}

// ============================================================================
// Columnar layouts: same order and same nulls as the Option<T> versions
// ============================================================================

fn nullable<T: Clone, S: Default + Sink<T> + PushNull>(col: &[Option<T>]) -> NullableCol<S> {
    let mut out = NullableCol::<S>::default();
    for v in col {
        out.push(v.clone());
    }
    out
}

/// A dictionary over the column's distinct strings in shuffled order, with
/// one value listed twice (codes of equal values must behave as one value).
fn dict_of(col: &[Option<String>], rng: &mut StdRng) -> NullableCol<DictCol<String>> {
    let mut dict: Vec<String> = unique_in_order(col).into_iter().flatten().collect();
    dict.shuffle(rng);
    if let Some(first) = dict.first().cloned() {
        dict.push(first);
    }
    let dict: Arc<[String]> = dict.into();
    let mut out = NullableCol {
        values: DictCol::new(Arc::clone(&dict)),
        validity: Bitmap::new(),
    };
    for v in col {
        out.push(v.as_ref().map(|s| {
            // pick either code of a duplicated value
            let codes: Vec<usize> = (0..dict.len()).filter(|&c| &dict[c] == s).collect();
            DictValue {
                code: codes[rng.random_range(0..codes.len())] as u32,
                values: Arc::clone(&dict),
            }
        }));
    }
    out
}

fn as_options<T: Clone>(col: &NullableCol<Vec<T>>) -> Vec<Option<T>> {
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.values[i].clone()))
        .collect()
}

fn str_options(col: &NullableCol<StrCol>) -> Vec<Option<String>> {
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.values.get(i).to_string()))
        .collect()
}

fn dict_options(col: &NullableCol<DictCol<String>>) -> Vec<Option<String>> {
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.values.get(i).clone()))
        .collect()
}

/// Radix order over StrCol / DictCol / Vec<int> columns (with validity) equals
/// the comparison order over the equivalent `Option` columns.
#[test]
fn columnar_radix_order_equals_comparison_order() {
    let rng = &mut StdRng::seed_from_u64(7);
    for round in 0..1_000 {
        let t = random_table(rng);
        let len = t.s.len();
        let strs: NullableCol<StrCol> = nullable(&t.s);
        let dict = dict_of(&t.s, rng);
        let ints: NullableCol<Vec<i32>> = nullable(&t.i);
        // every other round, a span too wide for the direct path (hash fallback)
        let scale = if round % 2 == 0 { 1 } else { 1_000_000_007 };
        let wide: Vec<Option<i64>> = t.t.iter().map(|v| v.map(|x| x * scale)).collect();
        let longs: NullableCol<Vec<i64>> = nullable(&wide);
        let combos: [KeyPair; 4] = [
            (vec![&strs, &ints], vec![&t.s, &t.i]),
            (vec![&dict, &longs], vec![&t.s, &wide]),
            (vec![&ints, &dict, &strs], vec![&t.i, &t.s, &t.s]),
            (vec![&longs, &ints, &dict], vec![&wide, &t.i, &t.s]),
        ];
        for (radix_keys, cmp_keys) in combos {
            assert_eq!(
                radix_order_by(len, &radix_keys),
                stable_order_by(len, &cmp_keys)
            );
        }
        // reordering keeps values and validity together
        let order = radix_order_by(len, &[&dict as &dyn RankKey]);
        assert_eq!(dict_options(&dict.take_rows(&order)), take(&t.s, &order));
        assert_eq!(str_options(&strs.take_rows(&order)), take(&t.s, &order));
        assert_eq!(as_options(&ints.take_rows(&order)), take(&t.i, &order));
    }
}

/// The pre-columnar algorithm, kept here as an independent oracle: distinct
/// values (None included) in order of first appearance, sample, null matches.
fn reference_null_values<T: Hash + Eq + Clone>(
    rng: &mut RRng,
    col: &mut [Option<T>],
    frac: f64,
) -> usize {
    let distinct = unique_in_order(col);
    let k = fraction_of(distinct.len(), frac);
    if k == 0 {
        return 0;
    }
    let chosen: HashSet<Option<T>> = RSampler
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

/// R-exact null injection on columnar layouts nulls exactly the rows the
/// `Option`-based algorithm does, and draws the same RNG stream.
#[test]
fn columnar_null_injection_matches_option_reference() {
    let rng = &mut StdRng::seed_from_u64(11);
    for round in 0..500u32 {
        let t = random_table(rng);
        let frac = [0.05, 0.3, 0.5, 1.0][round as usize % 4];
        let seed = round as i32;

        let mut want_s = t.s.clone();
        let mut r = RRng::new(seed);
        let k = reference_null_values(&mut r, &mut want_s, frac);
        let want_next = r.next_u32();

        let mut dict = dict_of(&t.s, rng);
        let mut r = RRng::new(seed);
        assert_eq!(
            null_rows_with_sampled_values(RSampler, &mut r, &mut dict, frac),
            k
        );
        assert_eq!(dict_options(&dict), want_s);
        assert_eq!(r.next_u32(), want_next);

        let mut strs: NullableCol<StrCol> = nullable(&t.s);
        let mut r = RRng::new(seed);
        null_rows_with_sampled_values(RSampler, &mut r, &mut strs, frac);
        assert_eq!(str_options(&strs), want_s);

        let mut want_i = t.i.clone();
        let mut r = RRng::new(seed);
        reference_null_values(&mut r, &mut want_i, frac);
        let mut ints: NullableCol<Vec<i32>> = nullable(&t.i);
        let mut r = RRng::new(seed);
        null_rows_with_sampled_values(RSampler, &mut r, &mut ints, frac);
        assert_eq!(as_options(&ints), want_i);

        // and the generic version on Option columns agrees too
        let mut opt = t.s.clone();
        let mut r = RRng::new(seed);
        null_rows_with_sampled_values(RSampler, &mut r, &mut opt, frac);
        assert_eq!(opt, want_s);

        // row sampling: same rows as on an Option column
        let kk = t.i.len() / 3;
        let mut want_rows = t.i.clone();
        null_sampled_rows(RSampler, &mut RRng::new(seed), &mut want_rows, kk);
        let mut rows: NullableCol<Vec<i32>> = nullable(&t.i);
        null_sampled_rows(RSampler, &mut RRng::new(seed), &mut rows, kk);
        assert_eq!(as_options(&rows), want_rows);
    }
}
