// ========================================================================
// `skip`: move a distribution on without producing rows
// ========================================================================

use crate::Distribution;
use crate::dist::{
    chars, choose, constant, list, map, range, text, uniform, weighted, with_column,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

/// Further than any default `skip` could get.
const FAR: u64 = u64::MAX;

fn make_constant() -> impl Distribution<Item = i32> {
    constant(3)
}

fn make_fixed_list() -> impl Distribution<Item = Vec<u8>> {
    list(constant(2usize), constant(1u8))
}

fn make_uniform() -> impl Distribution<Item = i32> {
    uniform(4, 4)
}

fn make_uniform_float() -> impl Distribution<Item = f64> {
    uniform(0.5, 0.5)
}

fn make_choose() -> impl Distribution<Item = u16> {
    choose(&[8])
}

fn make_weighted() -> impl Distribution<Item = i8> {
    weighted(&[(6, 1)])
}

fn make_text() -> impl Distribution<Item = String> {
    text(&["word"], 1, 1)
}

fn make_chars() -> impl Distribution<Item = String> {
    chars(b"z", 2, 2)
}

fn make_range() -> impl Distribution<Item = i64> {
    range(10, 3)
}

fn make_float_range() -> impl Distribution<Item = f64> {
    range(0.5, 0.25)
}

fn make_mapped() -> impl Distribution<Item = i64> {
    map(range(0i64, 1), |x| x * x)
}

fn make_tuple() -> impl Distribution<Item = (i64, i32, i64)> {
    (range(0, 1), constant(7), range(100, -2))
}

fn make_with_column() -> impl Distribution<Item = (i64, i64)> {
    with_column(range(0i64, 5), |id: i64, _rng| id + 1)
}

fn make_list() -> impl Distribution<Item = Vec<i32>> {
    list(range(1usize, 1), constant(9))
}

/// Counts in a field of its own and keeps the default `skip`.
struct Counter(u32);

impl Distribution for Counter {
    type Item = u32;

    fn sample(&mut self, _rng: &mut impl Rng) -> u32 {
        self.0 += 1;
        self.0 - 1
    }
}

fn make_counter() -> impl Distribution<Item = u32> {
    Counter(0)
}

fn rng() -> SmallRng {
    SmallRng::seed_from_u64(1)
}

/// Rows `skip..skip + n`, reached by skipping.
fn after_skip<D: Distribution>(mut dist: D, skip: u64, n: usize) -> crate::Columns<D::Item> {
    dist.skip(skip);
    dist.collect(&mut rng(), n)
}

#[test]
fn range_skips_to_its_row() {
    assert_eq!(after_skip(make_range(), 0, 3), [10, 13, 16]);
    assert_eq!(after_skip(make_range(), 4, 3), [22, 25, 28]);
    assert_eq!(
        after_skip(make_range(), 1_000_000_000_000, 1),
        [3_000_000_000_010]
    );
}

#[test]
fn float_range_skips_to_its_row() {
    assert_eq!(after_skip(make_float_range(), 6, 2), [2.0, 2.25]);
}

#[test]
fn skip_then_collect_is_the_tail_of_collect() {
    let all = make_range().collect(&mut rng(), 20);
    assert_eq!(after_skip(make_range(), 12, 8), all[12..]);
}

#[test]
fn map_skips_its_source() {
    assert_eq!(after_skip(make_mapped(), 5, 3), [25, 36, 49]);
}

#[test]
fn tuple_skips_every_column() {
    let (a, b, c) = after_skip(make_tuple(), 10, 2);
    assert_eq!(a, [10, 11]);
    assert_eq!(b, [7, 7]);
    assert_eq!(c, [80, 78]);
}

#[test]
fn with_column_skips_its_parent() {
    let (id, next) = after_skip(make_with_column(), 3, 2);
    assert_eq!(id, [15, 20]);
    assert_eq!(next, [16, 21]);
}

#[test]
fn list_skips_its_lengths() {
    let lists = after_skip(make_list(), 2, 2);
    assert_eq!(lists.offsets(), [0, 3, 7]);
    assert_eq!(lists.values, [9; 7]);
}

#[test]
fn default_skip_samples_and_drops() {
    assert_eq!(after_skip(make_counter(), 5, 3), [5, 6, 7]);
}

/// Every built-in has its own `skip`. One that fell back to the default
/// would sample 2^64 rows here, and this test would never finish.
#[test]
fn built_ins_skip_in_constant_time() {
    assert_eq!(after_skip(make_constant(), FAR, 2), [3, 3]);
    // 10 + 3 * (2^64 - 1) wraps to 7
    assert_eq!(after_skip(make_range(), FAR, 2), [7, 10]);
    assert_eq!(after_skip(make_mapped(), FAR, 1), [1]);
    assert_eq!(after_skip(make_tuple(), FAR, 1).1, [7]);
    assert_eq!(after_skip(make_with_column(), FAR, 1).0, [-5]);
    assert_eq!(after_skip(make_fixed_list(), FAR, 2).values, [1; 4]);
    assert_eq!(after_skip(make_uniform(), FAR, 2), [4, 4]);
    assert_eq!(after_skip(make_uniform_float(), FAR, 1), [0.5]);
    assert_eq!(after_skip(make_choose(), FAR, 2), [8, 8]);
    assert_eq!(after_skip(make_weighted(), FAR, 2), [6, 6]);
    assert_eq!(after_skip(make_text(), FAR, 1).get(0), "word");
    assert_eq!(after_skip(make_chars(), FAR, 1).get(0), "zz");
}
