// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), genrand.c, permute.c and dist.c. See NOTICE.md.

//! DSB's random primitives: exponential ("rank") skew, bucketed ranges,
//! permutations of value domains, and the hierarchical and correlated keys
//! DSB adds to dsdgen.
//!
//! dsdgen draws every column from its own Lehmer stream and burns each
//! stream to a fixed budget at the end of a row. DSB keeps that design, so
//! these functions take the column's [`RandomNumberStream`] exactly like
//! tpcdsgen's own generators.

pub mod address;
pub mod attributes;
pub mod join;

use crate::random::RandomNumberStream;
use crate::types::{Date, Decimal};

/// `DIST_EXP_LAMBDA_LOW`: the skew DSB uses almost everywhere.
pub const LAMBDA_LOW: f64 = 0.1;
/// `DIST_EXP_LAMBDA_HIGH`.
pub const LAMBDA_HIGH: f64 = 0.5;
const X_MIN: f64 = 0.01;
const X_MAX: f64 = 20.0;

/// `DATE_BUCKET_COUNT` and friends (permute.h).
pub const DATE_BUCKET_COUNT: i32 = 50;
pub const DISCOUNT_BUCKET_COUNT: i32 = 20;
pub const WHOLESALE_COST_BUCKET_COUNT: i32 = 20;
pub const MARKUP_BUCKET_COUNT: i32 = 20;

/// `genrand_exp_rank`: a rank in `[min, max]`, exponentially skewed towards
/// `min`. One draw.
pub fn exp_rank(min: i64, max: i64, lambda: f64, stream: &mut RandomNumberStream) -> i64 {
    let prob = stream.next_random_double();
    let cdf_min = 1.0 - (-lambda * X_MIN).exp();
    let cdf_max = 1.0 - (-lambda * X_MAX).exp();
    let cdf = cdf_min + (cdf_max - cdf_min) * prob;
    let x = -(1.0 - cdf).ln() / lambda;
    let ratio = (x - X_MIN) / (X_MAX - X_MIN);
    let ratio = f64_max(0.0, ratio);
    let ratio = f64_min(1.0, ratio);
    min + (0.5 + (max - min) as f64 * ratio) as i64
}

// C's fmax/fmin (no NaNs reach them here).
fn f64_max(a: f64, b: f64) -> f64 {
    if a > b { a } else { b }
}

fn f64_min(a: f64, b: f64) -> f64 {
    if a < b { a } else { b }
}

/// `genrand_integer(.., DIST_EXP_RANK_LOW, min, max, ..)`.
pub fn exp_int(min: i32, max: i32, stream: &mut RandomNumberStream) -> i32 {
    exp_rank(min as i64, max as i64, LAMBDA_LOW, stream) as i32
}

/// `genrand_key(.., DIST_EXP_RANK_LOW, min, max, ..)`.
pub fn exp_key(min: i64, max: i64, stream: &mut RandomNumberStream) -> i64 {
    exp_rank(min, max, LAMBDA_LOW, stream)
}

/// `get_exp_cdf`: the normalised CDF at `rank` of `count`.
pub fn exp_cdf(rank: i64, count: i64, lambda: f64) -> f64 {
    let prob = rank as f64 / count as f64;
    let x = X_MIN + (X_MAX - X_MIN) * prob;
    let cdf = 1.0 - (-lambda * x).exp();
    let cdf_min = 1.0 - (-lambda * X_MIN).exp();
    let cdf_max = 1.0 - (-lambda * X_MAX).exp();
    (cdf - cdf_min) / (cdf_max - cdf_min)
}

/// `get_exp_rank_weight`: the probability of `rank` (1-based) of `count`.
pub fn exp_rank_weight(rank: i64, count: i64, lambda: f64) -> f64 {
    exp_cdf(rank, count, lambda) - exp_cdf(rank - 1, count, lambda)
}

/// `makeBucket`: split `item_count` values into about `estimate` buckets.
/// Returns `(bucket_size, bucket_count)`.
pub fn make_bucket(estimate: i32, item_count: i32) -> (i32, i32) {
    let count = if estimate < item_count { estimate } else { item_count };
    let size = (item_count + count - 1) / count;
    let count = (item_count + size - 1) / size;
    (size, count)
}

/// `makeRangeFromBucket`: the values `[lo, hi]` of bucket `idx` (1-based).
pub fn range_from_bucket(size: i32, idx: i32, min: i32, max: i32) -> (i32, i32) {
    let lo = (idx - 1) * size + min;
    let v_max = idx * size + min;
    (lo, if v_max < max { v_max } else { max })
}

/// `mapPermutation`: 1-based `value` through a 0-based permutation.
#[inline]
pub fn map_perm(value: i32, perm: &[i32]) -> i32 {
    perm[(value - 1) as usize] + 1
}

/// `mapKeyPermutation`.
#[inline]
pub fn map_key_perm(value: i64, perm: &[i64]) -> i64 {
    perm[(value - 1) as usize] + 1
}

/// `makePermutation`: a shuffle of `0..n`, `n` draws.
pub fn make_perm(n: i32, stream: &mut RandomNumberStream) -> Vec<i32> {
    crate::permutations::make_permutation(n as usize, stream)
}

/// `makeKeyPermutation`: a shuffle of `0..n` as keys, `n` draws.
pub fn make_key_perm(n: i64, stream: &mut RandomNumberStream) -> Vec<i64> {
    let mut set: Vec<i64> = (0..n).collect();
    for i in 0..set.len() {
        // genrand_key(DIST_UNIFORM, 0, n - 1): `% (int)(max - min + 1)`.
        let mut r = stream.next_random();
        r %= n as i32 as i64;
        set.swap(i, r as usize);
    }
    set
}

/// An exponential pick of a 1-based index of `count`, mapped through
/// `perm` when given: the index `dist_op` uses for `DIST_EXP_RANK_LOW`
/// (returns the 0-based value index).
pub fn exp_pick_index(count: i32, perm: Option<&[i32]>, stream: &mut RandomNumberStream) -> usize {
    let mut index = exp_int(1, count, stream);
    if let Some(p) = perm {
        index = map_perm(index, p);
    }
    (index - 1) as usize
}

/// An exponential value in `[min, max]` through a bucket permutation:
/// `genrand_integer(EXP, 1, buckets)`, mapped, then uniform in the bucket.
/// Two draws.
pub fn exp_bucketed_int(
    min: i32,
    max: i32,
    bucket: (i32, i32),
    perm: &[i32],
    stream: &mut RandomNumberStream,
) -> i32 {
    let (size, count) = bucket;
    let b = map_perm(exp_int(1, count, stream), perm);
    let (lo, hi) = range_from_bucket(size, b, min, max);
    uniform_int(lo, hi, stream)
}

/// `genrand_integer(DIST_UNIFORM, min, max)`.
#[inline]
pub fn uniform_int(min: i32, max: i32, stream: &mut RandomNumberStream) -> i32 {
    crate::random::RandomValueGenerator::generate_uniform_random_int(min, max, stream)
}

/// `genrand_decimal(EXP_RANK_LOW)` through a bucket permutation
/// (`genrand_decimal_permutation`).
pub fn exp_decimal(
    min: Decimal,
    max: Decimal,
    bucket_count: i32,
    perm: Option<&[i32]>,
    stream: &mut RandomNumberStream,
) -> Decimal {
    let precision = min.get_precision().min(max.get_precision());
    let mut n = exp_int(1, bucket_count, stream);
    if let Some(p) = perm {
        n = map_perm(n, p);
    }
    let number = min.get_number()
        + (max.get_number() - min.get_number()) * (n as i64 - 1) / (bucket_count as i64 - 1);
    Decimal::new(number, precision)
}

/// `get_nday`: days in `month` of `year` (dsdgen's leap rule).
pub fn days_in_month(year: i32, month: i32) -> i32 {
    const DAYS: [[i32; 14]; 2] = [
        [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
        [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
    ];
    let leap = Date::is_leap_year(year) as usize;
    DAYS[leap][month as usize + 1] - DAYS[leap][month as usize]
}

/// Year, month and day permutations for [`exp_date`].
pub struct DatePerms {
    pub year: Vec<i32>,
    pub month: Vec<i32>,
    pub day: Vec<i32>,
}

/// `genrand_date_permutation(EXP_RANK_LOW)`: year, then month and day by
/// rejection, each exponential and permuted. Variable draws (3 or more).
///
/// The year is `min.year + perm(rank)`, so it runs one past `max.year`
/// (DSB's off-by-one, kept).
pub fn exp_date(min: Date, max: Date, perms: &DatePerms, stream: &mut RandomNumberStream) -> Date {
    let range_year = max.get_year() - min.get_year() + 1;
    let year = map_perm(exp_int(1, range_year, stream), &perms.year) + min.get_year();
    let month_min = if year == min.get_year() { min.get_month() } else { 1 };
    let month_max = if year == max.get_year() { max.get_month() } else { 12 };
    let month = loop {
        let m = map_perm(exp_int(1, 12, stream), &perms.month);
        if m >= month_min && m <= month_max {
            break m;
        }
    };
    let day_min = if year == min.get_year() && month == min.get_month() {
        min.get_day()
    } else {
        1
    };
    let day_max = if year == max.get_year() && month == max.get_month() {
        max.get_day()
    } else {
        days_in_month(year, month)
    };
    let day = loop {
        let d = map_perm(exp_int(1, 31, stream), &perms.day);
        if d >= day_min && d <= day_max {
            break d;
        }
    };
    Date::from_julian_days(Date::new(year, month, day).to_julian_days())
}

/// `genrand_hierarchical_key`: a key of a cross-product table drawn one
/// coordinate at a time, each exponential and permuted (`sizes[0]` is the
/// most significant). One draw per coordinate.
pub fn hierarchical_key(sizes: &[i32], perms: &[Vec<i32>], stream: &mut RandomNumberStream) -> i64 {
    let mut key: i64 = 0;
    for i in 0..sizes.len() {
        let v = map_perm(exp_int(1, sizes[i], stream), &perms[i]);
        key += (v - 1) as i64;
        if i + 1 < sizes.len() {
            key *= sizes[i + 1] as i64;
        }
    }
    key
}

/// `genrandCorrelatedKeyDistribution`: an attribute id near
/// `drive_id / drive_count` of `count`, exponential within `radius`.
pub fn correlated_attribute(
    drive_count: i32,
    drive_id: i64,
    count: i32,
    perm: &[i64],
    radius: f64,
    stream: &mut RandomNumberStream,
) -> i64 {
    let ratio = 1.0 * (drive_id + 1) as f64 / drive_count as f64;
    let mut lo = (count as f64 * ratio) as i64;
    if lo < 1 {
        lo = 1;
    }
    let mut hi = (count as f64 * (ratio + radius)) as i64;
    if hi < 1 {
        hi = 1;
    }
    let v = exp_key(lo, hi, stream);
    let v = v % count as i64 + 1;
    map_key_perm(v, perm)
}
