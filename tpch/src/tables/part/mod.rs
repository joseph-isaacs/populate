mod constants;

use std::sync::LazyLock;

use constants::{
    COLORS, CONTAINER_SIZE, CONTAINER_TYPE, TYPE_SYLLABLE1, TYPE_SYLLABLE2, TYPE_SYLLABLE3,
};
use populate_builtins::{Combine, JoinExt, MapExt, RangeSampler, combine2, combine3, inc, merge};
use populate_builtins_tpch::{PickNBuffered, PickOneRef, Range, TpcHRange, TpchRng, seeded};
use populate_core::Distribution;

/// Create a TPC-H range distribution. Errors here if T is not supported by TpcHRange.
fn range<T: Clone>(min: T, max: T) -> Range<T, TpcHRange>
where
    TpcHRange: RangeSampler<T>,
{
    Range::with_sampler(min, max)
}

/// Create a TPC-H RNG with the given seed.
fn rng(seed: i64) -> TpchRng {
    TpchRng::new(seed)
}

/// Pick N unique elements. Errors here if T is not Clone.
fn pick_n<'a, T: Clone, const N: usize>(values: &'a [T]) -> PickNBuffered<'a, T, N> {
    PickNBuffered::new(values)
}

/// Pick one element by index. Errors here if values slice is invalid.
fn pick_one<'a, T>(values: &'a [T]) -> PickOneRef<'a, T, Range<i64, TpcHRange>>
where
    TpcHRange: RangeSampler<i64>,
{
    PickOneRef::with_sampler(values)
}

static PART_TYPES: LazyLock<Combine> =
    LazyLock::new(|| combine3(&TYPE_SYLLABLE3, &TYPE_SYLLABLE2, &TYPE_SYLLABLE1, " "));

static PART_CONTAINERS: LazyLock<Combine> =
    LazyLock::new(|| combine2(&CONTAINER_SIZE, &CONTAINER_TYPE, " "));

/// Calculate part retail price from partkey (result in cents)
pub fn calculate_retail_price(partkey: i64) -> i64 {
    90000 + (partkey / 10) % 20001 + (partkey % 1000) * 100
}

/// (p_partkey, p_retailprice, p_name, p_type, p_size, p_container, p_mfgr, p_brand)
pub fn parts() -> impl Distribution<(
    i64,
    i64,
    String,
    &'static String,
    i64,
    &'static String,
    i32,
    i32,
)> {
    merge(
        merge(
            inc(1i64).map(|key| (key, calculate_retail_price(key))), // (key, retail)
            (
                seeded(rng(709314158), pick_n::<_, 5>(&COLORS)).join(" "), // p_name
                seeded(rng(1841581359), pick_one(&PART_TYPES)),            // p_type
                seeded(rng(1193163244), range(1, 51)),                     // p_size
                seeded(rng(727633698), pick_one(&PART_CONTAINERS)),        // p_container
            ),
        ),
        (
            seeded(rng(1), range(1i32, 6)),
            seeded(rng(46831694), range(1i32, 6)),
        )
            .map(|(mfgr, brand_raw)| (mfgr, (mfgr * 10 + brand_raw))), // (mfgr, brand)
    )
}
