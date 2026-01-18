mod constants;

use std::sync::LazyLock;

use constants::{
    COLORS, CONTAINER_SIZE, CONTAINER_TYPE, TYPE_SYLLABLE1, TYPE_SYLLABLE2, TYPE_SYLLABLE3,
};
use populate_builtins::{Combine, JoinExt, MapExt, combine2, combine3, inc, pick_one_ref};
use populate_builtins_tpch::{PickNBuffered, Range, TpcHRange, TpchRng, seeded};
use populate_core::Distribution;

fn range(min: i64, max: i64) -> Range<i64, TpcHRange> {
    Range::with_sampler(min, max)
}

fn rng(seed: i64) -> TpchRng {
    TpchRng::new(seed)
}

static PART_TYPES: LazyLock<Combine> =
    LazyLock::new(|| combine3(&TYPE_SYLLABLE3, &TYPE_SYLLABLE2, &TYPE_SYLLABLE1, " "));

static PART_CONTAINERS: LazyLock<Combine> =
    LazyLock::new(|| combine2(&CONTAINER_SIZE, &CONTAINER_TYPE, " "));

/// Calculate part retail price from partkey (result in cents)
pub fn calculate_retail_price(partkey: i64) -> i64 {
    90000 + (partkey / 10) % 20001 + (partkey % 1000) * 100
}

/// (p_partkey, p_name, p_type, p_size, p_container, p_mfgr_num, p_brand_num, p_retailprice)
pub fn parts() -> impl Distribution<(i64, String, &'static str, i64, &'static str, i32, i32, i64)> {
    (
        inc(1i64),                                                                      // p_partkey
        seeded(TpchRng::new(709314158), PickNBuffered::<_, 5>::new(&COLORS)).join(" "), // p_name
        // p_type: pick from 150 combinations (6*5*5)
        seeded(
            TpchRng::new(1841581359),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_TYPES.len() as i64),
                &PART_TYPES,
            ),
        ),
        seeded(rng(1193163244), range(1, 51)), // p_size: 1-50
        // p_container: pick from 40 combinations (5*8)
        seeded(
            TpchRng::new(727633698),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_CONTAINERS.len() as i64),
                &PART_CONTAINERS,
            ),
        ),
        seeded(rng(1), range(1, 6)),        // p_mfgr: range 1-5
        seeded(rng(46831694), range(1, 6)), // p_brand_raw: range 1-5 (combined with mfgr below)
    )
        .map(|(key, name, typ, size, container, mfgr, brand_raw)| {
            // brand = mfgr * 10 + brand_raw (e.g., mfgr=1, brand_raw=3 -> brand=13)
            let retailprice = calculate_retail_price(key);
            (
                key,
                name,
                typ.as_str(),
                size,
                container.as_str(),
                mfgr as i32,
                (mfgr * 10 + brand_raw) as i32,
                retailprice,
            )
        })
}
