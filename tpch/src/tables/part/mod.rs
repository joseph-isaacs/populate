mod constants;

use std::sync::LazyLock;

use constants::{
    COLORS, CONTAINER_SIZE, CONTAINER_TYPE, TYPE_SYLLABLE1, TYPE_SYLLABLE2, TYPE_SYLLABLE3,
};
use populate_builtins::{Combine, JoinExt, MapExt, TapExt, combine2, combine3, inc, pick_one_ref};
use populate_builtins_tpch::{PickNBuffered, Range, TpcHRange, TpchRng, seeded};
use populate_core::{Distribution, unnest};

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
///
/// This distribution is structured for column-by-column collection:
/// - Independent columns: partkey, name, type, size, container, retailprice
/// - Coupled columns (via Unnest): mfgr and brand (brand depends on mfgr)
#[allow(clippy::type_complexity)]
pub fn parts() -> impl Distribution<
    (i64, String, &'static str, i64, &'static str, i32, i32, i64),
    Sinks = (
        Vec<i64>,
        Vec<String>,
        Vec<&'static str>,
        Vec<i64>,
        Vec<&'static str>,
        Vec<i32>,
        Vec<i32>,
        Vec<i64>,
    ),
> {
    (
        // p_partkey - independent
        inc(1i64).tap("inc"),
        // p_name - independent
        seeded(rng(709314158), PickNBuffered::<_, 5>::new(&COLORS)).join(" ").tap("p naame"),
        // p_type - independent (single-column map for .as_str())
        seeded(
            rng(1841581359),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_TYPES.len() as i64),
                &PART_TYPES,
            ),
        )
        .map(|c| c.as_str()),
        // p_size - independent
        seeded(rng(1193163244), range(1, 51)),
        // p_container - independent (single-column map for .as_str())
        seeded(
            rng(727633698),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_CONTAINERS.len() as i64),
                &PART_CONTAINERS,
            ),
        )
        .map(|c| c.as_str()),
        // (p_mfgr, p_brand) - COUPLED via Unnest
        // brand = mfgr * 10 + brand_raw, so these must be generated together
        unnest(
            (
                seeded(rng(1), range(1, 6)),
                seeded(rng(46831694), range(1, 6)),
            )
                .map(|(mfgr, brand_raw)| (mfgr as i32, (mfgr * 10 + brand_raw) as i32)),
        ),
        // p_retailprice - independent (separate inc iterator, deterministic)
        inc(1i64).map(calculate_retail_price),
    )
}

/// Same as `parts()` but with tap on each column to show execution order.
/// Run with `cargo test test_parts_tapped -- --nocapture` to see output.
#[allow(clippy::type_complexity)]
pub fn parts_tapped() -> impl Distribution<
    (i64, String, &'static str, i64, &'static str, i32, i32, i64),
    Sinks = (
        Vec<i64>,
        Vec<String>,
        Vec<&'static str>,
        Vec<i64>,
        Vec<&'static str>,
        Vec<i32>,
        Vec<i32>,
        Vec<i64>,
    ),
> {
    (
        // p_partkey - independent
        inc(1i64).tap("partkey"),
        // p_name - independent
        seeded(rng(709314158), PickNBuffered::<_, 5>::new(&COLORS))
            .join(" ")
            .tap("name"),
        // p_type - independent
        seeded(
            rng(1841581359),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_TYPES.len() as i64),
                &PART_TYPES,
            ),
        )
        .map(|c| c.as_str())
        .tap("type"),
        // p_size - independent
        seeded(rng(1193163244), range(1, 51)).tap("size"),
        // p_container - independent
        seeded(
            rng(727633698),
            pick_one_ref(
                Range::<_, TpcHRange>::with_sampler(0i64, PART_CONTAINERS.len() as i64),
                &PART_CONTAINERS,
            ),
        )
        .map(|c| c.as_str())
        .tap("container"),
        // (p_mfgr, p_brand) - COUPLED via Unnest
        unnest(
            (
                seeded(rng(1), range(1, 6)),
                seeded(rng(46831694), range(1, 6)),
            )
                .map(|(mfgr, brand_raw)| (mfgr as i32, (mfgr * 10 + brand_raw) as i32))
                .tap("mfgr_brand"),
        ),
        // p_retailprice - independent
        inc(1i64).map(calculate_retail_price).tap("retailprice"),
    )
}
