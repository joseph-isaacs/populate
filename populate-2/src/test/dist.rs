// ========================================================================
// Helper functions returning impl Distribution — all generic over Backend.
// Sink type is derived from Value via SinkFor, so return types are clean.
// ========================================================================

use crate::dist::{DistBuilder, cols, constant, list, map, named, nested, range};
use crate::test::{Log, tracked};
use crate::traits::{
    Backend, Distribution, NamedVecBackend, Nested, SampleIter, Sink,
};

// -- Scalar helpers --

pub(crate) fn make_constant<B: Backend>() -> impl Distribution<B, Value = i32> {
    constant(42i32)
}

pub(crate) fn make_range<B: Backend>() -> impl Distribution<B, Value = i64> {
    range(0i64, 1)
}

pub(crate) fn make_mapped<B: Backend>() -> impl Distribution<B, Value = i64> {
    map(range(0i64, 1), |x| x * 2i64)
}

// -- Tuple helpers --

pub(crate) fn make_tuple<B: Backend>() -> impl Distribution<B, Value = (i32, i64)> {
    (constant(1i32), range(0i64, 10))
}

pub(crate) fn make_3tuple<B: Backend>() -> impl Distribution<B, Value = (i32, i64, bool)> {
    cols((constant(1i32), range(0i64, 10), constant(true)))
}

pub(crate) fn make_4tuple<B: Backend>() -> impl Distribution<B, Value = (i32, i64, bool, f64)> {
    cols((
        constant(1i32),
        range(0i64, 10),
        constant(true),
        constant(2.5f64),
    ))
}

// ========================================================================
// Generic collect using row_iter + Sink (row-by-row)
// ========================================================================

pub(crate) fn collect<B: Backend, D: Distribution<B>, S: Sink<D::Value>>(
    dist: &mut D,
    sink: &mut S,
    n: usize,
    rng: &mut impl rand::Rng,
) {
    let mut iter = dist.row_iter();
    for _ in 0..n {
        sink.push(iter.next(rng));
    }
}

// -- Tracked helpers --

pub(crate) fn make_tracked_2tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

pub(crate) fn make_tracked_3tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, i64, i32)> {
    cols((
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i32, 1), log),
    ))
}

pub(crate) fn make_tracked_4tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, i64, i64, i64)> {
    cols((
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i64, 1), log),
        tracked("COL4", range(10000i64, 1), log),
    ))
}

pub(crate) fn make_map_in_tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, i64, i64)> {
    cols((
        tracked("COL1", range(0i64, 1), log),
        map(tracked("MAP_SRC", range(100i64, 1), log), |x| x * 2),
        tracked("COL3", range(1000i64, 1), log),
    ))
}

pub(crate) fn make_nested_tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i32, i32, i32, i32)> {
    cols((
        tracked("COL1", range(0i32, 1), log),
        (
            tracked("COL2", range(100i32, 1), log),
            tracked("COL3", range(1000i32, 1), log),
        ),
        tracked("COL4", range(0i32, 5), log),
    ))
}

pub(crate) fn make_row_order<B: Backend>(log: &Log) -> impl Distribution<B, Value = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

// -- Auto-flatten helpers --

pub(crate) fn gen_col4<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i16, i64, i64, i32)> {
    cols((
        tracked("COL1", range(0i16, 1), log),
        map(
            (
                tracked("MAP_A", range(100i64, 1), log),
                tracked("MAP_B", range(1000i64, 1), log),
            ),
            |(a, b)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    ))
}

pub(crate) fn make_flatten_order<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i32, i32, i32, i32)> {
    cols((
        tracked("COL1", range(0i32, 1), log),
        map(
            (
                tracked("MAP_A", range(100i32, 1), log),
                tracked("MAP_B", range(1000i32, 1), log),
            ),
            |(a, b)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    ))
}

// -- List helpers --

pub(crate) fn gen_list<B: Backend>() -> impl Distribution<B, Value = Vec<i64>> {
    list(range(0usize, 1), range(0i64, 1))
}

pub(crate) fn make_list_in_tuple<B: Backend>() -> impl Distribution<B, Value = (i32, Vec<i64>)> {
    (constant(42i32), list(constant(2usize), range(0i64, 1)))
}

pub(crate) fn make_tracked_list_in_tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i32, Vec<i64>)> {
    (
        tracked("COL1", constant(42i32), log),
        tracked("LIST", list(constant(2usize), range(0i64, 1)), log),
    )
}

// -- Named helpers --

pub(crate) fn make_named_scalar<B: Backend>() -> impl Distribution<B, Value = i64> {
    named("id", range(0i64, 1))
}

pub(crate) fn make_named_tuple<B: Backend>() -> impl Distribution<B, Value = (i64, i32)> {
    (named("id", range(0i64, 1)), named("val", constant(42i32)))
}

pub(crate) fn make_named_3tuple<B: Backend>() -> impl Distribution<B, Value = (i64, i32, bool)> {
    cols((
        named("id", range(0i64, 1)),
        named("val", constant(42i32)),
        named("flag", constant(true)),
    ))
}

pub(crate) fn make_named_list<B: Backend>() -> impl Distribution<B, Value = (i32, Vec<i64>)> {
    (
        named("id", constant(1i32)),
        named("items", list(constant(2usize), range(0i64, 1))),
    )
}

pub(crate) fn make_named_tracked_tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, i64)> {
    (
        named("id", tracked("COL1", range(0i64, 1), log)),
        named("val", tracked("COL2", range(100i64, 1), log)),
    )
}

// -- Nested column helpers --

pub(crate) fn make_named_nested<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, Nested<(i32, i32)>, i32)> {
    cols((
        named("id", tracked("COL_ID", range(0i64, 1), log)),
        named(
            "address",
            nested((
                named("street", tracked("COL_STREET", range(100i32, 1), log)),
                named("city", tracked("COL_CITY", range(1000i32, 1), log)),
            )),
        ),
        named("age", tracked("COL_AGE", constant(25i32), log)),
    ))
}

pub(crate) fn make_named_nested_concrete(
    log: &Log,
) -> impl Distribution<NamedVecBackend, Value = (i64, Nested<(i32, i32)>, i32)> {
    cols((
        named("id", tracked("COL_ID", range(0i64, 1), log)),
        named(
            "address",
            nested((
                named("street", tracked("COL_STREET", range(100i32, 1), log)),
                named("city", tracked("COL_CITY", range(1000i32, 1), log)),
            )),
        ),
        named("age", tracked("COL_AGE", constant(25i32), log)),
    ))
}

pub(crate) fn make_named_nested_builder<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Value = (i64, Nested<(i32, i32)>, i32)> {
    named("id", tracked("COL_ID", range(0i64, 1), log))
        .append((
            named("street", tracked("COL_STREET", range(100i32, 1), log)),
            named("city", tracked("COL_CITY", range(1000i32, 1), log)),
        ))
        .extend(named("age", tracked("COL_AGE", constant(25i32), log)))
}
