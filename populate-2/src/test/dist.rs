// ========================================================================
// Helper functions returning impl Distribution — NO extra bounds needed!
// ========================================================================

use crate::dist::{constant, flatten, list, map, range};
use crate::test::{Log, tracked};
use crate::traits::{Backend, DefaultSink, Distribution, HAppend, SampleIter, Sink, VecBackend};

pub(crate) fn make_constant<B: Backend>() -> impl Distribution<B, Item = i32>
where
    <i32 as DefaultSink<B>>::Sink: Sink<i32>,
{
    constant(42)
}

pub(crate) fn make_range<B: Backend>() -> impl Distribution<B, Item = i64>
where
    <i64 as DefaultSink<B>>::Sink: Sink<i64>,
{
    range(0, 1)
}

pub(crate) fn make_tuple<B: Backend>() -> impl Distribution<B, Item = (i32, i64)>
where
    <i32 as DefaultSink<B>>::Sink: Sink<i32>,
    <i64 as DefaultSink<B>>::Sink: Sink<i64>,
    <i32 as DefaultSink<B>>::Sink: HAppend<<i64 as DefaultSink<B>>::Sink>,
{
    (constant(1), range(0, 10))
}

pub(crate) fn make_mapped() -> impl Distribution<VecBackend, Item = i64> {
    map(range(0, 1), |x| x * 2i64)
}

pub(crate) fn make_3tuple() -> impl Distribution<VecBackend, Item = (i32, i64, bool)> {
    (constant(1i32), range(0, 10), constant(true))
}

// ========================================================================
// Generic collect using row_iter + Sink (row-by-row)
// ========================================================================

pub(crate) fn collect<B: Backend, D: Distribution<B>, S: Sink<D::Item>>(
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

pub(crate) fn make_tracked_2tuple(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

pub(crate) fn make_tracked_3tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i64, i64, i32)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i32, 1), log),
    )
}

pub(crate) fn make_4tuple() -> impl Distribution<VecBackend, Item = (i32, i64, bool, f64)> {
    (
        constant(1i32),
        range(0i64, 10),
        constant(true),
        constant(2.5f64),
    )
}

pub(crate) fn make_tracked_4tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i64, i64, i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i64, 1), log),
        tracked("COL4", range(10000i64, 1), log),
    )
}

pub(crate) fn make_map_in_tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i64, i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        map(tracked("MAP_SRC", range(100i64, 1), log), |x| x * 2),
        tracked("COL3", range(1000i64, 1), log),
    )
}

pub(crate) fn gen_col4<B: Backend>(log: &Log) -> impl Distribution<B, Item = (i16, i64, i64, i32)> {
    flatten((
        tracked("COL1", range(0i16, 1), log),
        map(
            (
                tracked("MAP_A", range(100i64, 1), log),
                tracked("MAP_B", range(1000, 1), log),
            ),
            |(a, b)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    ))
}

pub(crate) fn make_flatten_order<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Item = (i32, i32, i32, i32)> {
    flatten((
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

pub(crate) fn make_nested_tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i32, i32, i32, i32)> {
    flatten((
        tracked("COL1", range(0i32, 1), log),
        (
            tracked("COL2", range(100i32, 1), log),
            tracked("COL3", range(1000i32, 1), log),
        ),
        tracked("COL4", range(0i32, 5), log),
    ))
}

pub(crate) fn gen_list() -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(range(0usize, 1), range(0, 1))
}

pub(crate) fn make_list_in_tuple() -> impl Distribution<VecBackend, Item = (i32, Vec<i64>)> {
    (constant(42i32), list(constant(2usize), range(0, 1)))
}

pub(crate) fn make_tracked_list_in_tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i32, Vec<i64>)> {
    (
        tracked("COL1", constant(42i32), log),
        tracked("LIST", list(constant(2usize), range(0, 1)), log),
    )
}

pub(crate) fn make_row_order(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}
