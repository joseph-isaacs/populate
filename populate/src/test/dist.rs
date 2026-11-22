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

pub(crate) fn make_row_order(log: &Log) -> impl Distribution<VecBackend, Item = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

// ========================================================================
// List helpers
// ========================================================================

/// lens 0,1,2,..; elements 0,1,2,.. continuing across rows
pub(crate) fn gen_list() -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(range(0usize, 1), range(0i64, 1))
}

pub(crate) fn gen_list_of_tuples() -> impl Distribution<VecBackend, Item = Vec<(i64, bool)>> {
    list(constant(2usize), (range(0i64, 1), constant(true)))
}

pub(crate) fn gen_nested_list() -> impl Distribution<VecBackend, Item = Vec<Vec<i64>>> {
    list(constant(2usize), list(range(1usize, 1), range(0i64, 1)))
}

pub(crate) fn gen_list_in_tuple<B: Backend>() -> impl Distribution<B, Item = (i32, Vec<i64>)> {
    (constant(7i32), list(constant(2usize), range(0i64, 1)))
}

pub(crate) fn gen_list_in_flatten() -> impl Distribution<VecBackend, Item = (i32, Vec<i64>, i64)> {
    flatten((
        constant(1i32),
        (list(constant(1usize), range(0i64, 1)), constant(2i64)),
    ))
}

pub(crate) fn gen_map_to_list_of_tuples() -> impl Distribution<VecBackend, Item = Vec<(i64, i32)>> {
    map(range(0i64, 1), |x: i64| vec![(x, x as i32); 2])
}

// ---- tracked list helpers (tick tests) ----

pub(crate) fn make_tracked_list(log: &Log) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(
        tracked("LEN", range(0usize, 1), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_empty_list(log: &Log) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(
        tracked("LEN", constant(0usize), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_list_from_1(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(
        tracked("LEN", range(1usize, 1), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_list_len2(log: &Log) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    list(
        tracked("LEN", constant(2usize), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_list_between_cols(
    log: &Log,
) -> impl Distribution<VecBackend, Item = (i32, Vec<i64>, i32)> {
    (
        tracked("C1", range(0i32, 1), log),
        list(
            tracked("LEN", constant(2usize), log),
            tracked("E", range(0i64, 1), log),
        ),
        tracked("C3", range(0i32, 1), log),
    )
}

pub(crate) fn make_tracked_list_of_tuples(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<(i64, i32, bool)>> {
    list(
        tracked("LEN", constant(3usize), log),
        (
            tracked("A", range(0i64, 1), log),
            tracked("B", range(0i32, 1), log),
            tracked("C", constant(true), log),
        ),
    )
}

pub(crate) fn make_tracked_nested_list(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<Vec<i64>>> {
    list(
        tracked("OUTER", constant(2usize), log),
        list(
            tracked("INNER", range(1usize, 1), log),
            tracked("E", range(0i64, 1), log),
        ),
    )
}

pub(crate) fn make_list_wrapped_in_tracked(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    tracked(
        "LIST",
        list(
            tracked("LEN", constant(2usize), log),
            tracked("E", range(0i64, 1), log),
        ),
        log,
    )
}

pub(crate) fn make_tracked_map_to_list(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<i64>> {
    map(tracked("SRC", range(1usize, 1), log), |n: usize| {
        (0..n as i64).collect::<Vec<_>>()
    })
}

pub(crate) fn make_tracked_list_in_tuple<B: Backend>(
    log: &Log,
) -> impl Distribution<B, Item = (i32, Vec<i64>)> {
    (
        tracked("C1", constant(7i32), log),
        list(
            tracked("LEN", constant(2usize), log),
            tracked("E", range(0i64, 1), log),
        ),
    )
}

// ========================================================================
// Struct/list nesting helpers ("struct" = tuple; fields become columns)
// ========================================================================

/// struct of list: { id: i32, tags: list<i64> }
pub(crate) fn gen_struct_of_list<B: Backend>() -> impl Distribution<B, Item = (i32, Vec<i64>)> {
    (range(0i32, 1), list(range(0usize, 1), range(0i64, 1)))
}

/// struct of list of struct: { id: i32, items: list<{ qty: i64, ok: bool }> }
pub(crate) fn gen_struct_of_list_of_struct<B: Backend>()
-> impl Distribution<B, Item = (i32, Vec<(i64, bool)>)> {
    (
        range(0i32, 1),
        list(constant(2usize), (range(0i64, 1), constant(true))),
    )
}

/// list of struct of list: list<{ id: i32, tags: list<i64> }>
pub(crate) fn gen_list_of_struct_of_list<B: Backend>()
-> impl Distribution<B, Item = Vec<(i32, Vec<i64>)>> {
    list(
        constant(2usize),
        (range(0i32, 1), list(range(1usize, 1), range(0i64, 1))),
    )
}

/// tracked list of struct of list — for tick-order tests
pub(crate) fn make_tracked_list_of_struct_of_list(
    log: &Log,
) -> impl Distribution<VecBackend, Item = Vec<(i32, Vec<i64>)>> {
    list(
        tracked("OUTER", constant(2usize), log),
        (
            tracked("ID", range(0i32, 1), log),
            list(
                tracked("TLEN", range(1usize, 1), log),
                tracked("TAG", range(0i64, 1), log),
            ),
        ),
    )
}
