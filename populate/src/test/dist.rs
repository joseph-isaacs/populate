// ========================================================================
// Helper functions returning impl Distribution — NO extra bounds needed!
// ========================================================================

use crate::Distribution;
use crate::dist::{constant, derive, list, range};
use crate::test::{Log, tracked};

pub(crate) fn make_constant() -> impl Distribution<Item = i32> {
    constant(42)
}

pub(crate) fn make_range() -> impl Distribution<Item = i64> {
    range(0, 1)
}

pub(crate) fn make_tuple() -> impl Distribution<Item = (i32, i64)> {
    (constant(1), range(0, 10))
}

pub(crate) fn make_mapped() -> impl Distribution<Item = i64> {
    derive(range(0i64, 1), |x: &i64| x * 2)
}

pub(crate) fn make_3tuple() -> impl Distribution<Item = (i32, i64, bool)> {
    (constant(1i32), range(0, 10), constant(true))
}

pub(crate) fn make_tracked_2tuple(log: &Log) -> impl Distribution<Item = (i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

pub(crate) fn make_tracked_3tuple(log: &Log) -> impl Distribution<Item = (i64, i64, i32)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i32, 1), log),
    )
}

pub(crate) fn make_4tuple() -> impl Distribution<Item = (i32, i64, bool, f64)> {
    (
        constant(1i32),
        range(0i64, 10),
        constant(true),
        constant(2.5f64),
    )
}

pub(crate) fn make_tracked_4tuple(log: &Log) -> impl Distribution<Item = (i64, i64, i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i64, 1), log),
        tracked("COL4", range(10000i64, 1), log),
    )
}

pub(crate) fn make_map_in_tuple(log: &Log) -> impl Distribution<Item = (i64, i64, i64)> {
    (
        tracked("COL1", range(0i64, 1), log),
        derive(tracked("MAP_SRC", range(100i64, 1), log), |x: &i64| x * 2),
        tracked("COL3", range(1000i64, 1), log),
    )
}

pub(crate) fn gen_col4(log: &Log) -> impl Distribution<Item = (i16, (i64, i64), i32)> {
    (
        tracked("COL1", range(0i16, 1), log),
        derive(
            (
                tracked("MAP_A", range(100i64, 1), log),
                tracked("MAP_B", range(1000i64, 1), log),
            ),
            |(a, b): (&i64, &i64)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

pub(crate) fn make_flatten_order(log: &Log) -> impl Distribution<Item = (i32, (i32, i32), i32)> {
    (
        tracked("COL1", range(0i32, 1), log),
        derive(
            (
                tracked("MAP_A", range(100i32, 1), log),
                tracked("MAP_B", range(1000i32, 1), log),
            ),
            |(a, b): (&i32, &i32)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

pub(crate) fn make_nested_tuple(log: &Log) -> impl Distribution<Item = (i32, (i32, i32), i32)> {
    (
        tracked("COL1", range(0i32, 1), log),
        (
            tracked("COL2", range(100i32, 1), log),
            tracked("COL3", range(1000i32, 1), log),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

// ========================================================================
// List helpers
// ========================================================================

/// lens 0,1,2,..; elements 0,1,2,.. continuing across rows
pub(crate) fn gen_list() -> impl Distribution<Item = Vec<i64>> {
    list(range(0i32, 1), range(0i64, 1))
}

pub(crate) fn gen_list_of_tuples() -> impl Distribution<Item = Vec<(i64, bool)>> {
    list(constant(2u8), (range(0i64, 1), constant(true)))
}

pub(crate) fn gen_nested_list() -> impl Distribution<Item = Vec<Vec<i64>>> {
    list(constant(2i32), list(range(1i32, 1), range(0i64, 1)))
}

pub(crate) fn gen_list_in_tuple() -> impl Distribution<Item = (i32, Vec<i64>)> {
    (constant(7i32), list(constant(2i32), range(0i64, 1)))
}

pub(crate) fn gen_list_in_nested_tuple() -> impl Distribution<Item = (i32, (Vec<i64>, i64))> {
    (
        constant(1i32),
        (list(constant(1i32), range(0i64, 1)), constant(2i64)),
    )
}

pub(crate) fn gen_map_to_list_of_tuples() -> impl Distribution<Item = Vec<(i64, i32)>> {
    derive(range(0i64, 1), |&x: &i64| vec![(x, x as i32); 2])
}

// ---- tracked list helpers (tick tests) ----

pub(crate) fn make_tracked_list(log: &Log) -> impl Distribution<Item = Vec<i64>> {
    list(
        tracked("LEN", range(0i32, 1), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_empty_list(log: &Log) -> impl Distribution<Item = Vec<i64>> {
    list(
        tracked("LEN", constant(0i32), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_list_len2(log: &Log) -> impl Distribution<Item = Vec<i64>> {
    list(
        tracked("LEN", constant(2i32), log),
        tracked("E", range(0i64, 1), log),
    )
}

pub(crate) fn make_tracked_list_between_cols(
    log: &Log,
) -> impl Distribution<Item = (i32, Vec<i64>, i32)> {
    (
        tracked("C1", range(0i32, 1), log),
        list(
            tracked("LEN", constant(2i32), log),
            tracked("E", range(0i64, 1), log),
        ),
        tracked("C3", range(0i32, 1), log),
    )
}

pub(crate) fn make_tracked_list_of_tuples(
    log: &Log,
) -> impl Distribution<Item = Vec<(i64, i32, bool)>> {
    list(
        tracked("LEN", constant(3i32), log),
        (
            tracked("A", range(0i64, 1), log),
            tracked("B", range(0i32, 1), log),
            tracked("C", constant(true), log),
        ),
    )
}

pub(crate) fn make_tracked_nested_list(log: &Log) -> impl Distribution<Item = Vec<Vec<i64>>> {
    list(
        tracked("OUTER", constant(2i32), log),
        list(
            tracked("INNER", range(1i32, 1), log),
            tracked("E", range(0i64, 1), log),
        ),
    )
}

pub(crate) fn make_list_wrapped_in_tracked(log: &Log) -> impl Distribution<Item = Vec<i64>> {
    tracked(
        "LIST",
        list(
            tracked("LEN", constant(2i32), log),
            tracked("E", range(0i64, 1), log),
        ),
        log,
    )
}

pub(crate) fn make_tracked_map_to_list(log: &Log) -> impl Distribution<Item = Vec<i64>> {
    derive(tracked("SRC", range(1i64, 1), log), |&n: &i64| {
        (0..n).collect::<Vec<_>>()
    })
}

pub(crate) fn make_tracked_list_in_tuple(log: &Log) -> impl Distribution<Item = (i32, Vec<i64>)> {
    (
        tracked("C1", constant(7i32), log),
        list(
            tracked("LEN", constant(2i32), log),
            tracked("E", range(0i64, 1), log),
        ),
    )
}

// ========================================================================
// Struct/list nesting helpers ("struct" = tuple; fields become columns)
// ========================================================================

/// struct of list: { id: i32, tags: list<i64> }
pub(crate) fn gen_struct_of_list() -> impl Distribution<Item = (i32, Vec<i64>)> {
    (range(0i32, 1), list(range(0i32, 1), range(0i64, 1)))
}

/// struct of list of struct: { id: i32, items: list<{ qty: i64, ok: bool }> }
pub(crate) fn gen_struct_of_list_of_struct() -> impl Distribution<Item = (i32, Vec<(i64, bool)>)> {
    (
        range(0i32, 1),
        list(constant(2i32), (range(0i64, 1), constant(true))),
    )
}

/// list of struct of list: list<{ id: i32, tags: list<i64> }>
pub(crate) fn gen_list_of_struct_of_list() -> impl Distribution<Item = Vec<(i32, Vec<i64>)>> {
    list(
        constant(2i32),
        (range(0i32, 1), list(range(1i32, 1), range(0i64, 1))),
    )
}

/// tracked list of struct of list — for tick-order tests
pub(crate) fn make_tracked_list_of_struct_of_list(
    log: &Log,
) -> impl Distribution<Item = Vec<(i32, Vec<i64>)>> {
    list(
        tracked("OUTER", constant(2i32), log),
        (
            tracked("ID", range(0i32, 1), log),
            list(
                tracked("TLEN", range(1i32, 1), log),
                tracked("TAG", range(0i64, 1), log),
            ),
        ),
    )
}
