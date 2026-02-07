// ========================================================================
// Helper functions returning impl Distribution — NO extra bounds needed!
// Sink type is exposed so callers can use collect_flat_into/collect_col.
// ========================================================================

use crate::dist::{constant, list, map, range};
use crate::test::{Log, tracked};
use crate::traits::{Backend, Distribution, HCons, HNil, SampleIter, Sink, VecBackend};

// -- Scalar helpers (generic B) --

pub(crate) fn make_constant<B: Backend>()
-> impl Distribution<B, Value = i32, Sink = HCons<B::Prim<i32>, HNil>> {
    constant(42i32)
}

pub(crate) fn make_range<B: Backend>()
-> impl Distribution<B, Value = i64, Sink = HCons<B::Prim<i64>, HNil>> {
    range(0i64, 1)
}

// -- Tuple helpers (generic B) --

pub(crate) fn make_tuple<B: Backend>()
-> impl Distribution<B, Value = (i32, i64), Sink = HCons<B::Prim<i32>, HCons<B::Prim<i64>, HNil>>> {
    (constant(1i32), range(0i64, 10))
}

// -- VecBackend-specific helpers --

pub(crate) fn make_mapped()
-> impl Distribution<VecBackend, Value = i64, Sink = HCons<Vec<i64>, HNil>> {
    map(range(0i64, 1), |x| x * 2i64)
}

pub(crate) fn make_3tuple() -> impl Distribution<
    VecBackend,
    Value = (i32, i64, bool),
    Sink = HCons<Vec<i32>, HCons<Vec<i64>, HCons<Vec<bool>, HNil>>>,
> {
    (constant(1i32), range(0i64, 10), constant(true))
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

// -- Tracked tuple helpers (VecBackend) --

pub(crate) fn make_tracked_2tuple(
    log: &Log,
) -> impl Distribution<VecBackend, Value = (i64, i64), Sink = HCons<Vec<i64>, HCons<Vec<i64>, HNil>>>
{
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}

pub(crate) fn make_tracked_3tuple(
    log: &Log,
) -> impl Distribution<
    VecBackend,
    Value = (i64, i64, i32),
    Sink = HCons<Vec<i64>, HCons<Vec<i64>, HCons<Vec<i32>, HNil>>>,
> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i32, 1), log),
    )
}

pub(crate) fn make_4tuple() -> impl Distribution<
    VecBackend,
    Value = (i32, i64, bool, f64),
    Sink = HCons<Vec<i32>, HCons<Vec<i64>, HCons<Vec<bool>, HCons<Vec<f64>, HNil>>>>,
> {
    (
        constant(1i32),
        range(0i64, 10),
        constant(true),
        constant(2.5f64),
    )
}

pub(crate) fn make_tracked_4tuple(
    log: &Log,
) -> impl Distribution<
    VecBackend,
    Value = (i64, i64, i64, i64),
    Sink = HCons<Vec<i64>, HCons<Vec<i64>, HCons<Vec<i64>, HCons<Vec<i64>, HNil>>>>,
> {
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
        tracked("COL3", range(1000i64, 1), log),
        tracked("COL4", range(10000i64, 1), log),
    )
}

pub(crate) fn make_map_in_tuple(
    log: &Log,
) -> impl Distribution<
    VecBackend,
    Value = (i64, i64, i64),
    Sink = HCons<Vec<i64>, HCons<Vec<i64>, HCons<Vec<i64>, HNil>>>,
> {
    (
        tracked("COL1", range(0i64, 1), log),
        map(tracked("MAP_SRC", range(100i64, 1), log), |x| x * 2),
        tracked("COL3", range(1000i64, 1), log),
    )
}

// -- Auto-flatten helpers (generic B) --

pub(crate) fn gen_col4<B: Backend>(
    log: &Log,
) -> impl Distribution<
    B,
    Value = (i16, i64, i64, i32),
    Sink = HCons<B::Prim<i16>, HCons<B::Prim<i64>, HCons<B::Prim<i64>, HCons<B::Prim<i32>, HNil>>>>,
> {
    (
        tracked("COL1", range(0i16, 1), log),
        map(
            (
                tracked("MAP_A", range(100i64, 1), log),
                tracked("MAP_B", range(1000i64, 1), log),
            ),
            |(a, b)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

pub(crate) fn make_flatten_order<B: Backend>(
    log: &Log,
) -> impl Distribution<
    B,
    Value = (i32, i32, i32, i32),
    Sink = HCons<B::Prim<i32>, HCons<B::Prim<i32>, HCons<B::Prim<i32>, HCons<B::Prim<i32>, HNil>>>>,
> {
    (
        tracked("COL1", range(0i32, 1), log),
        map(
            (
                tracked("MAP_A", range(100i32, 1), log),
                tracked("MAP_B", range(1000i32, 1), log),
            ),
            |(a, b)| (a + b, a * b),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

pub(crate) fn make_nested_tuple(
    log: &Log,
) -> impl Distribution<
    VecBackend,
    Value = (i32, i32, i32, i32),
    // Sink = HCons<Vec<i32>, HCons<Vec<i32>, HCons<Vec<i32>, HCons<Vec<i32>, HNil>>>>,
> {
    (
        tracked("COL1", range(0i32, 1), log),
        (
            tracked("COL2", range(100i32, 1), log),
            tracked("COL3", range(1000i32, 1), log),
        ),
        tracked("COL4", range(0i32, 5), log),
    )
}

// -- List helpers --

pub(crate) fn gen_list()
-> impl Distribution<VecBackend, Value = Vec<i64>, Sink = HCons<Vec<Vec<i64>>, HNil>> {
    list(range(0usize, 1), range(0i64, 1))
}

pub(crate) fn make_list_in_tuple() -> impl Distribution<
    VecBackend,
    Value = (i32, Vec<i64>),
    Sink = HCons<Vec<i32>, HCons<Vec<Vec<i64>>, HNil>>,
> {
    (constant(42i32), list(constant(2usize), range(0i64, 1)))
}

pub(crate) fn make_tracked_list_in_tuple(
    log: &Log,
) -> impl Distribution<
    VecBackend,
    Value = (i32, Vec<i64>),
    Sink = HCons<Vec<i32>, HCons<Vec<Vec<i64>>, HNil>>,
> {
    (
        tracked("COL1", constant(42i32), log),
        tracked("LIST", list(constant(2usize), range(0i64, 1)), log),
    )
}

pub(crate) fn make_row_order(
    log: &Log,
) -> impl Distribution<VecBackend, Value = (i64, i64), Sink = HCons<Vec<i64>, HCons<Vec<i64>, HNil>>>
{
    (
        tracked("COL1", range(0i64, 1), log),
        tracked("COL2", range(100i64, 1), log),
    )
}
