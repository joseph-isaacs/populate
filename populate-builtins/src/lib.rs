mod constant;
mod cycle;
mod inc;
mod join;
mod list;
mod map;
mod pick;
mod range;
mod seeded;
mod tpch_rng;

pub use constant::{Constant, ConstantIter};
pub use cycle::{Cycle, CycleIter};
pub use inc::{Inc, IncIter};
pub use join::{
    Join, JoinExt, JoinIter, JoinTuple2, JoinTuple2Ext, JoinTuple2Iter, JoinTuple3, JoinTuple3Ext,
    JoinTuple3Iter, JoinVec, JoinVecExt, JoinVecIter,
};
pub use list::{List, ListIter};
pub use map::{Map, MapExt, MapIter};
pub use pick::{Pick, PickIter, PickOne, PickOneIter};
pub use range::{Range, RangeIter};
pub use seeded::{Seeded, SeededIter};
pub use tpch_rng::{TpchBoundedInt, TpchBoundedIntIter, TpchRng, tpch_bounded_int};

// Re-export tuple types and unnest from core (they must be there due to orphan rules)
use populate_core::Distribution;
pub use populate_core::{
    TupleIter2, TupleIter3, TupleIter4, TupleIter5, TupleIter6, TupleIter7, TupleUnnest2Iter,
    Unnest, unnest,
};

// Convenience functions
pub fn constant<T: Clone>(value: T) -> Constant<T> {
    Constant::new(value)
}

pub fn cycle<T: Clone, const N: usize>(values: [T; N]) -> Cycle<T, N> {
    Cycle::new(values)
}

pub fn inc<T: Clone>(start: T) -> Inc<T> {
    Inc::new(start)
}

pub fn range<T: Clone>(min: T, max: T) -> Range<T> {
    Range::new(min, max)
}

pub fn list<L: Distribution<usize>, E>(lengths: L, elements: E) -> List<L, E> {
    List::new(lengths, elements)
}

pub fn seeded<R, D>(rng: R, dist: D) -> Seeded<R, D> {
    Seeded::new(rng, dist)
}

pub fn pick_one<T>(values: &[T]) -> PickOne<'_, T> {
    PickOne::new(values)
}

pub fn pick<T, const N: usize>(values: &[T]) -> Pick<'_, T, N> {
    Pick::new(values)
}
