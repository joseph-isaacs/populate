mod combine;
mod constant;
mod cycle;
mod inc;
mod join;
mod list;
mod map;
mod pick;
mod range;
mod seeded;

pub use combine::{Combine, combine2, combine3};
pub use constant::{Constant, ConstantIter};
pub use cycle::{Cycle, CycleIter};
pub use inc::{Inc, IncIter};
pub use join::{
    Join, JoinExt, JoinIter, JoinTuple2, JoinTuple2Ext, JoinTuple2Iter, JoinTuple3, JoinTuple3Ext,
    JoinTuple3Iter, JoinVec, JoinVecExt, JoinVecIter,
};
pub use list::{List, ListIter};
pub use map::{Map, MapExt, MapIter};
pub use pick::{Pick, PickIter, PickN, PickNIter, PickOne, PickOneIter, PickStrategy, Unique};
pub use range::{Range, RangeIter, RangeSampler, range};
pub use seeded::{Seeded, SeededIter};

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

pub fn list<L: Distribution<usize>, E>(lengths: L, elements: E) -> List<L, E> {
    List::new(lengths, elements)
}

pub fn seeded<R, D>(rng: R, dist: D) -> Seeded<R, D> {
    Seeded::new(rng, dist)
}

pub fn pick_one<T, I>(indices: I, values: &[T]) -> PickOne<'_, T, I> {
    PickOne::new(values, indices)
}

pub fn pick<T, I, const N: usize>(indices: I, values: &[T]) -> Pick<'_, T, I, N> {
    Pick::new(values, indices)
}
