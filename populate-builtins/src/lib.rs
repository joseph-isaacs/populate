mod constant;
mod list;
mod range;

pub use constant::{Constant, ConstantIter};
pub use list::{List, ListIter};
pub use range::{Range, RangeIter};

// Re-export tuple types from core (they must be there due to orphan rules)
pub use populate_core::{TupleIter2, TupleIter3};
use populate_core::Distribution;

// Convenience functions
pub fn constant<T: Clone>(value: T) -> Constant<T> {
    Constant::new(value)
}

pub fn range<T: Clone>(min: T, max: T) -> Range<T> {
    Range::new(min, max)
}

pub fn list<L: Distribution<usize>, E>(lengths: L, elements: E) -> List<L, E> {
    List::new(lengths, elements)
}
