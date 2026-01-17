pub mod collect;
pub mod constant;
pub mod list;
pub mod range;
pub mod sink;
pub mod tuple;

pub use collect::{ElementCollector, ListCollector, StructCollector, Vecs};
pub use constant::Constant;
pub use list::{List, ListIter};
pub use range::Range;
pub use sink::{
    collect_into, collect_into_column, collect_list_into, collect_struct2_into,
    collect_struct3_into, ListSink, Sink, VecListSink, VecSink,
};
#[cfg(feature = "arrow")]
pub use sink::{ArrowListSink, ArrowListSinkType, ArrowSinkType, BooleanSink, PrimitiveSink, StringSink};
pub use tuple::TupleIter2;

use rand::Rng;

/// An infinite iterator that produces values using an RNG.
///
/// # Clone semantics
///
/// Iterators must implement `Clone` such that cloning produces an iterator
/// with no accumulated internal state. A cloned iterator must produce the
/// same sequence (given the same RNG state) as a freshly created iterator.
///
/// This invariant is used by list collectors to ensure each list gets
/// independent element generation - the element iterator is cloned before
/// generating each list's values.
///
/// For stateless iterators (e.g., `Range`, `Constant`), this is trivially satisfied.
/// For stateful iterators (e.g., `Runs`), clone must reset any accumulated state
/// (e.g., the "current run value") to its initial condition.
pub trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}

/// A factory that produces sample iterators.
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}
