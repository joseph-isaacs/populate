//! Tuples of distributions are distributions: `(D1, D2)` yields `(D1::Item,
//! D2::Item)` and collects column by column, appending each part's HList sink.

use crate::traits::{Backend, DefaultSink, Distribution, HAppend, SampleIter};
use rand::Rng;

// ============================================================================
// Row iterators: one struct per arity, generated
// ============================================================================

macro_rules! tuple_iter {
    ($name:ident; $($i:ident $idx:tt),+) => {
        pub struct $name<$($i),+>($(pub $i),+);

        impl<$($i: SampleIter),+> SampleIter for $name<$($i),+> {
            type Item = ($($i::Item),+);

            fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
                ($(self.$idx.next(rng)),+)
            }
        }
    };
}

tuple_iter!(TupleIter2; I1 0, I2 1);
tuple_iter!(TupleIter3; I1 0, I2 1, I3 2);
tuple_iter!(TupleIter4; I1 0, I2 1, I3 2, I4 3);

// ============================================================================
// Distribution for tuples — col-by-col, one macro for every arity
// ============================================================================

/// One step of tuple collection: split `sink` into the columns of the first
/// k-1 distributions and those of the k-th, fill the prefix with `prefix`,
/// then the last part with `last`, and append them back together.
pub fn collect_split<B, P, D, F, R>(
    sink: P::Output,
    rng: &mut R,
    n: usize,
    prefix: F,
    last: &mut D,
) -> P::Output
where
    B: Backend,
    D: Distribution<B>,
    P: HAppend<<D::Item as DefaultSink<B>>::Sink>,
    F: FnOnce(P, &mut R) -> P,
    R: Rng,
{
    let (p, s) = P::unappend(sink);
    let p = prefix(p, rng);
    let s = last.collect_into(s, rng, n);
    p.append(s)
}

type SinkOf<B, D> = <<D as Distribution<B>>::Item as DefaultSink<B>>::Sink;

macro_rules! impl_tuple_distribution {
    // All distributions folded in: emit the impl.
    (@emit $iter:ident [$($d:ident $i:ident $idx:tt),+] [$($bounds:tt)*]
        $self:ident $sink:ident $rng:ident $n:ident $body:expr) => {
        impl<B: Backend, $($d: Distribution<B>),+> Distribution<B> for ($($d),+)
        where $($bounds)*
        {
            type Item = ($($d::Item),+);
            type RowIter = $iter<$($d::RowIter),+>;

            fn row_iter(&mut self) -> Self::RowIter {
                $iter($(self.$idx.row_iter()),+)
            }

            fn collect_into(
                &mut $self,
                $sink: <Self::Item as DefaultSink<B>>::Sink,
                $rng: &mut impl Rng,
                $n: usize,
            ) -> <Self::Item as DefaultSink<B>>::Sink {
                $body
            }
        }
    };
    // Fold in the next distribution: its sink is appended to the prefix's.
    (@fold $iter:ident $all:tt $acc:ty; [$($bounds:tt)*] [$self:ident $sink:ident $rng:ident $n:ident]
        $prev:expr; $next:ident $idx:tt $(, $rest:ident $ridx:tt)*) => {
        impl_tuple_distribution!(@fold $iter $all
            <$acc as HAppend<SinkOf<B, $next>>>::Output;
            [$($bounds)* $acc: HAppend<SinkOf<B, $next>>,]
            [$self $sink $rng $n]
            |s, rng| collect_split::<B, $acc, $next, _, _>(s, rng, $n, $prev, &mut $self.$idx);
            $($rest $ridx),*
        );
    };
    (@fold $iter:ident $all:tt $acc:ty; $bounds:tt [$self:ident $sink:ident $rng:ident $n:ident]
        $prev:expr;) => {
        impl_tuple_distribution!(@emit $iter $all $bounds $self $sink $rng $n ($prev)($sink, $rng));
    };
    // Entry: `TupleIterN; D1 I1 0, D2 I2 1, ...`
    ($iter:ident; $d1:ident $i1:ident $idx1:tt $(, $d:ident $i:ident $idx:tt)+) => {
        impl_tuple_distribution!(@fold $iter [$d1 $i1 $idx1 $(, $d $i $idx)+]
            SinkOf<B, $d1>; [] [self sink rng n]
            |s, rng| self.$idx1.collect_into(s, rng, n);
            $($d $idx),+
        );
    };
}

impl_tuple_distribution!(TupleIter2; D1 I1 0, D2 I2 1);
impl_tuple_distribution!(TupleIter3; D1 I1 0, D2 I2 1, D3 I3 2);
impl_tuple_distribution!(TupleIter4; D1 I1 0, D2 I2 1, D3 I3 2, D4 I4 3);
