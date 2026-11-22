//! Tuples of distributions are distributions: `(D1, D2)` yields `(D1::Item,
//! D2::Item)` and collects column by column, appending each part's HList sink.

use crate::traits::{Backend, DefaultSink, Distribution, HAppend, SampleIter};
use rand::Rng;

// ============================================================================
// Tuple (D1, D2) — col-by-col using HAppend
// ============================================================================

pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng))
    }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>> Distribution<B> for (D1, D2)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
{
    type Item = (D1::Item, D2::Item);
    type RowIter = TupleIter2<D1::RowIter, D2::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter2(self.0.row_iter(), self.1.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(sink);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        s1.append(s2)
    }
}

// ============================================================================
// Tuple (D1, D2, D3) — col-by-col
// ============================================================================

pub struct TupleIter3<I1, I2, I3>(pub I1, pub I2, pub I3);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter> SampleIter for TupleIter3<I1, I2, I3> {
    type Item = (I1::Item, I2::Item, I3::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng), self.2.next(rng))
    }
}

impl<B: Backend, D1: Distribution<B>, D2: Distribution<B>, D3: Distribution<B>> Distribution<B>
    for (D1, D2, D3)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
    <<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink<B>>::Sink>,
{
    type Item = (D1::Item, D2::Item, D3::Item);
    type RowIter = TupleIter3<D1::RowIter, D2::RowIter, D3::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter3(self.0.row_iter(), self.1.row_iter(), self.2.row_iter())
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item, D3::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S3<B, D3> = <<D3 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S12<B, D1, D2> = <S1<B, D1> as HAppend<S2<B, D2>>>::Output;
        let (s12, s3) = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::unappend(sink);
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(s12);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        s1.append(s2).append(s3)
    }
}

// ============================================================================
// Tuple (D1, D2, D3, D4) — col-by-col
// ============================================================================

pub struct TupleIter4<I1, I2, I3, I4>(pub I1, pub I2, pub I3, pub I4);

impl<I1: SampleIter, I2: SampleIter, I3: SampleIter, I4: SampleIter> SampleIter
    for TupleIter4<I1, I2, I3, I4>
{
    type Item = (I1::Item, I2::Item, I3::Item, I4::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (
            self.0.next(rng),
            self.1.next(rng),
            self.2.next(rng),
            self.3.next(rng),
        )
    }
}

impl<
        B: Backend,
        D1: Distribution<B>,
        D2: Distribution<B>,
        D3: Distribution<B>,
        D4: Distribution<B>,
    > Distribution<B> for (D1, D2, D3, D4)
where
    <D1::Item as DefaultSink<B>>::Sink: HAppend<<D2::Item as DefaultSink<B>>::Sink>,
    <<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D3::Item as DefaultSink<B>>::Sink>,
    <<<D1::Item as DefaultSink<B>>::Sink as HAppend<<D2::Item as DefaultSink<B>>::Sink>>::Output
        as HAppend<<D3::Item as DefaultSink<B>>::Sink>>::Output:
        HAppend<<D4::Item as DefaultSink<B>>::Sink>,
{
    type Item = (D1::Item, D2::Item, D3::Item, D4::Item);
    type RowIter = TupleIter4<D1::RowIter, D2::RowIter, D3::RowIter, D4::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TupleIter4(
            self.0.row_iter(),
            self.1.row_iter(),
            self.2.row_iter(),
            self.3.row_iter(),
        )
    }

    fn collect_into(
        &mut self,
        sink: <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <(D1::Item, D2::Item, D3::Item, D4::Item) as DefaultSink<B>>::Sink {
        type S1<B, D1> = <<D1 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S2<B, D2> = <<D2 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S3<B, D3> = <<D3 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S4<B, D4> = <<D4 as Distribution<B>>::Item as DefaultSink<B>>::Sink;
        type S12<B, D1, D2> = <S1<B, D1> as HAppend<S2<B, D2>>>::Output;
        type S123<B, D1, D2, D3> = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::Output;
        let (s123, s4) = <S123<B, D1, D2, D3> as HAppend<S4<B, D4>>>::unappend(sink);
        let (s12, s3) = <S12<B, D1, D2> as HAppend<S3<B, D3>>>::unappend(s123);
        let (s1, s2) = <S1<B, D1> as HAppend<S2<B, D2>>>::unappend(s12);
        let s1 = self.0.collect_into(s1, rng, n);
        let s2 = self.1.collect_into(s2, rng, n);
        let s3 = self.2.collect_into(s3, rng, n);
        let s4 = self.3.collect_into(s4, rng, n);
        s1.append(s2).append(s3).append(s4)
    }
}
