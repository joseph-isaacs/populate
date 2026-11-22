use crate::traits::{Backend, DefaultSink, Distribution, SampleIter, Sink, collect_rows};
use std::cell::RefCell;
use std::rc::Rc;

#[cfg(feature = "arrow")]
mod arrow;
mod col;
mod deque;
mod derive;
mod dist;
mod row;
mod scalar;

type Log = Rc<RefCell<Vec<&'static str>>>;

// ========================================================================
// Tracked distribution for verifying iteration order
// ========================================================================

struct Tracked<D> {
    name: &'static str,
    inner: D,
    log: Log,
}

struct TrackedIter<I> {
    name: &'static str,
    inner: I,
    log: Log,
}

impl<I: SampleIter> SampleIter for TrackedIter<I> {
    type Item = I::Item;
    fn next(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
        self.log.borrow_mut().push(self.name);
        self.inner.next(rng)
    }
}

impl<B: Backend, D: Distribution<B>> Distribution<B> for Tracked<D>
where
    <D::Item as DefaultSink<B>>::Sink: Sink<D::Item>,
{
    type Item = D::Item;
    type RowIter = TrackedIter<D::RowIter>;

    fn row_iter(&mut self) -> Self::RowIter {
        TrackedIter {
            name: self.name,
            inner: self.inner.row_iter(),
            log: Rc::clone(&self.log),
        }
    }

    fn collect_into(
        &mut self,
        sink: <D::Item as DefaultSink<B>>::Sink,
        rng: &mut impl rand::Rng,
        n: usize,
    ) -> <D::Item as DefaultSink<B>>::Sink {
        collect_rows(self.row_iter(), sink, rng, n)
    }
}

fn tracked<D>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
    Tracked {
        name,
        inner,
        log: Rc::clone(log),
    }
}
mod list;
mod list_ticks;
mod nested;
