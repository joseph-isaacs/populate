use crate::traits::{Backend, Distribution, SampleIter, Sink, SinkOf};
use std::cell::RefCell;
use std::rc::Rc;

mod col;
mod deque;
mod dist;
mod list;
mod named;
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
    SinkOf<B, D::Value>: Sink<D::Value>,
{
    type Value = D::Value;
    type Iter = TrackedIter<D::Iter>;

    fn row_iter(&mut self) -> Self::Iter {
        TrackedIter {
            name: self.name,
            inner: self.inner.row_iter(),
            log: Rc::clone(&self.log),
        }
    }

    fn default_sink(&self) -> SinkOf<B, Self::Value> {
        self.inner.default_sink()
    }

    fn collect_into(
        &mut self,
        mut sink: SinkOf<B, Self::Value>,
        rng: &mut impl rand::Rng,
        n: usize,
    ) -> SinkOf<B, Self::Value> {
        let mut iter = self.row_iter();
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

fn tracked<D>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
    Tracked {
        name,
        inner,
        log: Rc::clone(log),
    }
}
