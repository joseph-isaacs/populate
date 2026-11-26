use crate::{Columns, Distribution};
use rand::Rng;
use std::cell::RefCell;
use std::rc::Rc;

#[cfg(feature = "arrow")]
mod arrow;
mod col;
mod dist;
mod list;
mod list_ticks;
mod nested;
mod random;
mod scalar;
mod skip;

type Log = Rc<RefCell<Vec<&'static str>>>;

// ========================================================================
// Tracked distribution for verifying sampling order
// ========================================================================

/// Logs its name once per row it collects, all of a block's ticks at
/// once, so the log shows which column was filled when, and how many rows.
struct Tracked<D> {
    name: &'static str,
    inner: D,
    log: Log,
}

impl<D: Distribution> Distribution for Tracked<D> {
    type Item = D::Item;

    fn collect_into(&mut self, cols: &mut Columns<D::Item>, rng: &mut impl Rng, n: usize) {
        self.log
            .borrow_mut()
            .extend(std::iter::repeat_n(self.name, n));
        self.inner.collect_into(cols, rng, n);
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n);
    }
}

fn tracked<D>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
    Tracked {
        name,
        inner,
        log: Rc::clone(log),
    }
}
