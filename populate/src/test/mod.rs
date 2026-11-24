use crate::Distribution;
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
mod row;
mod scalar;
mod skip;
mod with_column;

type Log = Rc<RefCell<Vec<&'static str>>>;

// ========================================================================
// Tracked distribution for verifying sampling order
// ========================================================================

/// Logs its name at every `sample`. It keeps the default `collect_into`, so
/// it collects by sampling: one tick per row.
struct Tracked<D> {
    name: &'static str,
    inner: D,
    log: Log,
}

impl<D: Distribution> Distribution for Tracked<D> {
    type Item = D::Item;

    fn sample(&mut self, rng: &mut impl rand::Rng) -> Self::Item {
        self.log.borrow_mut().push(self.name);
        self.inner.sample(rng)
    }
}

fn tracked<D>(name: &'static str, inner: D, log: &Log) -> Tracked<D> {
    Tracked {
        name,
        inner,
        log: Rc::clone(log),
    }
}
