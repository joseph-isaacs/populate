//! Distributions over state that is computed up front: [`column()`] (rows
//! already in memory, [`column_as`] converted) and [`checkpointed`] (a stream that can only skip by
//! running, made seekable by saving its state every so many rows).
//!
//! ```
//! use populate_builtins::{checkpointed, column, range};
//! use populate_core::Distribution;
//! use std::sync::Arc;
//!
//! let mut rng = rand::rng();
//! let mut c = column(Arc::new(vec![5u32, 6, 7, 8]));
//! c.skip(1);
//! assert_eq!(c.collect(&mut rng, 2), [6, 7]);
//!
//! // `range` skips in O(1) anyway; a stream whose skip runs it is where
//! // this pays
//! let (mut k, mut end) = checkpointed(range(0u64, 1), 100, 1000);
//! k.skip(250);
//! assert_eq!(k.collect(&mut rng, 2), [250, 251]);
//! assert_eq!(end.collect(&mut rng, 1), [1000]);
//! ```

use populate_core::{Columnar, Columns, Distribution, Sink};
use rand::Rng;
use std::marker::PhantomData;
use std::sync::Arc;

// ============================================================================
// Column
// ============================================================================

/// The rows of a column in memory, as `Out`. See [`column()`].
#[derive(Debug)]
pub struct Column<T, Out = T> {
    values: Arc<Vec<T>>,
    at: usize,
    _out: PhantomData<fn() -> Out>,
}

impl<T, Out> Clone for Column<T, Out> {
    fn clone(&self) -> Self {
        Column {
            values: self.values.clone(),
            at: self.at,
            _out: PhantomData,
        }
    }
}

/// The values of `values`, in order: a column that had to be built whole
/// before any row of it is known (a shuffle, a sort), shared by every
/// clone. Skipping is free. It is a `Vec` in an `Arc` so a column built in
/// memory is shared without copying it.
///
/// # Panics
///
/// Reading past the end.
pub fn column<T: Copy + Columnar>(values: Arc<Vec<T>>) -> Column<T> {
    column_as(values)
}

/// [`column()`], each value converted to `Out` as it is collected: one
/// pass from the shared column into the output's, where a
/// [`derive`](crate::derive()) would copy the rows out first. Several
/// columns of different types can read one stored column.
pub fn column_as<Out: From<T> + Columnar, T: Copy>(values: Arc<Vec<T>>) -> Column<T, Out> {
    Column {
        values,
        at: 0,
        _out: PhantomData,
    }
}

impl<T: Copy, Out: From<T> + Columnar> Distribution for Column<T, Out> {
    type Item = Out;

    fn collect_into(&mut self, cols: &mut Columns<Out>, _rng: &mut impl Rng, n: usize) {
        let rows = &self.values[self.at..self.at + n];
        cols.reserve(n);
        cols.push_n(rows.iter().map(|&v| Out::from(v)));
        self.at += n;
    }

    fn skip(&mut self, n: u64) {
        self.at += usize::try_from(n).expect("skip within the column");
    }
}

// ============================================================================
// Checkpointed
// ============================================================================

/// `inner`, with its state saved every `every` rows. See [`checkpointed`].
#[derive(Debug, Clone)]
pub struct Checkpointed<D> {
    /// `inner` at rows `0, every, 2 * every, ...` up to `rows`.
    saved: Arc<Vec<D>>,
    every: u64,
    inner: D,
    /// Rows `inner` has moved past.
    at: u64,
}

impl<D> Checkpointed<D> {
    /// The wrapped distribution as it stands.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

/// `inner` over its first `rows` rows, made cheap to skip: one pass over
/// the rows (by `inner`'s own [`skip`](Distribution::skip)) saves its
/// state every `every` rows, and a skip then starts from the last saved
/// state before the target and skips the rest, fewer than `every` rows.
///
/// For a stream that has no faster way to its `n`th row than to run
/// there: one whose rows use varying numbers of draws (rejection
/// sampling), or a generator that can't jump. Saved states are clones of
/// `inner`, so a big state (a Mersenne Twister's 2.5 KB) wants a big
/// `every`; a table's chunk size is the natural one.
///
/// Also returns `inner` after the `rows` rows, which is where the next
/// column starts when columns are consecutive runs of one stream.
pub fn checkpointed<D: Distribution + Clone>(
    inner: D,
    every: u64,
    rows: u64,
) -> (Checkpointed<D>, D) {
    assert!(every > 0, "checkpoint every > 0 rows");
    let mut saved = Vec::with_capacity((rows / every + 1) as usize);
    let mut d = inner.clone();
    let mut at = 0;
    loop {
        saved.push(d.clone());
        let n = every.min(rows - at);
        d.skip(n);
        at += n;
        if at == rows {
            break;
        }
    }
    if rows > 0 && rows.is_multiple_of(every) {
        saved.push(d.clone());
    }
    (
        Checkpointed {
            saved: Arc::new(saved),
            every,
            inner,
            at: 0,
        },
        d,
    )
}

impl<D: Distribution + Clone> Distribution for Checkpointed<D> {
    type Item = D::Item;

    fn collect_into(&mut self, cols: &mut Columns<D::Item>, rng: &mut impl Rng, n: usize) {
        self.inner.collect_into(cols, rng, n);
        self.at += n as u64;
    }

    fn skip(&mut self, n: u64) {
        let to = self.at + n;
        let k = to / self.every;
        match self.saved.get(k as usize) {
            Some(s) if k > self.at / self.every => {
                self.inner = s.clone();
                self.inner.skip(to - k * self.every);
            }
            _ => self.inner.skip(n),
        }
        self.at = to;
    }

    fn column_names(&self, out: &mut Vec<String>) {
        self.inner.column_names(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;

    /// 0, 1, 2, ..., skipping by running: `.1` counts the rows run.
    #[derive(Clone)]
    struct Slow(u64, Arc<AtomicU64>);

    impl Distribution for Slow {
        type Item = u64;

        fn collect_into(&mut self, cols: &mut Vec<u64>, _rng: &mut impl Rng, n: usize) {
            cols.extend(self.0..self.0 + n as u64);
            self.skip(n as u64);
        }

        fn skip(&mut self, n: u64) {
            self.1.fetch_add(n, Relaxed);
            self.0 += n;
        }
    }

    #[test]
    fn skips_start_from_the_last_checkpoint() {
        let ran = Arc::new(AtomicU64::new(0));
        let (k, end) = checkpointed(Slow(0, ran.clone()), 10, 95);
        assert_eq!(k.saved.len(), 10);
        let (whole, _) = checkpointed(Slow(0, ran.clone()), 10, 100);
        assert_eq!(whole.saved.len(), 11);
        assert_eq!(end.0, 95);
        let rng = &mut SmallRng::seed_from_u64(0);
        for first in [0, 9, 10, 11, 57, 90, 94] {
            let mut d = k.clone();
            d.skip(first);
            assert_eq!(d.collect(rng, 1), [first]);
            // and again from there
            d.skip(13);
            assert_eq!(d.collect(rng, 1), [first + 14]);
        }
        ran.store(0, Relaxed);
        let mut d = k.clone();
        d.skip(78);
        assert_eq!(ran.load(Relaxed), 8);
    }

    fn first_ten() -> impl Distribution<Item = i32> {
        column(Arc::new((0..10).collect()))
    }

    #[test]
    fn column_rows() {
        let mut c = first_ten();
        let rng = &mut SmallRng::seed_from_u64(0);
        assert_eq!(c.collect(rng, 1), [0]);
        c.skip(4);
        assert_eq!(c.collect(rng, 3), [5, 6, 7]);
    }
}
