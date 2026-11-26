//! Chunked generation into raw column buffers, independent of an output backend.

use crate::sched::{par_ordered, resolve_threads};
use crate::{Columns, Distribution};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::convert::Infallible;

/// Default source rows per chunk.
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
/// Default source rows per batch within a chunk.
pub const DEFAULT_BATCH_ROWS: usize = 8 * 1024;

/// SplitMix64 finaliser, used to seed independent batches by position.
#[inline]
pub const fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A finite distribution generated in independently addressable chunks.
///
/// Batches are [`Columns<D::Item>`], so a backend consumes native buffers
/// without an Arrow schema or conversion. This also supports custom
/// [`Columnar`](crate::Columnar) types that have no Arrow representation.
///
/// Each chunk clones the distribution and skips to its first source row.
/// Within the chunk the distribution advances continuously, with a caller
/// RNG seeded from `(seed, batch start)` for each batch. For fixed chunk and
/// batch sizes, output is independent of thread count and chunk order.
/// Changing either size can change random values; `skip` does not advance
/// the caller RNG. Seekable distributions that ignore that RNG can provide
/// stronger guarantees.
///
/// Counts refer to source rows. A distribution of [`Children`](crate::Children)
/// or [`Tables`](crate::Tables) can emit a different number of output rows.
#[derive(Debug, Clone)]
pub struct DistChunks<D> {
    dist: D,
    rows: u64,
    chunk_rows: u64,
    batch_rows: u64,
    seed: u64,
}

impl<D> DistChunks<D> {
    /// `rows` source rows, in chunks of 256Ki and batches of 8Ki, seed 0.
    pub fn new(rows: u64, dist: D) -> Self {
        Self {
            dist,
            rows,
            chunk_rows: DEFAULT_CHUNK_ROWS,
            batch_rows: DEFAULT_BATCH_ROWS as u64,
            seed: 0,
        }
    }

    /// The raw distribution, at its original position.
    pub fn dist(&self) -> &D {
        &self.dist
    }

    /// Recover the original distribution without generating any rows.
    pub fn into_dist(self) -> D {
        self.dist
    }

    /// Total source rows.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Seed for distributions drawing from the caller RNG.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Source rows per independently generated chunk; must be positive.
    pub fn chunk_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.chunk_rows = n;
        self
    }

    /// Maximum source rows per batch; must be positive and fit in `usize`.
    pub fn batch_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "batch_rows must be > 0");
        usize::try_from(n).expect("batch fits in memory");
        self.batch_rows = n;
        self
    }

    /// Number of chunks, including a possibly shorter final chunk.
    pub fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows)
    }
}

impl<D: Distribution + Clone> DistChunks<D> {
    fn collect_next(&self, dist: &mut D, first: u64, n: u64) -> Columns<D::Item> {
        let mut rng = SmallRng::seed_from_u64(mix64(self.seed ^ first));
        dist.collect(&mut rng, usize::try_from(n).expect("batch fits in memory"))
    }

    /// Collect a source range as raw columns. Panics if it exceeds `rows`.
    pub fn batch(&self, first: u64, n: u64) -> Columns<D::Item> {
        assert!(
            first <= self.rows && n <= self.rows - first,
            "batch out of bounds"
        );
        let mut dist = self.dist.clone();
        dist.skip(first);
        self.collect_next(&mut dist, first, n)
    }

    /// Generate chunk `i`, emitting raw batches in order. Panics for an
    /// invalid index. Only one batch is allocated at a time by this method.
    pub fn chunk(&self, i: u64, mut emit: impl FnMut(Columns<D::Item>)) {
        let result: Result<(), Infallible> = self.try_chunk(i, |cols| {
            emit(cols);
            Ok(())
        });
        result.unwrap_or_else(|e| match e {});
    }

    /// Generate a chunk, stopping as soon as the backend returns an error.
    pub fn try_chunk<E>(
        &self,
        i: u64,
        mut emit: impl FnMut(Columns<D::Item>) -> Result<(), E>,
    ) -> Result<(), E> {
        assert!(i < self.num_chunks(), "chunk index out of bounds");
        let first = i * self.chunk_rows;
        let end = first + self.chunk_rows.min(self.rows - first);
        let mut dist = self.dist.clone();
        dist.skip(first);
        let mut at = first;
        while at < end {
            let n = self.batch_rows.min(end - at);
            emit(self.collect_next(&mut dist, at, n))?;
            at += n;
        }
        Ok(())
    }
}

impl<D> DistChunks<D>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: Send,
{
    /// Generate chunks on `threads` threads (0 = all cores), passing raw
    /// batches to the backend in order on the calling thread.
    /// At most `2 * threads` chunks are buffered; use `chunk` for a backend
    /// that should consume each batch directly on its own worker.
    pub fn for_each_batch(&self, threads: usize, mut emit: impl FnMut(Columns<D::Item>)) {
        let result: Result<(), Infallible> = self.try_for_each_batch(threads, |cols| {
            emit(cols);
            Ok(())
        });
        result.unwrap_or_else(|e| match e {});
    }

    /// Ordered parallel generation that stops consuming on the first
    /// backend error. Work already running may finish before returning.
    pub fn try_for_each_batch<E>(
        &self,
        threads: usize,
        mut emit: impl FnMut(Columns<D::Item>) -> Result<(), E>,
    ) -> Result<(), E> {
        let threads = resolve_threads(threads);
        if threads == 1 {
            for i in 0..self.num_chunks() {
                self.try_chunk(i, &mut emit)?;
            }
            return Ok(());
        }
        // Bound reordering even when an early chunk is much slower than
        // all later chunks. Each window finishes before another is queued.
        let window = (threads as u64).saturating_mul(2);
        let mut first = 0;
        while first < self.num_chunks() {
            let n = window.min(self.num_chunks() - first);
            par_ordered(
                n,
                threads,
                |i| {
                    let mut batches = Vec::new();
                    self.chunk(first + i, |cols| batches.push(cols));
                    batches
                },
                |batches| batches.into_iter().try_for_each(&mut emit),
            )?;
            first += n;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Columnar;
    use rand::Rng;

    // Deliberately has no Arrow conversion or schema.
    #[derive(Debug, PartialEq)]
    struct Row {
        id: u64,
        random: u64,
    }

    impl Columnar for Row {
        type Columns = Vec<Row>;
    }

    #[derive(Clone)]
    struct Source(u64);

    impl Distribution for Source {
        type Item = Row;

        fn collect_into(&mut self, cols: &mut Vec<Row>, rng: &mut impl Rng, n: usize) {
            cols.extend((0..n).map(|_| {
                let id = self.0;
                self.0 += 1;
                Row {
                    id,
                    random: rng.random(),
                }
            }));
        }

        fn skip(&mut self, n: u64) {
            self.0 += n;
        }
    }

    fn source() -> impl Distribution<Item = Row> + Clone + Sync {
        Source(0)
    }

    #[test]
    fn custom_backend_gets_identical_values_in_order_on_any_thread_count() {
        let chunks = source().chunks(43).chunk_rows(5).batch_rows(2).seed(17);
        let mut expected = Vec::new();
        chunks.for_each_batch(1, |cols| expected.extend(cols));
        assert_eq!(
            expected.iter().map(|r| r.id).collect::<Vec<_>>(),
            (0..43).collect::<Vec<_>>()
        );
        for threads in [2, 4] {
            let mut got = Vec::new();
            chunks.for_each_batch(threads, |cols| got.extend(cols));
            assert_eq!(got, expected);
        }
        for i in (0..chunks.num_chunks()).rev() {
            let mut got = Vec::new();
            chunks.chunk(i, |cols| got.extend(cols));
            let first = (i * 5) as usize;
            assert_eq!(got, expected[first..(first + 5).min(43)]);
        }
        let mut other_seed = Vec::new();
        chunks
            .seed(18)
            .for_each_batch(2, |cols| other_seed.extend(cols));
        assert_ne!(other_seed, expected);
    }

    #[test]
    fn batches_preserve_positions_and_the_final_partial_chunk() {
        let chunks = source().chunks(13).chunk_rows(5).batch_rows(3);
        assert_eq!(chunks.num_chunks(), 3);
        let mut got = Vec::new();
        chunks.chunk(2, |cols| got.push(cols));
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].iter().map(|r| r.id).collect::<Vec<_>>(),
            [10, 11, 12]
        );
        assert_eq!(got[0], chunks.batch(10, 3));
        let mut original = chunks.into_dist();
        assert_eq!(original.collect(&mut rand::rng(), 1)[0].id, 0);
    }

    #[test]
    fn empty_sources_and_backend_errors() {
        for threads in [1, 3] {
            let empty = source().chunks(0);
            empty.for_each_batch(threads, |_| panic!("empty source emitted a batch"));
            assert!(empty.batch(0, 0).is_empty());
            let chunks = source().chunks(100).chunk_rows(5).batch_rows(2);
            let mut ids = Vec::new();
            let result = chunks.try_for_each_batch(threads, |cols| {
                ids.extend(cols.iter().map(|r| r.id));
                if cols[0].id == 5 {
                    Err("backend full")
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err("backend full"));
            assert_eq!(ids, [0, 1, 2, 3, 4, 5, 6]);
        }
    }

    #[test]
    fn last_chunk_near_u64_limit_does_not_overflow() {
        let chunks = source().chunks(u64::MAX).chunk_rows(u64::MAX - 2);
        let mut got = Vec::new();
        chunks.chunk(1, |cols| got.extend(cols));
        assert_eq!(
            got.iter().map(|r| r.id).collect::<Vec<_>>(),
            [u64::MAX - 2, u64::MAX - 1]
        );
    }

    #[test]
    #[should_panic(expected = "chunk index out of bounds")]
    fn invalid_chunk_is_rejected() {
        source().chunks(3).chunk_rows(2).chunk(u64::MAX, |_| {});
    }

    #[test]
    #[should_panic(expected = "batch out of bounds")]
    fn overflowing_batch_is_rejected() {
        source().chunks(3).batch(2, u64::MAX);
    }
}
