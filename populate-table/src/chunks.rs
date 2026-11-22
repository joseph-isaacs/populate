//! [`Chunks`]: any table that can be generated as independent, ordered
//! chunks, and the parallel drivers that run one.
//!
//! [`Table`](crate::Table) is one (each chunk is a run of batches). A
//! generator with its own row logic is another: implement [`Chunks`] and it
//! gets the same multi-core [`for_each_batch`] and Parquet writer
//! (`write_parquet`, feature `parquet`).

use crate::table::par_ordered;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::convert::Infallible;

/// A table split into chunks that can be generated in any order, on any
/// thread. Chunk `i` must depend only on `i`, so output is the same for any
/// thread count. When written to Parquet, each chunk is one row group.
pub trait Chunks: Sync {
    /// Schema of every batch.
    fn schema(&self) -> SchemaRef;

    /// Number of chunks.
    fn num_chunks(&self) -> u64;

    /// Generate chunk `i`, passing its batches to `emit` in order.
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch));
}

/// A source whose chunks produce rows for several tables at once (say, a
/// sales table and its returns, generated in one pass). Every [`Chunks`] is
/// a `MultiChunks` with one output.
pub trait MultiChunks: Sync {
    /// Schema of each output table.
    fn schemas(&self) -> Vec<SchemaRef>;

    /// Number of chunks.
    fn num_chunks(&self) -> u64;

    /// Generate chunk `i`, passing `(output, batch)` to `emit`, in order
    /// within each output.
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch));
}

impl<T: Chunks> MultiChunks for T {
    fn schemas(&self) -> Vec<SchemaRef> {
        vec![Chunks::schema(self)]
    }

    fn num_chunks(&self) -> u64 {
        Chunks::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        Chunks::chunk(self, i, &mut |b| emit(0, b))
    }
}

/// Generate every chunk on `threads` threads (0 = all cores) and call `f`
/// with each batch, in order, on the calling thread.
pub fn for_each_batch(src: &impl Chunks, threads: usize, mut f: impl FnMut(RecordBatch)) {
    let r: Result<(), Infallible> = try_for_each_batch(src, threads, |b| {
        f(b);
        Ok(())
    });
    r.unwrap_or_else(|e| match e {});
}

/// [`for_each_batch`] that stops at the first error.
pub fn try_for_each_batch<E>(
    src: &impl Chunks,
    threads: usize,
    mut f: impl FnMut(RecordBatch) -> Result<(), E>,
) -> Result<(), E> {
    par_ordered(
        src.num_chunks(),
        threads,
        |i| {
            let mut v = Vec::new();
            src.chunk(i, &mut |b| v.push(b));
            v
        },
        |v| v.into_iter().try_for_each(&mut f),
    )
}

/// The chunks of several sources as one sequence of jobs: job `j` is chunk
/// `c` of source `s`, sources in order. For running many tables on one
/// [`par_ordered`] pool.
#[derive(Debug, Clone)]
pub struct Jobs {
    /// First job of each source, then the total.
    starts: Vec<u64>,
}

impl Jobs {
    /// Jobs for sources with these chunk counts.
    pub fn new(chunks: impl IntoIterator<Item = u64>) -> Self {
        let mut starts = vec![0];
        for n in chunks {
            starts.push(starts.last().expect("non-empty") + n);
        }
        Jobs { starts }
    }

    /// Number of jobs.
    pub fn total(&self) -> u64 {
        *self.starts.last().expect("non-empty")
    }

    /// `(source, chunk)` of job `j`.
    pub fn locate(&self, j: u64) -> (usize, u64) {
        let s = self.starts.partition_point(|&st| st <= j) - 1;
        (s, j - self.starts[s])
    }
}

#[cfg(test)]
mod tests {
    use super::Jobs;

    #[test]
    fn jobs_locate() {
        let j = Jobs::new([2, 0, 3]);
        assert_eq!(j.total(), 5);
        let got: Vec<_> = (0..5).map(|i| j.locate(i)).collect();
        assert_eq!(got, [(0, 0), (0, 1), (2, 0), (2, 1), (2, 2)]);
    }
}
