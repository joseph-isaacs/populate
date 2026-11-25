//! [`Chunks`]: any table that can be generated as independent, ordered
//! chunks, and the parallel drivers that run one.
//!
//! [`Table`](crate::Table) is one (each chunk is a run of batches). A
//! generator with its own row logic is another: implement [`Chunks`] and it
//! gets the same multi-core [`for_each_batch`] and Parquet writer
//! (`write_parquet`, feature `parquet`), and several of them run on one
//! pool with [`for_each_batch_many`].

use crate::sched::par_sources;
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
    for_each_batch_many(&[src as &dyn MultiChunks], threads, |_, b| f(b));
}

/// [`for_each_batch`] that stops at the first error.
pub fn try_for_each_batch<E>(
    src: &impl Chunks,
    threads: usize,
    mut f: impl FnMut(RecordBatch) -> Result<(), E>,
) -> Result<(), E> {
    try_for_each_batch_many(&[src as &dyn MultiChunks], threads, |_, b| f(b))
}

/// Generate several sources on one shared pool of `threads` threads (0 =
/// all cores) and call `f(output, batch)` on the calling thread with every
/// batch of every output, in order within each output. Outputs are
/// numbered as in `write_parquet_many`: sources
/// in order, each source's outputs in order. Sources with the most remaining
/// work go first ([`par_sources`]).
pub fn for_each_batch_many(
    srcs: &[&dyn MultiChunks],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch),
) {
    let r: Result<(), Infallible> = try_for_each_batch_many(srcs, threads, |o, b| {
        f(o, b);
        Ok(())
    });
    r.unwrap_or_else(|e| match e {});
}

/// [`for_each_batch_many`] that stops at the first error.
pub fn try_for_each_batch_many<E>(
    srcs: &[&dyn MultiChunks],
    threads: usize,
    mut f: impl FnMut(usize, RecordBatch) -> Result<(), E>,
) -> Result<(), E> {
    // output `first[s] + k` is output `k` of source `s`
    let mut first = Vec::with_capacity(srcs.len());
    let mut outputs = 0;
    for s in srcs {
        first.push(outputs);
        outputs += s.schemas().len();
    }
    let chunks: Vec<u64> = srcs.iter().map(|s| s.num_chunks()).collect();
    par_sources(
        &chunks,
        threads,
        |s, i| {
            let mut v = Vec::new();
            srcs[s].chunk(i, &mut |k, b| v.push((first[s] + k, b)));
            v
        },
        |_, v| v.into_iter().try_for_each(|(o, b)| f(o, b)),
    )
}

/// The chunks of several sources as one sequence of jobs: job `j` is chunk
/// `c` of source `s`, sources in order. For running many tables on one
/// [`par_ordered`](crate::par_ordered) pool.
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
    use super::*;
    use arrow_array::{Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    /// `chunks` chunks; chunk `i` is two batches, `10 * i` and `10 * i + 1`.
    struct One(u64);

    impl Chunks for One {
        fn schema(&self) -> SchemaRef {
            schema()
        }
        fn num_chunks(&self) -> u64 {
            self.0
        }
        fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
            emit(batch(10 * i as i64));
            emit(batch(10 * i as i64 + 1));
        }
    }

    /// Two outputs: chunk `i` gives `-i` to output 0 and `100 + i` to output 1.
    struct Two(u64);

    impl MultiChunks for Two {
        fn schemas(&self) -> Vec<SchemaRef> {
            vec![schema(), schema()]
        }
        fn num_chunks(&self) -> u64 {
            self.0
        }
        fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
            emit(1, batch(100 + i as i64));
            emit(0, batch(-(i as i64)));
        }
    }

    fn value(b: &RecordBatch) -> i64 {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.len(), 1);
        a.value(0)
    }

    #[test]
    fn many_sources_in_order_per_output() {
        let (a, b, c) = (One(5), Two(9), One(0));
        let srcs: [&dyn MultiChunks; 3] = [&a, &b, &c];
        for threads in [1, 2, 4] {
            let mut got = vec![Vec::new(); 3];
            for_each_batch_many(&srcs, threads, |o, b| got[o].push(value(&b)));
            let one: Vec<i64> = (0..5).flat_map(|i| [10 * i, 10 * i + 1]).collect();
            assert_eq!(got[0], one, "{threads} threads");
            assert_eq!(got[1], (0..9).map(|i| -i).collect::<Vec<_>>());
            assert_eq!(got[2], (0..9).map(|i| 100 + i).collect::<Vec<_>>());
        }
        let mut seen = Vec::new();
        for_each_batch(&a, 3, |b| seen.push(value(&b)));
        assert_eq!(seen.len(), 10);
        let r = try_for_each_batch(&a, 2, |b| if value(&b) == 21 { Err(21) } else { Ok(()) });
        assert_eq!(r, Err(21));
    }

    #[test]
    fn jobs_locate() {
        let j = Jobs::new([2, 0, 3]);
        assert_eq!(j.total(), 5);
        let got: Vec<_> = (0..5).map(|i| j.locate(i)).collect();
        assert_eq!(got, [(0, 0), (0, 1), (2, 0), (2, 1), (2, 2)]);
    }
}
