//! Zero-copy list generation.
//!
//! A counting global allocator (scoped to the current thread by a flag) proves
//! that, with `ArrowBackend`:
//!
//! 1. collecting a list column performs exactly one allocation per buffer
//!    (offsets, values) at its final size, and never reallocates;
//! 2. turning the sink into a `RecordBatch` allocates no data-sized buffer,
//!    and the arrays point at the very same memory that was generated into.
//!
//! The `VecBackend` control test shows the harness does catch a copy.

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ListArray, RecordBatch, StructArray};
use populate::arrow::{ArrowBackend, ArrowOffsets, IntoArrowColumns, into_record_batch};
use populate::dist::{constant, list, map, range};
use populate::traits::{
    Backend, DefaultSink, Distribution, HCons, HNil, ListCol, Sink, VecBackend,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

// ============================================================================
// Counting allocator
// ============================================================================

struct Counting;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static REALLOCS: Cell<usize> = const { Cell::new(0) };
    static MAX_ALLOC: Cell<usize> = const { Cell::new(0) };
}

fn record_alloc(size: usize) {
    if TRACK.with(Cell::get) {
        ALLOCS.with(|c| c.set(c.get() + 1));
        MAX_ALLOC.with(|c| c.set(c.get().max(size)));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK.with(Cell::get) {
            REALLOCS.with(|c| c.set(c.get() + 1));
            MAX_ALLOC.with(|c| c.set(c.get().max(new_size)));
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Debug, PartialEq)]
struct Allocs {
    allocs: usize,
    reallocs: usize,
    max_size: usize,
}

/// Run `f` and report the allocations it made on this thread.
fn count_allocs<R>(f: impl FnOnce() -> R) -> (R, Allocs) {
    ALLOCS.with(|c| c.set(0));
    REALLOCS.with(|c| c.set(0));
    MAX_ALLOC.with(|c| c.set(0));
    TRACK.with(|t| t.set(true));
    let r = f();
    TRACK.with(|t| t.set(false));
    let a = Allocs {
        allocs: ALLOCS.with(Cell::get),
        reallocs: REALLOCS.with(Cell::get),
        max_size: MAX_ALLOC.with(Cell::get),
    };
    (r, a)
}

// ============================================================================
// Distributions
// ============================================================================

const N: usize = 100_000;

/// Anything this big is a data buffer, not Arc/schema bookkeeping.
const DATA_SIZED: usize = 4096;

fn row_len(i: usize) -> usize {
    i % 8
}

/// list<i64>: lengths cycle 0..8, values 0, 1, 2, ...
fn gen_list<B: Backend>() -> impl Distribution<B, Item = Vec<i64>>
where
    <usize as DefaultSink<B>>::Sink: Sink<usize>,
    <i64 as DefaultSink<B>>::Sink: Sink<i64>,
{
    list(map(range(0usize, 1), row_len), range(0i64, 1))
}

/// list<{ id: i32, tags: list<i64> }>: 2 structs per row, tag lens cycle 0..8
fn gen_list_of_struct_of_list() -> impl Distribution<ArrowBackend, Item = Vec<(i32, Vec<i64>)>> {
    list(
        constant(2usize),
        (
            range(0i32, 1),
            list(map(range(0usize, 1), row_len), range(0i64, 1)),
        ),
    )
}

fn expected_offsets(rows: usize) -> Vec<i32> {
    let mut out = Vec::with_capacity(rows + 1);
    out.push(0);
    for i in 0..rows {
        out.push(out[i] + row_len(i) as i32);
    }
    out
}

fn collect<B: Backend, D: Distribution<B>>(d: &mut D, n: usize) -> <D::Item as DefaultSink<B>>::Sink
where
    <D::Item as DefaultSink<B>>::Sink: Default,
{
    d.collect_into(Default::default(), &mut rand::rng(), n)
}

fn to_batch<S: IntoArrowColumns>(sink: S) -> RecordBatch {
    into_record_batch(sink).expect("valid record batch")
}

type ArrowList<V> = HCons<ListCol<ArrowOffsets, V>, HNil>;

// ============================================================================
// Tests
// ============================================================================

#[test]
fn list_is_generated_into_two_buffers_and_moved_into_arrow() {
    let mut d = gen_list::<ArrowBackend>();
    let _ = rand::rng(); // init thread rng outside the measured region

    // 1. Generation: exactly two allocations (offsets, values), no growth.
    let (sink, gen_allocs): (ArrowList<HCons<Vec<i64>, HNil>>, _) =
        count_allocs(|| collect(&mut d, N));
    let offsets = expected_offsets(N);
    let total = *offsets.last().unwrap() as usize;
    assert_eq!(
        gen_allocs,
        Allocs {
            allocs: 2,
            reallocs: 0,
            max_size: total * size_of::<i64>(),
        }
    );
    let col = &sink.0;
    assert_eq!(col.offsets.as_slice(), offsets.as_slice());
    assert_eq!(col.values.0, (0..total as i64).collect::<Vec<_>>());
    // buffers are exactly sized: nothing over-allocated, nothing to shrink
    assert_eq!(col.values.0.capacity(), total);
    let offsets_ptr = col.offsets.as_slice().as_ptr();
    let values_ptr = col.values.0.as_ptr();

    // 2. Arrow conversion: no data-sized allocation, same memory.
    let (batch, conv_allocs) = count_allocs(|| to_batch(sink));
    assert_eq!(conv_allocs.reallocs, 0);
    assert!(
        conv_allocs.max_size < DATA_SIZED,
        "conversion copied data: {conv_allocs:?}"
    );

    let l: &ListArray = batch.column(0).as_list();
    assert_eq!(l.value_offsets(), offsets.as_slice());
    assert_eq!(l.value_offsets().as_ptr(), offsets_ptr);
    let values = l.values().as_primitive::<Int64Type>().values();
    assert_eq!(values.as_ptr(), values_ptr);
    assert_eq!(
        &values[..],
        (0..total as i64).collect::<Vec<_>>().as_slice()
    );
}

#[test]
fn nested_list_of_struct_is_zero_copy() {
    let mut d = gen_list_of_struct_of_list();
    let _ = rand::rng();

    // outer offsets, id values, inner offsets, tag values: one allocation each
    let (sink, gen_allocs) = count_allocs(|| collect(&mut d, N));
    assert_eq!((gen_allocs.allocs, gen_allocs.reallocs), (4, 0));

    let outer = &sink.0;
    let HCons(ids, HCons(tags, HNil)) = &outer.values;
    let ptrs = (
        outer.offsets.as_slice().as_ptr(),
        ids.as_ptr(),
        tags.offsets.as_slice().as_ptr(),
        tags.values.0.as_ptr(),
    );

    let (batch, conv_allocs) = count_allocs(|| to_batch(sink));
    assert_eq!(conv_allocs.reallocs, 0);
    assert!(
        conv_allocs.max_size < DATA_SIZED,
        "conversion copied data: {conv_allocs:?}"
    );

    let l: &ListArray = batch.column(0).as_list();
    let s: &StructArray = l.values().as_struct();
    let id_col = s.column(0).as_primitive::<Int32Type>().values();
    let tag_list: &ListArray = s.column(1).as_list();
    let tag_vals = tag_list.values().as_primitive::<Int64Type>().values();
    assert_eq!(
        (
            l.value_offsets().as_ptr(),
            id_col.as_ptr(),
            tag_list.value_offsets().as_ptr(),
            tag_vals.as_ptr(),
        ),
        ptrs
    );

    // values: outer rows are pairs, ids count up, tag lists follow row_len
    let outer_offsets: Vec<i32> = (0..=N as i32).map(|i| i * 2).collect();
    assert_eq!(l.value_offsets(), outer_offsets.as_slice());
    assert_eq!(
        &id_col[..],
        (0..2 * N as i32).collect::<Vec<_>>().as_slice()
    );
    let tag_offsets = expected_offsets(2 * N);
    assert_eq!(tag_list.value_offsets(), tag_offsets.as_slice());
    let total = *tag_offsets.last().unwrap() as i64;
    assert_eq!(&tag_vals[..], (0..total).collect::<Vec<_>>().as_slice());
}

/// Control: `VecBackend` stores `usize` end offsets, so conversion has to
/// build a new `i32` offsets buffer. The allocator must see that.
#[test]
fn vec_backend_offsets_are_copied_control() {
    let mut d = gen_list::<VecBackend>();
    let _ = rand::rng();
    let sink = collect(&mut d, N);

    let (batch, conv_allocs) = count_allocs(|| to_batch(sink));
    assert!(
        conv_allocs.max_size >= (N + 1) * size_of::<i32>(),
        "expected an offsets copy: {conv_allocs:?}"
    );
    let l: &ListArray = batch.column(0).as_list();
    assert_eq!(l.value_offsets(), expected_offsets(N).as_slice());
}

// ============================================================================
// Parquet (optional `parquet` feature)
// ============================================================================

#[cfg(feature = "parquet")]
mod parquet_out {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use populate::parquet::{DistributionParquet, write_columns, write_parquet};

    /// id: i64, tags: list<i64>
    fn gen_table() -> impl Distribution<ArrowBackend, Item = (i64, Vec<i64>)> {
        (range(0i64, 1), gen_list::<ArrowBackend>())
    }

    /// 64 KiB pages: the writer's working memory is a page, not a column.
    const PAGE: usize = 1 << 16;

    fn small_pages() -> WriterProperties {
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(PAGE)
            .build()
    }

    fn read_back(bytes: Vec<u8>) -> RecordBatch {
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .expect("parquet reader")
            .with_batch_size(2 * N)
            .build()
            .expect("build reader");
        let batch = reader.next().expect("one batch").expect("read");
        assert!(reader.next().is_none());
        batch
    }

    fn assert_table(batch: &RecordBatch) {
        assert_eq!(batch.num_rows(), N);
        let ids = batch.column(0).as_primitive::<Int64Type>().values();
        assert_eq!(&ids[..], (0..N as i64).collect::<Vec<_>>().as_slice());
        let l: &ListArray = batch.column(1).as_list();
        let offsets = expected_offsets(N);
        assert_eq!(l.value_offsets(), offsets.as_slice());
        let total = *offsets.last().unwrap() as i64;
        let vals = l.values().as_primitive::<Int64Type>().values();
        assert_eq!(&vals[..], (0..total).collect::<Vec<_>>().as_slice());
    }

    /// The zero-copy writer never allocates anything near the size of a
    /// column: values are encoded straight out of the generated buffers and
    /// pages stream to the writer, so peak allocation is bounded by the page
    /// size, not by N. And the file reads back to the generated data.
    #[test]
    fn list_to_parquet_without_copies() {
        let mut d = gen_table();
        let sink = collect(&mut d, N);
        let HCons(ids, HCons(tags, HNil)) = &sink;
        let data_bytes = size_of_val(ids.as_slice())
            + size_of_val(tags.values.0.as_slice())
            + size_of_val(tags.offsets.as_slice());
        let ptrs = (ids.as_ptr(), tags.values.0.as_ptr());

        let (_, allocs) =
            count_allocs(|| write_columns(&sink, std::io::sink(), Some(small_pages())).unwrap());
        assert!(
            allocs.max_size < 4 * PAGE,
            "writer allocated a column-sized buffer: {allocs:?}"
        );
        assert!(allocs.max_size * 8 < data_bytes);

        // nothing moved: the sink still owns the same buffers
        let HCons(ids, HCons(tags, HNil)) = &sink;
        assert_eq!((ids.as_ptr(), tags.values.0.as_ptr()), ptrs);

        let bytes = write_columns(&sink, Vec::new(), Some(small_pages())).unwrap();
        assert_table(&read_back(bytes));
    }

    /// `collect_parquet` = generate into `ArrowBackend` + `write_columns`.
    #[test]
    fn collect_parquet_roundtrip() {
        let mut d = gen_table();
        let bytes = d
            .collect_parquet(&mut rand::rng(), N, Vec::new(), None)
            .unwrap();
        assert_table(&read_back(bytes));
    }

    /// Control: `ArrowWriter` (used by `write_parquet`) builds per-value
    /// level/index arrays for list columns, so it does allocate column-sized
    /// buffers. This is why `write_columns` exists.
    #[test]
    fn arrow_writer_allocates_column_sized_buffers_control() {
        let mut d = gen_table();
        let batch = to_batch(collect(&mut d, N));
        let total = batch.column(1).as_list::<i32>().values().len();

        let (_, allocs) =
            count_allocs(|| write_parquet(&batch, std::io::sink(), Some(small_pages())).unwrap());
        assert!(
            allocs.max_size >= total * size_of::<i64>(),
            "expected ArrowWriter to materialise levels: {allocs:?}"
        );

        let bytes = write_parquet(&batch, Vec::new(), None).unwrap();
        assert_table(&read_back(bytes));
    }
}
