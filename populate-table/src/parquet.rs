//! Parquet output for [`Chunks`] and [`Table`] (feature `parquet`).
//!
//! Generation *and* encoding run on all cores: a worker generates one
//! chunk's batches, each field of its row group is then encoded as a job of
//! its own, and the calling thread appends finished row groups to the file
//! in order. The file's contents are the same for any thread count.

use crate::chunks::{Chunks, MultiChunks};
use crate::sched::{Schedule, resolve_threads};
use crate::table::Table;
use arrow_array::RecordBatch;
use arrow_schema::{Field, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};
use populate_sink_parquet::encode::{DirectColumn, Rope, eligible};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

/// Write `src` as one Parquet file, one row group per chunk, generating and
/// encoding on `threads` threads (0 = all cores). Returns the writer.
pub fn write_parquet<W: Write + Send>(
    src: &impl Chunks,
    writer: W,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<W, ParquetError> {
    let mut w = write_parquet_many(&[src as &dyn MultiChunks], vec![writer], threads, props)?;
    Ok(w.pop().expect("one writer"))
}

/// Write several sources on one shared pool of `threads` threads: every
/// chunk of every source is a job, so small and large tables keep all cores
/// busy together. A source may produce several tables ([`MultiChunks`]);
/// `writers` has one writer per output, sources in order, each source's
/// outputs in order. Each chunk becomes one row group in each output it
/// produced rows for. Sources with the most remaining work go first
/// ([`Schedule`]), whatever their order in `srcs`.
/// Returns the writers in order.
pub fn write_parquet_many<W: Write + Send>(
    srcs: &[&dyn MultiChunks],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
) -> Result<Vec<W>, ParquetError> {
    let props = props.unwrap_or_default();
    // output `first[s] + k` is output `k` of source `s`
    let mut first = Vec::with_capacity(srcs.len());
    let mut schemas: Vec<SchemaRef> = Vec::new();
    for s in srcs {
        first.push(schemas.len());
        schemas.extend(s.schemas());
    }
    assert_eq!(schemas.len(), writers.len(), "one writer per output");
    let mut files = Vec::with_capacity(schemas.len());
    let mut factories = Vec::with_capacity(schemas.len());
    let mut layouts = Vec::with_capacity(schemas.len());
    for (w, schema) in writers.into_iter().zip(&schemas) {
        let (file, factory) = ArrowWriter::try_new(w, schema.clone(), Some(props.clone()))?
            .into_serialized_writer()?;
        layouts.push(Layout::new(schema, file.schema_descr(), &props));
        files.push(file);
        factories.push(factory);
    }

    let ctx = Ctx {
        srcs,
        first: &first,
        schemas: &schemas,
        layouts: &layouts,
        factories: &factories,
        props: &props,
    };
    // each output belongs to one source, so per-source order is file order
    run(&ctx, threads, |groups| {
        for (o, leaves) in groups {
            let mut rg = files[o].next_row_group()?;
            for leaf in leaves {
                match leaf {
                    Leaf::Arrow(c) => c.append_to_row_group(&mut rg)?,
                    Leaf::Direct(bytes, close) => rg.append_column(&bytes, close)?,
                }
            }
            rg.close()?;
        }
        Ok(())
    })?;
    files.into_iter().map(|f| f.into_inner()).collect()
}

/// How a schema's fields map to Parquet leaf columns, and which leaves are
/// encoded by `populate_sink_parquet::encode` instead of arrow-rs.
struct Layout {
    /// Leaf column indexes of each field.
    leaves: Vec<Vec<usize>>,
    /// Per leaf: the descriptor, if it is encoded directly.
    direct: Vec<Option<ColumnDescPtr>>,
}

impl Layout {
    fn new(schema: &SchemaRef, descr: &SchemaDescriptor, props: &WriterProperties) -> Self {
        let mut leaves = vec![Vec::new(); schema.fields().len()];
        for l in 0..descr.num_columns() {
            leaves[descr.get_column_root_idx(l)].push(l);
        }
        let mut direct = vec![None; descr.num_columns()];
        for (f, field) in schema.fields().iter().enumerate() {
            if let [l] = leaves[f][..] {
                let d = descr.column(l);
                if eligible(field.data_type(), &d, props) {
                    direct[l] = Some(d);
                }
            }
        }
        Self { leaves, direct }
    }
}

/// One encoded column chunk.
enum Leaf {
    Arrow(ArrowColumnChunk),
    Direct(Rope, ColumnCloseResult),
}

/// The column writers of one field's leaves in one row group: arrow-rs's,
/// or ours for a flat column it covers.
enum FieldWriter {
    Arrow(Vec<ArrowColumnWriter>),
    Direct(DirectColumn),
}

impl FieldWriter {
    /// One writer per field of `layout`, from a row group's leaf writers.
    fn split(layout: &Layout, writers: Vec<ArrowColumnWriter>) -> Vec<Self> {
        let mut writers: Vec<Option<ArrowColumnWriter>> = writers.into_iter().map(Some).collect();
        layout
            .leaves
            .iter()
            .map(|leaves| match leaves[..] {
                [l] if layout.direct[l].is_some() => {
                    let d = layout.direct[l].clone().expect("direct leaf");
                    FieldWriter::Direct(DirectColumn::new(d))
                }
                _ => FieldWriter::Arrow(
                    leaves
                        .iter()
                        .map(|&l| writers[l].take().expect("each leaf once"))
                        .collect(),
                ),
            })
            .collect()
    }

    /// Encode field `f` of `batch`.
    fn write(&mut self, field: &Field, f: usize, batch: &RecordBatch) -> Result<(), ParquetError> {
        match self {
            FieldWriter::Direct(d) => d.write(batch.column(f)),
            FieldWriter::Arrow(ws) => {
                for (leaf, w) in compute_leaves(field, batch.column(f))?.iter().zip(ws) {
                    w.write(leaf)?;
                }
            }
        }
        Ok(())
    }

    /// Close the field's column chunks.
    fn close(self, props: &WriterProperties) -> Result<Vec<Leaf>, ParquetError> {
        match self {
            FieldWriter::Direct(d) => {
                let (bytes, close) = d.close(props)?;
                Ok(vec![Leaf::Direct(bytes, close)])
            }
            FieldWriter::Arrow(ws) => ws.into_iter().map(|w| w.close().map(Leaf::Arrow)).collect(),
        }
    }

    /// Encode field `f` of every batch and close its column chunks.
    fn encode(
        mut self,
        field: &Field,
        f: usize,
        batches: &[RecordBatch],
        props: &WriterProperties,
    ) -> Result<Vec<Leaf>, ParquetError> {
        for b in batches {
            self.write(field, f, b)?;
        }
        self.close(props)
    }
}

/// What every worker of [`write_parquet_many`] reads.
struct Ctx<'a> {
    srcs: &'a [&'a dyn MultiChunks],
    /// Output index of each source's first output.
    first: &'a [usize],
    schemas: &'a [SchemaRef],
    layouts: &'a [Layout],
    factories: &'a [ArrowRowGroupWriterFactory],
    props: &'a WriterProperties,
}

/// One field of one output of one chunk, waiting to be encoded.
struct Encode {
    s: usize,
    g: u64,
    /// Position in the chunk's [`Open::outputs`], the output, the field.
    k: usize,
    o: usize,
    f: usize,
    batches: Arc<Vec<RecordBatch>>,
    writer: FieldWriter,
}

/// A chunk whose fields are being encoded.
struct Open {
    /// Fields not yet encoded.
    left: usize,
    /// Seconds spent on the chunk so far, for the [`Schedule`].
    secs: f64,
    /// Per output of the source that has rows: the output and each field's
    /// leaves.
    outputs: Vec<(usize, Vec<Option<Vec<Leaf>>>)>,
}

/// Shared state of [`run`]'s workers.
struct State {
    schedule: Schedule,
    ready: VecDeque<Encode>,
    open: HashMap<(usize, u64), Open>,
    /// Chunks generated (or being generated) and not yet consumed.
    in_flight: usize,
    /// Jobs running.
    busy: usize,
    /// Per source: whether its chunks are big enough to split by field.
    big: Vec<bool>,
    /// Chunks not yet started.
    unpicked: u64,
    failed: Option<ParquetError>,
}

enum Job {
    /// Generate and encode a whole chunk.
    Whole(usize, u64),
    /// Generate a chunk and queue its fields.
    Generate(usize, u64),
    Encode(Encode),
}

/// What a job leaves behind.
enum Done {
    /// A whole chunk and its Arrow bytes.
    Whole(usize, u64, Encoded, usize),
    Generated(usize, u64, Vec<Encode>, Open),
    /// Source, chunk, position in its `Open::outputs`, field, leaves.
    Encoded(usize, u64, usize, usize, Vec<Leaf>),
}

type Encoded = Vec<(usize, Vec<Leaf>)>;

/// Generate chunk `g` of source `s`: its batches per output, and one
/// [`Encode`] per field of each output with rows.
fn generate(ctx: &Ctx, s: usize, g: u64) -> Result<(Vec<Encode>, Open), ParquetError> {
    let n = ctx.srcs[s].schemas().len();
    let mut batches: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];
    ctx.srcs[s].chunk(g, &mut |k, b| {
        if b.num_rows() > 0 {
            batches[k].push(b);
        }
    });
    let mut jobs = Vec::new();
    let mut outputs = Vec::new();
    for (k, b) in batches.into_iter().enumerate() {
        if b.is_empty() {
            continue;
        }
        let o = ctx.first[s] + k;
        let writers = ctx.factories[o].create_column_writers(g as usize)?;
        let fields = FieldWriter::split(&ctx.layouts[o], writers);
        let b = Arc::new(b);
        outputs.push((o, (0..fields.len()).map(|_| None).collect()));
        for (f, writer) in fields.into_iter().enumerate() {
            jobs.push(Encode {
                s,
                g,
                k: outputs.len() - 1,
                o,
                f,
                batches: b.clone(),
                writer,
            });
        }
    }
    let open = Open {
        left: jobs.len(),
        secs: 0.0,
        outputs,
    };
    Ok((jobs, open))
}

/// Generate chunk `g` of source `s` and encode each batch as it comes, on
/// this thread; returns the encoded chunk and its Arrow bytes.
fn generate_whole(ctx: &Ctx, s: usize, g: u64) -> Result<(Encoded, usize), ParquetError> {
    let n = ctx.srcs[s].schemas().len();
    let mut cols: Vec<Option<Vec<FieldWriter>>> = (0..n).map(|_| None).collect();
    let mut bytes = 0;
    let mut err: Result<(), ParquetError> = Ok(());
    ctx.srcs[s].chunk(g, &mut |k, batch| {
        if err.is_err() || batch.num_rows() == 0 {
            return;
        }
        bytes += batch.get_array_memory_size();
        let o = ctx.first[s] + k;
        err = (|| {
            let fields = match &mut cols[k] {
                Some(c) => c,
                c => c.insert(FieldWriter::split(
                    &ctx.layouts[o],
                    ctx.factories[o].create_column_writers(g as usize)?,
                )),
            };
            let schema = &ctx.schemas[o];
            for (f, w) in fields.iter_mut().enumerate() {
                w.write(schema.field(f), f, &batch)?;
            }
            Ok(())
        })();
    });
    err?;
    let mut out = Vec::new();
    for (k, c) in cols.into_iter().enumerate() {
        if let Some(fields) = c {
            let mut leaves = Vec::new();
            for w in fields {
                leaves.extend(w.close(ctx.props)?);
            }
            out.push((ctx.first[s] + k, leaves));
        }
    }
    Ok((out, bytes))
}

/// Sources whose chunks hold fewer Arrow bytes than this are encoded by the
/// worker that generates them, batch by batch as before any split: holding
/// a chunk's batches until its fields are encoded costs page faults, and a
/// field job a lock round-trip and another core's cache, which outweigh the
/// balance they buy on a small chunk (TSBS writes 1,800-row chunks of nine
/// tables).
const SPLIT_BYTES: usize = 8 << 20;

/// Chunks per thread, at the end of the run, that are split by field.
const TAIL: u64 = 2;

fn finish(open: Open) -> Encoded {
    open.outputs
        .into_iter()
        .map(|(o, fields)| {
            let leaves = fields
                .into_iter()
                .flat_map(|l| l.expect("every field encoded"))
                .collect();
            (o, leaves)
        })
        .collect()
}

/// Generate every chunk of every source and encode each of its fields as a
/// job of its own, on `threads` threads (0 = all cores), and call `consume`
/// with each chunk's column chunks, in chunk order per source, on the
/// calling thread.
///
/// A chunk is one row group, and a row group of a big table is ~100 ms of
/// work: with whole chunks as jobs the cores finish up to a chunk apart.
/// Encoding is most of a chunk's cost and its fields are independent column
/// chunks, so they run as separate jobs, and a free core takes a field to
/// encode before it starts a new chunk. Chunks to generate are picked by
/// [`Schedule`], as in [`par_sources`](crate::par_sources). Every column
/// chunk is a function of its chunk's batches, so the file is the same for
/// any thread count and any order.
fn run(
    ctx: &Ctx,
    threads: usize,
    mut consume: impl FnMut(Encoded) -> Result<(), ParquetError>,
) -> Result<(), ParquetError> {
    let chunks: Vec<u64> = ctx.srcs.iter().map(|s| s.num_chunks()).collect();
    let jobs: u64 = chunks.iter().sum();
    let threads = resolve_threads(threads).min(jobs.max(1) as usize);
    if threads <= 1 {
        for (s, &n) in chunks.iter().enumerate() {
            for g in 0..n {
                consume(generate_whole(ctx, s, g)?.0)?;
            }
        }
        return Ok(());
    }
    let state = Mutex::new(State {
        schedule: Schedule::new(&chunks),
        ready: VecDeque::new(),
        open: HashMap::new(),
        in_flight: 0,
        busy: 0,
        big: vec![false; chunks.len()],
        unpicked: jobs,
        failed: None,
    });
    let wake = Condvar::new();
    let limit = 3 * threads;
    thread::scope(|scope| {
        let (tx, rx) = channel::<(usize, u64, Encoded)>();
        for _ in 0..threads {
            let tx = tx.clone();
            let (state, wake) = (&state, &wake);
            scope.spawn(move || {
                let mut st = state.lock().expect("state");
                loop {
                    let job = if st.failed.is_some() {
                        break;
                    } else if let Some(e) = st.ready.pop_front() {
                        Job::Encode(e)
                    } else if st.in_flight < limit
                        && let Some((s, g)) = st.schedule.pick()
                    {
                        st.in_flight += 1;
                        st.unpicked -= 1;
                        // only the last chunks of the run are split, and only
                        // of sources a whole chunk showed big
                        if st.big[s] && st.unpicked < threads as u64 * TAIL {
                            Job::Generate(s, g)
                        } else {
                            Job::Whole(s, g)
                        }
                    } else if st.busy == 0 && st.in_flight == 0 {
                        // nothing running, queued or left to start
                        break;
                    } else {
                        st = wake.wait(st).expect("state");
                        continue;
                    };
                    st.busy += 1;
                    drop(st);
                    let t0 = Instant::now();
                    let done = match job {
                        Job::Whole(s, g) => {
                            generate_whole(ctx, s, g).map(|(e, bytes)| Done::Whole(s, g, e, bytes))
                        }
                        Job::Generate(s, g) => generate(ctx, s, g)
                            .map(|(jobs, open)| Done::Generated(s, g, jobs, open)),
                        Job::Encode(e) => {
                            let field = ctx.schemas[e.o].field(e.f);
                            e.writer
                                .encode(field, e.f, &e.batches, ctx.props)
                                .map(|l| Done::Encoded(e.s, e.g, e.k, e.f, l))
                        }
                    };
                    let secs = t0.elapsed().as_secs_f64();
                    st = state.lock().expect("state");
                    st.busy -= 1;
                    let finished = match done {
                        Err(e) => {
                            st.failed.get_or_insert(e);
                            None
                        }
                        Ok(Done::Whole(s, g, encoded, bytes)) => {
                            st.big[s] |= bytes >= SPLIT_BYTES;
                            st.schedule.finished(s, secs);
                            // the receiver only goes away once a worker failed
                            let _ = tx.send((s, g, encoded));
                            None
                        }
                        Ok(Done::Generated(s, g, jobs, mut open)) => {
                            open.secs += secs;
                            if jobs.is_empty() {
                                Some((s, g, open))
                            } else {
                                st.open.insert((s, g), open);
                                st.ready.extend(jobs);
                                None
                            }
                        }
                        Ok(Done::Encoded(s, g, k, f, leaves)) => {
                            let open = st.open.get_mut(&(s, g)).expect("open chunk");
                            open.secs += secs;
                            open.outputs[k].1[f] = Some(leaves);
                            open.left -= 1;
                            if open.left == 0 {
                                st.open.remove(&(s, g)).map(|open| (s, g, open))
                            } else {
                                None
                            }
                        }
                    };
                    if let Some((s, g, open)) = finished {
                        st.schedule.finished(s, open.secs);
                        // the receiver only goes away once a worker failed
                        let _ = tx.send((s, g, finish(open)));
                    }
                    wake.notify_all();
                }
                drop(st);
                wake.notify_all();
            });
        }
        drop(tx);
        // Reorder per source; a consumed chunk frees a slot for a new one.
        let mut pending: Vec<BTreeMap<u64, Encoded>> =
            chunks.iter().map(|_| BTreeMap::new()).collect();
        let mut want = vec![0u64; chunks.len()];
        let mut result = Ok(());
        'recv: for (s, g, groups) in rx.iter() {
            pending[s].insert(g, groups);
            while let Some(groups) = pending[s].remove(&want[s]) {
                want[s] += 1;
                if let Err(e) = consume(groups) {
                    result = Err(e);
                    break 'recv;
                }
                state.lock().expect("state").in_flight -= 1;
                wake.notify_all();
            }
        }
        let mut st = state.lock().expect("state");
        if result.is_err() {
            // stop the workers; they never block on the channel
            st.failed
                .get_or_insert(ParquetError::General("stopped".into()));
            wake.notify_all();
            return result;
        }
        match st.failed.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
}

/// A [`Table`] as row groups of `per_group` batches.
struct TableGroups<'a> {
    table: &'a Table,
    batch_size: usize,
    per_group: u64,
}

impl Chunks for TableGroups<'_> {
    fn schema(&self) -> SchemaRef {
        self.table.schema().clone()
    }

    fn num_chunks(&self) -> u64 {
        self.table
            .num_batches(self.batch_size)
            .div_ceil(self.per_group)
    }

    fn chunk(&self, g: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let batches = self.table.num_batches(self.batch_size);
        for i in g * self.per_group..((g + 1) * self.per_group).min(batches) {
            emit(self.table.batch(i, self.batch_size));
        }
    }
}

impl Table {
    /// Generate the table on `threads` threads (0 = all cores) and write it
    /// as one Parquet file. Row groups hold `props.max_row_group_row_count()`
    /// rows (default 1Mi) rounded up to whole batches. Returns the writer.
    pub fn write_parquet<W: Write + Send>(
        &self,
        writer: W,
        batch_size: usize,
        threads: usize,
        props: Option<WriterProperties>,
    ) -> Result<W, ParquetError> {
        let props = props.unwrap_or_default();
        let rows = props.max_row_group_row_count().unwrap_or(1024 * 1024);
        let groups = TableGroups {
            table: self,
            batch_size,
            per_group: (rows / batch_size).max(1) as u64,
        };
        write_parquet(&groups, writer, threads, Some(props))
    }
}
