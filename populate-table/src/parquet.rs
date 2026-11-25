//! Parquet output for [`Chunks`] and [`Table`] (feature `parquet`).
//!
//! Generation *and* encoding run on all cores: each worker takes one chunk,
//! generates its batches and encodes every column chunk of its row group,
//! and the calling thread appends finished row groups to the file in order.
//! The file's contents are the same for any thread count.

use crate::chunks::{Chunks, MultiChunks};
use crate::sched::par_sources;
use crate::table::Table;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};
use populate_sink_parquet::encode::{DirectColumn, Rope, eligible};
use std::io::Write;

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
/// ([`par_sources`]), whatever their order in `srcs`.
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
    let chunks: Vec<u64> = srcs.iter().map(|s| s.num_chunks()).collect();

    type Encoded = Vec<(usize, Vec<Leaf>)>;
    let encode = |s: usize, g: u64| -> Result<Encoded, ParquetError> {
        let mut cols: Vec<Option<Group>> = (0..srcs[s].schemas().len()).map(|_| None).collect();
        let mut err = Ok(());
        srcs[s].chunk(g, &mut |k, batch| {
            if err.is_err() || batch.num_rows() == 0 {
                return;
            }
            let o = first[s] + k;
            err = (|| {
                let c = match &mut cols[k] {
                    Some(c) => c,
                    c => c.insert(Group::new(
                        &layouts[o],
                        factories[o].create_column_writers(g as usize)?,
                    )),
                };
                c.write(&schemas[o], &layouts[o], &batch)
            })();
        });
        err?;
        let mut out = Vec::new();
        for (k, c) in cols.into_iter().enumerate() {
            if let Some(c) = c {
                out.push((first[s] + k, c.close(&props)?));
            }
        }
        Ok(out)
    };
    // each output belongs to one source, so per-source order is file order
    par_sources(&chunks, threads, encode, |_, groups| {
        for (o, leaves) in groups? {
            let mut rg = files[o].next_row_group()?;
            for leaf in leaves {
                match leaf {
                    Leaf::Arrow(c) => c.append_to_row_group(&mut rg)?,
                    Leaf::Direct(bytes, close) => rg.append_column(&bytes, close)?,
                }
            }
            rg.close()?;
        }
        Ok::<(), ParquetError>(())
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

/// One row group's column writers: arrow-rs's or ours, per leaf.
struct Group {
    arrow: Vec<Option<ArrowColumnWriter>>,
    direct: Vec<Option<DirectColumn>>,
}

/// One encoded column chunk.
enum Leaf {
    Arrow(ArrowColumnChunk),
    Direct(Rope, ColumnCloseResult),
}

impl Group {
    fn new(layout: &Layout, writers: Vec<ArrowColumnWriter>) -> Self {
        let mut arrow = Vec::with_capacity(writers.len());
        let mut direct = Vec::with_capacity(writers.len());
        for (w, d) in writers.into_iter().zip(&layout.direct) {
            match d {
                Some(d) => {
                    arrow.push(None);
                    direct.push(Some(DirectColumn::new(d.clone())));
                }
                None => {
                    arrow.push(Some(w));
                    direct.push(None);
                }
            }
        }
        Self { arrow, direct }
    }

    fn write(
        &mut self,
        schema: &SchemaRef,
        layout: &Layout,
        batch: &RecordBatch,
    ) -> Result<(), ParquetError> {
        for ((field, array), leaves) in schema
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(&layout.leaves)
        {
            if let [l] = leaves[..]
                && let Some(d) = &mut self.direct[l]
            {
                d.write(array);
                continue;
            }
            for (leaf, &l) in compute_leaves(field, array)?.iter().zip(leaves) {
                self.arrow[l].as_mut().expect("arrow-rs leaf").write(leaf)?;
            }
        }
        Ok(())
    }

    fn close(self, props: &WriterProperties) -> Result<Vec<Leaf>, ParquetError> {
        self.arrow
            .into_iter()
            .zip(self.direct)
            .map(|(a, d)| match (a, d) {
                (_, Some(d)) => d.close(props).map(|(b, c)| Leaf::Direct(b, c)),
                (Some(a), None) => a.close().map(Leaf::Arrow),
                (None, None) => unreachable!("every leaf has a writer"),
            })
            .collect()
    }
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
