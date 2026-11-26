//! Parquet output for [`Chunks`] and [`Table`] (feature `parquet`).
//!
//! Generation *and* encoding run on all cores: each worker takes one chunk,
//! generates its batches and encodes every column chunk of its row group,
//! and the calling thread appends finished row groups to the file in order.
//! The file's contents are the same for any thread count.

use crate::chunks::{Chunks, MultiChunks};
use crate::sched::par_sources;
use crate::table::Table;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use arrow_select::concat::concat;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};
use populate_sink_parquet::encode::{DirectColumn, Rope, eligible};
use std::io::Write;
use std::mem::take;

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
    write_many(srcs, writers, threads, props, None)
}

/// [`write_parquet_many`], with the files it writes when every chunk's
/// batches are cut into batches of `batch_rows[s]` rows for source `s`
/// (the last of each chunk shorter), whatever batches the chunks emit: Parquet pages end
/// where the writer's batches end, so this makes a file independent of
/// how a source batches its rows. The directly encoded flat columns are
/// paged as if cut, without copying; any other column's arrays are cut
/// (and joined across batches) before they are written.
pub fn write_parquet_many_in_batches<W: Write + Send>(
    srcs: &[&dyn MultiChunks],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
    batch_rows: &[usize],
) -> Result<Vec<W>, ParquetError> {
    assert_eq!(batch_rows.len(), srcs.len(), "a batch size per source");
    write_many(srcs, writers, threads, props, Some(batch_rows))
}

fn write_many<W: Write + Send>(
    srcs: &[&dyn MultiChunks],
    writers: Vec<W>,
    threads: usize,
    props: Option<WriterProperties>,
    batch_rows: Option<&[usize]>,
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
                        batch_rows.map(|b| b[s].max(1)),
                    )),
                };
                c.write(&schemas[o], &layouts[o], &batch)
            })();
        });
        err?;
        let mut out = Vec::new();
        for (k, c) in cols.into_iter().enumerate() {
            if let Some(c) = c {
                let o = first[s] + k;
                out.push((o, c.close(&schemas[o], &layouts[o], &props)?));
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
    /// Per field: whether it is one directly encoded flat leaf, which
    /// [`DirectColumn::with_batch_rows`] pages as if its arrays were cut.
    paged_as_cut: Vec<bool>,
}

impl Layout {
    fn new(schema: &SchemaRef, descr: &SchemaDescriptor, props: &WriterProperties) -> Self {
        let mut leaves = vec![Vec::new(); schema.fields().len()];
        for l in 0..descr.num_columns() {
            leaves[descr.get_column_root_idx(l)].push(l);
        }
        let mut direct = vec![None; descr.num_columns()];
        let mut paged_as_cut = vec![false; schema.fields().len()];
        for (f, field) in schema.fields().iter().enumerate() {
            if let [l] = leaves[f][..] {
                let d = descr.column(l);
                if eligible(field.data_type(), &d, props) {
                    direct[l] = Some(d);
                    paged_as_cut[f] = !matches!(
                        field.data_type(),
                        DataType::List(_) | DataType::FixedSizeList(..)
                    );
                }
            }
        }
        Self {
            leaves,
            direct,
            paged_as_cut,
        }
    }
}

/// One row group's column writers: arrow-rs's or ours, per leaf.
struct Group {
    arrow: Vec<Option<ArrowColumnWriter>>,
    direct: Vec<Option<DirectColumn>>,
    /// With batches of `n` rows ([`write_parquet_many_in_batches`]): per
    /// field, its arrays not yet written, if the field's arrays must be
    /// cut before they are written (it is not paged as if cut).
    cut: Option<(usize, Vec<Option<Pending>>)>,
}

/// A field's arrays waiting to be cut into batches of `n` rows.
#[derive(Default)]
struct Pending {
    arrays: Vec<ArrayRef>,
    rows: usize,
}

/// One encoded column chunk.
enum Leaf {
    Arrow(ArrowColumnChunk),
    Direct(Rope, ColumnCloseResult),
}

impl Group {
    fn new(layout: &Layout, writers: Vec<ArrowColumnWriter>, batch_rows: Option<usize>) -> Self {
        let mut arrow = Vec::with_capacity(writers.len());
        let mut direct = Vec::with_capacity(writers.len());
        for (w, d) in writers.into_iter().zip(&layout.direct) {
            match d {
                Some(d) => {
                    arrow.push(None);
                    let c = DirectColumn::new(d.clone());
                    direct.push(Some(match batch_rows {
                        Some(n) => c.with_batch_rows(n),
                        None => c,
                    }));
                }
                None => {
                    arrow.push(Some(w));
                    direct.push(None);
                }
            }
        }
        let cut = batch_rows.map(|n| {
            let pending = (0..layout.leaves.len())
                .map(|f| (!layout.paged_as_cut[f]).then(Pending::default))
                .collect();
            (n, pending)
        });
        Self { arrow, direct, cut }
    }

    fn write(
        &mut self,
        schema: &SchemaRef,
        layout: &Layout,
        batch: &RecordBatch,
    ) -> Result<(), ParquetError> {
        for (f, array) in batch.columns().iter().enumerate() {
            let Some((n, pending)) = &mut self.cut else {
                self.write_field(schema, layout, f, array)?;
                continue;
            };
            let n = *n;
            let Some(p) = &mut pending[f] else {
                self.write_field(schema, layout, f, array)?;
                continue;
            };
            p.arrays.push(array.clone());
            p.rows += array.len();
            if p.rows >= n {
                let all = take(&mut p.arrays);
                let rows = take(&mut p.rows);
                let all = join(&all)?;
                let whole = rows / n * n;
                if whole < rows {
                    p.arrays.push(all.slice(whole, rows - whole));
                    p.rows = rows - whole;
                }
                for at in (0..whole).step_by(n) {
                    self.write_field(schema, layout, f, &all.slice(at, n))?;
                }
            }
        }
        Ok(())
    }

    /// Field `f`'s `array`, to its leaves' writers.
    fn write_field(
        &mut self,
        schema: &SchemaRef,
        layout: &Layout,
        f: usize,
        array: &ArrayRef,
    ) -> Result<(), ParquetError> {
        let leaves = &layout.leaves[f];
        if let [l] = leaves[..]
            && let Some(d) = &mut self.direct[l]
        {
            d.write(array);
            return Ok(());
        }
        for (leaf, &l) in compute_leaves(schema.field(f), array)?.iter().zip(leaves) {
            self.arrow[l].as_mut().expect("arrow-rs leaf").write(leaf)?;
        }
        Ok(())
    }

    fn close(
        mut self,
        schema: &SchemaRef,
        layout: &Layout,
        props: &WriterProperties,
    ) -> Result<Vec<Leaf>, ParquetError> {
        // the rest of each cut field: the chunk's last, shorter batch
        if let Some((_, pending)) = self.cut.take() {
            for (f, p) in pending.into_iter().enumerate() {
                if let Some(p) = p.filter(|p| p.rows > 0) {
                    self.write_field(schema, layout, f, &join(&p.arrays)?)?;
                }
            }
        }
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

/// `arrays` as one array (the one itself, uncopied, if there is one).
fn join(arrays: &[ArrayRef]) -> Result<ArrayRef, ParquetError> {
    match arrays {
        [a] => Ok(a.clone()),
        _ => {
            let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
            Ok(concat(&refs)?)
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::{Int32Array, StringViewArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    /// Two chunks of `rows` rows each: a nullable Int32 and Utf8View
    /// (encoded directly) and a nullable List (by arrow-rs), emitted in
    /// batches of the lengths `cuts` cycles through.
    struct Rows {
        rows: usize,
        cuts: Vec<usize>,
    }

    fn schema() -> SchemaRef {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8View, true),
            Field::new("l", list, true),
        ]))
    }

    fn batch(rows: std::ops::Range<usize>) -> RecordBatch {
        let i: Int32Array = rows
            .clone()
            .map(|r| (r % 7 != 0).then_some((r * 37 % 1000) as i32))
            .collect();
        let s: StringViewArray = rows
            .clone()
            .map(|r| (r % 5 != 0).then(|| format!("value number {}", r % 300)))
            .collect();
        let mut l = ListBuilder::new(Int32Builder::new());
        for r in rows {
            if r % 3 == 0 {
                l.append_null();
            } else {
                l.append_value((0..r % 4).map(|k| Some((r + k) as i32)));
            }
        }
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(i), Arc::new(s), Arc::new(l.finish())],
        )
        .unwrap()
    }

    impl Chunks for Rows {
        fn schema(&self) -> SchemaRef {
            schema()
        }

        fn num_chunks(&self) -> u64 {
            2
        }

        fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
            let (mut at, end) = (i as usize * self.rows, (i as usize + 1) * self.rows);
            for &n in self.cuts.iter().cycle() {
                if at == end {
                    break;
                }
                let n = n.min(end - at);
                emit(batch(at..at + n));
                at += n;
            }
        }
    }

    fn file(src: &Rows, batch_rows: Option<usize>) -> Vec<u8> {
        let props = WriterProperties::builder()
            .set_write_batch_size(100)
            .set_data_page_row_count_limit(250)
            .build();
        let srcs = [src as &dyn MultiChunks];
        let mut out = match batch_rows {
            None => write_parquet_many(&srcs, vec![Vec::new()], 2, Some(props)),
            Some(n) => write_parquet_many_in_batches(&srcs, vec![Vec::new()], 2, Some(props), &[n]),
        }
        .unwrap();
        out.pop().unwrap()
    }

    #[test]
    fn in_batches_is_the_file_of_those_batches() {
        let rows = 2_345;
        for n in [100, 300, 512] {
            let want = file(
                &Rows {
                    rows,
                    cuts: vec![n],
                },
                None,
            );
            let ragged = Rows {
                rows,
                cuts: vec![1, 777, 64, 1_000, 33],
            };
            // as they come, the batches would end pages elsewhere
            assert_ne!(file(&ragged, None), want, "batches of {n}");
            assert_eq!(file(&ragged, Some(n)), want, "batches of {n}");
        }
    }
}
