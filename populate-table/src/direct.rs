//! Parquet straight from a [`Distribution`]'s collected buffers, with no
//! Arrow in between (feature `parquet`).
//!
//! [`write_dist_parquet`] splits `rows` into row groups of
//! [`ParquetOptions::rows_per_group`]. A worker builds row group `g` on its
//! own: it clones the distribution, [`skip`](Distribution::skip)s it to the
//! group's first row, collects the group with an RNG seeded from
//! `(seed, g)`, and encodes every column chunk from the collected `Vec`s
//! where they are. The calling thread appends finished row groups to the
//! file in order ([`par_ordered`]), so the file is the same for any thread
//! count.
//!
//! The columns, their schema and their encoding are
//! `populate-sink-parquet`'s ([`ParquetColumns`], [`encode_leaf`]): the
//! same encoder as [`Table::write_parquet`](crate::Table::write_parquet),
//! and the same columns as its single-threaded `write_columns`. This
//! module adds the row groups and the threads.

use crate::sched::{par_map, par_ordered, resolve_threads};
use crate::stream::mix64;
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::ColumnDescPtr;
use populate_core::{Columns, Distribution};
use populate_sink_parquet::{Chunk, ParquetColumns, encode_leaf, leaves, schema};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::io::Write;
use std::sync::Arc;

/// How [`write_dist_parquet`] splits and encodes its rows.
#[derive(Debug, Clone)]
pub struct ParquetOptions {
    /// Rows per row group (the last may be shorter). Part of the data's
    /// identity, like the seed: each group has its own RNG.
    pub rows_per_group: usize,
    /// Worker threads, 0 for all cores. Does not change the output.
    pub threads: usize,
    /// Seeds every row group's RNG, with the group's index.
    pub seed: u64,
    /// Parquet writer settings: compression, dictionaries, page sizes.
    pub props: WriterProperties,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            rows_per_group: 1 << 20,
            threads: 0,
            seed: 0,
            props: WriterProperties::default(),
        }
    }
}

/// The RNG of row group `g`.
pub fn group_rng(seed: u64, g: u64) -> SmallRng {
    SmallRng::seed_from_u64(mix64(seed ^ mix64(g)))
}

/// Write `rows` rows of `dist` as one Parquet file and return the writer.
/// Row groups are generated and encoded on `opts.threads` threads and
/// appended in order, so the file is the same for any thread count.
pub fn write_dist_parquet<D, W>(dist: &D, rows: u64, writer: W, opts: &ParquetOptions) -> Result<W>
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: ParquetColumns,
    W: Write + Send,
{
    let schema = schema::<Columns<D::Item>>()?;
    let props: WriterPropertiesPtr = Arc::new(opts.props.clone());
    let mut file = SerializedFileWriter::new(writer, schema, props.clone())?;
    let descrs: Vec<ColumnDescPtr> = file.schema_descr().columns().to_vec();
    let per = opts.rows_per_group.max(1) as u64;
    let groups = rows.div_ceil(per);
    let threads = resolve_threads(opts.threads);
    let work = |g: u64| -> Result<Vec<Chunk>> {
        let start = g * per;
        let len = (rows - start).min(per) as usize;
        let mut d = dist.clone();
        d.skip(start);
        let cols = d.collect(&mut group_rng(opts.seed, g), len);
        let leaves = leaves(&cols, len)?;
        // fewer row groups than threads: the spare threads share the
        // group's columns
        let leaf_threads = (threads / groups.max(1) as usize).max(1);
        par_map(leaves.len(), leaf_threads, |i| {
            encode_leaf(&leaves[i], &descrs[i], &props)
        })
        .into_iter()
        .collect()
    };
    par_ordered(groups, threads, work, |chunks| {
        let mut rg = file.next_row_group()?;
        for (chunk, close) in chunks? {
            rg.append_column(&chunk, close)?;
        }
        rg.close()?;
        Ok::<(), ParquetError>(())
    })?;
    file.into_inner()
}
