//! Parquet output (optional `parquet` feature).
//!
//! Two writers:
//!
//! - [`write_columns`] / [`DistributionParquet::collect_parquet`]: zero-copy
//!   path for the `ArrowBackend` sink of top-level numeric columns and
//!   `list<numeric>` columns. Value buffers are handed to Parquet's column
//!   writer as slices of the buffer they were generated into, and the
//!   definition/repetition levels a list needs are derived from the offsets
//!   in a small reusable scratch buffer, one chunk of rows at a time. Pages
//!   stream straight to the `Write`r, so nothing data-sized is ever allocated
//!   besides the generated buffers themselves.
//! - [`write_parquet`]: any `RecordBatch` via `ArrowWriter`. Primitive values
//!   are still read in place, but `ArrowWriter` materialises per-value level
//!   and index arrays for list columns and buffers a whole row group in
//!   memory, so it is not zero-copy.

use crate::arrow::{ArrowBackend, ArrowOffsets};
use crate::hlist::{HCons, HNil};
use crate::traits::{DefaultSink, Distribution, ListCol};
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use parquet::data_type::{DataType, DoubleType, FloatType, Int32Type, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::types::{Type, TypePtr};
use rand::Rng;
use std::io::Write;
use std::iter::repeat_n;
use std::sync::Arc;

type Result<T> = std::result::Result<T, ParquetError>;

/// Max level entries per `write_batch` call on a list column (a single row
/// longer than this is written in one call of its own length).
const LEVEL_CHUNK: usize = 4096;

// ============================================================================
// ArrowWriter path (any RecordBatch)
// ============================================================================

/// Write one `RecordBatch` as a complete Parquet file and return the writer.
pub fn write_parquet<W: Write + Send>(
    batch: &RecordBatch,
    writer: W,
    props: Option<WriterProperties>,
) -> Result<W> {
    let mut w = ArrowWriter::try_new(writer, batch.schema(), props)?;
    w.write(batch)?;
    w.into_inner()
}

// ============================================================================
// Zero-copy column writer
// ============================================================================

/// Rust native -> Parquet physical type.
pub trait ParquetNative: Copy {
    type Parquet: DataType<T = Self>;
    const PHYSICAL: PhysicalType;
}

macro_rules! impl_parquet_native {
    ($($t:ty => $p:ty, $phys:ident);* $(;)?) => {
        $(impl ParquetNative for $t {
            type Parquet = $p;
            const PHYSICAL: PhysicalType = PhysicalType::$phys;
        })*
    };
}

impl_parquet_native!(
    i32 => Int32Type, INT32;
    i64 => Int64Type, INT64;
    f32 => FloatType, FLOAT;
    f64 => DoubleType, DOUBLE;
);

/// One sink column that can be written to Parquet without copying its data.
pub trait ParquetColumn {
    fn parquet_type(name: &str) -> Result<TypePtr>;
    fn write_column<W: Write + Send>(&self, rg: &mut SerializedRowGroupWriter<'_, W>)
    -> Result<()>;
    fn num_rows(&self) -> usize;
}

fn leaf<T: ParquetNative>(name: &str, repetition: Repetition) -> Result<TypePtr> {
    Ok(Arc::new(
        Type::primitive_type_builder(name, T::PHYSICAL)
            .with_repetition(repetition)
            .build()?,
    ))
}

fn next_column<'a, W: Write + Send>(
    rg: &'a mut SerializedRowGroupWriter<'_, W>,
) -> Result<parquet::file::writer::SerializedColumnWriter<'a>> {
    rg.next_column()?
        .ok_or_else(|| ParquetError::General("schema has fewer columns than the sink".into()))
}

/// Required numeric column: the whole buffer goes to the encoder as one slice.
impl<T: ParquetNative> ParquetColumn for Vec<T> {
    fn parquet_type(name: &str) -> Result<TypePtr> {
        leaf::<T>(name, Repetition::REQUIRED)
    }

    fn write_column<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        let mut col = next_column(rg)?;
        col.typed::<T::Parquet>().write_batch(self, None, None)?;
        col.close()
    }

    fn num_rows(&self) -> usize {
        self.len()
    }
}

/// Required `list<required numeric>`, standard 3-level LIST layout
/// (`max_def = 1`, `max_rep = 1`). Values are sliced from the generated
/// buffer; levels come from the offsets, `LEVEL_CHUNK` at a time.
impl<T: ParquetNative> ParquetColumn for ListCol<ArrowOffsets, HCons<Vec<T>, HNil>> {
    fn parquet_type(name: &str) -> Result<TypePtr> {
        let list = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![leaf::<T>("element", Repetition::REQUIRED)?])
            .build()?;
        Ok(Arc::new(
            Type::group_type_builder(name)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::List))
                .with_fields(vec![Arc::new(list)])
                .build()?,
        ))
    }

    fn write_column<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        let mut col = next_column(rg)?;
        let writer = col.typed::<T::Parquet>();
        let values = &self.values.0;
        let offsets = self.offsets.as_slice();
        let mut def = Vec::with_capacity(LEVEL_CHUNK);
        let mut rep = Vec::with_capacity(LEVEL_CHUNK);
        let mut v_start = 0usize;
        let mut v_end = 0usize;

        for w in offsets.windows(2) {
            let (start, end) = (w[0] as usize, w[1] as usize);
            // levels for this row: an empty list is one (def 0, rep 0) entry
            let row_levels = (end - start).max(1);
            if !def.is_empty() && def.len() + row_levels > LEVEL_CHUNK {
                writer.write_batch(&values[v_start..v_end], Some(&def), Some(&rep))?;
                def.clear();
                rep.clear();
                v_start = v_end;
            }
            if start == end {
                def.push(0);
                rep.push(0);
            } else {
                def.extend(repeat_n(1, end - start));
                rep.push(0);
                rep.extend(repeat_n(1, end - start - 1));
            }
            v_end = end;
        }
        if !def.is_empty() {
            writer.write_batch(&values[v_start..v_end], Some(&def), Some(&rep))?;
        }
        col.close()
    }

    fn num_rows(&self) -> usize {
        self.offsets.as_slice().len().saturating_sub(1)
    }
}

/// HList of columns, named `c0, c1, ...` like the Arrow output.
pub trait ParquetColumns {
    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()>;
    fn write_columns<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()>;
    fn num_rows(&self) -> Option<usize>;
}

impl ParquetColumns for HNil {
    fn parquet_types(_idx: usize, _out: &mut Vec<TypePtr>) -> Result<()> {
        Ok(())
    }

    fn write_columns<W: Write + Send>(
        &self,
        _rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        Ok(())
    }

    fn num_rows(&self) -> Option<usize> {
        None
    }
}

impl<H: ParquetColumn, T: ParquetColumns> ParquetColumns for HCons<H, T> {
    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        out.push(H::parquet_type(&format!("c{idx}"))?);
        T::parquet_types(idx + 1, out)
    }

    fn write_columns<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        self.0.write_column(rg)?;
        self.1.write_columns(rg)
    }

    fn num_rows(&self) -> Option<usize> {
        let n = self.0.num_rows();
        match self.1.num_rows() {
            Some(m) if m != n => None,
            _ => Some(n),
        }
    }
}

/// Default properties for the zero-copy path: dictionary encoding off (it
/// would build a hash table over the values; generated data is mostly unique).
pub fn default_props() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build()
}

/// Write an `ArrowBackend` sink as a single-row-group Parquet file, reading
/// every value buffer in place. Returns the writer.
pub fn write_columns<S: ParquetColumns, W: Write + Send>(
    sink: &S,
    writer: W,
    props: Option<WriterProperties>,
) -> Result<W> {
    if sink.num_rows().is_none() {
        return Err(ParquetError::General(
            "columns have different row counts".into(),
        ));
    }
    let mut fields = Vec::new();
    S::parquet_types(0, &mut fields)?;
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?,
    );
    let props = Arc::new(props.unwrap_or_else(default_props));
    let mut file = SerializedFileWriter::new(writer, schema, props)?;
    let mut rg = file.next_row_group()?;
    sink.write_columns(&mut rg)?;
    rg.close()?;
    file.into_inner()
}

// ============================================================================
// DistributionParquet — generate and write
// ============================================================================

pub trait DistributionParquet: Distribution<ArrowBackend> {
    /// Generate `n` rows column by column into `ArrowBackend` buffers and
    /// write them as Parquet without copying the values.
    fn collect_parquet<W: Write + Send>(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        writer: W,
        props: Option<WriterProperties>,
    ) -> Result<W>
    where
        <Self::Item as DefaultSink<ArrowBackend>>::Sink: Default + ParquetColumns,
    {
        let sink = self.collect_into(Default::default(), rng, n);
        write_columns(&sink, writer, props)
    }
}

impl<D: Distribution<ArrowBackend>> DistributionParquet for D {}
