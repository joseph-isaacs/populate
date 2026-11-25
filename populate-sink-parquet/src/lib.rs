//! Parquet output (optional `parquet` feature).
//!
//! Two writers:
//!
//! - [`write_columns`] / [`DistributionParquet::collect_parquet`]: zero-copy
//!   path for collected columns: top-level numeric columns and
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

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use parquet::data_type::{DataType, DoubleType, FloatType, Int32Type, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::types::{Type, TypePtr};
use populate_core::{Columns, Distribution, ListCol};
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

/// One column that can be written to Parquet without copying its data.
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
impl<T: ParquetNative> ParquetColumn for ListCol<Vec<T>> {
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
        let values = &self.values;
        let offsets = self.offsets();
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
        self.len()
    }
}

/// Columns named `c0, c1, ...` like the Arrow output: one column, or a
/// tuple of them (nested tuples are flattened).
pub trait ParquetColumns {
    /// Number of Parquet columns.
    const COLUMNS: usize;
    /// Append the type of each column, the first named `c{idx}`.
    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()>;
    fn write_columns<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()>;
    /// The common row count of every column, or `None` if they differ (or
    /// there are no columns).
    fn num_rows(&self) -> Option<usize>;
}

impl<C: ParquetColumn> ParquetColumns for C {
    const COLUMNS: usize = 1;

    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        out.push(C::parquet_type(&format!("c{idx}"))?);
        Ok(())
    }

    fn write_columns<W: Write + Send>(
        &self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        self.write_column(rg)
    }

    fn num_rows(&self) -> Option<usize> {
        Some(ParquetColumn::num_rows(self))
    }
}

impl ParquetColumns for () {
    const COLUMNS: usize = 0;

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

macro_rules! tuple_columns {
    ($($C:ident $idx:tt),+) => {
        impl<$($C: ParquetColumns),+> ParquetColumns for ($($C,)+) {
            const COLUMNS: usize = 0 $(+ $C::COLUMNS)+;

            fn parquet_types(mut idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
                $(
                    $C::parquet_types(idx, out)?;
                    idx += $C::COLUMNS;
                )+
                let _ = idx;
                Ok(())
            }

            fn write_columns<W: Write + Send>(
                &self,
                rg: &mut SerializedRowGroupWriter<'_, W>,
            ) -> Result<()> {
                $(self.$idx.write_columns(rg)?;)+
                Ok(())
            }

            fn num_rows(&self) -> Option<usize> {
                // every field must agree: a mismatch anywhere is `None`
                let rows = [$(self.$idx.num_rows()),+];
                rows[0].filter(|_| rows.iter().all(|r| *r == rows[0]))
            }
        }
    };
}

tuple_columns!(A 0, B 1);
tuple_columns!(A 0, B 1, C 2);
tuple_columns!(A 0, B 1, C 2, D 3);
tuple_columns!(A 0, B 1, C 2, D 3, E 4);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Default properties for the zero-copy path: dictionary encoding off (it
/// would build a hash table over the values; generated data is mostly unique).
pub fn default_props() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build()
}

/// Write collected columns as a single-row-group Parquet file, reading
/// every value buffer in place. Returns the writer.
pub fn write_columns<S: ParquetColumns, W: Write + Send>(
    sink: &S,
    writer: W,
    props: Option<WriterProperties>,
) -> Result<W> {
    if S::COLUMNS == 0 {
        return Err(ParquetError::General("sink has no columns".into()));
    }
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

pub trait DistributionParquet: Distribution {
    /// Generate `n` rows column by column and write them as Parquet without
    /// copying the values.
    fn collect_parquet<W: Write + Send>(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        writer: W,
        props: Option<WriterProperties>,
    ) -> Result<W>
    where
        Columns<Self::Item>: ParquetColumns,
    {
        write_columns(&self.collect(rng, n), writer, props)
    }
}

impl<D: Distribution> DistributionParquet for D {}

#[cfg(test)]
mod tests {
    use super::{ParquetColumns, write_columns};

    #[test]
    fn num_rows_matching() {
        let sink = (vec![1i64; 3], vec![2i32; 3]);
        assert_eq!(sink.num_rows(), Some(3));
    }

    #[test]
    fn num_rows_detects_mismatch_past_the_first_pair() {
        // The 5-row middle column used to be masked by the 10-row first one.
        let sink = (vec![1i64; 10], vec![1i64; 5], vec![1i64; 10]);
        assert_eq!(sink.num_rows(), None);
        // nor by nesting
        let sink = (vec![1i64; 10], (vec![1i64; 10], vec![1i64; 5]));
        assert_eq!(sink.num_rows(), None);
        let err = write_columns(&sink, Vec::new(), None).unwrap_err();
        assert!(err.to_string().contains("different row counts"), "{err}");
    }

    #[test]
    fn empty_sink_is_rejected() {
        let err = write_columns(&(), Vec::new(), None).unwrap_err();
        assert!(err.to_string().contains("no columns"), "{err}");
    }
}
