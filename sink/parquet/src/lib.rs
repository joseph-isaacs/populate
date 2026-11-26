#![warn(missing_docs)]
//! Parquet output.
//!
//! - [`write_columns`] / [`DistributionParquet::collect_parquet`]: collected
//!   columns (numbers, strings, lists of numbers, and tuples of them) as one
//!   row group, read in place from the buffers they were generated into,
//!   with no Arrow in between. Columns are named `c0, c1, ...` like the
//!   Arrow output (nested tuples are flattened) and are all required.
//!   `populate-table`'s `write_dist_parquet` builds on the same pieces
//!   ([`ParquetColumns`], [`encode_leaf`]) to write row groups in parallel.
//! - [`encode`]: the column chunk encoder behind both. It also encodes
//!   Arrow arrays, for `populate-table`'s `RecordBatch` writer.
//! - [`write_parquet`]: any `RecordBatch` via arrow-rs's `ArrowWriter`.
//!   It materialises per-value level and index arrays for list columns and
//!   buffers a whole row group in memory.

pub mod encode;

use arrow_array::RecordBatch;
use bytes::Bytes;
use encode::{Rope, encode_strings, encode_values, encode_var_list, rope_sink, slices_ok};
use parquet::arrow::ArrowWriter;
use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use parquet::column::writer::{ColumnCloseResult, get_column_writer, get_typed_column_writer};
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType, DoubleType, FloatType, Int32Type, Int64Type,
};
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};
use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter};
use parquet::schema::types::{ColumnDescPtr, Type, TypePtr};
use populate_core::{Columns, Distribution, ListCol, StrCol};
use rand::Rng;
use std::io::Write;
use std::iter::repeat_n;
use std::sync::Arc;

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
// Collected columns
// ============================================================================

/// One leaf column of a collected row group, borrowed.
#[derive(Debug, Clone, Copy)]
pub enum Leaf<'a> {
    /// Numbers, one per row.
    Num(Num<'a>),
    /// Arrow `Utf8` layout: `offsets` has one more entry than there are
    /// rows (or none at all for no rows).
    Str {
        /// Where each value starts in `data`, and where the last ends.
        offsets: &'a [i32],
        /// Every value's UTF-8 bytes, end to end.
        data: &'a [u8],
    },
    /// Arrow `List` layout, of numbers.
    List {
        /// Where each row's elements start in `values`, and where the last
        /// ends.
        offsets: &'a [i32],
        /// Every element of every row, end to end.
        values: Num<'a>,
    },
}

/// A buffer of numbers.
#[derive(Debug, Clone, Copy)]
pub enum Num<'a> {
    /// Parquet `INT32`.
    I32(&'a [i32]),
    /// Parquet `INT64`.
    I64(&'a [i64]),
    /// Parquet `FLOAT`.
    F32(&'a [f32]),
    /// Parquet `DOUBLE`.
    F64(&'a [f64]),
}

impl Leaf<'_> {
    /// Number of rows.
    pub fn rows(&self) -> usize {
        match self {
            Leaf::Num(n) => n.len(),
            Leaf::Str { offsets, .. } | Leaf::List { offsets, .. } => {
                offsets.len().saturating_sub(1)
            }
        }
    }
}

impl Num<'_> {
    fn len(&self) -> usize {
        match self {
            Num::I32(v) => v.len(),
            Num::I64(v) => v.len(),
            Num::F32(v) => v.len(),
            Num::F64(v) => v.len(),
        }
    }
}

/// Collected columns that can be written as Parquet: numbers, strings,
/// lists of numbers, and tuples of them. Implemented for every such
/// `Columns<T>`, so users never implement it.
pub trait ParquetColumns {
    /// Number of leaf columns.
    const COLUMNS: usize;
    /// Append the Parquet type of each leaf column, the first named `c{idx}`.
    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()>;
    /// Append each leaf column's buffers.
    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>);
}

/// A number type with a Parquet column: its physical type, and its buffer
/// as a [`Num`].
pub trait ParquetNative: Copy {
    /// The `parquet` crate's type for the column.
    type Parquet: DataType<T = Self>;
    /// The column's physical type.
    const PHYSICAL: PhysicalType;
    /// `v` as a [`Num`].
    fn num(v: &[Self]) -> Num<'_>;
}

macro_rules! parquet_native {
    ($($t:ty => $p:ty, $phys:ident, $var:ident);* $(;)?) => {
        $(impl ParquetNative for $t {
            type Parquet = $p;
            const PHYSICAL: PhysicalType = PhysicalType::$phys;
            fn num(v: &[Self]) -> Num<'_> {
                Num::$var(v)
            }
        })*
    };
}

parquet_native!(
    i32 => Int32Type, INT32, I32;
    i64 => Int64Type, INT64, I64;
    f32 => FloatType, FLOAT, F32;
    f64 => DoubleType, DOUBLE, F64;
);

fn primitive(name: &str, phys: PhysicalType, logical: Option<LogicalType>) -> Result<TypePtr> {
    Ok(Arc::new(
        Type::primitive_type_builder(name, phys)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(logical)
            .build()?,
    ))
}

impl<T: ParquetNative> ParquetColumns for Vec<T> {
    const COLUMNS: usize = 1;

    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        out.push(primitive(&format!("c{idx}"), T::PHYSICAL, None)?);
        Ok(())
    }

    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>) {
        out.push(Leaf::Num(T::num(self)));
    }
}

impl ParquetColumns for StrCol {
    const COLUMNS: usize = 1;

    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        let name = format!("c{idx}");
        out.push(primitive(
            &name,
            PhysicalType::BYTE_ARRAY,
            Some(LogicalType::String),
        )?);
        Ok(())
    }

    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>) {
        out.push(Leaf::Str {
            offsets: self.offsets(),
            data: self.data(),
        });
    }
}

/// Required `list<required number>`, the standard 3-level LIST layout.
impl<T: ParquetNative> ParquetColumns for ListCol<Vec<T>> {
    const COLUMNS: usize = 1;

    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        let list = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![primitive("element", T::PHYSICAL, None)?])
            .build()?;
        out.push(Arc::new(
            Type::group_type_builder(&format!("c{idx}"))
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::List))
                .with_fields(vec![Arc::new(list)])
                .build()?,
        ));
        Ok(())
    }

    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>) {
        out.push(Leaf::List {
            offsets: self.offsets(),
            values: T::num(&self.values),
        });
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

            fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>) {
                $(self.$idx.leaves(out);)+
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

/// One encoded column chunk, offsets relative to its start.
pub type Chunk = (Rope, ColumnCloseResult);

/// The file schema of `S`'s columns. Errors if there are none.
pub fn schema<S: ParquetColumns>() -> Result<TypePtr> {
    if S::COLUMNS == 0 {
        return Err(ParquetError::General("no columns".into()));
    }
    let mut fields = Vec::with_capacity(S::COLUMNS);
    S::parquet_types(0, &mut fields)?;
    Ok(Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?,
    ))
}

/// The leaf columns of `cols`, checked to hold `rows` rows each.
pub fn leaves<S: ParquetColumns>(cols: &S, rows: usize) -> Result<Vec<Leaf<'_>>> {
    let mut out = Vec::with_capacity(S::COLUMNS);
    cols.leaves(&mut out);
    if out.iter().any(|l| l.rows() != rows) {
        return Err(ParquetError::General(
            "columns have different row counts".into(),
        ));
    }
    Ok(out)
}

/// Default properties for [`write_columns`]: dictionary encoding off (it
/// would build a hash table over the values; generated data is mostly
/// unique).
pub fn default_props() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build()
}

/// Write collected columns as a single-row-group Parquet file, reading
/// every value buffer in place. Returns the writer.
pub fn write_columns<S: ParquetColumns, W: Write + Send>(
    cols: &S,
    writer: W,
    props: Option<WriterProperties>,
) -> Result<W> {
    let schema = schema::<S>()?;
    let props: WriterPropertiesPtr = Arc::new(props.unwrap_or_else(default_props));
    let mut file = SerializedFileWriter::new(writer, schema, props.clone())?;
    let mut first = Vec::with_capacity(1);
    cols.leaves(&mut first);
    let rows = first.first().map_or(0, Leaf::rows);
    let leaves = leaves(cols, rows)?;
    let descrs: Vec<ColumnDescPtr> = file.schema_descr().columns().to_vec();
    let mut rg = file.next_row_group()?;
    for (leaf, descr) in leaves.iter().zip(&descrs) {
        let (chunk, close) = encode_leaf(leaf, descr, &props)?;
        rg.append_column(&chunk, close)?;
    }
    rg.close()?;
    file.into_inner()
}

/// Collecting straight to a Parquet file, for every [`Distribution`] whose
/// columns [`ParquetColumns`] covers.
///
/// ```
/// use populate_builtins::{list, range, uniform};
/// use populate_sink_parquet::DistributionParquet;
///
/// let mut rows = (range(0i64, 1), list(uniform(0i32, 4), uniform(0.0f64, 1.0)));
/// let bytes = rows.collect_parquet(&mut rand::rng(), 1_000, Vec::new(), None).unwrap();
/// assert_eq!(&bytes[..4], b"PAR1");
/// ```
pub trait DistributionParquet: Distribution {
    /// Generate `n` rows column by column and write them as Parquet
    /// ([`write_columns`]) without copying the values.
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

/// Encode one leaf as a column chunk: with [`encode`] when the settings
/// allow, else with the `parquet` crate's column writer, so every setting
/// works and only the speed differs.
pub fn encode_leaf(
    leaf: &Leaf<'_>,
    descr: &ColumnDescPtr,
    props: &WriterPropertiesPtr,
) -> Result<Chunk> {
    match *leaf {
        Leaf::Num(n) if slices_ok(descr, props) => match n {
            Num::I32(v) => encode_values(descr, props, v),
            Num::I64(v) => encode_values(descr, props, v),
            Num::F32(v) => encode_values(descr, props, v),
            Num::F64(v) => encode_values(descr, props, v),
        },
        Leaf::Num(n) => match n {
            Num::I32(v) => generic::<Int32Type>(descr, props, v, None),
            Num::I64(v) => generic::<Int64Type>(descr, props, v, None),
            Num::F32(v) => generic::<FloatType>(descr, props, v, None),
            Num::F64(v) => generic::<DoubleType>(descr, props, v, None),
        },
        Leaf::Str { offsets, data } if slices_ok(descr, props) => {
            encode_strings(descr, props, offsets, data)
        }
        Leaf::Str { offsets, data } => {
            let data = Bytes::copy_from_slice(data);
            let v: Vec<ByteArray> = offsets
                .windows(2)
                .map(|w| data.slice(w[0] as usize..w[1] as usize).into())
                .collect();
            generic::<ByteArrayType>(descr, props, &v, None)
        }
        Leaf::List { offsets, values } if slices_ok(descr, props) => match values {
            Num::I32(v) => encode_var_list(descr, props, offsets, v),
            Num::I64(v) => encode_var_list(descr, props, offsets, v),
            Num::F32(v) => encode_var_list(descr, props, offsets, v),
            Num::F64(v) => encode_var_list(descr, props, offsets, v),
        },
        Leaf::List { offsets, values } => {
            let levels = list_levels(offsets);
            let lv = Some((&levels.0[..], &levels.1[..]));
            match values {
                Num::I32(v) => generic::<Int32Type>(descr, props, v, lv),
                Num::I64(v) => generic::<Int64Type>(descr, props, v, lv),
                Num::F32(v) => generic::<FloatType>(descr, props, v, lv),
                Num::F64(v) => generic::<DoubleType>(descr, props, v, lv),
            }
        }
    }
}

/// Definition and repetition levels of a required list of required values.
fn list_levels(offsets: &[i32]) -> (Vec<i16>, Vec<i16>) {
    let (mut def, mut rep) = (Vec::new(), Vec::new());
    for w in offsets.windows(2) {
        let len = (w[1] - w[0]) as usize;
        if len == 0 {
            def.push(0);
            rep.push(0);
        } else {
            def.extend(repeat_n(1, len));
            rep.push(0);
            rep.extend(repeat_n(1, len - 1));
        }
    }
    (def, rep)
}

/// One column chunk from the `parquet` crate's column writer, in memory.
fn generic<T: DataType>(
    descr: &ColumnDescPtr,
    props: &WriterPropertiesPtr,
    values: &[T::T],
    levels: Option<(&[i16], &[i16])>,
) -> Result<Chunk> {
    let mut sink = rope_sink();
    let close = {
        let pages = SerializedPageWriter::new(&mut sink);
        let w = get_column_writer(descr.clone(), props.clone(), Box::new(pages));
        let mut w = get_typed_column_writer::<T>(w);
        match levels {
            Some((def, rep)) => w.write_batch(values, Some(def), Some(rep))?,
            None => w.write_batch(values, None, None)?,
        };
        w.close()?
    };
    Ok((sink.into_inner()?.finish(), close))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatched_row_counts_are_rejected() {
        // The 5-row middle column must not be masked by the 10-row first one.
        let cols = (vec![1i64; 10], vec![1i64; 5], vec![1i64; 10]);
        assert!(leaves(&cols, 10).is_err());
        // nor by nesting
        let cols = (vec![1i64; 10], (vec![1i64; 10], vec![1i64; 5]));
        let err = write_columns(&cols, Vec::new(), None).unwrap_err();
        assert!(err.to_string().contains("different row counts"), "{err}");
        assert_eq!(leaves(&(vec![1i64; 3], vec![2i32; 3]), 3).unwrap().len(), 2);
    }
}
