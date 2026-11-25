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
//! Numbers, strings and lists of numbers are encoded by the same encoder
//! as [`Table::write_parquet`](crate::Table::write_parquet) (dictionaries,
//! statistics, page indexes). Settings it does not cover (other
//! compressions, Parquet v2 pages, bloom filters, per-column encodings)
//! go through the `parquet` crate's own column writer instead, so
//! every setting works; only the speed differs.
//!
//! Columns are named `c0, c1, ...` in order (nested tuples are flattened),
//! and are all required.

use crate::parquet_direct::{
    Rope, encode_strings, encode_values, encode_var_list, rope_sink, slices_ok,
};
use crate::sched::{par_ordered, resolve_threads};
use crate::stream::mix64;
use bytes::Bytes;
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
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::convert::Infallible;
use std::io::Write;
use std::iter::repeat_n;
use std::sync::Arc;

/// How [`write_dist_parquet`] splits and encodes its rows.
#[derive(Debug, Clone)]
pub struct ParquetOptions {
    /// Rows per row group (the last may be shorter). Part of the data's
    /// identity, like the seed: each group has its own RNG.
    pub rows_per_group: usize,
    /// Worker threads, 0 for all cores. Does not change the output.
    pub threads: usize,
    pub seed: u64,
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

/// One leaf column of a collected row group, borrowed.
#[derive(Debug, Clone, Copy)]
pub enum Leaf<'a> {
    Num(Num<'a>),
    /// Arrow `Utf8` layout: `offsets` has one more entry than there are
    /// rows (or none at all for no rows).
    Str {
        offsets: &'a [i32],
        data: &'a [u8],
    },
    /// Arrow `List` layout, of numbers.
    List {
        offsets: &'a [i32],
        values: Num<'a>,
    },
}

/// A buffer of numbers.
#[derive(Debug, Clone, Copy)]
pub enum Num<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    F32(&'a [f32]),
    F64(&'a [f64]),
}

impl Leaf<'_> {
    fn rows(&self) -> usize {
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

/// Collected columns that [`write_dist_parquet`] can encode: numbers,
/// strings, lists of numbers, and tuples of them. Implemented for every
/// such `Columns<T>`, so users never implement it.
pub trait DirectColumns {
    /// Number of leaf columns.
    const COLUMNS: usize;
    /// Append the Parquet type of each leaf column, the first named `c{idx}`.
    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()>;
    /// Append each leaf column's buffers.
    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>);
}

/// A number type with a direct Parquet column.
pub trait DirectNum: Copy {
    type Parquet: DataType<T = Self>;
    const PHYSICAL: PhysicalType;
    fn num(v: &[Self]) -> Num<'_>;
}

macro_rules! direct_num {
    ($($t:ty => $p:ty, $phys:ident, $var:ident);* $(;)?) => {
        $(impl DirectNum for $t {
            type Parquet = $p;
            const PHYSICAL: PhysicalType = PhysicalType::$phys;
            fn num(v: &[Self]) -> Num<'_> {
                Num::$var(v)
            }
        })*
    };
}

direct_num!(
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

impl<T: DirectNum> DirectColumns for Vec<T> {
    const COLUMNS: usize = 1;

    fn parquet_types(idx: usize, out: &mut Vec<TypePtr>) -> Result<()> {
        out.push(primitive(&format!("c{idx}"), T::PHYSICAL, None)?);
        Ok(())
    }

    fn leaves<'a>(&'a self, out: &mut Vec<Leaf<'a>>) {
        out.push(Leaf::Num(T::num(self)));
    }
}

impl DirectColumns for StrCol {
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
impl<T: DirectNum> DirectColumns for ListCol<Vec<T>> {
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
        impl<$($C: DirectColumns),+> DirectColumns for ($($C,)+) {
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

tuple_columns!(A 0);
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
type Chunk = (Rope, ColumnCloseResult);

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
    Columns<D::Item>: DirectColumns,
    W: Write + Send,
{
    let n_cols = <Columns<D::Item> as DirectColumns>::COLUMNS;
    if n_cols == 0 {
        return Err(ParquetError::General("no columns".into()));
    }
    let mut fields = Vec::with_capacity(n_cols);
    <Columns<D::Item> as DirectColumns>::parquet_types(0, &mut fields)?;
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?,
    );
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
        let mut leaves = Vec::with_capacity(n_cols);
        cols.leaves(&mut leaves);
        if leaves.iter().any(|l| l.rows() != len) {
            return Err(ParquetError::General(
                "columns have different row counts".into(),
            ));
        }
        // fewer row groups than threads: the spare threads share the
        // group's columns
        let leaf_threads = (threads / groups.max(1) as usize).max(1);
        par_map(leaf_threads, leaves.len(), |i| {
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

/// `f(0..n)` on up to `threads` threads, results in order.
fn par_map<T: Send>(threads: usize, n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let mut out = Vec::with_capacity(n);
    let r: std::result::Result<(), Infallible> = par_ordered(
        n as u64,
        threads.max(1),
        |i| f(i as usize),
        |t| {
            out.push(t);
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
    out
}

/// Encode one leaf: here when the settings allow, else with the `parquet`
/// crate's column writer.
pub(crate) fn encode_leaf(
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
