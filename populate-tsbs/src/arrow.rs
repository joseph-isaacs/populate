//! Arrow output: one table per measurement (`cpu`, `mem`, ...,
//! `readings`, `diagnostics`), rows in the order the text formats write
//! them, with a `time` column, one column per tag and one per field.

use crate::chunked::Chunked;
use crate::model::{NO_TAG, Point, Statics, TagVal, Type, Val};
use crate::sim::AnySim;
use arrow_array::builder::{Float32Builder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use populate_table::MultiChunks;
use std::sync::Arc;

/// Schemas of every measurement table of a run. Tags and fields are
/// nullable where the run can clear them (IoT) or omit them
/// (devops-generic hosts with fewer metrics).
pub fn schemas(st: &Statics, nullable_tags: bool, nullable_fields: bool) -> Vec<SchemaRef> {
    st.measurements
        .iter()
        .map(|m| {
            let mut fields = vec![Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            )];
            for (k, t) in st.tag_keys.iter().zip(&st.tag_types) {
                let dt = if *t == Type::F32 {
                    DataType::Float32
                } else {
                    DataType::Utf8
                };
                fields.push(Field::new(&**k, dt, nullable_tags));
            }
            for k in &m.extra_tags {
                fields.push(Field::new(&**k, DataType::Utf8, false));
            }
            for (k, t) in m.fields.iter().zip(&m.types) {
                let dt = if *t == Type::F64 {
                    DataType::Float64
                } else {
                    DataType::Int64
                };
                fields.push(Field::new(&**k, dt, nullable_fields));
            }
            Arc::new(Schema::new(fields))
        })
        .collect()
}

enum Col {
    Str(StringBuilder),
    F32(Float32Builder),
    I64(Int64Builder),
    F64(Float64Builder),
}

/// Column builders for one measurement table.
struct Builder {
    time: Vec<i64>,
    cols: Vec<Col>,
    n_tags: usize,
    n_extra: usize,
}

impl Builder {
    fn new(schema: &Schema, n_tags: usize, n_extra: usize) -> Self {
        let cols = schema.fields()[1..]
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8 => Col::Str(StringBuilder::new()),
                DataType::Float32 => Col::F32(Float32Builder::new()),
                DataType::Float64 => Col::F64(Float64Builder::new()),
                _ => Col::I64(Int64Builder::new()),
            })
            .collect();
        Builder {
            time: Vec::new(),
            cols,
            n_tags,
            n_extra,
        }
    }

    fn push(&mut self, st: &Statics, p: &Point) {
        let g = p.source as usize;
        self.time.push(p.ts);
        for (i, (c, v)) in self.cols[..self.n_tags]
            .iter_mut()
            .zip(&st.gen_tags[g])
            .enumerate()
        {
            let zero = p.zero_tag != NO_TAG && i == p.zero_tag as usize;
            match (c, v) {
                (Col::Str(b), _) if zero => b.append_null(),
                (Col::F32(b), _) if zero => b.append_null(),
                (Col::Str(b), TagVal::Str(s)) => b.append_value(s),
                (Col::F32(b), TagVal::F32(f)) => b.append_value(*f),
                _ => unreachable!("tag types match the schema"),
            }
        }
        let extra = &st.extra_tags[g][p.meas as usize];
        for (c, v) in self.cols[self.n_tags..][..self.n_extra]
            .iter_mut()
            .zip(extra)
        {
            if let Col::Str(b) = c {
                b.append_value(v);
            }
        }
        let fields = &mut self.cols[self.n_tags + self.n_extra..];
        for (i, c) in fields.iter_mut().enumerate() {
            match (c, p.fields.get(i).copied().unwrap_or(Val::Nil)) {
                (Col::I64(b), Val::I(v)) => b.append_value(v),
                (Col::F64(b), v @ (Val::F(_) | Val::Fixed(..))) => {
                    b.append_value(v.as_f64().expect("a float"))
                }
                (Col::I64(b), _) => b.append_null(),
                (Col::F64(b), _) => b.append_null(),
                _ => unreachable!("field columns are numeric"),
            }
        }
    }

    fn finish(&mut self, schema: &SchemaRef) -> RecordBatch {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len() + 1);
        arrays.push(Arc::new(
            TimestampNanosecondArray::from(std::mem::take(&mut self.time)).with_timezone("UTC"),
        ));
        for c in &mut self.cols {
            arrays.push(match c {
                Col::Str(b) => Arc::new(b.finish()),
                Col::F32(b) => Arc::new(b.finish()),
                Col::I64(b) => Arc::new(b.finish()),
                Col::F64(b) => Arc::new(b.finish()),
            });
        }
        RecordBatch::try_new(schema.clone(), arrays).expect("columns match the schema")
    }
}

/// A TSBS run as Arrow tables, one per measurement, on the
/// [`populate_table`] chunk engine ([`MultiChunks`]: Parquet via
/// `write_parquet_many`, batches via `par_sources`).
pub struct TsbsArrow {
    chunks: Chunked<AnySim>,
    statics: Arc<Statics>,
    schemas: Vec<SchemaRef>,
}

impl TsbsArrow {
    /// Arrow tables of `chunks`.
    pub fn new(chunks: Chunked<AnySim>, iot: bool, generic: bool) -> Self {
        let statics = chunks.statics();
        let schemas = schemas(&statics, iot, iot || generic);
        TsbsArrow {
            chunks,
            statics,
            schemas,
        }
    }

    /// Measurement (table) names, in output order.
    pub fn table_names(&self) -> Vec<String> {
        self.statics
            .measurements
            .iter()
            .map(|m| m.name.to_string())
            .collect()
    }
}

impl MultiChunks for TsbsArrow {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.schemas.clone()
    }

    fn num_chunks(&self) -> u64 {
        self.chunks.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let st = &*self.statics;
        let mut b: Vec<Builder> = self
            .schemas
            .iter()
            .zip(&st.measurements)
            .map(|(s, m)| Builder::new(s, st.tag_keys.len(), m.extra_tags.len()))
            .collect();
        self.chunks
            .run(i, |_: &AnySim, p| b[p.meas as usize].push(st, p));
        for (k, b) in b.iter_mut().enumerate() {
            if !b.time.is_empty() {
                emit(k, b.finish(&self.schemas[k]));
            }
        }
    }
}
