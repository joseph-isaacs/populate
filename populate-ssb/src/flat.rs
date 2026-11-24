//! `lineorder_flat`: `lineorder` joined with its customer, supplier and
//! part (the denormalised SSB-flat table, as ClickHouse defines it).
//!
//! The three dimension tables are generated once (on all cores) and kept;
//! each `lineorder` batch is then joined by gathering dimension rows by
//! key. Strings are gathered as views, so no text is copied.

use crate::dict::Dict;
use crate::tables::{self, Scale, lineorder_fields};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use populate_table::par_ordered;
use std::convert::Infallible;
use std::sync::{Arc, LazyLock};

/// Dimension columns appended to `lineorder`: `(table, column)`.
const DIMS: [(Dim, &str); 21] = [
    (Dim::Customer, "c_name"),
    (Dim::Customer, "c_address"),
    (Dim::Customer, "c_city"),
    (Dim::Customer, "c_nation"),
    (Dim::Customer, "c_region"),
    (Dim::Customer, "c_phone"),
    (Dim::Customer, "c_mktsegment"),
    (Dim::Supplier, "s_name"),
    (Dim::Supplier, "s_address"),
    (Dim::Supplier, "s_city"),
    (Dim::Supplier, "s_nation"),
    (Dim::Supplier, "s_region"),
    (Dim::Supplier, "s_phone"),
    (Dim::Part, "p_name"),
    (Dim::Part, "p_mfgr"),
    (Dim::Part, "p_category"),
    (Dim::Part, "p_brand1"),
    (Dim::Part, "p_color"),
    (Dim::Part, "p_type"),
    (Dim::Part, "p_size"),
    (Dim::Part, "p_container"),
];

#[derive(Clone, Copy)]
enum Dim {
    Customer,
    Supplier,
    Part,
}

impl Dim {
    fn schema(self) -> &'static SchemaRef {
        match self {
            Dim::Customer => &tables::CUSTOMER,
            Dim::Supplier => &tables::SUPPLIER,
            Dim::Part => &tables::PART,
        }
    }

    /// The `lineorder` column holding this table's (1-based) key.
    fn key(self) -> usize {
        match self {
            Dim::Customer => 2,
            Dim::Supplier => 4,
            Dim::Part => 3,
        }
    }
}

pub(crate) static FLAT: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = lineorder_fields();
    for (t, c) in DIMS {
        let f = t.schema().field_with_name(c).expect("dimension column");
        fields.push(Field::clone(f));
    }
    Arc::new(Schema::new(fields))
});

/// The dimension tables, whole.
#[derive(Debug)]
pub(crate) struct Dims {
    customer: RecordBatch,
    supplier: RecordBatch,
    part: RecordBatch,
}

/// Rows per generation job when building the dimensions.
const JOB: u64 = 64 * 1024;

fn whole(
    schema: &SchemaRef,
    rows: u64,
    threads: usize,
    f: impl Fn(u64, usize) -> RecordBatch + Sync,
) -> RecordBatch {
    let mut parts = Vec::new();
    let r: Result<(), Infallible> = par_ordered(
        rows.div_ceil(JOB),
        threads,
        |j| {
            let first = j * JOB;
            f(first, (rows - first).min(JOB) as usize)
        },
        |b| {
            parts.push(b);
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
    concat_batches(schema, &parts).expect("same schema")
}

impl Dims {
    pub(crate) fn build(s: &Scale, threads: usize) -> Dims {
        let d = Dict::get();
        Dims {
            customer: whole(&tables::CUSTOMER, s.customers, threads, |f, n| {
                tables::customer(d, s, f, n)
            }),
            supplier: whole(&tables::SUPPLIER, s.suppliers, threads, |f, n| {
                tables::supplier(d, f, n)
            }),
            part: whole(&tables::PART, s.parts, threads, |f, n| {
                tables::part(d, f, n)
            }),
        }
    }

    fn table(&self, t: Dim) -> &RecordBatch {
        match t {
            Dim::Customer => &self.customer,
            Dim::Supplier => &self.supplier,
            Dim::Part => &self.part,
        }
    }

    /// `lineorder` batch `lo` joined with the dimensions.
    pub(crate) fn join(&self, lo: RecordBatch) -> RecordBatch {
        let idx = |col: usize| -> Int32Array {
            let keys = lo
                .column(col)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 key");
            keys.unary(|k| k - 1)
        };
        let indices = [
            (Dim::Customer.key(), idx(Dim::Customer.key())),
            (Dim::Supplier.key(), idx(Dim::Supplier.key())),
            (Dim::Part.key(), idx(Dim::Part.key())),
        ];
        let mut cols: Vec<ArrayRef> = lo.columns().to_vec();
        for (t, c) in DIMS {
            let i = &indices.iter().find(|(k, _)| *k == t.key()).expect("key").1;
            let src = self.table(t).column_by_name(c).expect("dimension column");
            cols.push(take(src.as_ref(), i, None).expect("keys in range"));
        }
        RecordBatch::try_new(FLAT.clone(), cols).expect("columns match the schema")
    }
}
