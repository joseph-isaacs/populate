//! `lineorder_flat`: `lineorder` joined with its customer, supplier and
//! part (the denormalised SSB-flat table, as ClickHouse defines it).
//!
//! The three dimension tables are drawn once (on all cores) and kept as
//! small per-row codes (about 8 bytes a part, 3 a customer), and the
//! columns unique per row (names, addresses, phone numbers) as whole
//! `Utf8View` columns. Each `lineorder` batch is then joined by key:
//! dictionary columns become views into the shared dictionary, and the
//! unique columns are gathered as views into the dimension's buffers. So no
//! text is copied, but those columns of every batch share (and keep alive)
//! the dimension's whole buffer.

use crate::dict::Dict;
use crate::tables::{self, PartRows, Party, PartyRows, Scale, lineorder_fields};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema, SchemaRef};
use arrow_select::take::take;
use populate_table::par_ordered;
use std::convert::Infallible;
use std::sync::{Arc, LazyLock};

/// `lineorder`'s key columns.
const CUSTKEY: usize = 2;
const PARTKEY: usize = 3;
const SUPPKEY: usize = 4;

pub(crate) static FLAT: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = lineorder_fields();
    for t in [&tables::CUSTOMER, &tables::SUPPLIER, &tables::PART] {
        // every column but the key
        fields.extend(t.fields().iter().skip(1).map(|f| Field::clone(f)));
    }
    Arc::new(Schema::new(fields))
});

/// The dimension tables, whole.
#[derive(Debug)]
pub(crate) struct Dims {
    customer: Dim,
    supplier: Dim,
    part: PartRows,
}

/// A supplier or customer table: its codes, and its whole name, address
/// and phone columns.
#[derive(Debug)]
struct Dim {
    rows: PartyRows,
    unique: [ArrayRef; 3],
}

impl Dim {
    fn new(d: &Dict, rows: PartyRows) -> Self {
        let n = rows.len();
        let all: Vec<u32> = (0..n as u32).collect();
        let unique = rows.unique_columns(d, &all);
        Dim { rows, unique }
    }

    /// The columns after the key of rows `idx`.
    fn columns(&self, d: &Dict, idx: Vec<u32>) -> Vec<ArrayRef> {
        let at = UInt32Array::from(idx);
        let gather = |a: &ArrayRef| take(a.as_ref(), &at, None).expect("keys in range");
        let [names, addresses, phones] = &self.unique;
        let mut cols = vec![gather(names), gather(addresses)];
        let mut coded = self.rows.coded_columns(d, at.values());
        // city, nation, region, then the phone, then the segment
        cols.extend(coded.drain(..3));
        cols.push(gather(phones));
        cols.extend(coded);
        cols
    }
}

/// Rows per generation job when building the dimensions.
const JOB: u64 = 64 * 1024;

/// Rows `0..rows` made by `f(first, n)` in jobs on `threads` threads,
/// appended in order.
fn whole<T: Send>(
    rows: u64,
    threads: usize,
    f: impl Fn(u64, usize) -> T + Sync,
    mut append: impl FnMut(T),
) {
    let r: Result<(), Infallible> = par_ordered(
        rows.div_ceil(JOB),
        threads,
        |j| {
            let first = j * JOB;
            f(first, (rows - first).min(JOB) as usize)
        },
        |b| {
            append(b);
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
}

fn party(d: &Dict, s: &Scale, p: Party, rows: u64, threads: usize) -> PartyRows {
    let mut all = PartyRows::new(d, s, p, 0, 0);
    whole(
        rows,
        threads,
        |f, n| PartyRows::new(d, s, p, f, n),
        |r| all.append(r),
    );
    all
}

impl Dims {
    pub(crate) fn build(s: &Scale, threads: usize) -> Dims {
        let d = Dict::get();
        let mut part = PartRows::default();
        whole(
            s.parts,
            threads,
            |f, n| PartRows::new(d, f, n),
            |r| part.append(r),
        );
        Dims {
            customer: Dim::new(d, party(d, s, Party::Customer, s.customers, threads)),
            supplier: Dim::new(d, party(d, s, Party::Supplier, s.suppliers, threads)),
            part,
        }
    }

    /// `lineorder` batch `lo` joined with the dimensions.
    pub(crate) fn join(&self, d: &Dict, lo: RecordBatch) -> RecordBatch {
        let rows = |col: usize| -> Vec<u32> {
            let keys = lo
                .column(col)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 key");
            keys.values().iter().map(|&k| k as u32 - 1).collect()
        };
        let mut cols: Vec<ArrayRef> = lo.columns().to_vec();
        cols.extend(self.customer.columns(d, rows(CUSTKEY)));
        cols.extend(self.supplier.columns(d, rows(SUPPKEY)));
        cols.extend(self.part.columns(d, &rows(PARTKEY)));
        RecordBatch::try_new(FLAT.clone(), cols).expect("columns match the schema")
    }
}
