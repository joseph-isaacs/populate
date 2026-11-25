//! `lineorder_flat`: `lineorder` joined with its customer, supplier and
//! part (the denormalised SSB-flat table, as ClickHouse defines it).
//!
//! The three dimension tables are drawn once (on all cores) and kept as
//! their code columns ([`part_codes`], [`party_codes`]: about 11 bytes a
//! part, 9 a customer), and the columns unique per row (names, addresses,
//! phone numbers) as `Utf8View` text with one buffer per 64Ki rows. Then
//! each block of `lineorder` rows is joined by key ([`Join`]), a column at
//! a time: dictionary views from the codes, as
//! [`part_words`](crate::tables::part_words) and
//! [`party_words`](crate::tables::party_words) map them, and the unique
//! columns as views into the dimension's buffers. So no text is copied,
//! but those columns of every batch share (and keep alive) the
//! dimension's whole buffer.

use crate::TableName;
use crate::cols::{Shared, SharedCols, Word, Words, children_of};
use crate::dict::{DATA, Dict, Views};
use crate::tables::{
    LINEORDER, Line, PART_COLUMNS, PartCode, PartWords, Party, PartyCode, Scale, TableGen, at,
    lines, orders, part_codes, party_codes, party_text, segment_codes, table,
};
use arrow_buffer::Buffer;
use populate_builtins::{ReadRows, named_all};
use populate_core::{ChildCols, Children, Columns, Distribution, Sink};
use populate_table::par_ordered;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::convert::Infallible;
use std::sync::Arc;

/// A customer's columns after its key.
type CustomerRow = (Shared, Shared, Word, Word, Word, Shared, Word);
/// A supplier's columns after its key.
type SupplierRow = (Shared, Shared, Word, Word, Word, Shared);
/// One `lineorder_flat` row.
type FlatRow = (Line, (CustomerRow, SupplierRow, PartWords));

/// One text column of a whole dimension table: its views and the
/// buffers they point into.
#[derive(Debug, Default)]
struct Text {
    views: Vec<u128>,
    buffers: Arc<[Buffer]>,
}

impl Text {
    /// Rows `rows`, pushed onto `cols` (which then point at this
    /// column's buffers).
    fn gather(&self, rows: &[usize], cols: &mut SharedCols) {
        if !Arc::ptr_eq(&cols.buffers, &self.buffers) {
            assert!(cols.rows() == 0, "views into one dimension's buffers only");
            cols.buffers = self.buffers.clone();
        }
        cols.push_n(rows.iter().map(|&r| Shared(self.views[r])));
    }

    /// The per-job columns, end to end: job `k`'s bytes become buffer
    /// `k`, and its views (which name buffer `DATA`) are pointed at it.
    fn concat(parts: Vec<Views>) -> Text {
        let mut views = Vec::with_capacity(parts.iter().map(|p| p.views.len()).sum());
        let mut buffers = Vec::with_capacity(parts.len());
        for (k, p) in parts.into_iter().enumerate() {
            debug_assert!(
                p.views
                    .iter()
                    .all(|&v| v as u32 <= 12 || (v >> 64) as u32 == DATA)
            );
            let k = u128::from(u32::try_from(k).expect("buffers fit in u32"));
            views.extend(p.views.into_iter().map(|v| {
                if v as u32 <= 12 {
                    v
                } else {
                    // the buffer index is bits 64..96 of a long view
                    v & !(u128::from(u32::MAX) << 64) | k << 64
                }
            }));
            buffers.push(Buffer::from_vec(p.data));
        }
        Text {
            views,
            buffers: Arc::from(buffers),
        }
    }
}

/// A whole supplier or customer table: its codes, its market segments
/// (customers only), and its name, address and phone columns.
#[derive(Debug, Default)]
struct Dim {
    codes: Columns<PartyCode>,
    segment: Vec<u8>,
    text: [Text; 3],
}

impl Dim {
    /// Name, address, city, nation, region and phone of rows `rows`, a
    /// column at a time.
    fn gather(
        &self,
        d: &Dict,
        rows: &[usize],
        (name, address, city, nation, region, phone): (
            &mut SharedCols,
            &mut SharedCols,
            &mut Words,
            &mut Words,
            &mut Words,
            &mut SharedCols,
        ),
    ) {
        let [n, a, p] = &self.text;
        n.gather(rows, name);
        a.gather(rows, address);
        let (nations, cities) = &self.codes;
        // as `party_words` maps them (the tests check the join against it)
        let nat = |r: usize| nations[r] as usize;
        city.push_n(
            rows.iter()
                .map(|&r| Word(d.cities[nat(r) * 10 + cities[r] as usize])),
        );
        nation.push_n(rows.iter().map(|&r| Word(d.nations[nat(r)])));
        region.push_n(rows.iter().map(|&r| Word(d.regions[nat(r)])));
        p.gather(rows, phone);
    }
}

/// The dimension tables, whole.
#[derive(Debug, Default)]
pub(crate) struct Dims {
    customer: Dim,
    supplier: Dim,
    part: Columns<PartCode>,
}

/// Rows per generation job when building the dimensions.
const JOB: u64 = 64 * 1024;

/// Rows `0..rows` of `dist`, made in jobs on `threads` threads, each job's
/// columns given to `append` in order.
fn whole<D: Distribution + Clone + Sync>(
    dist: &D,
    rows: u64,
    threads: usize,
    mut append: impl FnMut(Columns<D::Item>),
) where
    Columns<D::Item>: Send,
{
    let r: Result<(), Infallible> = par_ordered(
        rows.div_ceil(JOB),
        threads,
        |j| {
            let first = j * JOB;
            // the streams don't use the caller's RNG
            let mut rng = SmallRng::seed_from_u64(0);
            at(dist.clone(), first).collect(&mut rng, (rows - first).min(JOB) as usize)
        },
        |cols| {
            append(cols);
            Ok(())
        },
    );
    r.unwrap_or_else(|e| match e {});
}

/// The rows of `from` pushed onto `to`.
fn extend<T, C: Sink<T>>(to: &mut C, from: &impl ReadRows<Row = T>) {
    for row in from.rows_from(0) {
        to.push(row);
    }
}

/// A whole supplier or customer table. Each job writes its rows' names,
/// addresses and phones into its own buffers, so every buffer stays far
/// below the 4 GiB a view's offset can reach.
fn party(d: &'static Dict, s: &Scale, p: Party, rows: u64, threads: usize) -> Dim {
    let mut dim = Dim::default();
    let segments = match p {
        Party::Customer => rows,
        Party::Supplier => 0,
    };
    whole(&party_codes(p, s), rows, threads, |c| {
        extend(&mut dim.codes, &c)
    });
    whole(&segment_codes(d), segments, threads, |c| {
        dim.segment.extend(c)
    });
    let mut parts: [Vec<Views>; 3] = Default::default();
    whole(&party_text(p, s), rows, threads, |(n, a, ph)| {
        for (v, c) in parts.iter_mut().zip([n, a, ph]) {
            v.push(c);
        }
    });
    dim.text = parts.map(Text::concat);
    dim
}

impl Dims {
    pub(crate) fn build(s: &Scale, threads: usize) -> Dims {
        let d = Dict::get();
        let mut part = Columns::<PartCode>::default();
        whole(&part_codes(d), s.parts, threads, |c| extend(&mut part, &c));
        Dims {
            customer: party(d, s, Party::Customer, s.customers, threads),
            supplier: party(d, s, Party::Supplier, s.suppliers, threads),
            part,
        }
    }

    /// The columns after `p_partkey` of parts `rows`, a column at a time.
    fn gather_part(&self, d: &Dict, rows: &[usize], out: &mut Columns<PartWords>) {
        let ((name, color), category, brand, kind, container, size) = &self.part;
        let (o_name, o_mfgr, o_cat, o_brand, o_color, o_type, o_size, o_container) = out;
        // as `part_words` maps them (the tests check the join against it)
        let cat = |r: usize| category[r] as usize;
        o_name.push_n(rows.iter().map(|&r| Word(d.names[name[r] as usize])));
        o_mfgr.push_n(rows.iter().map(|&r| Word(d.mfgrs[cat(r) / 5])));
        o_cat.push_n(rows.iter().map(|&r| Word(d.categories[cat(r)])));
        o_brand.push_n(
            rows.iter()
                .map(|&r| Word(d.brands[cat(r) * 40 + brand[r] as usize])),
        );
        o_color.push_n(rows.iter().map(|&r| Word(d.colors[color[r] as usize])));
        o_type.push_n(rows.iter().map(|&r| Word(d.types.views[kind[r] as usize])));
        o_size.extend(rows.iter().map(|&r| size[r]));
        o_container.push_n(
            rows.iter()
                .map(|&r| Word(d.containers.views[container[r] as usize])),
        );
    }
}

/// `lineorder`'s lines joined with `dims`. See [`generator`].
#[derive(Debug, Clone)]
struct Join<D> {
    lines: D,
    dims: Arc<Dims>,
}

impl<D: Distribution<Item = Children<Line>>> Distribution for Join<D> {
    type Item = Children<FlatRow>;

    fn sample(&mut self, rng: &mut impl Rng) -> Children<FlatRow> {
        let cols = self.collect(rng, 1);
        Children(cols.values.rows_from(0).collect())
    }

    /// The lines, then each dimension column gathered from them in a pass
    /// of its own: the lookups of one column are independent, so their
    /// cache misses overlap.
    fn collect_into(
        &mut self,
        cols: &mut ChildCols<Columns<FlatRow>>,
        rng: &mut impl Rng,
        n: usize,
    ) {
        let d = Dict::get();
        let lines = self.lines.collect(rng, n).values;
        let ((_, _, cust, part, supp, ..), _) = &lines;
        let rows = |keys: &[i32]| -> Vec<usize> { keys.iter().map(|&k| k as usize - 1).collect() };
        let (cust, part, supp) = (rows(cust), rows(part), rows(supp));
        let (lo, (c, s, p)) = &mut cols.values;
        let dims = &self.dims;
        dims.customer.gather(
            d,
            &cust,
            (&mut c.0, &mut c.1, &mut c.2, &mut c.3, &mut c.4, &mut c.5),
        );
        c.6.push_n(
            cust.iter()
                .map(|&r| Word(d.segments.views[dims.customer.segment[r] as usize])),
        );
        dims.supplier.gather(
            d,
            &supp,
            (&mut s.0, &mut s.1, &mut s.2, &mut s.3, &mut s.4, &mut s.5),
        );
        dims.gather_part(d, &part, p);
        if lo.rows() == 0 {
            *lo = lines;
        } else {
            for l in lines.rows_from(0) {
                lo.push(l);
            }
        }
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        self.lines.skip(n);
    }
}

/// `lineorder_flat` at scale `s`, joined with `dims` (which may be empty
/// for its schema alone).
pub(crate) fn generator(s: &Scale, dims: Arc<Dims>) -> Arc<dyn TableGen> {
    let d = Dict::get();
    let names: Vec<&str> = LINEORDER
        .into_iter()
        .chain(Party::Customer.columns()[1..].iter().copied())
        .chain(Party::Supplier.columns()[1..7].iter().copied())
        .chain(PART_COLUMNS)
        .collect();
    let lines = Join {
        lines: children_of::<_, Line, _>(orders(s), lines(d)),
        dims,
    };
    table(TableName::LineOrderFlat, s.orders, named_all(&names, lines))
}
