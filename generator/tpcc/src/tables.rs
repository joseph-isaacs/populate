//! Every TPC-C table as one populate [`Distribution`]: a tuple of named
//! columns, each a function of its row ([`per_row`], [`text`]). The Arrow
//! schema is read off the distribution ([`schema_of`]).

use crate::cols::{Text, per_row, text};
use crate::rng::{ALNUM, Charset, DIGITS, Draws, LETTERS, below, nurand};
use crate::strings::Views;
use crate::tpcc::{
    CUSTOMERS, DISTRICTS, FIRST_NEW_ORDER, ITEMS, NEW_ORDERS, ORDERS, STOCK_PER_WAREHOUSE,
    SYLLABLES, Tpcc, TpccTableName,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_builtins::{flat_map, named, named_all};
use populate_core::{Columns, Decimal, Distribution, Sink, Timestamp, time_unit};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::{DistTable, Stream};
use rand::Rng;
use std::sync::{Arc, LazyLock};

/// A table, generated from any range of its source rows.
pub(crate) trait TableGen: Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// Source rows `first..first + n`.
    fn batch(&self, first: u64, n: u64) -> RecordBatch;
}

impl<D> TableGen for DistTable<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
    fn schema(&self) -> SchemaRef {
        DistTable::schema(self).clone()
    }

    fn batch(&self, first: u64, n: u64) -> RecordBatch {
        DistTable::batch(self, first, n)
    }
}

fn table<D>(t: TpccTableName, rows: u64, dist: D) -> Arc<dyn TableGen>
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns,
{
    Arc::new(DistTable::new(t.as_str(), rows, dist))
}

/// Every table's schema, read off its distribution (no rows generated).
pub(crate) static SCHEMAS: LazyLock<Vec<SchemaRef>> = LazyLock::new(|| {
    let c = Tpcc::new(1);
    TpccTableName::ALL
        .iter()
        .map(|&t| generator(t, &c).schema())
        .collect()
});

/// Table `t` of `cfg`.
pub(crate) fn generator(t: TpccTableName, cfg: &Tpcc) -> Arc<dyn TableGen> {
    let g = Gen::new(cfg);
    let rows = cfg.source_rows(t);
    use TpccTableName::*;
    match t {
        Warehouse => g.warehouse(rows),
        District => g.district(rows),
        Customer => g.customer(rows),
        History => g.history(rows),
        NewOrder => g.new_order(rows),
        Orders => g.orders(rows),
        OrderLine => g.order_line(rows),
        Item => g.item(rows),
        Stock => g.stock(rows),
    }
}

// ---------------------------------------------------------------------------
// Leaves

/// A column distribution a table can hold.
trait Leaf<T>: Distribution<Item = T> + Clone + Send + Sync + 'static {}

impl<T, D: Distribution<Item = T> + Clone + Send + Sync + 'static> Leaf<T> for D {}

/// Uniform in `lo..=hi` per row.
fn rand_int(s: Stream, lo: i64, hi: i64) -> impl Leaf<i32> {
    per_row(move |r| Draws::new(s, r).int(lo, hi) as i32)
}

/// Uniform in `lo..=hi` units of `10^-S` per row.
fn rand_dec<const P: u8, const S: i8>(s: Stream, lo: i64, hi: i64) -> impl Leaf<Decimal<P, S>> {
    per_row(move |r| Decimal(Draws::new(s, r).int(lo, hi) as i128))
}

/// The row number.
fn row_number() -> impl Leaf<u64> {
    per_row(|r| r)
}

fn int(v: i32) -> impl Leaf<i32> {
    per_row(move |_| v)
}

fn dec<const P: u8, const S: i8>(units: i128) -> impl Leaf<Decimal<P, S>> {
    per_row(move |_| Decimal(units))
}

/// One of the ids a row number decodes to: `r / DIV % M + 1` (constants,
/// so the divisions are multiplications).
fn id<const DIV: u64, const M: u64>() -> impl Leaf<i32> {
    per_row(|r| (r / DIV % M) as i32 + 1)
}

/// Random strings of uniform length in `min..=max` from `cs`.
fn rand_str(s: Stream, min: usize, max: usize, cs: Charset) -> impl Leaf<Text> {
    let avg = if max > 12 { (min + max).div_ceil(2) } else { 0 };
    text(avg, move |r, v| {
        let mut d = Draws::new(s, r);
        let len = d.int(min as i64, max as i64) as usize;
        v.push_with(len, |b| cs.fill(&mut d, b));
    })
}

/// `i_data` / `s_data`: a-string `[26 .. 50]`, 10% with `ORIGINAL` at a
/// random position.
fn data_str(s: Stream) -> impl Leaf<Text> {
    text(38, move |r, v| {
        let mut d = Draws::new(s, r);
        let len = d.int(26, 50) as usize;
        let original = below(d.next(), 10) == 0;
        let at = d.int(0, len as i64 - 8) as usize;
        v.push_with(len, |b| {
            ALNUM.fill(&mut d, b);
            if original {
                b[at..at + 8].copy_from_slice(b"ORIGINAL");
            }
        });
    })
}

/// Zip codes: 4 random digits and `11111` (4.3.2.7).
fn zip(s: Stream) -> impl Leaf<Text> {
    text(0, move |r, v| {
        let mut d = Draws::new(s, r);
        v.push_with(9, |b| {
            DIGITS.fill(&mut d, &mut b[..4]);
            b[4..].copy_from_slice(b"11111");
        });
    })
}

/// The same short string in every row.
fn word(w: &'static [u8]) -> impl Leaf<Text> {
    text(0, move |_, v| v.push(w))
}

/// `c_last` of number `n` in `0..=999` (4.3.2.3).
fn last_name(n: usize, v: &mut Views) {
    let parts = [
        SYLLABLES[n / 100],
        SYLLABLES[n / 10 % 10],
        SYLLABLES[n % 10],
    ];
    let len = parts.iter().map(|p| p.len()).sum();
    v.push_with(len, |b| {
        let mut at = 0;
        for p in parts {
            b[at..at + p.len()].copy_from_slice(p.as_bytes());
            at += p.len();
        }
    });
}

/// `o_c_id`: per district, a Fisher-Yates shuffle of `1..=3000` (one
/// permutation per district, computed once per district a batch touches).
#[derive(Clone)]
struct OrderCustomers {
    stream: Stream,
    next: u64,
}

impl OrderCustomers {
    fn perm(&self, district: u64) -> Vec<i32> {
        let mut p: Vec<i32> = (1..=CUSTOMERS as i32).collect();
        let mut d = Draws::new(self.stream, district);
        for i in (1..p.len()).rev() {
            let j = below(d.next(), i as u64 + 1) as usize;
            p.swap(i, j);
        }
        p
    }
}

impl Distribution for OrderCustomers {
    type Item = i32;

    fn sample(&mut self, _rng: &mut impl Rng) -> i32 {
        let r = self.next;
        self.next += 1;
        self.perm(r / ORDERS)[(r % ORDERS) as usize]
    }

    fn collect_into(&mut self, cols: &mut Vec<i32>, _rng: &mut impl Rng, n: usize) {
        cols.reserve(n);
        let end = self.next + n as u64;
        let mut r = self.next;
        while r < end {
            let d = r / ORDERS;
            let to = end.min((d + 1) * ORDERS);
            let p = self.perm(d);
            cols.extend_from_slice(&p[(r % ORDERS) as usize..(to - d * ORDERS) as usize]);
            r = to;
        }
        self.next = end;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}

/// A time, in microseconds since 1970 UTC.
type Micros = Timestamp<{ time_unit::MICROSECOND }>;

/// One `order_line` row.
type Line = (
    i32,
    i32,
    i32,
    i32,
    i32,
    i32,
    Option<Micros>,
    i32,
    Decimal<6, 2>,
    Text,
);

// ---------------------------------------------------------------------------
// Tables

/// Per-column streams of one database.
struct Gen {
    seed: u64,
    load_time: i64,
    c_load: i64,
}

impl Gen {
    fn new(cfg: &Tpcc) -> Self {
        Gen {
            seed: cfg.seed_value(),
            load_time: cfg.load_time_value(),
            c_load: cfg.c_load(),
        }
    }

    fn s(&self, column: &str) -> Stream {
        Stream::new(self.seed, column)
    }

    fn now(&self) -> impl Leaf<Micros> {
        let t = Timestamp(self.load_time);
        per_row(move |_| t)
    }

    /// Street 1 and 2, city, state, zip, of warehouse (`p` = `w`) or
    /// district (`d`).
    fn address(&self, p: &str) -> impl Leaf<(Text, Text, Text, Text, Text)> {
        let s = |c: &str| self.s(&format!("{p}_{c}"));
        let n = |c: &str| format!("{p}_{c}");
        (
            named(&n("street_1"), rand_str(s("street_1"), 10, 20, ALNUM)),
            named(&n("street_2"), rand_str(s("street_2"), 10, 20, ALNUM)),
            named(&n("city"), rand_str(s("city"), 10, 20, ALNUM)),
            named(&n("state"), rand_str(s("state"), 2, 2, LETTERS)),
            named(&n("zip"), zip(s("zip"))),
        )
    }

    fn warehouse(&self, rows: u64) -> Arc<dyn TableGen> {
        table(
            TpccTableName::Warehouse,
            rows,
            (
                named("w_id", per_row(|r| r as i32 + 1)),
                named("w_name", rand_str(self.s("w_name"), 6, 10, ALNUM)),
                self.address("w"),
                named("w_tax", rand_dec::<4, 4>(self.s("w_tax"), 0, 2000)),
                named("w_ytd", dec::<12, 2>(30_000_000)),
            ),
        )
    }

    fn district(&self, rows: u64) -> Arc<dyn TableGen> {
        table(
            TpccTableName::District,
            rows,
            (
                named("d_id", id::<1, DISTRICTS>()),
                named("d_w_id", id::<DISTRICTS, { u64::MAX }>()),
                named("d_name", rand_str(self.s("d_name"), 6, 10, ALNUM)),
                self.address("d"),
                named("d_tax", rand_dec::<4, 4>(self.s("d_tax"), 0, 2000)),
                named("d_ytd", dec::<12, 2>(3_000_000)),
                named("d_next_o_id", int(ORDERS as i32 + 1)),
            ),
        )
    }

    fn customer(&self, rows: u64) -> Arc<dyn TableGen> {
        let s = |c: &str| self.s(c);
        let (last, c_load) = (s("c_last"), self.c_load);
        let credit = s("c_credit");
        table(
            TpccTableName::Customer,
            rows,
            (
                (
                    named("c_id", id::<1, CUSTOMERS>()),
                    named("c_d_id", id::<CUSTOMERS, DISTRICTS>()),
                    named("c_w_id", id::<{ CUSTOMERS * DISTRICTS }, { u64::MAX }>()),
                    named("c_first", rand_str(s("c_first"), 8, 16, ALNUM)),
                    named("c_middle", word(b"OE")),
                    named(
                        "c_last",
                        text(0, move |r, v| {
                            let id = r % CUSTOMERS;
                            let num = if id < 1000 {
                                id as i64
                            } else {
                                nurand(&mut Draws::new(last, r), 255, 0, 999, c_load)
                            };
                            last_name(num as usize, v);
                        }),
                    ),
                    named("c_street_1", rand_str(s("c_street_1"), 10, 20, ALNUM)),
                    named("c_street_2", rand_str(s("c_street_2"), 10, 20, ALNUM)),
                    named("c_city", rand_str(s("c_city"), 10, 20, ALNUM)),
                    named("c_state", rand_str(s("c_state"), 2, 2, LETTERS)),
                    named("c_zip", zip(s("c_zip"))),
                ),
                (
                    named("c_phone", rand_str(s("c_phone"), 16, 16, DIGITS)),
                    named("c_since", self.now()),
                    named(
                        "c_credit",
                        text(0, move |r, v| {
                            let bad = below(Draws::new(credit, r).next(), 10) == 0;
                            v.push(if bad { b"BC" } else { b"GC" });
                        }),
                    ),
                    named("c_credit_lim", dec::<12, 2>(5_000_000)),
                    named("c_discount", rand_dec::<4, 4>(s("c_discount"), 0, 5000)),
                    named("c_balance", dec::<12, 2>(-10_00)),
                    named("c_ytd_payment", dec::<12, 2>(10_00)),
                    named("c_payment_cnt", int(1)),
                    named("c_delivery_cnt", int(0)),
                    named("c_data", rand_str(s("c_data"), 300, 500, ALNUM)),
                ),
            ),
        )
    }

    fn history(&self, rows: u64) -> Arc<dyn TableGen> {
        table(
            TpccTableName::History,
            rows,
            (
                named("h_c_id", id::<1, CUSTOMERS>()),
                named("h_c_d_id", id::<CUSTOMERS, DISTRICTS>()),
                named("h_c_w_id", id::<{ CUSTOMERS * DISTRICTS }, { u64::MAX }>()),
                named("h_d_id", id::<CUSTOMERS, DISTRICTS>()),
                named("h_w_id", id::<{ CUSTOMERS * DISTRICTS }, { u64::MAX }>()),
                named("h_date", self.now()),
                named("h_amount", dec::<6, 2>(10_00)),
                named("h_data", rand_str(self.s("h_data"), 12, 24, ALNUM)),
            ),
        )
    }

    fn new_order(&self, rows: u64) -> Arc<dyn TableGen> {
        table(
            TpccTableName::NewOrder,
            rows,
            (
                named(
                    "no_o_id",
                    per_row(|r| (FIRST_NEW_ORDER + r % NEW_ORDERS) as i32),
                ),
                named("no_d_id", id::<NEW_ORDERS, DISTRICTS>()),
                named("no_w_id", id::<{ NEW_ORDERS * DISTRICTS }, { u64::MAX }>()),
            ),
        )
    }

    /// `o_ol_cnt` per order row: in `orders`, and the line count of
    /// `order_line`.
    fn ol_cnt(&self) -> impl Leaf<i32> {
        rand_int(self.s("o_ol_cnt"), 5, 15)
    }

    fn orders(&self, rows: u64) -> Arc<dyn TableGen> {
        let carrier = self.s("o_carrier_id");
        table(
            TpccTableName::Orders,
            rows,
            (
                named("o_id", id::<1, ORDERS>()),
                named("o_d_id", id::<ORDERS, DISTRICTS>()),
                named("o_w_id", id::<{ ORDERS * DISTRICTS }, { u64::MAX }>()),
                named(
                    "o_c_id",
                    OrderCustomers {
                        stream: self.s("o_c_id"),
                        next: 0,
                    },
                ),
                named("o_entry_d", self.now()),
                named(
                    "o_carrier_id",
                    per_row(move |r| {
                        delivered(r).then(|| Draws::new(carrier, r).int(1, 10) as i32)
                    }),
                ),
                named("o_ol_cnt", self.ol_cnt()),
                named("o_all_local", int(1)),
            ),
        )
    }

    fn order_line(&self, rows: u64) -> Arc<dyn TableGen> {
        let (item, amount, dist) = (
            self.s("ol_i_id"),
            self.s("ol_amount"),
            self.s("ol_dist_info"),
        );
        let now: Micros = Timestamp(self.load_time);
        let lines = flat_map::<_, Line, _, _>(
            (row_number(), self.ol_cnt()),
            |(_, n)| *n as usize,
            move |(r, n), out| {
                let (r, n) = (*r, *n);
                let o = (r % ORDERS + 1) as i32;
                let d = (r / ORDERS % DISTRICTS) as i32 + 1;
                let w = (r / (ORDERS * DISTRICTS)) as i32 + 1;
                let del = delivered(r);
                for l in 0..n as u64 {
                    // each line's row key: order row * 16 + line (lines < 16)
                    let k = r * 16 + l;
                    out.0.push(o);
                    out.1.push(d);
                    out.2.push(w);
                    out.3.push(l as i32 + 1);
                    out.4.push(Draws::new(item, k).int(1, ITEMS as i64) as i32);
                    out.5.push(w);
                    out.6.push(del.then_some(now));
                    out.7.push(5);
                    out.8.push(Decimal(if del {
                        0
                    } else {
                        Draws::new(amount, k).int(1, 999_999) as i128
                    }));
                    let mut x = Draws::new(dist, k);
                    out.9.0.push_with(24, |b| ALNUM.fill(&mut x, b));
                }
            },
        );
        table(
            TpccTableName::OrderLine,
            rows,
            named_all(
                &[
                    "ol_o_id",
                    "ol_d_id",
                    "ol_w_id",
                    "ol_number",
                    "ol_i_id",
                    "ol_supply_w_id",
                    "ol_delivery_d",
                    "ol_quantity",
                    "ol_amount",
                    "ol_dist_info",
                ],
                lines,
            ),
        )
    }

    fn item(&self, rows: u64) -> Arc<dyn TableGen> {
        table(
            TpccTableName::Item,
            rows,
            (
                named("i_id", per_row(|r| r as i32 + 1)),
                named("i_im_id", rand_int(self.s("i_im_id"), 1, 10_000)),
                named("i_name", rand_str(self.s("i_name"), 14, 24, ALNUM)),
                named("i_price", rand_dec::<5, 2>(self.s("i_price"), 100, 10_000)),
                named("i_data", data_str(self.s("i_data"))),
            ),
        )
    }

    fn stock(&self, rows: u64) -> Arc<dyn TableGen> {
        let dist = |k: u32| {
            let name = format!("s_dist_{k:02}");
            named(&name, rand_str(self.s(&name), 24, 24, ALNUM))
        };
        table(
            TpccTableName::Stock,
            rows,
            (
                (
                    named("s_i_id", id::<1, STOCK_PER_WAREHOUSE>()),
                    named("s_w_id", id::<STOCK_PER_WAREHOUSE, { u64::MAX }>()),
                    named("s_quantity", rand_int(self.s("s_quantity"), 10, 100)),
                ),
                (
                    dist(1),
                    dist(2),
                    dist(3),
                    dist(4),
                    dist(5),
                    dist(6),
                    dist(7),
                    dist(8),
                    dist(9),
                    dist(10),
                ),
                (
                    named("s_ytd", int(0)),
                    named("s_order_cnt", int(0)),
                    named("s_remote_cnt", int(0)),
                    named("s_data", data_str(self.s("s_data"))),
                ),
            ),
        )
    }
}

/// Orders before `FIRST_NEW_ORDER` have been delivered.
fn delivered(r: u64) -> bool {
    r % ORDERS + 1 < FIRST_NEW_ORDER
}
