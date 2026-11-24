//! Each sales table and its returns from one pass, built straight into
//! Arrow columns.
//!
//! dsbgen's returns generators replay every sales line item to find the
//! ~10% that are returned, so generating both tables costs the sales work
//! twice. Here one loop per channel (store, catalog, web) walks the orders
//! once and emits both tables. The loops are ports of dsbgen's
//! `{Store,Catalog,Web}{Sales,Returns}RowGenerator` (Apache-2.0, from the
//! tpchgen-rs project, itself a port of dsdgen). They draw from the same
//! random streams in the same order, using dsbgen's public helpers, so
//! the output is identical; the tests compare every channel with
//! dsbgen-arrow, both tables together and each alone.
//!
//! Columns are pushed into per-column buffers as each line item is made,
//! instead of collecting row structs and converting them column by column.

use crate::TableName;
use crate::dat;
use crate::seeds;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use std::sync::Arc;
use dsbgen::config::{Session, Table as Ds};
use dsbgen::distribution::hours_distribution::HoursWeights;
use dsbgen::generator::CatalogReturnsGeneratorColumn as CR;
use dsbgen::generator::CatalogSalesGeneratorColumn as CS;
use dsbgen::generator::StoreReturnsGeneratorColumn as R;
use dsbgen::generator::StoreSalesGeneratorColumn as S;
use dsbgen::generator::WebReturnsGeneratorColumn as WR;
use dsbgen::generator::WebSalesGeneratorColumn as WS;
use dsbgen::join_key_utils::{
    CS_MAX_SHIP_DELAY, CS_MIN_SHIP_DELAY, WS_MAX_SHIP_DELAY, WS_MIN_SHIP_DELAY,
    generate_catalog_page_join_key, generate_returns_date_join_key, generate_sales_date_join_key,
    generate_scd_join_key, generate_time_join_key, generate_uniform_join_key,
};
use dsbgen::nulls::create_null_bit_map;
use dsbgen::permutations::{get_permutation_entry, make_permutation};
use dsbgen::random::RandomValueGenerator;
use dsbgen::row::AbstractRowGenerator;
use dsbgen::slowly_changing_dimension_utils::match_surrogate_key;
use dsbgen::table::Table as NullTable;
use dsbgen::types::{
    Date, Decimal, Pricing, generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_catalog_sales_pricing_limits, get_store_sales_pricing_limits, get_web_sales_pricing_limits,
};
use dsbgen_arrow::{
    CatalogReturnsArrow, CatalogSalesArrow, StoreReturnsArrow, StoreSalesArrow, WebReturnsArrow,
    WebSalesArrow,
};

/// Percentage of line items that are returned.
const SR_RETURN_PCT: i32 = 10;
/// Percentage of returns made by the customer who bought the item.
const SR_SAME_CUSTOMER: i32 = 80;

/// One nullable column being built.
struct Col<T> {
    values: Vec<T>,
    valid: BooleanBufferBuilder,
}

impl<T: arrow_buffer::ArrowNativeType> Col<T> {
    fn new(cap: usize) -> Self {
        Col {
            values: Vec::with_capacity(cap),
            valid: BooleanBufferBuilder::new(cap),
        }
    }

    #[inline(always)]
    fn push(&mut self, v: T, valid: bool) {
        self.values.push(v);
        self.valid.append(valid);
    }

    fn finish<A>(&mut self, cap: usize) -> PrimitiveArray<A>
    where
        A: arrow_array::ArrowPrimitiveType<Native = T>,
    {
        let values = std::mem::replace(&mut self.values, Vec::with_capacity(cap));
        let nulls = NullBuffer::new(self.valid.finish());
        PrimitiveArray::new(ScalarBuffer::from(values), Some(nulls))
    }
}

#[inline]
fn bit(nbm: i64, pos: u32) -> bool {
    (nbm >> pos) & 1 == 0
}

/// Columns of a batch of one table: `I` Int32, `L` Int64, `D` Decimal(7,2),
/// in schema order given by `layout`.
struct Builder {
    schema: SchemaRef,
    layout: &'static [Kind],
    i32s: Vec<Col<i32>>,
    i64s: Vec<Col<i64>>,
    decs: Vec<Col<i128>>,
    rows: usize,
    cap: usize,
    /// The next column of each kind in the row being pushed.
    next: [usize; 3],
}

#[derive(Clone, Copy)]
enum Kind {
    I,
    L,
    D,
}

impl Builder {
    fn new(schema: SchemaRef, layout: &'static [Kind], cap: usize) -> Self {
        let count = |k: fn(&Kind) -> bool| layout.iter().filter(|x| k(x)).count();
        Builder {
            schema,
            layout,
            i32s: (0..count(|k| matches!(k, Kind::I)))
                .map(|_| Col::new(cap))
                .collect(),
            i64s: (0..count(|k| matches!(k, Kind::L)))
                .map(|_| Col::new(cap))
                .collect(),
            decs: (0..count(|k| matches!(k, Kind::D)))
                .map(|_| Col::new(cap))
                .collect(),
            rows: 0,
            cap,
            next: [0; 3],
        }
    }

    fn finish(&mut self) -> RecordBatch {
        let (mut i, mut l, mut d) = (0, 0, 0);
        let cols: Vec<ArrayRef> = self
            .layout
            .iter()
            .map(|k| -> ArrayRef {
                match k {
                    Kind::I => {
                        i += 1;
                        Arc::new(self.i32s[i - 1].finish::<Int32Type>(self.cap))
                    }
                    Kind::L => {
                        l += 1;
                        Arc::new(self.i64s[l - 1].finish::<Int64Type>(self.cap))
                    }
                    Kind::D => {
                        d += 1;
                        Arc::new(
                            self.decs[d - 1]
                                .finish::<Decimal128Type>(self.cap)
                                .with_precision_and_scale(7, 2)
                                .expect("decimal(7,2)"),
                        )
                    }
                }
            })
            .collect();
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), cols).expect("columns match schema")
    }
}

use Kind::{D, I, L};

/// Where a channel's rows go: one row's values in schema order, then
/// [`Row::end`]. [`Builder`] makes Arrow columns of them, [`Text`] dsdgen's
/// `.dat` lines.
pub(crate) trait Row {
    /// An Int32 key, NULL by bit `pos` of `nbm` or by being negative.
    fn sk(&mut self, nbm: i64, pos: u32, v: i64);
    fn int(&mut self, v: i32, valid: bool);
    fn long(&mut self, v: i64, valid: bool);
    /// A decimal(7,2), NULL by bit `pos` of `nbm`.
    fn dec(&mut self, nbm: i64, pos: u32, v: Decimal);
    fn end(&mut self);
}

impl Row for Builder {
    #[inline(always)]
    fn sk(&mut self, nbm: i64, pos: u32, v: i64) {
        self.int(v as i32, bit(nbm, pos) && v >= 0);
    }

    #[inline(always)]
    fn int(&mut self, v: i32, valid: bool) {
        let c = &mut self.i32s[self.next[0]];
        self.next[0] += 1;
        c.push(v, valid);
    }

    #[inline(always)]
    fn long(&mut self, v: i64, valid: bool) {
        let c = &mut self.i64s[self.next[1]];
        self.next[1] += 1;
        c.push(v, valid);
    }

    #[inline(always)]
    fn dec(&mut self, nbm: i64, pos: u32, v: Decimal) {
        let c = &mut self.decs[self.next[2]];
        self.next[2] += 1;
        c.push(v.get_number() as i128, bit(nbm, pos));
    }

    #[inline(always)]
    fn end(&mut self) {
        self.next = [0; 3];
        self.rows += 1;
    }
}

/// `.dat` lines, as [`dat::write_batch`] writes the same columns.
struct Text<'a>(&'a mut Vec<u8>);

impl Row for Text<'_> {
    #[inline(always)]
    fn sk(&mut self, nbm: i64, pos: u32, v: i64) {
        self.int(v as i32, bit(nbm, pos) && v >= 0);
    }

    #[inline(always)]
    fn int(&mut self, v: i32, valid: bool) {
        self.long(v as i64, valid);
    }

    #[inline(always)]
    fn long(&mut self, v: i64, valid: bool) {
        if valid {
            dat::int_field(self.0, v);
        } else {
            self.0.push(b'|');
        }
    }

    #[inline(always)]
    fn dec(&mut self, nbm: i64, pos: u32, v: Decimal) {
        if bit(nbm, pos) {
            dat::decimal2_field(self.0, v.get_number());
        } else {
            self.0.push(b'|');
        }
    }

    #[inline(always)]
    fn end(&mut self) {
        self.0.push(b'\n');
    }
}

/// A channel's two outputs, sales (0) and returns (1).
pub(crate) trait Out {
    type R: Row;
    fn row(&mut self, k: usize) -> &mut Self::R;
    /// A row of output `k` is complete.
    fn done(&mut self, k: usize);
    /// The chunk is complete.
    fn finish(&mut self);
}

/// Arrow batches of up to `batch` rows, passed to `emit` as they fill.
struct ArrowOut<'a> {
    b: [Builder; 2],
    batch: usize,
    emit: &'a mut dyn FnMut(usize, RecordBatch),
}

impl Out for ArrowOut<'_> {
    type R = Builder;

    #[inline]
    fn row(&mut self, k: usize) -> &mut Builder {
        &mut self.b[k]
    }

    #[inline]
    fn done(&mut self, k: usize) {
        if self.b[k].rows == self.batch {
            let b = self.b[k].finish();
            (self.emit)(k, b);
        }
    }

    fn finish(&mut self) {
        for k in 0..2 {
            if self.b[k].rows > 0 {
                let b = self.b[k].finish();
                (self.emit)(k, b);
            }
        }
    }
}

/// `.dat` text appended to two buffers.
struct TextOut<'a>([Text<'a>; 2]);

impl<'a> Out for TextOut<'a> {
    type R = Text<'a>;

    #[inline]
    fn row(&mut self, k: usize) -> &mut Text<'a> {
        &mut self.0[k]
    }

    #[inline]
    fn done(&mut self, _: usize) {}

    fn finish(&mut self) {}
}

const SALES: &[Kind] = &[
    I, I, I, I, I, I, I, I, I, L, I, D, D, D, D, D, D, D, D, D, D, D, D,
];
const RETURNS: &[Kind] = &[I, I, I, I, I, I, I, I, I, L, I, D, D, D, D, D, D, D, D, D];
const CATALOG_SALES: &[Kind] = &[
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, L, I, D, D, D, D, D, D, D, D, D, D, D, D, D,
    D, D,
];
const WEB_SALES: &[Kind] = CATALOG_SALES;
const WEB_RETURNS: &[Kind] = &[
    I, I, I, I, I, I, I, I, I, I, I, I, I, L, I, D, D, D, D, D, D, D, D, D,
];
const CATALOG_RETURNS: &[Kind] = &[
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, L, I, D, D, D, D, D, D, D, D, D,
];

/// The order-level values shared by a ticket's line items.
#[derive(Default, Clone, Copy)]
struct Order {
    store: i64,
    time: i64,
    date: i64,
    customer: i64,
    cdemo: i64,
    hdemo: i64,
    addr: i64,
    ticket: i64,
}

/// A sales channel: a sales table and its returns, generated together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Store,
    Catalog,
    Web,
}

impl Channel {
    /// The channel of a sales or returns table, and whether it is the sales one.
    pub(crate) fn of(t: TableName) -> Option<(Channel, bool)> {
        use TableName::*;
        Some(match t {
            StoreSales => (Channel::Store, true),
            StoreReturns => (Channel::Store, false),
            CatalogSales => (Channel::Catalog, true),
            CatalogReturns => (Channel::Catalog, false),
            WebSales => (Channel::Web, true),
            WebReturns => (Channel::Web, false),
            _ => return None,
        })
    }

    /// Sales (output 0) and/or returns (output 1) for source rows
    /// `first..=last` (1-based), in batches of up to `batch` rows.
    pub(crate) fn run(
        self,
        session: &Session,
        first: u64,
        last: u64,
        batch: usize,
        want: Want,
        emit: &mut dyn FnMut(usize, RecordBatch),
    ) {
        let (s, sl, r, rl) = match self {
            Channel::Store => (
                StoreSalesArrow::schema_ref(),
                SALES,
                StoreReturnsArrow::schema_ref(),
                RETURNS,
            ),
            Channel::Catalog => (
                CatalogSalesArrow::schema_ref(),
                CATALOG_SALES,
                CatalogReturnsArrow::schema_ref(),
                CATALOG_RETURNS,
            ),
            Channel::Web => (
                WebSalesArrow::schema_ref(),
                WEB_SALES,
                WebReturnsArrow::schema_ref(),
                WEB_RETURNS,
            ),
        };
        let mut out = ArrowOut {
            b: [Builder::new(s, sl, batch), Builder::new(r, rl, batch)],
            batch,
            emit,
        };
        self.run_into(session, first, last, want, &mut out);
    }

    /// [`Self::run`] as `.dat` text, appended to `sales` and `returns`:
    /// the bytes [`dat::write_batch`] makes of its batches, without
    /// building them.
    pub(crate) fn run_dat(
        self,
        session: &Session,
        first: u64,
        last: u64,
        want: Want,
        sales: &mut Vec<u8>,
        returns: &mut Vec<u8>,
    ) {
        let mut out = TextOut([Text(sales), Text(returns)]);
        self.run_into(session, first, last, want, &mut out);
    }

    fn run_into(self, session: &Session, first: u64, last: u64, want: Want, out: &mut impl Out) {
        match self {
            Channel::Store => store_channel(session, first, last, want, out),
            Channel::Catalog => catalog_channel(session, first, last, want, out),
            Channel::Web => web_channel(session, first, last, want, out),
        }
    }
}

/// Which tables of a channel to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Want {
    pub sales: bool,
    pub returns: bool,
}

/// Generate `store_sales` (output 0) and/or `store_returns` (output 1) for
/// tickets `first..=last` (1-based), in batches of up to `batch` rows each.
fn store_channel(session: &Session, first: u64, last: u64, want: Want, out: &mut impl Out) {
    let scaling = session.get_scaling();
    let tickets = scaling.get_row_count(Ds::StoreSales);
    let mut sg = AbstractRowGenerator::<S>::new(tickets);
    sg.set_source_row_range(first, last);
    let mut rg = AbstractRowGenerator::<R>::new(tickets);
    rg.set_source_row_range(first, last);

    let item_count = scaling.get_id_count(Ds::Item) as usize;
    let limits = get_store_sales_pricing_limits();
    let permutation = make_permutation(item_count, sg.get_random_number_stream(&S::SsPermutation));

    for row in first..=last.min(tickets) {
        let o = Order {
            store: generate_scd_join_key(
                Ds::Store,
                sg.get_random_number_stream(&S::SsSoldStoreSk),
                1,
                scaling,
            ),
            time: generate_time_join_key(
                HoursWeights::Store,
                sg.get_random_number_stream(&S::SsSoldTimeSk),
            ),
            date: generate_sales_date_join_key(sg.get_random_number_stream(&S::SsSoldDateSk)),
            customer: generate_uniform_join_key(
                Ds::Customer,
                sg.get_random_number_stream(&S::SsSoldCustomerSk),
                scaling,
            ),
            cdemo: generate_uniform_join_key(
                Ds::CustomerDemographics,
                sg.get_random_number_stream(&S::SsSoldCdemoSk),
                scaling,
            ),
            hdemo: generate_uniform_join_key(
                Ds::HouseholdDemographics,
                sg.get_random_number_stream(&S::SsSoldHdemoSk),
                scaling,
            ),
            addr: generate_uniform_join_key(
                Ds::CustomerAddress,
                sg.get_random_number_stream(&S::SsSoldAddrSk),
                scaling,
            ),
            ticket: row as i64,
        };
        let lines = RandomValueGenerator::generate_uniform_random_int(
            8,
            16,
            sg.get_random_number_stream(&S::SsTicketNumber),
        );
        let mut item_index = RandomValueGenerator::generate_uniform_random_int(
            1,
            item_count as i32,
            sg.get_random_number_stream(&S::SsSoldItemSk),
        );
        for _ in 0..lines {
            // items are distinct within a ticket: consecutive permutation entries
            item_index += 1;
            if item_index > item_count as i32 {
                item_index = 1;
            }
            let item_key = get_permutation_entry(&permutation, item_index);
            let item = match_surrogate_key(item_key as i64, o.date, Ds::Item, scaling);
            let returned = RandomValueGenerator::generate_uniform_random_int(
                0,
                99,
                sg.get_random_number_stream(&S::SrIsReturned),
            ) < SR_RETURN_PCT;

            // the sales row (its draws are needed even when only returns are wanted)
            let nbm = create_null_bit_map(
                NullTable::StoreSales,
                sg.get_random_number_stream(&S::SsNulls),
            );
            let promo = generate_uniform_join_key(
                Ds::Promotion,
                sg.get_random_number_stream(&S::SsSoldPromoSk),
                scaling,
            );
            let pricing = generate_pricing_for_sales_table(
                &limits,
                sg.get_random_number_stream(&S::SsPricing),
            );
            if want.sales {
                push_sale(out.row(0), nbm, &o, item, promo, &pricing);
                out.done(0);
            }
            if returned && want.returns {
                push_return(out.row(1), &mut rg, scaling, &o, item, &pricing);
                out.done(1);
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    out.finish();
}

fn push_sale(b: &mut impl Row, nbm: i64, o: &Order, item: i64, promo: i64, p: &Pricing) {
    b.sk(nbm, 0, o.date);
    b.sk(nbm, 1, o.time);
    b.sk(nbm, 2, item);
    b.sk(nbm, 3, o.customer);
    b.sk(nbm, 4, o.cdemo);
    b.sk(nbm, 5, o.hdemo);
    b.sk(nbm, 6, o.addr);
    b.sk(nbm, 7, o.store);
    b.sk(nbm, 8, promo);
    b.long(o.ticket, bit(nbm, 9) && o.ticket >= 0);
    b.int(p.get_quantity(), bit(nbm, 10));
    b.dec(nbm, 11, p.get_wholesale_cost());
    b.dec(nbm, 12, p.get_list_price());
    b.dec(nbm, 13, p.get_sales_price());
    // dsdgen prints the coupon amount as ss_ext_discount_amt (a bug kept for
    // compatibility), and again as ss_coupon_amt
    b.dec(nbm, 14, p.get_coupon_amount());
    b.dec(nbm, 15, p.get_ext_sales_price());
    b.dec(nbm, 16, p.get_ext_wholesale_cost());
    b.dec(nbm, 17, p.get_ext_list_price());
    b.dec(nbm, 18, p.get_ext_tax());
    b.dec(nbm, 14, p.get_coupon_amount());
    b.dec(nbm, 19, p.get_net_paid());
    b.dec(nbm, 20, p.get_net_paid_including_tax());
    b.dec(nbm, 21, p.get_net_profit());
    b.end();
}

fn push_return(
    b: &mut impl Row,
    rg: &mut AbstractRowGenerator<R>,
    scaling: &dsbgen::config::Scaling,
    o: &Order,
    item: i64,
    sale: &Pricing,
) {
    let nbm = create_null_bit_map(
        NullTable::StoreReturns,
        rg.get_random_number_stream(&R::SrNulls),
    );
    let mut customer = generate_uniform_join_key(
        Ds::Customer,
        rg.get_random_number_stream(&R::SrCustomerSk),
        scaling,
    );
    if RandomValueGenerator::generate_uniform_random_int(
        1,
        100,
        rg.get_random_number_stream(&R::SrTicketNumber),
    ) < SR_SAME_CUSTOMER
    {
        customer = o.customer;
    }
    let date = generate_returns_date_join_key(
        rg.get_random_number_stream(&R::SrReturnedDateSk),
        o.date,
        CS_MIN_SHIP_DELAY,
        CS_MAX_SHIP_DELAY,
    );
    let time = RandomValueGenerator::generate_uniform_random_int(
        8 * 3600 - 1,
        17 * 3600 - 1,
        rg.get_random_number_stream(&R::SrReturnedTimeSk),
    ) as i64;
    let cdemo = generate_uniform_join_key(
        Ds::CustomerDemographics,
        rg.get_random_number_stream(&R::SrCdemoSk),
        scaling,
    );
    let hdemo = generate_uniform_join_key(
        Ds::HouseholdDemographics,
        rg.get_random_number_stream(&R::SrHdemoSk),
        scaling,
    );
    let addr = generate_uniform_join_key(
        Ds::CustomerAddress,
        rg.get_random_number_stream(&R::SrAddrSk),
        scaling,
    );
    let store = generate_scd_join_key(
        Ds::Store,
        rg.get_random_number_stream(&R::SrStoreSk),
        1,
        scaling,
    );
    let reason = generate_uniform_join_key(
        Ds::Reason,
        rg.get_random_number_stream(&R::SrReasonSk),
        scaling,
    );
    let quantity = RandomValueGenerator::generate_uniform_random_int(
        1,
        sale.get_quantity(),
        rg.get_random_number_stream(&R::SrPricing),
    );
    let p = generate_pricing_for_returns_table(
        rg.get_random_number_stream(&R::SrPricing),
        quantity,
        sale,
    );

    b.sk(nbm, 0, date);
    b.sk(nbm, 1, time);
    b.sk(nbm, 2, item);
    b.sk(nbm, 3, customer);
    b.sk(nbm, 4, cdemo);
    b.sk(nbm, 5, hdemo);
    b.sk(nbm, 6, addr);
    b.sk(nbm, 7, store);
    b.sk(nbm, 8, reason);
    b.long(o.ticket, bit(nbm, 9) && o.ticket >= 0);
    b.int(p.get_quantity(), bit(nbm, 10));
    b.dec(nbm, 11, p.get_net_paid());
    b.dec(nbm, 12, p.get_ext_tax());
    b.dec(nbm, 13, p.get_net_paid_including_tax());
    b.dec(nbm, 14, p.get_fee());
    b.dec(nbm, 15, p.get_ext_ship_cost());
    b.dec(nbm, 16, p.get_refunded_cash());
    b.dec(nbm, 17, p.get_reversed_charge());
    b.dec(nbm, 18, p.get_store_credit());
    b.dec(nbm, 19, p.get_net_loss());
    b.end();
}

/// Percentage of catalog orders shipped to another customer.
const CS_GIFT_PCT: i32 = 10;
/// Percentage of catalog returns made by the customer it was shipped to.
const CR_GIFT_PCT: i32 = 10;

/// A catalog order's values shared by its line items.
#[derive(Default, Clone, Copy)]
struct ChannelOrder {
    date: i64,
    time: i64,
    call_center: i64,
    bill: [i64; 4],
    ship: [i64; 4],
    number: i64,
}

/// A catalog line item's sales values a return needs.
struct LineSale {
    ship_date: i64,
    /// The catalog page, or the web page.
    page: i64,
    item: i64,
    pricing: Pricing,
}

/// Generate `catalog_sales` (output 0) and/or `catalog_returns` (output 1)
/// for orders `first..=last` (1-based): a port of dsbgen's
/// `CatalogSalesRowGenerator` and `CatalogReturnsRowGenerator`, which
/// replays every sales line item to find the returned ones.
fn catalog_channel(session: &Session, first: u64, last: u64, want: Want, out: &mut impl Out) {
    let scaling = session.get_scaling();
    let orders = scaling.get_row_count(Ds::CatalogSales);
    let mut sg = AbstractRowGenerator::<CS>::new(orders);
    sg.set_source_row_range(first, last);
    let mut rg = AbstractRowGenerator::<CR>::new(orders);
    rg.set_source_row_range(first, last);

    let item_count = scaling.get_id_count(Ds::Item) as usize;
    let limits = get_catalog_sales_pricing_limits();
    let permutation = make_permutation(item_count, sg.get_random_number_stream(&CS::CsPermute));
    // the sold date advances with the row number, from the start of the data
    let mut julian = Date::JULIAN_DATA_START_DATE;
    let mut next_date = scaling.get_row_count_for_date(Ds::CatalogSales, julian) + 1;

    for row in first..=last.min(orders) {
        while row > next_date {
            julian += 1;
            next_date += scaling.get_row_count_for_date(Ds::CatalogSales, julian);
        }
        let date = julian;
        let time = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            sg.get_random_number_stream(&CS::CsSoldTimeSk),
        );
        let call_center = if date == -1 {
            -1
        } else {
            generate_scd_join_key(
                Ds::CallCenter,
                sg.get_random_number_stream(&CS::CsCallCenterSk),
                date,
                scaling,
            )
        };
        let uniform = |t, c: &CS, sg: &mut AbstractRowGenerator<CS>| {
            generate_uniform_join_key(t, sg.get_random_number_stream(c), scaling)
        };
        let bill = [
            uniform(Ds::Customer, &CS::CsBillCustomerSk, &mut sg),
            uniform(Ds::CustomerDemographics, &CS::CsBillCdemoSk, &mut sg),
            uniform(Ds::HouseholdDemographics, &CS::CsBillHdemoSk, &mut sg),
            uniform(Ds::CustomerAddress, &CS::CsBillAddrSk, &mut sg),
        ];
        let gift = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            sg.get_random_number_stream(&CS::CsShipCustomerSk),
        ) <= CS_GIFT_PCT;
        let ship = if gift {
            [
                uniform(Ds::Customer, &CS::CsShipCustomerSk, &mut sg),
                uniform(Ds::CustomerDemographics, &CS::CsShipCdemoSk, &mut sg),
                uniform(Ds::HouseholdDemographics, &CS::CsShipHdemoSk, &mut sg),
                uniform(Ds::CustomerAddress, &CS::CsShipAddrSk, &mut sg),
            ]
        } else {
            bill
        };
        let o = ChannelOrder {
            date,
            time,
            call_center,
            bill,
            ship,
            number: row as i64,
        };
        let mut item_index = RandomValueGenerator::generate_uniform_random_int(
            1,
            item_count as i32,
            sg.get_random_number_stream(&CS::CsSoldItemSk),
        );
        let lines = RandomValueGenerator::generate_uniform_random_int(
            4,
            14,
            sg.get_random_number_stream(&CS::CsOrderNumber),
        );
        for _ in 0..lines {
            item_index += 1;
            if item_index > item_count as i32 {
                item_index = 1;
            }
            let item_key = get_permutation_entry(&permutation, item_index);
            let item = match_surrogate_key(item_key as i64, o.date, Ds::Item, scaling);
            let returned = RandomValueGenerator::generate_uniform_random_int(
                0,
                99,
                sg.get_random_number_stream(&CS::CrIsReturned),
            ) < SR_RETURN_PCT;

            let nbm = create_null_bit_map(
                NullTable::CatalogSales,
                sg.get_random_number_stream(&CS::CsNulls),
            );
            let lag = RandomValueGenerator::generate_uniform_random_int(
                CS_MIN_SHIP_DELAY,
                CS_MAX_SHIP_DELAY,
                sg.get_random_number_stream(&CS::CsShipDateSk),
            );
            let ship_date = if o.date == -1 {
                -1
            } else {
                o.date + lag as i64
            };
            let catalog_page = if o.date == -1 {
                -1
            } else {
                generate_catalog_page_join_key(
                    sg.get_random_number_stream(&CS::CsCatalogPageSk),
                    o.date,
                    scaling,
                )
            };
            let ship_mode = uniform(Ds::ShipMode, &CS::CsShipModeSk, &mut sg);
            let warehouse = uniform(Ds::Warehouse, &CS::CsWarehouseSk, &mut sg);
            let promo = uniform(Ds::Promotion, &CS::CsPromoSk, &mut sg);
            let pricing = generate_pricing_for_sales_table(
                &limits,
                sg.get_random_number_stream(&CS::CsPricing),
            );
            let sale = LineSale {
                ship_date,
                page: catalog_page,
                item,
                pricing,
            };
            if want.sales {
                push_catalog_sale(out.row(0), nbm, &o, &sale, ship_mode, warehouse, promo);
                out.done(0);
            }
            if returned && want.returns {
                push_catalog_return(out.row(1), &mut rg, scaling, &o, &sale);
                out.done(1);
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    out.finish();
}

fn push_catalog_sale(
    b: &mut impl Row,
    nbm: i64,
    o: &ChannelOrder,
    s: &LineSale,
    ship_mode: i64,
    warehouse: i64,
    promo: i64,
) {
    let keys = [
        o.date,
        o.time,
        s.ship_date,
        o.bill[0],
        o.bill[1],
        o.bill[2],
        o.bill[3],
        o.ship[0],
        o.ship[1],
        o.ship[2],
        o.ship[3],
        o.call_center,
        s.page,
        ship_mode,
        warehouse,
        s.item,
        promo,
    ];
    for (pos, k) in keys.into_iter().enumerate() {
        b.sk(nbm, pos as u32, k);
    }
    b.long(o.number, bit(nbm, 17));
    let p = &s.pricing;
    b.int(p.get_quantity(), bit(nbm, 18));
    b.dec(nbm, 19, p.get_wholesale_cost());
    b.dec(nbm, 20, p.get_list_price());
    b.dec(nbm, 21, p.get_sales_price());
    b.dec(nbm, 24, p.get_ext_discount_amount());
    b.dec(nbm, 23, p.get_ext_sales_price());
    b.dec(nbm, 25, p.get_ext_wholesale_cost());
    b.dec(nbm, 26, p.get_ext_list_price());
    b.dec(nbm, 27, p.get_ext_tax());
    b.dec(nbm, 22, p.get_coupon_amount());
    b.dec(nbm, 28, p.get_ext_ship_cost());
    b.dec(nbm, 29, p.get_net_paid());
    b.dec(nbm, 30, p.get_net_paid_including_tax());
    b.dec(nbm, 31, p.get_net_paid_including_shipping());
    b.dec(nbm, 32, p.get_net_paid_including_shipping_and_tax());
    b.dec(nbm, 33, p.get_net_profit());
    b.end();
}

fn push_catalog_return(
    b: &mut impl Row,
    rg: &mut AbstractRowGenerator<CR>,
    scaling: &dsbgen::config::Scaling,
    o: &ChannelOrder,
    s: &LineSale,
) {
    let nbm = create_null_bit_map(
        NullTable::CatalogReturns,
        rg.get_random_number_stream(&CR::CrNulls),
    );
    let uniform = |t, c: &CR, rg: &mut AbstractRowGenerator<CR>| {
        generate_uniform_join_key(t, rg.get_random_number_stream(c), scaling)
    };
    let mut returning = [
        uniform(Ds::Customer, &CR::CrReturningCustomerSk, rg),
        uniform(Ds::CustomerDemographics, &CR::CrReturningCdemoSk, rg),
        uniform(Ds::HouseholdDemographics, &CR::CrReturningHdemoSk, rg),
        uniform(Ds::CustomerAddress, &CR::CrReturningAddrSk, rg),
    ];
    if RandomValueGenerator::generate_uniform_random_int(
        0,
        99,
        rg.get_random_number_stream(&CR::CrReturningCustomerSk),
    ) < CR_GIFT_PCT
    {
        // the ship customer returns it; the hdemo is not on the sale
        returning[0] = o.ship[0];
        returning[1] = o.ship[1];
        returning[3] = o.ship[3];
    }
    let sold = s.pricing.get_quantity();
    let quantity = if sold == -1 {
        sold
    } else {
        RandomValueGenerator::generate_uniform_random_int(
            1,
            sold,
            rg.get_random_number_stream(&CR::CrPricing),
        )
    };
    let p = generate_pricing_for_returns_table(
        rg.get_random_number_stream(&CR::CrPricing),
        quantity,
        &s.pricing,
    );
    let date = generate_returns_date_join_key(
        rg.get_random_number_stream(&CR::CrReturnedDateSk),
        s.ship_date,
        CS_MIN_SHIP_DELAY,
        CS_MAX_SHIP_DELAY,
    );
    let time = generate_time_join_key(
        HoursWeights::CatalogAndWeb,
        rg.get_random_number_stream(&CR::CrReturnedTimeSk),
    );
    let ship_mode = uniform(Ds::ShipMode, &CR::CrShipModeSk, rg);
    let warehouse = uniform(Ds::Warehouse, &CR::CrWarehouseSk, rg);
    let reason = uniform(Ds::Reason, &CR::CrReasonSk, rg);

    let keys = [
        date,
        time,
        s.item,
        o.bill[0],
        o.bill[1],
        o.bill[2],
        o.bill[3],
        returning[0],
        returning[1],
        returning[2],
        returning[3],
        o.call_center,
        s.page,
        ship_mode,
        warehouse,
        reason,
    ];
    for (pos, k) in keys.into_iter().enumerate() {
        b.sk(nbm, pos as u32, k);
    }
    b.long(o.number, bit(nbm, 16));
    b.int(p.get_quantity(), bit(nbm, 17));
    b.dec(nbm, 18, p.get_net_paid());
    b.dec(nbm, 19, p.get_ext_tax());
    b.dec(nbm, 20, p.get_net_paid_including_tax());
    b.dec(nbm, 21, p.get_fee());
    b.dec(nbm, 22, p.get_ext_ship_cost());
    b.dec(nbm, 23, p.get_refunded_cash());
    b.dec(nbm, 24, p.get_reversed_charge());
    b.dec(nbm, 25, p.get_store_credit());
    b.dec(nbm, 26, p.get_net_loss());
    b.end();
}

/// Web orders shipped to the billed customer: those drawing at most this
/// (of 0..99) in dsbgen; returns by the ship customer: below it.
const WEB_GIFT_PCT: i32 = 7;

/// Generate `web_sales` (output 0) and/or `web_returns` (output 1) for
/// orders `first..=last` (1-based): a port of dsbgen's
/// `WebSalesRowGenerator` and `WebReturnsRowGenerator`.
fn web_channel(session: &Session, first: u64, last: u64, want: Want, out: &mut impl Out) {
    let scaling = session.get_scaling();
    let orders = scaling.get_row_count(Ds::WebSales);
    let mut sg = AbstractRowGenerator::<WS>::new(orders);
    sg.set_source_row_range(first, last);
    let mut rg = AbstractRowGenerator::<WR>::new(orders);
    rg.set_source_row_range(first, last);

    let item_count = scaling.get_id_count(Ds::Item) as usize;
    let limits = get_web_sales_pricing_limits();
    let permutation = make_permutation(item_count, sg.get_random_number_stream(&WS::WsPermutation));

    let uniform = |t, c: &WS, sg: &mut AbstractRowGenerator<WS>| {
        generate_uniform_join_key(t, sg.get_random_number_stream(c), scaling)
    };

    for row in first..=last.min(orders) {
        let date = generate_sales_date_join_key(sg.get_random_number_stream(&WS::WsSoldDateSk));
        let time = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            sg.get_random_number_stream(&WS::WsSoldTimeSk),
        );
        let bill = [
            uniform(Ds::Customer, &WS::WsBillCustomerSk, &mut sg),
            uniform(Ds::CustomerDemographics, &WS::WsBillCdemoSk, &mut sg),
            uniform(Ds::HouseholdDemographics, &WS::WsBillHdemoSk, &mut sg),
            uniform(Ds::CustomerAddress, &WS::WsBillAddrSk, &mut sg),
        ];
        let elsewhere = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            sg.get_random_number_stream(&WS::WsShipCustomerSk),
        ) > WEB_GIFT_PCT;
        let ship = if elsewhere {
            [
                uniform(Ds::Customer, &WS::WsShipCustomerSk, &mut sg),
                uniform(Ds::CustomerDemographics, &WS::WsShipCdemoSk, &mut sg),
                uniform(Ds::HouseholdDemographics, &WS::WsShipHdemoSk, &mut sg),
                uniform(Ds::CustomerAddress, &WS::WsShipAddrSk, &mut sg),
            ]
        } else {
            bill
        };
        let o = ChannelOrder {
            date,
            time,
            call_center: 0,
            bill,
            ship,
            number: row as i64,
        };
        let mut item_index = RandomValueGenerator::generate_uniform_random_int(
            1,
            item_count as i32,
            sg.get_random_number_stream(&WS::WsItemSk),
        );
        let lines = RandomValueGenerator::generate_uniform_random_int(
            8,
            16,
            sg.get_random_number_stream(&WS::WsOrderNumber),
        );
        for _ in 0..lines {
            item_index += 1;
            if item_index > item_count as i32 {
                item_index = 1;
            }
            let item_key = get_permutation_entry(&permutation, item_index);
            let item = match_surrogate_key(item_key as i64, o.date, Ds::Item, scaling);
            let returned = RandomValueGenerator::generate_uniform_random_int(
                0,
                99,
                sg.get_random_number_stream(&WS::WrIsReturned),
            ) < SR_RETURN_PCT;

            let nbm = create_null_bit_map(
                NullTable::WebSales,
                sg.get_random_number_stream(&WS::WsNulls),
            );
            let lag = RandomValueGenerator::generate_uniform_random_int(
                1,
                120,
                sg.get_random_number_stream(&WS::WsShipDateSk),
            );
            let ship_date = o.date + lag as i64;
            let web_page = generate_scd_join_key(
                Ds::WebPage,
                sg.get_random_number_stream(&WS::WsWebPageSk),
                o.date,
                scaling,
            );
            let web_site = generate_scd_join_key(
                Ds::WebSite,
                sg.get_random_number_stream(&WS::WsWebSiteSk),
                o.date,
                scaling,
            );
            let ship_mode = uniform(Ds::ShipMode, &WS::WsShipModeSk, &mut sg);
            let warehouse = uniform(Ds::Warehouse, &WS::WsWarehouseSk, &mut sg);
            let promo = uniform(Ds::Promotion, &WS::WsPromoSk, &mut sg);
            let pricing = generate_pricing_for_sales_table(
                &limits,
                sg.get_random_number_stream(&WS::WsPricing),
            );
            let sale = LineSale {
                ship_date,
                page: web_page,
                item,
                pricing,
            };
            if want.sales {
                push_web_sale(
                    out.row(0),
                    nbm,
                    &o,
                    &sale,
                    [web_site, ship_mode, warehouse, promo],
                );
                out.done(0);
            }
            if returned && want.returns {
                push_web_return(out.row(1), &mut rg, scaling, &o, &sale);
                out.done(1);
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    out.finish();
}

/// `rest`: web site, ship mode, warehouse, promotion.
fn push_web_sale(b: &mut impl Row, nbm: i64, o: &ChannelOrder, s: &LineSale, rest: [i64; 4]) {
    let keys = [
        o.date,
        o.time,
        s.ship_date,
        s.item,
        o.bill[0],
        o.bill[1],
        o.bill[2],
        o.bill[3],
        o.ship[0],
        o.ship[1],
        o.ship[2],
        o.ship[3],
        s.page,
        rest[0],
        rest[1],
        rest[2],
        rest[3],
    ];
    for (pos, k) in keys.into_iter().enumerate() {
        b.sk(nbm, pos as u32, k);
    }
    b.long(o.number, bit(nbm, 17));
    let p = &s.pricing;
    b.int(p.get_quantity(), bit(nbm, 18));
    b.dec(nbm, 19, p.get_wholesale_cost());
    b.dec(nbm, 20, p.get_list_price());
    b.dec(nbm, 21, p.get_sales_price());
    b.dec(nbm, 22, p.get_ext_discount_amount());
    b.dec(nbm, 23, p.get_ext_sales_price());
    b.dec(nbm, 24, p.get_ext_wholesale_cost());
    b.dec(nbm, 25, p.get_ext_list_price());
    b.dec(nbm, 26, p.get_ext_tax());
    b.dec(nbm, 27, p.get_coupon_amount());
    b.dec(nbm, 28, p.get_ext_ship_cost());
    b.dec(nbm, 29, p.get_net_paid());
    b.dec(nbm, 30, p.get_net_paid_including_tax());
    b.dec(nbm, 31, p.get_net_paid_including_shipping());
    b.dec(nbm, 32, p.get_net_paid_including_shipping_and_tax());
    b.dec(nbm, 33, p.get_net_profit());
    b.end();
}

fn push_web_return(
    b: &mut impl Row,
    rg: &mut AbstractRowGenerator<WR>,
    scaling: &dsbgen::config::Scaling,
    o: &ChannelOrder,
    s: &LineSale,
) {
    let nbm = create_null_bit_map(
        NullTable::WebReturns,
        rg.get_random_number_stream(&WR::WrNulls),
    );
    let date = generate_returns_date_join_key(
        rg.get_random_number_stream(&WR::WrReturnedDateSk),
        s.ship_date,
        WS_MIN_SHIP_DELAY,
        WS_MAX_SHIP_DELAY,
    );
    let time = generate_time_join_key(
        HoursWeights::CatalogAndWeb,
        rg.get_random_number_stream(&WR::WrReturnedTimeSk),
    );
    let uniform = |t, c: &WR, rg: &mut AbstractRowGenerator<WR>| {
        generate_uniform_join_key(t, rg.get_random_number_stream(c), scaling)
    };
    let mut refunded = [
        uniform(Ds::Customer, &WR::WrRefundedCustomerSk, rg),
        uniform(Ds::CustomerDemographics, &WR::WrRefundedCdemoSk, rg),
        uniform(Ds::HouseholdDemographics, &WR::WrRefundedHdemoSk, rg),
        uniform(Ds::CustomerAddress, &WR::WrRefundedAddrSk, rg),
    ];
    if RandomValueGenerator::generate_uniform_random_int(
        0,
        99,
        rg.get_random_number_stream(&WR::WrReturningCustomerSk),
    ) < WEB_GIFT_PCT
    {
        refunded = o.ship;
    }
    let reason = uniform(Ds::Reason, &WR::WrReasonSk, rg);
    let quantity = RandomValueGenerator::generate_uniform_random_int(
        1,
        s.pricing.get_quantity(),
        rg.get_random_number_stream(&WR::WrPricing),
    );
    let p = generate_pricing_for_returns_table(
        rg.get_random_number_stream(&WR::WrPricing),
        quantity,
        &s.pricing,
    );

    // the returning customer is the refunded one
    let keys = [
        date,
        time,
        s.item,
        refunded[0],
        refunded[1],
        refunded[2],
        refunded[3],
        refunded[0],
        refunded[1],
        refunded[2],
        refunded[3],
        s.page,
        reason,
    ];
    for (pos, k) in keys.into_iter().enumerate() {
        b.sk(nbm, pos as u32, k);
    }
    b.long(o.number, bit(nbm, 13));
    b.int(p.get_quantity(), bit(nbm, 14));
    b.dec(nbm, 15, p.get_net_paid());
    b.dec(nbm, 16, p.get_ext_tax());
    b.dec(nbm, 17, p.get_net_paid_including_tax());
    b.dec(nbm, 18, p.get_fee());
    b.dec(nbm, 19, p.get_ext_ship_cost());
    b.dec(nbm, 20, p.get_refunded_cash());
    b.dec(nbm, 21, p.get_reversed_charge());
    b.dec(nbm, 22, p.get_store_credit());
    b.dec(nbm, 23, p.get_net_loss());
    b.end();
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_select::concat::concat_batches;
    use dsbgen::config::SessionBuilder;

    fn collect(f: impl FnOnce(&mut dyn FnMut(RecordBatch))) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        f(&mut |b| v.push(b));
        v
    }

    /// `channel` gives dsbgen-arrow's sales and returns, together or
    /// alone, and the `.dat` text of them.
    fn check(
        s: &Session,
        channel: Channel,
        want_s: &[RecordBatch],
        want_r: &[RecordBatch],
        first: u64,
        last: u64,
    ) {
        let cat = |v: &[RecordBatch]| concat_batches(&v[0].schema(), v).unwrap();
        let (mut got_s, mut got_r) = (Vec::new(), Vec::new());
        let both = Want {
            sales: true,
            returns: true,
        };
        channel.run(s, first, last, 777, both, &mut |k, b| {
            if k == 0 { got_s.push(b) } else { got_r.push(b) }
        });
        assert_eq!(cat(&got_s), cat(want_s));
        assert_eq!(cat(&got_r), cat(want_r));
        let one = |want| collect(|e| channel.run(s, first, last, 777, want, &mut |_, b| e(b)));
        let (sales, returns) = (
            Want {
                sales: true,
                returns: false,
            },
            Want {
                sales: false,
                returns: true,
            },
        );
        assert_eq!(cat(&one(returns)), cat(want_r));
        assert_eq!(cat(&one(sales)), cat(want_s));

        // text straight from the channel = text of the Arrow batches
        let text = |v: &[RecordBatch]| {
            let mut out = Vec::new();
            dat::write_batch(&cat(v), dat::Encoding::Latin1, &mut out);
            String::from_utf8(out).unwrap()
        };
        let run = |want| {
            let (mut s_out, mut r_out) = (Vec::new(), Vec::new());
            channel.run_dat(s, first, last, want, &mut s_out, &mut r_out);
            (
                String::from_utf8(s_out).unwrap(),
                String::from_utf8(r_out).unwrap(),
            )
        };
        let (ts, tr) = (text(want_s), text(want_r));
        assert_eq!(run(both), (ts.clone(), tr.clone()));
        assert_eq!(run(sales).0, ts);
        assert_eq!(run(returns).1, tr);
    }

    #[test]
    fn catalog_matches_dsbgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        for (first, last) in [(1, 300), (54_321, 54_600), (159_900, 160_000)] {
            let want_s = collect(|e| {
                CatalogSalesArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let want_r = collect(|e| {
                CatalogReturnsArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            check(&s, Channel::Catalog, &want_s, &want_r, first, last);
        }
    }

    #[test]
    fn web_matches_dsbgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        for (first, last) in [(1, 200), (31_000, 31_250), (59_900, 60_000)] {
            let want_s = collect(|e| {
                WebSalesArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let want_r = collect(|e| {
                WebReturnsArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            check(&s, Channel::Web, &want_s, &want_r, first, last);
        }
    }

    #[test]
    fn matches_dsbgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        for (first, last) in [(1, 400), (12_345, 12_700)] {
            let want_s = collect(|e| {
                StoreSalesArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let want_r = collect(|e| {
                StoreReturnsArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            check(&s, Channel::Store, &want_s, &want_r, first, last);
        }
    }
}
