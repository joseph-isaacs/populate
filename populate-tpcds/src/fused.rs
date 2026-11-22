//! Each sales table and its returns from one pass, built straight into
//! Arrow columns.
//!
//! tpcdsgen's returns generators replay every sales line item to find the
//! ~10% that are returned, so generating both tables costs the sales work
//! twice. Here one loop per channel (store, catalog, web) walks the orders
//! once and emits both tables. The loops are ports of tpcdsgen's
//! `{Store,Catalog,Web}{Sales,Returns}RowGenerator` (Apache-2.0, from the
//! tpchgen-rs project, itself a port of dsdgen). They draw from the same
//! random streams in the same order, using tpcdsgen's public helpers, so
//! the output is identical; the tests compare every channel with
//! tpcdsgen-arrow, both tables together and each alone.
//!
//! Columns are pushed into per-column buffers as each line item is made,
//! instead of collecting row structs and converting them column by column.

use crate::TableName;
use crate::seeds;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::distribution::hours_distribution::HoursWeights;
use tpcdsgen::generator::CatalogReturnsGeneratorColumn as CR;
use tpcdsgen::generator::CatalogSalesGeneratorColumn as CS;
use tpcdsgen::generator::StoreReturnsGeneratorColumn as R;
use tpcdsgen::generator::StoreSalesGeneratorColumn as S;
use tpcdsgen::generator::WebReturnsGeneratorColumn as WR;
use tpcdsgen::generator::WebSalesGeneratorColumn as WS;
use tpcdsgen::join_key_utils::{
    CS_MAX_SHIP_DELAY, CS_MIN_SHIP_DELAY, WS_MAX_SHIP_DELAY, WS_MIN_SHIP_DELAY,
    generate_catalog_page_join_key, generate_returns_date_join_key, generate_sales_date_join_key,
    generate_scd_join_key, generate_time_join_key, generate_uniform_join_key,
};
use tpcdsgen::nulls::create_null_bit_map;
use tpcdsgen::permutations::{get_permutation_entry, make_permutation};
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::AbstractRowGenerator;
use tpcdsgen::slowly_changing_dimension_utils::match_surrogate_key;
use tpcdsgen::table::Table as NullTable;
use tpcdsgen::types::{
    Date, Decimal, Pricing, generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_catalog_sales_pricing_limits, get_store_sales_pricing_limits, get_web_sales_pricing_limits,
};
use tpcdsgen_arrow::{
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

    #[inline]
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

/// Keys, nullable by bit `pos` or by being negative.
#[inline]
fn sk(c: &mut Col<i32>, nbm: i64, pos: u32, v: i64) {
    c.push(v as i32, bit(nbm, pos) && v >= 0);
}

#[inline]
fn dec(c: &mut Col<i128>, nbm: i64, pos: u32, v: Decimal) {
    c.push(v.get_number() as i128, bit(nbm, pos));
}

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
        let f = match self {
            Channel::Store => store_channel,
            Channel::Catalog => catalog_channel,
            Channel::Web => web_channel,
        };
        f(session, first, last, batch, want, emit)
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
pub(crate) fn store_channel(
    session: &Session,
    first: u64,
    last: u64,
    batch: usize,
    want: Want,
    emit: &mut dyn FnMut(usize, RecordBatch),
) {
    let scaling = session.get_scaling();
    let tickets = scaling.get_row_count(Ds::StoreSales);
    let mut sg = AbstractRowGenerator::<S>::new(tickets);
    sg.set_source_row_range(first, last);
    let mut rg = AbstractRowGenerator::<R>::new(tickets);
    rg.set_source_row_range(first, last);

    let item_count = scaling.get_id_count(Ds::Item) as usize;
    let limits = get_store_sales_pricing_limits();
    let permutation = make_permutation(item_count, sg.get_random_number_stream(&S::SsPermutation));

    let mut sb = Builder::new(StoreSalesArrow::schema_ref(), SALES, batch);
    let mut rb = Builder::new(StoreReturnsArrow::schema_ref(), RETURNS, batch);

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
                push_sale(&mut sb, nbm, &o, item, promo, &pricing);
                if sb.rows == batch {
                    emit(0, sb.finish());
                }
            }
            if returned && want.returns {
                push_return(&mut rb, &mut rg, scaling, &o, item, &pricing);
                if rb.rows == batch {
                    emit(1, rb.finish());
                }
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    if sb.rows > 0 {
        emit(0, sb.finish());
    }
    if rb.rows > 0 {
        emit(1, rb.finish());
    }
}

fn push_sale(b: &mut Builder, nbm: i64, o: &Order, item: i64, promo: i64, p: &Pricing) {
    let i = &mut b.i32s;
    sk(&mut i[0], nbm, 0, o.date);
    sk(&mut i[1], nbm, 1, o.time);
    sk(&mut i[2], nbm, 2, item);
    sk(&mut i[3], nbm, 3, o.customer);
    sk(&mut i[4], nbm, 4, o.cdemo);
    sk(&mut i[5], nbm, 5, o.hdemo);
    sk(&mut i[6], nbm, 6, o.addr);
    sk(&mut i[7], nbm, 7, o.store);
    sk(&mut i[8], nbm, 8, promo);
    b.i64s[0].push(o.ticket, bit(nbm, 9) && o.ticket >= 0);
    i[9].push(p.get_quantity(), bit(nbm, 10));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 11, p.get_wholesale_cost());
    dec(&mut d[1], nbm, 12, p.get_list_price());
    dec(&mut d[2], nbm, 13, p.get_sales_price());
    // dsdgen prints the coupon amount as ss_ext_discount_amt (a bug kept for
    // compatibility), and again as ss_coupon_amt
    dec(&mut d[3], nbm, 14, p.get_coupon_amount());
    dec(&mut d[4], nbm, 15, p.get_ext_sales_price());
    dec(&mut d[5], nbm, 16, p.get_ext_wholesale_cost());
    dec(&mut d[6], nbm, 17, p.get_ext_list_price());
    dec(&mut d[7], nbm, 18, p.get_ext_tax());
    dec(&mut d[8], nbm, 14, p.get_coupon_amount());
    dec(&mut d[9], nbm, 19, p.get_net_paid());
    dec(&mut d[10], nbm, 20, p.get_net_paid_including_tax());
    dec(&mut d[11], nbm, 21, p.get_net_profit());
    b.rows += 1;
}

fn push_return(
    b: &mut Builder,
    rg: &mut AbstractRowGenerator<R>,
    scaling: &tpcdsgen::config::Scaling,
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

    let i = &mut b.i32s;
    sk(&mut i[0], nbm, 0, date);
    sk(&mut i[1], nbm, 1, time);
    sk(&mut i[2], nbm, 2, item);
    sk(&mut i[3], nbm, 3, customer);
    sk(&mut i[4], nbm, 4, cdemo);
    sk(&mut i[5], nbm, 5, hdemo);
    sk(&mut i[6], nbm, 6, addr);
    sk(&mut i[7], nbm, 7, store);
    sk(&mut i[8], nbm, 8, reason);
    b.i64s[0].push(o.ticket, bit(nbm, 9) && o.ticket >= 0);
    i[9].push(p.get_quantity(), bit(nbm, 10));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 11, p.get_net_paid());
    dec(&mut d[1], nbm, 12, p.get_ext_tax());
    dec(&mut d[2], nbm, 13, p.get_net_paid_including_tax());
    dec(&mut d[3], nbm, 14, p.get_fee());
    dec(&mut d[4], nbm, 15, p.get_ext_ship_cost());
    dec(&mut d[5], nbm, 16, p.get_refunded_cash());
    dec(&mut d[6], nbm, 17, p.get_reversed_charge());
    dec(&mut d[7], nbm, 18, p.get_store_credit());
    dec(&mut d[8], nbm, 19, p.get_net_loss());
    b.rows += 1;
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
/// for orders `first..=last` (1-based): a port of tpcdsgen's
/// `CatalogSalesRowGenerator` and `CatalogReturnsRowGenerator`, which
/// replays every sales line item to find the returned ones.
pub(crate) fn catalog_channel(
    session: &Session,
    first: u64,
    last: u64,
    batch: usize,
    want: Want,
    emit: &mut dyn FnMut(usize, RecordBatch),
) {
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

    let mut sb = Builder::new(CatalogSalesArrow::schema_ref(), CATALOG_SALES, batch);
    let mut rb = Builder::new(CatalogReturnsArrow::schema_ref(), CATALOG_RETURNS, batch);

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
                push_catalog_sale(&mut sb, nbm, &o, &sale, ship_mode, warehouse, promo);
                if sb.rows == batch {
                    emit(0, sb.finish());
                }
            }
            if returned && want.returns {
                push_catalog_return(&mut rb, &mut rg, scaling, &o, &sale);
                if rb.rows == batch {
                    emit(1, rb.finish());
                }
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    if sb.rows > 0 {
        emit(0, sb.finish());
    }
    if rb.rows > 0 {
        emit(1, rb.finish());
    }
}

fn push_catalog_sale(
    b: &mut Builder,
    nbm: i64,
    o: &ChannelOrder,
    s: &LineSale,
    ship_mode: i64,
    warehouse: i64,
    promo: i64,
) {
    let i = &mut b.i32s;
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
        sk(&mut i[pos], nbm, pos as u32, k);
    }
    b.i64s[0].push(o.number, bit(nbm, 17));
    let p = &s.pricing;
    i[17].push(p.get_quantity(), bit(nbm, 18));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 19, p.get_wholesale_cost());
    dec(&mut d[1], nbm, 20, p.get_list_price());
    dec(&mut d[2], nbm, 21, p.get_sales_price());
    dec(&mut d[3], nbm, 24, p.get_ext_discount_amount());
    dec(&mut d[4], nbm, 23, p.get_ext_sales_price());
    dec(&mut d[5], nbm, 25, p.get_ext_wholesale_cost());
    dec(&mut d[6], nbm, 26, p.get_ext_list_price());
    dec(&mut d[7], nbm, 27, p.get_ext_tax());
    dec(&mut d[8], nbm, 22, p.get_coupon_amount());
    dec(&mut d[9], nbm, 28, p.get_ext_ship_cost());
    dec(&mut d[10], nbm, 29, p.get_net_paid());
    dec(&mut d[11], nbm, 30, p.get_net_paid_including_tax());
    dec(&mut d[12], nbm, 31, p.get_net_paid_including_shipping());
    dec(
        &mut d[13],
        nbm,
        32,
        p.get_net_paid_including_shipping_and_tax(),
    );
    dec(&mut d[14], nbm, 33, p.get_net_profit());
    b.rows += 1;
}

fn push_catalog_return(
    b: &mut Builder,
    rg: &mut AbstractRowGenerator<CR>,
    scaling: &tpcdsgen::config::Scaling,
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

    let i = &mut b.i32s;
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
        sk(&mut i[pos], nbm, pos as u32, k);
    }
    b.i64s[0].push(o.number, bit(nbm, 16));
    i[16].push(p.get_quantity(), bit(nbm, 17));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 18, p.get_net_paid());
    dec(&mut d[1], nbm, 19, p.get_ext_tax());
    dec(&mut d[2], nbm, 20, p.get_net_paid_including_tax());
    dec(&mut d[3], nbm, 21, p.get_fee());
    dec(&mut d[4], nbm, 22, p.get_ext_ship_cost());
    dec(&mut d[5], nbm, 23, p.get_refunded_cash());
    dec(&mut d[6], nbm, 24, p.get_reversed_charge());
    dec(&mut d[7], nbm, 25, p.get_store_credit());
    dec(&mut d[8], nbm, 26, p.get_net_loss());
    b.rows += 1;
}

/// Web orders shipped to the billed customer: those drawing at most this
/// (of 0..99) in tpcdsgen; returns by the ship customer: below it.
const WEB_GIFT_PCT: i32 = 7;

/// Generate `web_sales` (output 0) and/or `web_returns` (output 1) for
/// orders `first..=last` (1-based): a port of tpcdsgen's
/// `WebSalesRowGenerator` and `WebReturnsRowGenerator`.
pub(crate) fn web_channel(
    session: &Session,
    first: u64,
    last: u64,
    batch: usize,
    want: Want,
    emit: &mut dyn FnMut(usize, RecordBatch),
) {
    let scaling = session.get_scaling();
    let orders = scaling.get_row_count(Ds::WebSales);
    let mut sg = AbstractRowGenerator::<WS>::new(orders);
    sg.set_source_row_range(first, last);
    let mut rg = AbstractRowGenerator::<WR>::new(orders);
    rg.set_source_row_range(first, last);

    let item_count = scaling.get_id_count(Ds::Item) as usize;
    let limits = get_web_sales_pricing_limits();
    let permutation = make_permutation(item_count, sg.get_random_number_stream(&WS::WsPermutation));

    let mut sb = Builder::new(WebSalesArrow::schema_ref(), WEB_SALES, batch);
    let mut rb = Builder::new(WebReturnsArrow::schema_ref(), WEB_RETURNS, batch);
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
                    &mut sb,
                    nbm,
                    &o,
                    &sale,
                    [web_site, ship_mode, warehouse, promo],
                );
                if sb.rows == batch {
                    emit(0, sb.finish());
                }
            }
            if returned && want.returns {
                push_web_return(&mut rb, &mut rg, scaling, &o, &sale);
                if rb.rows == batch {
                    emit(1, rb.finish());
                }
            }
        }
        seeds::finish_row(&mut sg);
        if want.returns {
            seeds::finish_row(&mut rg);
        }
    }
    if sb.rows > 0 {
        emit(0, sb.finish());
    }
    if rb.rows > 0 {
        emit(1, rb.finish());
    }
}

/// `rest`: web site, ship mode, warehouse, promotion.
fn push_web_sale(b: &mut Builder, nbm: i64, o: &ChannelOrder, s: &LineSale, rest: [i64; 4]) {
    let i = &mut b.i32s;
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
        sk(&mut i[pos], nbm, pos as u32, k);
    }
    b.i64s[0].push(o.number, bit(nbm, 17));
    let p = &s.pricing;
    i[17].push(p.get_quantity(), bit(nbm, 18));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 19, p.get_wholesale_cost());
    dec(&mut d[1], nbm, 20, p.get_list_price());
    dec(&mut d[2], nbm, 21, p.get_sales_price());
    dec(&mut d[3], nbm, 22, p.get_ext_discount_amount());
    dec(&mut d[4], nbm, 23, p.get_ext_sales_price());
    dec(&mut d[5], nbm, 24, p.get_ext_wholesale_cost());
    dec(&mut d[6], nbm, 25, p.get_ext_list_price());
    dec(&mut d[7], nbm, 26, p.get_ext_tax());
    dec(&mut d[8], nbm, 27, p.get_coupon_amount());
    dec(&mut d[9], nbm, 28, p.get_ext_ship_cost());
    dec(&mut d[10], nbm, 29, p.get_net_paid());
    dec(&mut d[11], nbm, 30, p.get_net_paid_including_tax());
    dec(&mut d[12], nbm, 31, p.get_net_paid_including_shipping());
    dec(
        &mut d[13],
        nbm,
        32,
        p.get_net_paid_including_shipping_and_tax(),
    );
    dec(&mut d[14], nbm, 33, p.get_net_profit());
    b.rows += 1;
}

fn push_web_return(
    b: &mut Builder,
    rg: &mut AbstractRowGenerator<WR>,
    scaling: &tpcdsgen::config::Scaling,
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

    let i = &mut b.i32s;
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
        sk(&mut i[pos], nbm, pos as u32, k);
    }
    b.i64s[0].push(o.number, bit(nbm, 13));
    i[13].push(p.get_quantity(), bit(nbm, 14));
    let d = &mut b.decs;
    dec(&mut d[0], nbm, 15, p.get_net_paid());
    dec(&mut d[1], nbm, 16, p.get_ext_tax());
    dec(&mut d[2], nbm, 17, p.get_net_paid_including_tax());
    dec(&mut d[3], nbm, 18, p.get_fee());
    dec(&mut d[4], nbm, 19, p.get_ext_ship_cost());
    dec(&mut d[5], nbm, 20, p.get_refunded_cash());
    dec(&mut d[6], nbm, 21, p.get_reversed_charge());
    dec(&mut d[7], nbm, 22, p.get_store_credit());
    dec(&mut d[8], nbm, 23, p.get_net_loss());
    b.rows += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_select::concat::concat_batches;
    use tpcdsgen::config::SessionBuilder;

    fn collect(f: impl FnOnce(&mut dyn FnMut(RecordBatch))) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        f(&mut |b| v.push(b));
        v
    }

    type Channel = fn(&Session, u64, u64, usize, Want, &mut dyn FnMut(usize, RecordBatch));

    /// `channel` gives tpcdsgen-arrow's sales and returns, together or alone.
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
        channel(s, first, last, 777, both, &mut |k, b| {
            if k == 0 { got_s.push(b) } else { got_r.push(b) }
        });
        assert_eq!(cat(&got_s), cat(want_s));
        assert_eq!(cat(&got_r), cat(want_r));
        let one = |want| collect(|e| channel(s, first, last, 777, want, &mut |_, b| e(b)));
        assert_eq!(
            cat(&one(Want {
                sales: false,
                returns: true
            })),
            cat(want_r)
        );
        assert_eq!(
            cat(&one(Want {
                sales: true,
                returns: false
            })),
            cat(want_s)
        );
    }

    #[test]
    fn catalog_matches_tpcdsgen_arrow() {
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
            check(&s, catalog_channel, &want_s, &want_r, first, last);
        }
    }

    #[test]
    fn web_matches_tpcdsgen_arrow() {
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
            check(&s, web_channel, &want_s, &want_r, first, last);
        }
    }

    #[test]
    fn matches_tpcdsgen_arrow() {
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
            let (mut got_s, mut got_r) = (Vec::new(), Vec::new());
            let both = Want {
                sales: true,
                returns: true,
            };
            store_channel(&s, first, last, 777, both, &mut |k, b| {
                if k == 0 { got_s.push(b) } else { got_r.push(b) }
            });
            let cat = |v: &[RecordBatch]| concat_batches(&v[0].schema(), v).unwrap();
            assert_eq!(cat(&got_s), cat(&want_s));
            assert_eq!(cat(&got_r), cat(&want_r));
            // one table alone gives the same rows
            let one =
                |want| collect(|e| store_channel(&s, first, last, 777, want, &mut |_, b| e(b)));
            assert_eq!(
                cat(&one(Want {
                    sales: false,
                    returns: true
                })),
                cat(&want_r)
            );
            assert_eq!(
                cat(&one(Want {
                    sales: true,
                    returns: false
                })),
                cat(&want_s)
            );
        }
    }
}
