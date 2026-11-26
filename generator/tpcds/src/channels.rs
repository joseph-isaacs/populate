// Modified from tpcdsgen 4.0.0: sales and returns write paired column buffers.
// See ../NOTICE.md and ../LICENSE-Apache-2.0.txt.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The sales channels (store, catalog, web): each sales table and its
//! returns as one distribution, [`fan_out`] over the tickets or orders.
//!
//! A source row is a ticket (an order): its number and where every random
//! stream of the sales and the returns generator stands at its start
//! ([`RowSeeds`]). For each one, the line loop makes 8 to 16 sales lines
//! (4 to 14 for catalog), and the returns of the ~10% that are returned,
//! in one pass. tpcdsgen's returns generators replay every sales line to
//! find the returned ones; here neither table costs the other's work.
//!
//! The loops are ports of tpcdsgen's `{Store,Catalog,Web}{Sales,Returns}
//! RowGenerator` (Apache-2.0, from the tpcgen-rs project, itself a port of
//! dsdgen). They draw from the same streams in the same order, through
//! tpcdsgen's public helpers, so the values are dsdgen's; the tests compare
//! every channel with tpcdsgen-arrow.

use crate::masked::{Masked, NOT_NULL as N, NullBits};
use crate::seeds::{RowSeeds, Streams};
use populate_builtins::{fan_out, named_all, range};
use populate_core::{Children, Columns, Decimal as Fixed, Distribution, Sink, Spread, SpreadCols};
use std::sync::Arc;
use tpcdsgen::config::{Scaling, Session, Table as Ds};
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
use tpcdsgen::random::{RandomNumberStream, RandomValueGenerator};
use tpcdsgen::row::AbstractRowGenerator;
use tpcdsgen::slowly_changing_dimension_utils::match_surrogate_key;
use tpcdsgen::table::Table as NullTable;
use tpcdsgen::types::{
    Date, Decimal, Pricing, PricingLimits, generate_pricing_for_returns_table,
    get_catalog_sales_pricing_limits, get_store_sales_pricing_limits, get_web_sales_pricing_limits,
};

/// A `Decimal(7, 2)`.
type Dec = Fixed<7, 2>;

/// The [`NullBits`] of a table.
macro_rules! null_bits {
    ($name:ident = $bits:expr) => {
        #[derive(Debug, Clone, PartialEq)]
        pub(crate) struct $name;

        impl NullBits for $name {
            const BITS: &'static [i8] = &$bits;
        }
    };
}

/// Percentage of line items that are returned.
const SR_RETURN_PCT: i32 = 10;
/// Percentage of returns made by the customer who bought the item.
const SR_SAME_CUSTOMER: i32 = 80;
/// Percentage of catalog orders shipped to another customer.
const CS_GIFT_PCT: i32 = 10;
/// Percentage of catalog returns made by the customer it was shipped to.
const CR_GIFT_PCT: i32 = 10;
/// Web orders shipped to the billed customer: those drawing at most this
/// (of 0..99) in tpcdsgen; returns by the ship customer: below it.
const WEB_GIFT_PCT: i32 = 7;

/// A key, NULL (bit `pos` of `nbm` set) also where it is negative
/// (tpcdsgen-arrow's `int32_sk`).
#[inline(always)]
fn sk(nbm: &mut i64, pos: u32, v: i64) -> i32 {
    *nbm |= ((v < 0) as i64) << pos;
    v as i32
}

/// The columns of `N` keys.
type Sks<const N: usize> = SpreadCols<Vec<i32>, N>;

/// Push key `keys[i]` to column `i`, at bit `first + i` of `nbm`.
#[inline(always)]
fn push_sks<const N: usize>(cols: &mut Sks<N>, nbm: &mut i64, first: u32, keys: [i64; N]) {
    for (i, (c, k)) in cols.0.iter_mut().zip(keys).enumerate() {
        c.push(sk(nbm, first + i as u32, k));
    }
}

/// The columns of `N` decimals.
type Decs<const N: usize> = SpreadCols<Vec<Dec>, N>;

/// Push decimal `v[i]` to column `i`. Written in place: a row of them
/// built first and pushed would be moved through `memcpy`.
#[inline(always)]
fn push_decs<const N: usize>(cols: &mut Decs<N>, v: [Decimal; N]) {
    for (c, x) in cols.0.iter_mut().zip(v) {
        c.push(Fixed(x.get_number() as i128));
    }
}

/// tpcdsgen's `generate_pricing_for_sales_table`, with the decimal
/// arithmetic done on plain integers: every operand has 2 decimals (the
/// quantity 0), so each product's rescaling is a constant division and
/// nothing tracks precisions. The same 8 draws in the same order, so the
/// same values (tested against tpcdsgen for every channel's limits).
/// tpcdsgen's version is a fifth of a sales table's generation.
pub(crate) fn sales_pricing(limits: &PricingLimits, s: &mut RandomNumberStream) -> Pricing {
    let two = |d: Decimal| {
        debug_assert_eq!(d.get_precision(), 2);
        d.get_number()
    };
    let (max_markup, max_discount, max_wholesale) = (
        two(limits.get_max_markup()),
        two(limits.get_max_discount()),
        two(limits.get_max_wholesale_cost()),
    );
    // RandomValueGenerator's uniform int and decimal draws
    let int = |s: &mut RandomNumberStream, lo: i32, hi: i32| {
        (s.next_random() as i32) % (hi - lo + 1) + lo
    };
    let dec = |s: &mut RandomNumberStream, lo: i64, hi: i64| s.next_random() % (hi - lo + 1) + lo;
    let q = int(s, Pricing::QUANTITY_MIN, limits.get_max_quantity_sold()) as i64;
    let wholesale = dec(s, 100, max_wholesale);
    let ext_wholesale = q * wholesale;
    let markup = dec(s, 0, max_markup) + 100;
    let list = wholesale * markup / 100;
    let discount = 100 - dec(s, 0, max_discount);
    let sales = list * discount / 100;
    let ext_list = list * q;
    let ext_sales = sales * q;
    let ext_discount = ext_list - ext_sales;
    let coupon = dec(s, 0, 100);
    let coupon_usage = int(s, 1, 100);
    // 20% of sales use a coupon
    let coupon_amount = if coupon_usage <= 20 {
        ext_sales * coupon / 100
    } else {
        0
    };
    let net_paid = ext_sales - coupon_amount;
    let shipping = dec(s, 0, 50);
    let ship = list * shipping / 100;
    let ext_ship = ship * q;
    let net_paid_ship = net_paid + ext_ship;
    let tax = dec(s, 0, 9);
    let ext_tax = net_paid * tax / 100;
    let d = |n: i64| Decimal::new(n, 2);
    Pricing::new(
        d(wholesale),
        d(list),
        d(sales),
        q as i32,
        d(ext_discount),
        d(ext_sales),
        d(ext_wholesale),
        d(ext_list),
        d(tax),
        d(ext_tax),
        d(coupon_amount),
        d(ship),
        d(ext_ship),
        d(net_paid),
        d(net_paid + ext_tax),
        d(net_paid_ship),
        d(net_paid_ship + ext_tax),
        d(net_paid - ext_wholesale),
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
    )
}

/// What every line loop needs: the scale, and the item permutation (a
/// stream that reserves no draws per row, so the same for every row).
struct Ctx {
    scaling: Scaling,
    item_count: i32,
    limits: PricingLimits,
    permutation: Vec<i32>,
}

impl Ctx {
    fn new<C>(session: &Session, rows: u64, limits: PricingLimits, permute: &C) -> Arc<Self>
    where
        C: tpcdsgen::generator::GeneratorColumn,
    {
        let scaling = session.get_scaling().clone();
        let item_count = scaling.get_id_count(Ds::Item) as usize;
        let mut g = AbstractRowGenerator::<C>::new(rows);
        let permutation = make_permutation(item_count, g.get_random_number_stream(permute));
        Arc::new(Ctx {
            scaling,
            item_count: item_count as i32,
            limits,
            permutation,
        })
    }

    /// The item of the next line: items are distinct within a ticket,
    /// consecutive permutation entries from a random start.
    #[inline]
    fn next_item(&self, index: &mut i32, date: i64) -> i64 {
        *index += 1;
        if *index > self.item_count {
            *index = 1;
        }
        let key = get_permutation_entry(&self.permutation, *index);
        match_surrogate_key(key as i64, date, Ds::Item, &self.scaling)
    }

    fn uniform(&self, t: Ds, s: &mut RandomNumberStream) -> i64 {
        generate_uniform_join_key(t, s, &self.scaling)
    }
}

/// A uniform int in `lo..=hi`.
#[inline]
fn uniform_int(lo: i32, hi: i32, s: &mut RandomNumberStream) -> i32 {
    RandomValueGenerator::generate_uniform_random_int(lo, hi, s)
}

// ============================================================================
// Store
// ============================================================================

/// date, time, item, customer, cdemo, hdemo, addr, store, promo (or
/// reason), ticket, quantity.
type StoreKeys = (i32, i32, i32, i32, i32, i32, i32, i32, i32, i64, i32);
type StoreSale = Masked<(StoreKeys, Spread<Dec, 12>), StoreSaleBits>;
type StoreReturn = Masked<(StoreKeys, Spread<Dec, 9>), StoreReturnBits>;

// dsdgen prints the coupon amount as ss_ext_discount_amt (a bug kept for
// compatibility), and again as ss_coupon_amt: both are bit 14
null_bits!(
    StoreSaleBits = [
        0, 1, N, 3, 4, 5, 6, 7, 8, N, 10, 11, 12, 13, 14, 15, 16, 17, 18, 14, 19, 20, 21
    ]
);
null_bits!(
    StoreReturnBits = [
        0, 1, N, 3, 4, 5, 6, 7, 8, N, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    ]
);
pub(crate) type StoreTables = (Children<StoreSale>, Children<StoreReturn>);

const STORE_NAMES: &[&str] = &[
    "ss_sold_date_sk",
    "ss_sold_time_sk",
    "ss_item_sk",
    "ss_customer_sk",
    "ss_cdemo_sk",
    "ss_hdemo_sk",
    "ss_addr_sk",
    "ss_store_sk",
    "ss_promo_sk",
    "ss_ticket_number",
    "ss_quantity",
    "ss_wholesale_cost",
    "ss_list_price",
    "ss_sales_price",
    "ss_ext_discount_amt",
    "ss_ext_sales_price",
    "ss_ext_wholesale_cost",
    "ss_ext_list_price",
    "ss_ext_tax",
    "ss_coupon_amt",
    "ss_net_paid",
    "ss_net_paid_inc_tax",
    "ss_net_profit",
    "sr_returned_date_sk",
    "sr_return_time_sk",
    "sr_item_sk",
    "sr_customer_sk",
    "sr_cdemo_sk",
    "sr_hdemo_sk",
    "sr_addr_sk",
    "sr_store_sk",
    "sr_reason_sk",
    "sr_ticket_number",
    "sr_return_quantity",
    "sr_return_amt",
    "sr_return_tax",
    "sr_return_amt_inc_tax",
    "sr_fee",
    "sr_return_ship_cost",
    "sr_refunded_cash",
    "sr_reversed_charge",
    "sr_store_credit",
    "sr_net_loss",
];

/// The ticket-level values shared by its line items.
struct Ticket {
    store: i64,
    time: i64,
    date: i64,
    customer: i64,
    cdemo: i64,
    hdemo: i64,
    addr: i64,
    ticket: i64,
}

/// `store_sales` and `store_returns`, one ticket per source row.
pub(crate) fn store(
    session: &Session,
) -> impl Distribution<Item = StoreTables> + Clone + Sync + use<> {
    let tickets = session.get_scaling().get_row_count(Ds::StoreSales);
    let cx = Ctx::new(
        session,
        tickets,
        get_store_sales_pricing_limits(),
        &S::SsPermutation,
    );
    let source = (
        range(1i64, 1),
        RowSeeds::<S, 26>::new(),
        RowSeeds::<R, 22>::new(),
    );
    named_all(
        STORE_NAMES,
        fan_out(
            source,
            move |(&ticket, s, r): (&i64, &[u32; 26], &[u32; 22]),
                  (sales, returns): &mut Columns<StoreTables>| {
                store_ticket(&cx, ticket, s, r, &mut sales.values, &mut returns.values)
            },
        )
        .reserving(&[12.5, 1.3]),
    )
}

fn store_ticket(
    cx: &Ctx,
    ticket: i64,
    s: &[u32; 26],
    r: &[u32; 22],
    sales: &mut Columns<StoreSale>,
    returns: &mut Columns<StoreReturn>,
) {
    let scaling = &cx.scaling;
    let mut sg = Streams::<S, 26>::at(s);
    let mut rg = None;
    let t = Ticket {
        store: generate_scd_join_key(Ds::Store, sg.get(&S::SsSoldStoreSk), 1, scaling),
        time: generate_time_join_key(HoursWeights::Store, sg.get(&S::SsSoldTimeSk)),
        date: generate_sales_date_join_key(sg.get(&S::SsSoldDateSk)),
        customer: cx.uniform(Ds::Customer, sg.get(&S::SsSoldCustomerSk)),
        cdemo: cx.uniform(Ds::CustomerDemographics, sg.get(&S::SsSoldCdemoSk)),
        hdemo: cx.uniform(Ds::HouseholdDemographics, sg.get(&S::SsSoldHdemoSk)),
        addr: cx.uniform(Ds::CustomerAddress, sg.get(&S::SsSoldAddrSk)),
        ticket,
    };
    let lines = uniform_int(8, 16, sg.get(&S::SsTicketNumber));
    let mut item_index = uniform_int(1, cx.item_count, sg.get(&S::SsSoldItemSk));
    for _ in 0..lines {
        let item = cx.next_item(&mut item_index, t.date);
        let returned = uniform_int(0, 99, sg.get(&S::SrIsReturned)) < SR_RETURN_PCT;
        let mut nbm = create_null_bit_map(NullTable::StoreSales, sg.get(&S::SsNulls));
        let promo = cx.uniform(Ds::Promotion, sg.get(&S::SsSoldPromoSk));
        let p = sales_pricing(&cx.limits, sg.get(&S::SsPricing));
        let n = &mut nbm;
        sales.values.0.push((
            sk(n, 0, t.date),
            sk(n, 1, t.time),
            item as i32,
            sk(n, 3, t.customer),
            sk(n, 4, t.cdemo),
            sk(n, 5, t.hdemo),
            sk(n, 6, t.addr),
            sk(n, 7, t.store),
            sk(n, 8, promo),
            t.ticket,
            p.get_quantity(),
        ));
        push_decs(
            &mut sales.values.1,
            [
                p.get_wholesale_cost(),
                p.get_list_price(),
                p.get_sales_price(),
                p.get_coupon_amount(),
                p.get_ext_sales_price(),
                p.get_ext_wholesale_cost(),
                p.get_ext_list_price(),
                p.get_ext_tax(),
                p.get_coupon_amount(),
                p.get_net_paid(),
                p.get_net_paid_including_tax(),
                p.get_net_profit(),
            ],
        );
        sales.nbm.push(nbm);
        if returned {
            let rg = rg.get_or_insert_with(|| Streams::<R, 22>::at(r));
            store_return(cx, rg, &t, item, &p, returns);
        }
    }
}

fn store_return(
    cx: &Ctx,
    rg: &mut Streams<R, 22>,
    t: &Ticket,
    item: i64,
    sale: &Pricing,
    out: &mut Columns<StoreReturn>,
) {
    let scaling = &cx.scaling;
    let mut nbm = create_null_bit_map(NullTable::StoreReturns, rg.get(&R::SrNulls));
    let mut customer = cx.uniform(Ds::Customer, rg.get(&R::SrCustomerSk));
    if uniform_int(1, 100, rg.get(&R::SrTicketNumber)) < SR_SAME_CUSTOMER {
        customer = t.customer;
    }
    let date = generate_returns_date_join_key(
        rg.get(&R::SrReturnedDateSk),
        t.date,
        CS_MIN_SHIP_DELAY,
        CS_MAX_SHIP_DELAY,
    );
    let time = uniform_int(8 * 3600 - 1, 17 * 3600 - 1, rg.get(&R::SrReturnedTimeSk)) as i64;
    let cdemo = cx.uniform(Ds::CustomerDemographics, rg.get(&R::SrCdemoSk));
    let hdemo = cx.uniform(Ds::HouseholdDemographics, rg.get(&R::SrHdemoSk));
    let addr = cx.uniform(Ds::CustomerAddress, rg.get(&R::SrAddrSk));
    let store = generate_scd_join_key(Ds::Store, rg.get(&R::SrStoreSk), 1, scaling);
    let reason = cx.uniform(Ds::Reason, rg.get(&R::SrReasonSk));
    let quantity = uniform_int(1, sale.get_quantity(), rg.get(&R::SrPricing));
    let p = generate_pricing_for_returns_table(rg.get(&R::SrPricing), quantity, sale);
    let n = &mut nbm;
    out.values.0.push((
        sk(n, 0, date),
        sk(n, 1, time),
        item as i32,
        sk(n, 3, customer),
        sk(n, 4, cdemo),
        sk(n, 5, hdemo),
        sk(n, 6, addr),
        sk(n, 7, store),
        sk(n, 8, reason),
        t.ticket,
        p.get_quantity(),
    ));
    push_return_decimals(&mut out.values.1, &p);
    out.nbm.push(nbm);
}

// ============================================================================
// Catalog and web
// ============================================================================

/// An order's values shared by its line items.
struct Order {
    date: i64,
    time: i64,
    call_center: i64,
    bill: [i64; 4],
    ship: [i64; 4],
    number: i64,
}

/// A line item's sales values a return needs.
struct LineSale {
    ship_date: i64,
    /// The catalog page, or the web page.
    page: i64,
    item: i64,
    pricing: Pricing,
}

/// The 15 decimals of a catalog or web sale.
#[inline(always)]
fn push_sale_decimals(cols: &mut Decs<15>, p: &Pricing) {
    let v = [
        p.get_wholesale_cost(),
        p.get_list_price(),
        p.get_sales_price(),
        p.get_ext_discount_amount(),
        p.get_ext_sales_price(),
        p.get_ext_wholesale_cost(),
        p.get_ext_list_price(),
        p.get_ext_tax(),
        p.get_coupon_amount(),
        p.get_ext_ship_cost(),
        p.get_net_paid(),
        p.get_net_paid_including_tax(),
        p.get_net_paid_including_shipping(),
        p.get_net_paid_including_shipping_and_tax(),
        p.get_net_profit(),
    ];
    push_decs(cols, v);
}

/// The 9 decimals of a return.
#[inline(always)]
fn push_return_decimals(cols: &mut Decs<9>, p: &Pricing) {
    let v = [
        p.get_net_paid(),
        p.get_ext_tax(),
        p.get_net_paid_including_tax(),
        p.get_fee(),
        p.get_ext_ship_cost(),
        p.get_refunded_cash(),
        p.get_reversed_charge(),
        p.get_store_credit(),
        p.get_net_loss(),
    ];
    push_decs(cols, v);
}

/// date, time, ship date, bill (4), ship (4), call center, catalog page,
/// ship mode, warehouse; item; promo; order number; quantity; decimals.
type CatalogSale = Masked<(Spread<i32, 15>, i32, i32, i64, i32, Spread<Dec, 15>), CatalogSaleBits>;
/// date, time; item; refunded (4), returning (4), call center, page, ship
/// mode, warehouse, reason; order number; quantity; decimals.
type CatalogReturn =
    Masked<(i32, i32, i32, Spread<i32, 13>, i64, i32, Spread<Dec, 9>), CatalogReturnBits>;
pub(crate) type CatalogTables = (Children<CatalogSale>, Children<CatalogReturn>);

null_bits!(
    CatalogSaleBits = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, N, 16, N, 18, 19, 20, 21, 24, 23, 25, 26,
        27, 22, 28, 29, 30, 31, 32, 33
    ]
);
null_bits!(
    CatalogReturnBits = [
        0, 1, N, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, N, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26
    ]
);

const CATALOG_NAMES: &[&str] = &[
    "cs_sold_date_sk",
    "cs_sold_time_sk",
    "cs_ship_date_sk",
    "cs_bill_customer_sk",
    "cs_bill_cdemo_sk",
    "cs_bill_hdemo_sk",
    "cs_bill_addr_sk",
    "cs_ship_customer_sk",
    "cs_ship_cdemo_sk",
    "cs_ship_hdemo_sk",
    "cs_ship_addr_sk",
    "cs_call_center_sk",
    "cs_catalog_page_sk",
    "cs_ship_mode_sk",
    "cs_warehouse_sk",
    "cs_item_sk",
    "cs_promo_sk",
    "cs_order_number",
    "cs_quantity",
    "cs_wholesale_cost",
    "cs_list_price",
    "cs_sales_price",
    "cs_ext_discount_amt",
    "cs_ext_sales_price",
    "cs_ext_wholesale_cost",
    "cs_ext_list_price",
    "cs_ext_tax",
    "cs_coupon_amt",
    "cs_ext_ship_cost",
    "cs_net_paid",
    "cs_net_paid_inc_tax",
    "cs_net_paid_inc_ship",
    "cs_net_paid_inc_ship_tax",
    "cs_net_profit",
    "cr_returned_date_sk",
    "cr_returned_time_sk",
    "cr_item_sk",
    "cr_refunded_customer_sk",
    "cr_refunded_cdemo_sk",
    "cr_refunded_hdemo_sk",
    "cr_refunded_addr_sk",
    "cr_returning_customer_sk",
    "cr_returning_cdemo_sk",
    "cr_returning_hdemo_sk",
    "cr_returning_addr_sk",
    "cr_call_center_sk",
    "cr_catalog_page_sk",
    "cr_ship_mode_sk",
    "cr_warehouse_sk",
    "cr_reason_sk",
    "cr_order_number",
    "cr_return_quantity",
    "cr_return_amount",
    "cr_return_tax",
    "cr_return_amt_inc_tax",
    "cr_fee",
    "cr_return_ship_cost",
    "cr_refunded_cash",
    "cr_reversed_charge",
    "cr_store_credit",
    "cr_net_loss",
];

/// The catalog sold date of every order: it advances with the order
/// number, from the start of the data, by each day's share of the orders.
struct SoldDates {
    /// `ends[k]`: the orders up to day `k` (from the data start), plus 1.
    ends: Vec<u64>,
}

impl SoldDates {
    fn new(scaling: &Scaling, orders: u64) -> Self {
        let mut julian = Date::JULIAN_DATA_START_DATE;
        let mut next = scaling.get_row_count_for_date(Ds::CatalogSales, julian) + 1;
        let mut ends = vec![next];
        while next < orders {
            julian += 1;
            next += scaling.get_row_count_for_date(Ds::CatalogSales, julian);
            ends.push(next);
        }
        SoldDates { ends }
    }

    /// The julian day of order `row` (1-based).
    fn of(&self, row: u64) -> i64 {
        Date::JULIAN_DATA_START_DATE + self.ends.partition_point(|&e| e < row) as i64
    }
}

/// `catalog_sales` and `catalog_returns`, one order per source row.
pub(crate) fn catalog(
    session: &Session,
) -> impl Distribution<Item = CatalogTables> + Clone + Sync + use<> {
    let orders = session.get_scaling().get_row_count(Ds::CatalogSales);
    let cx = Ctx::new(
        session,
        orders,
        get_catalog_sales_pricing_limits(),
        &CS::CsPermute,
    );
    let dates = Arc::new(SoldDates::new(&cx.scaling, orders));
    let source = (
        range(1i64, 1),
        RowSeeds::<CS, 39>::new(),
        RowSeeds::<CR, 29>::new(),
    );
    named_all(
        CATALOG_NAMES,
        fan_out(
            source,
            move |(&order, s, r): (&i64, &[u32; 39], &[u32; 29]),
                  (sales, returns): &mut Columns<CatalogTables>| {
                let date = dates.of(order as u64);
                catalog_order(
                    &cx,
                    order,
                    date,
                    s,
                    r,
                    &mut sales.values,
                    &mut returns.values,
                )
            },
        )
        .reserving(&[9.5, 1.0]),
    )
}

fn catalog_order(
    cx: &Ctx,
    number: i64,
    date: i64,
    s: &[u32; 39],
    r: &[u32; 29],
    sales: &mut Columns<CatalogSale>,
    returns: &mut Columns<CatalogReturn>,
) {
    let scaling = &cx.scaling;
    let mut sg = Streams::<CS, 39>::at(s);
    let mut rg = None;
    let time = generate_time_join_key(HoursWeights::CatalogAndWeb, sg.get(&CS::CsSoldTimeSk));
    let call_center = if date == -1 {
        -1
    } else {
        generate_scd_join_key(Ds::CallCenter, sg.get(&CS::CsCallCenterSk), date, scaling)
    };
    let bill = [
        cx.uniform(Ds::Customer, sg.get(&CS::CsBillCustomerSk)),
        cx.uniform(Ds::CustomerDemographics, sg.get(&CS::CsBillCdemoSk)),
        cx.uniform(Ds::HouseholdDemographics, sg.get(&CS::CsBillHdemoSk)),
        cx.uniform(Ds::CustomerAddress, sg.get(&CS::CsBillAddrSk)),
    ];
    let gift = uniform_int(0, 99, sg.get(&CS::CsShipCustomerSk)) <= CS_GIFT_PCT;
    let ship = if gift {
        [
            cx.uniform(Ds::Customer, sg.get(&CS::CsShipCustomerSk)),
            cx.uniform(Ds::CustomerDemographics, sg.get(&CS::CsShipCdemoSk)),
            cx.uniform(Ds::HouseholdDemographics, sg.get(&CS::CsShipHdemoSk)),
            cx.uniform(Ds::CustomerAddress, sg.get(&CS::CsShipAddrSk)),
        ]
    } else {
        bill
    };
    let o = Order {
        date,
        time,
        call_center,
        bill,
        ship,
        number,
    };
    let mut item_index = uniform_int(1, cx.item_count, sg.get(&CS::CsSoldItemSk));
    let lines = uniform_int(4, 14, sg.get(&CS::CsOrderNumber));
    for _ in 0..lines {
        let item = cx.next_item(&mut item_index, o.date);
        let returned = uniform_int(0, 99, sg.get(&CS::CrIsReturned)) < SR_RETURN_PCT;
        let mut nbm = create_null_bit_map(NullTable::CatalogSales, sg.get(&CS::CsNulls));
        let lag = uniform_int(
            CS_MIN_SHIP_DELAY,
            CS_MAX_SHIP_DELAY,
            sg.get(&CS::CsShipDateSk),
        );
        let ship_date = if o.date == -1 {
            -1
        } else {
            o.date + lag as i64
        };
        let page = if o.date == -1 {
            -1
        } else {
            generate_catalog_page_join_key(sg.get(&CS::CsCatalogPageSk), o.date, scaling)
        };
        let ship_mode = cx.uniform(Ds::ShipMode, sg.get(&CS::CsShipModeSk));
        let warehouse = cx.uniform(Ds::Warehouse, sg.get(&CS::CsWarehouseSk));
        let promo = cx.uniform(Ds::Promotion, sg.get(&CS::CsPromoSk));
        let p = sales_pricing(&cx.limits, sg.get(&CS::CsPricing));
        let keys = [
            o.date,
            o.time,
            ship_date,
            o.bill[0],
            o.bill[1],
            o.bill[2],
            o.bill[3],
            o.ship[0],
            o.ship[1],
            o.ship[2],
            o.ship[3],
            o.call_center,
            page,
            ship_mode,
            warehouse,
        ];
        let c = &mut sales.values;
        push_sks(&mut c.0, &mut nbm, 0, keys);
        c.1.push(item as i32);
        c.2.push(sk(&mut nbm, 16, promo));
        c.3.push(o.number);
        c.4.push(p.get_quantity());
        push_sale_decimals(&mut c.5, &p);
        sales.nbm.push(nbm);
        if returned {
            let rg = rg.get_or_insert_with(|| Streams::<CR, 29>::at(r));
            let sale = LineSale {
                ship_date,
                page,
                item,
                pricing: p,
            };
            catalog_return(cx, rg, &o, &sale, returns);
        }
    }
}

fn catalog_return(
    cx: &Ctx,
    rg: &mut Streams<CR, 29>,
    o: &Order,
    s: &LineSale,
    out: &mut Columns<CatalogReturn>,
) {
    let mut nbm = create_null_bit_map(NullTable::CatalogReturns, rg.get(&CR::CrNulls));
    let mut returning = [
        cx.uniform(Ds::Customer, rg.get(&CR::CrReturningCustomerSk)),
        cx.uniform(Ds::CustomerDemographics, rg.get(&CR::CrReturningCdemoSk)),
        cx.uniform(Ds::HouseholdDemographics, rg.get(&CR::CrReturningHdemoSk)),
        cx.uniform(Ds::CustomerAddress, rg.get(&CR::CrReturningAddrSk)),
    ];
    if uniform_int(0, 99, rg.get(&CR::CrReturningCustomerSk)) < CR_GIFT_PCT {
        // the ship customer returns it; the hdemo is not on the sale
        returning[0] = o.ship[0];
        returning[1] = o.ship[1];
        returning[3] = o.ship[3];
    }
    let sold = s.pricing.get_quantity();
    let quantity = if sold == -1 {
        sold
    } else {
        uniform_int(1, sold, rg.get(&CR::CrPricing))
    };
    let p = generate_pricing_for_returns_table(rg.get(&CR::CrPricing), quantity, &s.pricing);
    let date = generate_returns_date_join_key(
        rg.get(&CR::CrReturnedDateSk),
        s.ship_date,
        CS_MIN_SHIP_DELAY,
        CS_MAX_SHIP_DELAY,
    );
    let time = generate_time_join_key(HoursWeights::CatalogAndWeb, rg.get(&CR::CrReturnedTimeSk));
    let ship_mode = cx.uniform(Ds::ShipMode, rg.get(&CR::CrShipModeSk));
    let warehouse = cx.uniform(Ds::Warehouse, rg.get(&CR::CrWarehouseSk));
    let reason = cx.uniform(Ds::Reason, rg.get(&CR::CrReasonSk));
    let keys = [
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
    let c = &mut out.values;
    c.0.push(sk(&mut nbm, 0, date));
    c.1.push(sk(&mut nbm, 1, time));
    c.2.push(s.item as i32);
    push_sks(&mut c.3, &mut nbm, 3, keys);
    c.4.push(o.number);
    c.5.push(p.get_quantity());
    push_return_decimals(&mut c.6, &p);
    out.nbm.push(nbm);
}

/// date, time, ship date; item; bill (4), ship (4), web page, web site,
/// ship mode, warehouse, promo; order number; quantity; decimals.
type WebSale = Masked<
    (
        Spread<i32, 3>,
        i32,
        Spread<i32, 13>,
        i64,
        i32,
        Spread<Dec, 15>,
    ),
    WebSaleBits,
>;
/// date, time; item; refunded (4), returning (4), web page, reason; order
/// number; quantity; decimals.
type WebReturn = Masked<(i32, i32, i32, Spread<i32, 10>, i64, i32, Spread<Dec, 9>), WebReturnBits>;
pub(crate) type WebTables = (Children<WebSale>, Children<WebReturn>);

null_bits!(
    WebSaleBits = [
        0, 1, 2, N, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, N, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33
    ]
);
null_bits!(
    WebReturnBits = [
        0, 1, N, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, N, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
    ]
);

const WEB_NAMES: &[&str] = &[
    "ws_sold_date_sk",
    "ws_sold_time_sk",
    "ws_ship_date_sk",
    "ws_item_sk",
    "ws_bill_customer_sk",
    "ws_bill_cdemo_sk",
    "ws_bill_hdemo_sk",
    "ws_bill_addr_sk",
    "ws_ship_customer_sk",
    "ws_ship_cdemo_sk",
    "ws_ship_hdemo_sk",
    "ws_ship_addr_sk",
    "ws_web_page_sk",
    "ws_web_site_sk",
    "ws_ship_mode_sk",
    "ws_warehouse_sk",
    "ws_promo_sk",
    "ws_order_number",
    "ws_quantity",
    "ws_wholesale_cost",
    "ws_list_price",
    "ws_sales_price",
    "ws_ext_discount_amt",
    "ws_ext_sales_price",
    "ws_ext_wholesale_cost",
    "ws_ext_list_price",
    "ws_ext_tax",
    "ws_coupon_amt",
    "ws_ext_ship_cost",
    "ws_net_paid",
    "ws_net_paid_inc_tax",
    "ws_net_paid_inc_ship",
    "ws_net_paid_inc_ship_tax",
    "ws_net_profit",
    "wr_returned_date_sk",
    "wr_returned_time_sk",
    "wr_item_sk",
    "wr_refunded_customer_sk",
    "wr_refunded_cdemo_sk",
    "wr_refunded_hdemo_sk",
    "wr_refunded_addr_sk",
    "wr_returning_customer_sk",
    "wr_returning_cdemo_sk",
    "wr_returning_hdemo_sk",
    "wr_returning_addr_sk",
    "wr_web_page_sk",
    "wr_reason_sk",
    "wr_order_number",
    "wr_return_quantity",
    "wr_return_amt",
    "wr_return_tax",
    "wr_return_amt_inc_tax",
    "wr_fee",
    "wr_return_ship_cost",
    "wr_refunded_cash",
    "wr_reversed_charge",
    "wr_account_credit",
    "wr_net_loss",
];

/// `web_sales` and `web_returns`, one order per source row.
pub(crate) fn web(session: &Session) -> impl Distribution<Item = WebTables> + Clone + Sync + use<> {
    let orders = session.get_scaling().get_row_count(Ds::WebSales);
    let cx = Ctx::new(
        session,
        orders,
        get_web_sales_pricing_limits(),
        &WS::WsPermutation,
    );
    let source = (
        range(1i64, 1),
        RowSeeds::<WS, 38>::new(),
        RowSeeds::<WR, 26>::new(),
    );
    named_all(
        WEB_NAMES,
        fan_out(
            source,
            move |(&order, s, r): (&i64, &[u32; 38], &[u32; 26]),
                  (sales, returns): &mut Columns<WebTables>| {
                web_order(&cx, order, s, r, &mut sales.values, &mut returns.values)
            },
        )
        .reserving(&[12.5, 1.3]),
    )
}

fn web_order(
    cx: &Ctx,
    number: i64,
    s: &[u32; 38],
    r: &[u32; 26],
    sales: &mut Columns<WebSale>,
    returns: &mut Columns<WebReturn>,
) {
    let scaling = &cx.scaling;
    let mut sg = Streams::<WS, 38>::at(s);
    let mut rg = None;
    let date = generate_sales_date_join_key(sg.get(&WS::WsSoldDateSk));
    let time = generate_time_join_key(HoursWeights::CatalogAndWeb, sg.get(&WS::WsSoldTimeSk));
    let bill = [
        cx.uniform(Ds::Customer, sg.get(&WS::WsBillCustomerSk)),
        cx.uniform(Ds::CustomerDemographics, sg.get(&WS::WsBillCdemoSk)),
        cx.uniform(Ds::HouseholdDemographics, sg.get(&WS::WsBillHdemoSk)),
        cx.uniform(Ds::CustomerAddress, sg.get(&WS::WsBillAddrSk)),
    ];
    let elsewhere = uniform_int(0, 99, sg.get(&WS::WsShipCustomerSk)) > WEB_GIFT_PCT;
    let ship = if elsewhere {
        [
            cx.uniform(Ds::Customer, sg.get(&WS::WsShipCustomerSk)),
            cx.uniform(Ds::CustomerDemographics, sg.get(&WS::WsShipCdemoSk)),
            cx.uniform(Ds::HouseholdDemographics, sg.get(&WS::WsShipHdemoSk)),
            cx.uniform(Ds::CustomerAddress, sg.get(&WS::WsShipAddrSk)),
        ]
    } else {
        bill
    };
    let o = Order {
        date,
        time,
        call_center: 0,
        bill,
        ship,
        number,
    };
    let mut item_index = uniform_int(1, cx.item_count, sg.get(&WS::WsItemSk));
    let lines = uniform_int(8, 16, sg.get(&WS::WsOrderNumber));
    for _ in 0..lines {
        let item = cx.next_item(&mut item_index, o.date);
        let returned = uniform_int(0, 99, sg.get(&WS::WrIsReturned)) < SR_RETURN_PCT;
        let mut nbm = create_null_bit_map(NullTable::WebSales, sg.get(&WS::WsNulls));
        let lag = uniform_int(1, 120, sg.get(&WS::WsShipDateSk));
        let ship_date = o.date + lag as i64;
        let page = generate_scd_join_key(Ds::WebPage, sg.get(&WS::WsWebPageSk), o.date, scaling);
        let site = generate_scd_join_key(Ds::WebSite, sg.get(&WS::WsWebSiteSk), o.date, scaling);
        let ship_mode = cx.uniform(Ds::ShipMode, sg.get(&WS::WsShipModeSk));
        let warehouse = cx.uniform(Ds::Warehouse, sg.get(&WS::WsWarehouseSk));
        let promo = cx.uniform(Ds::Promotion, sg.get(&WS::WsPromoSk));
        let p = sales_pricing(&cx.limits, sg.get(&WS::WsPricing));
        let keys = [
            o.bill[0], o.bill[1], o.bill[2], o.bill[3], o.ship[0], o.ship[1], o.ship[2], o.ship[3],
            page, site, ship_mode, warehouse, promo,
        ];
        let c = &mut sales.values;
        push_sks(&mut c.0, &mut nbm, 0, [o.date, o.time, ship_date]);
        c.1.push(item as i32);
        push_sks(&mut c.2, &mut nbm, 4, keys);
        c.3.push(o.number);
        c.4.push(p.get_quantity());
        push_sale_decimals(&mut c.5, &p);
        sales.nbm.push(nbm);
        if returned {
            let rg = rg.get_or_insert_with(|| Streams::<WR, 26>::at(r));
            let sale = LineSale {
                ship_date,
                page,
                item,
                pricing: p,
            };
            web_return(cx, rg, &o, &sale, returns);
        }
    }
}

fn web_return(
    cx: &Ctx,
    rg: &mut Streams<WR, 26>,
    o: &Order,
    s: &LineSale,
    out: &mut Columns<WebReturn>,
) {
    let mut nbm = create_null_bit_map(NullTable::WebReturns, rg.get(&WR::WrNulls));
    let date = generate_returns_date_join_key(
        rg.get(&WR::WrReturnedDateSk),
        s.ship_date,
        WS_MIN_SHIP_DELAY,
        WS_MAX_SHIP_DELAY,
    );
    let time = generate_time_join_key(HoursWeights::CatalogAndWeb, rg.get(&WR::WrReturnedTimeSk));
    let mut refunded = [
        cx.uniform(Ds::Customer, rg.get(&WR::WrRefundedCustomerSk)),
        cx.uniform(Ds::CustomerDemographics, rg.get(&WR::WrRefundedCdemoSk)),
        cx.uniform(Ds::HouseholdDemographics, rg.get(&WR::WrRefundedHdemoSk)),
        cx.uniform(Ds::CustomerAddress, rg.get(&WR::WrRefundedAddrSk)),
    ];
    if uniform_int(0, 99, rg.get(&WR::WrReturningCustomerSk)) < WEB_GIFT_PCT {
        refunded = o.ship;
    }
    let reason = cx.uniform(Ds::Reason, rg.get(&WR::WrReasonSk));
    let quantity = uniform_int(1, s.pricing.get_quantity(), rg.get(&WR::WrPricing));
    let p = generate_pricing_for_returns_table(rg.get(&WR::WrPricing), quantity, &s.pricing);
    // the returning customer is the refunded one
    let keys = [
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
    let c = &mut out.values;
    c.0.push(sk(&mut nbm, 0, date));
    c.1.push(sk(&mut nbm, 1, time));
    c.2.push(s.item as i32);
    push_sks(&mut c.3, &mut nbm, 3, keys);
    c.4.push(o.number);
    c.5.push(p.get_quantity());
    push_return_decimals(&mut c.6, &p);
    out.nbm.push(nbm);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;
    use populate_core::Columns;
    use populate_sink_arrow::IntoArrowTables;
    use populate_table::DistTables;
    use tpcdsgen::config::SessionBuilder;
    use tpcdsgen::types::generate_pricing_for_sales_table;
    use tpcdsgen_arrow::{
        CatalogReturnsArrow, CatalogSalesArrow, StoreReturnsArrow, StoreSalesArrow,
        WebReturnsArrow, WebSalesArrow,
    };

    /// Every field of a pricing.
    fn fields(p: &Pricing) -> Vec<Decimal> {
        vec![
            p.get_wholesale_cost(),
            p.get_list_price(),
            p.get_sales_price(),
            Decimal::from_integer(p.get_quantity()),
            p.get_ext_discount_amount(),
            p.get_ext_sales_price(),
            p.get_ext_wholesale_cost(),
            p.get_ext_list_price(),
            p.get_tax_percent(),
            p.get_ext_tax(),
            p.get_coupon_amount(),
            p.get_ship_cost(),
            p.get_ext_ship_cost(),
            p.get_net_paid(),
            p.get_net_paid_including_tax(),
            p.get_net_paid_including_shipping(),
            p.get_net_paid_including_shipping_and_tax(),
            p.get_net_profit(),
            p.get_refunded_cash(),
            p.get_reversed_charge(),
            p.get_store_credit(),
            p.get_fee(),
            p.get_net_loss(),
        ]
    }

    #[test]
    fn sales_pricing_matches_tpcdsgen() {
        for limits in [
            get_store_sales_pricing_limits(),
            get_catalog_sales_pricing_limits(),
            get_web_sales_pricing_limits(),
        ] {
            let mut a = RandomNumberStream::new(8);
            let mut b = a.clone();
            for _ in 0..200_000 {
                let want = generate_pricing_for_sales_table(&limits, &mut a);
                let got = sales_pricing(&limits, &mut b);
                assert_eq!(fields(&got), fields(&want));
                assert_eq!(got.get_quantity(), want.get_quantity());
            }
            // the same draws used
            assert_eq!(a.next_random(), b.next_random());
        }
    }

    fn session() -> Session {
        SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap()
    }

    fn cat(v: &[RecordBatch]) -> RecordBatch {
        concat_batches(&v[0].schema(), v).unwrap()
    }

    /// Source rows `first..=last` (1-based) of `channel` are tpcdsgen-arrow's
    /// sales and returns, in one batch and in batches of 7 source rows.
    fn check<D>(
        channel: D,
        rows: u64,
        want: impl Fn(u64, u64) -> [Vec<RecordBatch>; 2],
        ranges: &[(u64, u64)],
    ) where
        D: Distribution + Clone + Sync,
        Columns<D::Item>: IntoArrowTables,
    {
        let t = DistTables::new(&["sales", "returns"], rows, channel);
        for &(first, last) in ranges {
            let [ws, wr] = want(first, last);
            let one = t.batches(first - 1, last - first + 1);
            assert_eq!(one[0], cat(&ws), "sales {first}..={last}");
            assert_eq!(one[1], cat(&wr), "returns {first}..={last}");
            let small = t.clone().batch_rows(7);
            let mut got = [Vec::new(), Vec::new()];
            small.range(first - 1, last, &mut |k, b| got[k].push(b));
            assert_eq!(cat(&got[0]), one[0]);
            assert_eq!(cat(&got[1]), one[1]);
        }
    }

    macro_rules! reference {
        ($s:expr, $sales:ident, $returns:ident) => {
            |first, last| {
                let run = |v: &mut Vec<RecordBatch>, b: Result<RecordBatch, _>| v.push(b.unwrap());
                let (mut s, mut r) = (Vec::new(), Vec::new());
                $sales::new($s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| run(&mut s, b));
                $returns::new($s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| run(&mut r, b));
                [s, r]
            }
        };
    }

    #[test]
    fn store_matches_tpcdsgen_arrow() {
        let s = session();
        let rows = s.get_scaling().get_row_count(Ds::StoreSales);
        let want = reference!(s, StoreSalesArrow, StoreReturnsArrow);
        check(store(&s), rows, want, &[(1, 400), (12_345, 12_700)]);
    }

    #[test]
    fn catalog_matches_tpcdsgen_arrow() {
        let s = session();
        let rows = s.get_scaling().get_row_count(Ds::CatalogSales);
        let want = reference!(s, CatalogSalesArrow, CatalogReturnsArrow);
        let ranges = [(1, 300), (54_321, 54_600), (159_900, 160_000)];
        check(catalog(&s), rows, want, &ranges);
    }

    #[test]
    fn web_matches_tpcdsgen_arrow() {
        let s = session();
        let rows = s.get_scaling().get_row_count(Ds::WebSales);
        let want = reference!(s, WebSalesArrow, WebReturnsArrow);
        let ranges = [(1, 200), (31_000, 31_250), (59_900, 60_000)];
        check(web(&s), rows, want, &ranges);
    }
}
