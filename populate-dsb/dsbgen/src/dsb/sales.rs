// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), scaling.c, pricing.c, joint_distribution.c and the
// w_*_sales.c files. See NOTICE.md.

//! What DSB's three sales channels share: the exponential date scaling of
//! orders, skewed pricing on one global stream, the joint item and customer
//! draw, and per-order facts (line items, gifts, returns) summed over the
//! orders before a chunk so that a chunk can start mid-table.

use super::attributes::AttributeKeyMap;
use super::{exp_decimal, exp_int, exp_rank_weight, make_bucket, make_perm, map_perm};
use super::{range_from_bucket, DATE_BUCKET_COUNT, LAMBDA_LOW};
use super::{DISCOUNT_BUCKET_COUNT, MARKUP_BUCKET_COUNT, WHOLESALE_COST_BUCKET_COUNT};
use crate::config::Session;
use crate::generator::{DateDimGeneratorColumn, GeneratorColumn, WebSalesGeneratorColumn};
use crate::random::{RandomNumberStream, RandomValueGenerator};
use crate::row::CallCenterRowGenerator;
use crate::types::{Date, Decimal, Pricing};
use std::sync::OnceLock;

/// A sales channel, in dsdgen's generation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Catalog,
    Store,
    Web,
}

impl Channel {
    fn index(self) -> usize {
        match self {
            Channel::Catalog => 0,
            Channel::Store => 1,
            Channel::Web => 2,
        }
    }

    /// The channel's table id in DSB's `tables.h`, which its pricing
    /// permutations use as a stream number.
    fn table_id(self) -> i32 {
        match self {
            Channel::Catalog => 8,
            Channel::Store => 17,
            Channel::Web => 22,
        }
    }
}

fn data_days() -> (i64, i64) {
    (Date::JULIAN_DATA_START_DATE, Date::JULIAN_DATA_END_DATE)
}

/// `makeBucket(DATE_BUCKET_COUNT, totalDays)`.
fn date_bucket() -> (i32, i32) {
    let (lo, hi) = data_days();
    make_bucket(DATE_BUCKET_COUNT, (hi - lo + 1) as i32)
}

/// `dateScaling` (or `dateScalingPermutation` with `perm`): the orders
/// dated `julian_date` out of `rows`, weighted by the exponential weight of
/// the (permuted) bucket of days holding it.
pub fn date_scaling(rows: i64, julian_date: i64, perm: Option<&[i32]>) -> i64 {
    let (days_min, days_max) = data_days();
    let (size, count) = date_bucket();
    let days = julian_date.min(days_max);
    let mut bucket = ((days - days_min) / size as i64 + 1) as i32;
    let (lo, hi) = range_from_bucket(size, bucket, days_min as i32, days_max as i32);
    if let Some(p) = perm {
        bucket = map_perm(bucket, p);
    }
    let weight =
        exp_rank_weight(bucket as i64, count as i64, LAMBDA_LOW) / (hi - lo) as f64 * (1.0 + 0.03);
    (rows as f64 * weight) as i32 as i64
}

/// The order dates: dsdgen's `jDate` and `kNewDateIndex`. The first day is
/// scaled before the date permutation exists (`skipDays`), later days
/// through it.
#[derive(Debug, Clone)]
pub struct OrderDates {
    rows: i64,
    perm: &'static [i32],
    julian_date: i64,
    next_date_index: i64,
}

impl OrderDates {
    pub fn new(channel: Channel, rows: i64) -> Self {
        let (start, _) = data_days();
        OrderDates {
            rows,
            perm: date_perm(channel),
            julian_date: start,
            next_date_index: date_scaling(rows, start, None) + 1,
        }
    }

    /// The date of order `row` (orders are visited in increasing order).
    pub fn date_of(&mut self, row: i64) -> i64 {
        while row > self.next_date_index {
            self.julian_date += 1;
            self.next_date_index += date_scaling(self.rows, self.julian_date, Some(self.perm));
        }
        self.julian_date
    }
}

/// The date bucket permutation of `channel`. Catalog, store and web sales
/// each draw one from `D_DATE_SK`, in that order.
pub fn date_perm(channel: Channel) -> &'static [i32] {
    static PERMS: OnceLock<[Vec<i32>; 3]> = OnceLock::new();
    &PERMS.get_or_init(|| {
        let c = DateDimGeneratorColumn::DDateSk;
        let mut s =
            RandomNumberStream::new_with_column(c.get_global_column_number(), c.get_seeds_per_row());
        let count = date_bucket().1;
        [
            make_perm(count, &mut s),
            make_perm(count, &mut s),
            make_perm(count, &mut s),
        ]
    })[channel.index()]
}

/// A channel's wholesale cost, markup and discount bucket permutations.
#[derive(Debug, Clone)]
pub struct PricingPerms {
    wholesale_cost: Vec<i32>,
    markup: Vec<i32>,
    discount: Vec<i32>,
}

impl PricingPerms {
    /// Drawn from the stream numbered like the channel's table: a
    /// `call_center` column, after that table.
    pub fn new(channel: Channel, session: &Session) -> Self {
        let mut s = CallCenterRowGenerator::stream_after_table(session, channel.table_id());
        PricingPerms {
            wholesale_cost: make_perm(WHOLESALE_COST_BUCKET_COUNT, &mut s),
            markup: make_perm(MARKUP_BUCKET_COUNT, &mut s),
            discount: make_perm(DISCOUNT_BUCKET_COUNT, &mut s),
        }
    }
}

/// Draws per line item of [`sales_pricing`].
pub const PRICING_DRAWS: u64 = 8;

/// `set_pricing_permutation(WS_PRICING, ..)`: every channel prices with
/// web sales' limits and its stream. Eight draws.
pub fn sales_pricing(perms: &PricingPerms, stream: &mut RandomNumberStream) -> Pricing {
    let quantity = RandomValueGenerator::generate_uniform_random_int(1, 100, stream);
    let q = Decimal::from_integer(quantity);
    let wholesale_cost = exp_decimal(
        Decimal::new(100, 2),
        Decimal::ONE_HUNDRED,
        WHOLESALE_COST_BUCKET_COUNT,
        Some(&perms.wholesale_cost),
        stream,
    );
    let ext_wholesale_cost = Decimal::multiply(q, wholesale_cost);
    let markup = exp_decimal(
        Pricing::markup_min(),
        Decimal::new(200, 2),
        MARKUP_BUCKET_COUNT,
        Some(&perms.markup),
        stream,
    );
    let list_price = Decimal::multiply(wholesale_cost, Decimal::add2(markup, Decimal::ONE));
    let discount = exp_decimal(
        Pricing::discount_min(),
        Decimal::ONE,
        DISCOUNT_BUCKET_COUNT,
        Some(&perms.discount),
        stream,
    );
    let discount = Decimal::add2(Decimal::negate(discount), Decimal::ONE);
    let sales_price = Decimal::multiply(list_price, discount);
    let ext_list_price = Decimal::multiply(list_price, q);
    let ext_sales_price = Decimal::multiply(sales_price, q);
    let ext_discount_amount = Decimal::subtract(ext_list_price, ext_sales_price);
    let coupon =
        RandomValueGenerator::generate_uniform_random_decimal(Decimal::ZERO, Decimal::ONE, stream);
    let coupon_usage = RandomValueGenerator::generate_uniform_random_int(1, 100, stream);
    let coupon_amount = if coupon_usage <= 20 {
        Decimal::multiply(ext_sales_price, coupon)
    } else {
        Decimal::ZERO
    };
    let net_paid = Decimal::subtract(ext_sales_price, coupon_amount);
    let shipping = RandomValueGenerator::generate_uniform_random_decimal(
        Decimal::ZERO,
        Decimal::ONE_HALF,
        stream,
    );
    let ship_cost = Decimal::multiply(list_price, shipping);
    let ext_ship_cost = Decimal::multiply(ship_cost, q);
    let net_paid_including_shipping = Decimal::add2(net_paid, ext_ship_cost);
    let tax_percent = RandomValueGenerator::generate_uniform_random_decimal(
        Decimal::ZERO,
        Decimal::NINE_PERCENT,
        stream,
    );
    let ext_tax = Decimal::multiply(net_paid, tax_percent);
    let net_paid_including_tax = Decimal::add2(net_paid, ext_tax);
    let net_paid_including_shipping_and_tax = Decimal::add2(net_paid_including_shipping, ext_tax);
    let net_profit = Decimal::subtract(net_paid, ext_wholesale_cost);
    Pricing::new(
        wholesale_cost,
        list_price,
        sales_price,
        quantity,
        ext_discount_amount,
        ext_sales_price,
        ext_wholesale_cost,
        ext_list_price,
        tax_percent,
        ext_tax,
        coupon_amount,
        ship_cost,
        ext_ship_cost,
        net_paid,
        net_paid_including_tax,
        net_paid_including_shipping,
        net_paid_including_shipping_and_tax,
        net_profit,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ZERO,
    )
}

/// `WS_PRICING` before any draw.
pub fn pricing_stream() -> RandomNumberStream {
    let c = WebSalesGeneratorColumn::WsPricing;
    RandomNumberStream::new_with_column(c.get_global_column_number(), c.get_seeds_per_row())
}

/// `genrandExpJointDistribution` with `JOINT_DISTRIBUTION_BUCKET_COUNT`:
/// a cell of the attribute grid of `map1` x `map2` by an exponential,
/// unpermuted bucket and an exponential value in it (DSB passes
/// `DIST_EXP_RANK_LOW` for both), each coordinate permuted,
/// then a uniform key of each attribute. Four draws.
pub fn exp_joint_keys(
    map1: &AttributeKeyMap,
    map2: &AttributeKeyMap,
    perm1: &[i32],
    perm2: &[i32],
    stream: &mut RandomNumberStream,
) -> (i64, i64) {
    let (c1, c2) = (map1.attribute_count(), map2.attribute_count());
    let n = c1 * c2;
    let (size, count) = make_bucket(100, n);
    let b = exp_int(1, count, stream);
    let (lo, hi) = range_from_bucket(size, b, 1, n);
    let cell = exp_int(lo, hi, stream);
    let a1 = map_perm((cell - 1) / c2 + 1, perm1);
    let mut a2 = (cell - 1) % c2 + 1;
    a2 = map_perm((a2 + a1) % c2 + 1, perm2);
    let mut a1 = a1;
    while map1.key_count(a1) == 0 {
        a1 = (a1 + 1) % c1 + 1;
    }
    while map2.key_count(a2) == 0 {
        a2 = (a2 + 1) % c2 + 1;
    }
    (
        map1.key_with_attribute(a1, stream),
        map2.key_with_attribute(a2, stream),
    )
}

/// Sums over a run of orders of what makes a channel's variable streams
/// move: line items (`WS_PRICING` draws eight each), gift orders (the ship
/// demographics joins overrun their budgets) and the returns' demographics
/// draws beyond their per-order budgets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderFacts {
    pub orders: u64,
    pub lines: u64,
    pub gifts: u64,
    /// Returning cdemo draws over budget (eight per return).
    pub return_cdemo_over: u64,
    /// Returning hdemo draws over budget (four per return).
    pub return_hdemo_over: u64,
}

impl OrderFacts {
    fn add(&mut self, o: &OrderFacts) {
        self.orders += o.orders;
        self.lines += o.lines;
        self.gifts += o.gifts;
        self.return_cdemo_over += o.return_cdemo_over;
        self.return_hdemo_over += o.return_hdemo_over;
    }

    pub fn minus(&self, o: &OrderFacts) -> OrderFacts {
        OrderFacts {
            orders: self.orders - o.orders,
            lines: self.lines - o.lines,
            gifts: self.gifts - o.gifts,
            return_cdemo_over: self.return_cdemo_over - o.return_cdemo_over,
            return_hdemo_over: self.return_hdemo_over - o.return_hdemo_over,
        }
    }
}

/// How a channel decides an order's line items, gift and returns: each from
/// a stream that moves a fixed amount per order, so any order can be
/// replayed on its own.
struct FactStreams {
    lines: (RandomNumberStream, u64, i32, i32),
    gift: Option<(RandomNumberStream, u64, i32)>,
    returned: (RandomNumberStream, u64, i32),
    return_budget: u64,
}

fn stream_of<C: GeneratorColumn>(c: C, first_order: u64) -> (RandomNumberStream, u64) {
    let mut s =
        RandomNumberStream::new_with_column(c.get_global_column_number(), c.get_seeds_per_row());
    s.skip_rows(first_order - 1);
    (s, c.get_seeds_per_row() as u64)
}

impl FactStreams {
    fn new(channel: Channel, first_order: u64) -> Self {
        use crate::generator::CatalogReturnsGeneratorColumn as Cr;
        use crate::generator::CatalogSalesGeneratorColumn as Cs;
        match channel {
            Channel::Catalog => {
                let (l, lb) = stream_of(Cs::CsOrderNumber, first_order);
                let (g, gb) = stream_of(Cs::CsShipCustomerSk, first_order);
                let (r, rb) = stream_of(Cs::CrIsReturned, first_order);
                FactStreams {
                    lines: (l, lb, 4, 14),
                    gift: Some((g, gb, 10)),
                    returned: (r, rb, 15),
                    return_budget: Cr::CrReturningCdemoSk.get_seeds_per_row() as u64,
                }
            }
            Channel::Store | Channel::Web => unimplemented!("DSB {channel:?} order facts"),
        }
    }

    fn next(&mut self) -> OrderFacts {
        let (s, budget, lo, hi) = &mut self.lines;
        let lines = RandomValueGenerator::generate_uniform_random_int(*lo, *hi, s) as u64;
        s.advance(*budget - 1);
        let gifts = match &mut self.gift {
            Some((s, budget, pct)) => {
                let g = RandomValueGenerator::generate_uniform_random_int(0, 99, s) <= *pct;
                s.advance(*budget - 1);
                g as u64
            }
            None => 0,
        };
        let (s, budget, pct) = &mut self.returned;
        let mut returns = 0u64;
        for _ in 0..lines {
            returns += (RandomValueGenerator::generate_uniform_random_int(0, 99, s) < *pct) as u64;
        }
        s.advance(*budget - lines);
        OrderFacts {
            orders: 1,
            lines,
            gifts,
            return_cdemo_over: (8 * returns).saturating_sub(self.return_budget),
            return_hdemo_over: (4 * returns).saturating_sub(self.return_budget),
        }
    }
}

/// Facts of orders `first..=last` (1-based), replayed.
pub fn order_facts(channel: Channel, first: u64, last: u64) -> OrderFacts {
    let mut sum = OrderFacts::default();
    if first > last {
        return sum;
    }
    let mut f = FactStreams::new(channel, first);
    for _ in first..=last {
        sum.add(&f.next());
    }
    sum
}

/// Orders per checkpoint of [`orders_before`]'s prefix sums.
const CHECKPOINT: u64 = 1 << 16;

type PrefixCache = std::sync::Mutex<
    std::collections::HashMap<(Channel, u64), std::sync::Arc<Vec<OrderFacts>>>,
>;

/// Prefix sums of [`OrderFacts`] at every [`CHECKPOINT`] orders of a
/// channel with `orders` orders, built in parallel on first use.
fn checkpoints(channel: Channel, orders: u64) -> std::sync::Arc<Vec<OrderFacts>> {
    static CACHE: OnceLock<PrefixCache> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(c) = cache.lock().expect("prefix cache").get(&(channel, orders)) {
        return c.clone();
    }
    let blocks = orders.div_ceil(CHECKPOINT);
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get()) as u64;
    let mut sums = vec![OrderFacts::default(); blocks as usize];
    std::thread::scope(|scope| {
        let per = blocks.div_ceil(threads.max(1)).max(1) as usize;
        for (t, part) in sums.chunks_mut(per).enumerate() {
            scope.spawn(move || {
                for (i, slot) in part.iter_mut().enumerate() {
                    let b = (t * per + i) as u64;
                    let first = b * CHECKPOINT + 1;
                    let last = ((b + 1) * CHECKPOINT).min(orders);
                    *slot = order_facts(channel, first, last);
                }
            });
        }
    });
    let mut prefix = Vec::with_capacity(blocks as usize + 1);
    let mut run = OrderFacts::default();
    prefix.push(run);
    for s in &sums {
        run.add(s);
        prefix.push(run);
    }
    let prefix = std::sync::Arc::new(prefix);
    cache
        .lock()
        .expect("prefix cache")
        .insert((channel, orders), prefix.clone());
    prefix
}

/// Facts of orders `1..order` of a channel with `orders` orders.
pub fn orders_before(channel: Channel, orders: u64, order: u64) -> OrderFacts {
    let done = order - 1;
    if done < CHECKPOINT {
        return order_facts(channel, 1, done);
    }
    let prefix = checkpoints(channel, orders);
    let k = (done / CHECKPOINT) as usize;
    let mut f = prefix[k];
    f.add(&order_facts(channel, k as u64 * CHECKPOINT + 1, done));
    f
}
