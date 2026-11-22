//! `store_sales` and `store_returns` from one pass, built straight into
//! Arrow columns.
//!
//! tpcdsgen's `StoreReturnsRowGenerator` replays every `store_sales` line
//! item to find the ~10% that are returned, so generating both tables costs
//! the sales work twice. Here one loop walks the tickets once and emits both
//! tables. The loop is a port of tpcdsgen's `StoreSalesRowGenerator` and
//! `StoreReturnsRowGenerator` (Apache-2.0, from the tpchgen-rs project,
//! itself a port of dsdgen). It draws from the same random streams in the
//! same order, using tpcdsgen's public helpers, so the output is identical;
//! the tests compare it with tpcdsgen-arrow for both tables.
//!
//! Columns are pushed into per-column buffers as each line item is made,
//! instead of collecting row structs and converting them column by column.

use crate::seeds;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::distribution::hours_distribution::HoursWeights;
use tpcdsgen::generator::StoreReturnsGeneratorColumn as R;
use tpcdsgen::generator::StoreSalesGeneratorColumn as S;
use tpcdsgen::join_key_utils::{
    CS_MAX_SHIP_DELAY, CS_MIN_SHIP_DELAY, generate_returns_date_join_key,
    generate_sales_date_join_key, generate_scd_join_key, generate_time_join_key,
    generate_uniform_join_key,
};
use tpcdsgen::nulls::create_null_bit_map;
use tpcdsgen::permutations::{get_permutation_entry, make_permutation};
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::AbstractRowGenerator;
use tpcdsgen::slowly_changing_dimension_utils::match_surrogate_key;
use tpcdsgen::table::Table as NullTable;
use tpcdsgen::types::{
    Decimal, Pricing, generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_store_sales_pricing_limits,
};
use tpcdsgen_arrow::{StoreReturnsArrow, StoreSalesArrow};

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
