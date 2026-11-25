//! A sales table and its returns from one pass, built straight into Arrow
//! columns or `.dat` text.
//!
//! dsbgen's returns generators replay every sales line item, so generating
//! `store_sales` and `store_returns` separately makes the sales twice. When
//! both are asked for with the same chunks, [`Channel::run`] (Arrow) and
//! [`Channel::run_dat`] (text) drive the returns generator's
//! [`next_with_sale`](StoreReturnsRowGenerator::next_with_sale) once, and
//! push each row's values into per-column buffers or the chunk's text as it
//! is made, instead of collecting rows and converting them column by
//! column. The `push_*` functions follow dsbgen-arrow's conversions
//! (`batch_from_rows`) column for column; the tests compare the outputs.

use crate::TableName;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, RecordBatch};
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, SchemaRef};
use dsbgen::config::Session;
use dsbgen::row::{
    CatalogReturnsRow, CatalogReturnsRowGenerator, CatalogSalesRow, StoreReturnsRow,
    StoreReturnsRowGenerator, StoreSalesRow, WebReturnsRow, WebReturnsRowGenerator, WebSalesRow,
};
use dsbgen::types::Decimal;
use dsbgen_arrow::{
    CatalogReturnsArrow, CatalogSalesArrow, StoreReturnsArrow, StoreSalesArrow, WebReturnsArrow,
    WebSalesArrow,
};
use populate_table::text::{decimal2_field, int_field};
use std::mem::replace;
use std::sync::Arc;

/// A sales channel: a sales table and its returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Store,
    Catalog,
    Web,
}

impl Channel {
    /// The channel of a sales or returns table, and whether it is the
    /// sales one.
    pub(crate) fn of(t: TableName) -> Option<(Channel, bool)> {
        Some(match t {
            TableName::StoreSales => (Channel::Store, true),
            TableName::StoreReturns => (Channel::Store, false),
            TableName::CatalogSales => (Channel::Catalog, true),
            TableName::CatalogReturns => (Channel::Catalog, false),
            TableName::WebSales => (Channel::Web, true),
            TableName::WebReturns => (Channel::Web, false),
            _ => return None,
        })
    }

    fn schemas(self) -> [SchemaRef; 2] {
        match self {
            Channel::Store => [
                StoreSalesArrow::schema_ref(),
                StoreReturnsArrow::schema_ref(),
            ],
            Channel::Catalog => [
                CatalogSalesArrow::schema_ref(),
                CatalogReturnsArrow::schema_ref(),
            ],
            Channel::Web => [WebSalesArrow::schema_ref(), WebReturnsArrow::schema_ref()],
        }
    }

    /// Sales (output 0) and returns (output 1) of source rows
    /// `first..=last` (1-based) of `source_rows`, in batches of `batch`
    /// rows (the last of each output may be shorter).
    pub(crate) fn run(
        self,
        session: &Session,
        source_rows: u64,
        first: u64,
        last: u64,
        batch: usize,
        emit: &mut dyn FnMut(usize, RecordBatch),
    ) {
        let [s, r] = self.schemas();
        let mut out = ArrowOut {
            b: [Builder::new(s, batch), Builder::new(r, batch)],
            batch,
            emit,
        };
        self.run_into(session, source_rows, first, last, &mut out);
        out.finish();
    }

    /// [`Self::run`] as `.dat` text appended to `sales` and `returns`: the
    /// bytes `dat::write_batch` makes of its batches, without building them.
    pub(crate) fn run_dat(
        self,
        session: &Session,
        source_rows: u64,
        first: u64,
        last: u64,
        sales: &mut Vec<u8>,
        returns: &mut Vec<u8>,
    ) {
        let mut out = TextOut([Text(sales), Text(returns)]);
        self.run_into(session, source_rows, first, last, &mut out);
    }

    fn run_into(
        self,
        session: &Session,
        source_rows: u64,
        first: u64,
        last: u64,
        out: &mut impl Out,
    ) {
        macro_rules! drive {
            ($gen:ident, $push_sale:ident, $push_return:ident) => {{
                let mut g = $gen::new(session.clone(), source_rows);
                g.set_source_row_range(first, last);
                while let Some((sale, ret)) = g.next_with_sale() {
                    $push_sale(out.row(0), &sale);
                    out.done(0);
                    if let Some(r) = ret {
                        $push_return(out.row(1), &r);
                        out.done(1);
                    }
                }
            }};
        }
        match self {
            Channel::Store => drive!(
                StoreReturnsRowGenerator,
                push_store_sales,
                push_store_returns
            ),
            Channel::Catalog => drive!(
                CatalogReturnsRowGenerator,
                push_catalog_sales,
                push_catalog_returns
            ),
            Channel::Web => drive!(WebReturnsRowGenerator, push_web_sales, push_web_returns),
        }
    }
}

/// Whether bit `pos` of a row's null bitmap makes that column NULL.
#[inline(always)]
fn null(nbm: i64, pos: u32) -> bool {
    (nbm >> pos) & 1 != 0
}

/// Where a channel's rows go: one row's values in schema order, then
/// [`Row::end`]. [`Builder`] makes Arrow columns of them, [`Text`] dsdgen's
/// `.dat` lines.
trait Row {
    /// An Int32 key: NULL by bit `pos` of `nbm` or by being negative.
    fn sk(&mut self, nbm: i64, pos: u32, v: i64);
    /// An Int64 key: NULL by bit `pos` of `nbm` or by being negative.
    fn sk64(&mut self, nbm: i64, pos: u32, v: i64);
    fn int(&mut self, v: i32, valid: bool);
    fn long(&mut self, v: i64, valid: bool);
    /// A decimal(7,2): NULL by bit `pos` of `nbm`.
    fn dec(&mut self, nbm: i64, pos: u32, v: Decimal);
    fn end(&mut self);
}

/// One nullable column being built.
struct Col<T> {
    values: Vec<T>,
    valid: BooleanBufferBuilder,
}

impl<T: ArrowNativeType> Col<T> {
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

    fn finish<A: ArrowPrimitiveType<Native = T>>(&mut self, cap: usize) -> PrimitiveArray<A> {
        let values = replace(&mut self.values, Vec::with_capacity(cap));
        let nulls =
            NullBuffer::new(replace(&mut self.valid, BooleanBufferBuilder::new(cap)).finish());
        PrimitiveArray::new(ScalarBuffer::from(values), Some(nulls))
    }
}

/// A column's Arrow type, as the sales and returns schemas use them.
#[derive(Clone, Copy)]
enum Kind {
    I32,
    I64,
    Dec,
}

/// The columns of a batch of one table, in schema order.
struct Builder {
    schema: SchemaRef,
    layout: Vec<Kind>,
    i32s: Vec<Col<i32>>,
    i64s: Vec<Col<i64>>,
    decs: Vec<Col<i128>>,
    rows: usize,
    cap: usize,
    /// The next column of each kind in the row being pushed.
    next: [usize; 3],
}

impl Builder {
    fn new(schema: SchemaRef, cap: usize) -> Self {
        let layout: Vec<Kind> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int32 => Kind::I32,
                DataType::Int64 => Kind::I64,
                DataType::Decimal128(7, 2) => Kind::Dec,
                t => unreachable!("a sales or returns column of type {t}"),
            })
            .collect();
        let count = |k: fn(&Kind) -> bool| layout.iter().filter(|x| k(x)).count();
        Builder {
            i32s: (0..count(|k| matches!(k, Kind::I32)))
                .map(|_| Col::new(cap))
                .collect(),
            i64s: (0..count(|k| matches!(k, Kind::I64)))
                .map(|_| Col::new(cap))
                .collect(),
            decs: (0..count(|k| matches!(k, Kind::Dec)))
                .map(|_| Col::new(cap))
                .collect(),
            schema,
            layout,
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
                    Kind::I32 => {
                        i += 1;
                        Arc::new(self.i32s[i - 1].finish::<Int32Type>(self.cap))
                    }
                    Kind::I64 => {
                        l += 1;
                        Arc::new(self.i64s[l - 1].finish::<Int64Type>(self.cap))
                    }
                    Kind::Dec => {
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

impl Row for Builder {
    #[inline(always)]
    fn sk(&mut self, nbm: i64, pos: u32, v: i64) {
        self.int(v as i32, !null(nbm, pos) && v >= 0);
    }

    #[inline(always)]
    fn sk64(&mut self, nbm: i64, pos: u32, v: i64) {
        self.long(v, !null(nbm, pos) && v >= 0);
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
        c.push(v.get_number() as i128, !null(nbm, pos));
    }

    #[inline(always)]
    fn end(&mut self) {
        self.next = [0; 3];
        self.rows += 1;
    }
}

/// `.dat` lines, as `dat::write_batch` writes the same columns.
struct Text<'a>(&'a mut Vec<u8>);

impl Row for Text<'_> {
    #[inline(always)]
    fn sk(&mut self, nbm: i64, pos: u32, v: i64) {
        self.long(v as i32 as i64, !null(nbm, pos) && v >= 0);
    }

    #[inline(always)]
    fn sk64(&mut self, nbm: i64, pos: u32, v: i64) {
        self.long(v, !null(nbm, pos) && v >= 0);
    }

    #[inline(always)]
    fn int(&mut self, v: i32, valid: bool) {
        self.long(v as i64, valid);
    }

    #[inline(always)]
    fn long(&mut self, v: i64, valid: bool) {
        if valid {
            int_field(self.0, v);
        } else {
            self.0.push(b'|');
        }
    }

    #[inline(always)]
    fn dec(&mut self, nbm: i64, pos: u32, v: Decimal) {
        if null(nbm, pos) {
            self.0.push(b'|');
        } else {
            decimal2_field(self.0, v.get_number());
        }
    }

    #[inline(always)]
    fn end(&mut self) {
        self.0.push(b'\n');
    }
}

/// A channel's two outputs, sales (0) and returns (1).
trait Out {
    type R: Row;
    fn row(&mut self, k: usize) -> &mut Self::R;
    /// A row of output `k` is complete.
    fn done(&mut self, k: usize);
}

/// Arrow batches of `batch` rows, passed to `emit` as they fill.
struct ArrowOut<'a> {
    b: [Builder; 2],
    batch: usize,
    emit: &'a mut dyn FnMut(usize, RecordBatch),
}

impl ArrowOut<'_> {
    fn finish(&mut self) {
        for k in 0..2 {
            if self.b[k].rows > 0 {
                let b = self.b[k].finish();
                (self.emit)(k, b);
            }
        }
    }
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
}

// The columns of each table, in schema order, as dsbgen-arrow's
// `batch_from_rows` converts them (null bitmap positions included: some
// columns share one, as dsdgen's do).

fn push_store_sales(b: &mut impl Row, r: &StoreSalesRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_ss_sold_date_sk());
    b.sk(n, 1, r.get_ss_sold_time_sk());
    b.sk(n, 2, r.get_ss_sold_item_sk());
    b.sk(n, 3, r.get_ss_sold_customer_sk());
    b.sk(n, 4, r.get_ss_sold_cdemo_sk());
    b.sk(n, 5, r.get_ss_sold_hdemo_sk());
    b.sk(n, 6, r.get_ss_sold_addr_sk());
    b.sk(n, 7, r.get_ss_sold_store_sk());
    b.sk(n, 8, r.get_ss_sold_promo_sk());
    b.sk64(n, 9, r.get_ss_ticket_number());
    b.int(r.get_ss_pricing().get_quantity(), !null(n, 10));
    b.dec(n, 11, r.get_ss_pricing().get_wholesale_cost());
    b.dec(n, 12, r.get_ss_pricing().get_list_price());
    b.dec(n, 13, r.get_ss_pricing().get_sales_price());
    b.dec(n, 14, r.get_ss_pricing().get_coupon_amount());
    b.dec(n, 15, r.get_ss_pricing().get_ext_sales_price());
    b.dec(n, 16, r.get_ss_pricing().get_ext_wholesale_cost());
    b.dec(n, 17, r.get_ss_pricing().get_ext_list_price());
    b.dec(n, 18, r.get_ss_pricing().get_ext_tax());
    b.dec(n, 14, r.get_ss_pricing().get_coupon_amount());
    b.dec(n, 19, r.get_ss_pricing().get_net_paid());
    b.dec(n, 20, r.get_ss_pricing().get_net_paid_including_tax());
    b.dec(n, 21, r.get_ss_pricing().get_net_profit());
    b.end();
}

fn push_store_returns(b: &mut impl Row, r: &StoreReturnsRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_sr_returned_date_sk());
    b.sk(n, 1, r.get_sr_returned_time_sk());
    b.sk(n, 2, r.get_sr_item_sk());
    b.sk(n, 3, r.get_sr_customer_sk());
    b.sk(n, 4, r.get_sr_cdemo_sk());
    b.sk(n, 5, r.get_sr_hdemo_sk());
    b.sk(n, 6, r.get_sr_addr_sk());
    b.sk(n, 7, r.get_sr_store_sk());
    b.sk(n, 8, r.get_sr_reason_sk());
    b.sk64(n, 9, r.get_sr_ticket_number());
    b.int(r.get_sr_pricing().get_quantity(), !null(n, 10));
    b.dec(n, 11, r.get_sr_pricing().get_net_paid());
    b.dec(n, 12, r.get_sr_pricing().get_ext_tax());
    b.dec(n, 13, r.get_sr_pricing().get_net_paid_including_tax());
    b.dec(n, 14, r.get_sr_pricing().get_fee());
    b.dec(n, 15, r.get_sr_pricing().get_ext_ship_cost());
    b.dec(n, 16, r.get_sr_pricing().get_refunded_cash());
    b.dec(n, 17, r.get_sr_pricing().get_reversed_charge());
    b.dec(n, 18, r.get_sr_pricing().get_store_credit());
    b.dec(n, 19, r.get_sr_pricing().get_net_loss());
    b.end();
}

fn push_catalog_sales(b: &mut impl Row, r: &CatalogSalesRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_cs_sold_date_sk());
    b.sk(n, 1, r.get_cs_sold_time_sk());
    b.sk(n, 2, r.get_cs_ship_date_sk());
    b.sk(n, 3, r.get_cs_bill_customer_sk());
    b.sk(n, 4, r.get_cs_bill_cdemo_sk());
    b.sk(n, 5, r.get_cs_bill_hdemo_sk());
    b.sk(n, 6, r.get_cs_bill_addr_sk());
    b.sk(n, 7, r.get_cs_ship_customer_sk());
    b.sk(n, 8, r.get_cs_ship_cdemo_sk());
    b.sk(n, 9, r.get_cs_ship_hdemo_sk());
    b.sk(n, 10, r.get_cs_ship_addr_sk());
    b.sk(n, 11, r.get_cs_call_center_sk());
    b.sk(n, 12, r.get_cs_catalog_page_sk());
    b.sk(n, 13, r.get_cs_ship_mode_sk());
    b.sk(n, 14, r.get_cs_warehouse_sk());
    b.sk(n, 15, r.get_cs_sold_item_sk());
    b.sk(n, 16, r.get_cs_promo_sk());
    b.long(r.get_cs_order_number(), !null(n, 17));
    b.int(r.get_cs_pricing().get_quantity(), !null(n, 18));
    b.dec(n, 19, r.get_cs_pricing().get_wholesale_cost());
    b.dec(n, 20, r.get_cs_pricing().get_list_price());
    b.dec(n, 21, r.get_cs_pricing().get_sales_price());
    b.dec(n, 24, r.get_cs_pricing().get_ext_discount_amount());
    b.dec(n, 23, r.get_cs_pricing().get_ext_sales_price());
    b.dec(n, 25, r.get_cs_pricing().get_ext_wholesale_cost());
    b.dec(n, 26, r.get_cs_pricing().get_ext_list_price());
    b.dec(n, 27, r.get_cs_pricing().get_ext_tax());
    b.dec(n, 22, r.get_cs_pricing().get_coupon_amount());
    b.dec(n, 28, r.get_cs_pricing().get_ext_ship_cost());
    b.dec(n, 29, r.get_cs_pricing().get_net_paid());
    b.dec(n, 30, r.get_cs_pricing().get_net_paid_including_tax());
    b.dec(n, 31, r.get_cs_pricing().get_net_paid_including_shipping());
    b.dec(
        n,
        32,
        r.get_cs_pricing().get_net_paid_including_shipping_and_tax(),
    );
    b.dec(n, 33, r.get_cs_pricing().get_net_profit());
    b.end();
}

fn push_catalog_returns(b: &mut impl Row, r: &CatalogReturnsRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_cr_returned_date_sk());
    b.sk(n, 1, r.get_cr_returned_time_sk());
    b.sk(n, 2, r.get_cr_item_sk());
    b.sk(n, 3, r.get_cr_refunded_customer_sk());
    b.sk(n, 4, r.get_cr_refunded_cdemo_sk());
    b.sk(n, 5, r.get_cr_refunded_hdemo_sk());
    b.sk(n, 6, r.get_cr_refunded_addr_sk());
    b.sk(n, 7, r.get_cr_returning_customer_sk());
    b.sk(n, 8, r.get_cr_returning_cdemo_sk());
    b.sk(n, 9, r.get_cr_returning_hdemo_sk());
    b.sk(n, 10, r.get_cr_returning_addr_sk());
    b.sk(n, 11, r.get_cr_call_center_sk());
    b.sk(n, 12, r.get_cr_catalog_page_sk());
    b.sk(n, 13, r.get_cr_ship_mode_sk());
    b.sk(n, 14, r.get_cr_warehouse_sk());
    b.sk(n, 15, r.get_cr_reason_sk());
    b.long(r.get_cr_order_number(), !null(n, 16));
    b.int(r.get_cr_pricing().get_quantity(), !null(n, 17));
    b.dec(n, 18, r.get_cr_pricing().get_net_paid());
    b.dec(n, 19, r.get_cr_pricing().get_ext_tax());
    b.dec(n, 20, r.get_cr_pricing().get_net_paid_including_tax());
    b.dec(n, 21, r.get_cr_pricing().get_fee());
    b.dec(n, 22, r.get_cr_pricing().get_ext_ship_cost());
    b.dec(n, 23, r.get_cr_pricing().get_refunded_cash());
    b.dec(n, 24, r.get_cr_pricing().get_reversed_charge());
    b.dec(n, 25, r.get_cr_pricing().get_store_credit());
    b.dec(n, 26, r.get_cr_pricing().get_net_loss());
    b.end();
}

fn push_web_sales(b: &mut impl Row, r: &WebSalesRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_ws_sold_date_sk());
    b.sk(n, 1, r.get_ws_sold_time_sk());
    b.sk(n, 2, r.get_ws_ship_date_sk());
    b.sk(n, 3, r.get_ws_item_sk());
    b.sk(n, 4, r.get_ws_bill_customer_sk());
    b.sk(n, 5, r.get_ws_bill_cdemo_sk());
    b.sk(n, 6, r.get_ws_bill_hdemo_sk());
    b.sk(n, 7, r.get_ws_bill_addr_sk());
    b.sk(n, 8, r.get_ws_ship_customer_sk());
    b.sk(n, 9, r.get_ws_ship_cdemo_sk());
    b.sk(n, 10, r.get_ws_ship_hdemo_sk());
    b.sk(n, 11, r.get_ws_ship_addr_sk());
    b.sk(n, 12, r.get_ws_web_page_sk());
    b.sk(n, 13, r.get_ws_web_site_sk());
    b.sk(n, 14, r.get_ws_ship_mode_sk());
    b.sk(n, 15, r.get_ws_warehouse_sk());
    b.sk(n, 16, r.get_ws_promo_sk());
    b.long(r.get_ws_order_number(), !null(n, 17));
    b.int(r.get_ws_pricing().get_quantity(), !null(n, 18));
    b.dec(n, 19, r.get_ws_pricing().get_wholesale_cost());
    b.dec(n, 20, r.get_ws_pricing().get_list_price());
    b.dec(n, 21, r.get_ws_pricing().get_sales_price());
    b.dec(n, 22, r.get_ws_pricing().get_ext_discount_amount());
    b.dec(n, 23, r.get_ws_pricing().get_ext_sales_price());
    b.dec(n, 24, r.get_ws_pricing().get_ext_wholesale_cost());
    b.dec(n, 25, r.get_ws_pricing().get_ext_list_price());
    b.dec(n, 26, r.get_ws_pricing().get_ext_tax());
    b.dec(n, 27, r.get_ws_pricing().get_coupon_amount());
    b.dec(n, 28, r.get_ws_pricing().get_ext_ship_cost());
    b.dec(n, 29, r.get_ws_pricing().get_net_paid());
    b.dec(n, 30, r.get_ws_pricing().get_net_paid_including_tax());
    b.dec(n, 31, r.get_ws_pricing().get_net_paid_including_shipping());
    b.dec(
        n,
        32,
        r.get_ws_pricing().get_net_paid_including_shipping_and_tax(),
    );
    b.dec(n, 33, r.get_ws_pricing().get_net_profit());
    b.end();
}

fn push_web_returns(b: &mut impl Row, r: &WebReturnsRow) {
    let n = r.null_bit_map();
    b.sk(n, 0, r.get_wr_returned_date_sk());
    b.sk(n, 1, r.get_wr_returned_time_sk());
    b.sk(n, 2, r.get_wr_item_sk());
    b.sk(n, 3, r.get_wr_refunded_customer_sk());
    b.sk(n, 4, r.get_wr_refunded_cdemo_sk());
    b.sk(n, 5, r.get_wr_refunded_hdemo_sk());
    b.sk(n, 6, r.get_wr_refunded_addr_sk());
    b.sk(n, 7, r.get_wr_returning_customer_sk());
    b.sk(n, 8, r.get_wr_returning_cdemo_sk());
    b.sk(n, 9, r.get_wr_returning_hdemo_sk());
    b.sk(n, 10, r.get_wr_returning_addr_sk());
    b.sk(n, 11, r.get_wr_web_page_sk());
    b.sk(n, 12, r.get_wr_reason_sk());
    b.long(r.get_wr_order_number(), !null(n, 13));
    b.int(r.get_wr_pricing().get_quantity(), !null(n, 14));
    b.dec(n, 15, r.get_wr_pricing().get_net_paid());
    b.dec(n, 16, r.get_wr_pricing().get_ext_tax());
    b.dec(n, 17, r.get_wr_pricing().get_net_paid_including_tax());
    b.dec(n, 18, r.get_wr_pricing().get_fee());
    b.dec(n, 19, r.get_wr_pricing().get_ext_ship_cost());
    b.dec(n, 20, r.get_wr_pricing().get_refunded_cash());
    b.dec(n, 21, r.get_wr_pricing().get_reversed_charge());
    b.dec(n, 22, r.get_wr_pricing().get_store_credit());
    b.dec(n, 23, r.get_wr_pricing().get_net_loss());
    b.end();
}
