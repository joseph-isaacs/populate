// Modified from tpcdsgen 4.0.0 for the data-comb DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Catalog sales row generator

use crate::config::Session;
use crate::distribution::hours_distribution::HoursWeights;
use crate::dsb::attributes::{attribute_map, AttributeKeyMap, MapName};
use crate::dsb::join::{join_key, scd_join_key, DemographicsJoin};
use crate::dsb::make_key_perm;
use crate::dsb::sales::{
    exp_joint_keys, order_facts, orders_before, pricing_stream, sales_pricing, Channel, OrderDates,
    PricingPerms, PRICING_DRAWS,
};
use crate::generator::{CatalogSalesGeneratorColumn, GeneratorColumn};
use crate::join_key_utils::{
    generate_catalog_page_join_key, generate_time_join_key, skip_catalog_page_join_key,
};
use crate::nulls::{create_null_bit_map, skip_null_bit_map};
use crate::permutations::{get_permutation_entry, make_permutation};
use crate::random::RandomNumberStream;
use crate::random::RandomValueGenerator;
use crate::row::catalog_returns_row_generator::RETURN_PERCENT;
use crate::row::catalog_sales_row::CatalogSalesRow;
use crate::row::{AbstractRowGenerator, LineItem};
use crate::slowly_changing_dimension_utils::match_surrogate_key;
use crate::table::Table;
use std::sync::Arc;

/// Minimum days from order to ship
pub const CS_MIN_SHIP_DELAY: i32 = 2;
/// Maximum days from order to ship
pub const CS_MAX_SHIP_DELAY: i32 = 90;
/// Percentage of orders that are gifts (ship to different customer)
const GIFT_PERCENTAGE: i32 = 10;

/// Order information shared across line items in the same order
struct OrderInfo {
    cs_sold_date_sk: i64,
    cs_sold_time_sk: i64,
    cs_call_center_sk: i64,
    cs_bill_customer_sk: i64,
    cs_bill_cdemo_sk: i64,
    cs_bill_hdemo_sk: i64,
    cs_bill_addr_sk: i64,
    cs_ship_customer_sk: i64,
    cs_ship_cdemo_sk: i64,
    cs_ship_hdemo_sk: i64,
    cs_ship_addr_sk: i64,
    cs_order_number: i64,
}

impl OrderInfo {
    fn default() -> Self {
        OrderInfo {
            cs_sold_date_sk: 0,
            cs_sold_time_sk: 0,
            cs_call_center_sk: 0,
            cs_bill_customer_sk: 0,
            cs_bill_cdemo_sk: 0,
            cs_bill_hdemo_sk: 0,
            cs_bill_addr_sk: 0,
            cs_ship_customer_sk: 0,
            cs_ship_cdemo_sk: 0,
            cs_ship_hdemo_sk: 0,
            cs_ship_addr_sk: 0,
            cs_order_number: 0,
        }
    }
}

/// Generates `catalog_sales` rows, several line items per source row (order).
///
/// [`CatalogReturnsRowGenerator`] replays the same line items through the
/// `pub(crate)` stepping methods and keeps the returned ones.
///
/// [`CatalogReturnsRowGenerator`]: crate::row::CatalogReturnsRowGenerator
pub struct CatalogSalesRowGenerator {
    abstract_generator: AbstractRowGenerator<CatalogSalesGeneratorColumn>,
    item_permutation: Vec<i32>,
    dates: OrderDates,
    remaining_line_items: i32,
    order_info: OrderInfo,
    ticket_item_base: i32,
    session: Session,
    perms: Arc<Perms>,
    /// `WS_PRICING`, which every channel prices from and nothing resets.
    pricing: RandomNumberStream,
}

/// What DSB's catalog_sales `mk_master` sets up on its first call.
struct Perms {
    ship_mode: Vec<i64>,
    promotion: Vec<i64>,
    call_center: Vec<i64>,
    warehouse: Vec<i64>,
    pricing: PricingPerms,
    item_category: Arc<AttributeKeyMap>,
    item_category_perm: Vec<i32>,
    customer_state: Arc<AttributeKeyMap>,
    customer_state_perm: Vec<i32>,
}

impl CatalogSalesRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use crate::config::Table as T;
        use CatalogSalesGeneratorColumn::*;
        let scaling = session.get_scaling();
        let rows = |t: T| scaling.get_row_count(t) as i64;
        let mut ag = AbstractRowGenerator::new(row_count);
        let item_permutation = make_permutation(
            scaling.get_id_count(T::Item) as usize,
            ag.get_random_number_stream(&CsPermute),
        );
        // The customer key permutation is drawn but unused (the customer
        // comes from the joint distribution).
        make_key_perm(
            rows(T::Customer),
            ag.get_random_number_stream(&CsBillCustomerSk),
        );
        let ship_mode = make_key_perm(
            rows(T::ShipMode),
            ag.get_random_number_stream(&CsShipModeSk),
        );
        let promotion = make_key_perm(rows(T::Promotion), ag.get_random_number_stream(&CsPromoSk));
        let call_center = make_key_perm(
            rows(T::CallCenter),
            ag.get_random_number_stream(&CsCallCenterSk),
        );
        let warehouse = make_key_perm(
            rows(T::Warehouse),
            ag.get_random_number_stream(&CsWarehouseSk),
        );
        let pricing = PricingPerms::new(Channel::Catalog, &session);
        let item_category = attribute_map(MapName::ItemCategory, &session);
        let item_category_perm = make_permutation(
            item_category.attribute_count() as usize,
            ag.get_random_number_stream(&CsSoldItemSk),
        );
        let county = attribute_map(MapName::CustomerCounty, &session);
        let s = ag.get_random_number_stream(&CsBillCustomerSk);
        make_permutation(county.attribute_count() as usize, s);
        let customer_state = attribute_map(MapName::CustomerState, &session);
        let customer_state_perm = make_permutation(customer_state.attribute_count() as usize, s);
        // Row 1's cdemo join draws that join's permutations.
        let s = ag.get_random_number_stream(&CsBillCdemoSk);
        let draws = DemographicsJoin::customer().draws;
        s.advance(draws);
        s.set_seeds_used(s.get_seeds_used() + draws as u32);
        // Hierarchical demographics keys (eight and four coordinates) and
        // the joint item and customer draw (four).
        ag.set_seeds_per_row(&CsBillCdemoSk, 8);
        ag.set_seeds_per_row(&CsBillHdemoSk, 4);
        ag.set_seeds_per_row(&CsBillCustomerSk, 4);
        ag.mark_row1();
        CatalogSalesRowGenerator {
            abstract_generator: ag,
            item_permutation,
            dates: OrderDates::new(Channel::Catalog, rows(T::CatalogSales)),
            remaining_line_items: 0,
            order_info: OrderInfo::default(),
            ticket_item_base: 0,
            session,
            perms: Arc::new(Perms {
                ship_mode,
                promotion,
                call_center,
                warehouse,
                pricing,
                item_category,
                item_category_perm,
                customer_state,
                customer_state_perm,
            }),
            pricing: pricing_stream(),
        }
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            while first.next().is_some() {}
            self.abstract_generator
                .set_row2(first.abstract_generator.seeds());
        }
    }

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .skip_rows_until_starting_row_number(starting_row_number);
        self.position_variable_streams(starting_row_number);
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .set_source_row_range(starting_row_number, ending_row_number);
        self.position_variable_streams(starting_row_number);
    }

    /// Streams whose draws per order vary, at order `start`: the shared
    /// pricing stream (eight draws per line item of every earlier order)
    /// and the ship demographics joins, which overrun their budgets of one
    /// on gift orders.
    fn position_variable_streams(&mut self, start: u64) {
        use CatalogSalesGeneratorColumn::*;
        let orders = self
            .session
            .get_scaling()
            .get_row_count(crate::config::Table::CatalogSales);
        let before = orders_before(Channel::Catalog, orders, start);
        self.pricing = pricing_stream();
        self.pricing.advance(PRICING_DRAWS * before.lines);
        if start > 1 {
            // Row 1 is in the row-2 snapshot.
            let gifts = before.minus(&order_facts(Channel::Catalog, 1, 1)).gifts;
            for (c, join) in [
                (CsShipCdemoSk, DemographicsJoin::customer()),
                (CsShipHdemoSk, DemographicsJoin::household()),
            ] {
                let over = join.sizes.len() as u64 - c.get_seeds_per_row() as u64;
                self.abstract_generator
                    .get_random_number_stream(&c)
                    .advance(over * gifts);
            }
        }
    }

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    /// Advance the random streams [`Self::generate_sales_row`] would have
    /// consumed, without calculating the row.
    ///
    /// Used by the returns generator for line items that are not returned.
    pub(crate) fn skip_item_sales_draws(&mut self) {
        use CatalogSalesGeneratorColumn::*;

        let stream = self.abstract_generator.get_random_number_stream(&CsNulls);
        skip_null_bit_map(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsShipDateSk);
        stream.next_random();

        if self.order_info.cs_sold_date_sk != -1 {
            let stream = self
                .abstract_generator
                .get_random_number_stream(&CsCatalogPageSk);
            skip_catalog_page_join_key(stream);
        }

        for c in [CsShipModeSk, CsWarehouseSk, CsPromoSk] {
            self.abstract_generator
                .get_random_number_stream(&c)
                .next_random();
        }

        self.pricing.advance(PRICING_DRAWS);
    }

    /// Generate the sales row for the current line item.
    pub(crate) fn generate_sales_row(&mut self, cs_sold_item_sk: i64) -> CatalogSalesRow {
        use CatalogSalesGeneratorColumn::*;

        let scaling = self.session.get_scaling();

        let stream = self.abstract_generator.get_random_number_stream(&CsNulls);
        let null_bit_map = create_null_bit_map(Table::CatalogSales, stream);

        // Orders are shipped some number of days after they are ordered
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsShipDateSk);
        let shipping_lag = RandomValueGenerator::generate_uniform_random_int(
            CS_MIN_SHIP_DELAY,
            CS_MAX_SHIP_DELAY,
            stream,
        );
        let cs_ship_date_sk = if self.order_info.cs_sold_date_sk == -1 {
            -1
        } else {
            self.order_info.cs_sold_date_sk + shipping_lag as i64
        };

        // Catalog page needs to be from a catalog active at the time of the sale
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsCatalogPageSk);
        let cs_catalog_page_sk = if self.order_info.cs_sold_date_sk == -1 {
            -1
        } else {
            generate_catalog_page_join_key(stream, self.order_info.cs_sold_date_sk, scaling)
        };

        let p = &self.perms;
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsShipModeSk);
        let cs_ship_mode_sk = join_key(
            crate::config::Table::ShipMode,
            stream,
            scaling,
            Some(&p.ship_mode),
        );

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsWarehouseSk);
        let cs_warehouse_sk = join_key(
            crate::config::Table::Warehouse,
            stream,
            scaling,
            Some(&p.warehouse),
        );

        let stream = self.abstract_generator.get_random_number_stream(&CsPromoSk);
        let cs_promo_sk = join_key(
            crate::config::Table::Promotion,
            stream,
            scaling,
            Some(&p.promotion),
        );

        let cs_pricing = sales_pricing(&p.pricing, &mut self.pricing);

        CatalogSalesRow::new(
            null_bit_map,
            self.order_info.cs_sold_date_sk,
            self.order_info.cs_sold_time_sk,
            cs_ship_date_sk,
            self.order_info.cs_bill_customer_sk,
            self.order_info.cs_bill_cdemo_sk,
            self.order_info.cs_bill_hdemo_sk,
            self.order_info.cs_bill_addr_sk,
            self.order_info.cs_ship_customer_sk,
            self.order_info.cs_ship_cdemo_sk,
            self.order_info.cs_ship_hdemo_sk,
            self.order_info.cs_ship_addr_sk,
            self.order_info.cs_call_center_sk,
            cs_catalog_page_sk,
            cs_ship_mode_sk,
            cs_warehouse_sk,
            cs_sold_item_sk,
            cs_promo_sk,
            self.order_info.cs_order_number,
            cs_pricing,
        )
    }

    fn generate_order_info(&mut self, row_number: u64) -> OrderInfo {
        use CatalogSalesGeneratorColumn::*;

        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let scaling = self.session.get_scaling();

        let cs_sold_date_sk = self.dates.date_of(row_number_i64);

        // cs_sold_time_sk uses cs_call_center_sk from previous order (like Java)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsSoldTimeSk);
        let cs_sold_time_sk = generate_time_join_key(HoursWeights::CatalogAndWeb, stream);

        let p = self.perms.clone();
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsCallCenterSk);
        let cs_call_center_sk = if cs_sold_date_sk == -1 {
            -1
        } else {
            scd_join_key(
                crate::config::Table::CallCenter,
                stream,
                cs_sold_date_sk,
                scaling,
                Some(&p.call_center),
            )
        };

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsBillCdemoSk);
        let cs_bill_cdemo_sk = DemographicsJoin::customer().key(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsBillHdemoSk);
        let cs_bill_hdemo_sk = DemographicsJoin::household().key(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsBillAddrSk);
        let cs_bill_addr_sk =
            join_key(crate::config::Table::CustomerAddress, stream, scaling, None);

        // DSB: the first item and the customer, jointly by category and
        // state.
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsBillCustomerSk);
        let (item, cs_bill_customer_sk) = exp_joint_keys(
            &p.item_category,
            &p.customer_state,
            &p.item_category_perm,
            &p.customer_state_perm,
            stream,
        );
        self.ticket_item_base = item as i32;

        // Most orders are for the ordering customer, some are gifts (10%)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CsShipCustomerSk);
        let gift_percentage = RandomValueGenerator::generate_uniform_random_int(0, 99, stream);

        let (cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk) =
            if gift_percentage <= GIFT_PERCENTAGE {
                let stream = self
                    .abstract_generator
                    .get_random_number_stream(&CsShipCustomerSk);
                let customer = join_key(crate::config::Table::Customer, stream, scaling, None);
                let stream = self
                    .abstract_generator
                    .get_random_number_stream(&CsShipCdemoSk);
                let cdemo = DemographicsJoin::customer().key(stream);
                let stream = self
                    .abstract_generator
                    .get_random_number_stream(&CsShipHdemoSk);
                let hdemo = DemographicsJoin::household().key(stream);
                let stream = self
                    .abstract_generator
                    .get_random_number_stream(&CsShipAddrSk);
                let addr = join_key(crate::config::Table::CustomerAddress, stream, scaling, None);
                (customer, cdemo, hdemo, addr)
            } else {
                (
                    cs_bill_customer_sk,
                    cs_bill_cdemo_sk,
                    cs_bill_hdemo_sk,
                    cs_bill_addr_sk,
                )
            };

        let cs_order_number = row_number_i64;

        OrderInfo {
            cs_sold_date_sk,
            cs_sold_time_sk,
            cs_call_center_sk,
            cs_bill_customer_sk,
            cs_bill_cdemo_sk,
            cs_bill_hdemo_sk,
            cs_bill_addr_sk,
            cs_ship_customer_sk,
            cs_ship_cdemo_sk,
            cs_ship_hdemo_sk,
            cs_ship_addr_sk,
            cs_order_number,
        }
    }

    /// Advance to the next line item, starting a new order when the
    /// previous one is complete.
    ///
    /// Returns `None` once every source row has been generated.
    pub(crate) fn next_line_item(&mut self) -> Option<LineItem> {
        use CatalogSalesGeneratorColumn::*;

        let row_number = self.abstract_generator.next_row_number()?;

        let scaling = self.session.get_scaling();
        let item_count = scaling.get_id_count(crate::config::Table::Item) as usize;

        // Start a new order if we've finished the previous one
        if self.remaining_line_items == 0 {
            self.order_info = self.generate_order_info(row_number);

            let stream = self
                .abstract_generator
                .get_random_number_stream(&CsOrderNumber);
            self.remaining_line_items =
                RandomValueGenerator::generate_uniform_random_int(4, 14, stream);
        }

        // Items need to be unique within an order
        // Use a sequence within the permutation
        self.ticket_item_base += 1;
        if self.ticket_item_base > item_count as i32 {
            self.ticket_item_base = 1;
        }

        // Get item from permutation and match surrogate key for SCD
        let item_key = get_permutation_entry(&self.item_permutation, self.ticket_item_base);
        let item_sk = match_surrogate_key(
            item_key as i64,
            self.order_info.cs_sold_date_sk,
            crate::config::Table::Item,
            self.session.get_scaling(),
        );

        // Row is returned if random_int < RETURN_PERCENT
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CrIsReturned);
        let random_int = RandomValueGenerator::generate_uniform_random_int(0, 99, stream);

        Some(LineItem {
            item_sk,
            is_returned: random_int < RETURN_PERCENT,
        })
    }

    /// Finish the current line item, after its sales row was generated or
    /// skipped.
    ///
    /// Returns true when it was the last line item of its order: the
    /// order's remaining seeds have been consumed and generation moves to
    /// the next source row.
    pub(crate) fn finish_line_item(&mut self) -> bool {
        self.remaining_line_items -= 1;
        let last_in_order = self.remaining_line_items == 0;
        if last_in_order {
            self.abstract_generator.finish_row();
        }
        last_in_order
    }
}

impl Iterator for CatalogSalesRowGenerator {
    type Item = CatalogSalesRow;

    fn next(&mut self) -> Option<CatalogSalesRow> {
        let item = self.next_line_item()?;
        let row = self.generate_sales_row(item.item_sk);
        self.finish_line_item();
        Some(row)
    }
}
