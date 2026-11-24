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

//! Web sales row generator

use crate::config::Session;
use crate::distribution::hours_distribution::HoursWeights;
use crate::dsb::attributes::{attribute_map, AttributeKeyMap, MapName};
use crate::dsb::join::{join_key, scd_join_key, DemographicsJoin};
use crate::dsb::make_key_perm;
use crate::dsb::sales::{
    exp_joint_keys, order_facts, orders_before, sales_pricing, total_lines, Channel, OrderDates,
    PricingPerms, PRICING_DRAWS,
};
use crate::generator::{GeneratorColumn, WebSalesGeneratorColumn};
use crate::join_key_utils::generate_time_join_key;
use crate::nulls::{create_null_bit_map, skip_null_bit_map};
use crate::permutations::{get_permutation_entry, make_permutation};
use crate::random::RandomValueGenerator;
use crate::row::web_sales_row::WebSalesRow;
use crate::row::{AbstractRowGenerator, LineItem};
use crate::slowly_changing_dimension_utils::match_surrogate_key;
use crate::table::Table;
use std::sync::Arc;

/// Percentage for determining if order ships to different customer (gift)
/// Note: In Java, condition is `randomInt > GIFT_PERCENTAGE`, meaning ~92% are gifts
pub const GIFT_PERCENTAGE: i32 = 7;
/// Percentage of orders that get returned
pub const RETURN_PERCENTAGE: i32 = 20;

/// Order information shared across line items in the same order
struct OrderInfo {
    ws_sold_date_sk: i64,
    ws_sold_time_sk: i64,
    ws_bill_customer_sk: i64,
    ws_bill_cdemo_sk: i64,
    ws_bill_hdemo_sk: i64,
    ws_bill_addr_sk: i64,
    ws_ship_customer_sk: i64,
    ws_ship_cdemo_sk: i64,
    ws_ship_hdemo_sk: i64,
    ws_ship_addr_sk: i64,
    ws_order_number: i64,
}

impl OrderInfo {
    fn default() -> Self {
        OrderInfo {
            ws_sold_date_sk: 0,
            ws_sold_time_sk: 0,
            ws_bill_customer_sk: 0,
            ws_bill_cdemo_sk: 0,
            ws_bill_hdemo_sk: 0,
            ws_bill_addr_sk: 0,
            ws_ship_customer_sk: 0,
            ws_ship_cdemo_sk: 0,
            ws_ship_hdemo_sk: 0,
            ws_ship_addr_sk: 0,
            ws_order_number: 0,
        }
    }
}

/// Generates `web_sales` rows, several line items per source row (order).
///
/// [`WebReturnsRowGenerator`] replays the same line items through the
/// `pub(crate)` stepping methods and keeps the returned ones.
///
/// [`WebReturnsRowGenerator`]: crate::row::WebReturnsRowGenerator
pub struct WebSalesRowGenerator {
    abstract_generator: AbstractRowGenerator<WebSalesGeneratorColumn>,
    item_permutation: Option<Vec<i32>>,
    dates: OrderDates,
    remaining_line_items: i32,
    order_info: OrderInfo,
    item_index: i32,
    session: Session,
    perms: Arc<Perms>,
}

/// What DSB's web_sales `mk_master` sets up on its first call.
struct Perms {
    ship_mode: Vec<i64>,
    promotion: Vec<i64>,
    warehouse: Vec<i64>,
    web_site: Vec<i64>,
    web_page: Vec<i64>,
    pricing: PricingPerms,
    item_category: Arc<AttributeKeyMap>,
    item_category_perm: Vec<i32>,
    customer_state: Arc<AttributeKeyMap>,
    customer_state_perm: Vec<i32>,
}

impl WebSalesRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use crate::config::Table as T;
        use WebSalesGeneratorColumn::*;
        let scaling = session.get_scaling();
        let rows = |t: T| scaling.get_row_count(t) as i64;
        let mut ag = AbstractRowGenerator::new(row_count);
        // Drawn, unused: the customer comes from the joint distribution.
        make_key_perm(
            rows(T::Customer),
            ag.get_random_number_stream(&WsBillCustomerSk),
        );
        let ship_mode = make_key_perm(
            rows(T::ShipMode),
            ag.get_random_number_stream(&WsShipModeSk),
        );
        let promotion = make_key_perm(rows(T::Promotion), ag.get_random_number_stream(&WsPromoSk));
        let warehouse = make_key_perm(
            rows(T::Warehouse),
            ag.get_random_number_stream(&WsWarehouseSk),
        );
        let web_site = make_key_perm(rows(T::WebSite), ag.get_random_number_stream(&WsWebSiteSk));
        let web_page = make_key_perm(rows(T::WebPage), ag.get_random_number_stream(&WsWebPageSk));
        let pricing = PricingPerms::new(Channel::Web, &session);
        let item_category = attribute_map(MapName::ItemCategory, &session);
        let item_category_perm = make_permutation(
            item_category.attribute_count() as usize,
            ag.get_random_number_stream(&WsItemSk),
        );
        let county = attribute_map(MapName::CustomerCounty, &session);
        let s = ag.get_random_number_stream(&WsBillCustomerSk);
        make_permutation(county.attribute_count() as usize, s);
        let customer_state = attribute_map(MapName::CustomerState, &session);
        let customer_state_perm = make_permutation(customer_state.attribute_count() as usize, s);
        // WS_PRICING comes to web sales after every catalog and store line
        // item has drawn from it; its count of draws is never reset, so the
        // first row overruns its budget.
        let s = ag.get_random_number_stream(&WsPricing);
        let earlier = PRICING_DRAWS
            * (total_lines(Channel::Catalog, &session) + total_lines(Channel::Store, &session));
        s.advance(earlier);
        s.set_seeds_used(
            s.get_seeds_used()
                .saturating_add(earlier.min(u32::MAX as u64) as u32),
        );
        // mk_detail's first call draws the item permutation (its stream has
        // no budget, so drawing it up front is the same).
        let item_permutation = make_permutation(
            scaling.get_id_count(T::Item) as usize,
            ag.get_random_number_stream(&WsPermutation),
        );
        ag.set_seeds_per_row(&WsBillCustomerSk, 4);
        ag.set_seeds_per_row(&WsBillCdemoSk, 8);
        ag.set_seeds_per_row(&WsBillHdemoSk, 4);
        ag.mark_row1();
        WebSalesRowGenerator {
            abstract_generator: ag,
            item_permutation: Some(item_permutation),
            dates: OrderDates::new(Channel::Web, rows(T::WebSales)),
            remaining_line_items: 0,
            order_info: OrderInfo::default(),
            item_index: 0,
            session,
            perms: Arc::new(Perms {
                ship_mode,
                promotion,
                warehouse,
                web_site,
                web_page,
                pricing,
                item_category,
                item_category_perm,
                customer_state,
                customer_state_perm,
            }),
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

    /// The ship demographics joins overrun their budgets on gift orders:
    /// move them past the earlier orders' overruns (row 1 is in the row-2
    /// snapshot).
    fn position_variable_streams(&mut self, start: u64) {
        use WebSalesGeneratorColumn::*;
        if start <= 1 {
            return;
        }
        let orders = self
            .session
            .get_scaling()
            .get_row_count(crate::config::Table::WebSales);
        let before = orders_before(Channel::Web, orders, start);
        let gifts = before.minus(&order_facts(Channel::Web, 1, 1)).gifts;
        for (c, join) in [
            (WsShipCdemoSk, DemographicsJoin::customer()),
            (WsShipHdemoSk, DemographicsJoin::household()),
        ] {
            let over = join.sizes.len() as u64 - c.get_seeds_per_row() as u64;
            self.abstract_generator
                .get_random_number_stream(&c)
                .advance(over * gifts);
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

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    /// Advance the random streams [`Self::generate_sales_row`] would have
    /// consumed, without calculating the row.
    ///
    /// Used by the returns generator for line items that are not returned.
    pub(crate) fn skip_item_sales_draws(&mut self) {
        use WebSalesGeneratorColumn::*;

        let stream = self.abstract_generator.get_random_number_stream(&WsNulls);
        skip_null_bit_map(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsShipDateSk);
        stream.next_random();

        for c in [
            WsWebPageSk,
            WsWebSiteSk,
            WsShipModeSk,
            WsWarehouseSk,
            WsPromoSk,
        ] {
            self.abstract_generator
                .get_random_number_stream(&c)
                .next_random();
        }
        // WS_PRICING has a row budget, so count the skipped draws.
        let s = self.abstract_generator.get_random_number_stream(&WsPricing);
        s.advance(PRICING_DRAWS);
        s.set_seeds_used(s.get_seeds_used() + PRICING_DRAWS as u32);
    }

    /// Generate the sales row for the current line item.
    pub(crate) fn generate_sales_row(&mut self, ws_item_sk: i64) -> WebSalesRow {
        use WebSalesGeneratorColumn::*;

        let scaling = self.session.get_scaling();

        let stream = self.abstract_generator.get_random_number_stream(&WsNulls);
        let null_bit_map = create_null_bit_map(Table::WebSales, stream);

        // Orders are shipped some number of days after they are ordered (1-120 days)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsShipDateSk);
        let ship_lag = RandomValueGenerator::generate_uniform_random_int(1, 120, stream);
        let ws_ship_date_sk = self.order_info.ws_sold_date_sk + ship_lag as i64;

        // The web page needs to be valid for the sale date
        let p = &self.perms;
        let date = self.order_info.ws_sold_date_sk;
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsWebPageSk);
        let ws_web_page_sk = scd_join_key(
            crate::config::Table::WebPage,
            stream,
            date,
            scaling,
            Some(&p.web_page),
        );
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsWebSiteSk);
        let ws_web_site_sk = scd_join_key(
            crate::config::Table::WebSite,
            stream,
            date,
            scaling,
            Some(&p.web_site),
        );
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsShipModeSk);
        let ws_ship_mode_sk = join_key(
            crate::config::Table::ShipMode,
            stream,
            scaling,
            Some(&p.ship_mode),
        );
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsWarehouseSk);
        let ws_warehouse_sk = join_key(
            crate::config::Table::Warehouse,
            stream,
            scaling,
            Some(&p.warehouse),
        );
        let stream = self.abstract_generator.get_random_number_stream(&WsPromoSk);
        let ws_promo_sk = join_key(
            crate::config::Table::Promotion,
            stream,
            scaling,
            Some(&p.promotion),
        );
        let pricing = p.pricing.clone();
        let stream = self.abstract_generator.get_random_number_stream(&WsPricing);
        let ws_pricing = sales_pricing(&pricing, stream);
        WebSalesRow::new(
            null_bit_map,
            self.order_info.ws_sold_date_sk,
            self.order_info.ws_sold_time_sk,
            ws_ship_date_sk,
            ws_item_sk,
            self.order_info.ws_bill_customer_sk,
            self.order_info.ws_bill_cdemo_sk,
            self.order_info.ws_bill_hdemo_sk,
            self.order_info.ws_bill_addr_sk,
            self.order_info.ws_ship_customer_sk,
            self.order_info.ws_ship_cdemo_sk,
            self.order_info.ws_ship_hdemo_sk,
            self.order_info.ws_ship_addr_sk,
            ws_web_page_sk,
            ws_web_site_sk,
            ws_ship_mode_sk,
            ws_warehouse_sk,
            ws_promo_sk,
            self.order_info.ws_order_number,
            ws_pricing,
        )
    }

    fn generate_order_info(&mut self, row_number: u64) -> OrderInfo {
        use WebSalesGeneratorColumn::*;

        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let scaling = self.session.get_scaling();

        let ws_sold_date_sk = self.dates.date_of(row_number_i64);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsSoldTimeSk);
        let ws_sold_time_sk = generate_time_join_key(HoursWeights::CatalogAndWeb, stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsBillCdemoSk);
        let ws_bill_cdemo_sk = DemographicsJoin::customer().key(stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsBillHdemoSk);
        let ws_bill_hdemo_sk = DemographicsJoin::household().key(stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsBillAddrSk);
        let ws_bill_addr_sk =
            join_key(crate::config::Table::CustomerAddress, stream, scaling, None);
        // DSB: the first item and the customer, jointly by category and
        // state.
        let p = self.perms.clone();
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsBillCustomerSk);
        let (item, ws_bill_customer_sk) = exp_joint_keys(
            &p.item_category,
            &p.customer_state,
            &p.item_category_perm,
            &p.customer_state_perm,
            stream,
        );
        self.item_index = item as i32;
        // Usually the billing info and shipping info are the same.
        // If it's a "gift", they'll be different.
        // Note: Java uses `randomInt > GIFT_PERCENTAGE` which means ~92% get different ship info
        let mut ws_ship_customer_sk = ws_bill_customer_sk;
        let mut ws_ship_cdemo_sk = ws_bill_cdemo_sk;
        let mut ws_ship_hdemo_sk = ws_bill_hdemo_sk;
        let mut ws_ship_addr_sk = ws_bill_addr_sk;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&WsShipCustomerSk);
        let random_int = RandomValueGenerator::generate_uniform_random_int(0, 99, stream);

        // Java: if (randomInt > GIFT_PERCENTAGE)
        if random_int > GIFT_PERCENTAGE {
            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsShipCustomerSk);
            ws_ship_customer_sk = join_key(crate::config::Table::Customer, stream, scaling, None);

            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsShipCdemoSk);
            ws_ship_cdemo_sk = DemographicsJoin::customer().key(stream);

            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsShipHdemoSk);
            ws_ship_hdemo_sk = DemographicsJoin::household().key(stream);

            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsShipAddrSk);
            ws_ship_addr_sk =
                join_key(crate::config::Table::CustomerAddress, stream, scaling, None);
        }

        let ws_order_number = row_number_i64;

        OrderInfo {
            ws_sold_date_sk,
            ws_sold_time_sk,
            ws_bill_customer_sk,
            ws_bill_cdemo_sk,
            ws_bill_hdemo_sk,
            ws_bill_addr_sk,
            ws_ship_customer_sk,
            ws_ship_cdemo_sk,
            ws_ship_hdemo_sk,
            ws_ship_addr_sk,
            ws_order_number,
        }
    }

    /// Advance to the next line item, starting a new order when the
    /// previous one is complete.
    ///
    /// Returns `None` once every source row has been generated.
    pub(crate) fn next_line_item(&mut self) -> Option<LineItem> {
        use WebSalesGeneratorColumn::*;

        let row_number = self.abstract_generator.next_row_number()?;

        let item_count = self
            .session
            .get_scaling()
            .get_id_count(crate::config::Table::Item) as usize;

        // Initialize item permutation if needed
        if self.item_permutation.is_none() {
            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsPermutation);
            self.item_permutation = Some(make_permutation(item_count, stream));
        }

        // Start a new order if we've finished the previous one
        if self.remaining_line_items == 0 {
            self.order_info = self.generate_order_info(row_number);

            // WebSales has 8-16 line items per order (vs 4-14 for CatalogSales)
            let stream = self
                .abstract_generator
                .get_random_number_stream(&WsOrderNumber);
            self.remaining_line_items =
                RandomValueGenerator::generate_uniform_random_int(8, 16, stream);
        }

        // Items need to be unique within an order
        self.item_index += 1;
        if self.item_index > item_count as i32 {
            self.item_index = 1;
        }

        // Get item from permutation and match surrogate key for SCD
        let permutation = self.item_permutation.as_ref().unwrap();
        let item_key = get_permutation_entry(permutation, self.item_index);
        let item_sk = match_surrogate_key(
            item_key as i64,
            self.order_info.ws_sold_date_sk,
            crate::config::Table::Item,
            self.session.get_scaling(),
        );

        // Row is returned if random_int < RETURN_PERCENTAGE
        let stream = self
            .abstract_generator
            .get_random_number_stream(&WrIsReturned);
        let random_int = RandomValueGenerator::generate_uniform_random_int(0, 99, stream);

        Some(LineItem {
            item_sk,
            is_returned: random_int < RETURN_PERCENTAGE,
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

impl Iterator for WebSalesRowGenerator {
    type Item = WebSalesRow;

    fn next(&mut self) -> Option<WebSalesRow> {
        let item = self.next_line_item()?;
        let row = self.generate_sales_row(item.item_sk);
        self.finish_line_item();
        Some(row)
    }
}
