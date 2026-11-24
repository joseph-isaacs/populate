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

//! Store sales row generator

use crate::config::Session;
use crate::distribution::hours_distribution::HoursWeights;
use crate::dsb::attributes::{attribute_map, AttributeKeyMap, MapName};
use crate::dsb::join::{join_key, scd_join_key, DemographicsJoin};
use crate::dsb::make_key_perm;
use crate::dsb::sales::{
    exp_joint_keys, pricing_stream_at, sales_pricing, Channel, OrderDates, PricingPerms,
    PRICING_DRAWS,
};
use crate::generator::StoreSalesGeneratorColumn;
use crate::join_key_utils::generate_time_join_key;
use crate::nulls::{create_null_bit_map, skip_null_bit_map};
use crate::permutations::{get_permutation_entry, make_permutation};
use crate::random::RandomNumberStream;
use crate::random::RandomValueGenerator;
use crate::row::store_sales_row::StoreSalesRow;
use crate::row::{AbstractRowGenerator, LineItem};
use crate::slowly_changing_dimension_utils::match_surrogate_key;
use crate::table::Table;
use std::sync::Arc;

/// Percentage of sales that get returned (DSB's `SR_RETURN_PCT`)
const SR_RETURN_PCT: i32 = 25;

/// Order information shared across line items in the same order
struct OrderInfo {
    ss_sold_store_sk: i64,
    ss_sold_time_sk: i64,
    ss_sold_date_sk: i64,
    ss_sold_customer_sk: i64,
    ss_sold_cdemo_sk: i64,
    ss_sold_hdemo_sk: i64,
    ss_sold_addr_sk: i64,
    ss_ticket_number: i64,
}

impl OrderInfo {
    #[allow(clippy::too_many_arguments)]
    fn new(
        ss_sold_store_sk: i64,
        ss_sold_time_sk: i64,
        ss_sold_date_sk: i64,
        ss_sold_customer_sk: i64,
        ss_sold_cdemo_sk: i64,
        ss_sold_hdemo_sk: i64,
        ss_sold_addr_sk: i64,
        ss_ticket_number: i64,
    ) -> Self {
        OrderInfo {
            ss_sold_store_sk,
            ss_sold_time_sk,
            ss_sold_date_sk,
            ss_sold_customer_sk,
            ss_sold_cdemo_sk,
            ss_sold_hdemo_sk,
            ss_sold_addr_sk,
            ss_ticket_number,
        }
    }

    fn default() -> Self {
        OrderInfo {
            ss_sold_store_sk: 0,
            ss_sold_time_sk: 0,
            ss_sold_date_sk: 0,
            ss_sold_customer_sk: 0,
            ss_sold_cdemo_sk: 0,
            ss_sold_hdemo_sk: 0,
            ss_sold_addr_sk: 0,
            ss_ticket_number: 0,
        }
    }
}

/// Generates `store_sales` rows, several line items per source row (ticket).
///
/// [`StoreReturnsRowGenerator`] replays the same line items through the
/// `pub(crate)` stepping methods and keeps the returned ones.
///
/// [`StoreReturnsRowGenerator`]: crate::row::StoreReturnsRowGenerator
pub struct StoreSalesRowGenerator {
    abstract_generator: AbstractRowGenerator<StoreSalesGeneratorColumn>,
    item_permutation: Vec<i32>,
    dates: OrderDates,
    remaining_line_items: i32,
    order_info: OrderInfo,
    item_index: i32,
    session: Session,
    perms: Arc<Perms>,
    /// `WS_PRICING`, shared with the other channels.
    pricing: RandomNumberStream,
}

/// What DSB's store_sales `mk_master` sets up on its first call.
struct Perms {
    promotion: Vec<i64>,
    store: Vec<i64>,
    pricing: PricingPerms,
    item_category: Arc<AttributeKeyMap>,
    item_category_perm: Vec<i32>,
    customer_state: Arc<AttributeKeyMap>,
    customer_state_perm: Vec<i32>,
}

impl StoreSalesRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use crate::config::Table as T;
        use StoreSalesGeneratorColumn::*;
        let scaling = session.get_scaling();
        let rows = |t: T| scaling.get_row_count(t) as i64;
        let mut ag = AbstractRowGenerator::new(row_count);
        let item_permutation = make_permutation(
            scaling.get_id_count(T::Item) as usize,
            ag.get_random_number_stream(&SsPermutation),
        );
        // Drawn, unused: the customer comes from the joint distribution.
        make_key_perm(
            rows(T::Customer),
            ag.get_random_number_stream(&SsSoldCustomerSk),
        );
        let promotion = make_key_perm(
            rows(T::Promotion),
            ag.get_random_number_stream(&SsSoldPromoSk),
        );
        let store = make_key_perm(rows(T::Store), ag.get_random_number_stream(&SsSoldStoreSk));
        let pricing = PricingPerms::new(Channel::Store, &session);
        let item_category = attribute_map(MapName::ItemCategory, &session);
        let item_category_perm = make_permutation(
            item_category.attribute_count() as usize,
            ag.get_random_number_stream(&SsSoldItemSk),
        );
        let county = attribute_map(MapName::CustomerCounty, &session);
        let s = ag.get_random_number_stream(&SsSoldCustomerSk);
        make_permutation(county.attribute_count() as usize, s);
        let customer_state = attribute_map(MapName::CustomerState, &session);
        let customer_state_perm = make_permutation(customer_state.attribute_count() as usize, s);
        // The joint item and customer draw (four) and hierarchical
        // demographics keys (eight and four coordinates).
        ag.set_seeds_per_row(&SsSoldCustomerSk, 4);
        ag.set_seeds_per_row(&SsSoldCdemoSk, 8);
        ag.set_seeds_per_row(&SsSoldHdemoSk, 4);
        ag.mark_row1();
        StoreSalesRowGenerator {
            abstract_generator: ag,
            item_permutation,
            dates: OrderDates::new(Channel::Store, rows(T::StoreSales)),
            remaining_line_items: 0,
            order_info: OrderInfo::default(),
            item_index: 0,
            pricing: pricing_stream_at(Channel::Store, &session, 1),
            session,
            perms: Arc::new(Perms {
                promotion,
                store,
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

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .skip_rows_until_starting_row_number(starting_row_number);
        self.pricing = pricing_stream_at(Channel::Store, &self.session, starting_row_number);
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .set_source_row_range(starting_row_number, ending_row_number);
        self.pricing = pricing_stream_at(Channel::Store, &self.session, starting_row_number);
    }

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    /// Advance the random streams [`Self::generate_sales_row`] would have
    /// consumed, without calculating the row.
    ///
    /// Used by the returns generator for line items that are not returned.
    pub(crate) fn skip_item_sales_draws(&mut self) {
        use StoreSalesGeneratorColumn::*;

        let stream = self.abstract_generator.get_random_number_stream(&SsNulls);
        skip_null_bit_map(stream);

        self.abstract_generator
            .get_random_number_stream(&SsSoldPromoSk)
            .next_random();

        self.pricing.advance(PRICING_DRAWS);
    }

    /// Generate the sales row for the current line item.
    pub(crate) fn generate_sales_row(&mut self, ss_sold_item_sk: i64) -> StoreSalesRow {
        use StoreSalesGeneratorColumn::*;

        let scaling = self.session.get_scaling();

        let stream = self.abstract_generator.get_random_number_stream(&SsNulls);
        let null_bit_map = create_null_bit_map(Table::StoreSales, stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldPromoSk);
        let ss_sold_promo_sk = join_key(
            crate::config::Table::Promotion,
            stream,
            scaling,
            Some(&self.perms.promotion),
        );

        let ss_pricing = sales_pricing(&self.perms.pricing, &mut self.pricing);

        StoreSalesRow::new(
            null_bit_map,
            self.order_info.ss_sold_date_sk,
            self.order_info.ss_sold_time_sk,
            ss_sold_item_sk,
            self.order_info.ss_sold_customer_sk,
            self.order_info.ss_sold_cdemo_sk,
            self.order_info.ss_sold_hdemo_sk,
            self.order_info.ss_sold_addr_sk,
            self.order_info.ss_sold_store_sk,
            ss_sold_promo_sk,
            self.order_info.ss_ticket_number,
            ss_pricing,
        )
    }

    fn generate_order_info(&mut self, row_number: u64) -> OrderInfo {
        use StoreSalesGeneratorColumn::*;

        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let scaling = self.session.get_scaling();

        let p = self.perms.clone();
        let ss_sold_date_sk = self.dates.date_of(row_number_i64);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldStoreSk);
        let ss_sold_store_sk = scd_join_key(
            crate::config::Table::Store,
            stream,
            1,
            scaling,
            Some(&p.store),
        );

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldTimeSk);
        let ss_sold_time_sk = generate_time_join_key(HoursWeights::Store, stream);

        // DSB: the first item and the customer, jointly by category and
        // state.
        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldCustomerSk);
        let (item, ss_sold_customer_sk) = exp_joint_keys(
            &p.item_category,
            &p.customer_state,
            &p.item_category_perm,
            &p.customer_state_perm,
            stream,
        );
        self.item_index = item as i32;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldCdemoSk);
        let ss_sold_cdemo_sk = DemographicsJoin::customer().key(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldHdemoSk);
        let ss_sold_hdemo_sk = DemographicsJoin::household().key(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&SsSoldAddrSk);
        let ss_sold_addr_sk =
            join_key(crate::config::Table::CustomerAddress, stream, scaling, None);

        let ss_ticket_number = row_number_i64;

        OrderInfo::new(
            ss_sold_store_sk,
            ss_sold_time_sk,
            ss_sold_date_sk,
            ss_sold_customer_sk,
            ss_sold_cdemo_sk,
            ss_sold_hdemo_sk,
            ss_sold_addr_sk,
            ss_ticket_number,
        )
    }

    /// Advance to the next line item, starting a new ticket when the
    /// previous one is complete.
    ///
    /// Returns `None` once every source row has been generated.
    pub(crate) fn next_line_item(&mut self) -> Option<LineItem> {
        use StoreSalesGeneratorColumn::*;

        let row_number = self.abstract_generator.next_row_number()?;

        let item_count = self
            .session
            .get_scaling()
            .get_id_count(crate::config::Table::Item) as usize;

        // Start a new order if we've finished the previous one
        if self.remaining_line_items == 0 {
            self.order_info = self.generate_order_info(row_number);

            let stream = self
                .abstract_generator
                .get_random_number_stream(&SsTicketNumber);
            self.remaining_line_items =
                RandomValueGenerator::generate_uniform_random_int(8, 16, stream);
        }

        // Items need to be unique within an order
        // Use a sequence within the permutation
        self.item_index += 1;
        if self.item_index > item_count as i32 {
            self.item_index = 1;
        }

        // Get item from permutation and match surrogate key for SCD
        let item_key = get_permutation_entry(&self.item_permutation, self.item_index);
        let item_sk = match_surrogate_key(
            item_key as i64,
            self.order_info.ss_sold_date_sk,
            crate::config::Table::Item,
            self.session.get_scaling(),
        );

        // Row is returned if random_int < SR_RETURN_PCT
        let stream = self
            .abstract_generator
            .get_random_number_stream(&SrIsReturned);
        let random_int = RandomValueGenerator::generate_uniform_random_int(0, 99, stream);

        Some(LineItem {
            item_sk,
            is_returned: random_int < SR_RETURN_PCT,
        })
    }

    /// Finish the current line item, after its sales row was generated or
    /// skipped.
    ///
    /// Returns true when it was the last line item of its ticket: the
    /// ticket's remaining seeds have been consumed and generation moves to
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

impl Iterator for StoreSalesRowGenerator {
    type Item = StoreSalesRow;

    fn next(&mut self) -> Option<StoreSalesRow> {
        let item = self.next_line_item()?;
        let row = self.generate_sales_row(item.item_sk);
        self.finish_line_item();
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Session, SessionBuilder};
    use crate::row::dat_values;
    use crate::row::StoreReturnsRowGenerator;

    #[test]
    fn test_store_sales_row_generator_creation() {
        let generator = StoreSalesRowGenerator::new(Session::default(), 1);
        assert!(!generator.item_permutation.is_empty());
        assert_eq!(generator.remaining_line_items, 0);
    }

    #[test]
    fn test_store_sales_row_generation() {
        let mut generator = StoreSalesRowGenerator::new(Session::default(), 1);
        let row = generator.next().expect("first line item");
        assert_eq!(dat_values(&row).len(), 23);
    }

    #[test]
    fn test_store_sales_order_grouping() {
        let mut generator = StoreSalesRowGenerator::new(Session::default(), 1);
        // A ticket has at least 8 line items, so the first two rows share
        // its ticket number
        let ticket1 = dat_values(&generator.next().unwrap())[9].clone();
        let ticket2 = dat_values(&generator.next().unwrap())[9].clone();
        assert_eq!(ticket1, ticket2);
    }

    /// Splitting a table into source row ranges must produce exactly the same
    /// rows as generating it in one pass.
    macro_rules! assert_source_row_ranges_concatenate {
        ($generator:ty) => {{
            let session = SessionBuilder::new()
                .with_scale_factor(1.0)
                .build()
                .expect("session");
            let source_rows = session
                .get_scaling()
                .get_row_count(crate::config::Table::StoreSales);
            assert!(source_rows > 100, "need enough rows to split");
            let dat = |start: u64, end: u64| -> Vec<String> {
                let mut rows = <$generator>::new(session.clone(), source_rows);
                rows.set_source_row_range(start, end);
                rows.map(|row| row.to_string()).collect()
            };

            // DSB needs scale 1, so split a few stretches of it, near the
            // start and deep into the table.
            for (start, mid, end) in [(1, 1, 400), (1, 250, 600), (150_000, 150_321, 150_700)] {
                let whole = dat(start, end);
                let mut chunked = dat(start, mid);
                chunked.extend(dat(mid + 1, end));
                assert!(!whole.is_empty());
                assert_eq!(whole, chunked);
            }
        }};
    }

    #[test]
    fn store_sales_splits_into_source_row_ranges() {
        assert_source_row_ranges_concatenate!(StoreSalesRowGenerator);
    }

    #[test]
    fn store_returns_splits_into_source_row_ranges() {
        assert_source_row_ranges_concatenate!(StoreReturnsRowGenerator);
    }
}
