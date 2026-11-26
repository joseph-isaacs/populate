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

use crate::business_key_generator::make_business_key;
use crate::config::Session;
use crate::config::Table as ConfigTable;
use crate::dsb::join::scd_join_key;
use crate::dsb::{exp_bucketed_int, exp_int, make_bucket, make_perm, map_perm};
use crate::generator::PromotionGeneratorColumn;
use crate::nulls::create_null_bit_map;
use crate::random::{RandomTextGenerator, RandomValueGenerator};
use crate::row::{AbstractRowGenerator, PromotionRow};
use crate::table::Table;
use crate::types::{Date, Decimal};

const PROMO_START_MIN: i32 = -720;
const PROMO_START_MAX: i32 = 100;
const PROMO_LENGTH_MIN: i32 = 1;
const PROMO_LENGTH_MAX: i32 = 60;
const PROMO_NAME_LENGTH: i32 = 5;
const PROMO_DETAIL_LENGTH_MIN: i32 = 20;
const PROMO_DETAIL_LENGTH_MAX: i32 = 60;

pub struct PromotionRowGenerator {
    abstract_row_generator: AbstractRowGenerator<PromotionGeneratorColumn>,
    text: RandomTextGenerator,
    session: Session,
    perms: Perms,
}

/// The permutations DSB's `mk_w_promotion` makes on its first call.
struct Perms {
    flags: Vec<i32>,
    length: Vec<i32>,
    start_bucket: (i32, i32),
    start: Vec<i32>,
}

impl PromotionRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use PromotionGeneratorColumn::*;
        let mut ag = AbstractRowGenerator::new(row_count);
        let flags = make_perm(512, ag.get_random_number_stream(&PChannelDmail));
        let length = make_perm(
            PROMO_LENGTH_MAX - PROMO_LENGTH_MIN + 1,
            ag.get_random_number_stream(&PEndDateId),
        );
        let start_bucket = make_bucket(50, PROMO_START_MAX - PROMO_START_MIN + 1);
        let start = make_perm(start_bucket.1, ag.get_random_number_stream(&PStartDateId));
        // An exponential bucket plus a uniform day: two draws every row.
        ag.set_seeds_per_row(&PStartDateId, 2);
        ag.mark_row1();
        PromotionRowGenerator {
            abstract_row_generator: ag,
            text: RandomTextGenerator::new(),
            session,
            perms: Perms {
                flags,
                length,
                start_bucket,
                start,
            },
        }
    }

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_row_generator
            .skip_rows_until_starting_row_number(starting_row_number);
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_row_generator
            .set_source_row_range(starting_row_number, ending_row_number);
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_row_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            first.next();
            self.abstract_row_generator
                .set_row2(first.abstract_row_generator.seeds());
        }
    }

    fn generate_promotion_row(&mut self, row_number: u64) -> PromotionRow {
        let session = &self.session;
        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let scaling = session.get_scaling();

        let null_bit_map = create_null_bit_map(
            Table::Promotion,
            self.abstract_row_generator
                .get_random_number_stream(&PromotionGeneratorColumn::PNulls),
        );

        let p_promo_sk = row_number_i64;
        let p_promo_id = make_business_key(row_number);

        // DSB: exponential, permuted start bucket and length.
        let p_start_date_id = Date::JULIAN_DATE_MINIMUM as i64
            + exp_bucketed_int(
                PROMO_START_MIN,
                PROMO_START_MAX,
                self.perms.start_bucket,
                &self.perms.start,
                self.abstract_row_generator
                    .get_random_number_stream(&PromotionGeneratorColumn::PStartDateId),
            ) as i64;

        let length = exp_int(
            PROMO_LENGTH_MIN,
            PROMO_LENGTH_MAX,
            self.abstract_row_generator
                .get_random_number_stream(&PromotionGeneratorColumn::PEndDateId),
        ) - (PROMO_LENGTH_MIN - 1);
        let length = map_perm(length, &self.perms.length) + PROMO_LENGTH_MIN - 1;
        let p_end_date_id = p_start_date_id + length as i64;

        let p_item_sk = scd_join_key(
            ConfigTable::Item,
            self.abstract_row_generator
                .get_random_number_stream(&PromotionGeneratorColumn::PItemSk),
            1,
            scaling,
            None,
        );

        let p_cost = Decimal::new(100000, 2);
        let p_response_target = 1;

        // generate_word doesn't use random numbers - it deterministically creates from seed
        // We still need to consume the stream to maintain RNG state
        let _promo_name_stream = self
            .abstract_row_generator
            .get_random_number_stream(&PromotionGeneratorColumn::PPromoName);
        let p_promo_name = RandomValueGenerator::generate_word(
            row_number_i64,
            PROMO_NAME_LENGTH,
            crate::distribution::get_syllables_distribution(),
        );

        // DSB: exponential, permuted flags, read low bit first (it fixed
        // dsdgen's `<<=`).
        let flags = exp_int(
            1,
            512,
            self.abstract_row_generator
                .get_random_number_stream(&PromotionGeneratorColumn::PChannelDmail),
        );
        let flags = map_perm(flags, &self.perms.flags) - 1;
        let bit = |i: u32| (flags >> i) & 1 != 0;
        let p_channel_dmail = bit(0);
        let p_channel_email = bit(1);
        let p_channel_catalog = bit(2);
        let p_channel_tv = bit(3);
        let p_channel_radio = bit(4);
        let p_channel_press = bit(5);
        let p_channel_event = bit(6);
        let p_channel_demo = bit(7);
        let p_discount_active = bit(8);

        let p_channel_details = self.text.random_text(
            PROMO_DETAIL_LENGTH_MIN,
            PROMO_DETAIL_LENGTH_MAX,
            self.abstract_row_generator
                .get_random_number_stream(&PromotionGeneratorColumn::PChannelDetails),
        );

        let p_purpose = "Unknown".to_string();

        PromotionRow::new(
            null_bit_map,
            p_promo_sk,
            p_promo_id,
            p_start_date_id,
            p_end_date_id,
            p_item_sk,
            p_cost,
            p_response_target,
            p_promo_name,
            p_channel_dmail,
            p_channel_email,
            p_channel_catalog,
            p_channel_tv,
            p_channel_radio,
            p_channel_press,
            p_channel_event,
            p_channel_demo,
            p_channel_details,
            p_purpose,
            p_discount_active,
        )
    }
}

impl Iterator for PromotionRowGenerator {
    type Item = PromotionRow;

    fn next(&mut self) -> Option<PromotionRow> {
        let row_number = self.abstract_row_generator.next_row_number()?;
        let row = self.generate_promotion_row(row_number);
        self.abstract_row_generator.finish_row();
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::dat_values;

    #[test]
    fn test_generate_promotion_row() {
        use crate::config::Session;

        let mut generator = PromotionRowGenerator::new(Session::default(), 1);
        let row = generator.next().expect("row");
        assert_eq!(dat_values(&row).len(), 19);
    }
}
