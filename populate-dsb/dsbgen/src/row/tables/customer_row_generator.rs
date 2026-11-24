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

//! Customer row generator (CustomerRowGenerator)

use crate::business_key_generator::make_business_key;
use crate::config::Session;
use crate::distribution::address_distributions::countries_distribution;
use crate::distribution::{FirstNamesWeights, NamesDistributions, SalutationsWeights};
use crate::dsb::attributes::{attribute_map, one_sided_correlated_key, AttributeKeyMap, MapName};
use crate::dsb::join::{join_key, DemographicsJoin};
use crate::dsb::{exp_date, exp_pick_index, make_key_perm, make_perm, DatePerms};
use crate::generator::{CustomerGeneratorColumn, GeneratorColumn};
use crate::nulls::create_null_bit_map;
use crate::random::{RandomNumberStream, RandomValueGenerator};
use crate::row::customer_row::CustomerRow;
use crate::row::AbstractRowGenerator;
use crate::table::Table;
use crate::types::Date;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// Rows between [`CustomerRowGenerator::birth_day_stream`]'s checkpoints.
const BIRTH_CHECKPOINT: u64 = 1 << 16;

/// `C_BIRTH_DAY`'s seed after every [`BIRTH_CHECKPOINT`] rows, by scale.
static BIRTH_SEEDS: OnceLock<Mutex<HashMap<u64, Vec<i64>>>> = OnceLock::new();

/// Customer row generator (CustomerRowGenerator)
pub struct CustomerRowGenerator {
    abstract_generator: AbstractRowGenerator<CustomerGeneratorColumn>,
    session: Session,
    perms: Arc<Perms>,
}

/// What DSB's `mk_w_customer` sets up on its first call: permutations and
/// the attribute key maps of customer_address and the demographics.
struct Perms {
    country: Vec<i32>,
    birth: DatePerms,
    address_state: Arc<AttributeKeyMap>,
    cdemo: Arc<AttributeKeyMap>,
    cdemo_perm: Vec<i64>,
    hdemo: Arc<AttributeKeyMap>,
    hdemo_perm: Vec<i64>,
}

fn birth_min() -> Date {
    Date::new(1924, 1, 1)
}

fn birth_max() -> Date {
    Date::new(1992, 12, 31)
}

impl CustomerRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use CustomerGeneratorColumn::*;
        let mut ag = AbstractRowGenerator::new(row_count);
        let country = make_perm(
            countries_distribution().get_size() as i32,
            ag.get_random_number_stream(&CBirthCountry),
        );
        let s = ag.get_random_number_stream(&CBirthDay);
        let birth = DatePerms {
            year: make_perm(birth_max().get_year() - birth_min().get_year() + 1, s),
            month: make_perm(12, s),
            day: make_perm(31, s),
        };
        let address_state = attribute_map(MapName::CustomerAddressState, &session);
        let cdemo = attribute_map(MapName::CustomerDemographics, &session);
        let cdemo_perm = make_key_perm(
            cdemo.attribute_count() as i64,
            ag.get_random_number_stream(&CCurrentCdemoSk),
        );
        let hdemo = attribute_map(MapName::HouseholdDemographics, &session);
        let s = ag.get_random_number_stream(&CCurrentHdemoSk);
        let hdemo_perm = make_key_perm(hdemo.attribute_count() as i64, s);
        // Row 1's hdemo join draws the join's permutations.
        let draws = DemographicsJoin::household().draws;
        s.advance(draws);
        s.set_seeds_used(s.get_seeds_used() + draws as u32);
        // An hdemo join (four coordinates) plus a correlated key (two
        // draws); the correlated cdemo key alone.
        ag.set_seeds_per_row(&CCurrentHdemoSk, 6);
        ag.set_seeds_per_row(&CCurrentCdemoSk, 2);
        ag.mark_row1();
        CustomerRowGenerator {
            abstract_generator: ag,
            session,
            perms: Arc::new(Perms {
                country,
                birth,
                address_state,
                cdemo,
                cdemo_perm,
                hdemo,
                hdemo_perm,
            }),
        }
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            first.next();
            self.abstract_generator
                .set_row2(first.abstract_generator.seeds());
        }
    }

    /// `C_BIRTH_DAY` at the start of `row`. Its rejection-sampled date takes
    /// three or more draws a row, always over its budget of one, so the
    /// stream is never burned and its position depends on every earlier
    /// row's dates: replay them.
    fn birth_day_stream(&self, row: u64) -> RandomNumberStream {
        let c = CustomerGeneratorColumn::CBirthDay;
        let mut s = RandomNumberStream::new_with_column(
            c.get_global_column_number(),
            c.get_seeds_per_row(),
        );
        let p = &self.perms.birth;
        s.advance((p.year.len() + p.month.len() + p.day.len()) as u64);
        // The seed every BIRTH_CHECKPOINT rows, shared by all generators at
        // this scale and extended as later chunks need it, so a chunk
        // start replays at most that many rows.
        let done = row - 1;
        let k = (done / BIRTH_CHECKPOINT) as usize;
        let scale = self.session.get_scaling().get_scale().to_bits();
        let cache = BIRTH_SEEDS.get_or_init(Default::default);
        let seed = {
            let mut cache = cache.lock().expect("birth day checkpoints");
            let seeds = cache.entry(scale).or_insert_with(|| vec![s.seed()]);
            while seeds.len() <= k {
                s.set_seed(*seeds.last().expect("a checkpoint"));
                for _ in 0..BIRTH_CHECKPOINT {
                    exp_date(birth_min(), birth_max(), p, &mut s);
                }
                seeds.push(s.seed());
            }
            seeds[k]
        };
        s.set_seed(seed);
        for _ in k as u64 * BIRTH_CHECKPOINT..done {
            exp_date(birth_min(), birth_max(), p, &mut s);
        }
        s.set_seeds_used(0);
        s
    }

    fn position_birth_day(&mut self, row: u64) {
        if row > 1 {
            let s = self.birth_day_stream(row);
            *self
                .abstract_generator
                .get_random_number_stream(&CustomerGeneratorColumn::CBirthDay) = s;
        }
    }

    fn generate_customer_row(&mut self, row_number: u64) -> CustomerRow {
        use CustomerGeneratorColumn::*;

        let session = &self.session;
        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let c_customer_sk = row_number_i64;
        let c_customer_id = make_business_key(row_number);

        // Preferred customer flag - MUST BE FIRST (matches Java order line 72)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CPreferredCustFlag);
        let random_int = RandomValueGenerator::generate_uniform_random_int(1, 100, stream);
        let c_preferred_percent = 50;
        let c_preferred_cust_flag = random_int < c_preferred_percent;

        let scaling = session.get_scaling();

        // DSB: the hdemo join's draws (its key is replaced below), then an
        // exponential address.
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CCurrentHdemoSk);
        DemographicsJoin::household().key(stream);

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CCurrentAddrSk);
        let c_current_addr_sk =
            join_key(crate::config::Table::CustomerAddress, stream, scaling, None);

        // Name generation (matches Java order lines 81-85)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CFirstName);
        let name_index =
            NamesDistributions::pick_random_index(FirstNamesWeights::GeneralFrequency, stream);
        let c_first_name = NamesDistributions::get_first_name_from_index(name_index).to_string();

        let stream = self.abstract_generator.get_random_number_stream(&CLastName);
        let c_last_name = NamesDistributions::pick_random_last_name(stream).to_string();

        // Salutation based on gender frequency
        let female_name_weight = NamesDistributions::get_weight_for_index(
            name_index,
            FirstNamesWeights::FemaleFrequency,
        );
        let salutation_weight = if female_name_weight == 0 {
            SalutationsWeights::Male
        } else {
            SalutationsWeights::Female
        };
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CSalutation);
        let c_salutation =
            NamesDistributions::pick_random_salutation(salutation_weight, stream).to_string();

        let one_year_ago = Date::from_julian_days(Date::JULIAN_TODAYS_DATE - 365);
        let ten_years_ago = Date::from_julian_days(Date::JULIAN_TODAYS_DATE - 3650);
        let today = Date::from_julian_days(Date::JULIAN_TODAYS_DATE);

        let stream = self.abstract_generator.get_random_number_stream(&CBirthDay);
        let birthday = exp_date(birth_min(), birth_max(), &self.perms.birth, stream);
        let c_birth_day = birthday.day();
        let c_birth_month = birthday.month();
        let c_birth_year = birthday.year();

        // Email generation (matches Java order line 97)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CEmailAddress);
        let c_email_address =
            RandomValueGenerator::generate_random_email(&c_first_name, &c_last_name, stream);

        // Last review date (matches Java order lines 98-99)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CLastReviewDate);
        let last_review_date =
            RandomValueGenerator::generate_uniform_random_date(one_year_ago, today, stream);
        let c_last_review_date = last_review_date.to_julian_days();

        // First sales date (matches Java order lines 100-102)
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CFirstSalesDateId);
        let first_sales_date =
            RandomValueGenerator::generate_uniform_random_date(ten_years_ago, today, stream);
        let c_first_sales_date_id = first_sales_date.to_julian_days();
        let c_first_shipto_date_id = c_first_sales_date_id + 30;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&CBirthCountry);
        let countries = countries_distribution();
        let c_birth_country = countries
            .get_value_at_index(
                0,
                exp_pick_index(
                    countries.get_size() as i32,
                    Some(&self.perms.country),
                    stream,
                ),
            )
            .to_string();

        // Demographics correlated with the address's state. DSB passes the
        // drive arguments swapped: the state id as the count, the state
        // count as the id.
        let p = self.perms.clone();
        let state = p.address_state.attribute_of(c_current_addr_sk);
        let states = p.address_state.attribute_count();
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CCurrentCdemoSk);
        let c_current_cdemo_sk =
            one_sided_correlated_key(state, states, &p.cdemo, &p.cdemo_perm, 0.01, stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&CCurrentHdemoSk);
        let c_current_hdemo_sk =
            one_sided_correlated_key(state, states, &p.hdemo, &p.hdemo_perm, 0.1, stream);

        // Generate null bit map (matches Java order line 123)
        let stream = self.abstract_generator.get_random_number_stream(&CNulls);
        let null_bit_map = create_null_bit_map(Table::Customer, stream);

        CustomerRow::new(
            c_customer_sk,
            c_customer_id,
            c_current_cdemo_sk,
            c_current_hdemo_sk,
            c_current_addr_sk,
            c_first_shipto_date_id,
            c_first_sales_date_id,
            c_salutation,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_day,
            c_birth_month,
            c_birth_year,
            c_birth_country,
            c_email_address,
            c_last_review_date,
            null_bit_map,
        )
    }

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .skip_rows_until_starting_row_number(starting_row_number);
        self.position_birth_day(starting_row_number);
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .set_source_row_range(starting_row_number, ending_row_number);
        self.position_birth_day(starting_row_number);
    }
}

impl Iterator for CustomerRowGenerator {
    type Item = CustomerRow;

    fn next(&mut self) -> Option<CustomerRow> {
        let row_number = self.abstract_generator.next_row_number()?;
        let row = self.generate_customer_row(row_number);
        self.abstract_generator.finish_row();
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Chunks starting either side of a birth day checkpoint give the
    /// sequential rows.
    #[test]
    fn chunks_across_birth_day_checkpoints() {
        let session = Session::default();
        let n = session.get_scaling().get_row_count(Table::Customer);
        let rows = |first: u64, last: u64| -> Vec<String> {
            let mut g = CustomerRowGenerator::new(session.clone(), n);
            g.set_source_row_range(first, last);
            g.map(|r| r.to_string()).collect()
        };
        let whole = rows(65_000, 70_000);
        let mut chunked = rows(65_000, 65_536);
        chunked.extend(rows(65_537, 65_537));
        chunked.extend(rows(65_538, 70_000));
        assert_eq!(whole, chunked);
    }
}
