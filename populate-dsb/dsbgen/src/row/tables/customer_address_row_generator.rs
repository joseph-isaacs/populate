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

//! Customer address row generator (CustomerAddressRowGenerator)

use crate::business_key_generator::make_business_key;
use crate::config::Session;
use crate::distribution::location_types_distribution::LocationTypesDistribution;
use crate::dsb::address::make_address;
use crate::dsb::{exp_pick_index, make_perm};
use crate::generator::CustomerAddressGeneratorColumn;
use crate::nulls::create_null_bit_map;
use crate::row::{AbstractRowGenerator, CustomerAddressRow};
use crate::table::Table;

/// Row generator for the CUSTOMER_ADDRESS table (CustomerAddressRowGenerator)
pub struct CustomerAddressRowGenerator {
    abstract_generator: AbstractRowGenerator<CustomerAddressGeneratorColumn>,
    session: Session,
    /// `pLocationTypePermutation`, drawn from `CA_LOCATION_TYPE` at row 1.
    location_type_perm: Vec<i32>,
    /// `g_address_county_id` of the last row (1-based fips_county index).
    last_county_id: Option<i32>,
}

impl CustomerAddressRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        let mut abstract_generator = AbstractRowGenerator::new(row_count);
        let n = LocationTypesDistribution::values().len() as i32;
        let location_type_perm = make_perm(
            n,
            abstract_generator
                .get_random_number_stream(&CustomerAddressGeneratorColumn::CaLocationType),
        );
        abstract_generator.mark_row1();
        Self {
            abstract_generator,
            session,
            location_type_perm,
            last_county_id: None,
        }
    }

    /// Generate a CustomerAddressRow with realistic data following Java implementation
    fn generate_customer_address_row(&mut self, row_number: u64) -> CustomerAddressRow {
        let session = &self.session;
        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        // Create null bit map (createNullBitMap call)
        let nulls_stream = self
            .abstract_generator
            .get_random_number_stream(&CustomerAddressGeneratorColumn::CaNulls);
        let null_bit_map = create_null_bit_map(Table::CustomerAddress, nulls_stream);

        let ca_addr_sk = row_number_i64;
        let ca_addr_id = make_business_key(row_number);

        // DSB: an exponential, permuted location type, then `mk_address`.
        let location_type_stream = self
            .abstract_generator
            .get_random_number_stream(&CustomerAddressGeneratorColumn::CaLocationType);
        let values = LocationTypesDistribution::values();
        let ca_location_type = values[exp_pick_index(
            values.len() as i32,
            Some(&self.location_type_perm),
            location_type_stream,
        )]
        .clone();

        let address_stream = self
            .abstract_generator
            .get_random_number_stream(&CustomerAddressGeneratorColumn::CaAddress);
        let (ca_address, county_id) =
            make_address(Table::CustomerAddress, address_stream, session.get_scaling());
        self.last_county_id = county_id;

        CustomerAddressRow::new(
            null_bit_map,
            ca_addr_sk,
            ca_addr_id,
            ca_address,
            ca_location_type,
        )
    }

    /// 1-based `fips_county` index of the last row's address (DSB's
    /// `g_address_county_id`).
    pub fn last_county_id(&self) -> Option<i32> {
        self.last_county_id
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            first.next();
            self.abstract_generator.set_row2(first.abstract_generator.seeds());
        }
    }

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .skip_rows_until_starting_row_number(starting_row_number);
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .set_source_row_range(starting_row_number, ending_row_number);
    }
}

impl Iterator for CustomerAddressRowGenerator {
    type Item = CustomerAddressRow;

    fn next(&mut self) -> Option<CustomerAddressRow> {
        let row_number = self.abstract_generator.next_row_number()?;
        let row = self.generate_customer_address_row(row_number);
        self.abstract_generator.finish_row();
        Some(row)
    }
}
