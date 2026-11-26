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

//! Customer address generator column definitions (CustomerAddressGeneratorColumn)

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Customer address generator column enum (CustomerAddressGeneratorColumn)
///
/// Maps to Java CustomerAddressGeneratorColumn with global column numbers and seeds per row
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CustomerAddressGeneratorColumn {
    CaAddressSk,
    CaAddressId,
    CaAddressStreetNum,
    CaAddressStreetName,
    CaAddressStreetType,
    CaAddressSuiteNum,
    CaAddressCity,
    CaAddressCounty,
    CaAddressState,
    CaAddressZip,
    CaAddressCountry,
    CaAddressGmtOffset,
    CaLocationType,
    CaNulls,
    CaAddress,
    CaAddressStreetName2,
}

impl GeneratorColumn for CustomerAddressGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::CustomerAddress
    }

    fn get_global_column_number(&self) -> i32 {
        match self {
            CustomerAddressGeneratorColumn::CaAddressSk => 60,
            CustomerAddressGeneratorColumn::CaAddressId => 61,
            CustomerAddressGeneratorColumn::CaAddressStreetNum => 62,
            CustomerAddressGeneratorColumn::CaAddressStreetName => 63,
            CustomerAddressGeneratorColumn::CaAddressStreetType => 64,
            CustomerAddressGeneratorColumn::CaAddressSuiteNum => 65,
            CustomerAddressGeneratorColumn::CaAddressCity => 66,
            CustomerAddressGeneratorColumn::CaAddressCounty => 67,
            CustomerAddressGeneratorColumn::CaAddressState => 68,
            CustomerAddressGeneratorColumn::CaAddressZip => 69,
            CustomerAddressGeneratorColumn::CaAddressCountry => 70,
            CustomerAddressGeneratorColumn::CaAddressGmtOffset => 71,
            CustomerAddressGeneratorColumn::CaLocationType => 72,
            CustomerAddressGeneratorColumn::CaNulls => 73,
            CustomerAddressGeneratorColumn::CaAddress => 74,
            CustomerAddressGeneratorColumn::CaAddressStreetName2 => 75,
        }
    }

    fn get_seeds_per_row(&self) -> u32 {
        match self {
            CustomerAddressGeneratorColumn::CaAddressSk => 1,
            CustomerAddressGeneratorColumn::CaAddressId => 1,
            CustomerAddressGeneratorColumn::CaAddressStreetNum => 1,
            CustomerAddressGeneratorColumn::CaAddressStreetName => 1,
            CustomerAddressGeneratorColumn::CaAddressStreetType => 1,
            CustomerAddressGeneratorColumn::CaAddressSuiteNum => 1,
            CustomerAddressGeneratorColumn::CaAddressCity => 1,
            CustomerAddressGeneratorColumn::CaAddressCounty => 1,
            CustomerAddressGeneratorColumn::CaAddressState => 1,
            CustomerAddressGeneratorColumn::CaAddressZip => 1,
            CustomerAddressGeneratorColumn::CaAddressCountry => 1,
            CustomerAddressGeneratorColumn::CaAddressGmtOffset => 1,
            CustomerAddressGeneratorColumn::CaLocationType => 1,
            CustomerAddressGeneratorColumn::CaNulls => 2,
            CustomerAddressGeneratorColumn::CaAddress => 7,
            CustomerAddressGeneratorColumn::CaAddressStreetName2 => 1,
        }
    }

    fn values() -> &'static [Self] {
        use CustomerAddressGeneratorColumn::*;
        static VALUES: [CustomerAddressGeneratorColumn; 16] = [
            CaAddressSk,
            CaAddressId,
            CaAddressStreetNum,
            CaAddressStreetName,
            CaAddressStreetType,
            CaAddressSuiteNum,
            CaAddressCity,
            CaAddressCounty,
            CaAddressState,
            CaAddressZip,
            CaAddressCountry,
            CaAddressGmtOffset,
            CaLocationType,
            CaNulls,
            CaAddress,
            CaAddressStreetName2,
        ];
        &VALUES
    }
}
