// Modified from tpcdsgen 4.0.0 for the populate DSB variant.
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

//! Customer generator column definitions (CustomerGeneratorColumn)

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Customer generator column enum (CustomerGeneratorColumn)
///
/// Each variant contains the global column number and seeds per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CustomerGeneratorColumn {
    CCustomerSk,
    CCustomerId,
    CCurrentCdemoSk,
    CCurrentHdemoSk,
    CCurrentAddrSk,
    CFirstShiptoDateId,
    CFirstSalesDateId,
    CSalutation,
    CFirstName,
    CLastName,
    CPreferredCustFlag,
    CBirthDay,
    CBirthMonth,
    CBirthYear,
    CBirthCountry,
    CLogin,
    CEmailAddress,
    CLastReviewDate,
    CNulls,
}

impl CustomerGeneratorColumn {
    /// Get the global column number for this column
    fn global_column_number(&self) -> i32 {
        use CustomerGeneratorColumn::*;
        match self {
            CCustomerSk => 92,
            CCustomerId => 93,
            CCurrentCdemoSk => 94,
            CCurrentHdemoSk => 95,
            CCurrentAddrSk => 96,
            CFirstShiptoDateId => 97,
            CFirstSalesDateId => 98,
            CSalutation => 99,
            CFirstName => 100,
            CLastName => 101,
            CPreferredCustFlag => 102,
            CBirthDay => 103,
            CBirthMonth => 104,
            CBirthYear => 105,
            CBirthCountry => 106,
            CLogin => 107,
            CEmailAddress => 108,
            CLastReviewDate => 109,
            CNulls => 110,
        }
    }

    /// Get the seeds per row for this column
    fn seeds_per_row(&self) -> u32 {
        use CustomerGeneratorColumn::*;
        match self {
            CCustomerSk => 1,
            CCustomerId => 1,
            CCurrentCdemoSk => 1,
            CCurrentHdemoSk => 1,
            CCurrentAddrSk => 1,
            CFirstShiptoDateId => 0,
            CFirstSalesDateId => 1,
            CSalutation => 1,
            CFirstName => 1,
            CLastName => 1,
            CPreferredCustFlag => 2,
            CBirthDay => 1,
            CBirthMonth => 0,
            CBirthYear => 0,
            CBirthCountry => 1,
            CLogin => 1,
            CEmailAddress => 23,
            CLastReviewDate => 1,
            CNulls => 2,
        }
    }
}

impl GeneratorColumn for CustomerGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::Customer
    }

    fn get_global_column_number(&self) -> i32 {
        self.global_column_number()
    }

    fn get_seeds_per_row(&self) -> u32 {
        self.seeds_per_row()
    }

    fn values() -> &'static [Self] {
        use CustomerGeneratorColumn::*;
        static VALUES: &[CustomerGeneratorColumn] = &[
            CCustomerSk,
            CCustomerId,
            CCurrentCdemoSk,
            CCurrentHdemoSk,
            CCurrentAddrSk,
            CFirstShiptoDateId,
            CFirstSalesDateId,
            CSalutation,
            CFirstName,
            CLastName,
            CPreferredCustFlag,
            CBirthDay,
            CBirthMonth,
            CBirthYear,
            CBirthCountry,
            CLogin,
            CEmailAddress,
            CLastReviewDate,
            CNulls,
        ];
        VALUES
    }
}
