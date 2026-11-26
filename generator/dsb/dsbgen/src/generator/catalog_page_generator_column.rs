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

//! Catalog page generator column definitions

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Enum representing all generator columns for the catalog_page table
/// Global column numbers 35-45
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogPageGeneratorColumn {
    CpCatalogPageSk,     // 35
    CpCatalogPageId,     // 36
    CpStartDateId,       // 37
    CpEndDateId,         // 38
    CpPromoId,           // 39 (unused)
    CpDepartment,        // 40
    CpCatalogNumber,     // 41
    CpCatalogPageNumber, // 42
    CpDescription,       // 43
    CpType,              // 44
    CpNulls,             // 45
}

impl GeneratorColumn for CatalogPageGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::CatalogPage
    }

    fn get_global_column_number(&self) -> i32 {
        match self {
            CatalogPageGeneratorColumn::CpCatalogPageSk => 111,
            CatalogPageGeneratorColumn::CpCatalogPageId => 112,
            CatalogPageGeneratorColumn::CpStartDateId => 113,
            CatalogPageGeneratorColumn::CpEndDateId => 114,
            CatalogPageGeneratorColumn::CpPromoId => 115,
            CatalogPageGeneratorColumn::CpDepartment => 116,
            CatalogPageGeneratorColumn::CpCatalogNumber => 117,
            CatalogPageGeneratorColumn::CpCatalogPageNumber => 118,
            CatalogPageGeneratorColumn::CpDescription => 119,
            CatalogPageGeneratorColumn::CpType => 120,
            CatalogPageGeneratorColumn::CpNulls => 121,
        }
    }

    fn get_seeds_per_row(&self) -> u32 {
        match self {
            CatalogPageGeneratorColumn::CpDescription => 100, // S_CP_DESCRIPTION
            CatalogPageGeneratorColumn::CpNulls => 2,
            _ => 1,
        }
    }

    fn values() -> &'static [Self] {
        use CatalogPageGeneratorColumn::*;
        &[
            CpCatalogPageSk,
            CpCatalogPageId,
            CpStartDateId,
            CpEndDateId,
            CpPromoId,
            CpDepartment,
            CpCatalogNumber,
            CpCatalogPageNumber,
            CpDescription,
            CpType,
            CpNulls,
        ]
    }
}
