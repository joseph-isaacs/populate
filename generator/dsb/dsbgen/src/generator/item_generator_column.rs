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

//! Item generator column definitions

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Generator columns for item table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemGeneratorColumn {
    IItemSk,
    IItemId,
    IRecStartDateId,
    IRecEndDateId,
    IItemDesc,
    ICurrentPrice,
    IWholesaleCost,
    IBrandId,
    IBrand,
    IClassId,
    IClass,
    ICategoryId,
    ICategory,
    IManufactId,
    IManufact,
    ISize,
    IFormulation,
    IColor,
    IUnits,
    IContainer,
    IManagerId,
    IProductName,
    INulls,
    IScd,
    IPromoSk,
}

impl GeneratorColumn for ItemGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::Item
    }

    fn get_global_column_number(&self) -> i32 {
        use ItemGeneratorColumn::*;
        match self {
            IItemSk => 35,
            IItemId => 36,
            IRecStartDateId => 37,
            IRecEndDateId => 38,
            IItemDesc => 39,
            ICurrentPrice => 40,
            IWholesaleCost => 41,
            IBrandId => 42,
            IBrand => 43,
            IClassId => 44,
            IClass => 45,
            ICategoryId => 46,
            ICategory => 47,
            IManufactId => 48,
            IManufact => 49,
            ISize => 50,
            IFormulation => 51,
            IColor => 52,
            IUnits => 53,
            IContainer => 54,
            IManagerId => 55,
            IProductName => 56,
            INulls => 57,
            IScd => 58,
            IPromoSk => 59,
        }
    }

    fn get_seeds_per_row(&self) -> u32 {
        use ItemGeneratorColumn::*;
        match self {
            IItemSk => 1,
            IItemId => 1,
            IRecStartDateId => 1,
            IRecEndDateId => 2,
            IItemDesc => 200,
            ICurrentPrice => 2,
            IWholesaleCost => 1,
            IBrandId => 1,
            IBrand => 1,
            IClassId => 1,
            IClass => 1,
            ICategoryId => 1,
            ICategory => 1,
            IManufactId => 2,
            IManufact => 1,
            ISize => 1,
            IFormulation => 50,
            IColor => 1,
            IUnits => 1,
            IContainer => 1,
            IManagerId => 2,
            IProductName => 1,
            INulls => 2,
            IScd => 1,
            IPromoSk => 2,
        }
    }

    fn values() -> &'static [Self] {
        use ItemGeneratorColumn::*;
        &[
            IItemSk,
            IItemId,
            IRecStartDateId,
            IRecEndDateId,
            IItemDesc,
            ICurrentPrice,
            IWholesaleCost,
            IBrandId,
            IBrand,
            IClassId,
            IClass,
            ICategoryId,
            ICategory,
            IManufactId,
            IManufact,
            ISize,
            IFormulation,
            IColor,
            IUnits,
            IContainer,
            IManagerId,
            IProductName,
            INulls,
            IScd,
            IPromoSk,
        ]
    }
}
