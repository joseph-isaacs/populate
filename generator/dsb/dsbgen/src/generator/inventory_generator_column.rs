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

//! Inventory generator column definitions

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Generator columns for the inventory table.
/// These map to InventoryGeneratorColumn.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InventoryGeneratorColumn {
    InvDateSk,
    InvItemSk,
    InvWarehouseSk,
    InvQuantityOnHand,
    InvNulls,
}

impl GeneratorColumn for InventoryGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::Inventory
    }

    fn get_global_column_number(&self) -> i32 {
        match self {
            InventoryGeneratorColumn::InvDateSk => 223,
            InventoryGeneratorColumn::InvItemSk => 224,
            InventoryGeneratorColumn::InvWarehouseSk => 225,
            InventoryGeneratorColumn::InvQuantityOnHand => 226,
            InventoryGeneratorColumn::InvNulls => 227,
        }
    }

    fn get_seeds_per_row(&self) -> u32 {
        match self {
            InventoryGeneratorColumn::InvDateSk => 1,
            InventoryGeneratorColumn::InvItemSk => 1,
            InventoryGeneratorColumn::InvWarehouseSk => 1,
            InventoryGeneratorColumn::InvQuantityOnHand => 1,
            InventoryGeneratorColumn::InvNulls => 2,
        }
    }

    fn values() -> &'static [Self] {
        use InventoryGeneratorColumn::*;
        static VARIANTS: [InventoryGeneratorColumn; 5] = [
            InvDateSk,
            InvItemSk,
            InvWarehouseSk,
            InvQuantityOnHand,
            InvNulls,
        ];
        &VARIANTS
    }
}
