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

//! Item row generator (Slowly Changing Dimension)

use crate::config::Session;
use crate::distribution::items_distributions::{
    category_class_at, category_class_count, category_count, colors_distribution,
    manager_id_item_count, manufact_id_item_count, units_distribution, CategoryClass,
};
use crate::distribution::{
    get_brand_syllables_distribution, get_category_at_index, get_has_size_at_index,
    pick_random_color, pick_random_current_price_range, pick_random_manufact_id_range,
    pick_random_size, ColorsWeights, IdWeights, SizeWeights,
};
use crate::dsb::{
    correlated_attribute, exp_int, exp_key, exp_pick_index, make_key_perm, make_perm, map_perm,
};
use crate::generator::ItemGeneratorColumn;
use crate::nulls::create_null_bit_map;
use crate::random::{RandomTextGenerator, RandomValueGenerator};
use crate::row::item_row::ItemRow;
use crate::row::AbstractRowGenerator;
use crate::slowly_changing_dimension_utils::{
    compute_scd_key, get_value_for_slowly_changing_dimension, ScdRowGenerator,
};
use crate::table::Table;
use crate::types::Decimal;

fn min_item_markdown_pct() -> Decimal {
    Decimal::new(30, 2)
}

fn max_item_markdown_pct() -> Decimal {
    Decimal::new(90, 2)
}
const ROW_SIZE_I_PRODUCT_NAME: i32 = 50;
const ROW_SIZE_I_ITEM_DESC: i32 = 200;
const ROW_SIZE_I_MANUFACT: i32 = 50;
const ROW_SIZE_I_FORMULATION: i32 = 20;
#[allow(dead_code)]
const I_PROMO_PERCENTAGE: i32 = 20;

/// DSB's permutations for `item` (made on its first row).
struct Perms {
    category: Vec<i32>,
    manager_count: i32,
    manager: Vec<i64>,
    manufact: Vec<i32>,
    colors: Vec<i32>,
    units: Vec<i32>,
}

pub struct ItemRowGenerator {
    abstract_generator: AbstractRowGenerator<ItemGeneratorColumn>,
    perms: Perms,
    last_color_id: i32,
    text: RandomTextGenerator,
    previous_row: Option<ItemRow>,
    session: Session,
}

impl ItemRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use ItemGeneratorColumn::*;
        let mut ag = AbstractRowGenerator::new(row_count);
        let manager_count = manager_id_item_count();
        let manager = make_key_perm(
            manager_count as i64,
            ag.get_random_number_stream(&IManagerId),
        );
        let manufact = make_perm(
            manufact_id_item_count(),
            ag.get_random_number_stream(&IManufactId),
        );
        let colors = make_perm(
            colors_distribution().get_size() as i32,
            ag.get_random_number_stream(&IColor),
        );
        // DSB draws the units permutation from I_COLOR's stream.
        let units = make_perm(
            units_distribution().get_size() as i32,
            ag.get_random_number_stream(&IColor),
        );
        // hierarchy_item's first call (row 1's category).
        let category = make_perm(
            category_count() as i32,
            ag.get_random_number_stream(&ICategory),
        );
        ag.mark_row1();
        ItemRowGenerator {
            abstract_generator: ag,
            perms: Perms {
                category,
                manager_count,
                manager,
                manufact,
                colors,
                units,
            },
            last_color_id: 0,
            text: RandomTextGenerator::new(),
            previous_row: None,
            session,
        }
    }

    /// DSB's `hierarchy_item` for row `row_number`: the category index,
    /// the class (new value), and the brand name and id. Reads the
    /// `I_CATEGORY` and `I_CLASS` streams.
    fn hierarchy(&mut self, row_number: u64) -> (usize, CategoryClass, String, i64) {
        use ItemGeneratorColumn::*;
        let stream = self.abstract_generator.get_random_number_stream(&ICategory);
        let category_index =
            exp_pick_index(category_count() as i32, Some(&self.perms.category), stream);
        let stream = self.abstract_generator.get_random_number_stream(&IClass);
        let class_index = exp_pick_index(category_class_count(category_index) as i32, None, stream);
        let class = category_class_at(category_index, class_index);
        let category_id = (category_index + 1) as i64;
        let brand_base = (row_number as i64) % class.get_brand_count() as i64 + 1;
        let brand = format!(
            "{} #{}",
            RandomValueGenerator::generate_word(
                category_id * 10 + class.get_id(),
                45,
                get_brand_syllables_distribution(),
            ),
            brand_base
        );
        let brand_id = brand_base + (category_id * 1000 + class.get_id()) * 1000;
        (category_index, class, brand, brand_id)
    }

    fn generate_item_row(&mut self, row_number: u64) -> ItemRow {
        use ItemGeneratorColumn::*;

        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        let stream = self.abstract_generator.get_random_number_stream(&INulls);
        let null_bit_map = create_null_bit_map(Table::Item, stream);

        let i_item_sk = row_number_i64;

        // DSB draws the hierarchy first, then decides class and brand from
        // stale flags with bFirstRecord = 0.
        let (category_index, class, i_brand, new_brand_id) = self.hierarchy(row_number);
        let i_category_id = (category_index + 1) as i64;
        let i_category = get_category_at_index(category_index).to_string();
        let i_class = class.get_name().to_string();
        // DSB moved `hierarchy_item` above the line that sets
        // `nFieldChangeFlags`, so class and brand go through `changeSCD`
        // with an uninitialised flag word and bFirstRecord = 0. For every
        // row after the first that stack slot holds 0 (both new). Before
        // row 1 it holds whatever the stack had, which changes with address
        // space randomisation: dsdgen's first item row has a different
        // brand and class id from run to run. With randomisation off
        // (`setarch -R`, or under gdb) the slot is odd then even: the class
        // id is kept from the zeroed statics and the brand is new. That is
        // the run we match.
        let i_class_id = if row_number == 1 { 0 } else { class.get_id() };
        let i_brand_id = new_brand_id;

        let has_size = get_has_size_at_index(category_index);
        let stream = self.abstract_generator.get_random_number_stream(&ISize);
        let i_size = pick_random_size(
            if has_size == 0 {
                SizeWeights::NoSize
            } else {
                SizeWeights::Sized
            },
            stream,
        );

        let stream = self
            .abstract_generator
            .get_random_number_stream(&IManagerId);
        let i_manager_id = correlated_attribute(
            category_count() as i32,
            i_category_id,
            self.perms.manager_count,
            &self.perms.manager,
            1.0,
            stream,
        );

        let scd_key = compute_scd_key(Table::Item, row_number);
        let i_item_id = scd_key.get_business_key().to_string();
        let i_rec_start_date_id = scd_key.get_start_date();
        let i_rec_end_date_id = scd_key.get_end_date();
        let is_new_business_key = scd_key.is_new_business_key();

        let stream = self.abstract_generator.get_random_number_stream(&IScd);
        let mut field_change_flags = stream.next_random() as i32;

        let stream = self.abstract_generator.get_random_number_stream(&IItemDesc);
        let mut i_item_desc = self.text.random_text(1, ROW_SIZE_I_ITEM_DESC, stream);
        if let Some(ref prev_row) = self.previous_row {
            i_item_desc = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_business_key,
                prev_row.get_i_item_desc().to_string(),
                i_item_desc,
            );
        }
        field_change_flags >>= 1;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&ICurrentPrice);
        let (price_min, price_max) = pick_random_current_price_range(stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&ICurrentPrice);
        let i_current_price =
            RandomValueGenerator::generate_uniform_random_decimal(price_min, price_max, stream);
        field_change_flags >>= 1;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&IWholesaleCost);
        let markdown = RandomValueGenerator::generate_uniform_random_decimal(
            min_item_markdown_pct(),
            max_item_markdown_pct(),
            stream,
        );
        let mut i_wholesale_cost = Decimal::multiply(i_current_price, markdown);
        if let Some(ref prev_row) = self.previous_row {
            i_wholesale_cost = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_business_key,
                prev_row.get_i_wholesale_cost(),
                i_wholesale_cost,
            );
        }
        field_change_flags >>= 1;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&IManufactId);
        let (manufact_min, manufact_max) =
            pick_random_manufact_id_range(IdWeights::Unified, stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&IManufactId);
        let mut i_manufact_id = map_perm(
            exp_int(manufact_min, manufact_max, stream),
            &self.perms.manufact,
        ) as i64;
        if let Some(ref prev_row) = self.previous_row {
            i_manufact_id = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_business_key,
                prev_row.get_i_manufact_id(),
                i_manufact_id,
            );
        }
        field_change_flags >>= 1;

        let mut i_manufact = RandomValueGenerator::generate_word(
            i_manufact_id,
            ROW_SIZE_I_MANUFACT,
            crate::distribution::get_syllables_distribution(),
        );
        if let Some(ref prev_row) = self.previous_row {
            i_manufact = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_business_key,
                prev_row.get_i_manufact().to_string(),
                i_manufact,
            );
        }
        field_change_flags >>= 1;

        let stream = self
            .abstract_generator
            .get_random_number_stream(&IFormulation);
        let mut i_formulation = RandomValueGenerator::generate_random_charset(
            RandomValueGenerator::DIGITS,
            ROW_SIZE_I_FORMULATION,
            ROW_SIZE_I_FORMULATION,
            stream,
        );
        let stream = self
            .abstract_generator
            .get_random_number_stream(&IFormulation);
        let color = pick_random_color(ColorsWeights::Skewed, stream);
        let stream = self
            .abstract_generator
            .get_random_number_stream(&IFormulation);
        let position = RandomValueGenerator::generate_uniform_random_int(
            0,
            i_formulation.len() as i32 - color.len() as i32 - 1,
            stream,
        ) as usize;
        let mut formulation_chars: Vec<char> = i_formulation.chars().collect();
        for (i, c) in color.chars().enumerate() {
            if position + i < formulation_chars.len() {
                formulation_chars[position + i] = c;
            }
        }
        i_formulation = formulation_chars.into_iter().collect();
        if let Some(ref prev_row) = self.previous_row {
            i_formulation = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_business_key,
                prev_row.get_i_formulation().to_string(),
                i_formulation,
            );
        }

        let colors = colors_distribution();
        let stream = self.abstract_generator.get_random_number_stream(&IColor);
        let color_index =
            exp_pick_index(colors.get_size() as i32, Some(&self.perms.colors), stream);
        let i_color = colors.get_value_at_index(0, color_index).to_string();
        self.last_color_id = color_index as i32 + 1;

        let units = units_distribution();
        let stream = self.abstract_generator.get_random_number_stream(&IUnits);
        let i_units = units
            .get_value_at_index(
                0,
                exp_pick_index(units.get_size() as i32, Some(&self.perms.units), stream),
            )
            .to_string();

        let i_container = "Unknown".to_string();

        let i_product_name = RandomValueGenerator::generate_word(
            row_number_i64,
            ROW_SIZE_I_PRODUCT_NAME,
            crate::distribution::get_syllables_distribution(),
        );

        // i_promo_sk (not written): DSB's skewed join, then the 20% check.
        let promotions = self
            .session
            .get_scaling()
            .get_row_count(crate::config::Table::Promotion) as i64;
        let stream = self.abstract_generator.get_random_number_stream(&IPromoSk);
        let _i_promo_sk = exp_key(1, promotions, stream);
        let _temp = RandomValueGenerator::generate_uniform_random_int(1, 100, stream);

        let row = ItemRow::new(
            null_bit_map,
            i_item_sk,
            i_item_id,
            i_rec_start_date_id,
            i_rec_end_date_id,
            i_item_desc,
            i_current_price,
            i_wholesale_cost,
            i_brand_id,
            i_brand,
            i_class_id,
            i_class,
            i_category_id,
            i_category,
            i_manufact_id,
            i_manufact,
            i_size,
            i_formulation,
            i_color,
            i_units,
            i_container,
            i_manager_id,
            i_product_name,
        );

        self.previous_row = Some(row.clone());
        row
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            first.next();
            self.abstract_generator
                .set_row2(first.abstract_generator.seeds());
        }
    }

    /// 1-based `colors` id of the last row generated (DSB's `item_color`
    /// attribute).
    pub fn last_color_id(&self) -> i32 {
        self.last_color_id
    }

    /// Start generating at `starting_row_number` (1-based), fast forwarding
    /// the random number streams to that row.
    pub fn skip_rows_until_starting_row_number(&mut self, starting_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .skip_rows_until_starting_row_number(starting_row_number);
        self.previous_row = None;
    }

    /// Restrict generation to source rows
    /// `starting_row_number..=ending_row_number` (1-based, inclusive).
    ///
    /// The ending row number is clamped to the table's row count.
    pub fn set_source_row_range(&mut self, starting_row_number: u64, ending_row_number: u64) {
        self.ensure_row2(starting_row_number);
        self.abstract_generator
            .set_source_row_range(starting_row_number, ending_row_number);
        self.previous_row = None;
    }
}

impl ScdRowGenerator for ItemRowGenerator {
    type Column = ItemGeneratorColumn;
    type Row = ItemRow;

    fn abstract_generator(&mut self) -> &mut AbstractRowGenerator<ItemGeneratorColumn> {
        &mut self.abstract_generator
    }

    fn previous_row(&self) -> Option<&ItemRow> {
        self.previous_row.as_ref()
    }

    fn generate_row(&mut self, row_number: u64) -> ItemRow {
        self.generate_item_row(row_number)
    }
}

impl Iterator for ItemRowGenerator {
    type Item = ItemRow;

    fn next(&mut self) -> Option<ItemRow> {
        self.next_row()
    }
}
