// Modified from tpcdsgen 4.0.0 for the populate DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

use crate::config::{Session, Table as ConfigTable};
use crate::distribution::web_page_use_distribution::WebPageUseDistribution;
use crate::dsb::join::join_key;
use crate::dsb::{exp_int, exp_pick_index, make_perm, map_perm};
use crate::generator::WebPageGeneratorColumn;
use crate::join_key_utils::generate_web_page_creation_date_join_key;
use crate::random::{RandomNumberStream, RandomValueGenerator};
use crate::row::{AbstractRowGenerator, WebPageRow};
use crate::slowly_changing_dimension_utils::{
    compute_scd_key, get_value_for_slowly_changing_dimension, ScdRowGenerator,
};
use crate::table::Table;
use crate::types::Date;

/// Row generator for the WEB_PAGE table (WebPageRowGenerator)
/// Pattern 2: SCD table with slowly changing dimension logic
pub struct WebPageRowGenerator {
    abstract_generator: AbstractRowGenerator<WebPageGeneratorColumn>,
    previous_row: Option<WebPageRow>,
    session: Session,
    perms: Perms,
}

const WP_LINK_MIN: i32 = 2;
const WP_LINK_MAX: i32 = 25;
const WP_IMAGE_MIN: i32 = 1;
const WP_IMAGE_MAX: i32 = 7;
const WP_AD_MIN: i32 = 0;
const WP_AD_MAX: i32 = 4;

/// The permutations DSB's `mk_w_web_page` makes on its first call.
struct Perms {
    link_count: Vec<i32>,
    image_count: Vec<i32>,
    max_ad_count: Vec<i32>,
    web_type: Vec<i32>,
}

/// An exponential value in `[min, max]` mapped through `perm`.
fn exp_permuted(min: i32, max: i32, perm: &[i32], stream: &mut RandomNumberStream) -> i32 {
    map_perm(exp_int(min, max, stream) - (min - 1), perm) + min - 1
}

impl WebPageRowGenerator {
    const WP_AUTOGEN_PERCENT: i32 = 30;

    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        use WebPageGeneratorColumn::*;
        let mut ag = AbstractRowGenerator::new(row_count);
        let link_count = make_perm(
            WP_LINK_MAX - WP_LINK_MIN + 1,
            ag.get_random_number_stream(&WpLinkCount),
        );
        let image_count = make_perm(
            WP_IMAGE_MAX - WP_IMAGE_MIN + 1,
            ag.get_random_number_stream(&WpImageCount),
        );
        let max_ad_count = make_perm(
            WP_AD_MAX - WP_AD_MIN + 1,
            ag.get_random_number_stream(&WpMaxAdCount),
        );
        let web_type = make_perm(
            WebPageUseDistribution::values().len() as i32,
            ag.get_random_number_stream(&WpType),
        );
        ag.mark_row1();
        Self {
            abstract_generator: ag,
            previous_row: None,
            session,
            perms: Perms {
                link_count,
                image_count,
                max_ad_count,
                web_type,
            },
        }
    }

    /// Generate a WebPageRow with SCD logic following Java implementation
    fn generate_web_page_row(&mut self, row_number: u64) -> WebPageRow {
        let session = &self.session;
        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        // Create null bit map
        let nulls_stream = self
            .abstract_generator
            .get_random_number_stream(&WebPageGeneratorColumn::WpNulls);
        let threshold = RandomValueGenerator::generate_uniform_random_int(0, 9999, nulls_stream);
        let bit_map = RandomValueGenerator::generate_uniform_random_int(1, i32::MAX, nulls_stream);

        let null_bit_map = if threshold < Table::WebPage.get_null_basis_points() {
            (bit_map as i64) & !Table::WebPage.get_not_null_bit_map()
        } else {
            0
        };

        let wp_page_sk = row_number_i64;

        // Compute SCD key information
        let scd_key = compute_scd_key(Table::WebPage, row_number);
        let wp_page_id = scd_key.get_business_key().to_string();
        let wp_rec_start_date_id = scd_key.get_start_date();
        let wp_rec_end_date_id = scd_key.get_end_date();
        let is_new_key = scd_key.is_new_business_key();

        // Get field change flags for SCD
        let mut field_change_flags = self
            .abstract_generator
            .get_random_number_stream(&WebPageGeneratorColumn::WpScd)
            .next_random() as i32;

        // wp_creation_date_sk - join to DATE_DIM
        let mut wp_creation_date_sk = generate_web_page_creation_date_join_key(
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpCreationDateSk),
            row_number_i64,
            session.get_scaling(),
        );
        if let Some(prev) = &self.previous_row {
            wp_creation_date_sk = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_creation_date_sk(),
                wp_creation_date_sk,
            );
        }
        field_change_flags >>= 1;

        // wp_access_date_sk - today's date minus last access days
        let last_access = RandomValueGenerator::generate_uniform_random_int(
            0,
            100,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpAccessDateSk),
        );
        let mut wp_access_date_sk = Date::JULIAN_TODAYS_DATE as i64 - last_access as i64;
        if let Some(prev) = &self.previous_row {
            wp_access_date_sk = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_access_date_sk(),
                wp_access_date_sk,
            );
        }
        field_change_flags >>= 1;

        // wp_autogen_flag - 30% chance of auto-generated
        let random_int = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpAutogenFlag),
        );
        let mut wp_autogen_flag = random_int < Self::WP_AUTOGEN_PERCENT;
        if let Some(prev) = &self.previous_row {
            wp_autogen_flag = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_autogen_flag(),
                wp_autogen_flag,
            );
        }
        field_change_flags >>= 1;

        // wp_customer_sk - join to CUSTOMER
        let mut wp_customer_sk = join_key(
            ConfigTable::Customer,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpCustomerSk),
            session.get_scaling(),
            None,
        );
        if let Some(prev) = &self.previous_row {
            wp_customer_sk = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_customer_sk(),
                wp_customer_sk,
            );
        }
        field_change_flags >>= 1;

        // wp_url - always returns the same value, so no need to check if it should change
        let wp_url = RandomValueGenerator::generate_random_url(
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpUrl),
        );
        field_change_flags >>= 1;

        // wp_type - always uses a new value due to a bug in the C code
        let types = WebPageUseDistribution::values();
        let wp_type = types[exp_pick_index(
            types.len() as i32,
            Some(&self.perms.web_type),
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpType),
        )]
        .clone();
        field_change_flags >>= 1;

        // wp_link_count
        let mut wp_link_count = exp_permuted(
            WP_LINK_MIN,
            WP_LINK_MAX,
            &self.perms.link_count,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpLinkCount),
        );
        if let Some(prev) = &self.previous_row {
            wp_link_count = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_link_count(),
                wp_link_count,
            );
        }
        field_change_flags >>= 1;

        // wp_image_count
        let mut wp_image_count = exp_permuted(
            WP_IMAGE_MIN,
            WP_IMAGE_MAX,
            &self.perms.image_count,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpImageCount),
        );
        if let Some(prev) = &self.previous_row {
            wp_image_count = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_image_count(),
                wp_image_count,
            );
        }
        field_change_flags >>= 1;

        // wp_max_ad_count
        let mut wp_max_ad_count = exp_permuted(
            WP_AD_MIN,
            WP_AD_MAX,
            &self.perms.max_ad_count,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpMaxAdCount),
        );
        if let Some(prev) = &self.previous_row {
            wp_max_ad_count = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_max_ad_count(),
                wp_max_ad_count,
            );
        }
        field_change_flags >>= 1;

        // wp_char_count - calculated based on link and image counts
        let mut wp_char_count = RandomValueGenerator::generate_uniform_random_int(
            wp_link_count * 125 + wp_image_count * 50,
            wp_link_count * 300 + wp_image_count * 150,
            self.abstract_generator
                .get_random_number_stream(&WebPageGeneratorColumn::WpCharCount),
        );
        if let Some(prev) = &self.previous_row {
            wp_char_count = get_value_for_slowly_changing_dimension(
                field_change_flags,
                is_new_key,
                prev.get_wp_char_count(),
                wp_char_count,
            );
        }

        // Store current row for next iteration (before creating output row)
        self.previous_row = Some(WebPageRow::new(
            null_bit_map,
            wp_page_sk,
            wp_page_id.clone(),
            wp_rec_start_date_id,
            wp_rec_end_date_id,
            wp_creation_date_sk,
            wp_access_date_sk,
            wp_autogen_flag,
            wp_customer_sk,
            wp_url.clone(),
            wp_type.clone(),
            wp_char_count,
            wp_link_count,
            wp_image_count,
            wp_max_ad_count,
        ));

        // Return row with wp_customer_sk set to -1 if not autogenerated (line 155 in Java)
        WebPageRow::new(
            null_bit_map,
            wp_page_sk,
            wp_page_id,
            wp_rec_start_date_id,
            wp_rec_end_date_id,
            wp_creation_date_sk,
            wp_access_date_sk,
            wp_autogen_flag,
            if wp_autogen_flag { wp_customer_sk } else { -1 },
            wp_url,
            wp_type,
            wp_char_count,
            wp_link_count,
            wp_image_count,
            wp_max_ad_count,
        )
    }

    fn ensure_row2(&mut self, row: u64) {
        if self.abstract_generator.needs_row2(row) {
            let mut first = Self::new(self.session.clone(), 1);
            first.next();
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

impl ScdRowGenerator for WebPageRowGenerator {
    type Column = WebPageGeneratorColumn;
    type Row = WebPageRow;

    fn abstract_generator(&mut self) -> &mut AbstractRowGenerator<WebPageGeneratorColumn> {
        &mut self.abstract_generator
    }

    fn previous_row(&self) -> Option<&WebPageRow> {
        self.previous_row.as_ref()
    }

    fn generate_row(&mut self, row_number: u64) -> WebPageRow {
        self.generate_web_page_row(row_number)
    }
}

impl Iterator for WebPageRowGenerator {
    type Item = WebPageRow;

    fn next(&mut self) -> Option<WebPageRow> {
        self.next_row()
    }
}
