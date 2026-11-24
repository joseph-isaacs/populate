use crate::business_key_generator::make_business_key;
use crate::config::Session;
use crate::dsb::address::make_address;
use crate::dsb::{exp_bucketed_int, make_bucket, make_perm};
use crate::generator::WarehouseGeneratorColumn;
use crate::random::{RandomTextGenerator, RandomValueGenerator};
use crate::row::{AbstractRowGenerator, WarehouseRow};
use crate::table::Table;

/// Row generator for the WAREHOUSE table (WarehouseRowGenerator)
pub struct WarehouseRowGenerator {
    abstract_generator: AbstractRowGenerator<WarehouseGeneratorColumn>,
    text: RandomTextGenerator,
    session: Session,
    /// DSB's square footage buckets and their permutation.
    sq_ft_bucket: (i32, i32),
    sq_ft_perm: Vec<i32>,
}

const W_SQFT_MIN: i32 = 50_000;
const W_SQFT_MAX: i32 = 1_000_000;

impl WarehouseRowGenerator {
    /// Generate source rows `1..=row_count`.
    pub fn new(session: Session, row_count: u64) -> Self {
        let mut abstract_generator = AbstractRowGenerator::new(row_count);
        let sq_ft_bucket = make_bucket(10, W_SQFT_MAX - W_SQFT_MIN + 1);
        let sq_ft_perm = make_perm(
            sq_ft_bucket.1,
            abstract_generator.get_random_number_stream(&WarehouseGeneratorColumn::WWarehouseSqFt),
        );
        // An exponential bucket plus a uniform value: two draws every row.
        abstract_generator.set_seeds_per_row(&WarehouseGeneratorColumn::WWarehouseSqFt, 2);
        abstract_generator.mark_row1();
        Self {
            abstract_generator,
            text: RandomTextGenerator::new(),
            session,
            sq_ft_bucket,
            sq_ft_perm,
        }
    }

    /// Generate a WarehouseRow with realistic data following Java implementation
    fn generate_warehouse_row(&mut self, row_number: u64) -> WarehouseRow {
        let session = &self.session;
        let row_number_i64 = i64::try_from(row_number).expect("row number fits in i64");

        // Create null bit map (createNullBitMap call)
        let nulls_stream = self
            .abstract_generator
            .get_random_number_stream(&WarehouseGeneratorColumn::WNulls);
        let threshold = RandomValueGenerator::generate_uniform_random_int(0, 9999, nulls_stream);
        let bit_map = RandomValueGenerator::generate_uniform_random_int(1, i32::MAX, nulls_stream);

        // Calculate null_bit_map based on threshold and table's not-null bitmap (Nulls.createNullBitMap)
        let null_bit_map = if threshold < Table::Warehouse.get_null_basis_points() {
            (bit_map as i64) & !Table::Warehouse.get_not_null_bit_map()
        } else {
            0
        };

        let w_warehouse_sk = row_number_i64;
        let w_warehouse_id = make_business_key(row_number);

        let name_stream = self
            .abstract_generator
            .get_random_number_stream(&WarehouseGeneratorColumn::WWarehouseName);
        let w_warehouse_name = self.text.random_text(10, 20, name_stream);

        let sq_ft_stream = self
            .abstract_generator
            .get_random_number_stream(&WarehouseGeneratorColumn::WWarehouseSqFt);
        let w_warehouse_sq_ft = exp_bucketed_int(
            W_SQFT_MIN,
            W_SQFT_MAX,
            self.sq_ft_bucket,
            &self.sq_ft_perm,
            sq_ft_stream,
        );

        let scaling = session.get_scaling();
        let address_stream = self
            .abstract_generator
            .get_random_number_stream(&WarehouseGeneratorColumn::WWarehouseAddress);
        let (w_address, _) = make_address(Table::Warehouse, address_stream, scaling);

        WarehouseRow::new(
            null_bit_map,
            w_warehouse_sk,
            w_warehouse_id,
            w_warehouse_name,
            w_warehouse_sq_ft,
            w_address,
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

impl Iterator for WarehouseRowGenerator {
    type Item = WarehouseRow;

    fn next(&mut self) -> Option<WarehouseRow> {
        let row_number = self.abstract_generator.next_row_number()?;
        let row = self.generate_warehouse_row(row_number);
        self.abstract_generator.finish_row();
        Some(row)
    }
}
