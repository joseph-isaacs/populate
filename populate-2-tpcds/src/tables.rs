//! TPC-DS tables as populate-2 distributions.

use crate::columns::{NullBitmap, NullI32, NullI32Col, stream, uniform_int};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_2::traits::{Distribution, VecBackend};
use populate_2_tpch::column::{Column, col};
use populate_2_tpch::rng::Stream;
use populate_2_tpch::tables::collect_batch;
use populate_2_tpch::values::{Str, StrCol};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Scaling, SessionBuilder, Table as DsTable};
use tpcdsgen::distribution::DemographicsDistributions as Demo;
use tpcdsgen::types::Date;

// ============================================================================
// Table trait + batching
// ============================================================================

/// Row counts for one scale factor (from tpcdsgen's scaling tables).
pub struct Scale {
    scaling: Scaling,
}

impl Scale {
    pub fn new(sf: f64) -> Self {
        let session = SessionBuilder::new()
            .with_scale_factor(sf)
            .build()
            .expect("valid scale factor");
        Self {
            scaling: session.get_scaling().clone(),
        }
    }

    pub fn rows(&self, table: DsTable) -> i64 {
        self.scaling.get_row_count(table) as i64
    }

    pub fn ids(&self, table: DsTable) -> i64 {
        self.scaling.get_id_count(table) as i64
    }
}

pub trait Table {
    fn schema() -> SchemaRef;
    fn row_count(scale: &Scale) -> i64;
    /// Rows `[start, start + n)` (0-based) as one batch.
    fn batch(scale: &Scale, start: i64, n: usize) -> RecordBatch;
}

/// All rows of `T` in batches of `batch_size`, like `tpcdsgen-arrow`.
pub fn batches<T: Table>(sf: f64, batch_size: usize) -> impl Iterator<Item = RecordBatch> {
    let scale = Scale::new(sf);
    let rows = T::row_count(&scale);
    (0..rows).step_by(batch_size).map(move |start| {
        T::batch(
            &scale,
            start,
            (rows - start).min(batch_size as i64) as usize,
        )
    })
}

fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
            .collect::<Vec<_>>(),
    ))
}

// ============================================================================
// INVENTORY: one row per (week, warehouse, item); 10% of rows draw a null
// bitmap, and only the quantity column can actually be null
// ============================================================================

/// Item surrogate key for a business key at a date: items have 1-3 SCD
/// revisions (`match_surrogate_key`).
pub fn match_item_key(unique: i64, julian_date: i64, item_rows: i64) -> i64 {
    const START: i64 = Date::JULIAN_DATA_START_DATE;
    const END: i64 = Date::JULIAN_DATA_END_DATE;
    const ONE_HALF: i64 = START + (END - START) / 2;
    const ONE_THIRD: i64 = START + (END - START) / 3;
    const TWO_THIRDS: i64 = ONE_THIRD + (END - START) / 3;
    let mut key = (unique / 3) * 6;
    match unique % 3 {
        1 => key += 1,
        2 => key += 2 + (julian_date > ONE_HALF) as i64,
        _ => key += -2 + (julian_date > ONE_THIRD) as i64 + (julian_date > TWO_THIRDS) as i64,
    }
    key.min(item_rows)
}

/// Which inventory key a column emits.
#[derive(Clone, Copy)]
pub enum InvKey {
    Date,
    Item,
    Warehouse,
}

/// Inventory key columns: (item, warehouse, week) advance like an odometer,
/// so no row needs a division.
#[derive(Clone, Copy)]
pub struct InventoryKey {
    key: InvKey,
    items: i64,
    warehouses: i64,
    item_rows: i64,
}

#[derive(Clone)]
pub struct Odometer {
    item: i64,
    warehouse: i64,
    week: i64,
    /// Item column: this week's keys for every item (`week_keys_for` week).
    week_keys: Vec<i32>,
    week_keys_for: i64,
}

impl InventoryKey {
    pub fn new(key: InvKey, items: i64, warehouses: i64, item_rows: i64) -> Self {
        Self {
            key,
            items,
            warehouses,
            item_rows,
        }
    }
}

impl Column for InventoryKey {
    type Item = i32;
    type Sink = Vec<i32>;
    type State = Odometer;

    fn reserve(&self, sink: &mut Vec<i32>, n: usize) {
        sink.reserve(n);
    }

    fn init(&self, start: i64) -> Odometer {
        Odometer {
            item: start % self.items,
            warehouse: (start / self.items) % self.warehouses,
            week: start / (self.items * self.warehouses),
            week_keys: Vec::new(),
            week_keys_for: -1,
        }
    }

    /// Bulk path: rows come in runs of one (week, warehouse) covering the
    /// item range, so each run is a fill (date, warehouse) or a slice copy of
    /// the week's item keys, computed once per week.
    fn write_n(&self, o: &mut Odometer, _start: i64, mut n: usize, sink: &mut Vec<i32>) {
        while n > 0 {
            let run = ((self.items - o.item) as usize).min(n);
            let date = Date::JULIAN_DATE_MINIMUM as i64 + o.week * 7;
            match self.key {
                InvKey::Date => sink.extend(std::iter::repeat_n(date as i32, run)),
                InvKey::Warehouse => {
                    sink.extend(std::iter::repeat_n((o.warehouse + 1) as i32, run))
                }
                InvKey::Item => {
                    if o.week_keys_for != o.week {
                        o.week_keys.clear();
                        o.week_keys.extend(
                            (1..=self.items)
                                .map(|u| match_item_key(u, date, self.item_rows) as i32),
                        );
                        o.week_keys_for = o.week;
                    }
                    let i = o.item as usize;
                    sink.extend_from_slice(&o.week_keys[i..i + run]);
                }
            }
            n -= run;
            o.item += run as i64;
            if o.item == self.items {
                o.item = 0;
                o.warehouse += 1;
                if o.warehouse == self.warehouses {
                    o.warehouse = 0;
                    o.week += 1;
                }
            }
        }
    }

    #[inline(always)]
    fn value(&self, o: &mut Odometer, _row: i64) -> i32 {
        let date = Date::JULIAN_DATE_MINIMUM as i64 + o.week * 7;
        let v = match self.key {
            InvKey::Date => date,
            InvKey::Warehouse => o.warehouse + 1,
            InvKey::Item => match_item_key(o.item + 1, date, self.item_rows),
        };
        o.item += 1;
        if o.item == self.items {
            o.item = 0;
            o.warehouse += 1;
            if o.warehouse == self.warehouses {
                o.warehouse = 0;
                o.week += 1;
            }
        }
        v as i32
    }
}

/// `inv_quantity_on_hand`: uniform 0..=1000, null when the row's null
/// bitmap has bit 3 set (re-runs the nulls stream itself).
#[derive(Clone, Copy)]
pub struct InventoryQuantity;

const INV_QUANTITY_COLUMN: i32 = 201;
const INV_NULLS_COLUMN: i32 = 202;

impl Column for InventoryQuantity {
    type Item = NullI32;
    type Sink = NullI32Col;
    type State = (Stream, Stream, NullBitmap);

    fn reserve(&self, sink: &mut NullI32Col, n: usize) {
        sink.reserve(n);
    }

    fn init(&self, start: i64) -> Self::State {
        (
            stream(INV_QUANTITY_COLUMN, 1, start),
            stream(INV_NULLS_COLUMN, 2, start),
            NullBitmap::of(DsTable::Inventory),
        )
    }

    #[inline(always)]
    fn value(&self, (q, nulls, nbm): &mut Self::State, _row: i64) -> NullI32 {
        let null = nbm.is_null(nulls.next_item(), 3);
        let v = uniform_int(q.next_item(), 0, 1000);
        NullI32((!null).then_some(v))
    }

    #[inline(always)]
    fn write(&self, (q, nulls, nbm): &mut Self::State, _row: i64, sink: &mut NullI32Col) {
        let null = nbm.is_null(nulls.next_item(), 3);
        let v = uniform_int(q.next_item(), 0, 1000);
        if null {
            sink.push_null();
        } else {
            sink.push_value(v);
        }
    }
}

pub struct Inventory;

static INVENTORY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("inv_date_sk", DataType::Int32, false),
        ("inv_item_sk", DataType::Int32, false),
        ("inv_warehouse_sk", DataType::Int32, false),
        ("inv_quantity_on_hand", DataType::Int32, true),
    ])
});

pub fn inventory(
    scale: &Scale,
    start: i64,
) -> impl Distribution<VecBackend, Item = (i32, i32, i32, NullI32)> {
    let k = |key| InventoryKey {
        key,
        items: scale.ids(DsTable::Item),
        warehouses: scale.rows(DsTable::Warehouse),
        item_rows: scale.rows(DsTable::Item),
    };
    (
        col(k(InvKey::Date), start),
        col(k(InvKey::Item), start),
        col(k(InvKey::Warehouse), start),
        col(InventoryQuantity, start),
    )
}

impl Table for Inventory {
    fn schema() -> SchemaRef {
        INVENTORY_SCHEMA.clone()
    }
    fn row_count(scale: &Scale) -> i64 {
        scale.rows(DsTable::Inventory)
    }
    fn batch(scale: &Scale, start: i64, n: usize) -> RecordBatch {
        collect_batch(inventory(scale, start), n, &INVENTORY_SCHEMA)
    }
}

// ============================================================================
// CUSTOMER_DEMOGRAPHICS: every column is a digit of the row index in a
// mixed radix (gender, marital, education, purchase, credit, 3 x 0..7).
// Null rate is 0, so its nulls stream never matters and isn't run.
// ============================================================================

/// One mixed-radix digit: value `(row / divisor) % len(values)`, walked with
/// a counter (no division per row).
#[derive(Clone)]
pub struct Digit<T: 'static> {
    divisor: i64,
    values: &'static [T],
}

#[derive(Clone)]
pub struct DigitState {
    digit: usize,
    left_in_run: i64,
    /// String digits: each value's view in the current sink, interned on the
    /// first write (one copy of each string per batch, not per row).
    views: Vec<u128>,
}

impl<T: 'static> Digit<T> {
    fn init_state(&self, start: i64) -> DigitState {
        DigitState {
            digit: ((start / self.divisor) % self.values.len() as i64) as usize,
            left_in_run: self.divisor - start % self.divisor,
            views: Vec::new(),
        }
    }

    #[inline(always)]
    fn step(&self, st: &mut DigitState) -> &'static T {
        &self.values[self.advance(st)]
    }

    /// Current digit, then move to the next row.
    #[inline(always)]
    fn advance(&self, st: &mut DigitState) -> usize {
        let v = st.digit;
        st.left_in_run -= 1;
        if st.left_in_run == 0 {
            st.left_in_run = self.divisor;
            st.digit += 1;
            if st.digit == self.values.len() {
                st.digit = 0;
            }
        }
        v
    }
}

impl Column for Digit<&'static str> {
    type Item = Str;
    type Sink = StrCol;
    type State = DigitState;

    fn reserve(&self, sink: &mut StrCol, n: usize) {
        sink.reserve(n, 4);
    }

    fn init(&self, start: i64) -> DigitState {
        self.init_state(start)
    }

    fn value(&self, st: &mut DigitState, _row: i64) -> Str {
        Str::borrowed(self.step(st))
    }

    #[inline(always)]
    fn write(&self, st: &mut DigitState, _row: i64, sink: &mut StrCol) {
        if st.views.is_empty() {
            st.views = self.values.iter().map(|v| sink.intern(v)).collect();
        }
        let d = self.advance(st);
        sink.push_view(st.views[d]);
    }
}

impl Column for Digit<i32> {
    type Item = i32;
    type Sink = Vec<i32>;
    type State = DigitState;

    fn reserve(&self, sink: &mut Vec<i32>, n: usize) {
        sink.reserve(n);
    }

    fn init(&self, start: i64) -> DigitState {
        self.init_state(start)
    }

    #[inline(always)]
    fn value(&self, st: &mut DigitState, _row: i64) -> i32 {
        *self.step(st)
    }
}

fn values<T>(n: usize, f: impl Fn(i64) -> T) -> &'static [T] {
    Vec::leak((0..n as i64).map(f).collect())
}

struct DemoLists {
    gender: &'static [&'static str],
    marital: &'static [&'static str],
    education: &'static [&'static str],
    purchase: &'static [i32],
    credit: &'static [&'static str],
    zero_to_six: &'static [i32],
}

static DEMO: LazyLock<DemoLists> = LazyLock::new(|| DemoLists {
    gender: values(Demo::get_gender_size(), Demo::get_gender_for_index_mod_size),
    marital: values(
        Demo::get_marital_status_size(),
        Demo::get_marital_status_for_index_mod_size,
    ),
    education: values(
        Demo::get_education_size(),
        Demo::get_education_for_index_mod_size,
    ),
    purchase: values(
        Demo::get_purchase_band_size(),
        Demo::get_purchase_band_for_index_mod_size,
    ),
    credit: values(
        Demo::get_credit_rating_size(),
        Demo::get_credit_rating_for_index_mod_size,
    ),
    zero_to_six: values(7, |i| i as i32),
});

pub struct CustomerDemographics;

static CUSTOMER_DEMOGRAPHICS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(&[
        ("cd_demo_sk", DataType::Int32, false),
        ("cd_gender", DataType::Utf8View, true),
        ("cd_marital_status", DataType::Utf8View, true),
        ("cd_education_status", DataType::Utf8View, true),
        ("cd_purchase_estimate", DataType::Int32, true),
        ("cd_credit_rating", DataType::Utf8View, true),
        ("cd_dep_count", DataType::Int32, true),
        ("cd_dep_employed_count", DataType::Int32, true),
        ("cd_dep_college_count", DataType::Int32, true),
    ])
});

#[allow(clippy::type_complexity)]
pub fn customer_demographics(
    start: i64,
) -> impl Distribution<VecBackend, Item = ((i32, Str, Str, Str), (i32, Str, i32, i32), i32)> {
    let d = &*DEMO;
    // divisor of each digit = product of the radixes before it
    let mut div = 1i64;
    let mut next = |len: usize| {
        let this = div;
        div *= len as i64;
        this
    };
    let gender = Digit {
        divisor: next(d.gender.len()),
        values: d.gender,
    };
    let marital = Digit {
        divisor: next(d.marital.len()),
        values: d.marital,
    };
    let education = Digit {
        divisor: next(d.education.len()),
        values: d.education,
    };
    let purchase = Digit {
        divisor: next(d.purchase.len()),
        values: d.purchase,
    };
    let credit = Digit {
        divisor: next(d.credit.len()),
        values: d.credit,
    };
    let dep = Digit {
        divisor: next(7),
        values: d.zero_to_six,
    };
    let employed = Digit {
        divisor: next(7),
        values: d.zero_to_six,
    };
    let college = Digit {
        divisor: next(7),
        values: d.zero_to_six,
    };
    (
        (
            populate_2::dist::range(start as i32 + 1, 1),
            col(gender, start),
            col(marital, start),
            col(education, start),
        ),
        (
            col(purchase, start),
            col(credit, start),
            col(dep, start),
            col(employed, start),
        ),
        col(college, start),
    )
}

impl Table for CustomerDemographics {
    fn schema() -> SchemaRef {
        CUSTOMER_DEMOGRAPHICS_SCHEMA.clone()
    }
    fn row_count(scale: &Scale) -> i64 {
        scale.rows(DsTable::CustomerDemographics)
    }
    fn batch(_scale: &Scale, start: i64, n: usize) -> RecordBatch {
        collect_batch(
            customer_demographics(start),
            n,
            &CUSTOMER_DEMOGRAPHICS_SCHEMA,
        )
    }
}
