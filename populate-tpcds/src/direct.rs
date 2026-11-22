//! `inventory` and `customer_demographics` built straight into Arrow
//! columns. They are the two biggest tables after the sales channels
//! (11.7M and 1.9M rows at SF 1) and nearly free to compute: a cross
//! product of keys, plus one random quantity and a null bitmap for
//! inventory. tpcdsgen builds a row struct per row (`String`s for
//! demographics) and tpcdsgen-arrow then walks the rows once per column;
//! here each column is filled directly. Both are tested equal to
//! tpcdsgen-arrow.

use crate::seeds;
use arrow_array::builder::make_view;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringViewArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use std::sync::Arc;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::distribution::DemographicsDistributions as Demo;
use tpcdsgen::generator::InventoryGeneratorColumn as Inv;
use tpcdsgen::nulls::create_null_bit_map;
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::AbstractRowGenerator;
use tpcdsgen::slowly_changing_dimension_utils::match_surrogate_key;
use tpcdsgen::table::Table as NullTable;
use tpcdsgen::types::Date;
use tpcdsgen_arrow::{CustomerDemographicsArrow, InventoryArrow};

fn i32s(v: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::new(ScalarBuffer::from(v), None))
}

/// Inventory source rows `first..=last` (1-based), in batches of `batch`.
/// Row `r` is item `(r-1) % items`, then warehouse, then week, slowest.
pub(crate) fn inventory(
    session: &Session,
    first: u64,
    last: u64,
    batch: usize,
    emit: &mut dyn FnMut(RecordBatch),
) {
    let scaling = session.get_scaling();
    let total = scaling.get_row_count(Ds::Inventory);
    let last = last.min(total);
    let mut g = AbstractRowGenerator::<Inv>::new(total);
    g.set_source_row_range(first, last);
    let items = scaling.get_id_count(Ds::Item);
    let warehouses = scaling.get_row_count(Ds::Warehouse);
    let index = first - 1;
    let (mut item, mut warehouse, mut week) = (
        index % items,
        (index / items) % warehouses,
        index / items / warehouses,
    );
    let schema = InventoryArrow::schema_ref();
    let mut row = first;
    while row <= last {
        let n = batch.min((last - row + 1) as usize);
        let (mut date, mut item_sk, mut wh) = (vec![0; n], vec![0; n], vec![0; n]);
        let mut qty = vec![0; n];
        let mut valid = vec![true; n];
        for i in 0..n {
            let nbm = create_null_bit_map(
                NullTable::Inventory,
                g.get_random_number_stream(&Inv::InvNulls),
            );
            let d = Date::JULIAN_DATE_MINIMUM as i64 + week as i64 * 7;
            date[i] = d as i32;
            item_sk[i] = match_surrogate_key(item as i64 + 1, d, Ds::Item, scaling) as i32;
            wh[i] = warehouse as i32 + 1;
            qty[i] = RandomValueGenerator::generate_uniform_random_int(
                0,
                1000,
                g.get_random_number_stream(&Inv::InvQuantityOnHand),
            );
            // the date, item and warehouse are NOT NULL
            valid[i] = nbm & (1 << 3) == 0;
            seeds::finish_row(&mut g);
            item += 1;
            if item == items {
                item = 0;
                warehouse += 1;
                if warehouse == warehouses {
                    warehouse = 0;
                    week += 1;
                }
            }
        }
        let nulls = NullBuffer::new(BooleanBuffer::from(valid));
        let qty = Int32Array::new(ScalarBuffer::from(qty), Some(nulls));
        let cols = vec![
            i32s(date),
            i32s(item_sk),
            i32s(wh),
            Arc::new(qty) as ArrayRef,
        ];
        emit(RecordBatch::try_new(schema.clone(), cols).expect("inventory schema"));
        row += n as u64;
    }
}

/// Whether [`customer_demographics`] applies: the table must have no
/// nulls, so that no random draw reaches the output.
pub(crate) fn demographics_direct() -> bool {
    NullTable::CustomerDemographics.get_null_basis_points() == 0
}

/// One `Utf8View` column over a fixed list of strings: every view points
/// into one shared buffer of all of them.
struct Strings {
    data: Arc<[Buffer]>,
    views: Vec<u128>,
}

impl Strings {
    fn new(values: &[&str]) -> Self {
        let mut data = Vec::new();
        let views = values
            .iter()
            .map(|s| {
                let v = make_view(s.as_bytes(), 0, data.len() as u32);
                if s.len() > 12 {
                    data.extend_from_slice(s.as_bytes());
                }
                v
            })
            .collect();
        Strings {
            data: Arc::new([Buffer::from_vec(data)]),
            views,
        }
    }

    fn column(&self, idx: impl Iterator<Item = usize>) -> ArrayRef {
        let views: ScalarBuffer<u128> = idx.map(|i| self.views[i]).collect();
        // SAFETY: every view is `make_view` of a valid UTF-8 string, at its
        // offset in `data` (block 0) when it is not inline.
        Arc::new(unsafe { StringViewArray::new_unchecked(views, self.data.clone(), None) })
    }
}

/// customer_demographics rows `first..=last`: `cd_demo_sk` is the row, and
/// the other columns are its digits in a mixed radix, gender fastest.
pub(crate) fn customer_demographics(
    session: &Session,
    first: u64,
    last: u64,
    batch: usize,
    emit: &mut dyn FnMut(RecordBatch),
) {
    let last = last.min(
        session
            .get_scaling()
            .get_row_count(Ds::CustomerDemographics),
    );
    let list = |n: usize, f: fn(i64) -> &'static str| -> Vec<&'static str> {
        (0..n as i64).map(f).collect()
    };
    let gender = Strings::new(&list(
        Demo::get_gender_size(),
        Demo::get_gender_for_index_mod_size,
    ));
    let marital = Strings::new(&list(
        Demo::get_marital_status_size(),
        Demo::get_marital_status_for_index_mod_size,
    ));
    let education = Strings::new(&list(
        Demo::get_education_size(),
        Demo::get_education_for_index_mod_size,
    ));
    let credit = Strings::new(&list(
        Demo::get_credit_rating_size(),
        Demo::get_credit_rating_for_index_mod_size,
    ));
    let purchase: Vec<i32> = (0..Demo::get_purchase_band_size() as i64)
        .map(Demo::get_purchase_band_for_index_mod_size)
        .collect();
    // radices, fastest first; the last three are the dependent counts
    let radix = [
        Demo::get_gender_size() as u64,
        Demo::get_marital_status_size() as u64,
        Demo::get_education_size() as u64,
        purchase.len() as u64,
        Demo::get_credit_rating_size() as u64,
        7,
        7,
        7,
    ];
    let schema = CustomerDemographicsArrow::schema_ref();
    let mut row = first;
    while row <= last {
        let n = batch.min((last - row + 1) as usize);
        // digit `d` of each row's index
        let digit = |d: usize| {
            let div: u64 = radix[..d].iter().product();
            (row - 1..row - 1 + n as u64).map(move |i| i / div % radix[d])
        };
        let ints = |d: usize| i32s(digit(d).map(|v| v as i32).collect());
        let cols = vec![
            i32s((row..row + n as u64).map(|r| r as i32).collect()),
            gender.column(digit(0).map(|v| v as usize)),
            marital.column(digit(1).map(|v| v as usize)),
            education.column(digit(2).map(|v| v as usize)),
            i32s(digit(3).map(|v| purchase[v as usize]).collect()),
            credit.column(digit(4).map(|v| v as usize)),
            ints(5),
            ints(6),
            ints(7),
        ];
        emit(RecordBatch::try_new(schema.clone(), cols).expect("demographics schema"));
        row += n as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpcdsgen::config::SessionBuilder;

    fn collect(f: impl FnOnce(&mut dyn FnMut(RecordBatch))) -> Vec<RecordBatch> {
        let mut v = Vec::new();
        f(&mut |b| v.push(b));
        let schema = v[0].schema();
        vec![arrow_select::concat::concat_batches(&schema, &v).unwrap()]
    }

    #[test]
    fn inventory_matches_tpcdsgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        // the start, a warehouse and a week boundary, the end
        for (first, last) in [
            (1, 5000),
            (17_990, 18_010),
            (89_990, 90_020),
            (11_744_000, 11_745_000),
        ] {
            let want = collect(|e| {
                InventoryArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let got = collect(|e| inventory(&s, first, last, 777, e));
            assert_eq!(got, want, "{first}..={last}");
        }
    }

    #[test]
    fn demographics_match_tpcdsgen_arrow() {
        assert!(demographics_direct());
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        for (first, last) in [(1, 3000), (1_000_000, 1_003_000), (1_920_000, 1_920_800)] {
            let want = collect(|e| {
                CustomerDemographicsArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let got = collect(|e| customer_demographics(&s, first, last, 500, e));
            assert_eq!(got, want, "{first}..={last}");
        }
    }
}
