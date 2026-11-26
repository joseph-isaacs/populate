//! `inventory` and `customer_demographics`: the two biggest tables after
//! the sales channels (11.7M and 1.9M rows at SF 1), and nearly free to
//! compute. Both are a cross product of keys; inventory adds a random
//! quantity, nulled by its row's null bitmap.

use crate::seeds::stream;
use populate_builtins::{derive, draws, named_all};
use populate_combinators::text::{StrViews, Word};
use populate_core::{Columns, Distribution, Sink};
use rand::Rng;
use std::sync::Arc;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::distribution::DemographicsDistributions as Demo;
use tpcdsgen::generator::InventoryGeneratorColumn as Inv;
use tpcdsgen::slowly_changing_dimension_utils::match_surrogate_key;
use tpcdsgen::table::Table as NullTable;
use tpcdsgen::types::Date;

/// The inventory keys of each row: item fastest, then warehouse, then week.
#[derive(Debug, Clone)]
struct InventoryKeys {
    session: Session,
    /// 0-based row of the next sample.
    row: u64,
    items: u64,
    warehouses: u64,
}

impl InventoryKeys {
    /// Rows `row..row + n`, a run of one week and warehouse at a time.
    fn runs(&self, n: u64, mut run: impl FnMut(i64, u64, u64, usize)) {
        let mut at = self.row;
        let end = self.row + n;
        while at < end {
            let item = at % self.items;
            let warehouse = (at / self.items) % self.warehouses;
            let week = at / self.items / self.warehouses;
            let len = (self.items - item).min(end - at);
            let date = Date::JULIAN_DATE_MINIMUM as i64 + week as i64 * 7;
            run(date, warehouse, item, len as usize);
            at += len;
        }
    }
}

impl Distribution for InventoryKeys {
    /// `inv_date_sk`, `inv_item_sk`, `inv_warehouse_sk`.
    type Item = (i32, i32, i32);

    fn sample(&mut self, rng: &mut impl Rng) -> (i32, i32, i32) {
        let mut c = Columns::<Self::Item>::default();
        self.collect_into(&mut c, rng, 1);
        (c.0[0], c.1[0], c.2[0])
    }

    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, _: &mut impl Rng, n: usize) {
        let scaling = self.session.get_scaling();
        let (date, item, wh) = cols;
        date.reserve(n);
        item.reserve(n);
        wh.reserve(n);
        self.runs(n as u64, |d, warehouse, first, len| {
            date.extend(std::iter::repeat_n(d as i32, len));
            wh.extend(std::iter::repeat_n(warehouse as i32 + 1, len));
            // match_surrogate_key(u, d) is (u / 3) * 6 plus an offset that
            // depends on u % 3 and the date, capped at the row count: the
            // offsets, once per run
            let at = |u: i64| match_surrogate_key(u, d, Ds::Item, scaling) - u / 3 * 6;
            let offset = [at(3), at(1), at(2)];
            let cap = scaling.get_row_count(Ds::Item) as i64;
            item.extend((first..first + len as u64).map(|i| {
                let u = i as i64 + 1;
                (u / 3 * 6 + offset[(u % 3) as usize]).min(cap) as i32
            }));
        });
        self.row += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.row += n;
    }
}

/// `inventory`: one row per item, warehouse and week.
pub(crate) fn inventory(
    session: &Session,
) -> impl Distribution<Item = ((i32, i32, i32), Option<i32>)> + Clone + Sync + use<> {
    let scaling = session.get_scaling();
    let keys = InventoryKeys {
        session: session.clone(),
        row: 0,
        items: scaling.get_id_count(Ds::Item),
        warehouses: scaling.get_row_count(Ds::Warehouse),
    };
    let basis = NullTable::Inventory.get_null_basis_points();
    let not_null = NullTable::Inventory.get_not_null_bit_map();
    // one draw for the quantity and two for the null bitmap: each stream's
    // whole budget, so a row's draws are the next ones
    let qty = derive(
        (
            draws::<_, 1>(stream(&Inv::InvQuantityOnHand)),
            draws::<_, 2>(stream(&Inv::InvNulls)),
        ),
        move |(&[q], &[n1, n2])| {
            // create_null_bit_map: a threshold in 0..10000, then a bitmap in
            // 1..=i32::MAX; the date, item and warehouse are NOT NULL
            let nbm = if (n1 as i32) % 10000 < basis {
                ((n2 as i32) % i32::MAX + 1) as i64 & !not_null
            } else {
                0
            };
            // generate_uniform_random_int(0, 1000)
            (nbm & (1 << 3) == 0).then_some((q as i32) % 1001)
        },
    );
    named_all(
        &[
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand",
        ],
        (keys, qty),
    )
}

/// A nullable column of a table with no nulls.
type Opt<T> = Option<T>;

/// A row of `customer_demographics`.
type DemoRow = (
    i32,
    Opt<Word>,
    Opt<Word>,
    Opt<Word>,
    Opt<i32>,
    Opt<Word>,
    Opt<i32>,
    Opt<i32>,
    Opt<i32>,
);

/// The rows of `customer_demographics`: `cd_demo_sk` is the row, and the
/// other columns are its digits in a mixed radix, gender fastest, then
/// marital status, education, purchase band, credit rating and the three
/// dependent counts (radix 7 each).
#[derive(Debug, Clone)]
struct Demographics {
    gender: Arc<[Word]>,
    marital: Arc<[Word]>,
    education: Arc<[Word]>,
    purchase: Arc<[i32]>,
    credit: Arc<[Word]>,
    /// 0-based row of the next sample.
    row: u32,
}

impl Demographics {
    /// Each digit's radix, fastest first.
    fn radix(&self) -> [u32; 8] {
        let n = |l: usize| l as u32;
        [
            n(self.gender.len()),
            n(self.marital.len()),
            n(self.education.len()),
            n(self.purchase.len()),
            n(self.credit.len()),
            7,
            7,
            7,
        ]
    }

    /// Digit `d` of rows `row..row + n`, counted up rather than divided
    /// out of each row.
    fn digits(&self, d: usize, n: usize) -> impl Iterator<Item = usize> + use<> {
        let radix = self.radix();
        let div: u32 = radix[..d].iter().product();
        let r = radix[d];
        let (mut v, mut left) = (self.row / div % r, div - self.row % div);
        (0..n).map(move |_| {
            let x = v;
            left -= 1;
            if left == 0 {
                left = div;
                v = if v + 1 == r { 0 } else { v + 1 };
            }
            x as usize
        })
    }
}

impl Distribution for Demographics {
    type Item = DemoRow;

    fn sample(&mut self, _: &mut impl Rng) -> DemoRow {
        let d = |k| self.digits(k, 1).next().unwrap_or(0);
        let row = (
            self.row as i32 + 1,
            Some(self.gender[d(0)]),
            Some(self.marital[d(1)]),
            Some(self.education[d(2)]),
            Some(self.purchase[d(3)]),
            Some(self.credit[d(4)]),
            Some(d(5) as i32),
            Some(d(6) as i32),
            Some(d(7) as i32),
        );
        self.row += 1;
        row
    }

    fn collect_into(&mut self, c: &mut Columns<DemoRow>, _: &mut impl Rng, n: usize) {
        let words = |list: &[Word], v: &mut StrViews, d| {
            Sink::<Word>::reserve(v, n);
            v.push_indexed(list, self.digits(d, n));
        };
        words(&self.gender, &mut c.1.values, 0);
        words(&self.marital, &mut c.2.values, 1);
        words(&self.education, &mut c.3.values, 2);
        words(&self.credit, &mut c.5.values, 4);
        c.0.extend((self.row..self.row + n as u32).map(|i| i as i32 + 1));
        c.4.values
            .extend(self.digits(3, n).map(|i| self.purchase[i]));
        for (v, d) in [
            (&mut c.6.values, 5),
            (&mut c.7.values, 6),
            (&mut c.8.values, 7),
        ] {
            v.extend(self.digits(d, n).map(|i| i as i32));
        }
        for valid in [
            &mut c.1.valid,
            &mut c.2.valid,
            &mut c.3.valid,
            &mut c.4.valid,
            &mut c.5.valid,
            &mut c.6.valid,
            &mut c.7.valid,
            &mut c.8.valid,
        ] {
            valid.push_valid(n);
        }
        self.row += n as u32;
    }

    fn skip(&mut self, n: u64) {
        self.row += n as u32;
    }
}

/// `customer_demographics`. The table has no nulls
/// ([`demographics_have_nulls`]), so no random draw reaches it.
pub(crate) fn customer_demographics() -> impl Distribution<Item = DemoRow> + Clone + Sync {
    let list = |n: usize, f: fn(i64) -> &'static str| -> Arc<[Word]> {
        (0..n as i64).map(|i| Word::new(f(i))).collect()
    };
    let demographics = Demographics {
        gender: list(Demo::get_gender_size(), Demo::get_gender_for_index_mod_size),
        marital: list(
            Demo::get_marital_status_size(),
            Demo::get_marital_status_for_index_mod_size,
        ),
        education: list(
            Demo::get_education_size(),
            Demo::get_education_for_index_mod_size,
        ),
        purchase: (0..Demo::get_purchase_band_size() as i64)
            .map(Demo::get_purchase_band_for_index_mod_size)
            .collect(),
        credit: list(
            Demo::get_credit_rating_size(),
            Demo::get_credit_rating_for_index_mod_size,
        ),
        row: 0,
    };
    named_all(
        &[
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
            "cd_purchase_estimate",
            "cd_credit_rating",
            "cd_dep_count",
            "cd_dep_employed_count",
            "cd_dep_college_count",
        ],
        demographics,
    )
}

/// Whether `customer_demographics` has nulls: dsdgen's configuration has
/// none, which [`customer_demographics`] relies on.
#[cfg(test)]
fn demographics_have_nulls() -> bool {
    NullTable::CustomerDemographics.get_null_basis_points() != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;
    use populate_sink_arrow::IntoArrowColumns;
    use populate_table::DistTable;
    use tpcdsgen::config::SessionBuilder;
    use tpcdsgen_arrow::{CustomerDemographicsArrow, InventoryArrow};

    fn session() -> Session {
        SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap()
    }

    fn concat(v: Vec<RecordBatch>) -> RecordBatch {
        concat_batches(&v[0].schema(), &v).unwrap()
    }

    /// Rows `first..=last` (1-based) of `t` in batches of 777, against
    /// `want`.
    fn check<D>(t: &DistTable<D>, first: u64, last: u64, want: Vec<RecordBatch>)
    where
        D: Distribution + Clone + Sync,
        Columns<D::Item>: IntoArrowColumns,
    {
        let t = t.clone().batch_rows(777);
        let mut got = Vec::new();
        t.range(first - 1, last, &mut |b| got.push(b));
        let (got, want) = (concat(got), concat(want));
        assert_eq!(got.schema(), want.schema());
        assert_eq!(got, want, "{first}..={last}");
    }

    #[test]
    fn inventory_matches_tpcdsgen_arrow() {
        let s = session();
        let t = DistTable::new(
            "inventory",
            s.get_scaling().get_row_count(Ds::Inventory),
            inventory(&s),
        );
        // the start, a warehouse and a week boundary, the end
        for (first, last) in [
            (1, 5000),
            (17_990, 18_010),
            (89_990, 90_020),
            (11_744_000, 11_745_000),
        ] {
            let want = InventoryArrow::new(s.clone())
                .with_source_row_range(first, last)
                .map(|b| b.unwrap())
                .collect();
            check(&t, first, last, want);
        }
    }

    #[test]
    fn demographics_match_tpcdsgen_arrow() {
        assert!(!demographics_have_nulls());
        let s = session();
        let rows = s.get_scaling().get_row_count(Ds::CustomerDemographics);
        let t = DistTable::new("customer_demographics", rows, customer_demographics());
        for (first, last) in [(1, 3000), (1_000_000, 1_003_000), (1_920_000, 1_920_800)] {
            let want = CustomerDemographicsArrow::new(s.clone())
                .with_source_row_range(first, last)
                .map(|b| b.unwrap())
                .collect();
            check(&t, first, last, want);
        }
    }
}
