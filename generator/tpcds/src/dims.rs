//! The dimension tables: each one distribution over tpcdsgen's row
//! generator for the table (dsdgen's logic), writing every row's fields
//! straight into the table's columns.
//!
//! The column types and their nulls are tpcdsgen-arrow's, which the tests
//! check them against: a key is NULL when its null-bitmap bit is set or it
//! is negative, a date is a Julian day (NULL when negative), text is
//! `Utf8View`, and an address is ten columns ([`Address`]).

use populate_combinators::text::{Gmt, PushText, Str};
use populate_builtins::{ReadRows, named_all};
use populate_core::{Columnar, Columns, Date32, Decimal as Fixed, Distribution, Sink};
use rand::Rng;
use std::borrow::Borrow;
use std::io::Write as _;
use std::marker::PhantomData;
use tpcdsgen::config::{Session, Table as Ds};
use tpcdsgen::row::*;
use tpcdsgen::types::{Address as DsAddress, Date, Decimal};

/// Julian day of 1970-01-01, Arrow's day 0.
const UNIX_EPOCH_JULIAN: i32 = 2_440_588;

/// Whether the field at bit `pos` of a row's null bitmap has a value.
#[inline]
fn valid(nbm: i64, pos: u32) -> bool {
    nbm >> pos & 1 == 0
}

/// A surrogate key: NULL by the bitmap, or when negative.
#[inline]
fn sk(nbm: i64, pos: u32, v: i64) -> Option<i32> {
    (valid(nbm, pos) && v >= 0).then_some(v as i32)
}

#[inline]
fn int(nbm: i64, pos: u32, v: i32) -> Option<i32> {
    valid(nbm, pos).then_some(v)
}

/// A date from its Julian day: NULL by the bitmap, or when negative.
#[inline]
fn julian(nbm: i64, pos: u32, day: i64) -> Option<Date32> {
    (valid(nbm, pos) && day >= 0).then(|| Date32(day as i32 - UNIX_EPOCH_JULIAN))
}

#[inline]
fn date32(d: &Date) -> Date32 {
    Date32(d.to_julian_days() - UNIX_EPOCH_JULIAN)
}

#[inline]
fn dec<const P: u8, const S: i8>(
    nbm: i64,
    pos: u32,
    d: impl Borrow<Decimal>,
) -> Option<Fixed<P, S>> {
    valid(nbm, pos).then(|| Fixed(d.borrow().get_number() as i128))
}

/// dsdgen's flags.
#[inline]
fn yn(b: bool) -> &'static str {
    if b { "Y" } else { "N" }
}

/// An address: street number, street name, street type, suite number,
/// city, county, state, zip, country and GMT offset, each with its own
/// null bit.
pub(crate) type Address = (
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Gmt>,
);

/// Address `a`, its fields' null bits from `base` on.
fn push_address(c: &mut Columns<Address>, nbm: i64, base: u32, a: &DsAddress) {
    let ok = |k: u32| valid(nbm, base + k);
    let (number, name, kind, suite, city, county, state, zip, country, gmt) = c;
    // as `Address::get_street_name`, without allocating
    number.push_with(ok(0), |o| {
        write!(o, "{}", a.get_street_number()).expect("to memory")
    });
    name.push_with(ok(1), |o| {
        write!(o, "{} {}", a.get_street_name1(), a.get_street_name2()).expect("to memory")
    });
    kind.push_text(ok(2).then(|| a.get_street_type()));
    suite.push_text(ok(3).then(|| a.get_suite_number()));
    city.push_text(ok(4).then(|| a.get_city()));
    county.push_text(if ok(5) { a.get_county() } else { None });
    state.push_text(ok(6).then(|| a.get_state()));
    zip.push_with(ok(7), |o| {
        write!(o, "{:05}", a.get_zip()).expect("to memory")
    });
    country.push_text(ok(8).then(|| a.get_country()));
    gmt.push(ok(9).then(|| Gmt(a.get_gmt_offset())));
}

/// A tpcdsgen row generator.
pub(crate) trait RowGenerator: Iterator {
    /// The generator of a table of `rows` rows, at row `first` (1-based).
    fn open(session: Session, rows: u64, first: u64) -> Self;
}

macro_rules! row_generators {
    ($($g:ident),*) => {$(
        impl RowGenerator for $g {
            fn open(session: Session, rows: u64, first: u64) -> Self {
                let mut g = $g::new(session, rows);
                g.set_source_row_range(first, rows);
                g
            }
        }
    )*};
}

row_generators!(
    CallCenterRowGenerator,
    CatalogPageRowGenerator,
    CustomerRowGenerator,
    CustomerAddressRowGenerator,
    DateDimRowGenerator,
    HouseholdDemographicsRowGenerator,
    IncomeBandRowGenerator,
    ItemRowGenerator,
    PromotionRowGenerator,
    ReasonRowGenerator,
    ShipModeRowGenerator,
    StoreRowGenerator,
    TimeDimRowGenerator,
    WarehouseRowGenerator,
    WebPageRowGenerator,
    WebSiteRowGenerator
);

/// The rows of a tpcdsgen row generator `G`, each written into the
/// table's columns by `push`.
///
/// The generator is made on the first collect, at the distribution's row:
/// tpcdsgen's generators skip to any row themselves. A clone or a skip
/// starts over from a new one.
pub(crate) struct Dim<G, F, T> {
    session: Session,
    rows: u64,
    /// 0-based row of the next sample.
    at: u64,
    generator: Option<G>,
    push: F,
    _t: PhantomData<fn() -> T>,
}

impl<G, F: Clone, T> Clone for Dim<G, F, T> {
    fn clone(&self) -> Self {
        Dim {
            session: self.session.clone(),
            rows: self.rows,
            at: self.at,
            generator: None,
            push: self.push.clone(),
            _t: PhantomData,
        }
    }
}

/// Table `table`'s rows, made by `G` and written by `push`.
fn dim<G, T, F>(session: &Session, table: Ds, push: F) -> Dim<G, F, T>
where
    G: RowGenerator,
    T: Columnar,
    F: Fn(&G::Item, &mut Columns<T>),
{
    Dim {
        rows: session.get_scaling().get_row_count(table),
        session: session.clone(),
        at: 0,
        generator: None,
        push,
        _t: PhantomData,
    }
}

impl<G, F, T> Distribution for Dim<G, F, T>
where
    G: RowGenerator,
    T: Columnar,
    Columns<T>: ReadRows<Row = T>,
    F: Fn(&G::Item, &mut Columns<T>),
{
    type Item = T;

    fn sample(&mut self, rng: &mut impl Rng) -> T {
        let mut c = Columns::<T>::default();
        self.collect_into(&mut c, rng, 1);
        c.rows_from(0).next().expect("one row")
    }

    fn collect_into(&mut self, cols: &mut Columns<T>, _: &mut impl Rng, n: usize) {
        let (session, rows, at) = (&self.session, self.rows, self.at);
        let g = self
            .generator
            .get_or_insert_with(|| G::open(session.clone(), rows, at + 1));
        cols.reserve(n);
        for _ in 0..n {
            let row = g.next().expect("a row in the table");
            (self.push)(&row, cols);
        }
        self.at += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.at += n;
        self.generator = None;
    }
}

/// A `call_center` row.
pub(crate) type CallCenter = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<Date32>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
    ),
    (
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Address,
        Option<Fixed<5, 2>>,
    ),
);

/// `call_center`.
pub(crate) fn call_center(
    s: &Session,
) -> impl Distribution<Item = CallCenter> + Clone + Sync + use<> {
    named_all(
        &[
            "cc_call_center_sk",
            "cc_call_center_id",
            "cc_rec_start_date",
            "cc_rec_end_date",
            "cc_closed_date_sk",
            "cc_open_date_sk",
            "cc_name",
            "cc_class",
            "cc_employees",
            "cc_sq_ft",
            "cc_hours",
            "cc_manager",
            "cc_mkt_id",
            "cc_mkt_class",
            "cc_mkt_desc",
            "cc_market_manager",
            "cc_division",
            "cc_division_name",
            "cc_company",
            "cc_company_name",
            "cc_street_number",
            "cc_street_name",
            "cc_street_type",
            "cc_suite_number",
            "cc_city",
            "cc_county",
            "cc_state",
            "cc_zip",
            "cc_country",
            "cc_gmt_offset",
            "cc_tax_percentage",
        ],
        dim::<CallCenterRowGenerator, _, _>(
            s,
            Ds::CallCenter,
            |r: &CallCenterRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21),
            ): &mut Columns<CallCenter>| {
                let nbm = r.get_null_bit_map();
                c0.push(r.get_cc_call_center_sk() as i32);
                c1.push_str(r.get_cc_call_center_id());
                c2.push(julian(nbm, 2, r.get_cc_rec_start_date_id()));
                c3.push(julian(nbm, 3, r.get_cc_rec_end_date_id()));
                c4.push(sk(nbm, 4, r.get_cc_closed_date_id()));
                c5.push(sk(nbm, 5, r.get_cc_open_date_id()));
                c6.push_text(valid(nbm, 6).then(|| r.get_cc_name()));
                c7.push_text(valid(nbm, 7).then(|| r.get_cc_class()));
                c8.push(int(nbm, 8, r.get_cc_employees()));
                c9.push(int(nbm, 9, r.get_cc_sq_ft()));
                c10.push_text(valid(nbm, 10).then(|| r.get_cc_hours()));
                c11.push_text(valid(nbm, 11).then(|| r.get_cc_manager()));
                c12.push(int(nbm, 12, r.get_cc_market_id()));
                c13.push_text(valid(nbm, 13).then(|| r.get_cc_market_class()));
                c14.push_text(valid(nbm, 14).then(|| r.get_cc_market_desc()));
                c15.push_text(valid(nbm, 15).then(|| r.get_cc_market_manager()));
                c16.push(int(nbm, 16, r.get_cc_division_id()));
                c17.push_text(valid(nbm, 17).then(|| r.get_cc_division_name()));
                c18.push(int(nbm, 18, r.get_cc_company()));
                c19.push_text(valid(nbm, 19).then(|| r.get_cc_company_name()));
                push_address(c20, nbm, 20, r.get_cc_address());
                c21.push(dec(nbm, 30, *r.get_cc_tax_percentage()));
            },
        ),
    )
}

/// A `catalog_page` row.
pub(crate) type CatalogPage = (
    i32,
    Str,
    Option<i32>,
    Option<i32>,
    Option<Str>,
    Option<i32>,
    Option<i32>,
    Option<Str>,
    Option<Str>,
);

/// `catalog_page`.
pub(crate) fn catalog_page(
    s: &Session,
) -> impl Distribution<Item = CatalogPage> + Clone + Sync + use<> {
    named_all(
        &[
            "cp_catalog_page_sk",
            "cp_catalog_page_id",
            "cp_start_date_sk",
            "cp_end_date_sk",
            "cp_department",
            "cp_catalog_number",
            "cp_catalog_page_number",
            "cp_description",
            "cp_type",
        ],
        dim::<CatalogPageRowGenerator, _, _>(
            s,
            Ds::CatalogPage,
            |r: &CatalogPageRow,
             (c0, c1, c2, c3, c4, c5, c6, c7, c8): &mut Columns<CatalogPage>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_cp_catalog_page_sk() as i32);
                c1.push_str(r.get_cp_catalog_page_id());
                c2.push(sk(nbm, 2, r.get_cp_start_date_id()));
                c3.push(sk(nbm, 3, r.get_cp_end_date_id()));
                c4.push_text(valid(nbm, 5).then(|| r.get_cp_department()));
                c5.push(int(nbm, 6, r.get_cp_catalog_number()));
                c6.push(int(nbm, 7, r.get_cp_catalog_page_number()));
                c7.push_text(valid(nbm, 8).then(|| r.get_cp_description()));
                c8.push_text(valid(nbm, 9).then(|| r.get_cp_type()));
            },
        ),
    )
}

/// A `customer` row.
pub(crate) type Customer = (
    (
        i32,
        Str,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
    ),
    (
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
    ),
);

/// `customer`.
pub(crate) fn customer(s: &Session) -> impl Distribution<Item = Customer> + Clone + Sync + use<> {
    named_all(
        &[
            "c_customer_sk",
            "c_customer_id",
            "c_current_cdemo_sk",
            "c_current_hdemo_sk",
            "c_current_addr_sk",
            "c_first_shipto_date_sk",
            "c_first_sales_date_sk",
            "c_salutation",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
            "c_birth_day",
            "c_birth_month",
            "c_birth_year",
            "c_birth_country",
            "c_login",
            "c_email_address",
            "c_last_review_date_sk",
        ],
        dim::<CustomerRowGenerator, _, _>(
            s,
            Ds::Customer,
            |r: &CustomerRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17),
            ): &mut Columns<Customer>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_c_customer_sk() as i32);
                c1.push_str(r.get_c_customer_id());
                c2.push(sk(nbm, 2, r.get_c_current_cdemo_sk()));
                c3.push(sk(nbm, 3, r.get_c_current_hdemo_sk()));
                c4.push(sk(nbm, 4, r.get_c_current_addr_sk()));
                c5.push(int(nbm, 5, r.get_c_first_shipto_date_id()));
                c6.push(int(nbm, 6, r.get_c_first_sales_date_id()));
                c7.push_text(valid(nbm, 7).then(|| r.get_c_salutation()));
                c8.push_text(valid(nbm, 8).then(|| r.get_c_first_name()));
                c9.push_text(valid(nbm, 9).then(|| r.get_c_last_name()));
                c10.push_text(valid(nbm, 10).then(|| yn(r.get_c_preferred_cust_flag())));
                c11.push(int(nbm, 11, r.get_c_birth_day()));
                c12.push(int(nbm, 12, r.get_c_birth_month()));
                c13.push(int(nbm, 13, r.get_c_birth_year()));
                c14.push_text(valid(nbm, 14).then(|| r.get_c_birth_country()));
                c15.push_text(None);
                c16.push_text(valid(nbm, 16).then(|| r.get_c_email_address()));
                c17.push(int(nbm, 17, r.get_c_last_review_date()));
            },
        ),
    )
}

/// A `customer_address` row.
pub(crate) type CustomerAddress = (i32, Str, Address, Option<Str>);

/// `customer_address`.
pub(crate) fn customer_address(
    s: &Session,
) -> impl Distribution<Item = CustomerAddress> + Clone + Sync + use<> {
    named_all(
        &[
            "ca_address_sk",
            "ca_address_id",
            "ca_street_number",
            "ca_street_name",
            "ca_street_type",
            "ca_suite_number",
            "ca_city",
            "ca_county",
            "ca_state",
            "ca_zip",
            "ca_country",
            "ca_gmt_offset",
            "ca_location_type",
        ],
        dim::<CustomerAddressRowGenerator, _, _>(
            s,
            Ds::CustomerAddress,
            |r: &CustomerAddressRow, (c0, c1, c2, c3): &mut Columns<CustomerAddress>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_ca_addr_sk() as i32);
                c1.push_str(r.get_ca_addr_id());
                push_address(c2, nbm, 2, r.get_ca_address());
                c3.push_text(valid(nbm, 12).then(|| r.get_ca_location_type()));
            },
        ),
    )
}

/// A `date_dim` row.
pub(crate) type DateDim = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
    ),
    (
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
    ),
    (
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
    ),
);

/// `date_dim`.
pub(crate) fn date_dim(s: &Session) -> impl Distribution<Item = DateDim> + Clone + Sync + use<> {
    named_all(
        &[
            "d_date_sk",
            "d_date_id",
            "d_date",
            "d_month_seq",
            "d_week_seq",
            "d_quarter_seq",
            "d_year",
            "d_dow",
            "d_moy",
            "d_dom",
            "d_qoy",
            "d_fy_year",
            "d_fy_quarter_seq",
            "d_fy_week_seq",
            "d_day_name",
            "d_quarter_name",
            "d_holiday",
            "d_weekend",
            "d_following_holiday",
            "d_first_dom",
            "d_last_dom",
            "d_same_day_ly",
            "d_same_day_lq",
            "d_current_day",
            "d_current_week",
            "d_current_month",
            "d_current_quarter",
            "d_current_year",
        ],
        dim::<DateDimRowGenerator, _, _>(
            s,
            Ds::DateDim,
            |r: &DateDimRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21),
                (c22, c23, c24, c25, c26, c27),
            ): &mut Columns<DateDim>| {
                let _nbm = r.null_bit_map();
                c0.push((r.d_date_sk) as i32);
                c1.push_str(r.d_date_id.as_str());
                c2.push(Some(date32(&r.d_date)));
                c3.push(Some(r.d_month_seq));
                c4.push(Some(r.d_week_seq));
                c5.push(Some(r.d_quarter_seq));
                c6.push(Some(r.d_year));
                c7.push(Some(r.d_dow));
                c8.push(Some(r.d_moy));
                c9.push(Some(r.d_dom));
                c10.push(Some(r.d_qoy));
                c11.push(Some(r.d_fy_year));
                c12.push(Some(r.d_fy_quarter_seq));
                c13.push(Some(r.d_fy_week_seq));
                c14.push_text(Some(r.d_day_name.as_str()));
                c15.push_text(Some(r.d_quarter_name.as_str()));
                c16.push_text(Some(yn(r.d_holiday)));
                c17.push_text(Some(yn(r.d_weekend)));
                c18.push_text(Some(yn(r.d_following_holiday)));
                c19.push(Some(r.d_first_dom));
                c20.push(Some(r.d_last_dom));
                c21.push(Some(r.d_same_day_ly));
                c22.push(Some(r.d_same_day_lq));
                c23.push_text(Some(yn(r.d_current_day)));
                c24.push_text(Some(yn(r.d_current_week)));
                c25.push_text(Some(yn(r.d_current_month)));
                c26.push_text(Some(yn(r.d_current_quarter)));
                c27.push_text(Some(yn(r.d_current_year)));
            },
        ),
    )
}

/// A `household_demographics` row.
pub(crate) type HouseholdDemographics = (i32, Option<i32>, Option<Str>, Option<i32>, Option<i32>);

/// `household_demographics`.
pub(crate) fn household_demographics(
    s: &Session,
) -> impl Distribution<Item = HouseholdDemographics> + Clone + Sync + use<> {
    named_all(
        &[
            "hd_demo_sk",
            "hd_income_band_sk",
            "hd_buy_potential",
            "hd_dep_count",
            "hd_vehicle_count",
        ],
        dim::<HouseholdDemographicsRowGenerator, _, _>(
            s,
            Ds::HouseholdDemographics,
            |r: &HouseholdDemographicsRow,
             (c0, c1, c2, c3, c4): &mut Columns<HouseholdDemographics>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_hd_demo_sk() as i32);
                c1.push(sk(nbm, 1, r.get_hd_income_band_sk()));
                c2.push_text(valid(nbm, 2).then(|| r.get_hd_buy_potential()));
                c3.push(int(nbm, 3, r.get_hd_dep_count()));
                c4.push(int(nbm, 4, r.get_hd_vehicle_count()));
            },
        ),
    )
}

/// A `income_band` row.
pub(crate) type IncomeBand = (i32, Option<i32>, Option<i32>);

/// `income_band`.
pub(crate) fn income_band(
    s: &Session,
) -> impl Distribution<Item = IncomeBand> + Clone + Sync + use<> {
    named_all(
        &["ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"],
        dim::<IncomeBandRowGenerator, _, _>(
            s,
            Ds::IncomeBand,
            |r: &IncomeBandRow, (c0, c1, c2): &mut Columns<IncomeBand>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_ib_income_band_sk());
                c1.push(int(nbm, 1, r.get_ib_lower_bound()));
                c2.push(int(nbm, 2, r.get_ib_upper_bound()));
            },
        ),
    )
}

/// A `item` row.
pub(crate) type Item = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<Date32>,
        Option<Str>,
        Option<Fixed<7, 2>>,
        Option<Fixed<7, 2>>,
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
    ),
    (
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
    ),
);

/// `item`.
pub(crate) fn item(s: &Session) -> impl Distribution<Item = Item> + Clone + Sync + use<> {
    named_all(
        &[
            "i_item_sk",
            "i_item_id",
            "i_rec_start_date",
            "i_rec_end_date",
            "i_item_desc",
            "i_current_price",
            "i_wholesale_cost",
            "i_brand_id",
            "i_brand",
            "i_class_id",
            "i_class",
            "i_category_id",
            "i_category",
            "i_manufact_id",
            "i_manufact",
            "i_size",
            "i_formulation",
            "i_color",
            "i_units",
            "i_container",
            "i_manager_id",
            "i_product_name",
        ],
        dim::<ItemRowGenerator, _, _>(
            s,
            Ds::Item,
            |r: &ItemRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21),
            ): &mut Columns<Item>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_i_item_sk() as i32);
                c1.push_str(r.get_i_item_id());
                c2.push(julian(nbm, 2, r.get_i_rec_start_date_id()));
                c3.push(julian(0, 3, r.get_i_rec_end_date_id()));
                c4.push_text(valid(nbm, 4).then(|| r.get_i_item_desc()));
                c5.push(dec(nbm, 5, r.get_i_current_price()));
                c6.push(dec(nbm, 6, r.get_i_wholesale_cost()));
                c7.push(int(
                    nbm,
                    7,
                    i32::try_from(r.get_i_brand_id()).expect("INTEGER"),
                ));
                c8.push_text(valid(nbm, 8).then(|| r.get_i_brand()));
                c9.push(int(
                    nbm,
                    9,
                    i32::try_from(r.get_i_class_id()).expect("INTEGER"),
                ));
                c10.push_text(valid(nbm, 10).then(|| r.get_i_class()));
                c11.push(int(
                    nbm,
                    11,
                    i32::try_from(r.get_i_category_id()).expect("INTEGER"),
                ));
                c12.push_text(valid(nbm, 12).then(|| r.get_i_category()));
                c13.push(int(
                    nbm,
                    13,
                    i32::try_from(r.get_i_manufact_id()).expect("INTEGER"),
                ));
                c14.push_text(valid(nbm, 14).then(|| r.get_i_manufact()));
                c15.push_text(valid(nbm, 15).then(|| r.get_i_size()));
                c16.push_text(valid(nbm, 16).then(|| r.get_i_formulation()));
                c17.push_text(valid(nbm, 17).then(|| r.get_i_color()));
                c18.push_text(valid(nbm, 18).then(|| r.get_i_units()));
                c19.push_text(valid(nbm, 19).then(|| r.get_i_container()));
                c20.push(int(
                    nbm,
                    20,
                    i32::try_from(r.get_i_manager_id()).expect("INTEGER"),
                ));
                c21.push_text(valid(nbm, 21).then(|| r.get_i_product_name()));
            },
        ),
    )
}

/// A `promotion` row.
pub(crate) type Promotion = (
    (
        i32,
        Str,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<Fixed<15, 2>>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
    ),
    (
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<Str>,
    ),
);

/// `promotion`.
pub(crate) fn promotion(s: &Session) -> impl Distribution<Item = Promotion> + Clone + Sync + use<> {
    named_all(
        &[
            "p_promo_sk",
            "p_promo_id",
            "p_start_date_sk",
            "p_end_date_sk",
            "p_item_sk",
            "p_cost",
            "p_response_target",
            "p_promo_name",
            "p_channel_dmail",
            "p_channel_email",
            "p_channel_catalog",
            "p_channel_tv",
            "p_channel_radio",
            "p_channel_press",
            "p_channel_event",
            "p_channel_demo",
            "p_channel_details",
            "p_purpose",
            "p_discount_active",
        ],
        dim::<PromotionRowGenerator, _, _>(
            s,
            Ds::Promotion,
            |r: &PromotionRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17, c18),
            ): &mut Columns<Promotion>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_p_promo_sk() as i32);
                c1.push_str(r.get_p_promo_id());
                c2.push(sk(nbm, 2, r.get_p_start_date_id()));
                c3.push(sk(nbm, 3, r.get_p_end_date_id()));
                c4.push(sk(nbm, 4, r.get_p_item_sk()));
                c5.push(dec(nbm, 5, r.get_p_cost()));
                c6.push(int(nbm, 6, r.get_p_response_target()));
                c7.push_text(valid(nbm, 7).then(|| r.get_p_promo_name()));
                c8.push_text(valid(nbm, 8).then(|| yn(r.get_p_channel_dmail())));
                c9.push_text(valid(nbm, 9).then(|| yn(r.get_p_channel_email())));
                c10.push_text(valid(nbm, 10).then(|| yn(r.get_p_channel_catalog())));
                c11.push_text(valid(nbm, 11).then(|| yn(r.get_p_channel_tv())));
                c12.push_text(valid(nbm, 12).then(|| yn(r.get_p_channel_radio())));
                c13.push_text(valid(nbm, 13).then(|| yn(r.get_p_channel_press())));
                c14.push_text(valid(nbm, 14).then(|| yn(r.get_p_channel_event())));
                c15.push_text(valid(nbm, 15).then(|| yn(r.get_p_channel_demo())));
                c16.push_text(valid(nbm, 16).then(|| r.get_p_channel_details()));
                c17.push_text(valid(nbm, 17).then(|| r.get_p_purpose()));
                c18.push_text(valid(nbm, 18).then(|| yn(r.get_p_discount_active())));
            },
        ),
    )
}

/// A `reason` row.
pub(crate) type Reason = (i32, Str, Option<Str>);

/// `reason`.
pub(crate) fn reason(s: &Session) -> impl Distribution<Item = Reason> + Clone + Sync + use<> {
    named_all(
        &["r_reason_sk", "r_reason_id", "r_reason_desc"],
        dim::<ReasonRowGenerator, _, _>(
            s,
            Ds::Reason,
            |r: &ReasonRow, (c0, c1, c2): &mut Columns<Reason>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_r_reason_sk() as i32);
                c1.push_str(r.get_r_reason_id());
                c2.push_text(valid(nbm, 2).then(|| r.get_r_reason_desc()));
            },
        ),
    )
}

/// A `ship_mode` row.
pub(crate) type ShipMode = (i32, Str, Option<Str>, Option<Str>, Option<Str>, Option<Str>);

/// `ship_mode`.
pub(crate) fn ship_mode(s: &Session) -> impl Distribution<Item = ShipMode> + Clone + Sync + use<> {
    named_all(
        &[
            "sm_ship_mode_sk",
            "sm_ship_mode_id",
            "sm_type",
            "sm_code",
            "sm_carrier",
            "sm_contract",
        ],
        dim::<ShipModeRowGenerator, _, _>(
            s,
            Ds::ShipMode,
            |r: &ShipModeRow, (c0, c1, c2, c3, c4, c5): &mut Columns<ShipMode>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_sm_ship_mode_sk() as i32);
                c1.push_str(r.get_sm_ship_mode_id());
                c2.push_text(valid(nbm, 2).then(|| r.get_sm_type()));
                c3.push_text(valid(nbm, 3).then(|| r.get_sm_code()));
                c4.push_text(valid(nbm, 4).then(|| r.get_sm_carrier()));
                c5.push_text(valid(nbm, 5).then(|| r.get_sm_contract()));
            },
        ),
    )
}

/// A `store` row.
pub(crate) type Store = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<Date32>,
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
    ),
    (
        Option<Str>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Address,
        Option<Fixed<5, 2>>,
    ),
);

/// `store`.
pub(crate) fn store(s: &Session) -> impl Distribution<Item = Store> + Clone + Sync + use<> {
    named_all(
        &[
            "s_store_sk",
            "s_store_id",
            "s_rec_start_date",
            "s_rec_end_date",
            "s_closed_date_sk",
            "s_store_name",
            "s_number_employees",
            "s_floor_space",
            "s_hours",
            "s_manager",
            "s_market_id",
            "s_geography_class",
            "s_market_desc",
            "s_market_manager",
            "s_division_id",
            "s_division_name",
            "s_company_id",
            "s_company_name",
            "s_street_number",
            "s_street_name",
            "s_street_type",
            "s_suite_number",
            "s_city",
            "s_county",
            "s_state",
            "s_zip",
            "s_country",
            "s_gmt_offset",
            "s_tax_precentage",
        ],
        dim::<StoreRowGenerator, _, _>(
            s,
            Ds::Store,
            |r: &StoreRow,
             (
                (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10),
                (c11, c12, c13, c14, c15, c16, c17, c18, c19),
            ): &mut Columns<Store>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_store_sk() as i32);
                c1.push_str(r.get_store_id());
                c2.push(julian(nbm, 2, r.get_rec_start_date_id()));
                c3.push(julian(0, 3, r.get_rec_end_date_id()));
                c4.push(sk(nbm, 4, r.get_closed_date_id()));
                c5.push_text(valid(nbm, 5).then(|| r.get_store_name()));
                c6.push(int(nbm, 6, r.get_employees()));
                c7.push(int(nbm, 7, r.get_floor_space()));
                c8.push_text(valid(nbm, 8).then(|| r.get_hours()));
                c9.push_text(valid(nbm, 9).then(|| r.get_store_manager()));
                c10.push(int(nbm, 10, r.get_market_id()));
                c11.push_text(valid(nbm, 12).then(|| r.get_geography_class()));
                c12.push_text(valid(nbm, 13).then(|| r.get_market_desc()));
                c13.push_text(valid(nbm, 14).then(|| r.get_market_manager()));
                c14.push(int(
                    nbm,
                    15,
                    i32::try_from(r.get_division_id()).expect("INTEGER"),
                ));
                c15.push_text(valid(nbm, 16).then(|| r.get_division_name()));
                c16.push(int(
                    nbm,
                    17,
                    i32::try_from(r.get_company_id()).expect("INTEGER"),
                ));
                c17.push_text(valid(nbm, 18).then(|| r.get_company_name()));
                push_address(c18, nbm, 19, r.get_address());
                c19.push(dec(nbm, 11, r.get_d_tax_percentage()));
            },
        ),
    )
}

/// A `time_dim` row.
pub(crate) type TimeDim = (
    i32,
    Str,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<Str>,
);

/// `time_dim`.
pub(crate) fn time_dim(s: &Session) -> impl Distribution<Item = TimeDim> + Clone + Sync + use<> {
    named_all(
        &[
            "t_time_sk",
            "t_time_id",
            "t_time",
            "t_hour",
            "t_minute",
            "t_second",
            "t_am_pm",
            "t_shift",
            "t_sub_shift",
            "t_meal_time",
        ],
        dim::<TimeDimRowGenerator, _, _>(
            s,
            Ds::TimeDim,
            |r: &TimeDimRow, (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9): &mut Columns<TimeDim>| {
                let nbm = r.null_bit_map();
                c0.push((r.t_time_sk) as i32);
                c1.push_str(r.t_time_id.as_str());
                c2.push(int(nbm, 2, r.t_time));
                c3.push(int(nbm, 3, r.t_hour));
                c4.push(int(nbm, 4, r.t_minute));
                c5.push(int(nbm, 5, r.t_second));
                c6.push_text(valid(nbm, 6).then(|| r.t_am_pm.as_str()));
                c7.push_text(valid(nbm, 7).then(|| r.t_shift.as_str()));
                c8.push_text(valid(nbm, 8).then(|| r.t_sub_shift.as_str()));
                c9.push_text((!r.t_meal_time.is_empty()).then_some(r.t_meal_time.as_str()));
            },
        ),
    )
}

/// A `warehouse` row.
pub(crate) type Warehouse = (i32, Str, Option<Str>, Option<i32>, Address);

/// `warehouse`.
pub(crate) fn warehouse(s: &Session) -> impl Distribution<Item = Warehouse> + Clone + Sync + use<> {
    named_all(
        &[
            "w_warehouse_sk",
            "w_warehouse_id",
            "w_warehouse_name",
            "w_warehouse_sq_ft",
            "w_street_number",
            "w_street_name",
            "w_street_type",
            "w_suite_number",
            "w_city",
            "w_county",
            "w_state",
            "w_zip",
            "w_country",
            "w_gmt_offset",
        ],
        dim::<WarehouseRowGenerator, _, _>(
            s,
            Ds::Warehouse,
            |r: &WarehouseRow, (c0, c1, c2, c3, c4): &mut Columns<Warehouse>| {
                let nbm = r.null_bit_map();
                c0.push(r.get_w_warehouse_sk() as i32);
                c1.push_str(r.get_w_warehouse_id());
                c2.push_text(valid(nbm, 2).then(|| r.get_w_warehouse_name()));
                c3.push(int(nbm, 3, r.get_w_warehouse_sq_ft()));
                push_address(c4, nbm, 4, r.get_w_address());
            },
        ),
    )
}

/// A `web_page` row.
pub(crate) type WebPage = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<Date32>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
    ),
    (Option<i32>, Option<i32>, Option<i32>),
);

/// `web_page`.
pub(crate) fn web_page(s: &Session) -> impl Distribution<Item = WebPage> + Clone + Sync + use<> {
    named_all(
        &[
            "wp_web_page_sk",
            "wp_web_page_id",
            "wp_rec_start_date",
            "wp_rec_end_date",
            "wp_creation_date_sk",
            "wp_access_date_sk",
            "wp_autogen_flag",
            "wp_customer_sk",
            "wp_url",
            "wp_type",
            "wp_char_count",
            "wp_link_count",
            "wp_image_count",
            "wp_max_ad_count",
        ],
        dim::<WebPageRowGenerator, _, _>(
            s,
            Ds::WebPage,
            |r: &WebPageRow,
             ((c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), (c11, c12, c13)): &mut Columns<
                WebPage,
            >| {
                let nbm = r.null_bit_map();
                c0.push(r.get_wp_page_sk() as i32);
                c1.push_str(r.get_wp_page_id());
                c2.push(julian(nbm, 2, r.get_wp_rec_start_date_id()));
                c3.push(julian(0, 3, r.get_wp_rec_end_date_id()));
                c4.push(sk(nbm, 4, r.get_wp_creation_date_sk()));
                c5.push(sk(nbm, 5, r.get_wp_access_date_sk()));
                c6.push_text(valid(nbm, 6).then(|| yn(r.get_wp_autogen_flag())));
                c7.push(sk(nbm, 7, r.get_wp_customer_sk()));
                c8.push_text(valid(nbm, 8).then(|| r.get_wp_url()));
                c9.push_text(valid(nbm, 9).then(|| r.get_wp_type()));
                c10.push(int(nbm, 10, r.get_wp_char_count()));
                c11.push(int(nbm, 11, r.get_wp_link_count()));
                c12.push(int(nbm, 12, r.get_wp_image_count()));
                c13.push(int(nbm, 13, r.get_wp_max_ad_count()));
            },
        ),
    )
}

/// A `web_site` row.
pub(crate) type WebSite = (
    (
        i32,
        Str,
        Option<Date32>,
        Option<Date32>,
        Option<Str>,
        Option<i32>,
        Option<i32>,
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
    ),
    (
        Option<Str>,
        Option<Str>,
        Option<i32>,
        Option<Str>,
        Address,
        Option<Fixed<5, 2>>,
    ),
);

/// `web_site`.
pub(crate) fn web_site(s: &Session) -> impl Distribution<Item = WebSite> + Clone + Sync + use<> {
    named_all(
        &[
            "web_site_sk",
            "web_site_id",
            "web_rec_start_date",
            "web_rec_end_date",
            "web_name",
            "web_open_date_sk",
            "web_close_date_sk",
            "web_class",
            "web_manager",
            "web_mkt_id",
            "web_mkt_class",
            "web_mkt_desc",
            "web_market_manager",
            "web_company_id",
            "web_company_name",
            "web_street_number",
            "web_street_name",
            "web_street_type",
            "web_suite_number",
            "web_city",
            "web_county",
            "web_state",
            "web_zip",
            "web_country",
            "web_gmt_offset",
            "web_tax_percentage",
        ],
        dim::<WebSiteRowGenerator, _, _>(
            s,
            Ds::WebSite,
            |r: &WebSiteRow, ((c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), (c11, c12, c13, c14, c15, c16)): &mut Columns<WebSite>| {
            let nbm = r.null_bit_map();
            c0.push(r.get_web_site_sk() as i32);
            c1.push_str(r.get_web_site_id());
            c2.push(julian(nbm, 2, r.get_web_rec_start_date_id()));
            c3.push(julian(0, 3, r.get_web_rec_end_date_id()));
            c4.push_text(valid(nbm, 4).then(|| r.web_name()));
            c5.push(sk(nbm, 5, r.web_open_date()));
            c6.push(sk(nbm, 6, r.web_close_date()));
            c7.push_text(valid(nbm, 7).then(|| r.web_class()));
            c8.push_text(valid(nbm, 8).then(|| r.web_manager()));
            c9.push(int(nbm, 9, r.web_market_id()));
            c10.push_text(valid(nbm, 10).then(|| r.web_market_class()));
            c11.push_text(valid(nbm, 11).then(|| r.web_market_desc()));
            c12.push_text(valid(nbm, 12).then(|| r.web_market_manager()));
            c13.push(int(nbm, 13, r.web_company_id()));
            c14.push_text(valid(nbm, 14).then(|| r.web_company_name()));
            push_address(c15, nbm, 15, r.web_address());
            c16.push(dec(nbm, 25, *r.web_tax_percentage()));
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;
    use populate_sink_arrow::IntoArrowColumns;
    use populate_table::DistTable;
    use tpcdsgen::config::SessionBuilder;
    use tpcdsgen_arrow::*;

    fn concat(v: &[RecordBatch]) -> RecordBatch {
        concat_batches(&v[0].schema(), v).unwrap()
    }

    /// The whole table in batches of 777 rows, against tpcdsgen-arrow.
    fn check<D>(t: &DistTable<D>, want: Vec<RecordBatch>)
    where
        D: Distribution + Clone + Sync,
        Columns<D::Item>: IntoArrowColumns,
    {
        let t = t.clone().batch_rows(777);
        let mut got = Vec::new();
        t.range(0, t.rows(), &mut |b| got.push(b));
        let (got, want) = (concat(&got), concat(&want));
        assert_eq!(got.schema(), want.schema(), "{}", t.name());
        assert_eq!(got.num_rows(), want.num_rows(), "{}", t.name());
        for (i, (g, w)) in got.columns().iter().zip(want.columns()).enumerate() {
            assert_eq!(
                g,
                w,
                "{} column {}",
                t.name(),
                want.schema().field(i).name()
            );
        }
    }

    macro_rules! dims_match {
        ($($t:ident => $arrow:ident, $ds:ident;)*) => {
            #[test]
            fn dims_match_tpcdsgen_arrow() {
                let s = SessionBuilder::new().with_scale_factor(1.0).build().unwrap();
                $(
                    let rows = s.get_scaling().get_row_count(Ds::$ds);
                    let t = DistTable::new(stringify!($t), rows, $t(&s));
                    let want = $arrow::new(s.clone()).map(|b| b.unwrap()).collect();
                    check(&t, want);
                )*
            }
        };
    }

    dims_match! {
        call_center => CallCenterArrow, CallCenter;
        catalog_page => CatalogPageArrow, CatalogPage;
        customer => CustomerArrow, Customer;
        customer_address => CustomerAddressArrow, CustomerAddress;
        date_dim => DateDimArrow, DateDim;
        household_demographics => HouseholdDemographicsArrow, HouseholdDemographics;
        income_band => IncomeBandArrow, IncomeBand;
        item => ItemArrow, Item;
        promotion => PromotionArrow, Promotion;
        reason => ReasonArrow, Reason;
        ship_mode => ShipModeArrow, ShipMode;
        store => StoreArrow, Store;
        time_dim => TimeDimArrow, TimeDim;
        warehouse => WarehouseArrow, Warehouse;
        web_page => WebPageArrow, WebPage;
        web_site => WebSiteArrow, WebSite;
    }
}
