//! DSB tables as populate distributions.
//!
//! Each table is one [`Distribution`]: a [`Rows`] leaf runs dsbgen's row
//! generator for the table and maps every row to a tuple of typed values,
//! and [`named_all`] names them. The Arrow schema is [`schema_of`] that
//! distribution: the types come from the values (`Option<_>` is a nullable
//! column, [`Str`] is `Utf8View`, [`Decimal<P, S>`] is `Decimal128(P, S)`,
//! [`Date32`] is `Date32`), the names from [`named_all`].
//!
//! [`schema_of`]: populate_sink_arrow::schema_of

use crate::TableName;
use crate::dat::Encoding;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use dsbgen::config::{Session, Table as Ds};
use dsbgen::distribution::DemographicsDistributions as Demo;
use dsbgen::generator::InventoryGeneratorColumn as Inv;
use dsbgen::random::RandomNumberStream;
use dsbgen::row::*;
use dsbgen::slowly_changing_dimension_utils::match_surrogate_key;
use dsbgen::table::Table as NullTable;
use dsbgen::types::Date;
use dsbgen::types::{Address, Decimal as DsDecimal};
use populate_builtins::{derive_columns, named_all, range};
use populate_combinators::lehmer::{A, M, mulmod};
use populate_combinators::text::{Gmt, Str, StrViews, Word};
use populate_core::{Children, Columnar, Columns, Date32, Decimal, Distribution, Nullable, Sink};
use populate_sink_arrow::{IntoArrowColumns, IntoArrowTables, into_record_batch_with};
use populate_table::text::{TextColumns, TextTables, write_columns};
use populate_table::{DistTable, DistTables};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::fmt::Write as _;
use std::iter::repeat_n;
use std::marker::PhantomData;
use std::sync::Arc;

// ============================================================================
// Values: dsbgen's fields and null bitmap, as dsbgen-arrow reads them
// ============================================================================

/// Julian day of 1970-01-01.
const UNIX_EPOCH_JULIAN: i64 = 2_440_588;

/// The row's null bit `pos` is set.
#[inline(always)]
fn null(nbm: i64, pos: u32) -> bool {
    (nbm >> pos) & 1 != 0
}

#[inline(always)]
fn valid<T>(nbm: i64, pos: u32, v: T) -> Option<T> {
    (!null(nbm, pos)).then_some(v)
}

/// A surrogate key: NULL if its bit is set or it is negative.
#[inline(always)]
fn sk(nbm: i64, pos: u32, v: i64) -> Option<i32> {
    (!null(nbm, pos) && v >= 0).then_some(v as i32)
}

#[inline(always)]
fn int(nbm: i64, pos: u32, v: i32) -> Option<i32> {
    valid(nbm, pos, v)
}

/// An `INTEGER` held as `i64`.
#[inline(always)]
fn int_of(nbm: i64, pos: u32, v: i64) -> Option<i32> {
    valid(nbm, pos, i32::try_from(v).expect("DSB INTEGER fits i32"))
}

#[inline(always)]
fn text(nbm: i64, pos: u32, s: &str) -> Option<Str> {
    (!null(nbm, pos)).then(|| Str::from(s))
}

fn yes_no(b: bool) -> Str {
    Str::from(if b { "Y" } else { "N" })
}

#[inline(always)]
fn flag(nbm: i64, pos: u32, b: bool) -> Option<Str> {
    (!null(nbm, pos)).then(|| yes_no(b))
}

/// A date from its Julian day: NULL if its bit is set or the day is negative.
#[inline(always)]
fn julian(nbm: i64, pos: u32, day: i64) -> Option<Date32> {
    (!null(nbm, pos) && day >= 0).then(|| Date32((day - UNIX_EPOCH_JULIAN) as i32))
}

#[inline(always)]
fn dec<const P: u8, const S: i8>(nbm: i64, pos: u32, d: DsDecimal) -> Option<Decimal<P, S>> {
    valid(nbm, pos, Decimal(i128::from(d.get_number())))
}

/// The ten columns of an address, whose null bits start at `base`.
mod addr {
    use super::*;

    pub(super) fn street_number(n: i64, base: u32, a: &Address) -> Option<Str> {
        (!null(n, base)).then(|| Str(a.get_street_number().to_string()))
    }
    pub(super) fn street_name(n: i64, base: u32, a: &Address) -> Option<Str> {
        (!null(n, base + 1)).then(|| {
            let mut s = String::new();
            write!(s, "{} {}", a.get_street_name1(), a.get_street_name2()).expect("a String");
            Str(s)
        })
    }
    pub(super) fn street_type(n: i64, base: u32, a: &Address) -> Option<Str> {
        text(n, base + 2, a.get_street_type())
    }
    pub(super) fn suite_number(n: i64, base: u32, a: &Address) -> Option<Str> {
        text(n, base + 3, a.get_suite_number())
    }
    pub(super) fn city(n: i64, base: u32, a: &Address) -> Option<Str> {
        text(n, base + 4, a.get_city())
    }
    pub(super) fn county(n: i64, base: u32, a: &Address) -> Option<Str> {
        a.get_county().filter(|_| !null(n, base + 5)).map(Str::from)
    }
    pub(super) fn state(n: i64, base: u32, a: &Address) -> Option<Str> {
        text(n, base + 6, a.get_state())
    }
    pub(super) fn zip(n: i64, base: u32, a: &Address) -> Option<Str> {
        (!null(n, base + 7)).then(|| Str(format!("{:05}", a.get_zip())))
    }
    pub(super) fn country(n: i64, base: u32, a: &Address) -> Option<Str> {
        text(n, base + 8, a.get_country())
    }
    pub(super) fn gmt_offset(n: i64, base: u32, a: &Address) -> Option<Gmt> {
        valid(n, base + 9, Gmt(a.get_gmt_offset()))
    }
}

// ============================================================================
// Rows: a dsbgen row generator as a distribution
// ============================================================================

/// A dsbgen row generator that can start at any source row.
pub(crate) trait Source: Iterator {
    /// The generator for source rows `first..=last` (1-based) of its table.
    fn open(session: &Session, first: u64, last: u64) -> Self;
}

macro_rules! sources {
    ($($gen:ident => $t:ident),* $(,)?) => {
        $(impl Source for $gen {
            fn open(session: &Session, first: u64, last: u64) -> Self {
                let rows = session.get_scaling().get_row_count(Ds::$t);
                let mut g = $gen::new(session.clone(), rows);
                g.set_source_row_range(first, last.min(rows));
                g
            }
        })*
    };
}

sources!(
    CallCenterRowGenerator => CallCenter,
    CatalogPageRowGenerator => CatalogPage,
    CustomerRowGenerator => Customer,
    CustomerAddressRowGenerator => CustomerAddress,
    CustomerDemographicsRowGenerator => CustomerDemographics,
    DateDimRowGenerator => DateDim,
    HouseholdDemographicsRowGenerator => HouseholdDemographics,
    IncomeBandRowGenerator => IncomeBand,
    InventoryRowGenerator => Inventory,
    ItemRowGenerator => Item,
    PromotionRowGenerator => Promotion,
    ReasonRowGenerator => Reason,
    ShipModeRowGenerator => ShipMode,
    StoreRowGenerator => Store,
    TimeDimRowGenerator => TimeDim,
    WarehouseRowGenerator => Warehouse,
    WebPageRowGenerator => WebPage,
    WebSiteRowGenerator => WebSite,
    StoreSalesRowGenerator => StoreSales,
    StoreReturnsRowGenerator => StoreReturns,
    CatalogSalesRowGenerator => CatalogSales,
    CatalogReturnsRowGenerator => CatalogReturns,
    WebSalesRowGenerator => WebSales,
    WebReturnsRowGenerator => WebReturns,
);

/// `f` of every row of generator `G`: one row per source row.
///
/// The generator is opened at the first row asked for, so the
/// distribution clones and skips for nothing; a skip moves the start and
/// reopens it there.
pub(crate) struct Rows<G, F, Out> {
    session: Session,
    /// The next source row (1-based).
    next: u64,
    open: Option<G>,
    f: F,
    _out: PhantomData<fn() -> Out>,
}

impl<G, F: Clone, Out> Clone for Rows<G, F, Out> {
    fn clone(&self) -> Self {
        Rows {
            session: self.session.clone(),
            next: self.next,
            open: None,
            f: self.f.clone(),
            _out: PhantomData,
        }
    }
}

impl<G, F, Out> Rows<G, F, Out>
where
    G: Source,
    F: Fn(&G::Item) -> Out,
{
    pub(crate) fn new(session: &Session, f: F) -> Self {
        Rows {
            session: session.clone(),
            next: 1,
            open: None,
            f,
            _out: PhantomData,
        }
    }
}

impl<G, F, Out> Distribution for Rows<G, F, Out>
where
    G: Source,
    F: Fn(&G::Item) -> Out,
    Out: Columnar,
{
    type Item = Out;

    fn sample(&mut self, _rng: &mut impl rand::Rng) -> Out {
        let (session, next) = (&self.session, self.next);
        let g = self
            .open
            .get_or_insert_with(|| G::open(session, next, u64::MAX));
        self.next += 1;
        (self.f)(&g.next().expect("a row in range"))
    }

    fn collect_into(&mut self, cols: &mut Columns<Out>, _rng: &mut impl rand::Rng, n: usize) {
        let (session, next) = (&self.session, self.next);
        let g = self
            .open
            .get_or_insert_with(|| G::open(session, next, u64::MAX));
        let f = &self.f;
        cols.reserve(n);
        cols.push_n(g.by_ref().take(n).map(|r| f(&r)));
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        if n > 0 {
            self.next += n;
            self.open = None;
        }
    }
}

/// Table `G` as `[name = value, ...]` groups (a tuple each, at most 12;
/// at least two groups of at least two), the value expressions over the
/// row `$r` and its null bitmap `$n`.
macro_rules! table {
    // one group: the row is that tuple
    ($session:expr, $gen:ident, |$r:ident, $n:ident| $nbm:expr;
        [ $($name:ident = $e:expr),+ $(,)? ]) => {
        boxed(named_all(
            &[$(stringify!($name)),+],
            Rows::<$gen, _, _>::new($session, |$r: &<$gen as Iterator>::Item| {
                let $n: i64 = $nbm;
                let _ = $n;
                ($($e,)+)
            }),
        ))
    };
    // several: a tuple of the groups' tuples
    ($session:expr, $gen:ident, |$r:ident, $n:ident| $nbm:expr;
        $([ $($name:ident = $e:expr),+ $(,)? ])+) => {
        boxed(named_all(
            &[$($(stringify!($name)),+),+],
            Rows::<$gen, _, _>::new($session, |$r: &<$gen as Iterator>::Item| {
                let $n: i64 = $nbm;
                let _ = $n;
                ($(($($e,)+),)+)
            }),
        ))
    };
}

// ============================================================================
// The tables
// ============================================================================

/// A table, type-erased: its schema and any range of its rows as batches.
pub(crate) trait Gen: Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// Source rows `first..=last` (1-based) as batches of up to `batch` rows.
    fn range(&self, first: u64, last: u64, batch: usize, emit: &mut dyn FnMut(RecordBatch));

    /// [`range`](Self::range) as `.dat` lines appended to `out`, written
    /// from the columns.
    fn range_dat(&self, first: u64, last: u64, batch: usize, encoding: Encoding, out: &mut Vec<u8>);
}

impl<D> Gen for DistTable<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowColumns + TextColumns,
{
    fn range_dat(
        &self,
        first: u64,
        last: u64,
        batch: usize,
        encoding: Encoding,
        out: &mut Vec<u8>,
    ) {
        blocks(self.dist(), first, last, batch, |cols| {
            write_columns(&cols, encoding, out)
        });
    }

    fn schema(&self) -> SchemaRef {
        DistTable::schema(self).clone()
    }

    fn range(&self, first: u64, last: u64, batch: usize, emit: &mut dyn FnMut(RecordBatch)) {
        let schema = DistTable::schema(self);
        blocks(self.dist(), first, last, batch, |cols| {
            let b = into_record_batch_with(schema, cols).expect("columns match the schema");
            // a block of tickets may have no returns
            if b.num_rows() > 0 {
                emit(b);
            }
        });
    }
}

/// Source rows `first..=last` (1-based) of `dist`, `f` on the columns of
/// each block of up to `block` of them. One stream from `first` on, not a
/// skip per block (as `DistTable::range` makes): DSB's generators are
/// cheap to step and dear to open.
fn blocks<D: Distribution + Clone>(
    dist: &D,
    first: u64,
    last: u64,
    block: usize,
    mut f: impl FnMut(Columns<D::Item>),
) {
    let mut d = dist.clone();
    d.skip(first - 1);
    // the leaves draw from their own streams, not this
    let mut rng = SmallRng::seed_from_u64(0);
    let mut at = first;
    while at <= last {
        let n = block.min((last - at + 1) as usize);
        f(d.collect(&mut rng, n));
        at += n as u64;
    }
}

fn boxed<D>(dist: D) -> Box<dyn Gen>
where
    D: Distribution + Clone + Send + Sync + 'static,
    Columns<D::Item>: IntoArrowColumns + TextColumns,
{
    Box::new(DistTable::new("dsb", 0, dist))
}

/// Dimension table `t`, or `None` for a fact table.
pub(crate) fn dimension(t: TableName, s: &Session) -> Option<Box<dyn Gen>> {
    Some(match t {
        TableName::CallCenter => call_center(s),
        TableName::CatalogPage => catalog_page(s),
        TableName::Customer => customer(s),
        TableName::CustomerAddress => customer_address(s),
        TableName::CustomerDemographics => customer_demographics(s),
        TableName::DateDim => date_dim(s),
        TableName::HouseholdDemographics => household_demographics(s),
        TableName::IncomeBand => income_band(s),
        TableName::Inventory => inventory(s),
        TableName::Item => item(s),
        TableName::Promotion => promotion(s),
        TableName::Reason => reason(s),
        TableName::ShipMode => ship_mode(s),
        TableName::Store => store(s),
        TableName::TimeDim => time_dim(s),
        TableName::Warehouse => warehouse(s),
        TableName::WebPage => web_page(s),
        TableName::WebSite => web_site(s),
        _ => return None,
    })
}

fn call_center(s: &Session) -> Box<dyn Gen> {
    table!(s, CallCenterRowGenerator, |r, n| r.get_null_bit_map();
    [
        cc_call_center_sk = r.get_cc_call_center_sk() as i32,
        cc_call_center_id = Str::from(r.get_cc_call_center_id()),
        cc_rec_start_date = julian(n, 2, r.get_cc_rec_start_date_id()),
        cc_rec_end_date = julian(n, 3, r.get_cc_rec_end_date_id()),
        cc_closed_date_sk = sk(n, 4, r.get_cc_closed_date_id()),
        cc_open_date_sk = sk(n, 5, r.get_cc_open_date_id()),
        cc_name = text(n, 6, r.get_cc_name()),
        cc_class = text(n, 7, r.get_cc_class()),
        cc_employees = int(n, 8, r.get_cc_employees()),
        cc_sq_ft = int(n, 9, r.get_cc_sq_ft()),
        cc_hours = text(n, 10, r.get_cc_hours()),
        cc_manager = text(n, 11, r.get_cc_manager()),
    ]
    [
        cc_mkt_id = int(n, 12, r.get_cc_market_id()),
        cc_mkt_class = text(n, 13, r.get_cc_market_class()),
        cc_mkt_desc = text(n, 14, r.get_cc_market_desc()),
        cc_market_manager = text(n, 15, r.get_cc_market_manager()),
        cc_division = int(n, 16, r.get_cc_division_id()),
        cc_division_name = text(n, 17, r.get_cc_division_name()),
        cc_company = int(n, 18, r.get_cc_company()),
        cc_company_name = text(n, 19, r.get_cc_company_name()),
    ]
    [
        cc_street_number = addr::street_number(n, 20, r.get_cc_address()),
        cc_street_name = addr::street_name(n, 20, r.get_cc_address()),
        cc_street_type = addr::street_type(n, 20, r.get_cc_address()),
        cc_suite_number = addr::suite_number(n, 20, r.get_cc_address()),
        cc_city = addr::city(n, 20, r.get_cc_address()),
        cc_county = addr::county(n, 20, r.get_cc_address()),
        cc_state = addr::state(n, 20, r.get_cc_address()),
        cc_zip = addr::zip(n, 20, r.get_cc_address()),
        cc_country = addr::country(n, 20, r.get_cc_address()),
        cc_gmt_offset = addr::gmt_offset(n, 20, r.get_cc_address()),
        cc_tax_percentage = dec::<5, 2>(n, 30, *r.get_cc_tax_percentage()),
    ])
}

fn catalog_page(s: &Session) -> Box<dyn Gen> {
    table!(s, CatalogPageRowGenerator, |r, n| r.null_bit_map();
    [
        cp_catalog_page_sk = r.get_cp_catalog_page_sk() as i32,
        cp_catalog_page_id = Str::from(r.get_cp_catalog_page_id()),
        cp_start_date_sk = sk(n, 2, r.get_cp_start_date_id()),
        cp_end_date_sk = sk(n, 3, r.get_cp_end_date_id()),
    ]
    [
        // bit 4 is TPC-DS's CP_PROMO_ID, which has no column
        cp_department = text(n, 5, r.get_cp_department()),
        cp_catalog_number = int(n, 6, r.get_cp_catalog_number()),
        cp_catalog_page_number = int(n, 7, r.get_cp_catalog_page_number()),
        cp_description = text(n, 8, r.get_cp_description()),
        cp_type = text(n, 9, r.get_cp_type()),
    ])
}

fn customer(s: &Session) -> Box<dyn Gen> {
    table!(s, CustomerRowGenerator, |r, n| r.null_bit_map();
    [
        c_customer_sk = r.get_c_customer_sk() as i32,
        c_customer_id = Str::from(r.get_c_customer_id()),
        c_current_cdemo_sk = sk(n, 2, r.get_c_current_cdemo_sk()),
        c_current_hdemo_sk = sk(n, 3, r.get_c_current_hdemo_sk()),
        c_current_addr_sk = sk(n, 4, r.get_c_current_addr_sk()),
        c_first_shipto_date_sk = int(n, 5, r.get_c_first_shipto_date_id()),
        c_first_sales_date_sk = int(n, 6, r.get_c_first_sales_date_id()),
        c_salutation = text(n, 7, r.get_c_salutation()),
        c_first_name = text(n, 8, r.get_c_first_name()),
    ]
    [
        c_last_name = text(n, 9, r.get_c_last_name()),
        c_preferred_cust_flag = flag(n, 10, r.get_c_preferred_cust_flag()),
        c_birth_day = int(n, 11, r.get_c_birth_day()),
        c_birth_month = int(n, 12, r.get_c_birth_month()),
        c_birth_year = int(n, 13, r.get_c_birth_year()),
        c_birth_country = text(n, 14, r.get_c_birth_country()),
        // always NULL, as TPC-DS specifies
        c_login = None::<Str>,
        c_email_address = text(n, 16, r.get_c_email_address()),
        c_last_review_date_sk = int(n, 17, r.get_c_last_review_date()),
    ])
}

fn customer_address(s: &Session) -> Box<dyn Gen> {
    table!(s, CustomerAddressRowGenerator, |r, n| r.null_bit_map();
    [
        ca_address_sk = r.get_ca_addr_sk() as i32,
        ca_address_id = Str::from(r.get_ca_addr_id()),
    ]
    [
        ca_street_number = addr::street_number(n, 2, r.get_ca_address()),
        ca_street_name = addr::street_name(n, 2, r.get_ca_address()),
        ca_street_type = addr::street_type(n, 2, r.get_ca_address()),
        ca_suite_number = addr::suite_number(n, 2, r.get_ca_address()),
        ca_city = addr::city(n, 2, r.get_ca_address()),
        ca_county = addr::county(n, 2, r.get_ca_address()),
        ca_state = addr::state(n, 2, r.get_ca_address()),
        ca_zip = addr::zip(n, 2, r.get_ca_address()),
        ca_country = addr::country(n, 2, r.get_ca_address()),
        ca_gmt_offset = addr::gmt_offset(n, 2, r.get_ca_address()),
        ca_location_type = text(n, 12, r.get_ca_location_type()),
    ])
}

fn customer_demographics_rows(s: &Session) -> Box<dyn Gen> {
    table!(s, CustomerDemographicsRowGenerator, |r, n| r.null_bit_map();
    [
        cd_demo_sk = r.get_cd_demo_sk() as i32,
        cd_gender = text(n, 1, r.get_cd_gender()),
        cd_marital_status = text(n, 2, r.get_cd_marital_status()),
        cd_education_status = text(n, 3, r.get_cd_education_status()),
    ]
    [
        cd_purchase_estimate = int(n, 4, r.get_cd_purchase_estimate()),
        cd_credit_rating = text(n, 5, r.get_cd_credit_rating()),
        cd_dep_count = int(n, 6, r.get_cd_dep_count()),
        cd_dep_employed_count = int(n, 7, r.get_cd_dep_employed_count()),
        cd_dep_college_count = int(n, 8, r.get_cd_dep_college_count()),
    ])
}

fn date_dim(s: &Session) -> Box<dyn Gen> {
    // only the key reads the null bitmap; the other columns are never NULL
    table!(s, DateDimRowGenerator, |r, n| 0;
    [
        d_date_sk = r.d_date_sk as i32,
        d_date_id = Str::from(r.d_date_id.as_str()),
        d_date = Some(Date32(
            (i64::from(r.d_date.to_julian_days()) - UNIX_EPOCH_JULIAN) as i32,
        )),
        d_month_seq = Some(r.d_month_seq),
        d_week_seq = Some(r.d_week_seq),
        d_quarter_seq = Some(r.d_quarter_seq),
        d_year = Some(r.d_year),
        d_dow = Some(r.d_dow),
        d_moy = Some(r.d_moy),
        d_dom = Some(r.d_dom),
        d_qoy = Some(r.d_qoy),
        d_fy_year = Some(r.d_fy_year),
    ]
    [
        d_fy_quarter_seq = Some(r.d_fy_quarter_seq),
        d_fy_week_seq = Some(r.d_fy_week_seq),
        d_day_name = Some(Str::from(r.d_day_name.as_str())),
        d_quarter_name = Some(Str::from(r.d_quarter_name.as_str())),
        d_holiday = Some(yes_no(r.d_holiday)),
        d_weekend = Some(yes_no(r.d_weekend)),
        d_following_holiday = Some(yes_no(r.d_following_holiday)),
        d_first_dom = Some(r.d_first_dom),
        d_last_dom = Some(r.d_last_dom),
        d_same_day_ly = Some(r.d_same_day_ly),
        d_same_day_lq = Some(r.d_same_day_lq),
        d_current_day = Some(yes_no(r.d_current_day)),
    ]
    [
        d_current_week = Some(yes_no(r.d_current_week)),
        d_current_month = Some(yes_no(r.d_current_month)),
        d_current_quarter = Some(yes_no(r.d_current_quarter)),
        d_current_year = Some(yes_no(r.d_current_year)),
    ])
}

fn household_demographics(s: &Session) -> Box<dyn Gen> {
    table!(s, HouseholdDemographicsRowGenerator, |r, n| r.null_bit_map();
    [
        hd_demo_sk = r.get_hd_demo_sk() as i32,
        hd_income_band_sk = sk(n, 1, r.get_hd_income_band_sk()),
    ]
    [
        hd_buy_potential = text(n, 2, r.get_hd_buy_potential()),
        hd_dep_count = int(n, 3, r.get_hd_dep_count()),
        hd_vehicle_count = int(n, 4, r.get_hd_vehicle_count()),
    ])
}

fn income_band(s: &Session) -> Box<dyn Gen> {
    table!(s, IncomeBandRowGenerator, |r, n| r.null_bit_map();
    [
        ib_income_band_sk = r.get_ib_income_band_sk(),
        ib_lower_bound = int(n, 1, r.get_ib_lower_bound()),
        ib_upper_bound = int(n, 2, r.get_ib_upper_bound()),
    ])
}

fn item(s: &Session) -> Box<dyn Gen> {
    table!(s, ItemRowGenerator, |r, n| r.null_bit_map();
    [
        i_item_sk = r.get_i_item_sk() as i32,
        i_item_id = Str::from(r.get_i_item_id()),
        i_rec_start_date = julian(n, 2, r.get_i_rec_start_date_id()),
        // ignores the bitmap: only a negative day is NULL
        i_rec_end_date = julian(0, 3, r.get_i_rec_end_date_id()),
        i_item_desc = text(n, 4, r.get_i_item_desc()),
        i_current_price = dec::<7, 2>(n, 5, r.get_i_current_price()),
        i_wholesale_cost = dec::<7, 2>(n, 6, r.get_i_wholesale_cost()),
        i_brand_id = int_of(n, 7, r.get_i_brand_id()),
        i_brand = text(n, 8, r.get_i_brand()),
        i_class_id = int_of(n, 9, r.get_i_class_id()),
        i_class = text(n, 10, r.get_i_class()),
    ]
    [
        i_category_id = int_of(n, 11, r.get_i_category_id()),
        i_category = text(n, 12, r.get_i_category()),
        i_manufact_id = int_of(n, 13, r.get_i_manufact_id()),
        i_manufact = text(n, 14, r.get_i_manufact()),
        i_size = text(n, 15, r.get_i_size()),
        i_formulation = text(n, 16, r.get_i_formulation()),
        i_color = text(n, 17, r.get_i_color()),
        i_units = text(n, 18, r.get_i_units()),
        i_container = text(n, 19, r.get_i_container()),
        i_manager_id = int_of(n, 20, r.get_i_manager_id()),
        i_product_name = text(n, 21, r.get_i_product_name()),
    ])
}

fn promotion(s: &Session) -> Box<dyn Gen> {
    table!(s, PromotionRowGenerator, |r, n| r.null_bit_map();
    [
        p_promo_sk = r.get_p_promo_sk() as i32,
        p_promo_id = Str::from(r.get_p_promo_id()),
        p_start_date_sk = sk(n, 2, r.get_p_start_date_id()),
        p_end_date_sk = sk(n, 3, r.get_p_end_date_id()),
        p_item_sk = sk(n, 4, r.get_p_item_sk()),
        p_cost = dec::<15, 2>(n, 5, r.get_p_cost()),
        p_response_target = int(n, 6, r.get_p_response_target()),
        p_promo_name = text(n, 7, r.get_p_promo_name()),
    ]
    [
        p_channel_dmail = flag(n, 8, r.get_p_channel_dmail()),
        p_channel_email = flag(n, 9, r.get_p_channel_email()),
        p_channel_catalog = flag(n, 10, r.get_p_channel_catalog()),
        p_channel_tv = flag(n, 11, r.get_p_channel_tv()),
        p_channel_radio = flag(n, 12, r.get_p_channel_radio()),
        p_channel_press = flag(n, 13, r.get_p_channel_press()),
        p_channel_event = flag(n, 14, r.get_p_channel_event()),
        p_channel_demo = flag(n, 15, r.get_p_channel_demo()),
        p_channel_details = text(n, 16, r.get_p_channel_details()),
        p_purpose = text(n, 17, r.get_p_purpose()),
        p_discount_active = flag(n, 18, r.get_p_discount_active()),
    ])
}

fn reason(s: &Session) -> Box<dyn Gen> {
    table!(s, ReasonRowGenerator, |r, n| r.null_bit_map();
    [
        r_reason_sk = r.get_r_reason_sk() as i32,
        r_reason_id = Str::from(r.get_r_reason_id()),
        r_reason_desc = text(n, 2, r.get_r_reason_desc()),
    ])
}

fn ship_mode(s: &Session) -> Box<dyn Gen> {
    table!(s, ShipModeRowGenerator, |r, n| r.null_bit_map();
    [
        sm_ship_mode_sk = r.get_sm_ship_mode_sk() as i32,
        sm_ship_mode_id = Str::from(r.get_sm_ship_mode_id()),
        sm_type = text(n, 2, r.get_sm_type()),
    ]
    [
        sm_code = text(n, 3, r.get_sm_code()),
        sm_carrier = text(n, 4, r.get_sm_carrier()),
        sm_contract = text(n, 5, r.get_sm_contract()),
    ])
}

fn store(s: &Session) -> Box<dyn Gen> {
    table!(s, StoreRowGenerator, |r, n| r.null_bit_map();
    [
        s_store_sk = r.get_store_sk() as i32,
        s_store_id = Str::from(r.get_store_id()),
        s_rec_start_date = julian(n, 2, r.get_rec_start_date_id()),
        // only a negative day is NULL here, not the bitmap
        s_rec_end_date = julian(0, 3, r.get_rec_end_date_id()),
        s_closed_date_sk = sk(n, 4, r.get_closed_date_id()),
        s_store_name = text(n, 5, r.get_store_name()),
        s_number_employees = int(n, 6, r.get_employees()),
        s_floor_space = int(n, 7, r.get_floor_space()),
        s_hours = text(n, 8, r.get_hours()),
        s_manager = text(n, 9, r.get_store_manager()),
    ]
    [
        s_market_id = int(n, 10, r.get_market_id()),
        // the tax percentage is bit 11 but the last column
        s_geography_class = text(n, 12, r.get_geography_class()),
        s_market_desc = text(n, 13, r.get_market_desc()),
        s_market_manager = text(n, 14, r.get_market_manager()),
        s_division_id = int_of(n, 15, r.get_division_id()),
        s_division_name = text(n, 16, r.get_division_name()),
        s_company_id = int_of(n, 17, r.get_company_id()),
        s_company_name = text(n, 18, r.get_company_name()),
    ]
    [
        s_street_number = addr::street_number(n, 19, r.get_address()),
        s_street_name = addr::street_name(n, 19, r.get_address()),
        s_street_type = addr::street_type(n, 19, r.get_address()),
        s_suite_number = addr::suite_number(n, 19, r.get_address()),
        s_city = addr::city(n, 19, r.get_address()),
        s_county = addr::county(n, 19, r.get_address()),
        s_state = addr::state(n, 19, r.get_address()),
        s_zip = addr::zip(n, 19, r.get_address()),
        s_country = addr::country(n, 19, r.get_address()),
        s_gmt_offset = addr::gmt_offset(n, 19, r.get_address()),
        s_tax_precentage = dec::<5, 2>(n, 11, r.get_d_tax_percentage()),
    ])
}

fn time_dim(s: &Session) -> Box<dyn Gen> {
    table!(s, TimeDimRowGenerator, |r, n| r.null_bit_map();
    [
        t_time_sk = r.t_time_sk as i32,
        t_time_id = Str::from(r.t_time_id.as_str()),
        t_time = int(n, 2, r.t_time),
        t_hour = int(n, 3, r.t_hour),
        t_minute = int(n, 4, r.t_minute),
    ]
    [
        t_second = int(n, 5, r.t_second),
        t_am_pm = text(n, 6, r.t_am_pm.as_str()),
        t_shift = text(n, 7, r.t_shift.as_str()),
        t_sub_shift = text(n, 8, r.t_sub_shift.as_str()),
        // NULL when empty, whatever the bitmap
        t_meal_time = (!r.t_meal_time.is_empty()).then(|| Str::from(r.t_meal_time.as_str())),
    ])
}

fn warehouse(s: &Session) -> Box<dyn Gen> {
    table!(s, WarehouseRowGenerator, |r, n| r.null_bit_map();
    [
        w_warehouse_sk = r.get_w_warehouse_sk() as i32,
        w_warehouse_id = Str::from(r.get_w_warehouse_id()),
        w_warehouse_name = text(n, 2, r.get_w_warehouse_name()),
        w_warehouse_sq_ft = int(n, 3, r.get_w_warehouse_sq_ft()),
    ]
    [
        w_street_number = addr::street_number(n, 4, r.get_w_address()),
        w_street_name = addr::street_name(n, 4, r.get_w_address()),
        w_street_type = addr::street_type(n, 4, r.get_w_address()),
        w_suite_number = addr::suite_number(n, 4, r.get_w_address()),
        w_city = addr::city(n, 4, r.get_w_address()),
        w_county = addr::county(n, 4, r.get_w_address()),
        w_state = addr::state(n, 4, r.get_w_address()),
        w_zip = addr::zip(n, 4, r.get_w_address()),
        w_country = addr::country(n, 4, r.get_w_address()),
        w_gmt_offset = addr::gmt_offset(n, 4, r.get_w_address()),
    ])
}

fn web_page(s: &Session) -> Box<dyn Gen> {
    table!(s, WebPageRowGenerator, |r, n| r.null_bit_map();
    [
        wp_web_page_sk = r.get_wp_page_sk() as i32,
        wp_web_page_id = Str::from(r.get_wp_page_id()),
        wp_rec_start_date = julian(n, 2, r.get_wp_rec_start_date_id()),
        // ignores the bitmap: only a negative day is NULL
        wp_rec_end_date = julian(0, 3, r.get_wp_rec_end_date_id()),
        wp_creation_date_sk = sk(n, 4, r.get_wp_creation_date_sk()),
        wp_access_date_sk = sk(n, 5, r.get_wp_access_date_sk()),
        wp_autogen_flag = flag(n, 6, r.get_wp_autogen_flag()),
    ]
    [
        wp_customer_sk = sk(n, 7, r.get_wp_customer_sk()),
        wp_url = text(n, 8, r.get_wp_url()),
        wp_type = text(n, 9, r.get_wp_type()),
        wp_char_count = int(n, 10, r.get_wp_char_count()),
        wp_link_count = int(n, 11, r.get_wp_link_count()),
        wp_image_count = int(n, 12, r.get_wp_image_count()),
        wp_max_ad_count = int(n, 13, r.get_wp_max_ad_count()),
    ])
}

fn web_site(s: &Session) -> Box<dyn Gen> {
    table!(s, WebSiteRowGenerator, |r, n| r.null_bit_map();
    [
        web_site_sk = r.get_web_site_sk() as i32,
        web_site_id = Str::from(r.get_web_site_id()),
        web_rec_start_date = julian(n, 2, r.get_web_rec_start_date_id()),
        // only a negative day is NULL here, not the bitmap
        web_rec_end_date = julian(0, 3, r.get_web_rec_end_date_id()),
        web_name = text(n, 4, r.web_name()),
        web_open_date_sk = sk(n, 5, r.web_open_date()),
        web_close_date_sk = sk(n, 6, r.web_close_date()),
        web_class = text(n, 7, r.web_class()),
        web_manager = text(n, 8, r.web_manager()),
        web_mkt_id = int(n, 9, r.web_market_id()),
        web_mkt_class = text(n, 10, r.web_market_class()),
    ]
    [
        web_mkt_desc = text(n, 11, r.web_market_desc()),
        web_market_manager = text(n, 12, r.web_market_manager()),
        web_company_id = int(n, 13, r.web_company_id()),
        web_company_name = text(n, 14, r.web_company_name()),
    ]
    [
        web_street_number = addr::street_number(n, 15, r.web_address()),
        web_street_name = addr::street_name(n, 15, r.web_address()),
        web_street_type = addr::street_type(n, 15, r.web_address()),
        web_suite_number = addr::suite_number(n, 15, r.web_address()),
        web_city = addr::city(n, 15, r.web_address()),
        web_county = addr::county(n, 15, r.web_address()),
        web_state = addr::state(n, 15, r.web_address()),
        web_zip = addr::zip(n, 15, r.web_address()),
        web_country = addr::country(n, 15, r.web_address()),
        web_gmt_offset = addr::gmt_offset(n, 15, r.web_address()),
        web_tax_percentage = dec::<5, 2>(n, 25, *r.web_tax_percentage()),
    ])
}

// ============================================================================
// inventory and customer_demographics: computed columns, not row structs
// ============================================================================

type DemoRow = (
    i32,
    Option<Str>,
    Option<Str>,
    Option<Str>,
    Option<i32>,
    Option<Str>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
);

/// customer_demographics: `cd_demo_sk` is the row, and the other columns
/// are its digits in a mixed radix, gender fastest. Exact only while the
/// table has no nulls (no random draw reaches the output); otherwise the
/// row generator runs.
fn customer_demographics(s: &Session) -> Box<dyn Gen> {
    if NullTable::CustomerDemographics.get_null_basis_points() != 0 {
        return customer_demographics_rows(s);
    }
    let list = |n: usize, f: fn(i64) -> &'static str| -> Arc<[Word]> {
        (0..n as i64).map(|i| Word::new(f(i))).collect()
    };
    let gender = list(Demo::get_gender_size(), Demo::get_gender_for_index_mod_size);
    let marital = list(
        Demo::get_marital_status_size(),
        Demo::get_marital_status_for_index_mod_size,
    );
    let education = list(
        Demo::get_education_size(),
        Demo::get_education_for_index_mod_size,
    );
    let credit = list(
        Demo::get_credit_rating_size(),
        Demo::get_credit_rating_for_index_mod_size,
    );
    let purchase: Arc<[i32]> = (0..Demo::get_purchase_band_size() as i64)
        .map(Demo::get_purchase_band_for_index_mod_size)
        .collect();
    // radices, fastest first; the last three are the dependent counts
    let radix = [
        gender.len() as i64,
        marital.len() as i64,
        education.len() as i64,
        purchase.len() as i64,
        credit.len() as i64,
        7,
        7,
        7,
    ];
    let mut div = [1i64; 8];
    for d in 1..8 {
        div[d] = div[d - 1] * radix[d - 1];
    }
    // digit `d` of rows `i..i + n` (0-based): runs of `div[d]` equal
    // digits, counting up mod `radix[d]`, so no division per row
    let digits = move |i: i64, n: usize, d: usize| {
        let (mut v, mut left) = ((i / div[d] % radix[d]) as usize, div[d] - i % div[d]);
        (0..n).map(move |_| {
            if left == 0 {
                v = if v + 1 == radix[d] as usize { 0 } else { v + 1 };
                left = div[d];
            }
            left -= 1;
            v
        })
    };
    boxed(named_all(
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
        // a block at a time: the digits are plain loops over the rows, and
        // the columns are never NULL
        derive_columns::<_, _, DemoRow>(
            range(1i64, 1),
            move |rows: &Vec<i64>, out: &mut Columns<DemoRow>| {
                let n = rows.len();
                let Some(&first) = rows.first() else { return };
                let i = first - 1;
                out.0.extend(rows.iter().map(|&r| r as i32));
                // each word's bytes once per block, not once per row
                let words = |col: &mut Nullable<StrViews>, list: &[Word], d: usize| {
                    Sink::<Str>::reserve(&mut col.values, n);
                    col.values.push_indexed(list, digits(i, n, d));
                    col.valid.push_valid(n);
                };
                words(&mut out.1, &gender, 0);
                words(&mut out.2, &marital, 1);
                words(&mut out.3, &education, 2);
                let ints = |col: &mut Nullable<Vec<i32>>, d: usize, f: &dyn Fn(usize) -> i32| {
                    col.values.extend(digits(i, n, d).map(f));
                    col.valid.push_valid(n);
                };
                ints(&mut out.4, 3, &|v| purchase[v]);
                words(&mut out.5, &credit, 4);
                ints(&mut out.6, 5, &|v| v as i32);
                ints(&mut out.7, 6, &|v| v as i32);
                ints(&mut out.8, 7, &|v| v as i32);
            },
        ),
    ))
}

/// Two steps of a Lehmer stream: `A^2 mod m`.
const TWO_DRAWS: u64 = A * A % M;

/// inventory: row `r` is item `(r-1) % items`, then warehouse, then week,
/// slowest; one random quantity and a null bitmap per row.
#[derive(Clone)]
struct Inventory {
    session: Session,
    /// The next row, 0-based.
    next: u64,
    /// The streams and position at `next`, made when first needed.
    at: Option<InventoryAt>,
}

#[derive(Clone)]
struct InventoryAt {
    q_stream: RandomNumberStream,
    q_bucket: (i32, i32),
    q_perm: Vec<i32>,
    /// The null stream's next two draws.
    n1: u64,
    n2: u64,
    item: u64,
    warehouse: u64,
    week: u64,
    /// Item surrogate keys of `keys_week`.
    keys: Vec<i32>,
    keys_week: u64,
}

impl Inventory {
    fn open(&self) -> InventoryAt {
        let scaling = self.session.get_scaling();
        let total = scaling.get_row_count(Ds::Inventory);
        let first = self.next + 1;
        let mut g = AbstractRowGenerator::<Inv>::new(total);
        g.set_source_row_range(first, total);
        // DSB's quantity: an exponential, permuted bucket, then a uniform
        // value (two draws a row; after the permutation row 1 also draws)
        let mut rows = InventoryRowGenerator::new(self.session.clone(), total);
        rows.set_source_row_range(first, total);
        let (q_stream, q_bucket, q_perm) = rows.quantity_sampler();
        let items = scaling.get_id_count(Ds::Item);
        let warehouses = scaling.get_row_count(Ds::Warehouse);
        // Each row takes one quantity draw and two null-bitmap draws,
        // exactly its budget (1 and 2 seeds), and the other streams are
        // never read, so the null stream just steps on: its first draws
        // here, then two Lehmer steps a row.
        let nulls = g.get_random_number_stream(&Inv::InvNulls);
        let n1 = nulls.next_random() as u64;
        let n2 = nulls.next_random() as u64;
        InventoryAt {
            q_stream,
            q_bucket,
            q_perm,
            n1,
            n2,
            item: self.next % items,
            warehouse: (self.next / items) % warehouses,
            week: self.next / items / warehouses,
            keys: Vec::new(),
            keys_week: u64::MAX,
        }
    }
}

type InventoryRow = (i32, i32, i32, Option<i32>);

impl Distribution for Inventory {
    type Item = InventoryRow;

    fn sample(&mut self, rng: &mut impl rand::Rng) -> InventoryRow {
        let mut c = Columns::<InventoryRow>::default();
        self.collect_into(&mut c, rng, 1);
        (
            c.0[0],
            c.1[0],
            c.2[0],
            c.3.valid.get(0).then_some(c.3.values[0]),
        )
    }

    fn collect_into(
        &mut self,
        (date, item_sk, wh, qty): &mut Columns<InventoryRow>,
        _rng: &mut impl rand::Rng,
        n: usize,
    ) {
        if self.at.is_none() {
            self.at = Some(self.open());
        }
        let s = self.at.as_mut().expect("opened");
        let scaling = self.session.get_scaling();
        let items = scaling.get_id_count(Ds::Item);
        let warehouses = scaling.get_row_count(Ds::Warehouse);
        let basis = NullTable::Inventory.get_null_basis_points();
        let not_null = NullTable::Inventory.get_not_null_bit_map();
        date.reserve(n);
        item_sk.reserve(n);
        wh.reserve(n);
        qty.reserve(n);
        let mut left = n;
        while left > 0 {
            let d = Date::JULIAN_DATE_MINIMUM as i64 + s.week as i64 * 7;
            if s.keys_week != s.week {
                s.keys.clear();
                s.keys.extend(
                    (0..items)
                        .map(|i| match_surrogate_key(i as i64 + 1, d, Ds::Item, scaling) as i32),
                );
                s.keys_week = s.week;
            }
            // the rest of this warehouse's items, at most `left`
            let run = ((items - s.item) as usize).min(left);
            let from = s.item as usize;
            date.extend(repeat_n(d as i32, run));
            wh.extend(repeat_n(s.warehouse as i32 + 1, run));
            item_sk.extend_from_slice(&s.keys[from..from + run]);
            for _ in 0..run {
                qty.values.push(InventoryRowGenerator::sample_quantity(
                    &mut s.q_stream,
                    s.q_bucket,
                    &s.q_perm,
                ));
                // create_null_bit_map: a threshold in 0..10000, then a bitmap
                // in 1..=i32::MAX; the date, item and warehouse are NOT NULL
                let nbm = if (s.n1 as i32) % 10000 < basis {
                    ((s.n2 as i32) % i32::MAX + 1) as i64 & !not_null
                } else {
                    0
                };
                qty.valid.push(nbm & (1 << 3) == 0);
                // the null stream reserves two draws per row
                s.n1 = mulmod(s.n1, TWO_DRAWS);
                s.n2 = mulmod(s.n2, TWO_DRAWS);
            }
            left -= run;
            s.item += run as u64;
            if s.item == items {
                s.item = 0;
                s.warehouse += 1;
                if s.warehouse == warehouses {
                    s.warehouse = 0;
                    s.week += 1;
                }
            }
        }
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        if n > 0 {
            self.next += n;
            self.at = None;
        }
    }
}

fn inventory(s: &Session) -> Box<dyn Gen> {
    boxed(named_all(
        &[
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand",
        ],
        Inventory {
            session: s.clone(),
            next: 0,
            at: None,
        },
    ))
}

// ============================================================================
// Sales and returns: the lines of each ticket or order
// ============================================================================

/// A sales or returns line as the table's columns.
pub(crate) trait Fact {
    /// The column names.
    const NAMES: &[&str];
    /// The row: a tuple of groups of columns.
    type Cols: Columnar;
    /// Lines per source row on average (`TableName::rows_per_source_row`).
    const PER_SOURCE_ROW: f64;
    fn cols(&self) -> Self::Cols;
    /// [`cols`](Self::cols) written straight into the columns, without
    /// making the tuple.
    fn push(&self, cols: &mut Columns<Self::Cols>);
}

/// [`Fact`] for dsbgen row `$row`: column names `$names` and row type
/// `$cols`, from `[(group, index) name: type = value]` groups of values of
/// the row `$r` and its null bitmap `$n`; `(group, index)` is the value's
/// column in the tuple.
macro_rules! fact {
    ($names:ident, $cols:ident, $row:ty, $per:expr, |$r:ident, $n:ident|
        $([ $(($g:tt, $i:tt) $name:ident: $t:ty = $e:expr),+ $(,)? ])+) => {
        const $names: &[&str] = &[$($(stringify!($name)),+),+];
        type $cols = ($(($($t,)+),)+);
        impl Fact for $row {
            const NAMES: &[&str] = $names;
            type Cols = $cols;
            const PER_SOURCE_ROW: f64 = $per;
            fn cols(&self) -> $cols {
                let $r = self;
                let $n = $r.null_bit_map();
                ($(($($e,)+),)+)
            }
            #[inline]
            fn push(&self, cols: &mut Columns<$cols>) {
                let $r = self;
                let $n = $r.null_bit_map();
                $($(cols.$g.$i.push($e);)+)+
            }
        }
    };
}

fact!(STORE_SALES, StoreSalesCols, StoreSalesRow, 12.0, |r, n|
[
    (0, 0) ss_sold_date_sk: Option<i32> = sk(n, 0, r.get_ss_sold_date_sk()),
    (0, 1) ss_sold_time_sk: Option<i32> = sk(n, 1, r.get_ss_sold_time_sk()),
    (0, 2) ss_item_sk: i32 = r.get_ss_sold_item_sk() as i32,
    (0, 3) ss_customer_sk: Option<i32> = sk(n, 3, r.get_ss_sold_customer_sk()),
    (0, 4) ss_cdemo_sk: Option<i32> = sk(n, 4, r.get_ss_sold_cdemo_sk()),
    (0, 5) ss_hdemo_sk: Option<i32> = sk(n, 5, r.get_ss_sold_hdemo_sk()),
    (0, 6) ss_addr_sk: Option<i32> = sk(n, 6, r.get_ss_sold_addr_sk()),
    (0, 7) ss_store_sk: Option<i32> = sk(n, 7, r.get_ss_sold_store_sk()),
    (0, 8) ss_promo_sk: Option<i32> = sk(n, 8, r.get_ss_sold_promo_sk()),
    (0, 9) ss_ticket_number: i64 = r.get_ss_ticket_number(),
    (0, 10) ss_quantity: Option<i32> = int(n, 10, r.get_ss_pricing().get_quantity()),
    (0, 11) ss_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 11, r.get_ss_pricing().get_wholesale_cost()),
]
[
    (1, 0) ss_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 12, r.get_ss_pricing().get_list_price()),
    (1, 1) ss_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 13, r.get_ss_pricing().get_sales_price()),
    (1, 2) ss_ext_discount_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 14, r.get_ss_pricing().get_coupon_amount()),
    (1, 3) ss_ext_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 15, r.get_ss_pricing().get_ext_sales_price()),
    (1, 4) ss_ext_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 16, r.get_ss_pricing().get_ext_wholesale_cost()),
    (1, 5) ss_ext_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 17, r.get_ss_pricing().get_ext_list_price()),
    (1, 6) ss_ext_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 18, r.get_ss_pricing().get_ext_tax()),
    (1, 7) ss_coupon_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 14, r.get_ss_pricing().get_coupon_amount()),
    (1, 8) ss_net_paid: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_ss_pricing().get_net_paid()),
    (1, 9) ss_net_paid_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 20, r.get_ss_pricing().get_net_paid_including_tax()),
    (1, 10) ss_net_profit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 21, r.get_ss_pricing().get_net_profit()),
]);

fact!(STORE_RETURNS, StoreReturnsCols, StoreReturnsRow, 1.2, |r, n|
[
    (0, 0) sr_returned_date_sk: Option<i32> = sk(n, 0, r.get_sr_returned_date_sk()),
    (0, 1) sr_return_time_sk: Option<i32> = sk(n, 1, r.get_sr_returned_time_sk()),
    (0, 2) sr_item_sk: i32 = r.get_sr_item_sk() as i32,
    (0, 3) sr_customer_sk: Option<i32> = sk(n, 3, r.get_sr_customer_sk()),
    (0, 4) sr_cdemo_sk: Option<i32> = sk(n, 4, r.get_sr_cdemo_sk()),
    (0, 5) sr_hdemo_sk: Option<i32> = sk(n, 5, r.get_sr_hdemo_sk()),
    (0, 6) sr_addr_sk: Option<i32> = sk(n, 6, r.get_sr_addr_sk()),
    (0, 7) sr_store_sk: Option<i32> = sk(n, 7, r.get_sr_store_sk()),
    (0, 8) sr_reason_sk: Option<i32> = sk(n, 8, r.get_sr_reason_sk()),
    (0, 9) sr_ticket_number: i64 = r.get_sr_ticket_number(),
    (0, 10) sr_return_quantity: Option<i32> = int(n, 10, r.get_sr_pricing().get_quantity()),
    (0, 11) sr_return_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 11, r.get_sr_pricing().get_net_paid()),
]
[
    (1, 0) sr_return_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 12, r.get_sr_pricing().get_ext_tax()),
    (1, 1) sr_return_amt_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 13, r.get_sr_pricing().get_net_paid_including_tax()),
    (1, 2) sr_fee: Option<Decimal<7, 2>> = dec::<7, 2>(n, 14, r.get_sr_pricing().get_fee()),
    (1, 3) sr_return_ship_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 15, r.get_sr_pricing().get_ext_ship_cost()),
    (1, 4) sr_refunded_cash: Option<Decimal<7, 2>> = dec::<7, 2>(n, 16, r.get_sr_pricing().get_refunded_cash()),
    (1, 5) sr_reversed_charge: Option<Decimal<7, 2>> = dec::<7, 2>(n, 17, r.get_sr_pricing().get_reversed_charge()),
    (1, 6) sr_store_credit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 18, r.get_sr_pricing().get_store_credit()),
    (1, 7) sr_net_loss: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_sr_pricing().get_net_loss()),
]);

fact!(CATALOG_SALES, CatalogSalesCols, CatalogSalesRow, 9.0, |r, n|
[
    (0, 0) cs_sold_date_sk: Option<i32> = sk(n, 0, r.get_cs_sold_date_sk()),
    (0, 1) cs_sold_time_sk: Option<i32> = sk(n, 1, r.get_cs_sold_time_sk()),
    (0, 2) cs_ship_date_sk: Option<i32> = sk(n, 2, r.get_cs_ship_date_sk()),
    (0, 3) cs_bill_customer_sk: Option<i32> = sk(n, 3, r.get_cs_bill_customer_sk()),
    (0, 4) cs_bill_cdemo_sk: Option<i32> = sk(n, 4, r.get_cs_bill_cdemo_sk()),
    (0, 5) cs_bill_hdemo_sk: Option<i32> = sk(n, 5, r.get_cs_bill_hdemo_sk()),
    (0, 6) cs_bill_addr_sk: Option<i32> = sk(n, 6, r.get_cs_bill_addr_sk()),
    (0, 7) cs_ship_customer_sk: Option<i32> = sk(n, 7, r.get_cs_ship_customer_sk()),
    (0, 8) cs_ship_cdemo_sk: Option<i32> = sk(n, 8, r.get_cs_ship_cdemo_sk()),
    (0, 9) cs_ship_hdemo_sk: Option<i32> = sk(n, 9, r.get_cs_ship_hdemo_sk()),
    (0, 10) cs_ship_addr_sk: Option<i32> = sk(n, 10, r.get_cs_ship_addr_sk()),
    (0, 11) cs_call_center_sk: Option<i32> = sk(n, 11, r.get_cs_call_center_sk()),
]
[
    (1, 0) cs_catalog_page_sk: Option<i32> = sk(n, 12, r.get_cs_catalog_page_sk()),
    (1, 1) cs_ship_mode_sk: Option<i32> = sk(n, 13, r.get_cs_ship_mode_sk()),
    (1, 2) cs_warehouse_sk: Option<i32> = sk(n, 14, r.get_cs_warehouse_sk()),
    (1, 3) cs_item_sk: i32 = r.get_cs_sold_item_sk() as i32,
    (1, 4) cs_promo_sk: Option<i32> = sk(n, 16, r.get_cs_promo_sk()),
    (1, 5) cs_order_number: i64 = r.get_cs_order_number(),
    (1, 6) cs_quantity: Option<i32> = int(n, 18, r.get_cs_pricing().get_quantity()),
    (1, 7) cs_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_cs_pricing().get_wholesale_cost()),
    (1, 8) cs_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 20, r.get_cs_pricing().get_list_price()),
    (1, 9) cs_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 21, r.get_cs_pricing().get_sales_price()),
    (1, 10) cs_ext_discount_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 24, r.get_cs_pricing().get_ext_discount_amount()),
    (1, 11) cs_ext_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 23, r.get_cs_pricing().get_ext_sales_price()),
]
[
    (2, 0) cs_ext_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 25, r.get_cs_pricing().get_ext_wholesale_cost()),
    (2, 1) cs_ext_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 26, r.get_cs_pricing().get_ext_list_price()),
    (2, 2) cs_ext_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 27, r.get_cs_pricing().get_ext_tax()),
    (2, 3) cs_coupon_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 22, r.get_cs_pricing().get_coupon_amount()),
    (2, 4) cs_ext_ship_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 28, r.get_cs_pricing().get_ext_ship_cost()),
    (2, 5) cs_net_paid: Option<Decimal<7, 2>> = dec::<7, 2>(n, 29, r.get_cs_pricing().get_net_paid()),
    (2, 6) cs_net_paid_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 30, r.get_cs_pricing().get_net_paid_including_tax()),
    (2, 7) cs_net_paid_inc_ship: Option<Decimal<7, 2>> = dec::<7, 2>(n, 31, r.get_cs_pricing().get_net_paid_including_shipping()),
    (2, 8) cs_net_paid_inc_ship_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 32, r.get_cs_pricing().get_net_paid_including_shipping_and_tax(),),
    (2, 9) cs_net_profit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 33, r.get_cs_pricing().get_net_profit()),
]);

fact!(CATALOG_RETURNS, CatalogReturnsCols, CatalogReturnsRow, 0.9, |r, n|
[
    (0, 0) cr_returned_date_sk: Option<i32> = sk(n, 0, r.get_cr_returned_date_sk()),
    (0, 1) cr_returned_time_sk: Option<i32> = sk(n, 1, r.get_cr_returned_time_sk()),
    (0, 2) cr_item_sk: i32 = r.get_cr_item_sk() as i32,
    (0, 3) cr_refunded_customer_sk: Option<i32> = sk(n, 3, r.get_cr_refunded_customer_sk()),
    (0, 4) cr_refunded_cdemo_sk: Option<i32> = sk(n, 4, r.get_cr_refunded_cdemo_sk()),
    (0, 5) cr_refunded_hdemo_sk: Option<i32> = sk(n, 5, r.get_cr_refunded_hdemo_sk()),
    (0, 6) cr_refunded_addr_sk: Option<i32> = sk(n, 6, r.get_cr_refunded_addr_sk()),
    (0, 7) cr_returning_customer_sk: Option<i32> = sk(n, 7, r.get_cr_returning_customer_sk()),
    (0, 8) cr_returning_cdemo_sk: Option<i32> = sk(n, 8, r.get_cr_returning_cdemo_sk()),
    (0, 9) cr_returning_hdemo_sk: Option<i32> = sk(n, 9, r.get_cr_returning_hdemo_sk()),
    (0, 10) cr_returning_addr_sk: Option<i32> = sk(n, 10, r.get_cr_returning_addr_sk()),
    (0, 11) cr_call_center_sk: Option<i32> = sk(n, 11, r.get_cr_call_center_sk()),
]
[
    (1, 0) cr_catalog_page_sk: Option<i32> = sk(n, 12, r.get_cr_catalog_page_sk()),
    (1, 1) cr_ship_mode_sk: Option<i32> = sk(n, 13, r.get_cr_ship_mode_sk()),
    (1, 2) cr_warehouse_sk: Option<i32> = sk(n, 14, r.get_cr_warehouse_sk()),
    (1, 3) cr_reason_sk: Option<i32> = sk(n, 15, r.get_cr_reason_sk()),
    (1, 4) cr_order_number: i64 = r.get_cr_order_number(),
    (1, 5) cr_return_quantity: Option<i32> = int(n, 17, r.get_cr_pricing().get_quantity()),
    (1, 6) cr_return_amount: Option<Decimal<7, 2>> = dec::<7, 2>(n, 18, r.get_cr_pricing().get_net_paid()),
    (1, 7) cr_return_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_cr_pricing().get_ext_tax()),
    (1, 8) cr_return_amt_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 20, r.get_cr_pricing().get_net_paid_including_tax()),
    (1, 9) cr_fee: Option<Decimal<7, 2>> = dec::<7, 2>(n, 21, r.get_cr_pricing().get_fee()),
    (1, 10) cr_return_ship_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 22, r.get_cr_pricing().get_ext_ship_cost()),
    (1, 11) cr_refunded_cash: Option<Decimal<7, 2>> = dec::<7, 2>(n, 23, r.get_cr_pricing().get_refunded_cash()),
]
[
    (2, 0) cr_reversed_charge: Option<Decimal<7, 2>> = dec::<7, 2>(n, 24, r.get_cr_pricing().get_reversed_charge()),
    (2, 1) cr_store_credit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 25, r.get_cr_pricing().get_store_credit()),
    (2, 2) cr_net_loss: Option<Decimal<7, 2>> = dec::<7, 2>(n, 26, r.get_cr_pricing().get_net_loss()),
]);

fact!(WEB_SALES, WebSalesCols, WebSalesRow, 12.0, |r, n|
[
    (0, 0) ws_sold_date_sk: Option<i32> = sk(n, 0, r.get_ws_sold_date_sk()),
    (0, 1) ws_sold_time_sk: Option<i32> = sk(n, 1, r.get_ws_sold_time_sk()),
    (0, 2) ws_ship_date_sk: Option<i32> = sk(n, 2, r.get_ws_ship_date_sk()),
    (0, 3) ws_item_sk: i32 = r.get_ws_item_sk() as i32,
    (0, 4) ws_bill_customer_sk: Option<i32> = sk(n, 4, r.get_ws_bill_customer_sk()),
    (0, 5) ws_bill_cdemo_sk: Option<i32> = sk(n, 5, r.get_ws_bill_cdemo_sk()),
    (0, 6) ws_bill_hdemo_sk: Option<i32> = sk(n, 6, r.get_ws_bill_hdemo_sk()),
    (0, 7) ws_bill_addr_sk: Option<i32> = sk(n, 7, r.get_ws_bill_addr_sk()),
    (0, 8) ws_ship_customer_sk: Option<i32> = sk(n, 8, r.get_ws_ship_customer_sk()),
    (0, 9) ws_ship_cdemo_sk: Option<i32> = sk(n, 9, r.get_ws_ship_cdemo_sk()),
    (0, 10) ws_ship_hdemo_sk: Option<i32> = sk(n, 10, r.get_ws_ship_hdemo_sk()),
    (0, 11) ws_ship_addr_sk: Option<i32> = sk(n, 11, r.get_ws_ship_addr_sk()),
]
[
    (1, 0) ws_web_page_sk: Option<i32> = sk(n, 12, r.get_ws_web_page_sk()),
    (1, 1) ws_web_site_sk: Option<i32> = sk(n, 13, r.get_ws_web_site_sk()),
    (1, 2) ws_ship_mode_sk: Option<i32> = sk(n, 14, r.get_ws_ship_mode_sk()),
    (1, 3) ws_warehouse_sk: Option<i32> = sk(n, 15, r.get_ws_warehouse_sk()),
    (1, 4) ws_promo_sk: Option<i32> = sk(n, 16, r.get_ws_promo_sk()),
    (1, 5) ws_order_number: i64 = r.get_ws_order_number(),
    (1, 6) ws_quantity: Option<i32> = int(n, 18, r.get_ws_pricing().get_quantity()),
    (1, 7) ws_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_ws_pricing().get_wholesale_cost()),
    (1, 8) ws_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 20, r.get_ws_pricing().get_list_price()),
    (1, 9) ws_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 21, r.get_ws_pricing().get_sales_price()),
    (1, 10) ws_ext_discount_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 22, r.get_ws_pricing().get_ext_discount_amount()),
    (1, 11) ws_ext_sales_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 23, r.get_ws_pricing().get_ext_sales_price()),
]
[
    (2, 0) ws_ext_wholesale_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 24, r.get_ws_pricing().get_ext_wholesale_cost()),
    (2, 1) ws_ext_list_price: Option<Decimal<7, 2>> = dec::<7, 2>(n, 25, r.get_ws_pricing().get_ext_list_price()),
    (2, 2) ws_ext_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 26, r.get_ws_pricing().get_ext_tax()),
    (2, 3) ws_coupon_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 27, r.get_ws_pricing().get_coupon_amount()),
    (2, 4) ws_ext_ship_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 28, r.get_ws_pricing().get_ext_ship_cost()),
    (2, 5) ws_net_paid: Option<Decimal<7, 2>> = dec::<7, 2>(n, 29, r.get_ws_pricing().get_net_paid()),
    (2, 6) ws_net_paid_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 30, r.get_ws_pricing().get_net_paid_including_tax()),
    (2, 7) ws_net_paid_inc_ship: Option<Decimal<7, 2>> = dec::<7, 2>(n, 31, r.get_ws_pricing().get_net_paid_including_shipping()),
    (2, 8) ws_net_paid_inc_ship_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 32, r.get_ws_pricing().get_net_paid_including_shipping_and_tax(),),
    (2, 9) ws_net_profit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 33, r.get_ws_pricing().get_net_profit()),
]);

fact!(WEB_RETURNS, WebReturnsCols, WebReturnsRow, 1.2, |r, n|
[
    (0, 0) wr_returned_date_sk: Option<i32> = sk(n, 0, r.get_wr_returned_date_sk()),
    (0, 1) wr_returned_time_sk: Option<i32> = sk(n, 1, r.get_wr_returned_time_sk()),
    (0, 2) wr_item_sk: i32 = r.get_wr_item_sk() as i32,
    (0, 3) wr_refunded_customer_sk: Option<i32> = sk(n, 3, r.get_wr_refunded_customer_sk()),
    (0, 4) wr_refunded_cdemo_sk: Option<i32> = sk(n, 4, r.get_wr_refunded_cdemo_sk()),
    (0, 5) wr_refunded_hdemo_sk: Option<i32> = sk(n, 5, r.get_wr_refunded_hdemo_sk()),
    (0, 6) wr_refunded_addr_sk: Option<i32> = sk(n, 6, r.get_wr_refunded_addr_sk()),
    (0, 7) wr_returning_customer_sk: Option<i32> = sk(n, 7, r.get_wr_returning_customer_sk()),
    (0, 8) wr_returning_cdemo_sk: Option<i32> = sk(n, 8, r.get_wr_returning_cdemo_sk()),
    (0, 9) wr_returning_hdemo_sk: Option<i32> = sk(n, 9, r.get_wr_returning_hdemo_sk()),
    (0, 10) wr_returning_addr_sk: Option<i32> = sk(n, 10, r.get_wr_returning_addr_sk()),
    (0, 11) wr_web_page_sk: Option<i32> = sk(n, 11, r.get_wr_web_page_sk()),
]
[
    (1, 0) wr_reason_sk: Option<i32> = sk(n, 12, r.get_wr_reason_sk()),
    (1, 1) wr_order_number: i64 = r.get_wr_order_number(),
    (1, 2) wr_return_quantity: Option<i32> = int(n, 14, r.get_wr_pricing().get_quantity()),
    (1, 3) wr_return_amt: Option<Decimal<7, 2>> = dec::<7, 2>(n, 15, r.get_wr_pricing().get_net_paid()),
    (1, 4) wr_return_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 16, r.get_wr_pricing().get_ext_tax()),
    (1, 5) wr_return_amt_inc_tax: Option<Decimal<7, 2>> = dec::<7, 2>(n, 17, r.get_wr_pricing().get_net_paid_including_tax()),
    (1, 6) wr_fee: Option<Decimal<7, 2>> = dec::<7, 2>(n, 18, r.get_wr_pricing().get_fee()),
    (1, 7) wr_return_ship_cost: Option<Decimal<7, 2>> = dec::<7, 2>(n, 19, r.get_wr_pricing().get_ext_ship_cost()),
    (1, 8) wr_refunded_cash: Option<Decimal<7, 2>> = dec::<7, 2>(n, 20, r.get_wr_pricing().get_refunded_cash()),
    (1, 9) wr_reversed_charge: Option<Decimal<7, 2>> = dec::<7, 2>(n, 21, r.get_wr_pricing().get_reversed_charge()),
    (1, 10) wr_account_credit: Option<Decimal<7, 2>> = dec::<7, 2>(n, 22, r.get_wr_pricing().get_store_credit()),
    (1, 11) wr_net_loss: Option<Decimal<7, 2>> = dec::<7, 2>(n, 23, r.get_wr_pricing().get_net_loss()),
]);

/// A sales or returns line: the source row (ticket or order) it is of.
pub(crate) trait Line {
    fn source_row(&self) -> u64;
}

macro_rules! lines_of {
    ($($row:ident => $get:ident),* $(,)?) => {
        $(impl Line for $row {
            fn source_row(&self) -> u64 {
                self.$get() as u64
            }
        })*
    };
}

lines_of!(
    StoreSalesRow => get_ss_ticket_number,
    StoreReturnsRow => get_sr_ticket_number,
    CatalogSalesRow => get_cs_order_number,
    CatalogReturnsRow => get_cr_order_number,
    WebSalesRow => get_ws_order_number,
    WebReturnsRow => get_wr_order_number,
);

/// Room for the lines of `n` source rows: their mean count with some slack,
/// so a block seldom outgrows it (growing doubles every column).
fn lines_in(n: usize, per_source_row: f64) -> usize {
    (n as f64 * per_source_row * 1.1) as usize + 16
}

/// A generator `G` from source row `next` on, with the first row it made
/// that belongs to a later source row than asked for so far.
struct Open<G, Row> {
    rows: G,
    held: Option<Row>,
}

/// Every line of the source rows (tickets, orders) asked for: the lines
/// row generator `G` makes, the children of their source row.
///
/// The generator stays open from one block of source rows to the next
/// (opening one positions every stream, which costs far more than a line).
pub(crate) struct Lines<G: Iterator> {
    session: Session,
    /// The next source row (1-based).
    next: u64,
    open: Option<Open<G, G::Item>>,
}

impl<G: Iterator> Clone for Lines<G> {
    fn clone(&self) -> Self {
        Lines {
            session: self.session.clone(),
            next: self.next,
            open: None,
        }
    }
}

impl<G> Lines<G>
where
    G: Source,
    G::Item: Line,
{
    /// Each line of source rows `next..next + n`, in order.
    fn lines(&mut self, n: u64, mut push: impl FnMut(&G::Item)) {
        let last = self.next + n - 1;
        let (session, next) = (&self.session, self.next);
        let o = self.open.get_or_insert_with(|| Open {
            rows: G::open(session, next, u64::MAX),
            held: None,
        });
        while let Some(row) = o.held.take().or_else(|| o.rows.next()) {
            if row.source_row() > last {
                o.held = Some(row);
                break;
            }
            push(&row);
        }
        self.next = last + 1;
    }
}

impl<G> Distribution for Lines<G>
where
    G: Source,
    G::Item: Line + Fact,
{
    type Item = Children<<G::Item as Fact>::Cols>;

    fn sample(&mut self, _rng: &mut impl rand::Rng) -> Self::Item {
        let mut v = Vec::new();
        self.lines(1, |r| v.push(r.cols()));
        Children(v)
    }

    fn collect_into(
        &mut self,
        cols: &mut Columns<Self::Item>,
        _rng: &mut impl rand::Rng,
        n: usize,
    ) {
        cols.values
            .reserve(lines_in(n, <G::Item as Fact>::PER_SOURCE_ROW));
        self.lines(n as u64, |r| r.push(&mut cols.values));
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        if n > 0 {
            self.next += n;
            self.open = None;
        }
    }

    fn column_names(&self, out: &mut Vec<String>) {
        out.extend(<G::Item as Fact>::NAMES.iter().map(|&n| n.to_owned()));
    }
}

/// A returns generator that also makes the sale line of every return.
pub(crate) trait WithSale: Source {
    type Sale: Line;
    /// `f` on the next sale line and its return, if any; and whether the
    /// line is its source row's last.
    fn with_next<R>(
        &mut self,
        f: impl FnOnce(&Self::Sale, Option<&Self::Item>) -> R,
    ) -> Option<(R, bool)>;
}

macro_rules! with_sale {
    ($($gen:ident => $sale:ident),* $(,)?) => {
        $(impl WithSale for $gen {
            type Sale = $sale;
            #[inline]
            fn with_next<R>(
                &mut self,
                f: impl FnOnce(&$sale, Option<&Self::Item>) -> R,
            ) -> Option<(R, bool)> {
                self.with_next_sale(f)
            }
        })*
    };
}

with_sale!(
    StoreReturnsRowGenerator => StoreSalesRow,
    CatalogReturnsRowGenerator => CatalogSalesRow,
    WebReturnsRowGenerator => WebSalesRow,
);

/// A sales table and its returns from one pass: the returns generator `G`
/// makes each sale line with its return, if any. Its generator stays open
/// as [`Lines`]'s does.
pub(crate) struct Pairs<G: WithSale> {
    session: Session,
    next: u64,
    /// The generator, at the first line of source row `next`.
    open: Option<G>,
}

impl<G: WithSale> Clone for Pairs<G> {
    fn clone(&self) -> Self {
        Pairs {
            session: self.session.clone(),
            next: self.next,
            open: None,
        }
    }
}

impl<G: WithSale> Pairs<G> {
    /// Each sale line of source rows `next..next + n`, and its return.
    fn lines(&mut self, n: u64, mut push: impl FnMut(&G::Sale, Option<&G::Item>)) {
        let last = self.next + n - 1;
        let (session, next) = (&self.session, self.next);
        let o = self
            .open
            .get_or_insert_with(|| G::open(session, next, u64::MAX));
        let mut left = n;
        while left > 0 {
            match o.with_next(&mut push) {
                Some(((), true)) => left -= 1,
                Some(((), false)) => {}
                None => break,
            }
        }
        self.next = last + 1;
    }
}

impl<G> Distribution for Pairs<G>
where
    G: WithSale,
    G::Sale: Fact,
    G::Item: Fact,
{
    type Item = (
        Children<<G::Sale as Fact>::Cols>,
        Children<<G::Item as Fact>::Cols>,
    );

    fn sample(&mut self, _rng: &mut impl rand::Rng) -> Self::Item {
        let (mut sales, mut rets) = (Vec::new(), Vec::new());
        self.lines(1, |s, r| {
            sales.push(s.cols());
            rets.extend(r.map(Fact::cols));
        });
        (Children(sales), Children(rets))
    }

    fn collect_into(
        &mut self,
        (sales, rets): &mut Columns<Self::Item>,
        _rng: &mut impl rand::Rng,
        n: usize,
    ) {
        sales
            .values
            .reserve(lines_in(n, <G::Sale as Fact>::PER_SOURCE_ROW));
        rets.values
            .reserve(lines_in(n, <G::Item as Fact>::PER_SOURCE_ROW));
        self.lines(n as u64, |s, r| {
            s.push(&mut sales.values);
            if let Some(r) = r {
                r.push(&mut rets.values);
            }
        });
        sales.sources += n;
        rets.sources += n;
    }

    fn skip(&mut self, n: u64) {
        if n > 0 {
            self.next += n;
            self.open = None;
        }
    }

    fn column_names(&self, out: &mut Vec<String>) {
        for names in [<G::Sale as Fact>::NAMES, <G::Item as Fact>::NAMES] {
            out.extend(names.iter().map(|&n| n.to_owned()));
        }
    }
}

/// Sales and returns tables, type-erased: their schemas, and any range of
/// source rows as batches of both.
pub(crate) trait PairGen: Send + Sync {
    /// Source rows `first..=last` (1-based), in blocks of `per_batch`
    /// source rows; `emit(k, batch)` for table `k` (0 sales, 1 returns).
    fn range(
        &self,
        first: u64,
        last: u64,
        per_batch: u64,
        emit: &mut dyn FnMut(usize, RecordBatch),
    );

    /// Source rows `first..=last` in blocks of `per_batch`, as `.dat`
    /// lines of the sales (`bufs[0]`) and returns (`bufs[1]`), written from
    /// the columns.
    fn range_dat(
        &self,
        first: u64,
        last: u64,
        per_batch: u64,
        encoding: Encoding,
        bufs: &mut [Vec<u8>],
    );
}

impl<D> PairGen for DistTables<D>
where
    D: Distribution + Clone + Send + Sync,
    Columns<D::Item>: IntoArrowTables + TextTables,
{
    fn range_dat(
        &self,
        first: u64,
        last: u64,
        per_batch: u64,
        encoding: Encoding,
        bufs: &mut [Vec<u8>],
    ) {
        blocks(self.dist(), first, last, per_batch as usize, |cols| {
            cols.write_tables(encoding, bufs)
        });
    }

    fn range(
        &self,
        first: u64,
        last: u64,
        per_batch: u64,
        emit: &mut dyn FnMut(usize, RecordBatch),
    ) {
        let schemas = DistTables::schemas(self);
        blocks(self.dist(), first, last, per_batch as usize, |cols| {
            let mut tables = Vec::with_capacity(2);
            cols.into_tables(&mut tables);
            for (k, cols) in tables.into_iter().enumerate() {
                // a block of tickets may have no returns
                if !cols[0].is_empty() {
                    let b = RecordBatch::try_new(schemas[k].clone(), cols)
                        .expect("columns match the schema");
                    emit(k, b);
                }
            }
        });
    }
}

/// Sales or returns table `t` alone, or `None` for a dimension.
pub(crate) fn fact(t: TableName, s: &Session) -> Option<Box<dyn Gen>> {
    fn lines<G>(s: &Session) -> Box<dyn Gen>
    where
        G: Source + Send + Sync + 'static,
        G::Item: Line + Fact + Send + Sync,
        Columns<Children<<G::Item as Fact>::Cols>>: IntoArrowColumns + TextColumns,
    {
        boxed(Lines::<G> {
            session: s.clone(),
            next: 1,
            open: None,
        })
    }
    Some(match t {
        TableName::StoreSales => lines::<StoreSalesRowGenerator>(s),
        TableName::StoreReturns => lines::<StoreReturnsRowGenerator>(s),
        TableName::CatalogSales => lines::<CatalogSalesRowGenerator>(s),
        TableName::CatalogReturns => lines::<CatalogReturnsRowGenerator>(s),
        TableName::WebSales => lines::<WebSalesRowGenerator>(s),
        TableName::WebReturns => lines::<WebReturnsRowGenerator>(s),
        _ => return None,
    })
}

/// A channel's sales and returns from one pass: `t` is either of them.
pub(crate) fn pair(t: TableName, s: &Session) -> Option<Box<dyn PairGen>> {
    fn pairs<G>(s: &Session, sales: Ds) -> Box<dyn PairGen>
    where
        G: WithSale + Send + Sync + 'static,
        G::Sale: Fact + Send + Sync,
        G::Item: Fact + Send + Sync,
        Columns<<Pairs<G> as Distribution>::Item>: IntoArrowTables + TextTables,
    {
        let dist = Pairs::<G> {
            session: s.clone(),
            next: 1,
            open: None,
        };
        let rows = s.get_scaling().get_row_count(sales);
        Box::new(DistTables::new(&["sales", "returns"], rows, dist))
    }
    Some(match t {
        TableName::StoreSales | TableName::StoreReturns => {
            pairs::<StoreReturnsRowGenerator>(s, Ds::StoreSales)
        }
        TableName::CatalogSales | TableName::CatalogReturns => {
            pairs::<CatalogReturnsRowGenerator>(s, Ds::CatalogSales)
        }
        TableName::WebSales | TableName::WebReturns => {
            pairs::<WebReturnsRowGenerator>(s, Ds::WebSales)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_select::concat::concat_batches;
    use dsbgen::config::SessionBuilder;
    use dsbgen_arrow::{CustomerDemographicsArrow, InventoryArrow};

    fn collect(f: impl FnOnce(&mut dyn FnMut(RecordBatch))) -> RecordBatch {
        let mut v = Vec::new();
        f(&mut |b| v.push(b));
        concat_batches(&v[0].schema(), &v).unwrap()
    }

    #[test]
    fn inventory_matches_dsbgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        let t = dimension(TableName::Inventory, &s).unwrap();
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
            let got = collect(|e| t.range(first, last, 777, e));
            assert_eq!(got, want, "{first}..={last}");
        }
    }

    #[test]
    fn demographics_match_dsbgen_arrow() {
        let s = SessionBuilder::new()
            .with_scale_factor(1.0)
            .build()
            .unwrap();
        let t = dimension(TableName::CustomerDemographics, &s).unwrap();
        for (first, last) in [(1, 3000), (1_000_000, 1_003_000), (1_920_000, 1_920_800)] {
            let want = collect(|e| {
                CustomerDemographicsArrow::new(s.clone())
                    .with_source_row_range(first, last)
                    .for_each(|b| e(b.unwrap()))
            });
            let got = collect(|e| t.range(first, last, 500, e));
            assert_eq!(got, want, "{first}..={last}");
        }
    }
}
