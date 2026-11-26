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

//! CSV formatting support for TPC-DS
//!
//! Each table row type gets a `<Table>Csv` wrapper implementing [`CsvRow`],
//! whose `Display` impl emits one CSV line
//!
//! # Character encoding
//!
//! CSV output is always UTF-8 (while The DAT output can optionally be encoded
//! as ISO-8859-1 in [`CompatMode::Trino`] (see [`crate::output::DatWriter`]);
//!
//! [`csv_header`] returns the header line for a [`Table`].
//!
//! [`CompatMode::Trino`]: crate::config::CompatMode::Trino

use crate::config::Table;
use crate::row::dbgen_version_row::TimeOfDay;
use crate::row::table_row::{CsvQuoted, CsvQuotedNullLiteral, DatField, NullLiteralField};
use crate::row::{
    CallCenterRow, CatalogPageRow, CatalogReturnsRow, CatalogSalesRow, CustomerAddressRow,
    CustomerDemographicsRow, CustomerRow, DateDimRow, DbgenVersionRow, HouseholdDemographicsRow,
    IncomeBandRow, InventoryRow, ItemRow, PromotionRow, ReasonRow, ShipModeRow, StoreReturnsRow,
    StoreRow, StoreSalesRow, TimeDimRow, WarehouseRow, WebPageRow, WebReturnsRow, WebSalesRow,
    WebSiteRow,
};
use std::fmt::{self, Display};

/// A [`Display`] wrapper that formats one table's rows as CSV lines.
pub trait CsvRow<'a>: Display + Sized {
    /// The row type this wrapper formats
    type Row: 'a;

    /// Create a wrapper that formats `inner` with the default `,` delimiter
    fn new(inner: &'a Self::Row) -> Self {
        Self::with_delimiter(inner, ',')
    }

    /// Create a wrapper that formats `inner` with a custom delimiter
    fn with_delimiter(inner: &'a Self::Row, delimiter: char) -> Self;

    /// Returns the comma-separated CSV header for this table
    fn header() -> &'static str;

    /// Returns the CSV header with a custom delimiter
    fn header_with_delimiter(delimiter: char) -> String {
        join_header(Self::header(), delimiter)
    }
}

/// Writes [`CallCenterRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CallCenterCsv, CsvRow};
/// # use dsbgen::row::CallCenterRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CallCenter);
/// let generator = CallCenterRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CallCenterCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CallCenterCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "cc_call_center_sk,cc_call_center_id,cc_rec_start_date,cc_rec_end_date,cc_closed_date_sk,cc_open_date_sk,cc_name,cc_class,cc_employees,cc_sq_ft,cc_hours,cc_manager,cc_mkt_id,cc_mkt_class,cc_mkt_desc,cc_market_manager,cc_division,cc_division_name,cc_company,cc_company_name,cc_street_number,cc_street_name,cc_street_type,cc_suite_number,cc_city,cc_county,cc_state,cc_zip,cc_country,cc_gmt_offset,cc_tax_percentage\n\
///    1,AAAAAAAABAAAAAAA,1998-01-01,,,2450952,NY Metro,small,4,984,8AM-8AM,Bob Belcher,5,\"More than other authori\",\"Shared others could not count fully dollars. New members ca\",Julius Tran,6,cally,4,ese,252,Seventh ,Circle,Suite 180,Riverside,Franklin Parish,LA,79231,United States,-6,0.11\n\
///    2,AAAAAAAACAAAAAAA,1998-01-01,2000-12-31,,2450806,Mid Atlantic,large,7,2779,8AM-8AM,Felipe Perkins,3,\"A bit narrow forms matter animals. Consist\",\"Largely blank years put substantially deaf, new others. Question\",Julius Durham,1,ought,3,pri,369,Third ,Ln,Suite 290,Oak Grove,Richland County,OH,48370,United States,-5,0.12\n\
///    3,AAAAAAAACAAAAAAA,2001-01-01,,,2450806,Mid Atlantic,small,7,2310,8AM-12AM,Mark Hightower,3,\"Wrong troops shall work sometimes in a opti\",\"Largely blank years put substantially deaf, new others. Question\",Julius Durham,3,pri,5,anti,369,Third ,Ln,Suite 290,Oak Grove,Richland County,OH,48370,United States,-5,0.01\n"
/// );
/// ```
pub struct CallCenterCsv<'a> {
    inner: &'a CallCenterRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CallCenterCsv<'a> {
    type Row = CallCenterRow;

    fn with_delimiter(inner: &'a CallCenterRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "cc_call_center_sk,cc_call_center_id,cc_rec_start_date,cc_rec_end_date,cc_closed_date_sk,cc_open_date_sk,cc_name,cc_class,cc_employees,cc_sq_ft,cc_hours,cc_manager,cc_mkt_id,cc_mkt_class,cc_mkt_desc,cc_market_manager,cc_division,cc_division_name,cc_company,cc_company_name,cc_street_number,cc_street_name,cc_street_type,cc_suite_number,cc_city,cc_county,cc_state,cc_zip,cc_country,cc_gmt_offset,cc_tax_percentage"
    }
}

impl Display for CallCenterCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.nulled(row.cc_call_center_sk, 0),
            row.nulled(&row.cc_call_center_id, 1),
            row.date_field(row.cc_rec_start_date_id, 2),
            row.date_field(row.cc_rec_end_date_id, 3),
            row.key_field(row.cc_closed_date_id, 4),
            row.key_field(row.cc_open_date_id, 5),
            row.nulled(&row.cc_name, 6),
            row.nulled(&row.cc_class, 7),
            row.nulled(row.cc_employees, 8),
            row.nulled(row.cc_sq_ft, 9),
            row.nulled(&row.cc_hours, 10),
            row.nulled(&row.cc_manager, 11),
            row.nulled(row.cc_market_id, 12),
            CsvQuotedNullLiteral::new(&row.cc_market_class, row.is_null(13)),
            CsvQuotedNullLiteral::new(&row.cc_market_desc, row.is_null(14)),
            row.nulled(&row.cc_market_manager, 15),
            row.nulled(row.cc_division_id, 16),
            row.nulled(&row.cc_division_name, 17),
            row.nulled(row.cc_company, 18),
            row.nulled(&row.cc_company_name, 19),
            row.nulled(row.cc_address.get_street_number(), 20),
            row.nulled(row.cc_address.get_street_name(), 21),
            row.nulled(row.cc_address.get_street_type(), 22),
            row.nulled(row.cc_address.get_suite_number(), 23),
            row.nulled(row.cc_address.get_city(), 24),
            row.nulled(row.cc_address.get_county().unwrap_or(""), 25),
            row.nulled(row.cc_address.get_state(), 26),
            DatField::zip(row.cc_address.get_zip(), row.is_null(27)),
            row.nulled(row.cc_address.get_country(), 28),
            row.nulled(row.cc_address.get_gmt_offset(), 29),
            row.nulled(row.cc_tax_percentage, 30),
        )
    }
}
/// Writes [`CatalogPageRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CatalogPageCsv, CsvRow};
/// # use dsbgen::row::CatalogPageRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CatalogPage);
/// let generator = CatalogPageRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CatalogPageCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CatalogPageCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "cp_catalog_page_sk,cp_catalog_page_id,cp_start_date_sk,cp_end_date_sk,cp_department,cp_catalog_number,cp_catalog_page_number,cp_description,cp_type\n\
///    1,AAAAAAAABAAAAAAA,2450815,2450996,DEPARTMENT,1,1,\"Arms matter then under invisible terms. Homes sound. Mor\",bi-annual\n\
///    2,AAAAAAAACAAAAAAA,2450815,2450996,DEPARTMENT,1,2,\"Social, exciting projects fly apparently other days. Female, used institutions used \",bi-annual\n\
///    3,AAAAAAAADAAAAAAA,2450815,2450996,DEPARTMENT,1,3,\"Hands can catch dogs. Industrial, key grounds might hear a\",bi-annual\n"
/// );
/// ```
pub struct CatalogPageCsv<'a> {
    inner: &'a CatalogPageRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CatalogPageCsv<'a> {
    type Row = CatalogPageRow;

    fn with_delimiter(inner: &'a CatalogPageRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "cp_catalog_page_sk,cp_catalog_page_id,cp_start_date_sk,cp_end_date_sk,cp_department,cp_catalog_number,cp_catalog_page_number,cp_description,cp_type"
    }
}

impl Display for CatalogPageCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::CatalogPageGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.cp_catalog_page_sk, &CpCatalogPageSk),
            row.field(&row.cp_catalog_page_id, &CpCatalogPageId),
            row.key_field(row.cp_start_date_id, &CpStartDateId),
            row.key_field(row.cp_end_date_id, &CpEndDateId),
            row.field(&row.cp_department, &CpDepartment),
            row.field(row.cp_catalog_number, &CpCatalogNumber),
            row.field(row.cp_catalog_page_number, &CpCatalogPageNumber),
            CsvQuoted::new(&row.cp_description, row.is_null(&CpDescription)),
            row.field(&row.cp_type, &CpType),
        )
    }
}
/// Writes [`CatalogReturnsRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CatalogReturnsCsv, CsvRow};
/// # use dsbgen::row::CatalogReturnsRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CatalogSales);
/// let generator = CatalogReturnsRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CatalogReturnsCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CatalogReturnsCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss\n\
///    2451033,224,4400,10561,,,,,,,4349,1,,4,,7,1,6,,,355.44,,,,33.77,32.45,\n\
///    2450982,44675,7105,73834,1224571,2668,10860,20973,1615201,5810,1371,1,10,17,1,12,3,3,153.60,1.53,155.13,45.29,0.00,59.90,8.43,85.27,46.82\n\
///    2450975,7785,16543,73834,1224571,2668,10860,4674,1544135,5187,15289,1,14,14,3,9,3,2,61.52,5.53,67.05,66.97,1.66,36.29,21.95,3.28,74.16\n"
/// );
/// ```
pub struct CatalogReturnsCsv<'a> {
    inner: &'a CatalogReturnsRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CatalogReturnsCsv<'a> {
    type Row = CatalogReturnsRow;

    fn with_delimiter(inner: &'a CatalogReturnsRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss"
    }
}

impl Display for CatalogReturnsCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::CatalogReturnsGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.cr_returned_date_sk, &CrReturnedDateSk),
            row.key_field(row.cr_returned_time_sk, &CrReturnedTimeSk),
            row.key_field(row.cr_item_sk, &CrItemSk),
            row.key_field(row.cr_refunded_customer_sk, &CrRefundedCustomerSk),
            row.key_field(row.cr_refunded_cdemo_sk, &CrRefundedCdemoSk),
            row.key_field(row.cr_refunded_hdemo_sk, &CrRefundedHdemoSk),
            row.key_field(row.cr_refunded_addr_sk, &CrRefundedAddrSk),
            row.key_field(row.cr_returning_customer_sk, &CrReturningCustomerSk),
            row.key_field(row.cr_returning_cdemo_sk, &CrReturningCdemoSk),
            row.key_field(row.cr_returning_hdemo_sk, &CrReturningHdemoSk),
            row.key_field(row.cr_returning_addr_sk, &CrReturningAddrSk),
            row.key_field(row.cr_call_center_sk, &CrCallCenterSk),
            row.key_field(row.cr_catalog_page_sk, &CrCatalogPageSk),
            row.key_field(row.cr_ship_mode_sk, &CrShipModeSk),
            row.key_field(row.cr_warehouse_sk, &CrWarehouseSk),
            row.key_field(row.cr_reason_sk, &CrReasonSk),
            row.field(row.cr_order_number, &CrOrderNumber),
            row.field(row.cr_pricing.get_quantity(), &CrPricingQuantity),
            row.field(row.cr_pricing.get_net_paid(), &CrPricingNetPaid),
            row.field(row.cr_pricing.get_ext_tax(), &CrPricingExtTax),
            row.field(
                row.cr_pricing.get_net_paid_including_tax(),
                &CrPricingNetPaidIncTax
            ),
            row.field(row.cr_pricing.get_fee(), &CrPricingFee),
            row.field(row.cr_pricing.get_ext_ship_cost(), &CrPricingExtShipCost),
            row.field(row.cr_pricing.get_refunded_cash(), &CrPricingRefundedCash),
            row.field(
                row.cr_pricing.get_reversed_charge(),
                &CrPricingReversedCharge
            ),
            row.field(row.cr_pricing.get_store_credit(), &CrPricingStoreCredit),
            row.field(row.cr_pricing.get_net_loss(), &CrPricingNetLoss),
        )
    }
}
/// Writes [`CatalogSalesRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CatalogSalesCsv, CsvRow};
/// # use dsbgen::row::CatalogSalesRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CatalogSales);
/// let generator = CatalogSalesRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CatalogSalesCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CatalogSalesCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit\n\
///    2450815,66744,2450895,10561,1199271,4668,1347,10561,1199271,4668,1347,1,27,15,1,17959,3,1,57,68.73,155.32,82.31,4161.57,4691.67,3917.61,8853.24,110.25,2486.58,1504.80,2205.09,2315.34,3709.89,3820.14,-1712.52\n\
///    2450815,66744,2450847,10561,1199271,4668,1347,10561,1199271,4668,1347,1,35,14,3,13480,96,1,38,11.42,29.34,6.45,869.82,245.10,433.96,1114.92,7.35,0.00,546.06,245.10,252.45,791.16,798.51,-188.86\n\
///    2450815,66744,2450867,10561,1199271,4668,1347,10561,1199271,4668,1347,1,91,14,5,8779,68,1,32,58.31,64.14,57.72,205.44,1847.04,1865.92,2052.48,73.88,0.00,492.48,1847.04,1920.92,2339.52,2413.40,-18.88\n"
/// );
/// ```
pub struct CatalogSalesCsv<'a> {
    inner: &'a CatalogSalesRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CatalogSalesCsv<'a> {
    type Row = CatalogSalesRow;

    fn with_delimiter(inner: &'a CatalogSalesRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit"
    }
}

impl Display for CatalogSalesCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::CatalogSalesGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.cs_sold_date_sk, &CsSoldDateSk),
            row.key_field(row.cs_sold_time_sk, &CsSoldTimeSk),
            row.key_field(row.cs_ship_date_sk, &CsShipDateSk),
            row.key_field(row.cs_bill_customer_sk, &CsBillCustomerSk),
            row.key_field(row.cs_bill_cdemo_sk, &CsBillCdemoSk),
            row.key_field(row.cs_bill_hdemo_sk, &CsBillHdemoSk),
            row.key_field(row.cs_bill_addr_sk, &CsBillAddrSk),
            row.key_field(row.cs_ship_customer_sk, &CsShipCustomerSk),
            row.key_field(row.cs_ship_cdemo_sk, &CsShipCdemoSk),
            row.key_field(row.cs_ship_hdemo_sk, &CsShipHdemoSk),
            row.key_field(row.cs_ship_addr_sk, &CsShipAddrSk),
            row.key_field(row.cs_call_center_sk, &CsCallCenterSk),
            row.key_field(row.cs_catalog_page_sk, &CsCatalogPageSk),
            row.key_field(row.cs_ship_mode_sk, &CsShipModeSk),
            row.field(row.cs_warehouse_sk, &CsWarehouseSk),
            row.key_field(row.cs_sold_item_sk, &CsSoldItemSk),
            row.key_field(row.cs_promo_sk, &CsPromoSk),
            row.field(row.cs_order_number, &CsOrderNumber),
            row.field(row.cs_pricing.get_quantity(), &CsPricingQuantity),
            row.field(
                row.cs_pricing.get_wholesale_cost(),
                &CsPricingWholesaleCost
            ),
            row.field(row.cs_pricing.get_list_price(), &CsPricingListPrice),
            row.field(row.cs_pricing.get_sales_price(), &CsPricingSalesPrice),
            row.field(
                row.cs_pricing.get_ext_discount_amount(),
                &CsPricingExtDiscountAmount
            ),
            row.field(
                row.cs_pricing.get_ext_sales_price(),
                &CsPricingExtSalesPrice
            ),
            row.field(
                row.cs_pricing.get_ext_wholesale_cost(),
                &CsPricingExtWholesaleCost
            ),
            row.field(
                row.cs_pricing.get_ext_list_price(),
                &CsPricingExtListPrice
            ),
            row.field(row.cs_pricing.get_ext_tax(), &CsPricingExtTax),
            row.field(row.cs_pricing.get_coupon_amount(), &CsPricingCouponAmt),
            row.field(row.cs_pricing.get_ext_ship_cost(), &CsPricingExtShipCost),
            row.field(row.cs_pricing.get_net_paid(), &CsPricingNetPaid),
            row.field(
                row.cs_pricing.get_net_paid_including_tax(),
                &CsPricingNetPaidIncTax
            ),
            row.field(
                row.cs_pricing.get_net_paid_including_shipping(),
                &CsPricingNetPaidIncShip
            ),
            row.field(
                row.cs_pricing.get_net_paid_including_shipping_and_tax(),
                &CsPricingNetPaidIncShipTax
            ),
            row.field(row.cs_pricing.get_net_profit(), &CsPricingNetProfit),
        )
    }
}
/// Writes [`CustomerRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CustomerCsv, CsvRow};
/// # use dsbgen::row::CustomerRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Customer);
/// let generator = CustomerRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CustomerCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CustomerCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date_sk\n\
///    1,AAAAAAAABAAAAAAA,526469,4407,40007,2450005,2449975,Mr.,Jonathan,Gunter,N,16,2,1932,\"JAPAN\",,Jonathan.Gunter@8X.com,2452387\n\
///    2,AAAAAAAACAAAAAAA,1638916,2930,14189,2452130,2452100,Mr.,Lamar,Young,N,24,10,1982,\"GREECE\",,Lamar.Young@gSje47t7Ryo.edu,2452493\n\
///    3,AAAAAAAADAAAAAAA,395777,6030,5496,2450600,2450570,Mr.,Ricky,Crow,N,5,12,1938,\"WALLIS AND FUTUNA\",,Ricky.Crow@d6YrZaxpH.edu,2452289\n"
/// );
/// ```
pub struct CustomerCsv<'a> {
    inner: &'a CustomerRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CustomerCsv<'a> {
    type Row = CustomerRow;

    fn with_delimiter(inner: &'a CustomerRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date_sk"
    }
}

impl Display for CustomerCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::CustomerGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.c_customer_sk, CCustomerSk),
            row.field(&row.c_customer_id, CCustomerId),
            row.key_field(row.c_current_cdemo_sk, CCurrentCdemoSk),
            row.key_field(row.c_current_hdemo_sk, CCurrentHdemoSk),
            row.key_field(row.c_current_addr_sk, CCurrentAddrSk),
            row.field(row.c_first_shipto_date_id, CFirstShiptoDateId),
            row.field(row.c_first_sales_date_id, CFirstSalesDateId),
            row.field(&row.c_salutation, CSalutation),
            row.field(&row.c_first_name, CFirstName),
            row.field(&row.c_last_name, CLastName),
            DatField::yes_no(row.c_preferred_cust_flag, row.is_null(CPreferredCustFlag)),
            row.field(row.c_birth_day, CBirthDay),
            row.field(row.c_birth_month, CBirthMonth),
            row.field(row.c_birth_year, CBirthYear),
            CsvQuoted::new(&row.c_birth_country, row.is_null(CBirthCountry)),
            // c_login is emitted without a null check, like get_values()
            row.c_login.as_deref().unwrap_or_default(),
            row.field(&row.c_email_address, CEmailAddress),
            row.field(row.c_last_review_date, CLastReviewDate),
        )
    }
}
/// Writes [`CustomerAddressRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CustomerAddressCsv, CsvRow};
/// # use dsbgen::row::CustomerAddressRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CustomerAddress);
/// let generator = CustomerAddressRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CustomerAddressCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CustomerAddressCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "ca_address_sk,ca_address_id,ca_street_number,ca_street_name,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset,ca_location_type\n\
///    1,AAAAAAAABAAAAAAA,71,Davis ,Ln,Suite 40,Forestville,Knox County,MO,63027,United States,-6,apartment\n\
///    2,AAAAAAAACAAAAAAA,746,Johnson ,Parkway,Suite 220,Kimball,Lawrence County,TN,33595,United States,-6,apartment\n\
///    3,AAAAAAAADAAAAAAA,841,Elm ,Ave,Suite E,Creston,Hancock County,ME,01704,United States,-5,condo\n"
/// );
/// ```
pub struct CustomerAddressCsv<'a> {
    inner: &'a CustomerAddressRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CustomerAddressCsv<'a> {
    type Row = CustomerAddressRow;

    fn with_delimiter(inner: &'a CustomerAddressRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "ca_address_sk,ca_address_id,ca_street_number,ca_street_name,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset,ca_location_type"
    }
}

impl Display for CustomerAddressCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.ca_addr_sk, 0),
            row.field(&row.ca_addr_id, 1),
            row.field(row.ca_address.get_street_number(), 2),
            row.field(row.ca_address.get_street_name(), 3),
            row.field(row.ca_address.get_street_type(), 4),
            row.field(row.ca_address.get_suite_number(), 5),
            row.field(row.ca_address.get_city(), 6),
            row.field(row.ca_address.get_county().unwrap_or(""), 7),
            row.field(row.ca_address.get_state(), 8),
            DatField::zip(row.ca_address.get_zip(), row.should_be_null(9)),
            row.field(row.ca_address.get_country(), 10),
            row.field(row.ca_address.get_gmt_offset(), 11),
            row.field(&row.ca_location_type, 12),
        )
    }
}
/// Writes [`CustomerDemographicsRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{CustomerDemographicsCsv, CsvRow};
/// # use dsbgen::row::CustomerDemographicsRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::CustomerDemographics);
/// let generator = CustomerDemographicsRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", CustomerDemographicsCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", CustomerDemographicsCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "cd_demo_sk,cd_gender,cd_marital_status,cd_education_status,cd_purchase_estimate,cd_credit_rating,cd_dep_count,cd_dep_employed_count,cd_dep_college_count\n\
///    1,M,M,Primary,500,Good,0,0,0\n\
///    2,F,M,Primary,500,Good,0,0,0\n\
///    3,M,S,Primary,500,Good,0,0,0\n"
/// );
/// ```
pub struct CustomerDemographicsCsv<'a> {
    inner: &'a CustomerDemographicsRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for CustomerDemographicsCsv<'a> {
    type Row = CustomerDemographicsRow;

    fn with_delimiter(inner: &'a CustomerDemographicsRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "cd_demo_sk,cd_gender,cd_marital_status,cd_education_status,cd_purchase_estimate,cd_credit_rating,cd_dep_count,cd_dep_employed_count,cd_dep_college_count"
    }
}

impl Display for CustomerDemographicsCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.cd_demo_sk, 0),
            row.field(&row.cd_gender, 1),
            row.field(&row.cd_marital_status, 2),
            row.field(&row.cd_education_status, 3),
            row.field(row.cd_purchase_estimate, 4),
            row.field(&row.cd_credit_rating, 5),
            row.field(row.cd_dep_count, 6),
            row.field(row.cd_dep_employed_count, 7),
            row.field(row.cd_dep_college_count, 8),
        )
    }
}
/// Writes [`DateDimRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{DateDimCsv, CsvRow};
/// # use dsbgen::row::DateDimRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::DateDim);
/// let generator = DateDimRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", DateDimCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", DateDimCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "d_date_sk,d_date_id,d_date,d_month_seq,d_week_seq,d_quarter_seq,d_year,d_dow,d_moy,d_dom,d_qoy,d_fy_year,d_fy_quarter_seq,d_fy_week_seq,d_day_name,d_quarter_name,d_holiday,d_weekend,d_following_holiday,d_first_dom,d_last_dom,d_same_day_ly,d_same_day_lq,d_current_day,d_current_week,d_current_month,d_current_quarter,d_current_year\n\
///    2415022,AAAAAAAAOKJNECAA,1900-01-02,0,1,1,1900,1,1,2,1,1900,1,1,Monday,1900Q1,N,N,Y,2415021,2415020,2414657,2414930,N,N,N,N,N\n\
///    2415023,AAAAAAAAPKJNECAA,1900-01-03,0,1,1,1900,2,1,3,1,1900,1,1,Tuesday,1900Q1,N,N,N,2415021,2415020,2414658,2414931,N,N,N,N,N\n\
///    2415024,AAAAAAAAALJNECAA,1900-01-04,0,1,1,1900,3,1,4,1,1900,1,1,Wednesday,1900Q1,N,N,N,2415021,2415020,2414659,2414932,N,N,N,N,N\n"
/// );
/// ```
pub struct DateDimCsv<'a> {
    inner: &'a DateDimRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for DateDimCsv<'a> {
    type Row = DateDimRow;

    fn with_delimiter(inner: &'a DateDimRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "d_date_sk,d_date_id,d_date,d_month_seq,d_week_seq,d_quarter_seq,d_year,d_dow,d_moy,d_dom,d_qoy,d_fy_year,d_fy_quarter_seq,d_fy_week_seq,d_day_name,d_quarter_name,d_holiday,d_weekend,d_following_holiday,d_first_dom,d_last_dom,d_same_day_ly,d_same_day_lq,d_current_day,d_current_week,d_current_month,d_current_quarter,d_current_year"
    }
}

impl Display for DateDimCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.d_date_sk, 0),
            row.field(&row.d_date_id, 1),
            row.field(&row.d_date, 2),
            row.field(row.d_month_seq, 3),
            row.field(row.d_week_seq, 4),
            row.field(row.d_quarter_seq, 5),
            row.field(row.d_year, 6),
            row.field(row.d_dow, 7),
            row.field(row.d_moy, 8),
            row.field(row.d_dom, 9),
            row.field(row.d_qoy, 10),
            row.field(row.d_fy_year, 11),
            row.field(row.d_fy_quarter_seq, 12),
            row.field(row.d_fy_week_seq, 13),
            row.field(&row.d_day_name, 14),
            row.field(&row.d_quarter_name, 15),
            row.field(DateDimRow::format_boolean(row.d_holiday), 16),
            row.field(DateDimRow::format_boolean(row.d_weekend), 17),
            row.field(DateDimRow::format_boolean(row.d_following_holiday), 18),
            row.field(row.d_first_dom, 19),
            row.field(row.d_last_dom, 20),
            row.field(row.d_same_day_ly, 21),
            row.field(row.d_same_day_lq, 22),
            row.field(DateDimRow::format_boolean(row.d_current_day), 23),
            row.field(DateDimRow::format_boolean(row.d_current_week), 24),
            row.field(DateDimRow::format_boolean(row.d_current_month), 25),
            row.field(DateDimRow::format_boolean(row.d_current_quarter), 26),
            row.field(DateDimRow::format_boolean(row.d_current_year), 27),
        )
    }
}
/// Writes [`DbgenVersionRow`]s in CSV format.
///
/// # Example
///
/// Note the `dbgen_version` row records the time the data was generated and
/// the command line used, neither of which is reproducible, so this example
/// checks only the stable parts of the line.
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{DbgenVersionCsv, CsvRow};
/// # use dsbgen::row::DbgenVersionRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::DbgenVersion);
/// let generator = DbgenVersionRowGenerator::new(session, row_count);
/// // Output the first row in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", DbgenVersionCsv::header()).unwrap(); // write header
/// for row in generator.take(1) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", DbgenVersionCsv::new(&row)).unwrap();
/// }
/// let mut lines = csv.lines();
/// assert_eq!(
///   lines.next().unwrap(),
///   "dv_version,dv_create_date,dv_create_time,dv_cmdline_args"
/// );
/// // the version is stable, but the creation date/time and command line are not
/// let data = lines.next().unwrap();
/// assert!(data.starts_with("2.0.0,"), "unexpected line: {data}");
/// assert_eq!(data.split(',').count(), 4, "unexpected line: {data}");
/// assert!(lines.next().is_none());
/// ```
pub struct DbgenVersionCsv<'a> {
    inner: &'a DbgenVersionRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for DbgenVersionCsv<'a> {
    type Row = DbgenVersionRow;

    fn with_delimiter(inner: &'a DbgenVersionRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "dv_version,dv_create_date,dv_create_time,dv_cmdline_args"
    }
}

impl Display for DbgenVersionCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}",
            row.field(&row.dv_version, 0),
            row.field(row.dv_create_date, 1),
            row.field(TimeOfDay(row.dv_create_time), 2),
            row.field(&row.dv_cmdline_args, 3),
        )
    }
}
/// Writes [`HouseholdDemographicsRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{HouseholdDemographicsCsv, CsvRow};
/// # use dsbgen::row::HouseholdDemographicsRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::HouseholdDemographics);
/// let generator = HouseholdDemographicsRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", HouseholdDemographicsCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", HouseholdDemographicsCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "hd_demo_sk,hd_income_band_sk,hd_buy_potential,hd_dep_count,hd_vehicle_count\n\
///    1,2,0-500,0,0\n\
///    2,3,0-500,0,0\n\
///    3,4,0-500,0,0\n"
/// );
/// ```
pub struct HouseholdDemographicsCsv<'a> {
    inner: &'a HouseholdDemographicsRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for HouseholdDemographicsCsv<'a> {
    type Row = HouseholdDemographicsRow;

    fn with_delimiter(inner: &'a HouseholdDemographicsRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "hd_demo_sk,hd_income_band_sk,hd_buy_potential,hd_dep_count,hd_vehicle_count"
    }
}

impl Display for HouseholdDemographicsCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}",
            NullLiteralField::new(row.hd_demo_sk, row.is_null(0)),
            NullLiteralField::new(row.hd_income_band_sk, row.is_null(1)),
            NullLiteralField::new(&row.hd_buy_potential, row.is_null(2)),
            NullLiteralField::new(row.hd_dep_count, row.is_null(3)),
            NullLiteralField::new(row.hd_vehicle_count, row.is_null(4)),
        )
    }
}
/// Writes [`IncomeBandRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{IncomeBandCsv, CsvRow};
/// # use dsbgen::row::IncomeBandRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::IncomeBand);
/// let generator = IncomeBandRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", IncomeBandCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", IncomeBandCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "ib_income_band_sk,ib_lower_bound,ib_upper_bound\n\
///    1,0,10000\n\
///    2,10001,20000\n\
///    3,20001,30000\n"
/// );
/// ```
pub struct IncomeBandCsv<'a> {
    inner: &'a IncomeBandRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for IncomeBandCsv<'a> {
    type Row = IncomeBandRow;

    fn with_delimiter(inner: &'a IncomeBandRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "ib_income_band_sk,ib_lower_bound,ib_upper_bound"
    }
}

impl Display for IncomeBandCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}",
            row.field(row.ib_income_band_sk, 0),
            row.field(row.ib_lower_bound, 1),
            row.field(row.ib_upper_bound, 2),
        )
    }
}
/// Writes [`InventoryRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{InventoryCsv, CsvRow};
/// # use dsbgen::row::InventoryRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Inventory);
/// let generator = InventoryRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", InventoryCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", InventoryCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "inv_date_sk,inv_item_sk,inv_warehouse_sk,inv_quantity_on_hand\n\
///    2450815,1,1,732\n\
///    2450815,2,1,321\n\
///    2450815,4,1,241\n"
/// );
/// ```
pub struct InventoryCsv<'a> {
    inner: &'a InventoryRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for InventoryCsv<'a> {
    type Row = InventoryRow;

    fn with_delimiter(inner: &'a InventoryRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "inv_date_sk,inv_item_sk,inv_warehouse_sk,inv_quantity_on_hand"
    }
}

impl Display for InventoryCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::InventoryGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}",
            DatField::new(row.inv_date_sk, row.is_null_at(InvDateSk)),
            DatField::new(row.inv_item_sk, row.is_null_at(InvItemSk)),
            DatField::new(row.inv_warehouse_sk, row.is_null_at(InvWarehouseSk)),
            DatField::new(row.inv_quantity_on_hand, row.is_null_at(InvQuantityOnHand)),
        )
    }
}
/// Writes [`ItemRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{ItemCsv, CsvRow};
/// # use dsbgen::row::ItemRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Item);
/// let generator = ItemRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", ItemCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", ItemCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name\n\
///    1,AAAAAAAABAAAAAAA,1997-12-26,,\"Lines could delive\",1.96,0.62,3001002,amalgexporti #2,0,newborn,3,Children,259,n stantiable,petite,5687137aquamarine622,smoke,Carton,Unknown,86,ought\n\
///    2,AAAAAAAACAAAAAAA,1997-12-26,2000-12-25,\"Firm centres demonstrate in a houses; fairly free months contribute more also labour sales; elections c\",0.74,0.23,6010003,univbrand #3,10,jewelry boxes,6,Jewelry,326,callyablepri,N/A,8light82873159702475,almond,Dram,Unknown,100,able\n\
///    3,AAAAAAAACAAAAAAA,2000-12-26,,\"Firm centres demonstrate in a houses; fairly free months contribute more also labour sales; elections c\",5.35,0.23,2001002,amalgimporto #2,1,accessories,2,Men,326,callyablepri,petite,8light82873159702475,linen,Bunch,Unknown,44,pri\n"
/// );
/// ```
pub struct ItemCsv<'a> {
    inner: &'a ItemRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for ItemCsv<'a> {
    type Row = ItemRow;

    fn with_delimiter(inner: &'a ItemRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name"
    }
}

impl Display for ItemCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::ItemGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.i_item_sk, &IItemSk),
            row.field(&row.i_item_id, &IItemId),
            row.date_field(row.i_rec_start_date_id, &IRecStartDateId),
            row.date_field(row.i_rec_end_date_id, &IRecEndDateId),
            CsvQuoted::new(&row.i_item_desc, row.is_null(&IItemDesc)),
            row.field(row.i_current_price, &ICurrentPrice),
            row.field(row.i_wholesale_cost, &IWholesaleCost),
            row.field(row.i_brand_id, &IBrandId),
            row.field(&row.i_brand, &IBrand),
            row.field(row.i_class_id, &IClassId),
            row.field(&row.i_class, &IClass),
            row.field(row.i_category_id, &ICategoryId),
            row.field(&row.i_category, &ICategory),
            row.field(row.i_manufact_id, &IManufactId),
            row.field(&row.i_manufact, &IManufact),
            row.field(&row.i_size, &ISize),
            row.field(&row.i_formulation, &IFormulation),
            row.field(&row.i_color, &IColor),
            row.field(&row.i_units, &IUnits),
            row.field(&row.i_container, &IContainer),
            row.field(row.i_manager_id, &IManagerId),
            row.field(&row.i_product_name, &IProductName),
        )
    }
}
/// Writes [`PromotionRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{PromotionCsv, CsvRow};
/// # use dsbgen::row::PromotionRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Promotion);
/// let generator = PromotionRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", PromotionCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", PromotionCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "p_promo_sk,p_promo_id,p_start_date_sk,p_end_date_sk,p_item_sk,p_cost,p_response_target,p_promo_name,p_channel_dmail,p_channel_email,p_channel_catalog,p_channel_tv,p_channel_radio,p_channel_press,p_channel_event,p_channel_demo,p_channel_details,p_purpose,p_discount_active\n\
///    1,AAAAAAAABAAAAAAA,2450597,2450600,8149,1000.00,1,ought,N,Y,N,Y,Y,Y,N,N,\"Men will not say merely. Old, available \",Unknown,N\n\
///    2,AAAAAAAACAAAAAAA,2450230,2450278,3166,1000.00,1,able,Y,N,N,Y,Y,Y,N,Y,\"So willing buildings coul\",Unknown,Y\n\
///    3,AAAAAAAADAAAAAAA,2450204,2450241,7045,1000.00,1,pri,N,Y,N,Y,Y,N,Y,N,\"Companies shall not pr\",Unknown,Y\n"
/// );
/// ```
pub struct PromotionCsv<'a> {
    inner: &'a PromotionRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for PromotionCsv<'a> {
    type Row = PromotionRow;

    fn with_delimiter(inner: &'a PromotionRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "p_promo_sk,p_promo_id,p_start_date_sk,p_end_date_sk,p_item_sk,p_cost,p_response_target,p_promo_name,p_channel_dmail,p_channel_email,p_channel_catalog,p_channel_tv,p_channel_radio,p_channel_press,p_channel_event,p_channel_demo,p_channel_details,p_purpose,p_discount_active"
    }
}

impl Display for PromotionCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::PromotionGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.p_promo_sk, PPromoSk),
            row.field(&row.p_promo_id, PPromoId),
            row.key_field(row.p_start_date_id, PStartDateId),
            row.key_field(row.p_end_date_id, PEndDateId),
            row.key_field(row.p_item_sk, PItemSk),
            row.field(row.p_cost, PCost),
            row.field(row.p_response_target, PResponseTarget),
            row.field(&row.p_promo_name, PPromoName),
            DatField::yes_no(row.p_channel_dmail, row.is_null_at(PChannelDmail)),
            DatField::yes_no(row.p_channel_email, row.is_null_at(PChannelEmail)),
            DatField::yes_no(row.p_channel_catalog, row.is_null_at(PChannelCatalog)),
            DatField::yes_no(row.p_channel_tv, row.is_null_at(PChannelTv)),
            DatField::yes_no(row.p_channel_radio, row.is_null_at(PChannelRadio)),
            DatField::yes_no(row.p_channel_press, row.is_null_at(PChannelPress)),
            DatField::yes_no(row.p_channel_event, row.is_null_at(PChannelEvent)),
            DatField::yes_no(row.p_channel_demo, row.is_null_at(PChannelDemo)),
            CsvQuoted::new(&row.p_channel_details, row.is_null_at(PChannelDetails)),
            row.field(&row.p_purpose, PPurpose),
            DatField::yes_no(row.p_discount_active, row.is_null_at(PDiscountActive)),
        )
    }
}
/// Writes [`ReasonRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{ReasonCsv, CsvRow};
/// # use dsbgen::row::ReasonRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Reason);
/// let generator = ReasonRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", ReasonCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", ReasonCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "r_reason_sk,r_reason_id,r_reason_desc\n\
///    1,AAAAAAAABAAAAAAA,Package was damaged\n\
///    2,AAAAAAAACAAAAAAA,Stopped working\n\
///    3,AAAAAAAADAAAAAAA,Did not get it on time\n"
/// );
/// ```
pub struct ReasonCsv<'a> {
    inner: &'a ReasonRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for ReasonCsv<'a> {
    type Row = ReasonRow;

    fn with_delimiter(inner: &'a ReasonRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "r_reason_sk,r_reason_id,r_reason_desc"
    }
}

impl Display for ReasonCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}",
            row.field(row.r_reason_sk, 0),
            row.field(&row.r_reason_id, 1),
            row.field(&row.r_reason_desc, 2),
        )
    }
}
/// Writes [`ShipModeRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{ShipModeCsv, CsvRow};
/// # use dsbgen::row::ShipModeRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::ShipMode);
/// let generator = ShipModeRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", ShipModeCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", ShipModeCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "sm_ship_mode_sk,sm_ship_mode_id,sm_type,sm_code,sm_carrier,sm_contract\n\
///    1,AAAAAAAABAAAAAAA,EXPRESS,AIR,UPS,YvxVaJI10\n\
///    2,AAAAAAAACAAAAAAA,NEXT DAY,AIR,FEDEX,ldhM8IvpzHgdbBgDfI\n\
///    3,AAAAAAAADAAAAAAA,OVERNIGHT,AIR,AIRBORNE,6Hzzp4JkzjqD8MGXLCDa\n"
/// );
/// ```
pub struct ShipModeCsv<'a> {
    inner: &'a ShipModeRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for ShipModeCsv<'a> {
    type Row = ShipModeRow;

    fn with_delimiter(inner: &'a ShipModeRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "sm_ship_mode_sk,sm_ship_mode_id,sm_type,sm_code,sm_carrier,sm_contract"
    }
}

impl Display for ShipModeCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.sm_ship_mode_sk, 0),
            row.field(&row.sm_ship_mode_id, 1),
            row.field(&row.sm_type, 2),
            row.field(&row.sm_code, 3),
            row.field(&row.sm_carrier, 4),
            row.field(&row.sm_contract, 5),
        )
    }
}
/// Writes [`StoreRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{StoreCsv, CsvRow};
/// # use dsbgen::row::StoreRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Store);
/// let generator = StoreRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", StoreCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", StoreCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "s_store_sk,s_store_id,s_rec_start_date,s_rec_end_date,s_closed_date_sk,s_store_name,s_number_employees,s_floor_space,s_hours,s_manager,s_market_id,s_geography_class,s_market_desc,s_market_manager,s_division_id,s_division_name,s_company_id,s_company_name,s_street_number,s_street_name,s_street_type,s_suite_number,s_city,s_county,s_state,s_zip,s_country,s_gmt_offset,s_tax_precentage\n\
///    1,AAAAAAAABAAAAAAA,1997-03-13,,2451189,ought,266,7447677,8AM-4PM,William Ward,3,Unknown,\"Enough high areas stop expectations. Elaborate, local is\",Charles Bartley,1,Unknown,1,Unknown,721,Poplar ,Ave,Suite 250,Pleasant Hill,Richland County,OH,43604,United States,-5,0.03\n\
///    2,AAAAAAAACAAAAAAA,1997-03-13,2000-03-12,,able,249,8860176,8AM-4PM,Scott Smith,7,Unknown,\"Parliamentary candidates wait then heavy, keen mil\",David Lamontagne,1,Unknown,1,Unknown,994,Pine ,Avenue,Suite 410,Midway,Ziebach County,SD,51904,United States,-6,0.03\n\
///    3,AAAAAAAACAAAAAAA,2000-03-13,,,able,249,8187125,8AM-4PM,Scott Smith,3,Unknown,\"Impossible, true arms can treat constant, complete w\",David Lamontagne,1,Unknown,1,Unknown,647,Pine Laurel,Ln,Suite T,Mount Pleasant,Jefferson Davis Parish,LA,71933,United States,-6,0.03\n"
/// );
/// ```
pub struct StoreCsv<'a> {
    inner: &'a StoreRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for StoreCsv<'a> {
    type Row = StoreRow;

    fn with_delimiter(inner: &'a StoreRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "s_store_sk,s_store_id,s_rec_start_date,s_rec_end_date,s_closed_date_sk,s_store_name,s_number_employees,s_floor_space,s_hours,s_manager,s_market_id,s_geography_class,s_market_desc,s_market_manager,s_division_id,s_division_name,s_company_id,s_company_name,s_street_number,s_street_name,s_street_type,s_suite_number,s_city,s_county,s_state,s_zip,s_country,s_gmt_offset,s_tax_precentage"
    }
}

impl Display for StoreCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::StoreGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.store_sk, &WStoreSk),
            row.field(&row.store_id, &WStoreId),
            row.date_field(row.rec_start_date_id, &WStoreRecStartDateId),
            row.date_field(row.rec_end_date_id, &WStoreRecEndDateId),
            row.key_field(row.closed_date_id, &WStoreClosedDateId),
            row.field(&row.store_name, &WStoreName),
            row.field(row.employees, &WStoreEmployees),
            row.field(row.floor_space, &WStoreFloorSpace),
            row.field(&row.hours, &WStoreHours),
            row.field(&row.store_manager, &WStoreManager),
            row.field(row.market_id, &WStoreMarketId),
            row.field(&row.geography_class, &WStoreGeographyClass),
            CsvQuoted::new(&row.market_desc, row.is_null(&WStoreMarketDesc)),
            row.field(&row.market_manager, &WStoreMarketManager),
            row.key_field(row.division_id, &WStoreDivisionId),
            row.field(&row.division_name, &WStoreDivisionName),
            row.key_field(row.company_id, &WStoreCompanyId),
            row.field(&row.company_name, &WStoreCompanyName),
            row.field(row.address.get_street_number(), &WStoreAddressStreetNum),
            row.field(row.address.get_street_name(), &WStoreAddressStreetName1),
            row.field(row.address.get_street_type(), &WStoreAddressStreetType),
            row.field(row.address.get_suite_number(), &WStoreAddressSuiteNum),
            row.field(row.address.get_city(), &WStoreAddressCity),
            row.field(row.address.get_county().unwrap_or(""), &WStoreAddressCounty),
            row.field(row.address.get_state(), &WStoreAddressState),
            DatField::zip(row.address.get_zip(), row.is_null(&WStoreAddressZip)),
            row.field(row.address.get_country(), &WStoreAddressCountry),
            row.field(row.address.get_gmt_offset(), &WStoreAddressGmtOffset),
            row.field(row.d_tax_percentage, &WStoreTaxPercentage),
        )
    }
}
/// Writes [`StoreReturnsRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::Session;
/// # use dsbgen::csv::{StoreReturnsCsv, CsvRow};
/// # use dsbgen::row::StoreReturnsRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// // Output the returns of the first 2 store_sales tickets in CSV format
/// let generator = StoreReturnsRowGenerator::new(session, 2);
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", StoreReturnsCsv::header()).unwrap(); // write header
/// for row in generator {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", StoreReturnsCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "sr_returned_date_sk,sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,sr_return_quantity,sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,sr_return_ship_cost,sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss\n\
///    2450986,46418,3302,78443,375811,3016,35,1,2,1,5,89.25,6.24,95.49,55.28,125.60,1.78,41.98,45.49,187.12\n\
///    2450824,47480,17551,78443,1228709,2291,9154,2,5,1,2,122.60,8.58,131.18,28.23,0.00,105.43,0.17,17.00,36.81\n\
///    2450912,41562,12994,78443,711303,6478,30992,7,22,1,27,679.59,47.57,727.16,85.02,855.63,299.01,262.60,117.98,988.22\n\
///    2450836,60999,13484,78443,366911,719,8692,4,3,1,61,1973.96,98.69,2072.65,5.67,1123.01,276.35,509.28,1188.33,1227.37\n\
///    2450848,36227,8522,78443,1576601,1549,19191,4,12,1,1,47.11,3.76,50.87,25.86,61.07,4.23,30.44,12.44,90.69\n\
///    2450944,37700,4414,46210,1210472,140,7870,8,11,2,22,423.28,4.23,427.51,11.50,26.40,397.88,14.98,10.42,42.13\n\
///    2450912,38831,3884,46210,1184982,1492,26228,2,21,2,66,6679.20,267.16,6946.36,17.91,1496.88,3740.35,558.38,2380.47,1781.95\n\
///    2450917,54062,937,191,1377762,5860,32516,2,2,2,29,969.76,77.58,1047.34,37.27,837.52,930.96,22.11,16.69,952.37\n\
///    2450845,56039,13280,46210,1499281,3954,8363,1,31,2,1,1.96,0.03,1.99,39.44,10.80,1.19,0.03,0.74,50.27\n"
/// );
/// ```
pub struct StoreReturnsCsv<'a> {
    inner: &'a StoreReturnsRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for StoreReturnsCsv<'a> {
    type Row = StoreReturnsRow;

    fn with_delimiter(inner: &'a StoreReturnsRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "sr_returned_date_sk,sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,sr_return_quantity,sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,sr_return_ship_cost,sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss"
    }
}

impl Display for StoreReturnsCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::StoreReturnsGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            DatField::key(row.sr_returned_date_sk, row.is_null_at(SrReturnedDateSk)),
            DatField::key(row.sr_returned_time_sk, row.is_null_at(SrReturnedTimeSk)),
            DatField::key(row.sr_item_sk, row.is_null_at(SrItemSk)),
            DatField::key(row.sr_customer_sk, row.is_null_at(SrCustomerSk)),
            DatField::key(row.sr_cdemo_sk, row.is_null_at(SrCdemoSk)),
            DatField::key(row.sr_hdemo_sk, row.is_null_at(SrHdemoSk)),
            DatField::key(row.sr_addr_sk, row.is_null_at(SrAddrSk)),
            DatField::key(row.sr_store_sk, row.is_null_at(SrStoreSk)),
            DatField::key(row.sr_reason_sk, row.is_null_at(SrReasonSk)),
            DatField::key(row.sr_ticket_number, row.is_null_at(SrTicketNumber)),
            DatField::new(
                row.sr_pricing.get_quantity(),
                row.is_null_at(SrPricingQuantity)
            ),
            DatField::new(
                row.sr_pricing.get_net_paid(),
                row.is_null_at(SrPricingNetPaid)
            ),
            DatField::new(
                row.sr_pricing.get_ext_tax(),
                row.is_null_at(SrPricingExtTax)
            ),
            DatField::new(
                row.sr_pricing.get_net_paid_including_tax(),
                row.is_null_at(SrPricingNetPaidIncTax)
            ),
            DatField::new(row.sr_pricing.get_fee(), row.is_null_at(SrPricingFee)),
            DatField::new(
                row.sr_pricing.get_ext_ship_cost(),
                row.is_null_at(SrPricingExtShipCost)
            ),
            DatField::new(
                row.sr_pricing.get_refunded_cash(),
                row.is_null_at(SrPricingRefundedCash)
            ),
            DatField::new(
                row.sr_pricing.get_reversed_charge(),
                row.is_null_at(SrPricingReversedCharge)
            ),
            DatField::new(
                row.sr_pricing.get_store_credit(),
                row.is_null_at(SrPricingStoreCredit)
            ),
            DatField::new(
                row.sr_pricing.get_net_loss(),
                row.is_null_at(SrPricingNetLoss)
            ),
        )
    }
}
/// Writes [`StoreSalesRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{StoreSalesCsv, CsvRow};
/// # use dsbgen::row::StoreSalesRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::StoreSales);
/// let generator = StoreSalesRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", StoreSalesCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", StoreSalesCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit\n\
///    2450815,65495,17881,78443,1751638,5428,27568,10,123,1,97,1.00,1.94,0.52,0.00,50.44,97.00,188.18,2.01,0.00,50.44,52.45,-46.56\n\
///    2450815,65495,3302,78443,1751638,5428,27568,10,164,1,94,32.26,66.13,17.85,1174.53,1677.90,3032.44,6216.22,35.23,1174.53,503.37,538.60,-2529.07\n\
///    2450815,65495,17551,78443,1751638,5428,27568,10,32,1,4,47.89,82.84,61.30,186.35,245.20,191.56,331.36,4.11,186.35,58.85,62.96,-132.71\n"
/// );
/// ```
pub struct StoreSalesCsv<'a> {
    inner: &'a StoreSalesRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for StoreSalesCsv<'a> {
    type Row = StoreSalesRow;

    fn with_delimiter(inner: &'a StoreSalesRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit"
    }
}

impl Display for StoreSalesCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::StoreSalesGeneratorColumn::*;
        use crate::row::table_row::DatField;

        let d = self.delimiter;
        let row = self.inner;

        // Note: Java has coupon_amount twice at positions 15 and 20 (bug in original)
        // We replicate this for byte-for-byte compatibility
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            DatField::key(row.ss_sold_date_sk, row.is_null_at(SsSoldDateSk)),
            DatField::key(row.ss_sold_time_sk, row.is_null_at(SsSoldTimeSk)),
            DatField::key(row.ss_sold_item_sk, row.is_null_at(SsSoldItemSk)),
            DatField::key(row.ss_sold_customer_sk, row.is_null_at(SsSoldCustomerSk)),
            DatField::key(row.ss_sold_cdemo_sk, row.is_null_at(SsSoldCdemoSk)),
            DatField::key(row.ss_sold_hdemo_sk, row.is_null_at(SsSoldHdemoSk)),
            DatField::key(row.ss_sold_addr_sk, row.is_null_at(SsSoldAddrSk)),
            DatField::key(row.ss_sold_store_sk, row.is_null_at(SsSoldStoreSk)),
            DatField::key(row.ss_sold_promo_sk, row.is_null_at(SsSoldPromoSk)),
            DatField::key(row.ss_ticket_number, row.is_null_at(SsTicketNumber)),
            DatField::new(
                row.ss_pricing.get_quantity(),
                row.is_null_at(SsPricingQuantity)
            ),
            DatField::new(
                row.ss_pricing.get_wholesale_cost(),
                row.is_null_at(SsPricingWholesaleCost)
            ),
            DatField::new(
                row.ss_pricing.get_list_price(),
                row.is_null_at(SsPricingListPrice)
            ),
            DatField::new(
                row.ss_pricing.get_sales_price(),
                row.is_null_at(SsPricingSalesPrice)
            ),
            DatField::new(
                row.ss_pricing.get_coupon_amount(),
                row.is_null_at(SsPricingCouponAmt)
            ),
            DatField::new(
                row.ss_pricing.get_ext_sales_price(),
                row.is_null_at(SsPricingExtSalesPrice)
            ),
            DatField::new(
                row.ss_pricing.get_ext_wholesale_cost(),
                row.is_null_at(SsPricingExtWholesaleCost)
            ),
            DatField::new(
                row.ss_pricing.get_ext_list_price(),
                row.is_null_at(SsPricingExtListPrice)
            ),
            DatField::new(
                row.ss_pricing.get_ext_tax(),
                row.is_null_at(SsPricingExtTax)
            ),
            DatField::new(
                row.ss_pricing.get_coupon_amount(),
                row.is_null_at(SsPricingCouponAmt)
            ),
            DatField::new(
                row.ss_pricing.get_net_paid(),
                row.is_null_at(SsPricingNetPaid)
            ),
            DatField::new(
                row.ss_pricing.get_net_paid_including_tax(),
                row.is_null_at(SsPricingNetPaidIncTax)
            ),
            DatField::new(
                row.ss_pricing.get_net_profit(),
                row.is_null_at(SsPricingNetProfit)
            ),
        )
    }
}
/// Writes [`TimeDimRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{TimeDimCsv, CsvRow};
/// # use dsbgen::row::TimeDimRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::TimeDim);
/// let generator = TimeDimRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", TimeDimCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", TimeDimCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "t_time_sk,t_time_id,t_time,t_hour,t_minute,t_second,t_am_pm,t_shift,t_sub_shift,t_meal_time\n\
///    0,AAAAAAAABAAAAAAA,0,0,0,0,AM,third,night,\n\
///    1,AAAAAAAACAAAAAAA,1,0,0,1,AM,third,night,\n\
///    2,AAAAAAAADAAAAAAA,2,0,0,2,AM,third,night,\n"
/// );
/// ```
pub struct TimeDimCsv<'a> {
    inner: &'a TimeDimRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for TimeDimCsv<'a> {
    type Row = TimeDimRow;

    fn with_delimiter(inner: &'a TimeDimRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "t_time_sk,t_time_id,t_time,t_hour,t_minute,t_second,t_am_pm,t_shift,t_sub_shift,t_meal_time"
    }
}

impl Display for TimeDimCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.t_time_sk, 0),
            row.field(&row.t_time_id, 1),
            row.field(row.t_time, 2),
            row.field(row.t_hour, 3),
            row.field(row.t_minute, 4),
            row.field(row.t_second, 5),
            row.field(&row.t_am_pm, 6),
            row.field(&row.t_shift, 7),
            row.field(&row.t_sub_shift, 8),
            row.field(&row.t_meal_time, 9),
        )
    }
}
/// Writes [`WarehouseRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{WarehouseCsv, CsvRow};
/// # use dsbgen::row::WarehouseRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::Warehouse);
/// let generator = WarehouseRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", WarehouseCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", WarehouseCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "w_warehouse_sk,w_warehouse_id,w_warehouse_name,w_warehouse_sq_ft,w_street_number,w_street_name,w_street_type,w_suite_number,w_city,w_county,w_state,w_zip,w_country,w_gmt_offset\n\
///    1,AAAAAAAABAAAAAAA,\"Conventional childr\",829206,581,Johnson ,Way,Suite 470,Pleasant Hill,Williamson County,TN,33604,United States,-5\n\
///    2,AAAAAAAACAAAAAAA,\"Important issues liv\",76496,96,Highland First,RD,Suite P,Fairview,Ziebach County,SD,55709,United States,-6\n\
///    3,AAAAAAAADAAAAAAA,\"Doors canno\",789610,953,Lee Laurel,Road,Suite 0,Five Points,Ziebach County,SD,56098,United States,-6\n"
/// );
/// ```
pub struct WarehouseCsv<'a> {
    inner: &'a WarehouseRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for WarehouseCsv<'a> {
    type Row = WarehouseRow;

    fn with_delimiter(inner: &'a WarehouseRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "w_warehouse_sk,w_warehouse_id,w_warehouse_name,w_warehouse_sq_ft,w_street_number,w_street_name,w_street_type,w_suite_number,w_city,w_county,w_state,w_zip,w_country,w_gmt_offset"
    }
}

impl Display for WarehouseCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::row::table_row::{CsvQuoted, DatField};

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.w_warehouse_sk, 0),
            row.field(&row.w_warehouse_id, 1),
            CsvQuoted::new(&row.w_warehouse_name, row.should_be_null(2)),
            row.field(row.w_warehouse_sq_ft, 3),
            row.field(row.w_address.get_street_number(), 4),
            row.field(row.w_address.get_street_name(), 5),
            row.field(row.w_address.get_street_type(), 6),
            row.field(row.w_address.get_suite_number(), 7),
            row.field(row.w_address.get_city(), 8),
            row.field(row.w_address.get_county().unwrap_or(""), 9),
            row.field(row.w_address.get_state(), 10),
            DatField::zip(row.w_address.get_zip(), row.should_be_null(11)),
            row.field(row.w_address.get_country(), 12),
            row.field(row.w_address.get_gmt_offset(), 13),
        )
    }
}
/// Writes [`WebPageRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{WebPageCsv, CsvRow};
/// # use dsbgen::row::WebPageRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::WebPage);
/// let generator = WebPageRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", WebPageCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", WebPageCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "wp_web_page_sk,wp_web_page_id,wp_rec_start_date,wp_rec_end_date,wp_creation_date_sk,wp_access_date_sk,wp_autogen_flag,wp_customer_sk,wp_url,wp_type,wp_char_count,wp_link_count,wp_image_count,wp_max_ad_count\n\
///    1,AAAAAAAABAAAAAAA,1997-09-03,,2450810,2452620,Y,42772,http://www.foo.com,order,2002,11,1,4\n\
///    2,AAAAAAAACAAAAAAA,1997-09-03,2000-09-02,2450814,2452580,N,,http://www.foo.com,protected,1891,10,5,4\n\
///    3,AAAAAAAACAAAAAAA,2000-09-03,,2450814,2452611,N,,http://www.foo.com,protected,1891,10,5,3\n"
/// );
/// ```
pub struct WebPageCsv<'a> {
    inner: &'a WebPageRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for WebPageCsv<'a> {
    type Row = WebPageRow;

    fn with_delimiter(inner: &'a WebPageRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "wp_web_page_sk,wp_web_page_id,wp_rec_start_date,wp_rec_end_date,wp_creation_date_sk,wp_access_date_sk,wp_autogen_flag,wp_customer_sk,wp_url,wp_type,wp_char_count,wp_link_count,wp_image_count,wp_max_ad_count"
    }
}

impl Display for WebPageCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::row::table_row::DatField;

        let d = self.delimiter;
        let row = self.inner;
        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.wp_page_sk, 0),
            row.field(&row.wp_page_id, 1),
            row.date_field(row.wp_rec_start_date_id, 2),
            row.date_field(row.wp_rec_end_date_id, 3),
            row.key_field(row.wp_creation_date_sk, 4),
            row.key_field(row.wp_access_date_sk, 5),
            DatField::yes_no(row.wp_autogen_flag, row.should_be_null(6)),
            row.key_field(row.wp_customer_sk, 7),
            row.field(&row.wp_url, 8),
            row.field(&row.wp_type, 9),
            row.field(row.wp_char_count, 10),
            row.field(row.wp_link_count, 11),
            row.field(row.wp_image_count, 12),
            row.field(row.wp_max_ad_count, 13),
        )
    }
}
/// Writes [`WebReturnsRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{WebReturnsCsv, CsvRow};
/// # use dsbgen::row::WebReturnsRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::WebSales);
/// let generator = WebReturnsRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", WebReturnsCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", WebReturnsCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,wr_reversed_charge,wr_account_credit,wr_net_loss\n\
///    2451085,7022,1394,3832,629630,3930,4500,3832,629630,3930,4500,60,8,1,3,532.86,0.00,532.86,18.63,338.43,229.12,291.59,12.15,357.06\n\
///    2451059,64915,7909,19283,1182083,1113,5658,19283,1182083,1113,5658,50,31,1,19,378.29,30.26,408.55,61.81,178.41,79.44,38.85,260.00,270.48\n\
///    2450988,,13436,,1189482,,14884,,1189482,6982,14884,,2,2,26,,35.37,,,47.58,,90.73,917.48,\n"
/// );
/// ```
pub struct WebReturnsCsv<'a> {
    inner: &'a WebReturnsRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for WebReturnsCsv<'a> {
    type Row = WebReturnsRow;

    fn with_delimiter(inner: &'a WebReturnsRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,wr_reversed_charge,wr_account_credit,wr_net_loss"
    }
}

impl Display for WebReturnsCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::WebReturnsGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.wr_returned_date_sk, WrReturnedDateSk),
            row.field(row.wr_returned_time_sk, WrReturnedTimeSk),
            row.field(row.wr_item_sk, WrItemSk),
            row.field(row.wr_refunded_customer_sk, WrRefundedCustomerSk),
            row.field(row.wr_refunded_cdemo_sk, WrRefundedCdemoSk),
            row.field(row.wr_refunded_hdemo_sk, WrRefundedHdemoSk),
            row.field(row.wr_refunded_addr_sk, WrRefundedAddrSk),
            row.field(row.wr_returning_customer_sk, WrReturningCustomerSk),
            row.field(row.wr_returning_cdemo_sk, WrReturningCdemoSk),
            row.field(row.wr_returning_hdemo_sk, WrReturningHdemoSk),
            row.field(row.wr_returning_addr_sk, WrReturningAddrSk),
            row.field(row.wr_web_page_sk, WrWebPageSk),
            row.field(row.wr_reason_sk, WrReasonSk),
            row.field(row.wr_order_number, WrOrderNumber),
            row.field(row.wr_pricing.get_quantity(), WrPricingQuantity),
            row.field(row.wr_pricing.get_net_paid(), WrPricingNetPaid),
            row.field(row.wr_pricing.get_ext_tax(), WrPricingExtTax),
            row.field(
                row.wr_pricing.get_net_paid_including_tax(),
                WrPricingNetPaidIncTax
            ),
            row.field(row.wr_pricing.get_fee(), WrPricingFee),
            row.field(row.wr_pricing.get_ext_ship_cost(), WrPricingExtShipCost),
            row.field(row.wr_pricing.get_refunded_cash(), WrPricingRefundedCash),
            row.field(
                row.wr_pricing.get_reversed_charge(),
                WrPricingReversedCharge
            ),
            row.field(row.wr_pricing.get_store_credit(), WrPricingStoreCredit),
            row.field(row.wr_pricing.get_net_loss(), WrPricingNetLoss),
        )
    }
}
/// Writes [`WebSalesRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{WebSalesCsv, CsvRow};
/// # use dsbgen::row::WebSalesRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::WebSales);
/// let generator = WebSalesRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", WebSalesCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", WebSalesCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit\n\
///    2450815,73313,2450914,16933,66833,745823,3684,3143,90264,1100283,440,6840,60,10,9,1,76,1,8,47.89,143.67,83.32,482.80,666.56,383.12,1149.36,0.00,0.00,448.24,666.56,666.56,1114.80,1114.80,283.44\n\
///    2450815,73313,2450843,3014,66833,745823,3684,3143,90264,1100283,440,6840,60,1,18,3,180,1,94,89.57,145.99,138.69,686.20,13036.86,8419.58,13723.06,208.59,7822.11,2469.38,5214.75,5423.34,7684.13,7892.72,-3204.83\n\
///    2450815,73313,2450845,4645,66833,745823,3684,3143,90264,1100283,440,6840,37,1,15,2,119,1,52,1.00,2.26,1.19,55.64,61.88,52.00,117.52,3.09,0.00,47.84,61.88,64.97,109.72,112.81,9.88\n"
/// );
/// ```
pub struct WebSalesCsv<'a> {
    inner: &'a WebSalesRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for WebSalesCsv<'a> {
    type Row = WebSalesRow;

    fn with_delimiter(inner: &'a WebSalesRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit"
    }
}

impl Display for WebSalesCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::WebSalesGeneratorColumn::*;

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.field(row.ws_sold_date_sk, WsSoldDateSk),
            row.field(row.ws_sold_time_sk, WsSoldTimeSk),
            row.field(row.ws_ship_date_sk, WsShipDateSk),
            row.field(row.ws_item_sk, WsItemSk),
            row.field(row.ws_bill_customer_sk, WsBillCustomerSk),
            row.field(row.ws_bill_cdemo_sk, WsBillCdemoSk),
            row.field(row.ws_bill_hdemo_sk, WsBillHdemoSk),
            row.field(row.ws_bill_addr_sk, WsBillAddrSk),
            row.field(row.ws_ship_customer_sk, WsShipCustomerSk),
            row.field(row.ws_ship_cdemo_sk, WsShipCdemoSk),
            row.field(row.ws_ship_hdemo_sk, WsShipHdemoSk),
            row.field(row.ws_ship_addr_sk, WsShipAddrSk),
            row.field(row.ws_web_page_sk, WsWebPageSk),
            row.field(row.ws_web_site_sk, WsWebSiteSk),
            row.field(row.ws_ship_mode_sk, WsShipModeSk),
            row.field(row.ws_warehouse_sk, WsWarehouseSk),
            row.field(row.ws_promo_sk, WsPromoSk),
            row.field(row.ws_order_number, WsOrderNumber),
            row.field(row.ws_pricing.get_quantity(), WsPricingQuantity),
            row.field(row.ws_pricing.get_wholesale_cost(), WsPricingWholesaleCost),
            row.field(row.ws_pricing.get_list_price(), WsPricingListPrice),
            row.field(row.ws_pricing.get_sales_price(), WsPricingSalesPrice),
            row.field(row.ws_pricing.get_ext_discount_amount(), WsPricingExtDiscountAmt),
            row.field(row.ws_pricing.get_ext_sales_price(), WsPricingExtSalesPrice),
            row.field(row.ws_pricing.get_ext_wholesale_cost(), WsPricingExtWholesaleCost),
            row.field(row.ws_pricing.get_ext_list_price(), WsPricingExtListPrice),
            row.field(row.ws_pricing.get_ext_tax(), WsPricingExtTax),
            row.field(row.ws_pricing.get_coupon_amount(), WsPricingCouponAmt),
            row.field(row.ws_pricing.get_ext_ship_cost(), WsPricingExtShipCost),
            row.field(row.ws_pricing.get_net_paid(), WsPricingNetPaid),
            row.field(row.ws_pricing.get_net_paid_including_tax(), WsPricingNetPaidIncTax),
            row.field(row.ws_pricing.get_net_paid_including_shipping(), WsPricingNetPaidIncShip),
            row.field(row.ws_pricing.get_net_paid_including_shipping_and_tax(), WsPricingNetPaidIncShipTax),
            row.field(row.ws_pricing.get_net_profit(), WsPricingNetProfit),
        )
    }
}
/// Writes [`WebSiteRow`]s in CSV format.
///
/// # Example
/// ```
/// # use dsbgen::config::{Session, Table};
/// # use dsbgen::csv::{WebSiteCsv, CsvRow};
/// # use dsbgen::row::WebSiteRowGenerator;
/// # use std::fmt::Write;
/// # let session = Session::default();
/// let row_count = session.get_scaling().get_row_count(Table::WebSite);
/// let generator = WebSiteRowGenerator::new(session, row_count);
/// // Output the first 3 rows in CSV format
/// let mut csv = String::new();
/// writeln!(&mut csv, "{}", WebSiteCsv::header()).unwrap(); // write header
/// for row in generator.take(3) {
///     // write line using CSV formatter
///     writeln!(&mut csv, "{}", WebSiteCsv::new(&row)).unwrap();
/// }
/// assert_eq!(
///   csv,
///   "web_site_sk,web_site_id,web_rec_start_date,web_rec_end_date,web_name,web_open_date_sk,web_close_date_sk,web_class,web_manager,web_mkt_id,web_mkt_class,web_mkt_desc,web_market_manager,web_company_id,web_company_name,web_street_number,web_street_name,web_street_type,web_suite_number,web_city,web_county,web_state,web_zip,web_country,web_gmt_offset,web_tax_percentage\n\
///    1,AAAAAAAABAAAAAAA,1997-08-16,,site_0,2450807,,Unknown,Ronald Shaffer,1,\"Grey lines ought to result indeed centres. Tod\",\"Well similar decisions used to keep hardly democratic, personal priorities.\",Joe George,2,able,915,Smith Sunset,Road,Suite 330,Friendship,Pennington County,MN,54536,United States,-6,0.10\n\
///    2,AAAAAAAACAAAAAAA,1997-08-16,2000-08-15,site_0,2450798,2447148,Unknown,Tommy Jones,1,\"Completely excellent things ought to pro\",\"Lucky passengers know. Red details will not hang alive, international s\",David Myers,5,anti,173,Tenth Wilson,Blvd,Suite 150,Greenwood,Wadena County,MN,58828,United States,-6,0.00\n\
///    3,AAAAAAAACAAAAAAA,2000-08-16,,site_0,2450798,2447148,Unknown,Tommy Jones,1,\"Completely excellent things ought to pro\",\"Particular, common seasons shall not indicate fully more single decisions; \",David Myers,5,anti,827,Lincoln ,Avenue,Suite 210,Pleasant Valley,Daviess County,MO,58828,United States,-6,0.02\n"
/// );
/// ```
pub struct WebSiteCsv<'a> {
    inner: &'a WebSiteRow,
    delimiter: char,
}

impl<'a> CsvRow<'a> for WebSiteCsv<'a> {
    type Row = WebSiteRow;

    fn with_delimiter(inner: &'a WebSiteRow, delimiter: char) -> Self {
        Self { inner, delimiter }
    }

    fn header() -> &'static str {
        "web_site_sk,web_site_id,web_rec_start_date,web_rec_end_date,web_name,web_open_date_sk,web_close_date_sk,web_class,web_manager,web_mkt_id,web_mkt_class,web_mkt_desc,web_market_manager,web_company_id,web_company_name,web_street_number,web_street_name,web_street_type,web_suite_number,web_city,web_county,web_state,web_zip,web_country,web_gmt_offset,web_tax_percentage"
    }
}

impl Display for WebSiteCsv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::generator::WebSiteGeneratorColumn::*;
        use crate::row::table_row::{CsvQuoted, DatField};

        let d = self.delimiter;
        let row = self.inner;

        write!(
            f,
            "{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}{d}{}",
            row.key_field(row.web_site_sk, WebSiteSk),
            row.field(&row.web_site_id, WebSiteId),
            row.date_field(row.web_rec_start_date_id, WebRecStartDateId),
            row.date_field(row.web_rec_end_date_id, WebRecEndDateId),
            row.field(&row.web_name, WebName),
            row.key_field(row.web_open_date, WebOpenDate),
            row.key_field(row.web_close_date, WebCloseDate),
            row.field(&row.web_class, WebClass),
            row.field(&row.web_manager, WebManager),
            row.field(row.web_market_id, WebMarketId),
            CsvQuoted::new(&row.web_market_class, row.is_null_at(WebMarketClass)),
            CsvQuoted::new(&row.web_market_desc, row.is_null_at(WebMarketDesc)),
            row.field(&row.web_market_manager, WebMarketManager),
            row.field(row.web_company_id, WebCompanyId),
            row.field(&row.web_company_name, WebCompanyName),
            row.field(row.web_address.get_street_number(), WebAddressStreetNum),
            row.field(row.web_address.get_street_name(), WebAddressStreetName1),
            row.field(row.web_address.get_street_type(), WebAddressStreetType),
            row.field(row.web_address.get_suite_number(), WebAddressSuiteNum),
            row.field(row.web_address.get_city(), WebAddressCity),
            row.field(
                row.web_address.get_county().unwrap_or(""),
                WebAddressCounty
            ),
            row.field(row.web_address.get_state(), WebAddressState),
            DatField::zip(row.web_address.get_zip(), row.is_null_at(WebAddressZip)),
            row.field(row.web_address.get_country(), WebAddressCountry),
            row.field(row.web_address.get_gmt_offset(), WebAddressGmtOffset),
            row.field(row.web_tax_percentage, WebTaxPercentage),
        )
    }
}

/// Returns the CSV header line for `table` with the given delimiter.
pub fn csv_header(table: Table, delimiter: char) -> String {
    let header = match table {
        Table::CallCenter => CallCenterCsv::header(),
        Table::CatalogPage => CatalogPageCsv::header(),
        Table::CatalogReturns => CatalogReturnsCsv::header(),
        Table::CatalogSales => CatalogSalesCsv::header(),
        Table::Customer => CustomerCsv::header(),
        Table::CustomerAddress => CustomerAddressCsv::header(),
        Table::CustomerDemographics => CustomerDemographicsCsv::header(),
        Table::DateDim => DateDimCsv::header(),
        Table::DbgenVersion => DbgenVersionCsv::header(),
        Table::HouseholdDemographics => HouseholdDemographicsCsv::header(),
        Table::IncomeBand => IncomeBandCsv::header(),
        Table::Inventory => InventoryCsv::header(),
        Table::Item => ItemCsv::header(),
        Table::Promotion => PromotionCsv::header(),
        Table::Reason => ReasonCsv::header(),
        Table::ShipMode => ShipModeCsv::header(),
        Table::Store => StoreCsv::header(),
        Table::StoreReturns => StoreReturnsCsv::header(),
        Table::StoreSales => StoreSalesCsv::header(),
        Table::TimeDim => TimeDimCsv::header(),
        Table::Warehouse => WarehouseCsv::header(),
        Table::WebPage => WebPageCsv::header(),
        Table::WebReturns => WebReturnsCsv::header(),
        Table::WebSales => WebSalesCsv::header(),
        Table::WebSite => WebSiteCsv::header(),
    };
    join_header(header, delimiter)
}

/// Join the comma-separated column names of `header` with `delimiter`,
/// double-quoting any name that contains the delimiter (for example `_`).
fn join_header(header: &str, delimiter: char) -> String {
    let mut out = String::with_capacity(header.len() + 16);
    for (i, name) in header.split(',').enumerate() {
        if i > 0 {
            out.push(delimiter);
        }
        if name.contains(delimiter) {
            out.push('"');
            out.push_str(name);
            out.push('"');
        } else {
            out.push_str(name);
        }
    }
    out
}
