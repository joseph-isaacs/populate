// Modified from tpcdsgen 4.0.0 for the data-comb DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Generator columns for CUSTOMER_DEMOGRAPHICS table (CustomerDemographicsGeneratorColumn)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomerDemographicsGeneratorColumn {
    CdDemoSk,
    CdGender,
    CdMaritalStatus,
    CdEducationStatus,
    CdPurchaseEstimate,
    CdCreditRating,
    CdDepCount,
    CdDepEmployedCount,
    CdDepCollegeCount,
    CdNulls,
}

impl GeneratorColumn for CustomerDemographicsGeneratorColumn {
    fn get_global_column_number(&self) -> i32 {
        match self {
            Self::CdDemoSk => 76,
            Self::CdGender => 77,
            Self::CdMaritalStatus => 78,
            Self::CdEducationStatus => 79,
            Self::CdPurchaseEstimate => 80,
            Self::CdCreditRating => 81,
            Self::CdDepCount => 82,
            Self::CdDepEmployedCount => 83,
            Self::CdDepCollegeCount => 84,
            Self::CdNulls => 85,
        }
    }

    fn get_seeds_per_row(&self) -> u32 {
        match self {
            Self::CdDemoSk => 1,
            Self::CdGender => 1,
            Self::CdMaritalStatus => 1,
            Self::CdEducationStatus => 1,
            Self::CdPurchaseEstimate => 1,
            Self::CdCreditRating => 1,
            Self::CdDepCount => 1,
            Self::CdDepEmployedCount => 1,
            Self::CdDepCollegeCount => 1,
            Self::CdNulls => 2,
        }
    }

    fn get_table(&self) -> Table {
        Table::CustomerDemographics
    }

    fn values() -> &'static [Self] {
        use CustomerDemographicsGeneratorColumn::*;
        static VALUES: &[CustomerDemographicsGeneratorColumn] = &[
            CdDemoSk,
            CdGender,
            CdMaritalStatus,
            CdEducationStatus,
            CdPurchaseEstimate,
            CdCreditRating,
            CdDepCount,
            CdDepEmployedCount,
            CdDepCollegeCount,
            CdNulls,
        ];
        VALUES
    }
}
