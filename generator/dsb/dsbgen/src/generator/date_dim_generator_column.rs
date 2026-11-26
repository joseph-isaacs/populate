// Modified from tpcdsgen 4.0.0 for the populate DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

use crate::column::Table;
use crate::generator::GeneratorColumn;

/// Generator columns for DATE_DIM table (DateDimGeneratorColumn)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateDimGeneratorColumn {
    DDateSk,
    DDateId,
    DDate,
    DMonthSeq,
    DWeekSeq,
    DQuarterSeq,
    DYear,
    DDow,
    DMoy,
    DDom,
    DQoy,
    DFyYear,
    DFyQuarterSeq,
    DFyWeekSeq,
    DDayName,
    DQuarterName,
    DHoliday,
    DWeekend,
    DFollowingHoliday,
    DFirstDom,
    DLastDom,
    DSameDayLy,
    DSameDayLq,
    DCurrentDay,
    DCurrentWeek,
    DCurrentMonth,
    DCurrentQuarter,
    DCurrentYear,
    DNulls,
}

impl DateDimGeneratorColumn {
    fn get_column_info(&self) -> (i32, u32) {
        use DateDimGeneratorColumn::*;
        match self {
            DDateSk => (190, 0),
            DDateId => (191, 0),
            DDate => (192, 0),
            DMonthSeq => (193, 0),
            DWeekSeq => (194, 0),
            DQuarterSeq => (195, 0),
            DYear => (196, 0),
            DDow => (197, 0),
            DMoy => (198, 0),
            DDom => (199, 0),
            DQoy => (200, 0),
            DFyYear => (201, 0),
            DFyQuarterSeq => (202, 0),
            DFyWeekSeq => (203, 0),
            DDayName => (204, 0),
            DQuarterName => (205, 0),
            DHoliday => (206, 0),
            DWeekend => (207, 0),
            DFollowingHoliday => (208, 0),
            DFirstDom => (209, 0),
            DLastDom => (210, 0),
            DSameDayLy => (211, 0),
            DSameDayLq => (212, 0),
            DCurrentDay => (213, 0),
            DCurrentWeek => (214, 0),
            DCurrentMonth => (215, 0),
            DCurrentQuarter => (216, 0),
            DCurrentYear => (217, 0),
            DNulls => (218, 2),
        }
    }
}

impl GeneratorColumn for DateDimGeneratorColumn {
    fn get_table(&self) -> Table {
        Table::DateDim
    }

    fn get_global_column_number(&self) -> i32 {
        self.get_column_info().0
    }

    fn get_seeds_per_row(&self) -> u32 {
        self.get_column_info().1
    }

    fn values() -> &'static [Self] {
        use DateDimGeneratorColumn::*;
        static VALUES: &[DateDimGeneratorColumn] = &[
            DDateSk,
            DDateId,
            DDate,
            DMonthSeq,
            DWeekSeq,
            DQuarterSeq,
            DYear,
            DDow,
            DMoy,
            DDom,
            DQoy,
            DFyYear,
            DFyQuarterSeq,
            DFyWeekSeq,
            DDayName,
            DQuarterName,
            DHoliday,
            DWeekend,
            DFollowingHoliday,
            DFirstDom,
            DLastDom,
            DSameDayLy,
            DSameDayLq,
            DCurrentDay,
            DCurrentWeek,
            DCurrentMonth,
            DCurrentQuarter,
            DCurrentYear,
            DNulls,
        ];
        VALUES
    }
}
