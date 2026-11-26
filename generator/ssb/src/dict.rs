//! Every SSB string that is not unique to a row, as `Utf8View`s into one
//! shared buffer, and the calendar the `date` table and the order dates
//! come from.
//!
//! SSB has no comments, so most of its text is drawn from small sets:
//! distribution values, cities (a nation's first 9 letters and a digit),
//! part names (two colours), manufacturer / category / brand codes and the
//! date table's names. All of them are built once into [`DICT`], and a
//! column of them is just a column of precomputed views. Only names,
//! addresses and phone numbers are written per batch, into [`DATA`].

use populate_combinators::views::{self, Buffers, Picks, Shared, static_buffer};
use std::sync::LazyLock;
use tpchgen::distribution::{Distribution, Distributions};

/// Buffer index of the shared dictionary.
pub(crate) const DICT: u32 = 1;

/// Days in ssb-dbgen's calendar (1992-01-01 to 1998-12-31) and in the full
/// `date` table.
pub(crate) const DAYS: usize = 2557;
/// Colours (`p_name` and `p_color` are drawn from them).
pub(crate) const COLORS: usize = 92;
/// Nations.
pub(crate) const NATIONS: usize = 25;

/// One day of the calendar.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Day {
    pub(crate) year: i32,
    /// 1..=12.
    pub(crate) month: i32,
    /// Day of the month, 1-based.
    pub(crate) mday: i32,
    /// Day of the year, 1-based.
    pub(crate) yday: i32,
    /// `tm_wday`: 0 = Sunday.
    pub(crate) wday: i32,
}

impl Day {
    /// `YYYYMMDD`, as ssb-dbgen prints dates.
    pub(crate) fn key(&self) -> i32 {
        self.year * 10_000 + self.month * 100 + self.mday
    }
}

fn leap(y: i32) -> bool {
    // dbgen's (and ssb-dbgen's) `LEAP`: no 400-year rule, which 1992-1998
    // never needs
    y % 4 == 0 && y % 100 != 0
}

pub(crate) fn days_in_month(y: i32, m: i32) -> i32 {
    match m {
        2 if leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

pub(crate) const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
pub(crate) const WEEKDAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
/// Selling seasons, first match wins: `(name, first month, last month,
/// first day, last day)`.
const SEASONS: [(&str, i32, i32, i32, i32); 5] = [
    ("Christmas", 11, 12, 1, 31),
    ("Summer", 5, 8, 1, 31),
    ("Winter", 1, 3, 1, 31),
    ("Spring", 4, 4, 1, 30),
    ("Fall", 9, 10, 1, 31),
];
/// Holidays, `(month, day)`.
const HOLIDAYS: [(i32, i32); 10] = [
    (12, 24),
    (1, 1),
    (2, 20),
    (4, 20),
    (5, 20),
    (7, 20),
    (8, 20),
    (9, 20),
    (10, 20),
    (11, 20),
];

/// Season index of a day (`SEASONS.len()` for none, which no day has).
pub(crate) fn season(month: i32, mday: i32) -> usize {
    SEASONS
        .iter()
        .position(|&(_, m0, m1, d0, d1)| (m0..=m1).contains(&month) && (d0..=d1).contains(&mday))
        .unwrap_or(SEASONS.len())
}

pub(crate) fn holiday(month: i32, mday: i32) -> bool {
    HOLIDAYS.contains(&(month, mday))
}

/// SSB's string buffers: each batch's own, then the dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Ssb;

impl Buffers for Ssb {
    fn shared() -> &'static Shared {
        &Dict::get().shared
    }
}

/// The shared dictionary.
#[derive(Debug)]
pub(crate) struct Dict {
    shared: Shared,
    pub(crate) nations: Vec<u128>,
    /// Region of each nation.
    pub(crate) regions: Vec<u128>,
    /// `NATIONS * 10`: nation `n`'s city `d` at `n * 10 + d`.
    pub(crate) cities: Vec<u128>,
    pub(crate) segments: Picks,
    pub(crate) priorities: Picks,
    pub(crate) modes: Picks,
    pub(crate) types: Picks,
    pub(crate) containers: Picks,
    pub(crate) colors: Vec<u128>,
    /// `COLORS^2`: `"a b"` at `a * COLORS + b`.
    pub(crate) names: Vec<u128>,
    /// `MFGR#m` at `m - 1`.
    pub(crate) mfgrs: Vec<u128>,
    /// `MFGR#mc` at `(m - 1) * 5 + c - 1`.
    pub(crate) categories: Vec<u128>,
    /// `MFGR#mcb` at `((m - 1) * 5 + c - 1) * 40 + b - 1`.
    pub(crate) brands: Vec<u128>,
    pub(crate) days: Vec<Day>,
    /// `YYYYMMDD` of each day.
    pub(crate) keys: Vec<i32>,
    /// `"January 1, 1992"` of each day.
    pub(crate) day_names: Vec<u128>,
    pub(crate) weekdays: Vec<u128>,
    pub(crate) months: Vec<u128>,
    /// `"Jan1992"` at `(year - 1992) * 12 + month - 1`.
    pub(crate) year_months: Vec<u128>,
    /// Each season, then `""`.
    pub(crate) seasons: Vec<u128>,
    /// `"0"`, `"1"`.
    pub(crate) flags: [u128; 2],
}

static DICT_: LazyLock<Dict> = LazyLock::new(Dict::build);

fn pick(b: &mut views::Dict, d: &Distribution) -> Picks {
    Picks::new(d.get_values(), |i| d.get_weight(i), b)
}

impl Dict {
    pub(crate) fn get() -> &'static Dict {
        &DICT_
    }

    fn build() -> Dict {
        let d = Distributions::static_default();
        let mut b = views::Dict::new(DICT);
        let nation_names: Vec<&str> = d.nations().get_values().to_vec();
        assert_eq!(nation_names.len(), NATIONS);
        let region_names = d.regions().get_values();
        let nations = nation_names.iter().map(|n| b.add(n)).collect();
        let regions = (0..NATIONS)
            .map(|i| b.add(region_names[d.nations().get_weight(i) as usize]))
            .collect();
        let cities = nation_names
            .iter()
            .flat_map(|n| (0..10).map(move |digit| city(n, digit)))
            .collect::<Vec<_>>()
            .iter()
            .map(|c| b.add(c))
            .collect();
        let color_names = d.part_colors().get_values();
        assert_eq!(color_names.len(), COLORS);
        assert!(color_names.iter().all(|c| !c.contains(' ')));
        let colors = color_names.iter().map(|c| b.add(c)).collect();
        let mut names = Vec::with_capacity(COLORS * COLORS);
        for x in color_names {
            for y in color_names {
                names.push(b.add(&format!("{x} {y}")));
            }
        }
        let mut mfgrs = Vec::new();
        let mut categories = Vec::new();
        let mut brands = Vec::new();
        for m in 1..=5 {
            mfgrs.push(b.add(&format!("MFGR#{m}")));
            for c in 1..=5 {
                categories.push(b.add(&format!("MFGR#{m}{c}")));
                for br in 1..=40 {
                    brands.push(b.add(&format!("MFGR#{m}{c}{br}")));
                }
            }
        }
        let days = calendar();
        let keys = days.iter().map(Day::key).collect();
        let day_names = days
            .iter()
            .map(|t| {
                b.add(&format!(
                    "{} {}, {}",
                    MONTHS[t.month as usize - 1],
                    t.mday,
                    t.year
                ))
            })
            .collect();
        let weekdays = WEEKDAYS.iter().map(|w| b.add(w)).collect();
        let months = MONTHS.iter().map(|m| b.add(m)).collect();
        let year_months = (1992..=1998)
            .flat_map(|y| MONTHS.iter().map(move |m| format!("{}{y}", &m[..3])))
            .collect::<Vec<_>>()
            .iter()
            .map(|s| b.add(s))
            .collect();
        let seasons = SEASONS
            .iter()
            .map(|s| s.0)
            .chain([""])
            .map(|s| b.add(s))
            .collect();
        let flags = [b.add("0"), b.add("1")];
        let segments = pick(&mut b, d.market_segments());
        let priorities = pick(&mut b, d.order_priority());
        let modes = pick(&mut b, d.ship_modes());
        let types = pick(&mut b, d.part_types());
        let containers = pick(&mut b, d.part_containers());
        Dict {
            shared: Shared::new(vec![static_buffer(b.leak())]),
            nations,
            regions,
            cities,
            segments,
            priorities,
            modes,
            types,
            containers,
            colors,
            names,
            mfgrs,
            categories,
            brands,
            days,
            keys,
            day_names,
            weekdays,
            months,
            year_months,
            seasons,
            flags,
        }
    }
}

/// ssb-dbgen's city: the nation's name cut or space-padded to 9 bytes, then
/// a digit.
pub(crate) fn city(nation: &str, digit: u32) -> String {
    let mut c: Vec<u8> = nation.bytes().take(9).collect();
    c.resize(9, b' ');
    c.push(b'0' + digit as u8);
    String::from_utf8(c).expect("ASCII")
}

/// 1992-01-01 (a Wednesday) and the 2556 days after it.
fn calendar() -> Vec<Day> {
    let mut v = Vec::with_capacity(DAYS);
    let (mut year, mut month, mut mday, mut yday, mut wday) = (1992, 1, 1, 1, 3);
    for _ in 0..DAYS {
        v.push(Day {
            year,
            month,
            mday,
            yday,
            wday,
        });
        wday = (wday + 1) % 7;
        mday += 1;
        yday += 1;
        if mday > days_in_month(year, month) {
            mday = 1;
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
                yday = 1;
            }
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_ends_1998_12_31() {
        let d = Dict::get();
        assert_eq!(d.keys[0], 19920101);
        assert_eq!(d.keys[DAYS - 1], 19981231);
        assert_eq!(d.days[59].key(), 19920229);
    }

    #[test]
    fn cities() {
        assert_eq!(city("MOROCCO", 6), "MOROCCO  6");
        assert_eq!(city("UNITED KINGDOM", 0), "UNITED KI0");
    }
}
