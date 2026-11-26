// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! `DateGenerator` and `DateUtils`: epoch milliseconds, UTC.

use crate::dict::PowerDistribution;
use crate::java::{ms_of_ymd, ymd_of_ms};
use crate::jrand::JavaRandom;
use crate::params::{ALPHA, Params};
use std::sync::LazyLock;

pub const ONE_DAY: i64 = 24 * 60 * 60 * 1000;
pub const SEVEN_DAYS: i64 = 7 * ONE_DAY;
const THIRTY_DAYS: i64 = 30 * ONE_DAY;
const ONE_YEAR: i64 = 365 * ONE_DAY;
const TWO_YEARS: i64 = 2 * ONE_YEAR;
const TEN_YEARS: i64 = 10 * ONE_YEAR;

/// Minutes of `PowerLawActivityDeleteDistribution`.
const MINUTES: [f64; 18] = [
    0.0, 0.5, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 60.0, 120.0, 300.0, 1440.0, 2880.0, 4320.0, 5760.0,
    7200.0, 8460.0, 10080.0,
];

#[derive(Debug, Clone)]
pub struct Dates {
    pub start: i64,
    pub end: i64,
    from_birthday: i64,
    to_birthday: i64,
    power: PowerDistribution,
    delete_dist: Vec<f64>,
    delta: i64,
}

impl Dates {
    pub fn new(p: &Params, power_law_delete: Vec<f64>) -> Self {
        Dates {
            start: ms_of_ymd(p.start_year as i64, 1, 1),
            end: ms_of_ymd((p.start_year + p.num_years) as i64, 1, 1),
            from_birthday: ms_of_ymd(1980, 1, 1),
            to_birthday: ms_of_ymd(1990, 1, 1),
            power: PowerDistribution::new(0.0, 1.0, ALPHA),
            delete_dist: power_law_delete,
            delta: p.delta,
        }
    }

    pub fn network_collapse(&self) -> i64 {
        self.start + TEN_YEARS
    }

    pub fn person_creation(&self, r: &mut JavaRandom) -> i64 {
        (self.start as f64 + r.next_double() * (self.end - self.start) as f64) as i64
    }

    pub fn person_deletion(&self, r: &mut JavaRandom, creation: i64, max: i64) -> i64 {
        self.random_date(r, creation + self.delta, max)
    }

    pub fn knows_creation(&self, r: &mut JavaRandom, a: (i64, i64), b: (i64, i64)) -> i64 {
        let from = a.0.max(b.0) + self.delta;
        let to = a.1.min(b.1).min(self.end);
        self.random_date(r, from, to)
    }

    pub fn knows_deletion(
        &self,
        r: &mut JavaRandom,
        a: (i64, i64),
        b: (i64, i64),
        creation: i64,
    ) -> i64 {
        let from = creation + self.delta;
        let to = a.1.min(b.1).min(self.end);
        self.random_date(r, from, to)
    }

    /// Whole 30-day months from `from` to the end of the simulation.
    pub fn number_of_months(&self, from: i64) -> i64 {
        (self.end - from) / THIRTY_DAYS
    }

    pub fn random_date_from(&self, r: &mut JavaRandom, min: i64) -> i64 {
        let max = (min + THIRTY_DAYS).max(self.end);
        self.random_date(r, min, max)
    }

    #[inline]
    pub fn random_date(&self, r: &mut JavaRandom, min: i64, max: i64) -> i64 {
        (r.next_double() * (max - min) as f64 + min as f64) as i64
    }

    fn power_law_minutes(&self, prob: f64, r: &mut JavaRandom) -> f64 {
        for i in 0..self.delete_dist.len() {
            if prob < self.delete_dist[i] {
                let (lo, hi) = (MINUTES[i - 1], MINUTES[i]);
                return lo + (hi - lo) * r.next_double();
            }
        }
        0.0
    }

    pub fn power_law_delete_date(&self, r: &mut JavaRandom, min: i64, max: i64) -> i64 {
        let p = r.next_double();
        let d = (min as f64 + self.power_law_minutes(p, r)) as i64;
        if d > max { min + (max - min) / 2 } else { d }
    }

    pub fn power_law_comm_date_day(&self, r: &mut JavaRandom, last: i64) -> i64 {
        (self.power.double(r) * ONE_DAY as f64 + last as f64) as i64
    }

    pub fn birthday(&self, r: &mut JavaRandom) -> i64 {
        let ms = (r.next_double() * (self.to_birthday - self.from_birthday) as f64) as i64
            + self.from_birthday;
        let (y, m, d) = ymd_of_ms(ms);
        ms_of_ymd(y, m, d)
    }

    pub fn class_year(&self, r: &mut JavaRandom, birthday: i64) -> i64 {
        let age = (r.next_int_bound(5) as i64 + 18) * ONE_YEAR;
        let c = birthday + age;
        if c > self.end { -1 } else { c }
    }

    pub fn work_from(&self, r: &mut JavaRandom, class_year: i64, birthday: i64) -> i64 {
        if class_year == -1 {
            let from = birthday + 18 * ONE_YEAR;
            ((r.next_double() * (self.end - from) as f64) as i64 + from).min(self.end)
        } else {
            class_year + (r.next_double() * TWO_YEARS as f64) as i64
        }
    }
}

pub fn year(ms: i64) -> i64 {
    ymd_of_ms(ms).0
}

pub fn month(ms: i64) -> u32 {
    ymd_of_ms(ms).1
}

/// `DateUtils.isTravelSeason`.
pub fn is_travel_season(ms: i64) -> bool {
    // one bit per day of 1970 to 2059 (the datagen's dates are 2010 to
    // 2020), the civil date only outside that
    const DAYS: usize = 90 * 366;
    static SEASON: LazyLock<Vec<u64>> = LazyLock::new(|| {
        let mut v = vec![0u64; DAYS.div_ceil(64)];
        for day in 0..DAYS {
            if travel_season_of(day as i64 * ONE_DAY) {
                v[day / 64] |= 1 << (day % 64);
            }
        }
        v
    });
    let day = ms.div_euclid(ONE_DAY);
    match usize::try_from(day) {
        Ok(d) if d < DAYS => SEASON[d / 64] >> (d % 64) & 1 == 1,
        _ => travel_season_of(ms),
    }
}

fn travel_season_of(ms: i64) -> bool {
    let (_, m, d) = ymd_of_ms(ms);
    (m > 4 && m < 7) || (m == 11 && d > 23)
}
