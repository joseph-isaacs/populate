//! NYC-taxi-like trips, in the TLC yellow-cab schema.
//!
//! Trips are sorted by pickup time: row `r` falls on day `r / trips_per_day`,
//! at its position in the day mapped through an hourly demand profile
//! (quiet before dawn, peaks in the evening), so times are non-decreasing
//! and every hour has a realistic share. The rest follows from a few draws
//! per trip:
//!
//! - distance is log-normal (median 1.7 miles); duration is distance over a
//!   speed that is lower in rush hours, plus a minute or two;
//! - pickup and dropoff zones are Zipf-skewed over the 265 zones (10% of
//!   trips stay in their zone);
//! - fare is metered from distance and duration (flat 70.00 on the JFK
//!   rate), tips go on card payments only, tolls mostly on long trips,
//!   the airport fee on airport pickups, and `total_amount` is the sum;
//! - about 3% of trips miss `passenger_count`, `RatecodeID`,
//!   `store_and_fwd_flag`, `congestion_surcharge` and `Airport_fee`
//!   together, with payment type 0, as in the real files.

use crate::Shape;
use crate::kernel::{
    Pool, Quantiles, Rows, View, Views, Weighted, Zipf, cents, put, put_nullable, row_numbers,
    threshold, unit,
};
use populate_builtins::{derive_columns, named_all};
use populate_core::{Columnar, Distribution, Nullable, Timestamp, Validity, time_unit};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::Stream;
use std::sync::Arc;

const MICROS_PER_DAY: f64 = 86_400e6;
/// 2024-01-01T00:00:00Z.
const DEFAULT_START_US: i64 = 1_704_067_200_000_000;
const ZONES: u64 = 265;
const JFK: i32 = 132;
const LGA: i32 = 138;

/// Share of trips by hour of day (NYC yellow cabs, roughly).
const HOURLY: [f64; 24] = [
    2.8, 1.9, 1.3, 0.9, 0.7, 0.8, 1.7, 3.0, 3.9, 4.1, 4.2, 4.4, 4.7, 4.8, 5.1, 5.4, 5.6, 6.2, 6.6,
    6.1, 5.5, 5.2, 4.9, 3.9,
];

/// Average speed (mph) by hour of day.
const SPEED: [f64; 24] = [
    16.0, 17.0, 18.0, 19.0, 20.0, 18.0, 14.0, 11.0, 9.5, 9.5, 10.0, 10.0, 10.0, 10.0, 9.5, 9.0,
    9.0, 9.0, 9.5, 11.0, 12.5, 13.5, 14.5, 15.0,
];

type Micros = Timestamp<{ time_unit::MICROSECOND }>;

/// Taxi trips. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Taxi {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone)]
struct Inner {
    seed: u64,
    trips_per_day: u64,
    start_us: i64,
    s: Streams,
    tod: Quantiles,
    distance: Quantiles,
    speed_noise: Quantiles,
    zone: Zipf,
    /// Zone id of each Zipf rank (the busiest zones are scattered).
    zone_of_rank: Vec<i32>,
    vendor: Weighted<i32>,
    passengers: Weighted<i32>,
    rate: Weighted<i32>,
    payment: Weighted<i32>,
    tip_pct: Weighted<f64>,
    flag: Pool,
}

#[derive(Debug, Clone, Copy)]
struct Streams {
    pickup: Stream,
    missing: Stream,
    vendor: Stream,
    passengers: Stream,
    distance: Stream,
    speed: Stream,
    wait: Stream,
    rate: Stream,
    flag: Stream,
    pu: Stream,
    same_zone: Stream,
    dropoff: Stream,
    payment: Stream,
    tip: Stream,
    tolls: Stream,
    congestion: Stream,
}

impl Taxi {
    /// 100,000 trips a day from 2024-01-01.
    pub fn new(seed: u64) -> Self {
        let st = |n| Stream::new(seed, n);
        let s = Streams {
            pickup: st("taxi.pickup"),
            missing: st("taxi.missing"),
            vendor: st("taxi.vendor"),
            passengers: st("taxi.passengers"),
            distance: st("taxi.distance"),
            speed: st("taxi.speed"),
            wait: st("taxi.wait"),
            rate: st("taxi.rate"),
            flag: st("taxi.flag"),
            pu: st("taxi.pu"),
            same_zone: st("taxi.same_zone"),
            dropoff: st("taxi.dropoff"),
            payment: st("taxi.payment"),
            tip: st("taxi.tip"),
            tolls: st("taxi.tolls"),
            congestion: st("taxi.congestion"),
        };
        // zones in a seeded random order of popularity
        let order = Stream::new(seed, "taxi.zone_order");
        let mut zone_of_rank: Vec<i32> = (1..=ZONES as i32).collect();
        zone_of_rank.sort_by_key(|&z| order.at(z as u64));
        let inner = Inner {
            seed,
            trips_per_day: 100_000,
            start_us: DEFAULT_START_US,
            s,
            tod: Quantiles::histogram(0.0, 86_400.0, &HOURLY),
            distance: Quantiles::lognormal(1.7f64.ln(), 0.85),
            speed_noise: Quantiles::lognormal(0.0, 0.25),
            zone: Zipf::new(ZONES, 1.05),
            zone_of_rank,
            vendor: Weighted::new(&[(1, 0.27), (2, 0.73)]),
            passengers: Weighted::new(&[
                (1, 0.72),
                (2, 0.14),
                (3, 0.04),
                (4, 0.02),
                (5, 0.03),
                (6, 0.02),
                (0, 0.03),
            ]),
            rate: Weighted::new(&[
                (1, 0.94),
                (2, 0.035),
                (3, 0.003),
                (4, 0.002),
                (5, 0.015),
                (99, 0.005),
            ]),
            payment: Weighted::new(&[(1, 0.75), (2, 0.21), (3, 0.015), (4, 0.025)]),
            tip_pct: Weighted::new(&[
                (0.0, 0.12),
                (0.15, 0.15),
                (0.18, 0.15),
                (0.20, 0.33),
                (0.25, 0.17),
                (0.30, 0.08),
            ]),
            flag: Pool::new(["N", "Y"]),
        };
        Taxi {
            inner: Arc::new(inner),
        }
    }

    /// Trips per day (sets how many days the rows span).
    pub fn trips_per_day(mut self, n: u64) -> Self {
        assert!(n > 0, "trips_per_day must be > 0");
        Arc::make_mut(&mut self.inner).trips_per_day = n;
        self
    }

    /// First day, as microseconds since the epoch at midnight UTC.
    pub fn start_micros(mut self, us: i64) -> Self {
        Arc::make_mut(&mut self.inner).start_us = us;
        self
    }

    pub fn seed(&self) -> u64 {
        self.inner.seed
    }
}

/// The columns, in schema order.
type Trip = (
    (
        i32,
        Micros,
        Micros,
        Option<i32>,
        f64,
        Option<i32>,
        Option<View>,
        i32,
        i32,
        i32,
    ),
    (f64, f64, f64, f64, f64, f64, f64, Option<f64>, Option<f64>),
);
type TripCols = (
    (
        Vec<i32>,
        Vec<Micros>,
        Vec<Micros>,
        Nullable<Vec<i32>>,
        Vec<f64>,
        Nullable<Vec<i32>>,
        Nullable<Views>,
        Vec<i32>,
        Vec<i32>,
        Vec<i32>,
    ),
    (
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Nullable<Vec<f64>>,
        Nullable<Vec<f64>>,
    ),
);

const COLUMNS: [&str; 19] = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
];

impl Inner {
    fn write(&self, rows: &Rows, out: &mut TripCols) {
        let s = &self.s;
        let len = rows.len();
        let span = rows.0.clone();
        let rows = || span.clone();
        let per_day = self.trips_per_day;

        // pickup: day, then position in the day through the hourly profile
        let mut hour = Vec::with_capacity(len);
        let pickup: Vec<Micros> = rows()
            .map(|r| {
                let (day, k) = (r / per_day, r % per_day);
                let p = (k as f64 + unit(s.pickup.at(r))) / per_day as f64;
                let secs = self.tod.at(p);
                hour.push((secs * (1.0 / 3600.0)) as usize % 24);
                Timestamp(self.start_us + (day as f64 * MICROS_PER_DAY + secs * 1e6) as i64)
            })
            .collect();

        let miss_t = threshold(0.03);
        let is_present: Vec<bool> = rows().map(|r| s.missing.at(r) >= miss_t).collect();
        let present: Validity = is_present.iter().copied().collect();
        let missing = |i: usize| !is_present[i];

        let vendor: Vec<i32> = rows().map(|r| self.vendor.sample(s.vendor.at(r))).collect();

        let passengers: Vec<i32> = rows()
            .map(|r| self.passengers.sample(s.passengers.at(r)))
            .collect();
        let rate: Vec<i32> = rows().map(|r| self.rate.sample(s.rate.at(r))).collect();

        // 1% of trips are zero-distance (cancelled or same-spot)
        let zero_t = threshold(0.01);
        let distance: Vec<f64> = rows()
            .map(|r| {
                let b = s.distance.at(r);
                let d = self.distance.sample(b).min(150.0);
                // the low bits decide zero trips, independent of the
                // quantile's top bits
                if (b << 20) < zero_t { 0.0 } else { cents(d) }
            })
            .collect();

        let duration_us: Vec<i64> = rows()
            .zip(&distance)
            .zip(&hour)
            .map(|((r, &d), &h)| {
                let mph = SPEED[h] * self.speed_noise.sample(s.speed.at(r));
                let wait = 30.0 + 150.0 * unit(s.wait.at(r));
                ((d / mph * 3600.0 + wait) * 1e6) as i64
            })
            .collect();
        let dropoff: Vec<Micros> = pickup
            .iter()
            .zip(&duration_us)
            .map(|(p, d)| Timestamp(p.0 + d))
            .collect();

        let flag_y = threshold(0.005);
        let flag: Vec<u32> = rows().map(|r| (s.flag.at(r) < flag_y) as u32).collect();

        let pu: Vec<i32> = rows()
            .map(|r| self.zone_of_rank[self.zone.sample(s.pu.at(r)) as usize])
            .collect();
        let same_t = threshold(0.1);
        let dropoff_zone: Vec<i32> = rows()
            .zip(&pu)
            .map(|(r, &p)| {
                let d = self.zone_of_rank[self.zone.sample(s.dropoff.at(r)) as usize];
                if s.same_zone.at(r) < same_t { p } else { d }
            })
            .collect();

        let payment: Vec<i32> = rows()
            .enumerate()
            .map(|(i, r)| {
                if missing(i) {
                    0
                } else {
                    self.payment.sample(s.payment.at(r))
                }
            })
            .collect();

        let fare: Vec<f64> = (0..len)
            .map(|i| {
                if rate[i] == 2 {
                    70.0
                } else {
                    let minutes = duration_us[i] as f64 * (1.0 / 60e6);
                    cents(3.0 + 2.5 * distance[i] + 0.7 * minutes)
                }
            })
            .collect();
        let extra: Vec<f64> = hour
            .iter()
            .map(|&h| match h {
                16..=19 => 2.5,
                20..=23 | 0..=5 => 1.0,
                _ => 0.0,
            })
            .collect();
        let tip: Vec<f64> = rows()
            .enumerate()
            .map(|(i, r)| {
                if payment[i] == 1 {
                    let b = s.tip.at(r);
                    // pct from the top bits, a little noise from the low ones
                    let pct = self.tip_pct.sample(b);
                    let noise = 1.0 + 0.1 * (unit(b << 20) - 0.5);
                    cents(fare[i] * pct * noise)
                } else {
                    0.0
                }
            })
            .collect();
        let long_t = threshold(0.45);
        let short_t = threshold(0.02);
        let tolls: Vec<f64> = rows()
            .zip(&distance)
            .map(|(r, &d)| {
                let t = if d > 10.0 { long_t } else { short_t };
                if s.tolls.at(r) < t { 6.94 } else { 0.0 }
            })
            .collect();
        let cong_t = threshold(0.92);
        let congestion: Vec<f64> = rows()
            .map(|r| {
                if s.congestion.at(r) < cong_t {
                    2.5
                } else {
                    0.0
                }
            })
            .collect();
        let airport: Vec<f64> = pu
            .iter()
            .map(|&z| if z == JFK || z == LGA { 1.75 } else { 0.0 })
            .collect();
        let total: Vec<f64> = (0..len)
            .map(|i| {
                let opt = if missing(i) {
                    0.0
                } else {
                    congestion[i] + airport[i]
                };
                cents(fare[i] + extra[i] + 0.5 + tip[i] + tolls[i] + 1.0 + opt)
            })
            .collect();

        let ((vendor_c, pu_t, do_t, pass_c, dist_c, rate_c, flag_c, pu_c, dz_c, pay_c), money) =
            out;
        put(vendor_c, vendor);
        put(pu_t, pickup);
        put(do_t, dropoff);
        put_nullable(pass_c, passengers, &present);
        put(dist_c, distance);
        put_nullable(rate_c, rate, &present);
        self.flag.write(&flag, &mut flag_c.values);
        flag_c.valid.append(&present);
        put(pu_c, pu);
        put(dz_c, dropoff_zone);
        put(pay_c, payment);
        let (fare_c, extra_c, mta_c, tip_c, tolls_c, imp_c, total_c, cong_c, air_c) = money;
        put(fare_c, fare);
        put(extra_c, extra);
        put(mta_c, vec![0.5; len]);
        put(tip_c, tip);
        put(tolls_c, tolls);
        put(imp_c, vec![1.0; len]);
        put(total_c, total);
        put_nullable(cong_c, congestion, &present);
        put_nullable(air_c, airport, &present);
    }
}

impl Shape for Taxi {
    fn name(&self) -> &'static str {
        "taxi"
    }

    fn is_plain(&self, column: &str) -> bool {
        column.ends_with("_datetime")
    }

    fn dist(
        &self,
    ) -> impl Distribution<Item: Columnar<Columns: IntoArrowColumns>> + Clone + Send + Sync + use<>
    {
        let t = self.inner.clone();
        named_all(
            &COLUMNS,
            derive_columns::<_, _, Trip>(row_numbers(), move |rows, out| t.write(rows, out)),
        )
    }
}
