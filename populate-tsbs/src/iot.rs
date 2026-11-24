//! The `iot` use case: trucks with `readings` and `diagnostics`, and the
//! batch simulator on top that drops, delays, reorders and zeroes entries.
//!
//! The draws interleave: batch configurations take `Float64`s between the
//! ticks, so everything runs in Go's order, one point at a time.

use crate::config::Config;
use crate::gorand::{GoRand, go_f64_to_i64};
use crate::model::{Measurement, NO_TAG, Point, Statics, TagVal, Type, Val};
use crate::sim::{Sim, epoch_generators};
use std::collections::VecDeque;
use std::sync::Arc;

const BATCH: usize = 10;
const READINGS: [&str; 7] = [
    "latitude",
    "longitude",
    "elevation",
    "velocity",
    "heading",
    "grade",
    "fuel_consumption",
];
const DIAGNOSTICS: [&str; 3] = ["fuel_state", "current_load", "status"];
/// Tag keys of every truck.
pub const TRUCK_TAG_KEYS: [&str; 8] = [
    "name",
    "fleet",
    "driver",
    "model",
    "device_version",
    "load_capacity",
    "fuel_capacity",
    "nominal_fuel_consumption",
];
const DRIVERS: [&str; 6] = ["Derek", "Rodney", "Albert", "Andy", "Seth", "Trish"];
const MODELS: [(&str, f32, f32, f32); 3] = [
    ("F-150", 2000.0, 200.0, 15.0),
    ("G-2000", 5000.0, 300.0, 19.0),
    ("H-2", 1500.0, 150.0, 12.0),
];
const DEVICE_VERSIONS: [&str; 4] = ["v1.0", "v1.5", "v2.0", "v2.3"];
const FLEETS: [&str; 4] = ["East", "West", "North", "South"];

/// Per truck: the seven readings walks, fuel, status.
const DISTS: usize = 9;

/// Readings walks: `(step low, step high, min, max, precision)`.
const WALKS: [(f64, f64, f64, f64, f64); 7] = [
    (-0.005, 0.005, -90.0, 90.0, 100000.0),
    (-0.005, 0.005, -180.0, 180.0, 100000.0),
    (-10.0, 10.0, 0.0, 5000.0, 1.0),
    (-10.0, 10.0, 0.0, 100.0, 1.0),
    (-5.0, 5.0, 0.0, 360.0, 1.0),
    (-5.0, 5.0, 0.0, 100.0, 1.0),
    (-5.0, 5.0, 0.0, 50.0, 10.0),
];

#[derive(Debug)]
struct Static {
    statics: Arc<Statics>,
    trucks: u64,
    /// `UnixNano` of the start: wrapping, like Go's.
    start: i64,
    interval: i64,
}

/// TSBS's `BaseSimulator` over the trucks.
#[derive(Clone)]
struct Base {
    rng: GoRand,
    /// Per truck, [`DISTS`] states.
    state: Vec<f64>,
    /// The `loadUD` value every truck's `current_load` reads (one shared
    /// distribution in Go).
    load: f64,
    made: u64,
    max: u64,
    source: u64,
    meas: usize,
    epoch: u64,
    epochs: u64,
    epoch_gens: u64,
    init_gens: u64,
}

/// One batch's random decisions (`batchConfig`).
#[derive(Default)]
struct BatchConfig {
    insert_previous: bool,
    missing: bool,
    out_of_order: bool,
    zero_fields: [Option<usize>; BATCH],
    zero_tags: [Option<usize>; BATCH],
    insert_previous_entry: [bool; BATCH],
    missing_entries: [bool; BATCH],
    out_of_order_entries: [bool; BATCH],
}

fn at(a: &[bool; BATCH], i: usize) -> bool {
    i < BATCH && a[i]
}

/// The IoT simulator.
#[derive(Clone)]
pub struct IotSim {
    st: Arc<Static>,
    base: Base,
    curr: VecDeque<Point>,
    ooo_batches: VecDeque<Vec<Point>>,
    ooo_entries: VecDeque<Point>,
    offset: usize,
}

impl Base {
    fn ud(&mut self, lo: f64, hi: f64) -> f64 {
        let mut x = self.rng.float64();
        x *= hi - lo;
        x += lo;
        x
    }

    fn tick(&mut self) {
        let n = self.state.len() / DISTS;
        for t in 0..n {
            for (k, &(lo, hi, min, max, _)) in WALKS.iter().enumerate() {
                let step = self.ud(lo, hi);
                let v = &mut self.state[t * DISTS + k];
                *v = clamp(*v + step, min, max);
            }
            // fuel: a clamped walk that refills when empty
            let step = self.ud(-0.001, 0.0);
            let v = &mut self.state[t * DISTS + 7];
            *v = clamp(*v + step, 0.0, 1.0);
            if *v == 0.0 {
                *v = 1.0;
            }
            // load: lazy, changes 5% of the time
            if self.ud(0.0, 1.0) >= 1.0 - 0.05 {
                self.load = self.ud(0.0, 5000.0);
            }
            // status
            let step = self.rng.norm_float64() * 1.0 + 0.0;
            let v = &mut self.state[t * DISTS + 8];
            *v = clamp(*v + step, 0.0, 5.0);
        }
        self.epoch += 1;
        self.epoch_gens = epoch_generators(self.init_gens, n as u64, self.epoch, self.epochs);
    }

    /// `BaseSimulator.Next`.
    fn next(&mut self, st: &Static, p: &mut Point) -> bool {
        if self.source == st.trucks {
            self.source = 0;
            self.meas += 1;
        }
        if self.meas == 2 {
            self.meas = 0;
            self.tick();
        }
        let t = self.source as usize;
        p.meas = self.meas as u32;
        p.source = t as u32;
        p.ts = st
            .start
            .wrapping_add((self.epoch as i64).wrapping_mul(st.interval));
        p.zero_tag = NO_TAG;
        p.fields.clear();
        let s = &self.state[t * DISTS..][..DISTS];
        let fp = |v: f64, prec: f64| go_f64_to_i64(v * prec) as f64 / prec;
        if self.meas == 0 {
            p.fields
                .extend(WALKS.iter().zip(s).map(|(w, &v)| Val::F(fp(v, w.4))));
        } else {
            p.fields.push(Val::F(fp(s[7], 10.0)));
            p.fields.push(Val::F(fp(self.load, 1.0)));
            p.fields.push(Val::I(go_f64_to_i64(fp(s[8], 1.0))));
        }
        let ret = self.source < self.epoch_gens;
        self.made += 1;
        self.source += 1;
        ret
    }

    fn finished(&self) -> bool {
        self.made >= self.max
    }

    /// `newBatchConfig`.
    fn batch_config(&mut self, ooo_batches: usize, mut ooo_entries: usize) -> BatchConfig {
        let mut bc = BatchConfig::default();
        if self.rng.float64() < 0.01 {
            bc.missing = true;
            return bc;
        }
        bc.out_of_order = self.rng.float64() < 0.05;
        if ooo_batches > 0 {
            bc.insert_previous = self.rng.float64() < 0.5;
        }
        let (fields, tags) = (READINGS.len(), TRUCK_TAG_KEYS.len());
        for i in 0..BATCH {
            if ooo_entries > 0 && self.rng.float64() < 0.5 {
                bc.insert_previous_entry[i] = true;
                ooo_entries -= 1;
            }
            if self.rng.float64() < 0.1 {
                bc.missing_entries[i] = true;
                continue;
            }
            if self.rng.float64() < 0.1 {
                bc.zero_fields[i] = Some(self.rng.intn(fields));
            }
            if self.rng.float64() < 0.01 {
                bc.zero_tags[i] = Some(self.rng.intn(tags));
            }
            if self.rng.float64() < 0.3 {
                bc.out_of_order_entries[i] = true;
            }
        }
        bc
    }
}

#[inline(always)]
fn clamp(mut v: f64, min: f64, max: f64) -> f64 {
    if v > max {
        v = max;
    }
    if v < min {
        v = min;
    }
    v
}

impl IotSim {
    /// `rand.Seed(cfg.seed)` and the trucks. `cfg` must be validated.
    pub fn new(cfg: &Config) -> Self {
        let trucks = cfg.scale;
        let mut rng = GoRand::new(cfg.seed);
        let mut state = Vec::with_capacity(trucks as usize * DISTS);
        let mut gen_tags = Vec::with_capacity(trucks as usize);
        for i in 0..trucks {
            // readings (latitude, longitude, elevation, velocity, heading,
            // grade, fuel_consumption), then diagnostics, then the tags
            let lat = rng.float64() * 90.0;
            let lon = rng.float64() * 180.0;
            let elev = rng.float64() * 500.0;
            let heading = rng.float64() * 360.0;
            state.extend([lat, lon, elev, 0.0, heading, 0.0, 25.0, 1.0, 0.0]);
            let m = MODELS[rng.intn(MODELS.len())];
            let fleet = FLEETS[rng.intn(FLEETS.len())];
            let driver = DRIVERS[rng.intn(DRIVERS.len())];
            let version = DEVICE_VERSIONS[rng.intn(DEVICE_VERSIONS.len())];
            let s = |v: &str| TagVal::Str(v.into());
            gen_tags.push(vec![
                s(&format!("truck_{i}")),
                s(fleet),
                s(driver),
                s(m.0),
                s(version),
                TagVal::F32(m.1),
                TagVal::F32(m.2),
                TagVal::F32(m.3),
            ]);
        }
        let ms = |name: &str, fields: &[&str], types: Vec<Type>| Measurement {
            name: name.into(),
            extra_tags: vec![],
            fields: fields.iter().map(|&f| f.into()).collect(),
            types,
        };
        let measurements = vec![
            ms("readings", &READINGS, vec![Type::F64; 7]),
            ms(
                "diagnostics",
                &DIAGNOSTICS,
                vec![Type::F64, Type::F64, Type::I64],
            ),
        ];
        let header_fields = measurements.iter().map(|m| m.fields.clone()).collect();
        let mut tag_types = vec![Type::Str; 5];
        tag_types.extend([Type::F32; 3]);
        let statics = Statics {
            start: cfg.start,
            tag_keys: TRUCK_TAG_KEYS.iter().map(|&s| s.into()).collect(),
            tag_types,
            measurements,
            extra_tags: vec![vec![vec![], vec![]]; trucks as usize],
            gen_tags,
            header_fields,
        };
        let epochs = cfg.epochs();
        let mut max = epochs.wrapping_mul(trucks).wrapping_mul(2);
        if cfg.limit > 0 && cfg.limit < max {
            max = cfg.limit;
        }
        IotSim {
            st: Arc::new(Static {
                statics: Arc::new(statics),
                trucks,
                start: cfg.start as i64,
                interval: cfg.interval,
            }),
            base: Base {
                rng,
                state,
                load: 0.0,
                made: 0,
                max,
                source: 0,
                meas: 0,
                epoch: 0,
                epochs,
                epoch_gens: cfg.initial_scale,
                init_gens: cfg.initial_scale,
            },
            curr: VecDeque::new(),
            ooo_batches: VecDeque::new(),
            ooo_entries: VecDeque::new(),
            offset: 0,
        }
    }

    fn pending(&self) -> bool {
        !self.ooo_batches.is_empty() || !self.ooo_entries.is_empty()
    }

    /// `batchPending`.
    fn batch_pending(&mut self) -> VecDeque<Point> {
        if let Some(b) = self.ooo_batches.pop_front() {
            return b.into();
        }
        let n = self.ooo_entries.len().min(BATCH);
        self.ooo_entries.drain(..n).collect()
    }

    /// `simulateNextBatch`.
    fn simulate_next_batch(&mut self) -> bool {
        loop {
            if self.base.finished() {
                if self.pending() {
                    self.curr = self.batch_pending();
                    return true;
                }
                return false;
            }
            let bc = self
                .base
                .batch_config(self.ooo_batches.len(), self.ooo_entries.len());
            if bc.insert_previous {
                let b = self
                    .ooo_batches
                    .pop_front()
                    .expect("trying to insert an out of order batch when there are none");
                self.curr = b.into();
                return true;
            }
            if bc.missing {
                // flushBatch
                let mut p = Point::default();
                for _ in 0..BATCH {
                    if !self.base.next(&self.st, &mut p) {
                        break;
                    }
                }
                continue;
            }
            if bc.out_of_order {
                let b = self.generate_batch(&bc);
                if !b.is_empty() {
                    self.ooo_batches.push_back(b);
                }
                continue;
            }
            let b = self.generate_batch(&bc);
            if b.is_empty() {
                continue;
            }
            self.curr = b.into();
            return true;
        }
    }

    /// `generateBatch`.
    fn generate_batch(&mut self, bc: &BatchConfig) -> Vec<Point> {
        let mut batch = Vec::with_capacity(BATCH);
        self.offset = 0;
        for i in 0..BATCH {
            if self.base.finished() {
                break;
            }
            let Some(mut entry) = self.next_entry(i, bc) else {
                break;
            };
            if let Some(mut k) = bc.zero_fields[i] {
                if k >= entry.fields.len() {
                    k %= entry.fields.len();
                }
                entry.fields[k] = Val::Nil;
            }
            if let Some(k) = bc.zero_tags[i] {
                entry.zero_tag = k as u8;
            }
            batch.push(entry);
        }
        batch
    }

    /// `getNextEntry`; `None` when the base simulator says not to write.
    fn next_entry(&mut self, index: usize, bc: &BatchConfig) -> Option<Point> {
        loop {
            let k = index + self.offset;
            let entry = if at(&bc.insert_previous_entry, k) {
                self.ooo_entries
                    .pop_front()
                    .expect("trying to insert an out of order entry when there are none")
            } else {
                let mut p = Point::default();
                if !self.base.next(&self.st, &mut p) {
                    return None;
                }
                p
            };
            if at(&bc.missing_entries, k) {
                self.offset += 1;
                continue;
            }
            if at(&bc.out_of_order_entries, k) {
                self.ooo_entries.push_back(entry);
                self.offset += 1;
                continue;
            }
            return Some(entry);
        }
    }
}

impl Sim for IotSim {
    fn statics(&self) -> &Arc<Statics> {
        &self.st.statics
    }

    fn finished(&self) -> bool {
        self.base.finished() && self.curr.is_empty() && !self.pending()
    }

    fn next(&mut self, p: &mut Point) -> bool {
        if !self.curr.is_empty() || self.simulate_next_batch() {
            *p = self.curr.pop_front().expect("non-empty batch");
            return true;
        }
        false
    }

    fn progress(&self) -> u64 {
        self.base.made
    }

    fn max_points(&self) -> u64 {
        self.base.max
    }
}
