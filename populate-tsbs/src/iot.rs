//! TSBS `iot` use case: trucks reporting `readings` and `diagnostics`, with
//! TSBS's realistic transport faults: missing batches, out-of-order batches
//! and entries, and randomly cleared fields/tags. Ported from
//! `pkg/data/usecases/iot`.

use crate::fixed::Fixed;
use crate::gorand::GoRandExt;
use crate::influx::{FieldValue, LineWriter};
use crate::panel::{Entity, EntityFactory, PanelConfig, PanelCursor};
use crate::process::{
    Lazy, Normal, Precision, Process, RefillWalk, SharedUniform, Uniform, Walk, clamped_walk, lazy,
    normal, precision, shared_uniform, uniform,
};
use arrow_array::{ArrayRef, Float64Array, Int64Array, StructArray};
use arrow_schema::{DataType, Field, Fields};
use populate_2::arrow::IntoArrowArray;
use populate_2::traits::{Backend, DefaultSink, Distribution, HCons, HNil, SampleIter, Sink};
use rand::Rng;
use std::array::from_fn;
use std::collections::VecDeque;
use std::sync::Arc;

pub const DRIVERS: [&str; 6] = ["Derek", "Rodney", "Albert", "Andy", "Seth", "Trish"];
pub const FLEETS: [&str; 4] = ["East", "West", "North", "South"];
pub const DEVICE_VERSIONS: [&str; 4] = ["v1.0", "v1.5", "v2.0", "v2.3"];
/// `(name, load_capacity, fuel_capacity, fuel_consumption)`
pub const MODELS: [(&str, f32, f32, f32); 3] = [
    ("F-150", 2000.0, 200.0, 15.0),
    ("G-2000", 5000.0, 300.0, 19.0),
    ("H-2", 1500.0, 150.0, 12.0),
];

pub const STRING_TAGS: [&str; 5] = ["name", "fleet", "driver", "model", "device_version"];
pub const FLOAT_TAGS: [&str; 3] = ["load_capacity", "fuel_capacity", "nominal_fuel_consumption"];
pub const READINGS_FIELDS: [&str; 7] = [
    "latitude",
    "longitude",
    "elevation",
    "velocity",
    "heading",
    "grade",
    "fuel_consumption",
];
pub const DIAGNOSTICS_FIELDS: [&str; 3] = ["fuel_state", "current_load", "status"];

// ============================================================================
// Rows
// ============================================================================

pub type StringTags = Fixed<Option<String>, 5>;
pub type FloatTags = Fixed<Option<f32>, 3>;
/// `(timestamp_ns, string tags, float tags, fields)`
pub type ReadingsRow = (i64, StringTags, FloatTags, Fixed<Option<f64>, 7>);
/// `(timestamp_ns, string tags, float tags, fields)`
pub type DiagnosticsRow = (i64, StringTags, FloatTags, DiagnosticsFields);

/// Diagnostics fields: two floats and an integer status.
#[derive(Clone, Debug, PartialEq)]
pub struct DiagnosticsFields {
    pub fuel_state: Option<f64>,
    pub current_load: Option<f64>,
    pub status: Option<i64>,
}

#[derive(Debug, Default, PartialEq)]
pub struct DiagnosticsCols<F, I> {
    pub fuel_state: F,
    pub current_load: F,
    pub status: I,
}

impl<B: Backend> DefaultSink<B> for DiagnosticsFields {
    type Sink = HCons<DiagnosticsCols<B::Prim<Option<f64>>, B::Prim<Option<i64>>>, HNil>;
}

impl<F: Sink<Option<f64>>, I: Sink<Option<i64>>> Sink<DiagnosticsFields> for DiagnosticsCols<F, I> {
    fn push(&mut self, v: DiagnosticsFields) {
        self.fuel_state.push(v.fuel_state);
        self.current_load.push(v.current_load);
        self.status.push(v.status);
    }
}

impl IntoArrowArray for DiagnosticsCols<Vec<Option<f64>>, Vec<Option<i64>>> {
    fn into_array(self) -> ArrayRef {
        let f = |n: &str, dt| Field::new(n, dt, true);
        Arc::new(StructArray::new(
            Fields::from(vec![
                f("f0", DataType::Float64),
                f("f1", DataType::Float64),
                f("f2", DataType::Int64),
            ]),
            vec![
                Arc::new(Float64Array::from(self.fuel_state)) as ArrayRef,
                Arc::new(Float64Array::from(self.current_load)),
                Arc::new(Int64Array::from(self.status)),
            ],
            None,
        ))
    }
}

/// One IoT point: a readings or diagnostics row.
#[derive(Clone, Debug, PartialEq)]
pub enum IotPoint {
    Readings(ReadingsRow),
    Diagnostics(DiagnosticsRow),
}

impl IotPoint {
    fn field_count(&self) -> usize {
        match self {
            IotPoint::Readings(_) => READINGS_FIELDS.len(),
            IotPoint::Diagnostics(_) => DIAGNOSTICS_FIELDS.len(),
        }
    }

    /// TSBS `ClearFieldValue(FieldKeys()[i])`.
    fn clear_field(&mut self, i: usize) {
        match self {
            IotPoint::Readings(r) => r.3.0[i] = None,
            IotPoint::Diagnostics(r) => match i {
                0 => r.3.fuel_state = None,
                1 => r.3.current_load = None,
                _ => r.3.status = None,
            },
        }
    }

    /// TSBS `ClearTagValue(TagKeys()[i])`: tags 0..5 are strings, 5..8 floats.
    fn clear_tag(&mut self, i: usize) {
        let (s, f) = match self {
            IotPoint::Readings(r) => (&mut r.1, &mut r.2),
            IotPoint::Diagnostics(r) => (&mut r.1, &mut r.2),
        };
        if i < 5 {
            s.0[i] = None;
        } else {
            f.0[i - 5] = None;
        }
    }

    /// Append this point as one TSBS-format Influx line (nothing if every
    /// field and float tag was cleared).
    pub fn write_line(&self, out: &mut String) {
        let (name, ts, s, f) = match self {
            IotPoint::Readings(r) => ("readings", r.0, &r.1, &r.2),
            IotPoint::Diagnostics(r) => ("diagnostics", r.0, &r.1, &r.2),
        };
        let mut w = LineWriter::new(out, name);
        for (k, v) in STRING_TAGS.iter().zip(&s.0) {
            w.tag(k, v.as_deref());
        }
        w.begin_fields(true);
        for (k, v) in FLOAT_TAGS.iter().zip(f.0) {
            w.field(k, v.map(FieldValue::Float32));
        }
        match self {
            IotPoint::Readings(r) => {
                for (k, v) in READINGS_FIELDS.iter().zip(r.3.0) {
                    w.field(k, v.map(FieldValue::Float));
                }
            }
            IotPoint::Diagnostics(r) => {
                w.field("fuel_state", r.3.fuel_state.map(FieldValue::Float));
                w.field("current_load", r.3.current_load.map(FieldValue::Float));
                w.field("status", r.3.status.map(FieldValue::Int));
            }
        }
        w.finish(ts);
    }
}

// ============================================================================
// Trucks
// ============================================================================

type Reading = Precision<Walk<Uniform>>;

/// A simulated truck.
#[derive(Clone, Debug)]
pub struct Truck {
    tags: StringTags,
    capacities: FloatTags,
    readings: [Reading; 7],
    fuel: Precision<RefillWalk<Uniform>>,
    load: Precision<Lazy<Uniform, SharedUniform>>,
    status: Precision<Walk<Normal>>,
}

impl Entity for Truck {
    type Point = IotPoint;
    fn slots(&self) -> usize {
        2
    }
    fn tick(&mut self, rng: &mut impl Rng, _interval_ns: i64) {
        for r in &mut self.readings {
            r.advance(rng);
        }
        self.fuel.advance(rng);
        self.load.advance(rng);
        self.status.advance(rng);
    }
    fn point(&self, slot: usize, ts: i64) -> IotPoint {
        let (s, f) = (self.tags.clone(), self.capacities.clone());
        if slot == 0 {
            IotPoint::Readings((ts, s, f, Fixed(from_fn(|i| Some(self.readings[i].get())))))
        } else {
            IotPoint::Diagnostics((
                ts,
                s,
                f,
                DiagnosticsFields {
                    fuel_state: Some(self.fuel.get()),
                    current_load: Some(self.load.get()),
                    status: Some(self.status.get() as i64),
                },
            ))
        }
    }
}

/// Builds trucks. All trucks share one `current_load` uniform (as in TSBS,
/// where it is a package-level variable); it is reset when truck 0 is built.
#[derive(Clone, Debug)]
pub struct TruckFactory {
    load: SharedUniform,
}

impl Default for TruckFactory {
    fn default() -> Self {
        TruckFactory {
            load: shared_uniform(0.0, 5000.0),
        }
    }
}

impl EntityFactory for TruckFactory {
    type Entity = Truck;
    fn make(&mut self, id: usize, rng: &mut impl Rng) -> Truck {
        if id == 0 {
            self.load = shared_uniform(0.0, 5000.0);
        }
        let geo = || uniform(-0.005, 0.005);
        let big = || uniform(-10.0, 10.0);
        let small = || uniform(-5.0, 5.0);
        // Field constructors run in order; some draw their initial state.
        let lat = precision(clamped_walk(geo(), -90.0, 90.0, rng.go_float64() * 90.0), 5);
        let lon = precision(
            clamped_walk(geo(), -180.0, 180.0, rng.go_float64() * 180.0),
            5,
        );
        let elevation = precision(
            clamped_walk(big(), 0.0, 5000.0, rng.go_float64() * 500.0),
            0,
        );
        let velocity = precision(clamped_walk(big(), 0.0, 100.0, 0.0), 0);
        let heading = precision(
            clamped_walk(small(), 0.0, 360.0, rng.go_float64() * 360.0),
            0,
        );
        let grade = precision(clamped_walk(small(), 0.0, 100.0, 0.0), 0);
        let fuel_consumption = precision(clamped_walk(small(), 0.0, 50.0, 25.0), 1);

        let (model, load_cap, fuel_cap, consumption) = *rng.go_choice(&MODELS);
        let fleet = rng.go_choice(&FLEETS).to_string();
        let driver = rng.go_choice(&DRIVERS).to_string();
        let version = rng.go_choice(&DEVICE_VERSIONS).to_string();
        Truck {
            tags: Fixed([
                Some(format!("truck_{id}")),
                Some(fleet),
                Some(driver),
                Some(model.to_string()),
                Some(version),
            ]),
            capacities: Fixed([Some(load_cap), Some(fuel_cap), Some(consumption)]),
            readings: [
                lat,
                lon,
                elevation,
                velocity,
                heading,
                grade,
                fuel_consumption,
            ],
            fuel: precision(
                RefillWalk(clamped_walk(uniform(-0.001, 0.0), 0.0, 1.0, 1.0)),
                1,
            ),
            load: precision(lazy(uniform(0.0, 1.0), self.load.clone(), 0.95), 0),
            status: precision(clamped_walk(normal(0.0, 1.0), 0.0, 5.0, 0.0), 0),
        }
    }
}

// ============================================================================
// Fault injection (iot/simulator.go + batch_config.go)
// ============================================================================

const BATCH_SIZE: usize = 10;
const FIELD_COUNT: usize = 7; // max fields over the two measurements
const TAG_COUNT: usize = 8;

/// Per-batch fault plan (TSBS `batchConfig`). Entry maps are keyed by
/// position `0..BATCH_SIZE`; lookups past that are "not set".
#[derive(Debug, Default)]
struct BatchConfig {
    insert_previous: bool,
    missing: bool,
    out_of_order: bool,
    zero_fields: [Option<usize>; BATCH_SIZE],
    zero_tags: [Option<usize>; BATCH_SIZE],
    insert_previous_entry: [bool; BATCH_SIZE],
    missing_entries: [bool; BATCH_SIZE],
    out_of_order_entries: [bool; BATCH_SIZE],
}

fn flag(map: &[bool; BATCH_SIZE], i: usize) -> bool {
    map.get(i).copied().unwrap_or(false)
}

fn new_batch_config(
    rng: &mut impl Rng,
    out_of_order_batches: usize,
    mut out_of_order_entries: usize,
) -> BatchConfig {
    if rng.go_float64() < 0.01 {
        return BatchConfig {
            missing: true,
            ..Default::default()
        };
    }
    let mut bc = BatchConfig {
        out_of_order: rng.go_float64() < 0.05,
        ..Default::default()
    };
    if out_of_order_batches > 0 {
        bc.insert_previous = rng.go_float64() < 0.5;
    }
    for i in 0..BATCH_SIZE {
        if out_of_order_entries > 0 && rng.go_float64() < 0.5 {
            bc.insert_previous_entry[i] = true;
            out_of_order_entries -= 1;
        }
        if rng.go_float64() < 0.1 {
            bc.missing_entries[i] = true;
            continue;
        }
        if rng.go_float64() < 0.1 {
            bc.zero_fields[i] = Some(rng.go_intn(FIELD_COUNT));
        }
        if rng.go_float64() < 0.01 {
            bc.zero_tags[i] = Some(rng.go_intn(TAG_COUNT));
        }
        if rng.go_float64() < 0.3 {
            bc.out_of_order_entries[i] = true;
        }
    }
    bc
}

/// Row stream with TSBS's IoT faults applied on top of a truck panel.
pub struct IotStream {
    base: PanelCursor<TruckFactory>,
    max_points: u64,
    curr_batch: VecDeque<IotPoint>,
    out_of_order_batches: VecDeque<Vec<IotPoint>>,
    out_of_order_entries: VecDeque<IotPoint>,
    offset: usize,
}

impl IotStream {
    /// `max_points` caps the underlying panel slots (TSBS `maxPoints`).
    pub fn new(config: PanelConfig, max_points: u64) -> Self {
        IotStream {
            base: PanelCursor::new(TruckFactory::default(), config),
            max_points,
            curr_batch: VecDeque::new(),
            out_of_order_batches: VecDeque::new(),
            out_of_order_entries: VecDeque::new(),
            offset: 0,
        }
    }

    fn base_finished(&self) -> bool {
        self.base.made() >= self.max_points
    }

    fn pending(&self) -> bool {
        !self.out_of_order_batches.is_empty() || !self.out_of_order_entries.is_empty()
    }

    /// TSBS `Simulator.Finished`.
    pub fn finished(&self) -> bool {
        self.base_finished() && self.curr_batch.is_empty() && !self.pending()
    }

    /// TSBS `Simulator.Next`: the next point to write, if any.
    pub fn next_point(&mut self, rng: &mut impl Rng) -> Option<IotPoint> {
        // Trucks are built before the first batch plan is drawn.
        self.base.init(rng);
        if !self.curr_batch.is_empty() || self.simulate_next_batch(rng) {
            return self.curr_batch.pop_front();
        }
        None
    }

    fn batch_pending(&mut self) -> Vec<IotPoint> {
        if let Some(b) = self.out_of_order_batches.pop_front() {
            return b;
        }
        let n = self.out_of_order_entries.len().min(BATCH_SIZE);
        self.out_of_order_entries.drain(..n).collect()
    }

    fn simulate_next_batch(&mut self, rng: &mut impl Rng) -> bool {
        loop {
            if self.base_finished() {
                if self.pending() {
                    self.curr_batch = self.batch_pending().into();
                    return true;
                }
                return false;
            }
            let bc = new_batch_config(
                rng,
                self.out_of_order_batches.len(),
                self.out_of_order_entries.len(),
            );
            if bc.insert_previous {
                let b = self
                    .out_of_order_batches
                    .pop_front()
                    .expect("insert previous batch without out-of-order batches");
                self.curr_batch = b.into();
                return true;
            }
            if bc.missing {
                self.flush_batch(rng);
                continue;
            }
            if bc.out_of_order {
                let b = self.generate_batch(rng, &bc);
                if !b.is_empty() {
                    self.out_of_order_batches.push_back(b);
                }
                continue;
            }
            let b = self.generate_batch(rng, &bc);
            if !b.is_empty() {
                self.curr_batch = b.into();
                return true;
            }
        }
    }

    fn generate_batch(&mut self, rng: &mut impl Rng, bc: &BatchConfig) -> Vec<IotPoint> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        self.offset = 0;
        for i in 0..BATCH_SIZE {
            if self.base_finished() {
                break;
            }
            let Some(mut entry) = self.next_entry(rng, i, bc) else {
                break;
            };
            if let Some(mut idx) = bc.zero_fields[i] {
                let n = entry.field_count();
                if idx >= n {
                    idx %= n;
                }
                entry.clear_field(idx);
            }
            if let Some(idx) = bc.zero_tags[i] {
                entry.clear_tag(idx);
            }
            batch.push(entry);
        }
        batch
    }

    fn next_entry(
        &mut self,
        rng: &mut impl Rng,
        index: usize,
        bc: &BatchConfig,
    ) -> Option<IotPoint> {
        loop {
            let at = index + self.offset;
            let entry = if flag(&bc.insert_previous_entry, at) {
                self.out_of_order_entries
                    .pop_front()
                    .expect("insert previous entry without out-of-order entries")
            } else {
                let (p, valid) = self.base.next_slot(rng);
                if !valid {
                    return None;
                }
                p
            };
            if flag(&bc.missing_entries, at) {
                self.offset += 1;
                continue;
            }
            if flag(&bc.out_of_order_entries, at) {
                self.out_of_order_entries.push_back(entry);
                self.offset += 1;
                continue;
            }
            return Some(entry);
        }
    }

    fn flush_batch(&mut self, rng: &mut impl Rng) {
        for _ in 0..BATCH_SIZE {
            if !self.base.next_slot(rng).1 {
                break;
            }
        }
    }
}

/// The IoT use case as a populate-2 distribution (rows = written points).
#[derive(Clone, Copy, Debug)]
pub struct Iot {
    pub config: PanelConfig,
    pub max_points: u64,
}

impl SampleIter for IotStream {
    type Item = IotPoint;
    fn next(&mut self, rng: &mut impl Rng) -> IotPoint {
        loop {
            if let Some(p) = self.next_point(rng) {
                return p;
            }
            assert!(!self.finished(), "TSBS iot simulation exhausted");
        }
    }
}

impl<B: Backend> Distribution<B> for Iot
where
    IotPoint: DefaultSink<B>,
    <IotPoint as DefaultSink<B>>::Sink: Sink<IotPoint>,
{
    type Item = IotPoint;
    type RowIter = IotStream;

    fn row_iter(&mut self) -> IotStream {
        IotStream::new(self.config, self.max_points)
    }

    fn collect_into(
        &mut self,
        mut sink: <IotPoint as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <IotPoint as DefaultSink<B>>::Sink {
        let mut iter = <Self as Distribution<B>>::row_iter(self);
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

// ============================================================================
// Demultiplexing sink
// ============================================================================

type SinkOf<B, R> = <R as DefaultSink<B>>::Sink;

/// Columnar sinks for readings and diagnostics.
pub struct IotSinks<B: Backend>
where
    ReadingsRow: DefaultSink<B>,
    DiagnosticsRow: DefaultSink<B>,
{
    pub readings: SinkOf<B, ReadingsRow>,
    pub diagnostics: SinkOf<B, DiagnosticsRow>,
}

impl<B: Backend> Default for IotSinks<B>
where
    ReadingsRow: DefaultSink<B>,
    DiagnosticsRow: DefaultSink<B>,
    SinkOf<B, ReadingsRow>: Default,
    SinkOf<B, DiagnosticsRow>: Default,
{
    fn default() -> Self {
        IotSinks {
            readings: Default::default(),
            diagnostics: Default::default(),
        }
    }
}

impl<B: Backend> Sink<IotPoint> for IotSinks<B>
where
    ReadingsRow: DefaultSink<B>,
    DiagnosticsRow: DefaultSink<B>,
    SinkOf<B, ReadingsRow>: Sink<ReadingsRow>,
    SinkOf<B, DiagnosticsRow>: Sink<DiagnosticsRow>,
{
    fn push(&mut self, value: IotPoint) {
        match value {
            IotPoint::Readings(r) => self.readings.push(r),
            IotPoint::Diagnostics(r) => self.diagnostics.push(r),
        }
    }
}

impl<B: Backend> DefaultSink<B> for IotPoint
where
    ReadingsRow: DefaultSink<B>,
    DiagnosticsRow: DefaultSink<B>,
{
    type Sink = HCons<IotSinks<B>, HNil>;
}
