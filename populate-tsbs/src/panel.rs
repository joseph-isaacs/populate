//! Entity × time panels: `N` entities, each with fixed attributes and evolving
//! state, observed at `T` evenly spaced timestamps.
//!
//! This is the shape of every TSBS use case (hosts × epochs, trucks × epochs)
//! and of panel / longitudinal data in general. The iteration order and RNG
//! call order are exactly TSBS's `BaseSimulator`:
//!
//! 1. all entities are created up front (`make(0)`, `make(1)`, ...);
//! 2. each epoch emits `slots` rows per entity, slot-major
//!    (`slot 0: e0, e1, ..`, `slot 1: e0, e1, ..`);
//! 3. between epochs every entity is ticked, in entity order;
//! 4. optionally only the first `active(epoch)` entities are emitted, ramping
//!    linearly from `initial` to all entities over the run (TSBS
//!    `--initial-scale`). Inactive entities still tick.

use populate_2::traits::{Backend, DefaultSink, Distribution, SampleIter, Sink};
use rand::Rng;

/// One simulated entity (a host, a truck).
pub trait Entity {
    type Point;
    /// Rows emitted per entity per epoch (measurements per host).
    fn slots(&self) -> usize;
    /// Move every piece of state forward by one interval.
    fn tick(&mut self, rng: &mut impl Rng, interval_ns: i64);
    /// The row for `slot` at `ts` (nanoseconds). Must not draw randomness.
    fn point(&self, slot: usize, ts: i64) -> Self::Point;
}

/// Builds entity `id` (all randomness for its attributes and initial state).
pub trait EntityFactory: Clone {
    type Entity: Entity;
    fn make(&mut self, id: usize, rng: &mut impl Rng) -> Self::Entity;
}

/// Timing and scale of a panel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PanelConfig {
    pub entities: u64,
    /// Entities active at epoch 0 (ramps to `entities`); `entities` for no ramp.
    pub initial_entities: u64,
    pub start_ns: i64,
    pub interval_ns: i64,
    /// Number of epochs (`(end - start) / interval`).
    pub epochs: u64,
}

impl PanelConfig {
    /// Entities emitted at `epoch` (TSBS `adjustNumHostsForEpoch`).
    pub fn active(&self, epoch: u64) -> u64 {
        if epoch == 0 {
            return self.initial_entities;
        }
        let missing = (self.entities - self.initial_entities) as f64;
        let ramp = missing * epoch as f64 / self.epochs.wrapping_sub(1) as f64;
        self.initial_entities.wrapping_add(go_f64_to_u64(ramp))
    }
}

/// Go's `uint64(f)` on amd64 (NaN / overflow → 2^63) so degenerate configs
/// (one epoch) behave like the reference instead of panicking.
fn go_f64_to_u64(f: f64) -> u64 {
    if !(0.0..18446744073709551616.0).contains(&f) {
        1 << 63
    } else {
        f as u64
    }
}

/// A slot-by-slot cursor over the panel: exactly TSBS's simulator `Next`.
/// Infinite; callers stop it (`made()` counts every slot, active or not).
pub struct PanelCursor<F: EntityFactory> {
    factory: F,
    config: PanelConfig,
    entities: Vec<F::Entity>,
    index: usize,
    slot: usize,
    epoch: u64,
    active: u64,
    made: u64,
}

impl<F: EntityFactory> PanelCursor<F> {
    pub fn new(factory: F, config: PanelConfig) -> Self {
        PanelCursor {
            factory,
            config,
            entities: Vec::new(),
            index: 0,
            slot: 0,
            epoch: 0,
            active: config.initial_entities,
            made: 0,
        }
    }

    /// Slots produced so far (TSBS `madePoints`).
    pub fn made(&self) -> u64 {
        self.made
    }

    /// Build all entities now (TSBS does this in `NewSimulator`, before any
    /// other draw). Idempotent; [`next_slot`](Self::next_slot) calls it.
    pub fn init(&mut self, rng: &mut impl Rng) {
        if self.entities.is_empty() {
            self.entities = (0..self.config.entities as usize)
                .map(|i| self.factory.make(i, rng))
                .collect();
        }
    }

    /// The next slot's row and whether its entity is active (TSBS `Next`'s
    /// return value: inactive rows are generated but not written).
    pub fn next_slot(&mut self, rng: &mut impl Rng) -> (<F::Entity as Entity>::Point, bool) {
        self.init(rng);
        if self.index == self.entities.len() {
            self.index = 0;
            self.slot += 1;
        }
        if self.slot == self.entities[0].slots() {
            self.slot = 0;
            for e in &mut self.entities {
                e.tick(rng, self.config.interval_ns);
            }
            self.epoch += 1;
            self.active = self.config.active(self.epoch);
        }
        let ts = self.config.start_ns + self.epoch as i64 * self.config.interval_ns;
        let point = self.entities[self.index].point(self.slot, ts);
        let active = (self.index as u64) < self.active;
        self.made += 1;
        self.index += 1;
        (point, active)
    }
}

/// The panel as a populate-2 distribution: each row is the next *active* slot.
#[derive(Clone)]
pub struct Panel<F> {
    pub factory: F,
    pub config: PanelConfig,
}

pub fn panel<F: EntityFactory>(factory: F, config: PanelConfig) -> Panel<F> {
    Panel { factory, config }
}

impl<F: EntityFactory> SampleIter for PanelCursor<F> {
    type Item = <F::Entity as Entity>::Point;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        loop {
            let (point, active) = self.next_slot(rng);
            if active {
                return point;
            }
        }
    }
}

impl<B: Backend, F: EntityFactory> Distribution<B> for Panel<F>
where
    <F::Entity as Entity>::Point: DefaultSink<B>,
    <<F::Entity as Entity>::Point as DefaultSink<B>>::Sink: Sink<<F::Entity as Entity>::Point>,
{
    type Item = <F::Entity as Entity>::Point;
    type RowIter = PanelCursor<F>;

    fn row_iter(&mut self) -> Self::RowIter {
        PanelCursor::new(self.factory.clone(), self.config)
    }

    fn collect_into(
        &mut self,
        mut sink: <Self::Item as DefaultSink<B>>::Sink,
        rng: &mut impl Rng,
        n: usize,
    ) -> <Self::Item as DefaultSink<B>>::Sink {
        let mut iter = <Self as Distribution<B>>::row_iter(self);
        for _ in 0..n {
            sink.push(iter.next(rng));
        }
        sink
    }
}

/// Rows a TSBS run writes: active slots among the first `max_points` slots,
/// where a run is `epochs × entities × slots` slots capped by `limit` (0 = none).
pub fn written_rows(config: &PanelConfig, slots: u64, limit: u64) -> u64 {
    let per_epoch = config.entities * slots;
    let mut max_points = config.epochs * per_epoch;
    if limit > 0 && limit < max_points {
        max_points = limit;
    }
    let mut rows = 0;
    let mut remaining = max_points;
    let mut epoch = 0;
    while remaining > 0 {
        let active = config.active(epoch).min(config.entities);
        let take = remaining.min(per_epoch);
        // `take` slots of this epoch: full slot passes then a partial one.
        let full = take / config.entities;
        let part = take % config.entities;
        rows += full * active + part.min(active);
        remaining -= take;
        epoch += 1;
    }
    rows
}
