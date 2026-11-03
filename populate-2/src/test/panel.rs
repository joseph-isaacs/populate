//! A toy panel: counters that tick by a random step, two slots per entity.

use crate::arrow::{DistributionArrow, flatten_named};
use crate::fixed::Fixed;
use crate::panel::{Entity, EntityFactory, PanelConfig, PanelCursor, panel, written_rows};
use crate::sampler::{FastSampler, Sampler};
use crate::traits::{Distribution, SampleIter, VecBackend};
use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, TimeUnit};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

struct Counter {
    id: i64,
    value: i64,
}

type Point = (i64, i64, Fixed<i64, 2>);

impl Entity for Counter {
    type Point = Point;
    fn slots(&self) -> usize {
        2
    }
    fn tick(&mut self, rng: &mut impl Rng, _interval_ns: i64) {
        self.value += 1 + FastSampler.index(rng, 3) as i64;
    }
    fn point(&self, slot: usize, ts: i64) -> Point {
        (ts, self.id, Fixed([slot as i64, self.value]))
    }
}

#[derive(Clone)]
struct Counters;

impl EntityFactory for Counters {
    type Entity = Counter;
    fn make(&mut self, id: usize, _rng: &mut impl Rng) -> Counter {
        Counter {
            id: id as i64,
            value: 0,
        }
    }
}

const CFG: PanelConfig = PanelConfig {
    entities: 3,
    initial_entities: 1,
    start_ns: 1_000,
    interval_ns: 10,
    epochs: 3,
};

fn make_panel() -> impl Distribution<VecBackend, Item = Point> {
    panel(Counters, CFG)
}

#[test]
fn panel_order_ramp_and_columns() {
    // active entities ramp 1 -> 2 -> 3 over the 3 epochs; 2 slots each.
    let rows = written_rows(&CFG, 2, 0);
    assert_eq!(rows, 2 * (1 + 2 + 3));
    assert_eq!(CFG.active(1), 2);

    let mut d = make_panel();
    let mut it = d.row_iter();
    let rng = &mut StdRng::seed_from_u64(5);
    let pts: Vec<Point> = (0..rows as usize).map(|_| it.next(rng)).collect();
    let ids: Vec<i64> = pts.iter().map(|p| p.1).collect();
    assert_eq!(ids, vec![0, 0, 0, 1, 0, 1, 0, 1, 2, 0, 1, 2]);
    let ts: Vec<i64> = pts.iter().map(|p| p.0).collect();
    assert_eq!(ts[..2], [1_000, 1_000]);
    assert_eq!(ts[11], 1_020);

    // Unflattened, the Fixed group is one struct column.
    let rb = d
        .collect_record_batch(&mut StdRng::seed_from_u64(5), rows as usize)
        .unwrap();
    assert_eq!(rb.num_columns(), 3);
    let sink = d.collect_into(
        Default::default(),
        &mut StdRng::seed_from_u64(5),
        rows as usize,
    );
    let flat = flatten_named(sink, &["ts", "id", "slot", "value"], Some(0)).unwrap();
    assert_eq!(
        flat.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    let values = flat.column(3).as_primitive::<Int64Type>().values().to_vec();
    assert_eq!(values, pts.iter().map(|p| p.2.0[1]).collect::<Vec<_>>());
    assert_eq!(flat.column(2).null_count(), 0);
}

#[test]
fn cursor_counts_inactive_slots() {
    let mut c = PanelCursor::new(Counters, CFG);
    let rng = &mut StdRng::seed_from_u64(6);
    let active = (0..6).filter(|_| c.next_slot(rng).1).count();
    assert_eq!((c.made(), active), (6, 2));
}
