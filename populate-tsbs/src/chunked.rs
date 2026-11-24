//! Splitting one sequential TSBS run into chunks that workers generate in
//! parallel.
//!
//! TSBS draws every random number from one global Go generator, in one
//! fixed order, so point `n` depends on everything before it. A chunk is
//! therefore a *resumable* stretch of the run: chunk `k` starts where the
//! simulator's progress (`madePoints`) first reaches `threshold(k)`, and a
//! checkpoint there (a clone of the simulator) is all a worker needs to
//! produce it.
//!
//! The checkpoints come from a *frontier*: one simulator that skips ahead
//! through the chunks in order, without building output (for devops that
//! is the ticks alone), and leaves a clone at each boundary. A worker that
//! wants chunk `k` takes its checkpoint, or advances the frontier to it.
//! The frontier is the only sequential work; with chunks of whole epochs a
//! devops worker never ticks, it only formats.

use crate::config::Config;
use crate::model::{Point, Statics};
use crate::sim::{AnySim, Sim};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Points per chunk by default (about 4 MB of influx text for devops).
pub const DEFAULT_CHUNK_POINTS: u64 = 16 * 1024;

/// A simulator plus Go's interleaved-group counter: everything a run
/// carries from one point to the next.
#[derive(Clone)]
pub struct Cursor<S> {
    /// The simulator.
    pub sim: S,
    /// `currGroupID`.
    group: u64,
}

#[derive(Debug, Clone, Copy)]
enum Layout {
    /// Chunk `k` starts at progress `k * unit`.
    Plain(u64),
    /// `m` whole epochs of `e` progress units per chunk.
    Epochs { e: u64, m: u64 },
    /// Each epoch of `e` units split in `c` chunks.
    Split { e: u64, c: u64 },
}

struct Frontier<S> {
    cur: Cursor<S>,
    /// The chunk `cur` is at the start of.
    next: u64,
}

/// A TSBS run as chunks: see the module docs.
pub struct Chunked<S = AnySim> {
    layout: Layout,
    n: u64,
    max: u64,
    group_id: u64,
    groups: u64,
    frontier: Mutex<Frontier<S>>,
    ready: Mutex<BTreeMap<u64, Cursor<S>>>,
}

impl Chunked<AnySim> {
    /// The run `cfg` describes (validated), in chunks of about
    /// `chunk_points` points (whole epochs when an epoch is smaller).
    pub fn new(cfg: &Config, chunk_points: u64) -> Self {
        Chunked::from_sim(AnySim::new(cfg), cfg, chunk_points)
    }
}

impl<S: Sim> Chunked<S> {
    /// Chunks of `sim`'s run.
    pub fn from_sim(mut sim: S, cfg: &Config, chunk_points: u64) -> Self {
        let target = chunk_points.max(1);
        let e = sim.epoch_progress();
        let layout = if e == 0 {
            Layout::Plain(target)
        } else if e <= target {
            Layout::Epochs { e, m: target / e }
        } else {
            Layout::Split {
                e,
                c: e.div_ceil(target),
            }
        };
        let max = sim.max_points();
        sim.normalize();
        let mut c = Chunked {
            layout,
            n: 1,
            max,
            group_id: cfg.group_id,
            groups: cfg.groups,
            frontier: Mutex::new(Frontier {
                cur: Cursor { sim, group: 0 },
                next: 0,
            }),
            ready: Mutex::new(BTreeMap::new()),
        };
        // the last chunk runs to the end, so it also drains IoT's pending
        // entries
        let mut lo = 1u64;
        let mut hi = 1u64;
        while c.threshold(hi) < max {
            lo = hi;
            hi = hi.saturating_mul(2);
        }
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if c.threshold(mid) >= max {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        c.n = hi.max(1);
        c
    }

    /// Number of chunks.
    pub fn num_chunks(&self) -> u64 {
        self.n
    }

    /// The statics of the run.
    pub fn statics(&self) -> Arc<Statics> {
        self.frontier
            .lock()
            .expect("frontier")
            .cur
            .sim
            .statics()
            .clone()
    }

    /// Progress at which chunk `k` starts.
    fn threshold(&self, k: u64) -> u64 {
        match self.layout {
            Layout::Plain(u) => k.saturating_mul(u),
            Layout::Epochs { e, m } => k.saturating_mul(m).saturating_mul(e),
            Layout::Split { e, c } => (k / c).saturating_mul(e) + (k % c) * e / c,
        }
    }

    /// Progress at which chunk `k` ends (`None`: at the end of the run).
    fn end(&self, k: u64) -> Option<u64> {
        (k + 1 < self.n).then(|| self.threshold(k + 1))
    }

    /// The checkpoint at the start of chunk `k`.
    pub fn cursor(&self, k: u64) -> Cursor<S> {
        if let Some(c) = self.ready.lock().expect("ready").remove(&k) {
            return c;
        }
        let mut f = self.frontier.lock().expect("frontier");
        loop {
            if f.next > k {
                return self
                    .ready
                    .lock()
                    .expect("ready")
                    .remove(&k)
                    .expect("checkpoint left by the frontier");
            }
            let here = f.next;
            let c = f.cur.clone();
            if here + 1 < self.n {
                let end = self.threshold(here + 1);
                self.skip(&mut f.cur, end);
            }
            f.next += 1;
            if here == k {
                return c;
            }
            self.ready.lock().expect("ready").insert(here, c);
        }
    }

    /// Advance `c` to progress `end` without output.
    fn skip(&self, c: &mut Cursor<S>, end: u64) {
        let mut scratch = Point::default();
        while !c.sim.finished() && c.sim.progress() < end {
            if c.sim.skip(&mut scratch) {
                c.group = (c.group + 1) % self.groups;
            }
        }
        c.sim.normalize();
    }

    /// Run chunk `k`, calling `f` with every point of this group, in order.
    pub fn run(&self, k: u64, mut f: impl FnMut(&S, &Point)) {
        let mut c = self.cursor(k);
        let end = self.end(k).unwrap_or(u64::MAX);
        let mut p = Point::default();
        while !c.sim.finished() && c.sim.progress() < end {
            if !c.sim.next(&mut p) {
                continue;
            }
            if c.group == self.group_id {
                f(&c.sim, &p);
            }
            c.group = (c.group + 1) % self.groups;
        }
    }

    /// The run's total `madePoints` (an upper bound on points).
    pub fn max_points(&self) -> u64 {
        self.max
    }
}
