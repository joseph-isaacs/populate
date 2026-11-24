//! The simulator interface every use case implements, and the bits of
//! TSBS's `BaseSimulator` they share.

use crate::config::{Config, UseCase};
use crate::devops::DevopsSim;
use crate::gorand::go_f64_to_u64;
use crate::iot::IotSim;
use crate::model::{Point, Statics};
use std::sync::Arc;

/// A TSBS simulator: `Finished` / `Next` exactly as Go's, plus cheap
/// cloning so a run can be resumed from any point (that is how chunks
/// start in the middle).
pub trait Sim: Clone + Send + Sync {
    /// The tags, measurements and keys the points refer to.
    fn statics(&self) -> &Arc<Statics>;

    /// `Finished()`.
    fn finished(&self) -> bool;

    /// `Next(p)`: fill `p` and say whether to write it.
    fn next(&mut self, p: &mut Point) -> bool;

    /// Points made so far: Go's `madePoints` (of the base simulator, for
    /// IoT). Chunk boundaries are placed by it.
    fn progress(&self) -> u64;

    /// `maxPoints`.
    fn max_points(&self) -> u64;

    /// Do any state change the next [`Sim::next`] would do before making
    /// its point (a pending tick), so clones made here share it.
    fn normalize(&mut self) {}

    /// [`Sim::next`] without the point, where the simulator can skip
    /// building it.
    fn skip(&mut self, scratch: &mut Point) -> bool {
        self.next(scratch)
    }

    /// Progress units per epoch, so chunks can be whole epochs (0 if
    /// they can't be).
    fn epoch_progress(&self) -> u64 {
        0
    }

    /// Keep what a clone taken earlier needs to replay this simulator's
    /// sequential work instead of redoing it (the frontier calls this).
    fn record(&mut self) {}

    /// Move what `frontier` recorded since `self` was cloned from it into
    /// `self`, which replays it.
    fn hand_over(&mut self, _frontier: &mut Self) {}
}

/// `epochHosts` after `epoch` ticks: `init + uint64(missing * epoch /
/// (epochs - 1))`, with Go's float and wrap-around semantics.
pub fn epoch_generators(init: u64, total: u64, epoch: u64, epochs: u64) -> u64 {
    if epoch == 0 {
        return init;
    }
    let missing = total.wrapping_sub(init) as f64;
    init.wrapping_add(go_f64_to_u64(
        missing * epoch as f64 / epochs.wrapping_sub(1) as f64,
    ))
}

/// Any use case's simulator.
#[derive(Clone)]
pub enum AnySim {
    /// devops, cpu-only, cpu-single, devops-generic.
    Devops(DevopsSim),
    /// iot.
    Iot(IotSim),
}

impl AnySim {
    /// Seed Go's generator with `cfg.seed` and build the hosts or trucks,
    /// as `tsbs_generate_data` does. `cfg` must be validated.
    pub fn new(cfg: &Config) -> Self {
        match cfg.use_case {
            UseCase::Iot => AnySim::Iot(IotSim::new(cfg)),
            _ => AnySim::Devops(DevopsSim::new(cfg)),
        }
    }
}

macro_rules! each {
    ($s:expr, $x:ident => $e:expr) => {
        match $s {
            AnySim::Devops($x) => $e,
            AnySim::Iot($x) => $e,
        }
    };
}

impl Sim for AnySim {
    fn statics(&self) -> &Arc<Statics> {
        each!(self, s => s.statics())
    }
    fn finished(&self) -> bool {
        each!(self, s => s.finished())
    }
    #[inline]
    fn next(&mut self, p: &mut Point) -> bool {
        each!(self, s => s.next(p))
    }
    fn progress(&self) -> u64 {
        each!(self, s => s.progress())
    }
    fn max_points(&self) -> u64 {
        each!(self, s => s.max_points())
    }
    fn normalize(&mut self) {
        each!(self, s => s.normalize())
    }
    fn skip(&mut self, scratch: &mut Point) -> bool {
        each!(self, s => s.skip(scratch))
    }
    fn epoch_progress(&self) -> u64 {
        each!(self, s => s.epoch_progress())
    }
    fn record(&mut self) {
        each!(self, s => s.record())
    }
    fn hand_over(&mut self, frontier: &mut Self) {
        match (self, frontier) {
            (AnySim::Devops(s), AnySim::Devops(f)) => s.hand_over(f),
            (AnySim::Iot(s), AnySim::Iot(f)) => s.hand_over(f),
            _ => unreachable!("a checkpoint and its frontier are one use case"),
        }
    }
}
