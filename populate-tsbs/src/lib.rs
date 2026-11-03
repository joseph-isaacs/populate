//! TSBS (Time Series Benchmark Suite) data generation on populate-2.
//!
//! A port of `tsbs_generate_data` for the `cpu-only`, `devops` and `iot` use
//! cases. With a [`GoRand`] seeded like `--seed`, [`influx_lines`] reproduces
//! the reference generator's `--format=influx` output byte for byte; the same
//! simulation can be collected column by column into Arrow `RecordBatch`es.
//!
//! Only the TSBS models (tag tables, measurement definitions, IoT fault
//! injection) and the Influx serializer live here; the building blocks are
//! generic populate-2 modules:
//!
//! - [`populate_2::sampler::go`]: Go `math/rand` RNG ([`GoRand`]) and
//!   algorithms ([`GoSampler`], [`GoRandExt`]);
//! - [`populate_2::process`]: random walks, clamping, … as stateful processes;
//! - [`populate_2::panel`]: entity × time panels (hosts × epochs);
//! - [`populate_2::fixed`] + [`populate_2::arrow::flatten_named`]: wide,
//!   named Arrow output.

pub mod devops;
pub mod influx;
pub mod iot;

#[cfg(test)]
mod tests;

use crate::devops::{
    CPU_FIELDS, CpuHostFactory, DISK_FIELDS, DISKIO_FIELDS, DevopsHostFactory, DevopsSinks,
    HOST_TAGS, KERNEL_FIELDS, MEASUREMENTS, MEM_FLOAT_FIELDS, MEM_INT_FIELDS, NET_FIELDS,
    NGINX_FIELDS, POSTGRESQL_FIELDS, REDIS_FIELDS, write_cpu_line,
};
use crate::iot::{
    DIAGNOSTICS_FIELDS, FLOAT_TAGS, Iot, IotSinks, IotStream, READINGS_FIELDS, STRING_TAGS,
};
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use populate_2::arrow::flatten_named;
use populate_2::panel::{Panel, PanelConfig, panel, written_rows};
use populate_2::traits::{Distribution, HCons, HNil, SampleIter, Sink, VecBackend};
use rand::Rng;
use std::iter::once;

pub use populate_2::sampler::{GoRand, GoRandExt, GoSampler};

/// TSBS use cases implemented here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UseCase {
    CpuOnly,
    Devops,
    Iot,
}

impl UseCase {
    /// Measurements (rows) per entity per epoch.
    pub fn slots(self) -> u64 {
        match self {
            UseCase::CpuOnly => 1,
            UseCase::Devops => 9,
            UseCase::Iot => 2,
        }
    }
}

/// `2016-01-01T00:00:00Z`, TSBS's default `--timestamp-start`.
pub const DEFAULT_START_NS: i64 = 1_451_606_400_000_000_000;
/// `2016-01-02T00:00:00Z`, TSBS's default `--timestamp-end`.
pub const DEFAULT_END_NS: i64 = DEFAULT_START_NS + 86_400_000_000_000;
/// `10s`, TSBS's default `--log-interval`.
pub const DEFAULT_INTERVAL_NS: i64 = 10_000_000_000;

/// The `tsbs_generate_data` flags that shape the data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TsbsConfig {
    pub use_case: UseCase,
    /// `--seed`
    pub seed: i64,
    /// `--scale` (hosts / trucks)
    pub scale: u64,
    /// `--initial-scale` (0 = `scale`)
    pub initial_scale: u64,
    /// `--timestamp-start`, ns since the epoch
    pub start_ns: i64,
    /// `--timestamp-end`, ns since the epoch
    pub end_ns: i64,
    /// `--log-interval`, ns
    pub interval_ns: i64,
    /// `--max-data-points` (0 = no limit)
    pub limit: u64,
}

impl TsbsConfig {
    /// TSBS defaults for a use case: seed 1, 1 entity, one day at 10s.
    pub fn new(use_case: UseCase) -> Self {
        TsbsConfig {
            use_case,
            seed: 1,
            scale: 1,
            initial_scale: 0,
            start_ns: DEFAULT_START_NS,
            end_ns: DEFAULT_END_NS,
            interval_ns: DEFAULT_INTERVAL_NS,
            limit: 0,
        }
    }

    pub fn panel(&self) -> PanelConfig {
        PanelConfig {
            entities: self.scale,
            initial_entities: if self.initial_scale == 0 {
                self.scale
            } else {
                self.initial_scale
            },
            start_ns: self.start_ns,
            interval_ns: self.interval_ns,
            epochs: ((self.end_ns - self.start_ns) / self.interval_ns) as u64,
        }
    }

    /// Simulation slots TSBS runs (`maxPoints`).
    pub fn max_points(&self) -> u64 {
        let all = self.panel().epochs * self.scale * self.use_case.slots();
        if self.limit > 0 && self.limit < all {
            self.limit
        } else {
            all
        }
    }

    /// Points written for `cpu-only` / `devops` (the IoT count depends on
    /// the random faults, see [`iot_points`]).
    pub fn rows(&self) -> u64 {
        written_rows(&self.panel(), self.use_case.slots(), self.limit)
    }

    /// The RNG `tsbs_generate_data --seed` uses.
    pub fn rng(&self) -> GoRand {
        GoRand::new(self.seed)
    }
}

/// `cpu-only` as a populate-2 distribution of [`CpuRow`](devops::CpuRow)s.
pub fn cpu_only(config: &TsbsConfig) -> Panel<CpuHostFactory> {
    panel(CpuHostFactory, config.panel())
}

/// `devops` as a populate-2 distribution of [`DevopsPoint`](devops::DevopsPoint)s.
pub fn devops(config: &TsbsConfig) -> Panel<DevopsHostFactory> {
    panel(DevopsHostFactory, config.panel())
}

/// `iot` as a populate-2 distribution of [`IotPoint`](iot::IotPoint)s.
pub fn iot(config: &TsbsConfig) -> Iot {
    Iot {
        config: config.panel(),
        max_points: config.max_points(),
    }
}

/// Every IoT point TSBS writes for this config, in order.
pub fn iot_points(config: &TsbsConfig, rng: &mut impl Rng) -> Vec<iot::IotPoint> {
    let mut stream = IotStream::new(config.panel(), config.max_points());
    let mut out = Vec::new();
    while !stream.finished() {
        if let Some(p) = stream.next_point(rng) {
            out.push(p);
        }
    }
    out
}

/// The run as TSBS `--format=influx` output.
pub fn influx_lines(config: &TsbsConfig, rng: &mut impl Rng) -> String {
    let mut out = String::new();
    let n = config.rows() as usize;
    match config.use_case {
        UseCase::CpuOnly => {
            let mut it = <Panel<_> as Distribution<VecBackend>>::row_iter(&mut cpu_only(config));
            for _ in 0..n {
                write_cpu_line(&mut out, &it.next(rng));
            }
        }
        UseCase::Devops => {
            let mut it = <Panel<_> as Distribution<VecBackend>>::row_iter(&mut devops(config));
            for _ in 0..n {
                it.next(rng).write_line(&mut out);
            }
        }
        UseCase::Iot => {
            for p in iot_points(config, rng) {
                p.write_line(&mut out);
            }
        }
    }
    out
}

/// Column names: `timestamp`, the tags, then the fields.
fn columns<'a>(tags: &[&'a str], fields: &[&[&'a str]]) -> Vec<&'a str> {
    once("timestamp")
        .chain(tags.iter().copied())
        .chain(fields.iter().flat_map(|f| f.iter().copied()))
        .collect()
}

/// `cpu-only` as one Arrow batch (columns `timestamp`, tags, fields).
pub fn cpu_only_record_batch(
    config: &TsbsConfig,
    rng: &mut impl Rng,
) -> Result<RecordBatch, ArrowError> {
    let n = config.rows() as usize;
    let sink = <Panel<_> as Distribution<VecBackend>>::collect_into(
        &mut cpu_only(config),
        Default::default(),
        rng,
        n,
    );
    flatten_named(sink, &columns(&HOST_TAGS, &[&CPU_FIELDS]), Some(0))
}

/// `devops` as one Arrow batch per measurement, named as in [`MEASUREMENTS`].
pub fn devops_record_batches(
    config: &TsbsConfig,
    rng: &mut impl Rng,
) -> Result<Vec<(&'static str, RecordBatch)>, ArrowError> {
    let n = config.rows() as usize;
    let HCons(s, HNil) = <Panel<_> as Distribution<VecBackend>>::collect_into(
        &mut devops(config),
        HCons(DevopsSinks::<VecBackend>::default(), HNil),
        rng,
        n,
    );
    let h = &HOST_TAGS[..];
    let plus =
        |extra: &[&'static str]| -> Vec<&'static str> { h.iter().chain(extra).copied().collect() };
    let batches = vec![
        flatten_named(s.cpu, &columns(h, &[&CPU_FIELDS]), Some(0))?,
        flatten_named(
            s.diskio,
            &columns(&plus(&["serial"]), &[&DISKIO_FIELDS]),
            Some(0),
        )?,
        flatten_named(
            s.disk,
            &columns(&plus(&["path", "fstype"]), &[&DISK_FIELDS]),
            Some(0),
        )?,
        flatten_named(s.kernel, &columns(h, &[&KERNEL_FIELDS]), Some(0))?,
        flatten_named(
            s.mem,
            &columns(h, &[&MEM_INT_FIELDS, &MEM_FLOAT_FIELDS]),
            Some(0),
        )?,
        flatten_named(
            s.net,
            &columns(&plus(&["interface"]), &[&NET_FIELDS]),
            Some(0),
        )?,
        flatten_named(
            s.nginx,
            &columns(&plus(&["port", "server"]), &[&NGINX_FIELDS]),
            Some(0),
        )?,
        flatten_named(s.postgresql, &columns(h, &[&POSTGRESQL_FIELDS]), Some(0))?,
        flatten_named(
            s.redis,
            &columns(&plus(&["port", "server"]), &[&REDIS_FIELDS]),
            Some(0),
        )?,
    ];
    Ok(MEASUREMENTS.into_iter().zip(batches).collect())
}

/// `iot` as `(readings, diagnostics)` Arrow batches. Cleared values are nulls.
pub fn iot_record_batches(
    config: &TsbsConfig,
    rng: &mut impl Rng,
) -> Result<(RecordBatch, RecordBatch), ArrowError> {
    let mut sinks = IotSinks::<VecBackend>::default();
    for p in iot_points(config, rng) {
        sinks.push(p);
    }
    let tags: Vec<&str> = STRING_TAGS.iter().chain(&FLOAT_TAGS).copied().collect();
    Ok((
        flatten_named(
            sinks.readings,
            &columns(&tags, &[&READINGS_FIELDS]),
            Some(0),
        )?,
        flatten_named(
            sinks.diagnostics,
            &columns(&tags, &[&DIAGNOSTICS_FIELDS]),
            Some(0),
        )?,
    ))
}
