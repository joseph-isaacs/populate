//! The devops use cases (`devops`, `cpu-only`, `cpu-single`,
//! `devops-generic`): TSBS's hosts and their simulated measurements.
//!
//! Every host's distributions live in one flat `f64` array, advanced in
//! Go's order (host by host, measurement by measurement, field by field)
//! by one `NormFloat64` each per tick. The array sits behind an `Arc`, so a
//! clone of the simulator in the middle of an epoch shares it, and a tick
//! copies it once.

use crate::config::{Config, UseCase};
use crate::gorand::{GoRand, Zipf, go_f64_to_i64};
use crate::model::{Measurement, Point, Statics, TagVal, Type, Val};
use crate::sim::{Sim, epoch_generators};
use std::sync::Arc;

/// How a distribution advances: `step = NormFloat64() * std + mean`, then
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    /// `State += step`, clamped to `[min, max]` (`CWD`).
    Clamped,
    /// `State += |step|` (`MWD`).
    Monotonic,
    /// `State += step` (`WD`).
    Walk,
    /// [`Kind::Clamped`] with the host's memory size: `std = total / 64`,
    /// `max = total`.
    Mem,
}

#[derive(Debug, Clone, Copy)]
struct Spec {
    kind: Kind,
    mean: f64,
    std: f64,
    min: f64,
    max: f64,
}

const fn cwd(mean: f64, std: f64, min: f64, max: f64) -> Spec {
    Spec {
        kind: Kind::Clamped,
        mean,
        std,
        min,
        max,
    }
}

const fn mwd(mean: f64) -> Spec {
    Spec {
        kind: Kind::Monotonic,
        mean,
        std: 1.0,
        min: 0.0,
        max: 0.0,
    }
}

const fn wd(mean: f64) -> Spec {
    Spec {
        kind: Kind::Walk,
        mean,
        std: 1.0,
        min: 0.0,
        max: 0.0,
    }
}

const MEM: Spec = Spec {
    kind: Kind::Mem,
    mean: 0.0,
    std: 0.0,
    min: 0.0,
    max: 0.0,
};

const ONE_TB: i64 = 1 << 40;
const SIXTEEN_GB: f64 = (16u64 * 1024 * 1024 * 1024) as f64;

/// The nine devops measurements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum M {
    Cpu,
    DiskIo,
    Disk,
    Kernel,
    Mem,
    Net,
    Nginx,
    Postgresql,
    Redis,
    Generic,
}

const CPU_FIELDS: [&str; 10] = [
    "usage_user",
    "usage_system",
    "usage_idle",
    "usage_nice",
    "usage_iowait",
    "usage_irq",
    "usage_softirq",
    "usage_steal",
    "usage_guest",
    "usage_guest_nice",
];
const DISKIO_FIELDS: [&str; 7] = [
    "reads",
    "writes",
    "read_bytes",
    "write_bytes",
    "read_time",
    "write_time",
    "io_time",
];
const DISK_FIELDS: [&str; 7] = [
    "total",
    "free",
    "used",
    "used_percent",
    "inodes_total",
    "inodes_free",
    "inodes_used",
];
const KERNEL_FIELDS: [&str; 6] = [
    "boot_time",
    "interrupts",
    "context_switches",
    "processes_forked",
    "disk_pages_in",
    "disk_pages_out",
];
const MEM_FIELDS: [&str; 9] = [
    "total",
    "available",
    "used",
    "free",
    "cached",
    "buffered",
    "used_percent",
    "available_percent",
    "buffered_percent",
];
const NET_FIELDS: [&str; 8] = [
    "bytes_sent",
    "bytes_recv",
    "packets_sent",
    "packets_recv",
    "err_in",
    "err_out",
    "drop_in",
    "drop_out",
];
const NGINX_FIELDS: [&str; 7] = [
    "accepts", "active", "handled", "reading", "requests", "waiting", "writing",
];
const PG_FIELDS: [&str; 16] = [
    "numbackends",
    "xact_commit",
    "xact_rollback",
    "blks_read",
    "blks_hit",
    "tup_returned",
    "tup_fetched",
    "tup_inserted",
    "tup_updated",
    "tup_deleted",
    "conflicts",
    "temp_files",
    "temp_bytes",
    "deadlocks",
    "blk_read_time",
    "blk_write_time",
];
const REDIS_FIELDS: [&str; 31] = [
    "uptime_in_seconds",
    "total_connections_received",
    "expired_keys",
    "evicted_keys",
    "keyspace_hits",
    "keyspace_misses",
    "instantaneous_ops_per_sec",
    "instantaneous_input_kbps",
    "instantaneous_output_kbps",
    "connected_clients",
    "used_memory",
    "used_memory_rss",
    "used_memory_peak",
    "used_memory_lua",
    "rdb_changes_since_last_save",
    "sync_full",
    "sync_partial_ok",
    "sync_partial_err",
    "pubsub_channels",
    "pubsub_patterns",
    "latest_fork_usec",
    "connected_slaves",
    "master_repl_offset",
    "repl_backlog_active",
    "repl_backlog_size",
    "repl_backlog_histlen",
    "mem_fragmentation_ratio",
    "used_cpu_sys",
    "used_cpu_user",
    "used_cpu_sys_children",
    "used_cpu_user_children",
];

/// Distributions of each measurement, in tick order.
fn specs(m: M) -> Vec<Spec> {
    let c5 = cwd(5.0, 1.0, 0.0, 1000.0);
    match m {
        M::Cpu => vec![cwd(0.0, 1.0, 0.0, 100.0); 10],
        M::DiskIo => vec![
            mwd(50.0),
            mwd(50.0),
            mwd(100.0),
            mwd(100.0),
            mwd(5.0),
            mwd(5.0),
            mwd(5.0),
        ],
        M::Disk => vec![cwd(50.0, 1.0, 0.0, ONE_TB as f64)],
        M::Kernel => vec![mwd(5.0); 5],
        M::Mem => vec![MEM; 3],
        M::Net => vec![
            mwd(50.0),
            mwd(50.0),
            mwd(50.0),
            mwd(50.0),
            mwd(5.0),
            mwd(5.0),
            mwd(5.0),
            mwd(5.0),
        ],
        M::Nginx => {
            let c = cwd(5.0, 1.0, 0.0, 100.0);
            vec![mwd(5.0), c, mwd(5.0), c, mwd(5.0), c, c]
        }
        M::Postgresql => {
            let mut v = vec![c5; 16];
            v[12] = cwd(1024.0, 1.0, 0.0, (1u64 << 30) as f64);
            v
        }
        M::Redis => {
            let mut v = vec![mwd(5.0)];
            v.extend([mwd(50.0); 4]);
            v.extend([wd(1.0); 3]);
            v.push(cwd(50.0, 1.0, 0.0, 10000.0));
            v.extend([cwd(50.0, 1.0, 0.0, SIXTEEN_GB); 4]);
            v.push(cwd(50.0, 1.0, 0.0, 10000.0));
            v.extend([c5; 11]);
            v.push(cwd(5.0, 1.0, 0.0, 100.0));
            v.extend([c5; 4]);
            v
        }
        M::Generic => vec![],
    }
}

fn measurement(m: M, n_generic: usize) -> Measurement {
    let (name, extra, fields): (&str, &[&str], Vec<String>) = {
        let f = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        match m {
            M::Cpu => ("cpu", &[], f(&CPU_FIELDS)),
            M::DiskIo => ("diskio", &["serial"], f(&DISKIO_FIELDS)),
            M::Disk => ("disk", &["path", "fstype"], f(&DISK_FIELDS)),
            M::Kernel => ("kernel", &[], f(&KERNEL_FIELDS)),
            M::Mem => ("mem", &[], f(&MEM_FIELDS)),
            M::Net => ("net", &["interface"], f(&NET_FIELDS)),
            M::Nginx => ("nginx", &["port", "server"], f(&NGINX_FIELDS)),
            M::Postgresql => ("postgresl", &[], f(&PG_FIELDS)),
            M::Redis => ("redis", &["port", "server"], f(&REDIS_FIELDS)),
            M::Generic => (
                "generic_metrics",
                &[],
                (0..n_generic).map(|i| format!("metric_{i}")).collect(),
            ),
        }
    };
    let mut types = vec![Type::I64; fields.len()];
    if m == M::Mem {
        types[6..].fill(Type::F64);
    }
    Measurement {
        name: name.into(),
        extra_tags: extra.iter().map(|&s| s.into()).collect(),
        fields: fields.into_iter().map(Into::into).collect(),
        types,
    }
}

struct Region {
    name: &'static str,
    dcs: &'static [&'static str],
}

const REGIONS: [Region; 9] = [
    Region {
        name: "us-east-1",
        dcs: &["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1e"],
    },
    Region {
        name: "us-west-1",
        dcs: &["us-west-1a", "us-west-1b"],
    },
    Region {
        name: "us-west-2",
        dcs: &["us-west-2a", "us-west-2b", "us-west-2c"],
    },
    Region {
        name: "eu-west-1",
        dcs: &["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    },
    Region {
        name: "eu-central-1",
        dcs: &["eu-central-1a", "eu-central-1b"],
    },
    Region {
        name: "ap-southeast-1",
        dcs: &["ap-southeast-1a", "ap-southeast-1b"],
    },
    Region {
        name: "ap-southeast-2",
        dcs: &["ap-southeast-2a", "ap-southeast-2b"],
    },
    Region {
        name: "ap-northeast-1",
        dcs: &["ap-northeast-1a", "ap-northeast-1c"],
    },
    Region {
        name: "sa-east-1",
        dcs: &["sa-east-1a", "sa-east-1b", "sa-east-1c"],
    },
];
const TEAMS: [&str; 4] = ["SF", "NYC", "LON", "CHI"];
const OSES: [&str; 3] = ["Ubuntu16.10", "Ubuntu16.04LTS", "Ubuntu15.10"];
const ARCHES: [&str; 2] = ["x64", "x86"];
const ENVS: [&str; 3] = ["production", "staging", "test"];
const FS_TYPES: [&str; 3] = ["ext3", "ext4", "btrfs"];
const MEM_TOTALS: [i64; 3] = [8 << 30, 12 << 30, 16 << 30];

/// Tag keys of every devops host.
pub const HOST_TAG_KEYS: [&str; 10] = [
    "hostname",
    "region",
    "datacenter",
    "rack",
    "os",
    "arch",
    "team",
    "service",
    "service_version",
    "service_environment",
];

/// What doesn't change: hosts, their constants, and the layout of the
/// state array.
#[derive(Debug)]
struct Static {
    statics: Arc<Statics>,
    ms: Vec<M>,
    /// Distributions of one host (all hosts but devops-generic's).
    specs: Vec<Spec>,
    /// Start of each host's distributions in the state array (`H + 1`).
    offsets: Vec<usize>,
    /// Start of each measurement's distributions within a host.
    meas_off: Vec<usize>,
    /// Per host: memory size, kernel boot time.
    mem_total: Vec<i64>,
    boot_time: Vec<i64>,
    /// devops-generic: epochs to live (0 = forever) and the epoch each
    /// host is first seen in.
    ttl: Vec<u64>,
    start_epoch: Vec<u64>,
    generic: bool,
    hosts: u64,
    /// `UnixNano` of the start: wrapping, like Go's.
    start: i64,
    interval: i64,
}

/// A devops simulator (any of the four devops use cases).
#[derive(Clone)]
pub struct DevopsSim {
    st: Arc<Static>,
    rng: GoRand,
    state: Arc<Vec<f64>>,
    made: u64,
    max: u64,
    host: u64,
    meas: usize,
    epoch: u64,
    epochs: u64,
    epoch_hosts: u64,
    init_hosts: u64,
}

/// Per-host values built with the measurements.
struct HostInit {
    tags: Vec<TagVal>,
    extra: Vec<Vec<Box<str>>>,
    state: Vec<f64>,
    mem_total: i64,
    boot_time: i64,
}

fn new_host(rng: &mut GoRand, id: u64, ms: &[M], cpu_fields: usize, n_generic: u64) -> HostInit {
    let mut state = Vec::new();
    let mut extra = Vec::with_capacity(ms.len());
    let (mut mem_total, mut boot_time) = (0, 0);
    // the measurements first (`gen(ctx)`), then the host's own tags
    for &m in ms {
        let mut e: Vec<Box<str>> = Vec::new();
        match m {
            M::Cpu => {
                for _ in 0..cpu_fields {
                    state.push(rng.float64() * 100.0);
                }
            }
            M::DiskIo => {
                state.extend([0.0; 7]);
                let (a, b, c) = (rng.intn(1000), rng.intn(1000), rng.intn(1000));
                e.push(format!("{a:03}-{b:03}-{c:03}").into());
            }
            M::Disk => {
                e.push(format!("/dev/sda{}", rng.intn(10)).into());
                e.push(FS_TYPES[rng.intn(3)].into());
                state.push((ONE_TB / 2) as f64);
            }
            M::Kernel => {
                state.extend([0.0; 5]);
                boot_time = rng.int63n(240);
            }
            M::Mem => {
                mem_total = MEM_TOTALS[rng.intn(3)];
                for _ in 0..3 {
                    state.push(rng.float64() * mem_total as f64);
                }
            }
            M::Net => {
                state.extend([0.0; 8]);
                e.push(format!("eth{}", rng.intn(4)).into());
            }
            M::Nginx => {
                state.extend([0.0; 7]);
                let server = format!("nginx_{}", rng.intn(100_000));
                let port = (rng.int63n(20_000) + 1024).to_string();
                e.push(port.into());
                e.push(server.into());
            }
            M::Postgresql => state.extend([0.0; 16]),
            M::Redis => {
                state.extend([0.0; 30]);
                let at = state.len() - 30 + 9;
                state[at..at + 4].fill(SIXTEEN_GB / 2.0);
                let server = format!("redis_{}", rng.intn(100_000));
                let port = (rng.int63n(20_000) + 1024).to_string();
                e.push(port.into());
                e.push(server.into());
            }
            M::Generic => {
                for _ in 0..n_generic {
                    state.push(rng.float64() * 1000.0);
                }
            }
        }
        extra.push(e);
    }
    let region = &REGIONS[rng.intn(REGIONS.len())];
    let name = format!("host_{id}");
    let dc = region.dcs[rng.intn(region.dcs.len())];
    let rack = rng.int63n(100).to_string();
    let arch = ARCHES[rng.intn(2)];
    let os = OSES[rng.intn(3)];
    let service = rng.int63n(20).to_string();
    let service_version = rng.int63n(2).to_string();
    let env = ENVS[rng.intn(3)];
    let team = TEAMS[rng.intn(4)];
    let s = |v: &str| TagVal::Str(v.into());
    let tags = vec![
        s(&name),
        s(region.name),
        s(dc),
        s(&rack),
        s(os),
        s(arch),
        s(team),
        s(&service),
        s(&service_version),
        s(env),
    ];
    HostInit {
        tags,
        extra,
        state,
        mem_total,
        boot_time,
    }
}

/// `genZipfArray`: a fresh `rand.New(rand.NewSource(1234))` each call.
fn zipf_array(n: u64, max: u64) -> Vec<u64> {
    let mut r = GoRand::new(1234);
    let z = Zipf::new(1.01, 1.0, max).expect("valid zipf");
    (0..n)
        .map(|_| {
            let v = z.uint64(&mut r);
            if v == max { v } else { v + 1 }
        })
        .collect()
}

impl DevopsSim {
    /// `rand.Seed(cfg.seed)` and the hosts. `cfg` must be validated.
    pub fn new(cfg: &Config) -> Self {
        let generic = cfg.use_case == UseCase::DevopsGeneric;
        let ms: Vec<M> = match cfg.use_case {
            UseCase::Devops => vec![
                M::Cpu,
                M::DiskIo,
                M::Disk,
                M::Kernel,
                M::Mem,
                M::Net,
                M::Nginx,
                M::Postgresql,
                M::Redis,
            ],
            UseCase::CpuOnly | UseCase::CpuSingle => vec![M::Cpu],
            UseCase::DevopsGeneric => vec![M::Generic],
            UseCase::Iot => unreachable!("iot is not a devops use case"),
        };
        let cpu_fields = if cfg.use_case == UseCase::CpuSingle {
            1
        } else {
            10
        };
        let hosts = cfg.scale;
        let epochs = cfg.epochs();
        let mut rng = GoRand::new(cfg.seed);
        let (metric_counts, ttl) = if generic {
            let mut counts = zipf_array(hosts - 1, cfg.max_metric_count);
            counts.push(cfg.max_metric_count);
            let mut ttl = vec![0u64; (hosts - hosts / 2) as usize];
            let mut short = zipf_array(hosts / 2, epochs);
            short.sort_unstable_by(|a, b| b.cmp(a));
            ttl.extend(short);
            (counts, ttl)
        } else {
            (Vec::new(), Vec::new())
        };
        let mut all_specs = Vec::new();
        let mut meas_off = Vec::new();
        for &m in &ms {
            meas_off.push(all_specs.len());
            let mut s = specs(m);
            if m == M::Cpu {
                s.truncate(cpu_fields);
            }
            all_specs.extend(s);
        }
        meas_off.push(all_specs.len());
        let mut state = Vec::new();
        let mut offsets = vec![0];
        let mut gen_tags = Vec::with_capacity(hosts as usize);
        let mut extra_tags = Vec::with_capacity(hosts as usize);
        let mut mem_total = Vec::with_capacity(hosts as usize);
        let mut boot_time = Vec::with_capacity(hosts as usize);
        for id in 0..hosts {
            let n = metric_counts.get(id as usize).copied().unwrap_or(0);
            let h = new_host(&mut rng, id, &ms, cpu_fields, n);
            state.extend(h.state);
            offsets.push(state.len());
            gen_tags.push(h.tags);
            extra_tags.push(h.extra);
            mem_total.push(h.mem_total);
            boot_time.push(h.boot_time);
        }
        let max_generic = metric_counts.iter().copied().max().unwrap_or(0) as usize;
        let mut measurements: Vec<Measurement> =
            ms.iter().map(|&m| measurement(m, max_generic)).collect();
        if ms == [M::Cpu] {
            measurements[0].fields.truncate(cpu_fields);
            measurements[0].types.truncate(cpu_fields);
        }
        let header_fields = measurements.iter().map(|m| m.fields.clone()).collect();
        let statics = Statics {
            start: cfg.start,
            tag_keys: HOST_TAG_KEYS.iter().map(|&s| s.into()).collect(),
            tag_types: vec![Type::Str; HOST_TAG_KEYS.len()],
            measurements,
            gen_tags,
            extra_tags,
            header_fields,
        };
        let points_per_epoch = if generic { 1 } else { ms.len() as u64 };
        let mut max = epochs.wrapping_mul(hosts).wrapping_mul(points_per_epoch);
        if cfg.limit > 0 && cfg.limit < max {
            max = cfg.limit;
        }
        let init_hosts = cfg.initial_scale;
        let mut start_epoch = Vec::new();
        if generic {
            // a host's StartEpoch is the first epoch it is visited in,
            // and epochHosts only grows
            start_epoch = vec![u64::MAX; hosts as usize];
            let last_epoch = max.div_ceil(hosts.max(1));
            let eh = |e: u64| epoch_generators(init_hosts, hosts, e, epochs).min(hosts);
            let (mut seen, mut e) = (0u64, 0u64);
            // epochHosts grows with the epoch (when initial scale <= scale
            // and there are two or more epochs), so the epoch each host
            // first appears in is a binary search away; otherwise walk.
            let monotone = init_hosts <= hosts && epochs >= 2;
            while seen < hosts && e < last_epoch {
                if monotone && eh(e) <= seen {
                    // first e' in (e, last_epoch) with eh(e') > seen
                    let (mut lo, mut hi) = (e + 1, last_epoch);
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if eh(mid) > seen {
                            hi = mid
                        } else {
                            lo = mid + 1
                        }
                    }
                    e = lo;
                    if e == last_epoch {
                        break;
                    }
                }
                let n = eh(e);
                while seen < n {
                    start_epoch[seen as usize] = e;
                    seen += 1;
                }
                e += 1;
            }
        }
        let st = Static {
            statics: Arc::new(statics),
            ms,
            specs: all_specs,
            offsets,
            meas_off,
            mem_total,
            boot_time,
            ttl,
            start_epoch,
            generic,
            hosts,
            start: cfg.start as i64,
            interval: cfg.interval,
        };
        DevopsSim {
            st: Arc::new(st),
            rng,
            state: Arc::new(state),
            made: 0,
            max,
            host: 0,
            meas: 0,
            epoch: 0,
            epochs,
            epoch_hosts: init_hosts,
            init_hosts,
        }
    }

    /// `TickAll` on every host, then `adjustNumHostsForEpoch`.
    fn tick(&mut self) {
        let st = &*self.st;
        let rng = &mut self.rng;
        let state = Arc::make_mut(&mut self.state);
        if st.generic {
            for v in state.iter_mut() {
                let step = rng.norm_float64() * 1.0 + 0.0;
                *v = clamp(*v + step, 0.0, 1000.0);
            }
        } else {
            let d = st.specs.len();
            for (h, vals) in state.chunks_exact_mut(d).enumerate() {
                let total = st.mem_total[h] as f64;
                for (v, s) in vals.iter_mut().zip(&st.specs) {
                    *v = match s.kind {
                        Kind::Clamped => {
                            clamp(*v + (rng.norm_float64() * s.std + s.mean), s.min, s.max)
                        }
                        Kind::Monotonic => *v + (rng.norm_float64() * s.std + s.mean).abs(),
                        Kind::Walk => *v + (rng.norm_float64() * s.std + s.mean),
                        Kind::Mem => {
                            clamp(*v + (rng.norm_float64() * (total / 64.0) + 0.0), 0.0, total)
                        }
                    };
                }
            }
        }
        self.epoch += 1;
        self.epoch_hosts =
            epoch_generators(self.init_hosts, self.st.hosts, self.epoch, self.epochs);
    }

    /// `ToPoint` of measurement `mi` of the current host.
    fn populate(&self, mi: usize, p: &mut Point) {
        let st = &*self.st;
        let h = self.host as usize;
        p.meas = mi as u32;
        p.source = h as u32;
        p.ts = st
            .start
            .wrapping_add((self.epoch as i64).wrapping_mul(st.interval));
        p.fields.clear();
        let vals = &self.state[st.offsets[h]..st.offsets[h + 1]];
        let vals = if st.generic {
            vals
        } else {
            &vals[st.meas_off[mi]..st.meas_off[mi + 1]]
        };
        let int = |v: f64| Val::I(go_f64_to_i64(v));
        match st.ms[mi] {
            M::Disk => {
                let free = go_f64_to_i64(vals[0]);
                let total = ONE_TB;
                let used = total - free;
                let used_percent = go_f64_to_i64(100.0 * (used as f64 / total as f64));
                let inodes_total = total / 4096;
                let inodes_free = free / 4096;
                p.fields.extend(
                    [
                        total,
                        free,
                        used,
                        used_percent,
                        inodes_total,
                        inodes_free,
                        inodes_total - inodes_free,
                    ]
                    .map(Val::I),
                );
            }
            M::Mem => {
                let total = st.mem_total[h];
                let used = go_f64_to_i64(vals[0]);
                let cached = go_f64_to_i64(vals[1]);
                let buffered = go_f64_to_i64(vals[2]);
                let available = total - used;
                p.fields
                    .extend([total, available, used, available, cached, buffered].map(Val::I));
                p.fields.push(Val::F(100.0 * (used as f64 / total as f64)));
                p.fields
                    .push(Val::F(100.0 * (available as f64 / total as f64)));
                p.fields
                    .push(Val::F(100.0 * (buffered as f64) / total as f64));
            }
            M::Kernel => {
                p.fields.push(Val::I(st.boot_time[h]));
                p.fields.extend(vals.iter().map(|&v| int(v)));
            }
            M::Redis => {
                // time.Duration.Seconds() of the uptime, truncated
                let d = self.epoch as i64 * st.interval;
                let secs = (d / 1_000_000_000) as f64 + (d % 1_000_000_000) as f64 / 1e9;
                p.fields.push(Val::I(go_f64_to_i64(secs)));
                p.fields.extend(vals.iter().map(|&v| int(v)));
            }
            _ => p.fields.extend(vals.iter().map(|&v| int(v))),
        }
    }

    /// The point-making half of `populatePoint`.
    #[inline]
    fn finish_point(&mut self) -> bool {
        let ret = self.host < self.epoch_hosts;
        self.made += 1;
        self.host += 1;
        ret
    }

    /// The wrap-around at the start of `Next`: a new measurement, or a new
    /// epoch (tick).
    fn wrap(&mut self) {
        if self.st.generic || self.st.ms.len() == 1 {
            if self.host >= self.st.hosts {
                self.host = 0;
                self.tick();
            }
            return;
        }
        if self.host == self.st.hosts {
            self.host = 0;
            self.meas += 1;
        }
        if self.meas == self.st.ms.len() {
            self.meas = 0;
            self.tick();
        }
    }

    /// devops-generic's `Next` after the wrap: is the current host alive?
    fn generic_alive(&self) -> bool {
        if self.host >= self.epoch_hosts {
            return false;
        }
        let h = self.host as usize;
        let ttl = self.st.ttl[h];
        ttl == 0 || self.st.start_epoch[h].wrapping_add(ttl) > self.epoch
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

impl Sim for DevopsSim {
    fn statics(&self) -> &Arc<Statics> {
        &self.st.statics
    }

    fn finished(&self) -> bool {
        self.made >= self.max
    }

    #[inline]
    fn next(&mut self, p: &mut Point) -> bool {
        self.wrap();
        if self.st.generic {
            if self.generic_alive() {
                self.populate(0, p);
                return self.finish_point();
            }
            self.host += 1;
            self.made += 1;
            return false;
        }
        self.populate(self.meas, p);
        self.finish_point()
    }

    fn skip(&mut self, _scratch: &mut Point) -> bool {
        self.wrap();
        if self.st.generic {
            if self.generic_alive() {
                return self.finish_point();
            }
            self.host += 1;
            self.made += 1;
            return false;
        }
        self.finish_point()
    }

    fn progress(&self) -> u64 {
        self.made
    }

    fn max_points(&self) -> u64 {
        self.max
    }

    fn normalize(&mut self) {
        if !self.finished() {
            self.wrap();
        }
    }

    fn epoch_progress(&self) -> u64 {
        let per_host = if self.st.generic {
            1
        } else {
            self.st.ms.len() as u64
        };
        per_host * self.st.hosts
    }
}
