//! TSBS `devops` and `cpu-only` use cases: hosts with 10 tags and up to 9
//! measurements (cpu, diskio, disk, kernel, mem, net, nginx, postgresql,
//! redis), ported from `pkg/data/usecases/devops`.
//!
//! RNG call order matches TSBS exactly: per host, every measurement is built
//! (in the order above, with its own draws) *before* the host tags are drawn.

use crate::influx::{FieldValue, LineWriter};
use populate_2::fixed::Fixed;
use populate_2::panel::{Entity, EntityFactory};
use populate_2::process::{
    Normal, Process, Walk, clamped_walk, monotonic_walk, normal, random_walk,
};
use populate_2::sampler::{GoRandExt, GoSampler};
use populate_2::traits::{Backend, DefaultSink, HCons, HNil, Sink};
use rand::Rng;
use std::array::from_fn;

// ============================================================================
// Tag tables (devops/host.go)
// ============================================================================

pub const REGIONS: [(&str, &[&str]); 9] = [
    (
        "us-east-1",
        &["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1e"],
    ),
    ("us-west-1", &["us-west-1a", "us-west-1b"]),
    ("us-west-2", &["us-west-2a", "us-west-2b", "us-west-2c"]),
    ("eu-west-1", &["eu-west-1a", "eu-west-1b", "eu-west-1c"]),
    ("eu-central-1", &["eu-central-1a", "eu-central-1b"]),
    ("ap-southeast-1", &["ap-southeast-1a", "ap-southeast-1b"]),
    ("ap-southeast-2", &["ap-southeast-2a", "ap-southeast-2b"]),
    ("ap-northeast-1", &["ap-northeast-1a", "ap-northeast-1c"]),
    ("sa-east-1", &["sa-east-1a", "sa-east-1b", "sa-east-1c"]),
];
pub const TEAMS: [&str; 4] = ["SF", "NYC", "LON", "CHI"];
pub const OSES: [&str; 3] = ["Ubuntu16.10", "Ubuntu16.04LTS", "Ubuntu15.10"];
pub const ARCHES: [&str; 2] = ["x64", "x86"];
pub const ENVIRONMENTS: [&str; 3] = ["production", "staging", "test"];
pub const RACKS: i64 = 100;
pub const SERVICES: i64 = 20;
pub const SERVICE_VERSIONS: i64 = 2;

pub const HOST_TAGS: [&str; 10] = [
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

// ============================================================================
// Field names per measurement
// ============================================================================

pub const CPU_FIELDS: [&str; 10] = [
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
pub const DISKIO_FIELDS: [&str; 7] = [
    "reads",
    "writes",
    "read_bytes",
    "write_bytes",
    "read_time",
    "write_time",
    "io_time",
];
pub const DISK_FIELDS: [&str; 7] = [
    "total",
    "free",
    "used",
    "used_percent",
    "inodes_total",
    "inodes_free",
    "inodes_used",
];
pub const KERNEL_FIELDS: [&str; 6] = [
    "boot_time",
    "interrupts",
    "context_switches",
    "processes_forked",
    "disk_pages_in",
    "disk_pages_out",
];
pub const MEM_INT_FIELDS: [&str; 6] = ["total", "available", "used", "free", "cached", "buffered"];
pub const MEM_FLOAT_FIELDS: [&str; 3] = ["used_percent", "available_percent", "buffered_percent"];
pub const NET_FIELDS: [&str; 8] = [
    "bytes_sent",
    "bytes_recv",
    "packets_sent",
    "packets_recv",
    "err_in",
    "err_out",
    "drop_in",
    "drop_out",
];
pub const NGINX_FIELDS: [&str; 7] = [
    "accepts", "active", "handled", "reading", "requests", "waiting", "writing",
];
pub const POSTGRESQL_FIELDS: [&str; 16] = [
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
pub const REDIS_FIELDS: [&str; 31] = [
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

/// The 9 devops measurements, in emission order. Note TSBS's `postgresl`
/// spelling of the measurement name, kept for byte compatibility.
pub const MEASUREMENTS: [&str; 9] = [
    "cpu",
    "diskio",
    "disk",
    "kernel",
    "mem",
    "net",
    "nginx",
    "postgresl",
    "redis",
];

// ============================================================================
// Rows
// ============================================================================

pub type HostTags = Fixed<String, 10>;
/// `(timestamp_ns, host tags, fields)`
pub type CpuRow = (i64, HostTags, Fixed<i64, 10>);
/// `(timestamp_ns, host tags + serial, fields)`
pub type DiskIoRow = (i64, Fixed<String, 11>, Fixed<i64, 7>);
/// `(timestamp_ns, host tags + path + fstype, fields)`
pub type DiskRow = (i64, Fixed<String, 12>, Fixed<i64, 7>);
pub type KernelRow = (i64, HostTags, Fixed<i64, 6>);
/// `(timestamp_ns, host tags, int fields, float fields)`
pub type MemRow = (i64, HostTags, Fixed<i64, 6>, Fixed<f64, 3>);
/// `(timestamp_ns, host tags + interface, fields)`
pub type NetRow = (i64, Fixed<String, 11>, Fixed<i64, 8>);
/// `(timestamp_ns, host tags + port + server, fields)`
pub type NginxRow = (i64, Fixed<String, 12>, Fixed<i64, 7>);
pub type PostgresqlRow = (i64, HostTags, Fixed<i64, 16>);
/// `(timestamp_ns, host tags + port + server, fields)`
pub type RedisRow = (i64, Fixed<String, 12>, Fixed<i64, 31>);

/// One devops point: any of the 9 measurements.
#[derive(Clone, Debug, PartialEq)]
pub enum DevopsPoint {
    Cpu(CpuRow),
    DiskIo(DiskIoRow),
    Disk(DiskRow),
    Kernel(KernelRow),
    Mem(MemRow),
    Net(NetRow),
    Nginx(NginxRow),
    Postgresql(PostgresqlRow),
    Redis(RedisRow),
}

fn tags_plus<const N: usize, const M: usize>(
    host: &HostTags,
    extra: [&str; M],
) -> Fixed<String, N> {
    debug_assert_eq!(N, 10 + M);
    Fixed(from_fn(|i| {
        if i < 10 {
            host.0[i].clone()
        } else {
            extra[i - 10].to_string()
        }
    }))
}

fn ints<const N: usize>(ws: &[Walk<Normal<GoSampler>>; N]) -> Fixed<i64, N> {
    Fixed(from_fn(|i| ws[i].get() as i64))
}

// ============================================================================
// Measurement state
// ============================================================================

const ONE_TERABYTE: i64 = 1 << 40;
const INODE_SIZE: i64 = 4096;
const SIXTEEN_GB: f64 = (16u64 * 1024 * 1024 * 1024) as f64;
const MEMORY_TOTALS: [i64; 3] = [8 << 30, 12 << 30, 16 << 30];
const FS_TYPES: [&str; 3] = ["ext3", "ext4", "btrfs"];

#[derive(Clone, Debug)]
// Field walks are boxed so the variants stay small.
enum Measurement {
    Cpu(Box<[Walk<Normal<GoSampler>>; 10]>),
    DiskIo(Box<[Walk<Normal<GoSampler>>; 7]>, String),
    Disk(Walk<Normal<GoSampler>>, String, String),
    Kernel(Box<[Walk<Normal<GoSampler>>; 5]>, i64),
    Mem(Box<[Walk<Normal<GoSampler>>; 3]>, i64),
    Net(Box<[Walk<Normal<GoSampler>>; 8]>, String),
    Nginx(Box<[Walk<Normal<GoSampler>>; 7]>, String, String),
    Postgresql(Box<[Walk<Normal<GoSampler>>; 16]>),
    Redis(Box<[Walk<Normal<GoSampler>>; 30]>, String, String, i64),
}

fn new_cpu(rng: &mut impl Rng) -> Measurement {
    // Each field's initial state draws Float64 in field order.
    let mut f = Vec::with_capacity(10);
    for _ in 0..10 {
        f.push(clamped_walk(
            normal(GoSampler, 0.0, 1.0),
            0.0,
            100.0,
            rng.go_float64() * 100.0,
        ));
    }
    Measurement::Cpu(Box::new(f.try_into().expect("10 fields")))
}

fn new_diskio(rng: &mut impl Rng) -> Measurement {
    let (ops, bytes, time) = (
        normal(GoSampler, 50.0, 1.0),
        normal(GoSampler, 100.0, 1.0),
        normal(GoSampler, 5.0, 1.0),
    );
    let f = [
        monotonic_walk(ops.clone(), 0.0),
        monotonic_walk(ops, 0.0),
        monotonic_walk(bytes.clone(), 0.0),
        monotonic_walk(bytes, 0.0),
        monotonic_walk(time.clone(), 0.0),
        monotonic_walk(time.clone(), 0.0),
        monotonic_walk(time, 0.0),
    ];
    let (a, b, c) = (rng.go_intn(1000), rng.go_intn(1000), rng.go_intn(1000));
    Measurement::DiskIo(Box::new(f), format!("{a:03}-{b:03}-{c:03}"))
}

fn new_disk(rng: &mut impl Rng) -> Measurement {
    let path = format!("/dev/sda{}", rng.go_intn(10));
    let fs = rng.go_choice(&FS_TYPES).to_string();
    let free = clamped_walk(
        normal(GoSampler, 50.0, 1.0),
        0.0,
        ONE_TERABYTE as f64,
        (ONE_TERABYTE / 2) as f64,
    );
    Measurement::Disk(free, path, fs)
}

fn new_kernel(rng: &mut impl Rng) -> Measurement {
    let f = from_fn(|_| monotonic_walk(normal(GoSampler, 5.0, 1.0), 0.0));
    Measurement::Kernel(Box::new(f), rng.go_int63n(240))
}

fn new_mem(rng: &mut impl Rng) -> Measurement {
    let total = *rng.go_choice(&MEMORY_TOTALS);
    let t = total as f64;
    let nd = normal(GoSampler, 0.0, t / 64.0);
    let mut f = Vec::with_capacity(3);
    for _ in 0..3 {
        f.push(clamped_walk(nd.clone(), 0.0, t, rng.go_float64() * t));
    }
    Measurement::Mem(Box::new(f.try_into().expect("3 fields")), total)
}

fn new_net(rng: &mut impl Rng) -> Measurement {
    let f = from_fn(|i| {
        let mean = if i < 4 { 50.0 } else { 5.0 };
        monotonic_walk(normal(GoSampler, mean, 1.0), 0.0)
    });
    Measurement::Net(Box::new(f), format!("eth{}", rng.go_intn(4)))
}

fn server_and_port(rng: &mut impl Rng, prefix: &str) -> (String, String) {
    let server = format!("{prefix}_{}", rng.go_intn(100000));
    let port = (rng.go_int63n(20000) + 1024).to_string();
    (port, server)
}

fn new_nginx(rng: &mut impl Rng) -> Measurement {
    // accepts, handled, requests are monotonic; the rest clamp to [0, 100].
    let f = from_fn(|i| {
        if matches!(i, 0 | 2 | 4) {
            monotonic_walk(normal(GoSampler, 5.0, 1.0), 0.0)
        } else {
            clamped_walk(normal(GoSampler, 5.0, 1.0), 0.0, 100.0, 0.0)
        }
    });
    let (port, server) = server_and_port(rng, "nginx");
    Measurement::Nginx(Box::new(f), port, server)
}

fn new_postgresql() -> Measurement {
    let f = from_fn(|i| {
        if i == 12 {
            clamped_walk(
                normal(GoSampler, 1024.0, 1.0),
                0.0,
                1024.0 * 1024.0 * 1024.0,
                0.0,
            )
        } else {
            clamped_walk(normal(GoSampler, 5.0, 1.0), 0.0, 1000.0, 0.0)
        }
    });
    Measurement::Postgresql(Box::new(f))
}

fn new_redis(rng: &mut impl Rng) -> Measurement {
    let low = || normal(GoSampler, 5.0, 1.0);
    let high = || normal(GoSampler, 50.0, 1.0);
    let f: [Walk<Normal<GoSampler>>; 30] = from_fn(|i| match i {
        0 => monotonic_walk(low(), 0.0),
        1..=4 => monotonic_walk(high(), 0.0),
        5..=7 => random_walk(normal(GoSampler, 1.0, 1.0), 0.0),
        8 | 13 => clamped_walk(high(), 0.0, 10000.0, 0.0),
        9..=12 => clamped_walk(high(), 0.0, SIXTEEN_GB, SIXTEEN_GB / 2.0),
        25 => clamped_walk(low(), 0.0, 100.0, 0.0),
        _ => clamped_walk(low(), 0.0, 1000.0, 0.0),
    });
    let (port, server) = server_and_port(rng, "redis");
    Measurement::Redis(Box::new(f), port, server, 0)
}

/// Go `time.Duration.Seconds()` truncated to int64.
fn duration_seconds(ns: i64) -> i64 {
    let sec = ns / 1_000_000_000;
    let nsec = ns % 1_000_000_000;
    (sec as f64 + nsec as f64 / 1e9) as i64
}

impl Measurement {
    fn tick(&mut self, rng: &mut impl Rng, interval_ns: i64) {
        fn adv(ws: &mut [Walk<Normal<GoSampler>>], rng: &mut impl Rng) {
            ws.iter_mut().for_each(|w| w.advance(rng));
        }
        match self {
            Measurement::Cpu(f) => adv(&mut f[..], rng),
            Measurement::DiskIo(f, _) => adv(&mut f[..], rng),
            Measurement::Disk(free, _, _) => free.advance(rng),
            Measurement::Kernel(f, _) => adv(&mut f[..], rng),
            Measurement::Mem(f, _) => adv(&mut f[..], rng),
            Measurement::Net(f, _) => adv(&mut f[..], rng),
            Measurement::Nginx(f, _, _) => adv(&mut f[..], rng),
            Measurement::Postgresql(f) => adv(&mut f[..], rng),
            Measurement::Redis(f, _, _, uptime) => {
                adv(&mut f[..], rng);
                *uptime += interval_ns;
            }
        }
    }

    fn point(&self, host: &HostTags, ts: i64) -> DevopsPoint {
        match self {
            Measurement::Cpu(f) => DevopsPoint::Cpu((ts, host.clone(), ints(f))),
            Measurement::DiskIo(f, serial) => {
                DevopsPoint::DiskIo((ts, tags_plus(host, [serial]), ints(f)))
            }
            Measurement::Disk(free, path, fs) => {
                let free = free.get() as i64;
                let total = ONE_TERABYTE;
                let used = total - free;
                let used_percent = (100.0 * (used as f64 / total as f64)) as i64;
                let inodes_total = total / INODE_SIZE;
                let inodes_free = free / INODE_SIZE;
                let fields = [
                    total,
                    free,
                    used,
                    used_percent,
                    inodes_total,
                    inodes_free,
                    inodes_total - inodes_free,
                ];
                DevopsPoint::Disk((ts, tags_plus(host, [path, fs]), Fixed(fields)))
            }
            Measurement::Kernel(f, boot_time) => DevopsPoint::Kernel((
                ts,
                host.clone(),
                Fixed(from_fn(|i| {
                    if i == 0 {
                        *boot_time
                    } else {
                        f[i - 1].get() as i64
                    }
                })),
            )),
            Measurement::Mem(f, total) => {
                let total = *total;
                let used = f[0].get() as i64;
                let cached = f[1].get() as i64;
                let buffered = f[2].get() as i64;
                let available = total - used;
                let t = total as f64;
                DevopsPoint::Mem((
                    ts,
                    host.clone(),
                    Fixed([total, available, used, available, cached, buffered]),
                    Fixed([
                        100.0 * (used as f64 / t),
                        100.0 * (available as f64 / t),
                        100.0 * (buffered as f64) / t,
                    ]),
                ))
            }
            Measurement::Net(f, iface) => DevopsPoint::Net((ts, tags_plus(host, [iface]), ints(f))),
            Measurement::Nginx(f, port, server) => {
                DevopsPoint::Nginx((ts, tags_plus(host, [port, server]), ints(f)))
            }
            Measurement::Postgresql(f) => DevopsPoint::Postgresql((ts, host.clone(), ints(f))),
            Measurement::Redis(f, port, server, uptime) => DevopsPoint::Redis((
                ts,
                tags_plus(host, [port, server]),
                Fixed(from_fn(|i| {
                    if i == 0 {
                        duration_seconds(*uptime)
                    } else {
                        f[i - 1].get() as i64
                    }
                })),
            )),
        }
    }
}

// ============================================================================
// Hosts
// ============================================================================

/// Which measurements each host carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostKind {
    CpuOnly,
    Devops,
}

/// A simulated host: tags plus measurement state.
#[derive(Clone, Debug)]
pub struct Host {
    pub tags: HostTags,
    measurements: Vec<Measurement>,
}

impl Host {
    /// TSBS `newHostWithMeasurementGenerator`: measurements first, then tags.
    pub fn new(id: usize, kind: HostKind, rng: &mut impl Rng) -> Host {
        let measurements = match kind {
            HostKind::CpuOnly => vec![new_cpu(rng)],
            HostKind::Devops => vec![
                new_cpu(rng),
                new_diskio(rng),
                new_disk(rng),
                new_kernel(rng),
                new_mem(rng),
                new_net(rng),
                new_nginx(rng),
                new_postgresql(),
                new_redis(rng),
            ],
        };
        let (region, datacenters) = *rng.go_choice(&REGIONS);
        let datacenter = rng.go_choice(datacenters).to_string();
        let rack = rng.go_int63n(RACKS).to_string();
        let arch = rng.go_choice(&ARCHES).to_string();
        let os = rng.go_choice(&OSES).to_string();
        let service = rng.go_int63n(SERVICES).to_string();
        let service_version = rng.go_int63n(SERVICE_VERSIONS).to_string();
        let environment = rng.go_choice(&ENVIRONMENTS).to_string();
        let team = rng.go_choice(&TEAMS).to_string();
        Host {
            tags: Fixed([
                format!("host_{id}"),
                region.to_string(),
                datacenter,
                rack,
                os,
                arch,
                team,
                service,
                service_version,
                environment,
            ]),
            measurements,
        }
    }

    fn tick(&mut self, rng: &mut impl Rng, interval_ns: i64) {
        for m in &mut self.measurements {
            m.tick(rng, interval_ns);
        }
    }
}

/// `cpu-only` host: one row per epoch, a [`CpuRow`].
#[derive(Clone, Debug)]
pub struct CpuHost(pub Host);

impl Entity for CpuHost {
    type Point = CpuRow;
    fn slots(&self) -> usize {
        1
    }
    fn tick(&mut self, rng: &mut impl Rng, interval_ns: i64) {
        self.0.tick(rng, interval_ns);
    }
    fn point(&self, _slot: usize, ts: i64) -> CpuRow {
        match self.0.measurements[0].point(&self.0.tags, ts) {
            DevopsPoint::Cpu(row) => row,
            _ => unreachable!("cpu-only hosts only carry cpu"),
        }
    }
}

/// `devops` host: 9 rows per epoch, one per measurement.
#[derive(Clone, Debug)]
pub struct DevopsHost(pub Host);

impl Entity for DevopsHost {
    type Point = DevopsPoint;
    fn slots(&self) -> usize {
        self.0.measurements.len()
    }
    fn tick(&mut self, rng: &mut impl Rng, interval_ns: i64) {
        self.0.tick(rng, interval_ns);
    }
    fn point(&self, slot: usize, ts: i64) -> DevopsPoint {
        self.0.measurements[slot].point(&self.0.tags, ts)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CpuHostFactory;

impl EntityFactory for CpuHostFactory {
    type Entity = CpuHost;
    fn make(&mut self, id: usize, rng: &mut impl Rng) -> CpuHost {
        CpuHost(Host::new(id, HostKind::CpuOnly, rng))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DevopsHostFactory;

impl EntityFactory for DevopsHostFactory {
    type Entity = DevopsHost;
    fn make(&mut self, id: usize, rng: &mut impl Rng) -> DevopsHost {
        DevopsHost(Host::new(id, HostKind::Devops, rng))
    }
}

// ============================================================================
// Demultiplexing sink: one columnar sink per measurement
// ============================================================================

type SinkOf<B, R> = <R as DefaultSink<B>>::Sink;

/// Columnar sinks for all 9 measurements; a [`DevopsPoint`] goes to its own.
pub struct DevopsSinks<B: Backend>
where
    CpuRow: DefaultSink<B>,
    DiskIoRow: DefaultSink<B>,
    DiskRow: DefaultSink<B>,
    KernelRow: DefaultSink<B>,
    MemRow: DefaultSink<B>,
    NetRow: DefaultSink<B>,
    NginxRow: DefaultSink<B>,
    PostgresqlRow: DefaultSink<B>,
    RedisRow: DefaultSink<B>,
{
    pub cpu: SinkOf<B, CpuRow>,
    pub diskio: SinkOf<B, DiskIoRow>,
    pub disk: SinkOf<B, DiskRow>,
    pub kernel: SinkOf<B, KernelRow>,
    pub mem: SinkOf<B, MemRow>,
    pub net: SinkOf<B, NetRow>,
    pub nginx: SinkOf<B, NginxRow>,
    pub postgresql: SinkOf<B, PostgresqlRow>,
    pub redis: SinkOf<B, RedisRow>,
}

macro_rules! devops_sinks {
    ($($field:ident: $row:ty => $variant:ident),* $(,)?) => {
        impl<B: Backend> Default for DevopsSinks<B>
        where
            $($row: DefaultSink<B>, SinkOf<B, $row>: Default,)*
        {
            fn default() -> Self {
                DevopsSinks { $($field: Default::default(),)* }
            }
        }

        impl<B: Backend> Sink<DevopsPoint> for DevopsSinks<B>
        where
            $($row: DefaultSink<B>, SinkOf<B, $row>: Sink<$row>,)*
        {
            fn push(&mut self, value: DevopsPoint) {
                match value {
                    $(DevopsPoint::$variant(r) => self.$field.push(r),)*
                }
            }
        }

        impl<B: Backend> DefaultSink<B> for DevopsPoint
        where
            $($row: DefaultSink<B>,)*
        {
            type Sink = HCons<DevopsSinks<B>, HNil>;
        }
    };
}

devops_sinks!(
    cpu: CpuRow => Cpu,
    diskio: DiskIoRow => DiskIo,
    disk: DiskRow => Disk,
    kernel: KernelRow => Kernel,
    mem: MemRow => Mem,
    net: NetRow => Net,
    nginx: NginxRow => Nginx,
    postgresql: PostgresqlRow => Postgresql,
    redis: RedisRow => Redis,
);

// ============================================================================
// Influx line protocol
// ============================================================================

fn line_ints<const T: usize, const F: usize>(
    out: &mut String,
    name: &str,
    tag_keys: &[&str],
    row: &(i64, Fixed<String, T>, Fixed<i64, F>),
    field_keys: &[&str],
) {
    let mut w = LineWriter::new(out, name);
    for (k, v) in tag_keys.iter().zip(&row.1.0) {
        w.tag(k, Some(v));
    }
    w.begin_fields(true);
    for (k, v) in field_keys.iter().zip(row.2.0) {
        w.field(k, Some(FieldValue::Int(v)));
    }
    w.finish(row.0);
}

/// Host tag keys followed by measurement-specific ones.
fn keys<const M: usize>(extra: [&'static str; M]) -> Vec<&'static str> {
    HOST_TAGS.iter().copied().chain(extra).collect()
}

/// Append `row` as one TSBS-format Influx line.
pub fn write_cpu_line(out: &mut String, row: &CpuRow) {
    line_ints(out, "cpu", &HOST_TAGS, row, &CPU_FIELDS);
}

impl DevopsPoint {
    /// Append this point as one TSBS-format Influx line.
    pub fn write_line(&self, out: &mut String) {
        match self {
            DevopsPoint::Cpu(r) => write_cpu_line(out, r),
            DevopsPoint::DiskIo(r) => {
                line_ints(out, "diskio", &keys(["serial"]), r, &DISKIO_FIELDS)
            }
            DevopsPoint::Disk(r) => {
                line_ints(out, "disk", &keys(["path", "fstype"]), r, &DISK_FIELDS)
            }
            DevopsPoint::Kernel(r) => line_ints(out, "kernel", &HOST_TAGS, r, &KERNEL_FIELDS),
            DevopsPoint::Mem((ts, tags, i, f)) => {
                let mut w = LineWriter::new(out, "mem");
                for (k, v) in HOST_TAGS.iter().zip(&tags.0) {
                    w.tag(k, Some(v));
                }
                w.begin_fields(true);
                for (k, v) in MEM_INT_FIELDS.iter().zip(i.0) {
                    w.field(k, Some(FieldValue::Int(v)));
                }
                for (k, v) in MEM_FLOAT_FIELDS.iter().zip(f.0) {
                    w.field(k, Some(FieldValue::Float(v)));
                }
                w.finish(*ts);
            }
            DevopsPoint::Net(r) => line_ints(out, "net", &keys(["interface"]), r, &NET_FIELDS),
            DevopsPoint::Nginx(r) => {
                line_ints(out, "nginx", &keys(["port", "server"]), r, &NGINX_FIELDS)
            }
            DevopsPoint::Postgresql(r) => {
                line_ints(out, "postgresl", &HOST_TAGS, r, &POSTGRESQL_FIELDS)
            }
            DevopsPoint::Redis(r) => {
                line_ints(out, "redis", &keys(["port", "server"]), r, &REDIS_FIELDS)
            }
        }
    }
}
