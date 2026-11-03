use crate::devops::{ARCHES, CpuRow, ENVIRONMENTS, HOST_TAGS, OSES, REGIONS, TEAMS};
use crate::fixed::Fixed;
use crate::gorand::{GoRand, GoRandExt};
use crate::influx::{go_format_float, go_format_float32};
use crate::iot::{DRIVERS, FLEETS, IotPoint, MODELS};
use crate::process::{
    Constant, Process, RefillWalk, clamped_walk, lazy, monotonic_walk, precision, random_walk,
    series,
};
use crate::{
    DEFAULT_START_NS, TsbsConfig, UseCase, cpu_only, cpu_only_record_batch, devops,
    devops_record_batches, influx_lines, iot_points, iot_record_batches,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int64Type, TimestampNanosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use populate_2::arrow::DistributionArrow;
use populate_2::traits::{Distribution, DistributionScalar, SampleIter, VecBackend};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::collections::HashSet;
use std::fs::read_to_string;

const MIN_NS: i64 = 60_000_000_000;

fn golden(name: &str) -> String {
    read_to_string(format!(
        "{}/tests/golden/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn config(use_case: UseCase, seed: i64, scale: u64, minutes: i64) -> TsbsConfig {
    TsbsConfig {
        seed,
        scale,
        end_ns: DEFAULT_START_NS + minutes * MIN_NS,
        ..TsbsConfig::new(use_case)
    }
}

// ============================================================================
// (a) Go RNG port vs a Go stdlib golden dump (validate/gorand)
// ============================================================================

#[test]
fn gorand_matches_go_stdlib() {
    let text = golden("gorand.txt");
    let mut rng = GoRand::new(0);
    let mut checked = 0;
    for line in text.lines() {
        let parts: Vec<&str> = line.split(' ').collect();
        match parts[..] {
            ["seed", s] => rng = GoRand::new(s.parse().unwrap()),
            ["int63", v] => assert_eq!(rng.go_int63(), v.parse::<i64>().unwrap()),
            ["float64", v] => {
                assert_eq!(
                    rng.go_float64().to_bits(),
                    u64::from_str_radix(v, 16).unwrap()
                )
            }
            ["intn", n, v] => {
                assert_eq!(rng.go_intn(n.parse().unwrap()), v.parse::<usize>().unwrap())
            }
            ["int63n", n, v] => {
                assert_eq!(rng.go_int63n(n.parse().unwrap()), v.parse::<i64>().unwrap())
            }
            ["norm", v] => assert_eq!(
                rng.go_norm_float64().to_bits(),
                u64::from_str_radix(v, 16).unwrap(),
                "line {line}"
            ),
            ["uint32", v] => assert_eq!(rng.go_uint32(), v.parse::<u32>().unwrap()),
            _ => panic!("unexpected golden line {line}"),
        }
        checked += 1;
    }
    assert!(checked > 6000);
}

// ============================================================================
// (b) Byte-exact excerpts of the reference `tsbs_generate_data --format=influx`
// ============================================================================

#[test]
fn cpu_only_matches_reference_inline() {
    // tsbs_generate_data --use-case=cpu-only --seed=123 --scale=2
    //   --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-01T00:00:30Z
    //   --log-interval=10s --format=influx | head -2
    let expected = "cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1451606400000000000
cpu,hostname=host_1,region=us-west-1,datacenter=us-west-1a,rack=41,os=Ubuntu15.10,arch=x64,team=NYC,service=9,service_version=1,service_environment=staging usage_user=84i,usage_system=11i,usage_idle=53i,usage_nice=87i,usage_iowait=29i,usage_irq=20i,usage_softirq=54i,usage_steal=77i,usage_guest=53i,usage_guest_nice=74i 1451606400000000000
";
    let mut c = config(UseCase::CpuOnly, 123, 2, 0);
    c.end_ns = DEFAULT_START_NS + 10_000_000_000;
    assert_eq!(influx_lines(&c, &mut c.rng()), expected);
}

#[test]
fn cpu_only_matches_reference() {
    let mut c = config(UseCase::CpuOnly, 123, 2, 0);
    c.end_ns = DEFAULT_START_NS + 30_000_000_000;
    assert_eq!(
        influx_lines(&c, &mut c.rng()),
        golden("cpu-only_seed123_scale2_30s.influx")
    );
}

#[test]
fn devops_matches_reference() {
    let mut c = config(UseCase::Devops, 5, 2, 0);
    c.end_ns = DEFAULT_START_NS + 20_000_000_000;
    assert_eq!(
        influx_lines(&c, &mut c.rng()),
        golden("devops_seed5_scale2_20s.influx")
    );
}

#[test]
fn devops_ramp_and_limit_match_reference() {
    let c = TsbsConfig {
        initial_scale: 2,
        limit: 250,
        ..config(UseCase::Devops, 9, 6, 1)
    };
    let out = influx_lines(&c, &mut c.rng());
    assert_eq!(out, golden("devops_seed9_scale6_init2_limit250.influx"));
    assert_eq!(c.rows(), 128);
}

#[test]
fn iot_matches_reference() {
    let c = config(UseCase::Iot, 7, 3, 10);
    assert_eq!(
        influx_lines(&c, &mut c.rng()),
        golden("iot_seed7_scale3_10m.influx")
    );
}

// ============================================================================
// Ported TSBS unit tests (deterministic parts)
// ============================================================================

#[test]
fn float_precision_get() {
    // common/distribution_test.go TestFloatPrecisionGet
    let cases = [
        (-1, 1.0),
        (0, 1.0),
        (1, 1.2),
        (2, 1.23),
        (3, 1.234),
        (4, 1.2345),
        (5, 1.23456),
        (6, 1.23456),
        (9, 1.23456),
    ];
    for (digits, want) in cases {
        assert_eq!(precision(Constant(1.234567890), digits).get(), want);
    }
}

#[test]
fn lazy_distribution() {
    // common/distribution_test.go TestLazyDistribution
    let mut rng = GoRand::new(1);
    let cases = [
        (1.0, 0.0, 1.0, 2.0, 2.0),
        (1.0, 1.0, 1.0, 2.0, 3.0),
        (0.5, 1.0, 0.5, 0.5, 2.0),
    ];
    for (threshold, motive, step, state, want) in cases {
        let mut ld = lazy(
            Constant(motive),
            clamped_walk(Constant(step), 2.0, 4.0, state),
            threshold,
        );
        ld.advance(&mut rng);
        assert_eq!(ld.get(), want);
    }
}

#[test]
fn refill_walk_refills_at_min() {
    // iot/diagnostics_test.go TestCustomFuelDistribution
    let mut rng = GoRand::new(1);
    let mut clamped = clamped_walk(Constant(-1.0), 10.0, 100.0, 100.0);
    let mut fuel = RefillWalk(clamped.clone());
    while clamped.get() > 10.0 {
        clamped.advance(&mut rng);
        fuel.advance(&mut rng);
        if clamped.get() == 10.0 {
            assert_eq!(fuel.get(), 100.0);
        } else {
            assert_eq!(fuel.get(), clamped.get());
        }
    }
}

#[test]
fn walks() {
    let mut rng = GoRand::new(1);
    let mut w = random_walk(Constant(-2.0), 1.0);
    let mut m = monotonic_walk(Constant(-2.0), 1.0);
    w.advance(&mut rng);
    m.advance(&mut rng);
    assert_eq!((w.get(), m.get()), (-1.0, 3.0));
}

// ============================================================================
// Processes as populate-2 columns
// ============================================================================

fn make_clamped_series() -> impl Distribution<VecBackend, Item = f64> {
    series(clamped_walk(Constant(30.0), 0.0, 100.0, 15.0))
}

#[test]
fn series_is_a_populate_column() {
    let mut d = make_clamped_series();
    let col: Vec<f64> = d.collect_col(Vec::new(), &mut GoRand::new(1), 5);
    assert_eq!(col, vec![15.0, 45.0, 75.0, 100.0, 100.0]);
}

fn make_cpu_panel(c: &TsbsConfig) -> impl Distribution<VecBackend, Item = CpuRow> {
    cpu_only(c)
}

#[test]
fn panel_collects_columnwise_and_rowwise_identically() {
    let c = config(UseCase::CpuOnly, 3, 4, 5);
    let n = c.rows() as usize;
    let mut d = make_cpu_panel(&c);
    let mut it = d.row_iter();
    let mut rng = c.rng();
    let rows: Vec<CpuRow> = (0..n).map(|_| it.next(&mut rng)).collect();
    let rb = d.collect_record_batch(&mut c.rng(), n).unwrap();
    // `Fixed` groups arrive as struct columns before flattening.
    assert_eq!(rb.num_columns(), 3);
    assert_eq!(rb.num_rows(), n);
    let ts = rb.column(0).as_primitive::<Int64Type>();
    let fields = rb.column(2).as_struct();
    for (i, (t, _, Fixed(f))) in rows.iter().enumerate() {
        assert_eq!(ts.value(i), *t);
        for (j, v) in f.iter().enumerate() {
            assert_eq!(fields.column(j).as_primitive::<Int64Type>().value(i), *v);
        }
    }
}

// ============================================================================
// (c) Structural checks on the Arrow output
// ============================================================================

fn strings(rb: &RecordBatch, name: &str) -> Vec<String> {
    rb.column_by_name(name)
        .unwrap()
        .as_string::<i32>()
        .iter()
        .map(|s| s.unwrap().to_string())
        .collect()
}

fn ints(rb: &RecordBatch, name: &str) -> Vec<i64> {
    rb.column_by_name(name)
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn cpu_only_record_batch_shape_and_domains() {
    let c = TsbsConfig {
        interval_ns: 10_000_000_000,
        ..config(UseCase::CpuOnly, 42, 25, 60)
    };
    let rb = cpu_only_record_batch(&c, &mut c.rng()).unwrap();
    let epochs = 360;
    assert_eq!(rb.num_rows(), 25 * epochs);
    assert_eq!(rb.num_columns(), 21);
    assert_eq!(rb.schema().field(0).name(), "timestamp");
    assert_eq!(
        rb.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    for (i, t) in HOST_TAGS.iter().enumerate() {
        assert_eq!(rb.schema().field(1 + i).name(), t);
    }
    assert_eq!(rb.schema().field(11).name(), "usage_user");

    // Epoch-major, host-minor; timestamps step by the interval.
    let ts = rb.column(0).as_primitive::<TimestampNanosecondType>();
    let hosts = strings(&rb, "hostname");
    for (i, hostname) in hosts.iter().enumerate() {
        let (epoch, host) = (i / 25, i % 25);
        assert_eq!(ts.value(i), DEFAULT_START_NS + epoch as i64 * c.interval_ns);
        assert_eq!(hostname, &format!("host_{host}"));
    }

    // Tags come from TSBS's tables and are fixed per host.
    let region = strings(&rb, "region");
    let dc = strings(&rb, "datacenter");
    for i in 0..rb.num_rows() {
        let (_, dcs) = REGIONS.iter().find(|(r, _)| *r == region[i]).unwrap();
        assert!(dcs.contains(&dc[i].as_str()));
        assert_eq!(region[i], region[i % 25]);
    }
    let within = |col: &str, choices: &[&str]| {
        assert!(
            strings(&rb, col)
                .iter()
                .all(|v| choices.contains(&v.as_str()))
        )
    };
    within("os", &OSES);
    within("arch", &ARCHES);
    within("team", &TEAMS);
    within("service_environment", &ENVIRONMENTS);
    let numeric = |col: &str, max: i64| {
        assert!(
            strings(&rb, col)
                .iter()
                .all(|v| (0..max).contains(&v.parse().unwrap()))
        )
    };
    numeric("rack", 100);
    numeric("service", 20);
    numeric("service_version", 2);

    // Clamped walks stay in [0, 100].
    for f in ["usage_user", "usage_idle", "usage_guest_nice"] {
        assert!(ints(&rb, f).iter().all(|v| (0..=100).contains(v)));
    }
}

#[test]
fn devops_record_batches_match_influx_stream() {
    let c = config(UseCase::Devops, 77, 3, 5);
    let batches = devops_record_batches(&c, &mut c.rng()).unwrap();
    let lines = influx_lines(&c, &mut c.rng());
    assert_eq!(batches.len(), 9);
    for (name, rb) in &batches {
        let prefix = format!("{name},");
        assert_eq!(
            rb.num_rows(),
            lines.lines().filter(|l| l.starts_with(&prefix)).count(),
            "{name}"
        );
        assert_eq!(rb.num_rows(), 3 * 30);
    }
    let (_, disk) = &batches[2];
    let (total, free, used) = (ints(disk, "total"), ints(disk, "free"), ints(disk, "used"));
    for i in 0..disk.num_rows() {
        assert_eq!(total[i], 1 << 40);
        assert_eq!(total[i] - used[i], free[i]);
    }
    let (_, mem) = &batches[4];
    let pct = mem
        .column_by_name("used_percent")
        .unwrap()
        .as_primitive::<Float64Type>();
    assert!(pct.values().iter().all(|p| (0.0..=100.0).contains(p)));
    // Monotonic counters never decrease per host.
    let (_, net) = &batches[5];
    let sent = ints(net, "bytes_sent");
    for i in 3..sent.len() {
        assert!(sent[i] >= sent[i - 3]);
    }
    // The first cpu row matches the first influx line.
    let (_, cpu) = &batches[0];
    let first = lines.lines().next().unwrap();
    assert!(first.contains(&format!("usage_user={}i", ints(cpu, "usage_user")[0])));
}

#[test]
fn iot_faults_and_nulls() {
    let c = config(UseCase::Iot, 11, 20, 120);
    let points = iot_points(&c, &mut c.rng());
    let (readings, diagnostics) = iot_record_batches(&c, &mut c.rng()).unwrap();
    assert_eq!(readings.num_rows() + diagnostics.num_rows(), points.len());

    // Some points are dropped (missing batches/entries) ...
    let full = c.max_points() as usize;
    assert!(points.len() < full);
    assert!(points.len() > full * 3 / 4);
    // ... some arrive out of order ...
    let ts: Vec<i64> = points
        .iter()
        .map(|p| match p {
            IotPoint::Readings(r) => r.0,
            IotPoint::Diagnostics(r) => r.0,
        })
        .collect();
    assert!(ts.windows(2).any(|w| w[1] < w[0]));
    // ... and some values are cleared, which become Arrow nulls.
    let nulls: usize = readings
        .columns()
        .iter()
        .chain(diagnostics.columns())
        .map(|c| c.null_count())
        .sum();
    assert!(nulls > 0);
    assert!(readings.schema().field(1).is_nullable());

    let fleets: HashSet<_> = readings
        .column_by_name("fleet")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .flatten()
        .map(str::to_string)
        .collect();
    assert!(fleets.iter().all(|f| FLEETS.contains(&f.as_str())));
    let drivers = readings
        .column_by_name("driver")
        .unwrap()
        .as_string::<i32>();
    assert!(drivers.iter().flatten().all(|d| DRIVERS.contains(&d)));
    let lat = readings
        .column_by_name("latitude")
        .unwrap()
        .as_primitive::<Float64Type>();
    assert!(lat.iter().flatten().all(|v| (-90.0..=90.0).contains(&v)));
    let load = readings
        .column_by_name("load_capacity")
        .unwrap()
        .as_primitive::<Float32Type>();
    assert!(
        load.iter()
            .flatten()
            .all(|v| MODELS.iter().any(|m| m.1 == v))
    );
}

// ============================================================================
// Fast mode: any rand::Rng drives the same model
// ============================================================================

#[test]
fn fast_mode_with_any_rng() {
    let c = config(UseCase::Devops, 0, 4, 10);
    let mut rng = SmallRng::seed_from_u64(7);
    let batches = devops_record_batches(&c, &mut rng).unwrap();
    assert!(batches.iter().all(|(_, rb)| rb.num_rows() == 4 * 60));
    let mut d = devops(&c);
    let mut it = <_ as Distribution<VecBackend>>::row_iter(&mut d);
    let _ = it.next(&mut SmallRng::seed_from_u64(7));
}

#[test]
fn go_float_formatting_breaks_ties_to_even() {
    // 77.722930908203125 is exactly representable: a tie at 16 digits.
    let tie = 77.0 + 0.722930908203125;
    assert_eq!(tie.to_string(), "77.72293090820313");
    assert_eq!(go_format_float(tie), "77.72293090820312");
    assert_eq!(go_format_float(0.1), "0.1");
    assert_eq!(go_format_float(25.0), "25");
    assert_eq!(go_format_float(1e21), "1000000000000000000000");
    assert_eq!(go_format_float(-0.5), "-0.5");
    assert_eq!(go_format_float32(2000.0), "2000");
    assert_eq!(go_format_float32(0.1), "0.1");
}
