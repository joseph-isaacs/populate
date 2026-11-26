//! Generate the YCSB load, or the operations of a YCSB run.
//!
//! ```text
//! ycsb-gen load [--records 1000] [--insert-start 0] [--insert-count N]
//!               [--field-count 10] [--field-length 100] [--field-prefix field]
//!               [--ordered] [--zero-padding 1] [--data-integrity]
//!               [--java-seed 0] [--java-thread 1] [common flags]
//! ycsb-gen run  [load flags] [--operations 1000] [--workload a-f]
//!               [--read P] [--update P] [--insert P] [--scan P] [--rmw P]
//!               [--distribution uniform|zipfian|latest|hotspot]
//!               [--hotspot-data 0.2] [--hotspot-ops 0.8] [--read-one-field]
//!               [--write-all-fields] [--scan-length MIN,MAX] [--scan-zipfian]
//!               [common flags]
//!
//! common flags: [--out DIR] [--format parquet|text|none] [--threads N]
//!               [--batch ROWS] [--row-group ROWS] [--compression snappy|none]
//!               [--no-dictionary]
//! ```
//!
//! `--format parquet` (the default) writes the load as
//! `DIR/usertable.parquet` and the run as `DIR/transactions.parquet` (one
//! row per binding call: `op`, key, scan length, fields read, values
//! written); `--format text` writes them as TSV (`key\tfield0\t...` for the
//! load); `--format none` only generates.
//! Every chunk of every table is a job on one pool of `--threads` threads
//! (default: all cores); the data is the same for any thread count and
//! `--row-group`.

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use parquet::file::properties::WriterPropertiesBuilder;
use populate_table::Chunks;
use populate_table::cli::{Args, Tool, num, report, write};
use populate_table::text::TextChunks;
use populate_ycsb::{JavaTlr, RequestDistribution, Ycsb, YcsbRun};
use std::process::exit;
use std::time::Instant;

/// mimalloc: buffers are reused instead of returned to the OS and faulted
/// in again
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const USAGE: &str = "usage: ycsb-gen load|run [flags]  (see --help)";

const HELP: &str =
    "ycsb-gen load [--records 1000] [--insert-start 0] [--insert-count N] [--field-count 10]
              [--field-length 100] [--field-prefix field] [--ordered] [--zero-padding 1]
              [--data-integrity] [--java-seed 0] [--java-thread 1]
ycsb-gen run  [load flags] [--operations 1000] [--workload a-f]
              [--read P] [--update P] [--insert P] [--scan P] [--rmw P]
              [--distribution uniform|zipfian|latest|hotspot] [--hotspot-data 0.2]
              [--hotspot-ops 0.8] [--read-one-field] [--write-all-fields]
              [--scan-length MIN,MAX] [--scan-zipfian]
common: [--out DIR] [--format parquet|text|none] [--threads N] [--batch ROWS]
        [--row-group ROWS] [--compression snappy|none] [--no-dictionary]";

const LOAD_FLAGS: &str = "[--records 1000] [--insert-start 0] [--insert-count N] \
[--field-count 10] [--field-length 100] [--field-prefix field] [--ordered] [--zero-padding 1] \
[--data-integrity] [--java-seed 0] [--java-thread 1]";

const RUN_FLAGS: &str = "[load flags] [--operations 1000] [--workload a-f] [--read P] \
[--update P] [--insert P] [--scan P] [--rmw P] [--distribution uniform|zipfian|latest|hotspot] \
[--hotspot-data 0.2] [--hotspot-ops 0.8] [--read-one-field] [--write-all-fields] \
[--scan-length MIN,MAX] [--scan-zipfian]";

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_default();
    let r = match cmd.as_str() {
        "load" => ycsb(args),
        "run" => ycsb_run(args),
        "-h" | "--help" => {
            println!("{HELP}");
            exit(0);
        }
        _ => Err(format!("unknown command `{cmd}`")),
    };
    if let Err(e) = r {
        eprintln!("{e}\n{USAGE}");
        exit(2);
    }
}

fn ycsb(args: impl Iterator<Item = String>) -> Result<(), String> {
    ycsb_cmd(args, false)
}

fn ycsb_run(args: impl Iterator<Item = String>) -> Result<(), String> {
    ycsb_cmd(args, true)
}

type RunSet = Box<dyn FnOnce(YcsbRun) -> YcsbRun>;

/// `ycsb` (the load) and `ycsb-run` (the transactions on top of it).
fn ycsb_cmd(args: impl Iterator<Item = String>, transactions: bool) -> Result<(), String> {
    let mut tlr = JavaTlr {
        seed: 0,
        thread_id: 1,
    };
    let mut records = 1000;
    let mut operations: u64 = 1000;
    let mut workload: Option<char> = None;
    let mut sets: Vec<Box<dyn FnOnce(Ycsb) -> Ycsb>> = Vec::new();
    let mut run_sets: Vec<RunSet> = Vec::new();
    let mut proportions: [Option<f64>; 5] = [None; 5];
    let mut distribution: Option<String> = None;
    let mut hotspot = (0.2, 0.8);
    let mut hotspot_set = false;
    let tool = Tool {
        ext: "tsv",
        max_sf: None,
        tables: false,
        flags: if transactions { RUN_FLAGS } else { LOAD_FLAGS },
        ..if transactions {
            Tool {
                out: "ycsb-run",
                ..Tool::new("ycsb-gen run", "text", 0.0)
            }
        } else {
            Tool::new("ycsb-gen load", "text", 0.0)
        }
    };
    let own = |flag: &str, val: &mut dyn FnMut() -> Result<String, String>| {
        let mut prop = |k: usize, v: f64| {
            proportions[k] = Some(v);
        };
        match flag {
            "--records" => records = num(flag, val()?)?,
            "--insert-start" => {
                let n: u64 = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.insert_start(n)))
            }
            "--insert-count" => {
                let n: u64 = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.insert_count(n)))
            }
            "--field-count" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.field_count(n)))
            }
            "--field-length" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.field_length(n)))
            }
            "--field-prefix" => {
                let p = val()?;
                if !p.bytes().all(|b| b.is_ascii_graphic() || b == b' ') {
                    return Err("--field-prefix must be printable ASCII".into());
                }
                sets.push(Box::new(move |y| y.field_prefix(&p)))
            }
            "--zero-padding" => {
                let n: usize = num(flag, val()?)?;
                sets.push(Box::new(move |y| y.zero_padding(n)))
            }
            "--ordered" => sets.push(Box::new(|y| y.ordered(true))),
            "--data-integrity" => sets.push(Box::new(|y| y.data_integrity(true))),
            "--java-seed" => tlr.seed = num::<i64>(flag, val()?)? as u64,
            "--java-thread" => tlr.thread_id = num(flag, val()?)?,
            "--operations" if transactions => operations = num(flag, val()?)?,
            "--workload" if transactions => {
                let w = val()?.to_ascii_lowercase();
                match w.as_str() {
                    "a" | "b" | "c" | "d" | "e" | "f" => workload = w.chars().next(),
                    _ => return Err(format!("--workload: `{w}` is not a to f")),
                }
            }
            "--read" if transactions => prop(0, num(flag, val()?)?),
            "--update" if transactions => prop(1, num(flag, val()?)?),
            "--insert" if transactions => prop(2, num(flag, val()?)?),
            "--scan" if transactions => prop(3, num(flag, val()?)?),
            "--rmw" if transactions => prop(4, num(flag, val()?)?),
            "--distribution" if transactions => distribution = Some(val()?),
            "--hotspot-data" if transactions => {
                hotspot.0 = num(flag, val()?)?;
                hotspot_set = true;
            }
            "--hotspot-ops" if transactions => {
                hotspot.1 = num(flag, val()?)?;
                hotspot_set = true;
            }
            "--read-one-field" if transactions => {
                run_sets.push(Box::new(|r| r.read_all_fields(false)))
            }
            "--write-all-fields" if transactions => {
                run_sets.push(Box::new(|r| r.write_all_fields(true)))
            }
            "--scan-length" if transactions => {
                let v = val()?;
                let (a, b) = v.split_once(',').ok_or("--scan-length takes MIN,MAX")?;
                let (a, b): (i64, i64) = (num(flag, a.into())?, num(flag, b.into())?);
                if !(0 <= a && a <= b) {
                    return Err("--scan-length: need 0 <= MIN <= MAX".into());
                }
                run_sets.push(Box::new(move |r| {
                    let z = r.scan_zipfian();
                    r.scan_length(a, b, z)
                }))
            }
            "--scan-zipfian" if transactions => run_sets.push(Box::new(|r| {
                let (a, b) = r.scan_lengths();
                r.scan_length(a, b, true)
            })),
            _ => return Ok(false),
        }
        Ok(true)
    };
    let a = Args::<String>::parse_from(tool, Vec::new, own, args)?;
    let mut y = Ycsb::new(records);
    for s in sets {
        y = s(y);
    }
    y = y.java_random(tlr);
    y.check()?;
    if !transactions {
        y = y.batch_size(a.batch);
        if let Some(r) = a.row_group {
            y = y.chunk_rows(r);
        }
        run(&a, "usertable", y, Ycsb::parquet_props());
        return Ok(());
    }
    let mut r = match workload {
        Some(w) => YcsbRun::workload(w, y, operations),
        None => YcsbRun::new(y, operations),
    };
    if hotspot_set && distribution.as_deref() != Some("hotspot") {
        return Err("--hotspot-data and --hotspot-ops need --distribution hotspot".into());
    }
    if proportions.iter().any(Option::is_some) {
        // override only the proportions given, as YCSB's -p does
        let mut p = r.proportion_values();
        for (v, o) in p.iter_mut().zip(proportions) {
            if let Some(o) = o {
                *v = o;
            }
        }
        if p.iter().any(|x| !(x.is_finite() && *x >= 0.0)) || p.iter().all(|&x| x == 0.0) {
            return Err("proportions must be finite and >= 0, and one > 0".into());
        }
        r = r.proportions(p[0], p[1], p[2], p[3], p[4]);
    }
    if let Some(d) = distribution {
        let (h, o) = hotspot;
        if d == "hotspot" && (!(0.0..=1.0).contains(&h) || !(0.0..=1.0).contains(&o)) {
            return Err("--hotspot-data and --hotspot-ops must be in 0..=1".into());
        }
        r = r.distribution(match d.as_str() {
            "uniform" => RequestDistribution::Uniform,
            "zipfian" => RequestDistribution::Zipfian,
            "latest" => RequestDistribution::Latest,
            "hotspot" => RequestDistribution::Hotspot {
                data_fraction: h,
                op_fraction: o,
            },
            _ => return Err(format!("--distribution: unknown `{d}`")),
        });
    }
    for s in run_sets {
        r = s(r);
    }
    r = r.batch_size(a.batch);
    if let Some(n) = a.row_group {
        r = r.chunk_ops(n);
    }
    r.check()?;
    run(&a, "transactions", r, YcsbRun::parquet_props());
    Ok(())
}

/// Write `table` as `a` says, and report.
fn run<T: Chunks + TextChunks>(
    a: &Args<String>,
    name: &str,
    table: T,
    props: WriterPropertiesBuilder,
) {
    let paths = a.paths(&[name]);
    let start = Instant::now();
    let (rows, bytes) = write(a, &paths, &[table], props);
    report(&[name], &rows, &bytes, start.elapsed().as_secs_f64(), "");
}
