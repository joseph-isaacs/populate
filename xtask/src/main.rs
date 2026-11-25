//! Repository tasks, run as `cargo xtask <task>`.
//!
//! `bench` builds the generator binaries in release mode, runs each
//! generator in each output format at each scale and thread count, and
//! prints a Markdown table. Where a reference tool is installed
//! (`tpcgen-cli` for TPC-H and TPC-DS, `tsbs_generate_data` for TSBS,
//! `ssb-dbgen` for SSB) it
//! runs alternately with ours, so drift on a noisy machine hits both alike.
//! Usage is in `USAGE` and BENCHMARKS.md.

use std::collections::{BTreeMap, HashMap};
use std::env::{args, current_dir, split_paths, temp_dir, var_os};
use std::fs::{File, create_dir_all, read_dir, remove_dir_all};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::thread::available_parallelism;
use std::time::{Duration, Instant};

mod report;

use report::Report;

const USAGE: &str = "\
usage: cargo xtask bench [--scale 1] [--threads 1,N] [--generators all] [--formats text,parquet,arrow]
                         [--runs 3] [--out DIR] [--tpcgen-cli PATH] [--tsbs-generate-data PATH]
                         [--ssb-dbgen PATH] [--ssb-dists PATH] [--no-reference] [--json PATH] [--list]

  --scale       comma-separated scale factors; each generator maps one to its own size (see --list)
  --threads     comma-separated thread counts (default: 1 and every core)
  --generators  comma-separated names from --list, or a family (h2o, ycsb, synth); default all
  --formats     text (the reference tool's own format), parquet, arrow (generated in memory, dropped)
  --runs        runs per configuration; the best wall time is kept
  --out         where output goes while a run lasts (default /dev/shm, else the temp dir);
                it is written to DIR/populate-bench and deleted after every run
  --json        save a versioned JSON report, checkpointed after each configuration
  --ssb-dbgen   SSB reference binary (single-threaded, text only)
  --ssb-dists   its dists.dss file (default: beside the binary or in its parent directory)
  --no-reference  skip reference tools even when they are installed";

fn main() -> ExitCode {
    let a: Vec<String> = args().skip(1).collect();
    let r = match a.first().map(String::as_str) {
        Some("bench") => parse(&a[1..]).and_then(bench),
        _ => Err(USAGE.to_string()),
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::from(2)
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Text,
    Parquet,
    Arrow,
}

impl Format {
    fn parse(s: &str) -> Result<Format, String> {
        match s {
            "text" => Ok(Format::Text),
            "parquet" => Ok(Format::Parquet),
            "arrow" | "none" => Ok(Format::Arrow),
            _ => Err(format!("unknown format {s}: text, parquet or arrow")),
        }
    }
}

/// Generator name, cargo package, binary, and what `--scale 1` means.
const GENERATORS: &[(&str, &str, &str, &str)] = &[
    ("tpch", "populate-tpch", "tpch-gen", "SF 1"),
    ("tpcds", "populate-tpcds", "tpcds-gen", "SF 1"),
    ("dsb", "populate-dsb", "dsb-gen", "SF 1 (whole SFs only)"),
    ("ssb", "populate-ssb", "ssb-gen", "SF 1"),
    ("h2o-groupby", "populate-h2o", "h2o-gen", "1e7 rows"),
    (
        "h2o-join",
        "populate-h2o",
        "h2o-gen",
        "1e7 rows (multiples of 1e7 only)",
    ),
    ("tpcc", "populate-tpcc", "tpcc-gen", "10 warehouses"),
    ("ycsb-load", "populate-ycsb", "ycsb-gen", "1M records"),
    (
        "ycsb-run",
        "populate-ycsb",
        "ycsb-gen",
        "10M workload A operations",
    ),
    ("synth-taxi", "populate-synth", "synth-gen", "10M rows"),
    ("synth-hits", "populate-synth", "synth-gen", "10M rows"),
    ("synth-features", "populate-synth", "synth-gen", "10M rows"),
    (
        "synth-embeddings",
        "populate-synth",
        "synth-gen",
        "1M rows of 384 floats",
    ),
    (
        "tsbs-devops",
        "populate-tsbs",
        "tsbs-gen",
        "100 hosts, one day",
    ),
    (
        "ldbc",
        "populate-ldbc",
        "ldbc-gen",
        "SF 1 (needs the datagen resources)",
    ),
];

struct Opts {
    scales: Vec<f64>,
    threads: Vec<usize>,
    generators: Vec<&'static str>,
    formats: Vec<Format>,
    runs: usize,
    out: PathBuf,
    tpcgen_cli: Option<PathBuf>,
    tsbs_ref: Option<PathBuf>,
    ssb_dbgen: Option<PathBuf>,
    ssb_dists: Option<PathBuf>,
    list: bool,
    json: Option<PathBuf>,
}

fn list(s: &str) -> impl Iterator<Item = &str> {
    s.split([',', ' ']).filter(|x| !x.is_empty())
}

fn num<T: std::str::FromStr>(flag: &str, s: &str) -> Result<T, String> {
    s.parse().map_err(|_| format!("{flag}: not a number: {s}"))
}

fn parse(a: &[String]) -> Result<Opts, String> {
    let cores = available_parallelism().map_or(1, |n| n.get());
    let shm = Path::new("/dev/shm");
    let mut o = Opts {
        scales: vec![1.0],
        threads: if cores > 1 { vec![1, cores] } else { vec![1] },
        generators: GENERATORS.iter().map(|g| g.0).collect(),
        formats: vec![Format::Text, Format::Parquet, Format::Arrow],
        runs: 3,
        out: if shm.is_dir() {
            shm.to_path_buf()
        } else {
            temp_dir()
        },
        tpcgen_cli: which("tpcgen-cli"),
        tsbs_ref: which("tsbs_generate_data"),
        ssb_dbgen: which("ssb-dbgen"),
        ssb_dists: None,
        list: false,
        json: None,
    };
    let mut it = a.iter();
    while let Some(flag) = it.next() {
        let mut val = || {
            it.next()
                .map(String::as_str)
                .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--scale" | "--sf" => {
                o.scales = list(val()?)
                    .map(|s| num(flag, s))
                    .collect::<Result<_, _>>()?
            }
            "--threads" => {
                o.threads = list(val()?)
                    .map(|s| num(flag, s))
                    .collect::<Result<_, _>>()?
            }
            "--generators" => {
                let v = val()?;
                o.generators = Vec::new();
                for g in list(v) {
                    let hits: Vec<_> = GENERATORS
                        .iter()
                        .map(|x| x.0)
                        .filter(|n| g == "all" || *n == g || n.starts_with(&format!("{g}-")))
                        .collect();
                    if hits.is_empty() {
                        return Err(format!("unknown generator {g} (see --list)"));
                    }
                    o.generators.extend(hits);
                }
            }
            "--formats" => o.formats = list(val()?).map(Format::parse).collect::<Result<_, _>>()?,
            "--runs" => o.runs = num(flag, val()?)?,
            "--out" => o.out = PathBuf::from(val()?),
            "--json" => o.json = Some(PathBuf::from(val()?)),
            "--tpcgen-cli" => o.tpcgen_cli = Some(PathBuf::from(val()?)),
            "--tsbs-generate-data" => o.tsbs_ref = Some(PathBuf::from(val()?)),
            "--ssb-dbgen" => o.ssb_dbgen = Some(PathBuf::from(val()?)),
            "--ssb-dists" => o.ssb_dists = Some(PathBuf::from(val()?)),
            "--no-reference" => {
                o.tpcgen_cli = None;
                o.tsbs_ref = None;
                o.ssb_dbgen = None;
            }
            "--list" => o.list = true,
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown flag {flag}\n{USAGE}")),
        }
    }
    if o.scales.is_empty()
        || o.threads.is_empty()
        || o.formats.is_empty()
        || o.generators.is_empty()
        || o.scales.iter().any(|s| !s.is_finite() || *s <= 0.0)
        || o.threads.contains(&0)
        || o.runs == 0
    {
        return Err("selections must be nonempty; --scale must be finite; --scale, --threads and --runs must be > 0".into());
    }
    o.out = o.out.join("populate-bench");
    if let Some(p) = &o.ssb_dbgen
        && !o.list
        && o.generators.contains(&"ssb")
        && o.formats.contains(&Format::Text)
    {
        if o.ssb_dists.is_none() {
            o.ssb_dists = p.parent().and_then(|dir| {
                [dir.join("dists.dss"), dir.join("../dists.dss")]
                    .into_iter()
                    .find(|p| p.is_file())
            });
        }
        if o.ssb_dists.as_ref().is_none_or(|p| !p.is_file()) {
            return Err("SSB reference needs dists.dss; pass --ssb-dists PATH".into());
        }
    }
    Ok(o)
}

fn which(name: &str) -> Option<PathBuf> {
    split_paths(&var_os("PATH")?)
        .map(|d| d.join(name))
        .find(|p| p.is_file())
}

/// One process to time. `stdout` is a file name inside the output directory.
struct Cmd {
    program: PathBuf,
    args: Vec<String>,
    stdout: Option<&'static str>,
    env: BTreeMap<String, String>,
}

impl Cmd {
    fn new(program: &Path, args: &[&str]) -> Cmd {
        Cmd {
            program: program.to_path_buf(),
            args: args.iter().map(|s| s.to_string()).collect(),
            stdout: None,
            env: BTreeMap::new(),
        }
    }

    fn arg(mut self, a: impl ToString) -> Cmd {
        self.args.push(a.to_string());
        self
    }

    fn key(&self) -> String {
        format!("{} {:?} {:?}", self.program.display(), self.args, self.env)
    }
}

struct Case {
    size: String,
    format: &'static str,
    ours: Cmd,
    /// Reference tool name and command.
    reference: Option<(&'static str, Cmd)>,
}

/// A count as 1M / 100k / 12.
fn human(n: u64) -> String {
    match n {
        _ if n >= 1_000_000 && n.is_multiple_of(1_000_000) => format!("{}M", n / 1_000_000),
        _ if n >= 1_000 && n.is_multiple_of(1_000) => format!("{}k", n / 1_000),
        _ => n.to_string(),
    }
}

fn scaled(base: f64, scale: f64) -> u64 {
    ((base * scale).round() as u64).max(1)
}

/// The command lines for one generator, scale, format and thread count, or
/// `None` where the generator has no such format.
fn case(o: &Opts, bin: &Path, g: &str, scale: f64, f: Format, threads: usize) -> Option<Case> {
    let out = o.out.to_str().expect("output path is not UTF-8");
    let t = threads.to_string();
    let exe = |b: &str| bin.join(b);
    let fmt3 = |text: &'static str| match f {
        Format::Text => text,
        Format::Parquet => "parquet",
        Format::Arrow => "none",
    };
    let with_out = |c: Cmd, ff: &str| match f {
        Format::Arrow => c.arg("--format").arg("none"),
        _ => c.arg("--format").arg(ff).arg("--out").arg(out),
    };
    // the four TPC-style generators share their flags
    let tpc = |b: &str, sf: f64, text: &'static str| {
        let ff = fmt3(text);
        let c = Cmd::new(&exe(b), &["--sf", &sf.to_string(), "--threads", &t]);
        (format!("SF {sf}"), ff, with_out(c, ff))
    };
    let tpcgen = |suite: &str, ff: &str, sf: f64| {
        let p = o.tpcgen_cli.as_ref().filter(|_| f != Format::Arrow)?;
        let c = Cmd::new(p, &[suite, ff, "-s", &sf.to_string(), "--output-dir", out])
            .arg("--num-threads")
            .arg(&t);
        Some(("tpcgen-cli", c))
    };
    Some(match g {
        "tpch" => {
            let (size, ff, ours) = tpc("tpch-gen", scale, "tbl");
            let reference = tpcgen("tpch", ff, scale);
            Case {
                size,
                format: ff,
                ours,
                reference,
            }
        }
        "tpcds" => {
            let (size, ff, mut ours) = tpc("tpcds-gen", scale, "dat");
            // tpcgen-cli writes UTF-8; dsdgen's default is Latin-1
            if f == Format::Text {
                ours = ours.arg("--encoding").arg("utf8");
            }
            let reference = tpcgen("tpcds", ff, scale);
            Case {
                size,
                format: ff,
                ours,
                reference,
            }
        }
        "dsb" => {
            let (size, ff, ours) = tpc("dsb-gen", scale.round().max(1.0), "dat");
            Case {
                size,
                format: ff,
                ours,
                reference: None,
            }
        }
        "ssb" => {
            let (size, ff, ours) = tpc("ssb-gen", scale, "tbl");
            let reference = o.ssb_dbgen.as_ref().filter(|_| f == Format::Text).map(|p| {
                let mut c = Cmd::new(p, &["-q", "-f", "-s", &scale.to_string(), "-b"]).arg(
                    o.ssb_dists
                        .as_ref()
                        .expect("validated distributions")
                        .display(),
                );
                c.env.insert("DSS_PATH".into(), out.into());
                ("ssb-dbgen", c)
            });
            Case {
                size,
                format: ff,
                ours,
                reference,
            }
        }
        "h2o-groupby" | "h2o-join" => {
            let join = g == "h2o-join";
            let n = if join {
                scaled(1.0, scale) * 10_000_000
            } else {
                scaled(1e7, scale)
            };
            let ff = fmt3("csv");
            let c = Cmd::new(&exe("h2o-gen"), &[if join { "join" } else { "groupby" }])
                .arg("--n")
                .arg(n)
                .arg("--threads")
                .arg(&t);
            Case {
                size: format!("{} rows", human(n)),
                format: ff,
                ours: with_out(c, ff),
                reference: None,
            }
        }
        "tpcc" => {
            let w = scaled(10.0, scale);
            let ff = fmt3("text");
            let c = Cmd::new(
                &exe("tpcc-gen"),
                &["--warehouses", &w.to_string(), "--threads", &t],
            );
            Case {
                size: format!("{w} wh"),
                format: ff,
                ours: with_out(c, ff),
                reference: None,
            }
        }
        "ycsb-load" | "ycsb-run" => {
            let records = scaled(1e6, scale);
            let ff = fmt3("text");
            let (size, c) = if g == "ycsb-load" {
                let c = Cmd::new(
                    &exe("ycsb-gen"),
                    &["load", "--records", &records.to_string()],
                );
                (format!("{} records", human(records)), c)
            } else {
                let ops = scaled(1e7, scale);
                let c = Cmd::new(&exe("ycsb-gen"), &["run", "--workload", "a"])
                    .arg("--records")
                    .arg(records)
                    .arg("--operations")
                    .arg(ops);
                (format!("{} ops", human(ops)), c)
            };
            let c = c.arg("--threads").arg(&t);
            Case {
                size,
                format: ff,
                ours: with_out(c, ff),
                reference: None,
            }
        }
        "synth-taxi" | "synth-hits" | "synth-features" | "synth-embeddings" => {
            let shape = &g["synth-".len()..];
            let rows = scaled(if shape == "embeddings" { 1e6 } else { 1e7 }, scale);
            let c = Cmd::new(
                &exe("synth-gen"),
                &[shape, "--rows", &rows.to_string(), "--threads", &t],
            );
            let (ff, c) = match f {
                Format::Text => return None,
                Format::Parquet => (
                    "parquet",
                    c.arg("--out").arg(format!("{out}/{shape}.parquet")),
                ),
                Format::Arrow => ("none", c),
            };
            Case {
                size: format!("{} rows", human(rows)),
                format: ff,
                ours: c,
                reference: None,
            }
        }
        "tsbs-devops" => {
            let hosts = scaled(100.0, scale);
            let common = [
                "--use-case=devops".to_string(),
                format!("--scale={hosts}"),
                "--seed=123".into(),
                "--timestamp-start=2016-01-01T00:00:00Z".into(),
                "--timestamp-end=2016-01-02T00:00:00Z".into(),
                "--log-interval=10s".into(),
            ];
            let mut c = Cmd::new(&exe("tsbs-gen"), &[]);
            c.args.extend(common.iter().cloned());
            let c = c.arg("--threads").arg(&t);
            let (ff, ours, reference) = match f {
                Format::Text => {
                    let mut ours = c.arg("--format=influx");
                    ours.stdout = Some("data.influx");
                    // Go's generator is single-threaded and takes no --threads
                    let reference = o.tsbs_ref.as_ref().map(|p| {
                        let mut r = Cmd::new(p, &[]);
                        r.args.extend(common.iter().cloned());
                        r = r.arg("--format=influx");
                        r.stdout = Some("data.influx");
                        ("tsbs_generate_data", r)
                    });
                    ("influx", ours, reference)
                }
                Format::Parquet => (
                    "parquet",
                    c.arg("--format=parquet").arg("--out").arg(out),
                    None,
                ),
                Format::Arrow => ("none", c.arg("--format=arrow"), None),
            };
            Case {
                size: format!("{hosts} hosts"),
                format: ff,
                ours,
                reference,
            }
        }
        "ldbc" => {
            let ff = fmt3("csv");
            let c = Cmd::new(
                &exe("ldbc-gen"),
                &["--sf", &scale.to_string(), "--threads", &t],
            );
            Case {
                size: format!("SF {scale}"),
                format: ff,
                ours: with_out(c, ff),
                reference: None,
            }
        }
        _ => unreachable!("{g}"),
    })
}

/// Total size of the files under `p`.
fn dir_size(p: &Path) -> u64 {
    let Ok(rd) = read_dir(p) else { return 0 };
    rd.flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_size(&e.path()),
            _ => e.metadata().map_or(0, |m| m.len()),
        })
        .sum()
}

/// Wall time and output bytes of one run, output wiped before and after.
fn run(c: &Cmd, out: &Path) -> Result<(Duration, u64), String> {
    let _ = remove_dir_all(out);
    create_dir_all(out).map_err(|e| format!("{}: {e}", out.display()))?;
    let stdout = match c.stdout {
        Some(name) => Stdio::from(File::create(out.join(name)).map_err(|e| e.to_string())?),
        None => Stdio::null(),
    };
    let t0 = Instant::now();
    let mut child = Command::new(&c.program)
        .args(&c.args)
        .envs(&c.env)
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{}: {e}", c.program.display()))?;
    let mut err = String::new();
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut err);
    }
    let status = child.wait().map_err(|e| e.to_string())?;
    let elapsed = t0.elapsed();
    let bytes = dir_size(out);
    let _ = remove_dir_all(out);
    if status.success() {
        Ok((elapsed, bytes))
    } else {
        let tail: Vec<&str> = err.lines().rev().take(3).collect();
        let tail: Vec<&str> = tail.into_iter().rev().collect();
        Err(format!(
            "{} failed ({status}): {}",
            c.key(),
            tail.join(" / ")
        ))
    }
}

/// Best of `runs`, or the first error.
fn best(times: &[Result<(Duration, u64), String>]) -> Result<(Duration, u64), String> {
    let mut b: Option<(Duration, u64)> = None;
    for t in times {
        let t = t.clone()?;
        if b.is_none_or(|b| t.0 < b.0) {
            b = Some(t);
        }
    }
    Ok(b.expect("at least one run"))
}

fn secs(d: Duration) -> String {
    format!("{:.2}", d.as_secs_f64())
}

fn mb(bytes: u64) -> String {
    if bytes == 0 {
        "-".into()
    } else {
        format!("{:.0}", bytes as f64 / 1e6)
    }
}

fn bench(o: Opts) -> Result<(), String> {
    if o.list {
        println!("generator         --scale 1 means");
        for (name, _, _, unit) in GENERATORS {
            println!("{name:<17} {unit}");
        }
        return Ok(());
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the workspace root");
    let target = match var_os("CARGO_TARGET_DIR") {
        Some(p) => current_dir().map_err(|e| e.to_string())?.join(p),
        None => root.join("target"),
    };
    let bin = target.join("release");
    let mut report = Report::new(root, &o)?;

    let mut wanted: Vec<&(&str, &str, &str, &str)> = GENERATORS
        .iter()
        .filter(|g| o.generators.contains(&g.0))
        .collect();
    let ldbc_resources = var_os("LDBC_DATAGEN_RESOURCES")
        .map(PathBuf::from)
        .unwrap_or_else(|| target.join("ldbc_snb_datagen_spark/src/main/resources"));
    if wanted.iter().any(|g| g.0 == "ldbc") && !ldbc_resources.join("dictionaries").is_dir() {
        eprintln!(
            "skipping ldbc: no datagen resources (run scripts/fetch-ldbc-resources.sh, \
             or set LDBC_DATAGEN_RESOURCES)"
        );
        wanted.retain(|g| g.0 != "ldbc");
        report.skip("populate", Some("ldbc"), "datagen resources unavailable");
    }
    if wanted.is_empty() {
        report.save(false)?;
        return Err("no generators to run".into());
    }

    let cargo = var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut build = Command::new(cargo);
    build
        .current_dir(root)
        .args(["build", "--release", "--quiet"]);
    let mut seen = Vec::new();
    for (_, pkg, b, _) in &wanted {
        if !seen.contains(b) {
            seen.push(*b);
            build.args(["-p", pkg, "--bin", b]);
        }
    }
    eprintln!("building {} in release mode", seen.join(", "));
    let ok = build.status().map_err(|e| format!("cargo: {e}"))?.success();
    if !ok {
        return Err("cargo build failed".into());
    }

    for (tool, p) in [
        ("tpcgen-cli", &o.tpcgen_cli),
        ("tsbs_generate_data", &o.tsbs_ref),
        ("ssb-dbgen", &o.ssb_dbgen),
    ] {
        match p {
            Some(p) => eprintln!("reference: {tool} at {}", p.display()),
            None => {
                eprintln!("reference: {tool} not found, skipped");
                report.skip(tool, None, "reference not installed or disabled");
            }
        }
    }

    // a reference tool without a thread flag runs once per configuration
    let mut ref_cache = HashMap::new();
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    for &scale in &o.scales {
        for g in &wanted {
            for &f in &o.formats {
                for &t in &o.threads {
                    let Some(c) = case(&o, &bin, g.0, scale, f, t) else {
                        report.skip("populate", Some(g.0), "requested format unsupported");
                        continue;
                    };
                    eprintln!("{} {} {} {} threads", g.0, c.size, c.format, t);
                    let cached = c
                        .reference
                        .as_ref()
                        .and_then(|(_, r)| ref_cache.get(&r.key()).cloned());
                    let mut ours = Vec::new();
                    let mut theirs = Vec::new();
                    for _ in 0..o.runs {
                        if let (Some((_, r)), None) = (&c.reference, &cached) {
                            theirs.push(run(r, &o.out));
                        }
                        ours.push(run(&c.ours, &o.out));
                    }
                    report.measurement("populate", (g.0, scale, t), &c, &c.ours, &ours);
                    let ours = best(&ours);
                    let theirs = match (&c.reference, cached) {
                        (Some(_), Some(r)) => Some(r),
                        (Some((name, r)), None) => {
                            // These references have no thread flag: cache the actual
                            // single-threaded measurement, not a copy for each t.
                            let threads = if matches!(*name, "tsbs_generate_data" | "ssb-dbgen") {
                                1
                            } else {
                                t
                            };
                            report.measurement(name, (g.0, scale, threads), &c, r, &theirs);
                            let b = best(&theirs);
                            ref_cache.insert(r.key(), b.clone());
                            Some(b)
                        }
                        (None, _) => None,
                    };
                    let mut row = vec![
                        g.0.to_string(),
                        c.size.clone(),
                        // `none` is the generators' flag for Arrow batches dropped in memory
                        if c.format == "none" {
                            "arrow".into()
                        } else {
                            c.format.into()
                        },
                        t.to_string(),
                    ];
                    match &ours {
                        Ok((d, b)) => row.extend([secs(*d), mb(*b)]),
                        Err(e) => {
                            errors.push(e.clone());
                            row.extend(["error".into(), String::new()]);
                        }
                    }
                    match (&c.reference, theirs) {
                        (Some((name, _)), Some(Ok((d, b)))) => {
                            row.extend([name.to_string(), secs(d), mb(b)]);
                            row.push(match &ours {
                                Ok((o, _)) => format!("{:.1}x", d.as_secs_f64() / o.as_secs_f64()),
                                Err(_) => String::new(),
                            });
                        }
                        (Some((name, _)), Some(Err(e))) => {
                            errors.push(e);
                            row.extend([
                                name.to_string(),
                                "error".into(),
                                String::new(),
                                String::new(),
                            ]);
                        }
                        _ => {
                            row.extend([String::new(), String::new(), String::new(), String::new()])
                        }
                    }
                    rows.push(row);
                    report.save(false)?;
                }
            }
        }
    }
    let _ = remove_dir_all(&o.out);
    report.save(true)?;

    println!();
    println!(
        "Best of {} run(s), output to {}. Speedup is reference time / populate time.",
        o.runs,
        o.out.parent().unwrap_or(&o.out).display()
    );
    println!();
    println!(
        "| generator | size | format | threads | populate (s) | MB | reference | reference (s) | reference MB | speedup |"
    );
    println!("|---|---|---|---:|---:|---:|---|---:|---:|---:|");
    for r in rows {
        println!("| {} |", r.join(" | "));
    }
    if !errors.is_empty() {
        println!();
        for e in &errors {
            println!("- {e}");
        }
        return Err(format!("{} run(s) failed", errors.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssb_reference_uses_one_cached_command_and_records_output_environment() {
        let mut o = parse(&["--no-reference".into()]).unwrap();
        o.ssb_dbgen = Some("reference/dbgen".into());
        o.ssb_dists = Some("reference/dists.dss".into());
        let a = case(&o, Path::new("bin"), "ssb", 1.0, Format::Text, 1).unwrap();
        let b = case(&o, Path::new("bin"), "ssb", 1.0, Format::Text, 14).unwrap();
        let (_, a) = a.reference.unwrap();
        let (_, b) = b.reference.unwrap();
        assert_eq!(a.key(), b.key());
        assert_eq!(a.env["DSS_PATH"], o.out.to_str().unwrap());
        assert!(
            a.args
                .ends_with(&["-b".into(), "reference/dists.dss".into()])
        );
        assert!(
            case(&o, Path::new("bin"), "ssb", 1.0, Format::Parquet, 1)
                .unwrap()
                .reference
                .is_none()
        );
        assert!(parse(&["--ssb-dbgen".into(), "/nonexistent/dbgen".into()]).is_err());
        assert!(
            parse(&[
                "--ssb-dbgen".into(),
                "/nonexistent/dbgen".into(),
                "--formats".into(),
                "parquet".into(),
            ])
            .is_ok()
        );
    }

    #[test]
    fn rejects_nonfinite_scales_and_empty_selections() {
        for (flag, value) in [
            ("--scale", "inf"),
            ("--scale", "NaN"),
            ("--scale", "0"),
            ("--scale", ""),
            ("--threads", ""),
            ("--generators", ""),
            ("--formats", ""),
            ("--threads", "0"),
            ("--runs", "0"),
        ] {
            assert!(
                parse(&[flag.into(), value.into()]).is_err(),
                "{flag} {value}"
            );
        }
    }
}
