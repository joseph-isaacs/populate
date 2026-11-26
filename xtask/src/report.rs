//! Engine-neutral benchmark interchange format, written by `--json`.
//! The types below are the format: `cargo xtask bench-schema` prints
//! their JSON Schema, and BENCHMARKS.md describes the fields.

use std::collections::BTreeMap;
use std::env::consts::{ARCH, OS};
use std::env::{args, current_dir, var};
use std::fs::{create_dir_all, read_to_string, rename, write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::available_parallelism;
use std::time::{SystemTime, UNIX_EPOCH};

use schemars::JsonSchema;
use serde::Serialize;

use crate::stats::{Sample, summarize};
use crate::{Cmd, Format, Opts};

/// One `cargo xtask bench --json` run.
#[derive(Clone, Serialize, JsonSchema)]
#[schemars(title = "populate benchmark report")]
pub(super) struct Data {
    /// Format version; 1 is the only one.
    schema_version: u32,
    started_at_unix_seconds: f64,
    finished_at_unix_seconds: Option<f64>,
    /// `false` while running or when interrupted; `true` once the whole
    /// matrix ran, even if samples failed or tools were skipped.
    complete: bool,
    source: Source,
    environment: Environment,
    /// The command line, program name first.
    invocation: Vec<String>,
    configuration: Configuration,
    specification: Specification,
    engines: Vec<Engine>,
    measurements: Vec<Measurement>,
    skipped: Vec<Skip>,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Source {
    revision: Option<String>,
    worktree_dirty: Option<bool>,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Environment {
    os: String,
    architecture: String,
    os_version: Option<String>,
    cpu: Option<String>,
    memory_bytes: Option<u64>,
    logical_cpus: Option<usize>,
    rustc: Option<String>,
    cargo: Option<String>,
    rustflags: Option<String>,
    cargo_encoded_rustflags: Option<String>,
    build_profile: String,
    output_directory: PathBuf,
    ldbc_resources: Option<String>,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Configuration {
    scales: Vec<f64>,
    threads: Vec<usize>,
    generators: Vec<String>,
    formats: Vec<String>,
    /// Minimum timed attempts per engine and configuration.
    runs: usize,
    warmup_runs: usize,
    statistic: String,
    max_runs: usize,
    /// More attempts while `(max - min) / median` exceeds this.
    noise_threshold: f64,
    sync_output: bool,
}

/// How the numbers were taken, in words.
#[derive(Clone, Serialize, JsonSchema)]
struct Specification {
    methodology: Methodology,
    storage: Storage,
    validation: Vec<Validation>,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Methodology {
    sampling: String,
    wall_time: String,
    arrow: String,
    flush: String,
    warmups: String,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Storage {
    output_directory: PathBuf,
    sync_output: bool,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Validation {
    compared: String,
    text: String,
    parquet: String,
    result: String,
}

#[derive(Clone, Serialize, JsonSchema)]
struct Engine {
    id: String,
    version: Option<String>,
    revision: Option<String>,
}

/// One engine at one configuration.
#[derive(Clone, Serialize, JsonSchema)]
struct Measurement {
    /// An `engines` id.
    engine: String,
    workload: String,
    scale: f64,
    /// Display label of the resolved size; `command.args` is exact.
    size: String,
    format: String,
    threads: usize,
    sink: Sink,
    validation_status: ValidationStatus,
    command: CommandRecord,
    /// Every timed attempt, in order.
    samples: Vec<SampleRecord>,
    /// `null` when any attempt failed.
    summary: Option<SummaryRecord>,
}

#[derive(Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Sink {
    /// Arrow batches generated and dropped in memory.
    Memory,
    File,
}

/// The output comparison against the reference.
#[derive(Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum ValidationStatus {
    Passed,
    /// The outputs differ; the run fails.
    Mismatch,
    NotRun,
}

#[derive(Clone, Serialize, JsonSchema)]
struct CommandRecord {
    program: PathBuf,
    args: Vec<String>,
    stdout_file: Option<String>,
    /// Environment overrides.
    env: BTreeMap<String, String>,
}

#[derive(Clone, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
enum SampleRecord {
    Ok {
        /// Process start to exit.
        wall_time_seconds: f64,
        output_bytes: u64,
        /// File sync after the timed run, with `--sync-output`.
        flush_time_seconds: Option<f64>,
        flushed_wall_time_seconds: Option<f64>,
    },
    Error {
        error: String,
    },
}

#[derive(Clone, Serialize, JsonSchema)]
struct SummaryRecord {
    statistic: String,
    /// Median.
    wall_time_seconds: f64,
    min_wall_time_seconds: f64,
    max_wall_time_seconds: f64,
    /// Median, rounded down for an even count.
    output_bytes: u64,
    flush_time_seconds: Option<f64>,
    /// Median of the per-sample totals.
    flushed_wall_time_seconds: Option<f64>,
}

#[derive(Clone, PartialEq, Serialize, JsonSchema)]
struct Skip {
    engine: String,
    workload: Option<String>,
    reason: String,
}

/// The report's JSON Schema, for `cargo xtask bench-schema`.
pub(super) fn schema() -> String {
    let s = schemars::schema_for!(Data);
    serde_json::to_string_pretty(&s).expect("a schema serializes")
}

pub(super) struct Report {
    path: Option<PathBuf>,
    data: Data,
}

fn output(root: &Path, program: &str, arguments: &[&str]) -> Option<String> {
    let o = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .ok()?;
    o.status
        .success()
        .then(|| String::from_utf8_lossy(&o.stdout).trim().to_owned())
}

fn unix_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn samples(times: &[Result<Sample, String>]) -> Vec<SampleRecord> {
    times
        .iter()
        .map(|r| match r {
            Ok(s) => SampleRecord::Ok {
                wall_time_seconds: s.wall.as_secs_f64(),
                output_bytes: s.bytes,
                flush_time_seconds: s.flush.map(|d| d.as_secs_f64()),
                flushed_wall_time_seconds: s.flush.map(|d| (s.wall + d).as_secs_f64()),
            },
            Err(e) => SampleRecord::Error { error: e.clone() },
        })
        .collect()
}

fn summary(times: &[Result<Sample, String>]) -> Option<SummaryRecord> {
    summarize(times).ok().map(|s| SummaryRecord {
        statistic: "median".into(),
        wall_time_seconds: s.wall.as_secs_f64(),
        min_wall_time_seconds: s.min.as_secs_f64(),
        max_wall_time_seconds: s.max.as_secs_f64(),
        output_bytes: s.bytes,
        flush_time_seconds: s.flush.map(|d| d.as_secs_f64()),
        flushed_wall_time_seconds: s.flushed_wall.map(|d| d.as_secs_f64()),
    })
}

impl Report {
    pub(super) fn new(root: &Path, o: &Opts) -> Result<Self, String> {
        let path = if let Some(path) = &o.json {
            let path = current_dir().map_err(|e| e.to_string())?.join(path);
            let parent = path.parent().ok_or("--json needs a file path")?;
            create_dir_all(parent).map_err(|e| format!("--json: {e}"))?;
            create_dir_all(&o.out).map_err(|e| format!("--out: {e}"))?;
            let parent = parent.canonicalize().map_err(|e| e.to_string())?;
            let out = o.out.canonicalize().map_err(|e| e.to_string())?;
            if parent.starts_with(out) {
                return Err(
                    "--json must be outside the temporary --out/populate-bench directory".into(),
                );
            }
            Some(parent.join(path.file_name().ok_or("--json needs a file name")?))
        } else {
            None
        };
        let revision = output(root, "git", &["rev-parse", "HEAD"]);
        let dirty = output(root, "git", &["status", "--porcelain"]).map(|s| !s.is_empty());
        let (cpu, memory) = match OS {
            "macos" => (
                output(root, "sysctl", &["-n", "machdep.cpu.brand_string"]),
                output(root, "sysctl", &["-n", "hw.memsize"]).and_then(|s| s.parse::<u64>().ok()),
            ),
            "linux" => (
                read_to_string("/proc/cpuinfo").ok().and_then(|s| {
                    s.lines().find_map(|l| {
                        l.strip_prefix("model name")
                            .and_then(|l| l.split_once(':'))
                            .map(|(_, v)| v.trim().to_string())
                    })
                }),
                read_to_string("/proc/meminfo").ok().and_then(|s| {
                    s.lines().find_map(|l| {
                        l.strip_prefix("MemTotal:")
                            .and_then(|v| v.split_whitespace().next())
                            .and_then(|v| v.parse::<u64>().ok())
                            .map(|kb| kb * 1024)
                    })
                }),
            ),
            _ => (None, None),
        };
        let mut engines = vec![Engine {
            id: "populate".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
            revision: revision.clone(),
        }];
        for (id, p) in [
            ("tpcgen-cli", &o.tpcgen_cli),
            ("tsbs_generate_data", &o.tsbs_ref),
            ("ssb-dbgen", &o.ssb_dbgen),
        ] {
            if p.is_some() {
                engines.push(Engine {
                    id: id.into(),
                    version: None,
                    revision: None,
                });
            }
        }
        let text = |s: &str| s.to_string();
        let report = Self {
            path,
            data: Data {
                schema_version: 1,
                started_at_unix_seconds: unix_seconds(),
                finished_at_unix_seconds: None,
                complete: false,
                source: Source {
                    revision,
                    worktree_dirty: dirty,
                },
                environment: Environment {
                    os: OS.into(),
                    architecture: ARCH.into(),
                    os_version: output(root, "uname", &["-srv"]),
                    cpu,
                    memory_bytes: memory,
                    logical_cpus: available_parallelism().ok().map(|n| n.get()),
                    rustc: output(root, "rustc", &["-Vv"]),
                    cargo: output(root, "cargo", &["-V"]),
                    rustflags: var("RUSTFLAGS").ok(),
                    cargo_encoded_rustflags: var("CARGO_ENCODED_RUSTFLAGS").ok(),
                    build_profile: "release".into(),
                    output_directory: o.out.clone(),
                    ldbc_resources: var("LDBC_DATAGEN_RESOURCES").ok(),
                },
                invocation: args().collect(),
                configuration: Configuration {
                    scales: o.scales.clone(),
                    threads: o.threads.clone(),
                    generators: o.generators.iter().map(|g| g.to_string()).collect(),
                    formats: o
                        .formats
                        .iter()
                        .map(|f| match f {
                            Format::Text => "text",
                            Format::Parquet => "parquet",
                            Format::Arrow => "arrow",
                        })
                        .map(text)
                        .collect(),
                    runs: o.runs,
                    warmup_runs: 0,
                    statistic: "median".into(),
                    max_runs: o.max_runs,
                    noise_threshold: o.noise_threshold,
                    sync_output: o.sync_output,
                },
                specification: Specification {
                    methodology: Methodology {
                        sampling: text(
                            "Reverse engine order each trial; extend only noisy engines, up to max_runs. Failures stop retries and invalidate the summary.",
                        ),
                        wall_time: text(
                            "Process startup through exit, including file close; excludes final fsync and cleanup.",
                        ),
                        arrow: text(
                            "Generate and drop RecordBatches in memory; no Arrow IPC serialization.",
                        ),
                        flush: text(
                            "Optional regular-file sync_all after timing. Flushed totals are computed per sample before taking the median; directory metadata is not synced.",
                        ),
                        warmups: text(
                            "No separate warmups or global page-cache drops. Where a reference runs, populate first runs once untimed: the reference takes its settings from that output, which is compared with the reference's first timed run.",
                        ),
                    },
                    storage: Storage {
                        output_directory: o.out.clone(),
                        sync_output: o.sync_output,
                    },
                    validation: vec![Validation {
                        compared: text(
                            "populate's untimed output against the reference's first timed run, per uncached reference configuration",
                        ),
                        text: text("byte for byte"),
                        parquet: text(
                            "row count, Arrow schema, decoded values, codecs and dictionary use",
                        ),
                        result: text(
                            "measurement validation_status: passed, mismatch (the run fails) or not_run",
                        ),
                    }],
                },
                engines,
                measurements: Vec::new(),
                skipped: Vec::new(),
            },
        };
        // Fail on an unwritable report before starting the expensive build/run.
        report.save(false)?;
        Ok(report)
    }

    pub(super) fn skip(&mut self, engine: &str, workload: Option<&str>, reason: &str) {
        let entry = Skip {
            engine: engine.into(),
            workload: workload.map(Into::into),
            reason: reason.into(),
        };
        if !self.data.skipped.contains(&entry) {
            self.data.skipped.push(entry);
        }
    }

    pub(super) fn measurement(
        &mut self,
        engine: &str,
        (workload, scale, threads): (&str, f64, usize),
        (size, format, validation): (&str, &str, ValidationStatus),
        cmd: &Cmd,
        times: &[Result<Sample, String>],
    ) {
        let memory = format == "none";
        self.data.measurements.push(Measurement {
            engine: engine.into(),
            workload: workload.into(),
            scale,
            size: size.into(),
            format: if memory { "arrow" } else { format }.into(),
            threads,
            sink: if memory { Sink::Memory } else { Sink::File },
            validation_status: validation,
            command: CommandRecord {
                program: cmd.program.clone(),
                args: cmd.args.clone(),
                stdout_file: cmd.stdout.map(Into::into),
                env: cmd.env.clone(),
            },
            samples: samples(times),
            summary: summary(times),
        });
    }

    pub(super) fn save(&self, complete: bool) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let mut data = self.data.clone();
        data.complete = complete;
        if complete {
            data.finished_at_unix_seconds = Some(unix_seconds());
        }
        let bytes = serde_json::to_vec(&data).map_err(|e| e.to_string())?;
        let tmp = path.with_extension("json.tmp");
        write(&tmp, bytes)
            .and_then(|()| rename(&tmp, path))
            .map_err(|e| format!("{}: {e}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use std::env::temp_dir;
    use std::fs::remove_dir_all;
    use std::process::id;
    use std::time::Duration;

    use serde_json::Value;

    #[test]
    fn preserves_samples_and_reports_even_medians() {
        let times = vec![
            Ok(Sample {
                wall: Duration::from_micros(1234),
                bytes: 10,
                flush: None,
            }),
            Ok(Sample {
                wall: Duration::from_micros(1200),
                bytes: 20,
                flush: None,
            }),
        ];
        let data = serde_json::to_value(samples(&times)).unwrap();
        let summary = |t: &[Result<Sample, String>]| serde_json::to_value(summary(t)).unwrap();
        assert_eq!(data[0]["wall_time_seconds"], 0.001234);
        assert_eq!(data[1]["output_bytes"], 20);
        assert_eq!(summary(&times)["wall_time_seconds"], 0.001217);
        assert_eq!(summary(&times)["min_wall_time_seconds"], 0.0012);
        assert_eq!(summary(&times)["max_wall_time_seconds"], 0.001234);
        assert!(data[0]["flush_time_seconds"].is_null());
        assert_eq!(summary(&times)["output_bytes"], 15);
    }

    #[test]
    fn errors_are_not_zero_timings_or_successful_summaries() {
        let times = vec![
            Ok(Sample {
                wall: Duration::from_secs(1),
                bytes: 0,
                flush: None,
            }),
            Err("bad \"engine\"\nfailed".into()),
        ];
        let data = serde_json::to_value(samples(&times)).unwrap();
        let summary = |t: &[Result<Sample, String>]| serde_json::to_value(summary(t)).unwrap();
        assert_eq!(data[0]["output_bytes"], 0);
        assert_eq!(data[1]["status"], "error");
        assert!(data[1].get("wall_time_seconds").is_none());
        assert!(summary(&times).is_null());
        let serialized = serde_json::to_string(&data).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&serialized).unwrap(), data);
    }

    #[test]
    fn report_cannot_be_written_inside_the_cleaned_output_directory() {
        let dir = temp_dir().join(format!(
            "xtask-report-test-{}-{}",
            id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let o = parse(&[
            "--out".into(),
            dir.to_string_lossy().into_owned(),
            "--json".into(),
            dir.join("populate-bench/results.json")
                .to_string_lossy()
                .into_owned(),
        ])
        .unwrap();
        let result = Report::new(Path::new(env!("CARGO_MANIFEST_DIR")), &o);
        assert!(matches!(result, Err(e) if e.contains("must be outside")));
        remove_dir_all(dir).unwrap();
    }
}
