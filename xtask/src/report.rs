//! Engine-neutral benchmark interchange format; see benchmark-report.schema.json.

use std::env::consts::{ARCH, OS};
use std::env::{args, current_dir, var};
use std::fs::{create_dir_all, read_to_string, rename, write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::available_parallelism;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::{Case, Cmd, Format, Opts, best};

pub(super) struct Report {
    path: Option<PathBuf>,
    data: Value,
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

fn samples(times: &[Result<(Duration, u64), String>]) -> Value {
    Value::Array(
        times
            .iter()
            .map(|r| match r {
                Ok((d, bytes)) => json!({
                    "status": "ok", "wall_time_seconds": d.as_secs_f64(), "output_bytes": bytes
                }),
                Err(e) => json!({"status": "error", "error": e}),
            })
            .collect(),
    )
}

fn summary(times: &[Result<(Duration, u64), String>]) -> Value {
    match best(times) {
        Ok((d, bytes)) => json!({
            "statistic": "min", "wall_time_seconds": d.as_secs_f64(), "output_bytes": bytes
        }),
        Err(_) => Value::Null,
    }
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
        let mut engines = vec![json!({
            "id": "populate", "version": env!("CARGO_PKG_VERSION"), "revision": revision
        })];
        for (id, p) in [
            ("tpcgen-cli", &o.tpcgen_cli),
            ("tsbs_generate_data", &o.tsbs_ref),
        ] {
            if p.is_some() {
                engines.push(json!({"id": id, "version": null, "revision": null}));
            }
        }
        let report = Self {
            path,
            data: json!({
                "schema_version": 1,
                "started_at_unix_seconds": unix_seconds(),
                "finished_at_unix_seconds": null,
                "complete": false,
                "source": {"revision": revision, "worktree_dirty": dirty},
                "environment": {
                    "os": OS, "architecture": ARCH,
                    "os_version": output(root, "uname", &["-srv"]),
                    "cpu": cpu, "memory_bytes": memory,
                    "logical_cpus": available_parallelism().ok().map(|n| n.get()),
                    "rustc": output(root, "rustc", &["-Vv"]),
                    "cargo": output(root, "cargo", &["-V"]),
                    "rustflags": var("RUSTFLAGS").ok(),
                    "cargo_encoded_rustflags": var("CARGO_ENCODED_RUSTFLAGS").ok(),
                    "build_profile": "release",
                    "output_directory": o.out,
                    "ldbc_resources": var("LDBC_DATAGEN_RESOURCES").ok()
                },
                "invocation": args().collect::<Vec<_>>(),
                "configuration": {
                    "scales": o.scales, "threads": o.threads,
                    "generators": o.generators,
                    "formats": o.formats.iter().map(|f| match f {
                        Format::Text => "text", Format::Parquet => "parquet", Format::Arrow => "arrow"
                    }).collect::<Vec<_>>(),
                    "runs": o.runs,
                    "warmup_runs": 0,
                    "statistic": "min"
                },
                "engines": engines,
                "measurements": [],
                "skipped": []
            }),
        };
        // Fail on an unwritable report before starting the expensive build/run.
        report.save(false)?;
        Ok(report)
    }

    pub(super) fn skip(&mut self, engine: &str, workload: Option<&str>, reason: &str) {
        let entry = json!({"engine": engine, "workload": workload, "reason": reason});
        let skips = self.data["skipped"].as_array_mut().unwrap();
        if !skips.contains(&entry) {
            skips.push(entry);
        }
    }

    pub(super) fn measurement(
        &mut self,
        engine: &str,
        (workload, scale, threads): (&str, f64, usize),
        case: &Case,
        cmd: &Cmd,
        times: &[Result<(Duration, u64), String>],
    ) {
        self.data["measurements"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "engine": engine,
                "workload": workload,
                "scale": scale,
                "size": case.size,
                "format": if case.format == "none" { "arrow" } else { case.format },
                "threads": threads,
                "command": {"program": cmd.program, "args": cmd.args, "stdout_file": cmd.stdout},
                "samples": samples(times),
                "summary": summary(times)
            }));
    }

    pub(super) fn save(&self, complete: bool) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let mut data = self.data.clone();
        data["complete"] = json!(complete);
        if complete {
            data["finished_at_unix_seconds"] = json!(unix_seconds());
        }
        let bytes = serde_json::to_vec_pretty(&data).map_err(|e| e.to_string())?;
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

    #[test]
    fn preserves_samples_and_pairs_bytes_with_fastest_run() {
        let times = vec![
            Ok((Duration::from_micros(1234), 10)),
            Ok((Duration::from_micros(1200), 20)),
        ];
        let data = samples(&times);
        assert_eq!(data[0]["wall_time_seconds"], 0.001234);
        assert_eq!(data[1]["output_bytes"], 20);
        assert_eq!(summary(&times)["wall_time_seconds"], 0.0012);
        assert_eq!(summary(&times)["output_bytes"], 20);
    }

    #[test]
    fn errors_are_not_zero_timings_or_successful_summaries() {
        let times = vec![
            Ok((Duration::from_secs(1), 0)),
            Err("bad \"engine\"\nfailed".into()),
        ];
        let data = samples(&times);
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
