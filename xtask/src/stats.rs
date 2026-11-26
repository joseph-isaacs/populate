//! Summaries retain the observed range and never turn a failed attempt into a speedup.

use std::time::Duration;

#[derive(Clone, Copy)]
pub(super) struct Sample {
    pub wall: Duration,
    pub bytes: u64,
    pub flush: Option<Duration>,
}

#[derive(Clone, Debug)]
pub(super) struct Summary {
    pub wall: Duration,
    pub min: Duration,
    pub max: Duration,
    pub bytes: u64,
    pub flush: Option<Duration>,
    pub flushed_wall: Option<Duration>,
}

fn median(mut times: Vec<Duration>) -> Duration {
    times.sort_unstable();
    let mid = times.len() / 2;
    if times.len().is_multiple_of(2) {
        times[mid - 1] + (times[mid] - times[mid - 1]) / 2
    } else {
        times[mid]
    }
}

impl Summary {
    pub fn spread(&self) -> f64 {
        if self.wall.is_zero() {
            if self.max.is_zero() {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            (self.max - self.min).as_secs_f64() / self.wall.as_secs_f64()
        }
    }

    pub fn display(&self) -> String {
        format!(
            "{:.3} [{:.3}, {:.3}]",
            self.wall.as_secs_f64(),
            self.min.as_secs_f64(),
            self.max.as_secs_f64()
        )
    }
}

pub(super) fn summarize(times: &[Result<Sample, String>]) -> Result<Summary, String> {
    let samples = times.iter().cloned().collect::<Result<Vec<_>, _>>()?;
    if samples.is_empty() {
        return Err("no timed attempts".into());
    }
    let walls: Vec<_> = samples.iter().map(|s| s.wall).collect();
    let mut bytes: Vec<_> = samples.iter().map(|s| s.bytes).collect();
    bytes.sort_unstable();
    let mid = bytes.len() / 2;
    let output = if bytes.len().is_multiple_of(2) {
        bytes[mid - 1] + (bytes[mid] - bytes[mid - 1]) / 2
    } else {
        bytes[mid]
    };
    let flushes: Option<Vec<_>> = samples.iter().map(|s| s.flush).collect();
    let totals: Option<Vec<_>> = samples
        .iter()
        .map(|s| s.flush.map(|f| s.wall + f))
        .collect();
    Ok(Summary {
        wall: median(walls.clone()),
        min: *walls.iter().min().unwrap(),
        max: *walls.iter().max().unwrap(),
        bytes: output,
        flush: flushes.map(median),
        flushed_wall: totals.map(median),
    })
}

pub(super) fn needs_more(
    times: &[Result<Sample, String>],
    min_runs: usize,
    max_runs: usize,
    threshold: f64,
) -> bool {
    if times.len() >= max_runs || times.iter().any(Result::is_err) {
        return false;
    }
    times.len() < min_runs || summarize(times).is_ok_and(|s| s.spread() > threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(wall: u64, bytes: u64, flush: u64) -> Result<Sample, String> {
        Ok(Sample {
            wall: Duration::from_secs(wall),
            bytes,
            flush: Some(Duration::from_secs(flush)),
        })
    }

    #[test]
    fn even_medians_and_flushed_totals_are_computed_independently() {
        let s = summarize(&[
            sample(9, 100, 1),
            sample(1, 10, 8),
            sample(2, 30, 100),
            sample(8, 20, 2),
        ])
        .unwrap();
        assert_eq!(s.wall, Duration::from_secs(5));
        assert_eq!(s.min, Duration::from_secs(1));
        assert_eq!(s.max, Duration::from_secs(9));
        assert_eq!(s.bytes, 25);
        assert_eq!(s.flush, Some(Duration::from_secs(5)));
        assert_eq!(s.flushed_wall, Some(Duration::from_secs(10)));
    }

    #[test]
    fn noisy_side_repeats_up_to_cap_and_errors_stop_retries() {
        let stable = vec![sample(10, 0, 0); 3];
        let noisy = vec![sample(10, 0, 0), sample(10, 0, 0), sample(20, 0, 0)];
        assert!(!needs_more(&stable, 3, 5, 0.1));
        assert!(needs_more(&noisy, 3, 5, 0.1));
        assert!(!needs_more(&noisy, 3, 3, 0.1));
        assert!(!needs_more(&[Err("failed".into())], 3, 5, 0.1));
        assert!(summarize(&[]).is_err());
        assert!(summarize(&[sample(1, 0, 0), Err("failed".into())]).is_err());
    }
}
