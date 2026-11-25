//! The thread pool every driver runs on.
//!
//! - [`par_sources`]: run the chunks of several sources, biggest remaining
//!   work first, and consume each source's results in order.
//! - [`par_ordered`]: the same for one source: jobs `0..n`, results in index
//!   order.
//!
//! Several tables are several sources, not one sequence of jobs in one
//! global order. Chunks of different tables differ in cost by 20x or more
//! (a TPC-DS store_sales chunk against an inventory one), so whichever table
//! comes last in a global order leaves a tail of big chunks with the other
//! cores idle. And a finished chunk of one table would wait behind an
//! unrelated slow chunk of another, though only each table's own order
//! matters.
//!
//! So each source's chunks start in order (a source's results come back
//! nearly in order and wait in a small buffer), but which source a free
//! worker takes is chosen by remaining work: the one with the most
//! `chunks left * mean chunk time`, the mean measured as chunks finish. A
//! source not yet timed runs one chunk first, to be measured. Big tables
//! start early, and the tail is made of the small chunks. Only timing
//! depends on the schedule: every chunk is a function of its index, so the
//! output is the same for any thread count and any order.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Instant;

/// `threads`, or every core when it is 0: what every driver's `threads`
/// argument means.
pub fn resolve_threads(threads: usize) -> usize {
    match threads {
        0 => thread::available_parallelism().map_or(1, |t| t.get()),
        t => t,
    }
}

/// Run `work(0..n)` on `threads` threads (0 = all cores) and feed the
/// results to `consume` in index order on the calling thread, stopping at
/// the first error. Workers run at most about `2 * threads` tasks ahead.
/// This is [`par_sources`] with one source.
pub fn par_ordered<T: Send, E>(
    n: u64,
    threads: usize,
    work: impl Fn(u64) -> T + Sync,
    mut consume: impl FnMut(T) -> Result<(), E>,
) -> Result<(), E> {
    par_sources(&[n], threads, |_, i| work(i), |_, t| consume(t))
}

/// Who runs next: each source's next chunk, how many it has, and what its
/// chunks have cost so far. [`par_sources`] uses it; a driver with its own
/// workers (one that writes files itself, say) can share one behind a
/// `Mutex`: [`Self::pick`] a chunk, run it, then [`Self::finished`] with
/// its time.
#[derive(Debug)]
pub struct Schedule {
    next: Vec<u64>,
    total: Vec<u64>,
    /// Seconds spent in finished chunks, and how many finished.
    spent: Vec<(f64, u64)>,
    /// Chunks started but not finished.
    running: Vec<u64>,
}

impl Schedule {
    /// A schedule for sources with these chunk counts.
    pub fn new(chunks: &[u64]) -> Self {
        let n = chunks.len();
        Schedule {
            next: vec![0; n],
            total: chunks.to_vec(),
            spent: vec![(0.0, 0); n],
            running: vec![0; n],
        }
    }

    /// The next `(source, chunk)` to run, if any are left.
    pub fn pick(&mut self) -> Option<(usize, u64)> {
        let left = |s: usize| self.total[s] - self.next[s];
        let open = (0..self.total.len()).filter(|&s| left(s) > 0);
        // a source with no measured chunk and none running: time one
        let s = open
            .clone()
            .find(|&s| self.spent[s].1 == 0 && self.running[s] == 0)
            .or_else(|| {
                // most remaining work; untimed (still running) sources last
                open.max_by(|&a, &b| {
                    let work = |s: usize| match self.spent[s] {
                        (_, 0) => -1.0,
                        (t, k) => left(s) as f64 * t / k as f64,
                    };
                    work(a).total_cmp(&work(b)).then(b.cmp(&a))
                })
            })?;
        let i = self.next[s];
        self.next[s] += 1;
        self.running[s] += 1;
        Some((s, i))
    }

    /// Chunk of source `s` done in `secs` seconds.
    pub fn finished(&mut self, s: usize, secs: f64) {
        self.running[s] -= 1;
        self.spent[s].0 += secs;
        self.spent[s].1 += 1;
    }
}

/// Run `work(source, chunk)` for every chunk of every source (`chunks[s]`
/// chunks for source `s`) on `threads` threads (0 = all cores), and call
/// `consume(source, result)` on the calling thread with each source's
/// results in chunk order; sources interleave in no fixed order. Stops at
/// the first error. Workers run at most about `2 * threads` chunks ahead of
/// `consume`. With one thread, sources run in order, one after another.
pub fn par_sources<T: Send, E>(
    chunks: &[u64],
    threads: usize,
    work: impl Fn(usize, u64) -> T + Sync,
    mut consume: impl FnMut(usize, T) -> Result<(), E>,
) -> Result<(), E> {
    let jobs: u64 = chunks.iter().sum();
    let threads = resolve_threads(threads).min(jobs.max(1) as usize);
    if threads <= 1 {
        for (s, &n) in chunks.iter().enumerate() {
            for i in 0..n {
                consume(s, work(s, i))?;
            }
        }
        return Ok(());
    }
    let schedule = Mutex::new(Schedule::new(chunks));
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        let (tx, rx) = sync_channel::<(usize, u64, T)>(threads * 2);
        for _ in 0..threads {
            let tx = tx.clone();
            let (schedule, stop, work) = (&schedule, &stop, &work);
            scope.spawn(move || {
                while !stop.load(Relaxed) {
                    let Some((s, i)) = schedule.lock().expect("schedule").pick() else {
                        break;
                    };
                    let t0 = Instant::now();
                    let t = work(s, i);
                    let secs = t0.elapsed().as_secs_f64();
                    schedule.lock().expect("schedule").finished(s, secs);
                    if tx.send((s, i, t)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        // Reorder per source. `rx` is dropped when this closure returns,
        // before the scope joins, so a worker blocked on a full channel
        // wakes up (its send fails) when we stop early.
        let mut pending: Vec<BTreeMap<u64, T>> = chunks.iter().map(|_| BTreeMap::new()).collect();
        let mut want = vec![0u64; chunks.len()];
        for (s, i, t) in rx.iter() {
            pending[s].insert(i, t);
            while let Some(t) = pending[s].remove(&want[s]) {
                want[s] += 1;
                if let Err(e) = consume(s, t) {
                    stop.store(true, Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn every_chunk_once_in_order_per_source() {
        let chunks = [3u64, 0, 7, 1, 12];
        for threads in [1, 2, 4, 8] {
            let mut got: Vec<Vec<u64>> = vec![Vec::new(); chunks.len()];
            let r: Result<(), ()> = par_sources(
                &chunks,
                threads,
                |s, i| {
                    // later chunks of source 4 are slow, to shuffle finishes
                    if s == 4 && i % 3 == 0 {
                        thread::sleep(Duration::from_millis(2));
                    }
                    (s, i * 10)
                },
                |s, (s2, v)| {
                    assert_eq!(s, s2);
                    got[s].push(v);
                    Ok(())
                },
            );
            r.unwrap();
            for (s, &n) in chunks.iter().enumerate() {
                let want: Vec<u64> = (0..n).map(|i| i * 10).collect();
                assert_eq!(got[s], want, "source {s}, {threads} threads");
            }
        }
    }

    #[test]
    fn stops_at_first_error() {
        let mut seen = 0;
        let r = par_sources(
            &[100, 100],
            4,
            |_, i| i,
            |_, i| {
                seen += 1;
                if i == 5 { Err(i) } else { Ok(()) }
            },
        );
        assert_eq!(r, Err(5));
        assert!(seen < 200);
    }

    #[test]
    fn biggest_remaining_work_first() {
        let mut s = Schedule::new(&[10, 10, 10]);
        // each source is timed once first
        assert_eq!(s.pick(), Some((0, 0)));
        assert_eq!(s.pick(), Some((1, 0)));
        assert_eq!(s.pick(), Some((2, 0)));
        s.finished(0, 1.0);
        s.finished(1, 5.0);
        s.finished(2, 2.0);
        // source 1: 9 left at 5 s each
        assert_eq!(s.pick(), Some((1, 1)));
        // 8 * 5 = 40 still beats 9 * 2 = 18
        assert_eq!(s.pick(), Some((1, 2)));
        // then whichever has more left: 7 * 5 = 35 down to 3 * 5 = 15
        // falls below source 2's 9 * 2 = 18, and they alternate
        let order: Vec<usize> = (0..8).map(|_| s.pick().unwrap().0).collect();
        assert_eq!(order, [1, 1, 1, 1, 2, 2, 1, 2]);
        // source 0 (9 * 1) waits until the others are cheaper
        let rest: Vec<usize> = (0..17).map(|_| s.pick().unwrap().0).collect();
        assert_eq!(rest.iter().filter(|&&x| x == 0).count(), 9);
        assert_eq!(s.pick(), None);
    }
}
