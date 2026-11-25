//! The datagen's BI mode (`--mode bi`): the raw tables split at the bulk
//! load threshold into an initial snapshot and daily (or yearly, ...)
//! batches of inserts and deletes, as `RawToBiTransform` does.
//!
//! - **Initial snapshot**: every row created before the threshold
//!   (`end - (end - start) * (1 - bulkloadPortion)`, deleted or not), without
//!   `deletionDate` and `explicitlyDeleted`. Static tables are unchanged.
//! - **Inserts**: rows created in `[threshold, end)`, the same columns,
//!   grouped by the batch of their creation date and sorted by it.
//! - **Deletes**: rows of the tables with `explicitlyDeleted` that are
//!   explicitly deleted in `[threshold, end)`: `deletionDate` and the
//!   primary key, grouped by the batch of the deletion date and sorted by
//!   it.
//!
//! The snapshot is a filter of each chunk, so it streams like the raw
//! tables. Inserts and deletes (a few percent of the rows) are kept per
//! chunk and sorted at the end; the sort is stable over rows in chunk
//! order, as Spark's radix sort is over its input.

use crate::csv;
use crate::java::civil_from_days;
use crate::layout::Layout;
use crate::rows::{Kind, Rows, TableDef};
use crate::{Ldbc, par_map, to_batch};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_table::MultiChunks;
use populate_table::text::TextChunks;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// How inserts and deletes are grouped (`--batch-period`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    Year,
    Month,
    Day,
    /// Spark's pattern for it is `yyyy-MM-dd'T'hh`: a 12-hour clock, so
    /// 01:00 and 13:00 share a batch. Reproduced.
    Hour,
    /// `yyyy-MM-dd'T'hh:mm`, the same 12-hour clock.
    Minute,
}

impl Period {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "year" => Period::Year,
            "month" => Period::Month,
            "day" => Period::Day,
            "hour" => Period::Hour,
            "minute" => Period::Minute,
            _ => return None,
        })
    }

    /// The batch id of epoch milliseconds `ms`: Spark's
    /// `date_format(date_trunc(period, to_timestamp(ms / 1000)), pattern)`
    /// in UTC.
    pub fn batch_id(self, ms: i64) -> String {
        let us = csv::spark_micros(ms);
        let secs = us.div_euclid(1_000_000);
        let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
        let sod = secs.rem_euclid(86_400);
        let (h, min) = (sod / 3600, sod / 60 % 60);
        let h12 = if h % 12 == 0 { 12 } else { h % 12 };
        match self {
            Period::Year => format!("{y:04}"),
            Period::Month => format!("{y:04}-{m:02}"),
            Period::Day => format!("{y:04}-{m:02}-{d:02}"),
            Period::Hour => format!("{y:04}-{m:02}-{d:02}T{h12:02}"),
            Period::Minute => format!("{y:04}-{m:02}-{d:02}T{h12:02}:{min:02}"),
        }
    }
}

/// BI settings: `--bulkload-portion` and `--batch-period`.
#[derive(Debug, Clone, Copy)]
pub struct Bi {
    pub bulkload_portion: f64,
    pub period: Period,
}

impl Default for Bi {
    fn default() -> Self {
        Bi {
            bulkload_portion: 0.97,
            period: Period::Day,
        }
    }
}

/// A projection of one of a layout's dynamic tables.
#[derive(Debug)]
pub struct Projected {
    /// The output table (same name as the source).
    pub def: &'static TableDef,
    /// Index in the layout's dynamic tables.
    pub src: usize,
    /// Source columns, in output order.
    cols: Vec<usize>,
}

impl Projected {
    fn new(d: &'static TableDef, src: usize, cols: Vec<usize>) -> Self {
        let out: Vec<(&'static str, Kind)> = cols.iter().map(|&c| d.cols[c]).collect();
        let def = Box::leak(Box::new(TableDef {
            name: d.name,
            cols: Box::leak(out.into_boxed_slice()),
        }));
        Projected { def, src, cols }
    }

    fn take(&self, rows: &Rows, idx: &[u32]) -> Rows {
        Rows {
            cols: self
                .cols
                .iter()
                .map(|&c| rows.cols[c].gather(idx))
                .collect(),
        }
    }
}

/// The BI tables of a layout: one snapshot and one insert table per
/// dynamic table (the same columns), and the delete tables.
#[derive(Debug)]
pub struct BiTables {
    pub snapshot: Vec<Projected>,
    pub deletes: Vec<Projected>,
}

impl BiTables {
    pub fn new(layout: &Layout) -> Self {
        let raw = ["deletionDate", "explicitlyDeleted"];
        let t = &layout.dynamic;
        let snapshot = t
            .iter()
            .enumerate()
            .map(|(k, o)| {
                let cols = (0..o.def.cols.len())
                    .filter(|&c| !raw.contains(&o.def.cols[c].0))
                    .collect();
                Projected::new(o.def, k, cols)
            })
            .collect();
        let deletes = t
            .iter()
            .enumerate()
            .filter(|(_, o)| !o.derived)
            .filter_map(|(k, o)| {
                let names: Vec<&str> = o.def.cols.iter().map(|c| c.0).collect();
                let pos = |n: &str| names.iter().position(|&x| x == n);
                let explicit = pos("explicitlyDeleted")?;
                // nodes: `id`; edges: the two ids after the raw columns
                let key = match pos("id") {
                    Some(id) => vec![id],
                    None => vec![explicit + 1, explicit + 2],
                };
                let mut cols = vec![pos("deletionDate").expect("deletionDate")];
                cols.extend(key);
                Some(Projected::new(o.def, k, cols))
            })
            .collect();
        BiTables { snapshot, deletes }
    }
}

/// One chunk's inserts (per dynamic table) and deletes (per delete table).
type Pending = (Vec<Rows>, Vec<Rows>);

/// A BI run over a network: the snapshot as a [`MultiChunks`] /
/// [`TextChunks`] source (one output per dynamic table), then
/// [`Self::inserts`] and [`Self::deletes`].
pub struct BiRun<'a> {
    g: &'a Ldbc,
    layout: &'a Layout,
    pub tables: BiTables,
    threshold: i64,
    end: i64,
    period: Period,
    pending: Mutex<Vec<Option<Pending>>>,
}

/// One table's batches: `(batch id, rows sorted by date)`, by batch id.
pub struct Batches {
    pub def: &'static TableDef,
    pub batches: Vec<(String, Rows)>,
}

/// One table's rows to sort and cut into batches, in chunk order.
type BatchJob = (&'static TableDef, Vec<Rows>);

impl<'a> BiRun<'a> {
    pub fn new(g: &'a Ldbc, layout: &'a Layout, bi: Bi) -> Self {
        let (start, end) = (g.dates.start, g.dates.end);
        let threshold = end - ((end - start) as f64 * (1.0 - bi.bulkload_portion)) as i64;
        BiRun {
            g,
            layout,
            tables: BiTables::new(layout),
            threshold,
            end,
            period: bi.period,
            pending: Mutex::new((0..g.num_chunks()).map(|_| None).collect()),
        }
    }

    /// Chunk `i`'s snapshot rows, per dynamic table; its inserts and
    /// deletes are kept for later.
    pub fn chunk_snapshot(&self, i: u64) -> Vec<Rows> {
        let raw = self.g.chunk_rows(i);
        let rows = Layout::apply(&self.layout.dynamic, &raw);
        drop(raw);
        let t = &self.tables;
        let (thr, end) = (self.threshold, self.end);
        let mut snap = Vec::with_capacity(rows.len());
        let mut ins = Vec::with_capacity(rows.len());
        for (p, r) in t.snapshot.iter().zip(&rows) {
            let created: Vec<i64> = r.cols[0].epoch_ms().collect();
            let before: Vec<u32> = (0..created.len() as u32)
                .filter(|&j| created[j as usize] < thr)
                .collect();
            let after: Vec<u32> = (0..created.len() as u32)
                .filter(|&j| (thr..end).contains(&created[j as usize]))
                .collect();
            snap.push(p.take(r, &before));
            ins.push(p.take(r, &after));
        }
        let dels = t
            .deletes
            .iter()
            .map(|p| {
                let (r, def) = (&rows[p.src], self.layout.dynamic[p.src].def);
                let col = |n: &str| def.cols.iter().position(|c| c.0 == n).expect("column");
                let deleted: Vec<i64> = r.cols[col("deletionDate")].epoch_ms().collect();
                let explicit = r.cols[col("explicitlyDeleted")].bools();
                let idx: Vec<u32> = (0..deleted.len() as u32)
                    .filter(|&j| explicit[j as usize] && (thr..end).contains(&deleted[j as usize]))
                    .collect();
                p.take(r, &idx)
            })
            .collect();
        self.pending.lock().expect("pending")[i as usize] = Some((ins, dels));
        snap
    }

    fn batches(&self, def: &'static TableDef, parts: Vec<Rows>) -> Batches {
        // rows as (part, row), in chunk order, stably sorted by column 0 (the
        // date the batch is of: creation, or deletion); each batch is
        // gathered straight from the parts, so no column ever holds more
        // than one batch's strings
        let mut order: Vec<(u32, u32, i64)> = parts
            .iter()
            .enumerate()
            .flat_map(|(k, p)| {
                p.cols[0]
                    .epoch_ms()
                    .enumerate()
                    .map(move |(j, d)| (k as u32, j as u32, d))
            })
            .collect();
        order.sort_by_key(|&(_, _, d)| d);
        let mut by: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
        for (k, j, d) in order {
            by.entry(self.period.batch_id(d)).or_default().push((k, j));
        }
        let batches = by
            .into_iter()
            .map(|(id, idx)| {
                let mut r = Rows::new(def);
                for (k, j) in idx {
                    for (c, src) in r.cols.iter_mut().zip(&parts[k as usize].cols) {
                        c.push_from(src, j as usize);
                    }
                }
                (id, r)
            })
            .collect();
        Batches { def, batches }
    }

    fn take_pending(&self) -> Vec<Pending> {
        let mut p = self.pending.lock().expect("pending");
        p.iter_mut()
            .map(|c| {
                c.take()
                    .expect("every chunk's snapshot was generated first")
            })
            .collect()
    }

    /// The insert and delete batches, per table (dynamic tables, then
    /// [`BiTables::deletes`]), on `threads` threads (0 = all cores). Call
    /// once, after every chunk's snapshot.
    pub fn finish(&self, threads: usize) -> (Vec<Batches>, Vec<Batches>) {
        let t = &self.tables;
        let pending = self.take_pending();
        let mut ins: Vec<Vec<Rows>> = vec![Vec::new(); t.snapshot.len()];
        let mut dels: Vec<Vec<Rows>> = vec![Vec::new(); t.deletes.len()];
        for (i, d) in pending {
            for (k, r) in i.into_iter().enumerate() {
                ins[k].push(r);
            }
            for (k, r) in d.into_iter().enumerate() {
                dels[k].push(r);
            }
        }
        let jobs: Vec<Mutex<Option<BatchJob>>> = t
            .snapshot
            .iter()
            .map(|p| p.def)
            .zip(ins)
            .chain(t.deletes.iter().map(|p| p.def).zip(dels))
            .map(|j| Mutex::new(Some(j)))
            .collect();
        let mut out = par_map(jobs.len(), threads, |k| {
            let (def, parts) = jobs[k].lock().expect("job").take().expect("once");
            self.batches(def, parts)
        });
        let deletes = out.split_off(t.snapshot.len());
        (out, deletes)
    }
}

impl MultiChunks for BiRun<'_> {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.tables
            .snapshot
            .iter()
            .map(|p| p.def.schema())
            .collect()
    }

    fn num_chunks(&self) -> u64 {
        self.g.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let snap = self.chunk_snapshot(i);
        for (k, (p, r)) in self.tables.snapshot.iter().zip(snap).enumerate() {
            if !r.is_empty() {
                emit(k, to_batch(p.def, r));
            }
        }
    }
}

impl TextChunks for BiRun<'_> {
    fn outputs(&self) -> usize {
        self.tables.snapshot.len()
    }

    fn num_chunks(&self) -> u64 {
        self.g.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let snap = self.chunk_snapshot(i);
        for ((p, r), buf) in self.tables.snapshot.iter().zip(&snap).zip(bufs.iter_mut()) {
            if i == 0 {
                csv::header(p.def, buf);
            }
            csv::write(p.def, r, buf);
        }
    }
}
