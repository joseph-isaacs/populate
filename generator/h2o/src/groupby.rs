//! The groupby data set: `groupby-datagen.R N K nas sort`.
//!
//! ```r
//! set.seed(108)
//! id1 = sample(sprintf("id%03d", 1:K), N, TRUE); id2 = <same>
//! id3 = sample(sprintf("id%010d", 1:(N/K)), N, TRUE)
//! id4 = sample(K, N, TRUE); id5 = <same>; id6 = sample(N/K, N, TRUE)
//! v1 = sample(5, N, TRUE); v2 = sample(15, N, TRUE)
//! v3 = round(runif(N, max = 100), 6)
//! # nas > 0: per id column, NA every row of nas% of its distinct values;
//! #          per v column, NA nas% of the rows
//! # sort == 1: setkey(id1, ..., id6)
//! fwrite(DT, "G1_<N>_<K>_<nas>_<sort>.csv")
//! ```
//!
//! Each column is one run of draws from one Mersenne Twister, so a single
//! sequential pass over the stream records the generator's state at the
//! start of every chunk of every column (2.5 KB each); chunks then
//! regenerate their rows independently, on any thread. NAs need the
//! columns' distinct values in order of first appearance, which the same
//! pass collects. Sorted output is generated in full (row-major), NA'd,
//! sorted by a stable parallel bucket sort (data.table's order, NA first),
//! and read back row by row in that order as batches are collected.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_builtins::{Checkpointed, checkpointed, named, named_all};
use populate_core::{Columns, Distribution};
use populate_table::text::{TextChunks, write_text_many};
use populate_table::{Chunks, DEFAULT_CHUNK_ROWS, resolve_threads};
use rand::Rng;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::thread::scope;

use crate::cols::{Bits, first_appearance, sort_rows};
use crate::fwrite::digits;
use crate::rng::{RMt, Runif100, Sample, sample_int};
use crate::values::{
    Id, Int, Micros, NaSet, NoRng, Table, Value, Values, header, na_rows, na_values, table,
};
use crate::{SEED, pretty_sci};

/// Default rows per Arrow batch.
pub const DEFAULT_BATCH_SIZE: usize = 64 * 1024;

const COLS: usize = 9;
const NAMES: [&str; COLS] = ["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"];

/// The groupby data set for one `(N, K, nas, sort)`.
#[derive(Clone)]
pub struct GroupBy {
    n: u64,
    k: u64,
    nas: u32,
    sort: bool,
    chunk_rows: u64,
    table: Arc<dyn Table>,
}

impl Debug for GroupBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupBy")
            .field("n", &self.n)
            .field("k", &self.k)
            .field("nas", &self.nas)
            .field("sort", &self.sort)
            .finish()
    }
}

/// The row type of a sorted table: one row, all nine columns.
type Row<const NULL: bool> = (
    Id<3, NULL>,
    Id<3, NULL>,
    Id<10, NULL>,
    Int<NULL>,
    Int<NULL>,
    Int<NULL>,
    Int<NULL>,
    Int<NULL>,
    Micros<NULL>,
);

impl GroupBy {
    /// `groupby-datagen.R n k nas sort`. Runs the sequential pass (and,
    /// with `sort`, generates and sorts the whole table in memory, on
    /// all cores).
    pub fn new(n: u64, k: u64, nas: u32, sort: bool) -> Self {
        Self::with_chunk_rows(n, k, nas, sort, DEFAULT_CHUNK_ROWS, 0)
    }

    /// [`new`](Self::new) with `chunk_rows` rows per chunk, sorting on
    /// `threads` threads (0 = all cores). The data is the same for any
    /// chunk size.
    pub fn with_chunk_rows(
        n: u64,
        k: u64,
        nas: u32,
        sort: bool,
        chunk_rows: u64,
        threads: usize,
    ) -> Self {
        assert!(k >= 1 && n >= k, "need 1 <= K <= N");
        assert!(n <= i32::MAX as u64, "N must fit R's integer vectors");
        assert!(nas <= 100, "nas is a percentage");
        assert!(chunk_rows > 0);
        let shape = Shape { n, k };
        let table = if nas > 0 {
            build::<true>(shape, nas, sort, chunk_rows, threads)
        } else {
            build::<false>(shape, nas, sort, chunk_rows, threads)
        };
        GroupBy {
            n,
            k,
            nas,
            sort,
            chunk_rows,
            table,
        }
    }

    /// Rows per Arrow batch (a chunk's last batch is cut at the chunk's end).
    pub fn batch_size(mut self, rows: usize) -> Self {
        assert!(rows > 0);
        self.table = self.table.with_batch_rows(rows as u64);
        self
    }

    pub fn rows(&self) -> u64 {
        self.n
    }

    /// The datagen's file name, e.g. `G1_1e7_1e2_0_0.csv`.
    pub fn file_name(&self) -> String {
        format!(
            "G1_{}_{}_{}_{}.csv",
            pretty_sci(self.n),
            pretty_sci(self.k),
            self.nas,
            self.sort as u8
        )
    }

    pub fn num_chunks(&self) -> u64 {
        self.n.div_ceil(self.chunk_rows)
    }

    fn chunk_range(&self, j: u64) -> (u64, u64) {
        let s = j * self.chunk_rows;
        (s, (s + self.chunk_rows).min(self.n))
    }

    /// Arrow schema, read off the table's distribution: ids 1-3
    /// `Utf8View`, 4-6 and v1-v2 `Int32`, v3 `Float64`; every column
    /// nullable when `nas > 0`.
    pub fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    /// Append chunk `j`'s CSV text (with the header for chunk 0).
    pub fn chunk_csv(&self, j: u64, out: &mut Vec<u8>) {
        if j == 0 {
            header(&self.schema(), out);
        }
        let (s, e) = self.chunk_range(j);
        self.table.csv(s, e, out);
    }

    /// Write the CSV `fwrite` writes, on `threads` threads (0 = all cores).
    pub fn write_csv<W: Write + Send>(&self, w: W, threads: usize) -> io::Result<W> {
        let mut v = write_text_many(&[self], vec![w], threads)?;
        Ok(v.pop().expect("one writer"))
    }
}

/// `N` and `K`, and the ranges of the script's `sample()`s.
#[derive(Debug, Clone, Copy)]
struct Shape {
    n: u64,
    k: u64,
}

impl Shape {
    /// `N / K` as `sample(N/K, ...)` sees it (id6): `(int) (N/K)`.
    fn groups(&self) -> u64 {
        ((self.n as f64) / (self.k as f64)) as u64
    }

    /// `length(1:(N/K))` (id3): R's `:` counts `(int)(N/K - 1 + 1 +
    /// FLT_EPSILON)` values for a fractional `N/K`, one more than
    /// [`groups`](Self::groups) when `N/K` is just below a whole number.
    fn id3_groups(&self) -> u64 {
        colon_len((self.n as f64) / (self.k as f64))
    }

    /// Range of `sample()` for column `c`.
    fn range(&self, c: usize) -> u64 {
        match c {
            0 | 1 | 3 | 4 => self.k,
            2 => self.id3_groups(),
            5 => self.groups(),
            6 => 5,
            7 => 15,
            _ => unreachable!(),
        }
    }
}

/// The next run of `rng` as a column of `sample(range, n, TRUE)`, its
/// state saved at every chunk; `rng` moves on to where the next column
/// starts.
fn drawn<T>(rng: &mut RMt, range: u64, chunk_rows: u64, n: u64) -> Checkpointed<Sample<T>>
where
    T: Value,
{
    let (d, end) = checkpointed(Sample::new(rng.clone(), range), chunk_rows, n);
    *rng = end.rng;
    d
}

/// The table: every column one run of R's stream, then the NAs, then
/// (with `sort`) sorted.
fn build<const NULL: bool>(
    shape: Shape,
    nas: u32,
    sort: bool,
    chunk_rows: u64,
    threads: usize,
) -> Arc<dyn Table> {
    let n = shape.n;
    let r = |c| shape.range(c);
    let mut rng = RMt::set_seed(SEED);
    // the sequential pass: each column's state at every chunk, and where
    // the next column starts
    let id1 = drawn::<Id<3, NULL>>(&mut rng, r(0), chunk_rows, n);
    let id2 = drawn::<Id<3, NULL>>(&mut rng, r(1), chunk_rows, n);
    let id3 = drawn::<Id<10, NULL>>(&mut rng, r(2), chunk_rows, n);
    let id4 = drawn::<Int<NULL>>(&mut rng, r(3), chunk_rows, n);
    let id5 = drawn::<Int<NULL>>(&mut rng, r(4), chunk_rows, n);
    let id6 = drawn::<Int<NULL>>(&mut rng, r(5), chunk_rows, n);
    let v1 = drawn::<Int<NULL>>(&mut rng, r(6), chunk_rows, n);
    let v2 = drawn::<Int<NULL>>(&mut rng, r(7), chunk_rows, n);
    let (v3, end) = checkpointed(Runif100::<Micros<NULL>>::new(rng.clone()), chunk_rows, n);
    rng = end.rng;
    let (ids, rows) = if nas > 0 {
        // each id column's values in order of first appearance, columns
        // on parallel threads
        let orders: Vec<Vec<u32>> = scope(|s| {
            let starts = [
                &id1.inner().rng,
                &id2.inner().rng,
                &id3.inner().rng,
                &id4.inner().rng,
                &id5.inner().rng,
                &id6.inner().rng,
            ];
            let hs: Vec<_> = starts
                .into_iter()
                .enumerate()
                .map(|(c, rng)| {
                    let d = Sample::<Int<false>>::new(rng.clone(), r(c));
                    s.spawn(move || appearance(d, r(c), n, chunk_rows))
                })
                .collect();
            hs.into_iter().map(|h| h.join().expect("unique")).collect()
        });
        nas_of(shape, nas, &orders, &mut rng)
    } else {
        (vec![None; 6], vec![None; 3])
    };
    let dist = (
        named("id1", na_values(id1, ids[0].clone())),
        named("id2", na_values(id2, ids[1].clone())),
        named("id3", na_values(id3, ids[2].clone())),
        named("id4", na_values(id4, ids[3].clone())),
        named("id5", na_values(id5, ids[4].clone())),
        named("id6", na_values(id6, ids[5].clone())),
        named("v1", na_rows(v1, rows[0].clone())),
        named("v2", na_rows(v2, rows[1].clone())),
        named("v3", na_rows(v3, rows[2].clone())),
    );
    let name = "groupby";
    if !sort {
        return table(name, n, DEFAULT_BATCH_SIZE, dist);
    }
    let (rows, perm) = sorted::<NULL, _>(shape, &dist, chunk_rows, threads);
    table(
        name,
        n,
        DEFAULT_BATCH_SIZE,
        named_all(
            &NAMES,
            SortedRows::<NULL> {
                rows: Arc::new(rows),
                perm: Arc::new(perm),
                at: 0,
            },
        ),
    )
}

/// The NA step: each id column's distinct values in order of first
/// appearance (R's `unique()`), `nas`% of them drawn to be NA; then
/// `nas`% of the rows of each v column.
fn nas_of(shape: Shape, nas: u32, orders: &[Vec<u32>], rng: &mut RMt) -> (Vec<NaSet>, Vec<NaSet>) {
    let n = shape.n;
    let frac = nas as f64 / 100.0;
    let ids = orders
        .iter()
        .enumerate()
        .map(|(c, ucol)| {
            let nna = (ucol.len() as f64 * frac) as u64;
            (nna > 0).then(|| {
                let domain = shape.range(c);
                let mut b = Bits::new(domain + 1);
                if c >= 3 && ucol.len() == 1 {
                    // sample(ucol, nna) on one number v is
                    // sample.int(v, nna): NA the values drawn
                    for v in sample_int(rng, ucol[0] as u64, nna) {
                        if v as u64 <= domain {
                            b.set(v as u64);
                        }
                    }
                } else {
                    for i in sample_int(rng, ucol.len() as u64, nna) {
                        b.set(ucol[i as usize - 1] as u64);
                    }
                }
                Arc::new(b)
            })
        })
        .collect();
    let nna = (n as f64 * frac) as u64;
    let rows = (0..3)
        .map(|_| {
            (nna > 0).then(|| {
                let mut b = Bits::new(n);
                for r in sample_int(rng, n, nna) {
                    b.set(r as u64 - 1);
                }
                Arc::new(b)
            })
        })
        .collect();
    (ids, rows)
}

/// A column's distinct values in order of first appearance, made from its
/// start chunk by chunk until every value of its range has appeared.
fn appearance(mut d: Sample<Int<false>>, range: u64, n: u64, chunk_rows: u64) -> Vec<u32> {
    let mut seen = Bits::new(range + 1);
    let mut order = vec![];
    let mut rng = NoRng;
    let mut buf = Columns::<Int<false>>::default();
    let mut at = 0;
    while at < n && (order.len() as u64) < range {
        let rows = chunk_rows.min(n - at);
        buf.values_mut().clear();
        d.collect_into(&mut buf, &mut rng, rows as usize);
        first_appearance(buf.values(), &mut seen, &mut order);
        at += rows;
    }
    order
}

/// The whole table, row-major (36 bytes, about one cache line, a row),
/// and its `setkey(id1, ..., id6)` order: NA first, then C-locale string
/// order for ids 1-3 and numeric order for 4-6.
fn sorted<const NULL: bool, D>(
    shape: Shape,
    dist: &D,
    chunk_rows: u64,
    threads: usize,
) -> (Vec<[u32; COLS]>, Vec<u32>)
where
    D: Distribution<Item = Row<NULL>> + Sync + Clone,
{
    let n = shape.n as usize;
    // every chunk generated straight into its rows, on all threads
    let mut rows: Vec<[u32; COLS]> = vec![[0; COLS]; n];
    {
        let parts: Vec<Mutex<&mut [[u32; COLS]]>> = rows
            .chunks_mut(chunk_rows as usize)
            .map(Mutex::new)
            .collect();
        let next = AtomicUsize::new(0);
        scope(|s| {
            for _ in 0..resolve_threads(threads) {
                s.spawn(|| {
                    loop {
                        let j = next.fetch_add(1, Relaxed);
                        let Some(part) = parts.get(j) else { break };
                        let mut part = part.lock().expect("rows");
                        let mut d = dist.clone();
                        d.skip(j as u64 * chunk_rows);
                        let c = d.collect(&mut NoRng, part.len());
                        let cols: [&Vec<u32>; COLS] = [
                            c.0.values(),
                            c.1.values(),
                            c.2.values(),
                            c.3.values(),
                            c.4.values(),
                            c.5.values(),
                            c.6.values(),
                            c.7.values(),
                            c.8.values(),
                        ];
                        for (r, row) in part.iter_mut().enumerate() {
                            *row = std::array::from_fn(|c| cols[c][r]);
                        }
                    }
                });
            }
        });
    }
    let rank = |width: usize, range: u64| -> Vec<u32> {
        // rank[v] for v in 1..=range of sprintf("id%0<width>d", v)
        let mut vs: Vec<u32> = (1..=range as u32).collect();
        vs.sort_by_cached_key(|&v| format!("{v:0width$}"));
        let mut rank = vec![0u32; range as usize + 1];
        for (i, &v) in vs.iter().enumerate() {
            rank[v as usize] = i as u32 + 1;
        }
        rank
    };
    // per column: sort key of a value (NA 0), and its domain size
    let ranks: Vec<Option<Vec<u32>>> = (0..6)
        .map(|c| match c {
            // string order is numeric order unless some ids are wider
            0 | 1 if digits(shape.range(c) as u32) > 3 => Some(rank(3, shape.range(c))),
            2 if digits(shape.range(c) as u32) > 10 => Some(rank(10, shape.range(c))),
            _ => None,
        })
        .collect();
    let domains: Vec<u64> = (0..6).map(|c| shape.range(c) + 1).collect();
    let perm = sort_rows(n, |c, r| rows[r][c], &ranks, &domains, threads);
    (rows, perm)
}

/// The sorted table's rows: `rows` in the order `perm`.
#[derive(Clone)]
struct SortedRows<const NULL: bool> {
    rows: Arc<Vec<[u32; COLS]>>,
    perm: Arc<Vec<u32>>,
    at: usize,
}

impl<const NULL: bool> Distribution for SortedRows<NULL> {
    type Item = Row<NULL>;

    fn sample(&mut self, _rng: &mut impl Rng) -> Row<NULL> {
        let r = self.rows[self.perm[self.at] as usize];
        self.at += 1;
        (
            Id(r[0]),
            Id(r[1]),
            Id(r[2]),
            Int(r[3]),
            Int(r[4]),
            Int(r[5]),
            Int(r[6]),
            Int(r[7]),
            Micros(r[8]),
        )
    }

    /// One random access per row, its nine values written to the nine
    /// columns.
    fn collect_into(&mut self, cols: &mut Columns<Row<NULL>>, _rng: &mut impl Rng, n: usize) {
        let perm = &self.perm[self.at..self.at + n];
        let mut out: [&mut Vec<u32>; COLS] = [
            cols.0.values_mut(),
            cols.1.values_mut(),
            cols.2.values_mut(),
            cols.3.values_mut(),
            cols.4.values_mut(),
            cols.5.values_mut(),
            cols.6.values_mut(),
            cols.7.values_mut(),
            cols.8.values_mut(),
        ];
        for c in out.iter_mut() {
            c.reserve(n);
        }
        for &r in perm {
            let row = &self.rows[r as usize];
            for (c, col) in out.iter_mut().enumerate() {
                col.push(row[c]);
            }
        }
        self.at += n;
    }

    fn skip(&mut self, n: u64) {
        self.at += n as usize;
    }
}

/// `length(1:x)` for `x >= 1` (R's `seq_colon`).
fn colon_len(x: f64) -> u64 {
    if x == x.trunc() {
        x as u64
    } else {
        ((x - 1.0) + 1.0 + f32::EPSILON as f64) as u64
    }
}

impl Chunks for GroupBy {
    fn schema(&self) -> SchemaRef {
        GroupBy::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        GroupBy::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let (s, e) = self.chunk_range(i);
        self.table.arrow(s, e, emit)
    }
}

impl TextChunks for GroupBy {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        GroupBy::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        self.chunk_csv(i, &mut bufs[0])
    }
}

#[cfg(test)]
mod tests {
    use super::colon_len;

    // length(1:(19999999/1e7)) is 2 in R, sample(19999999/1e7, ..) draws 1s
    #[test]
    fn colon_rounds_like_r() {
        assert_eq!(colon_len(19_999_999.0 / 1e7), 2);
        assert_eq!((19_999_999.0f64 / 1e7) as u64, 1);
        assert_eq!(colon_len(1e5), 100_000);
        assert_eq!(colon_len(10.0 / 3.0), 3);
    }
}
