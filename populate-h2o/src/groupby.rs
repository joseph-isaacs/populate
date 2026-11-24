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
//! pass collects. Sorted output is generated in full, NA'd, sorted by a
//! stable counting sort (data.table's order, NA first) and sliced.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_table::Chunks;
use populate_table::table::par_ordered;
use populate_table::text::{TextChunks, write_text_many};
use std::convert::Infallible;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Write};
use std::sync::Arc;

use crate::cols::{
    Bits, Cursor, NA, counting_pass, first_appearance, gather, id_strings, int32, micros,
    write_with,
};
use crate::fwrite::{digits, round6_micros};
use crate::rng::{RMt, sample_int};
use crate::{SEED, pretty_sci};

/// Default rows per chunk (= Parquet row group).
pub const DEFAULT_CHUNK_ROWS: u64 = 256 * 1024;
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
    batch_size: usize,
    plan: Arc<Plan>,
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

/// What chunks need from the sequential pass.
struct Plan {
    /// Chunk size the snapshots are for.
    chunk_rows: u64,
    /// Generator state at the start of chunk `j`'s rows of column `c`:
    /// `snaps[j * COLS + c]`.
    snaps: Vec<RMt>,
    /// NA'd values of each id column (by value) and rows of each v column.
    na: Option<Nas>,
    /// The sorted columns, when `sort`.
    sorted: Option<Vec<Vec<u32>>>,
}

struct Nas {
    ids: Vec<Option<Bits>>,
    rows: Vec<Option<Bits>>,
}

impl GroupBy {
    /// `groupby-datagen.R n k nas sort`. Runs the sequential pass (and,
    /// with `sort`, generates and sorts the whole table in memory, on
    /// `threads` threads, 0 = all cores).
    pub fn new(n: u64, k: u64, nas: u32, sort: bool) -> Self {
        Self::with_chunk_rows(n, k, nas, sort, DEFAULT_CHUNK_ROWS, 0)
    }

    /// [`new`](Self::new) with `chunk_rows` rows per chunk. The data is the
    /// same for any chunk size.
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
        let mut g = GroupBy {
            n,
            k,
            nas,
            sort,
            chunk_rows,
            batch_size: DEFAULT_BATCH_SIZE,
            plan: Arc::new(Plan {
                chunk_rows,
                snaps: vec![],
                na: None,
                sorted: None,
            }),
        };
        g.plan = Arc::new(g.build(threads));
        g
    }

    /// Rows per Arrow batch (at most one chunk).
    pub fn batch_size(mut self, rows: usize) -> Self {
        assert!(rows > 0);
        self.batch_size = rows;
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

    pub fn num_chunks(&self) -> u64 {
        self.n.div_ceil(self.chunk_rows)
    }

    fn chunk_range(&self, j: u64) -> (u64, u64) {
        let s = j * self.chunk_rows;
        (s, (s + self.chunk_rows).min(self.n))
    }

    fn build(&self, threads: usize) -> Plan {
        let chunks = self.num_chunks();
        let mut rng = RMt::set_seed(SEED);
        let mut snaps = vec![rng.clone(); (chunks as usize) * COLS];
        let track = self.nas > 0;
        let mut orders: Vec<Vec<u32>> = vec![vec![]; 6];
        let mut buf = vec![0u32; if track { self.chunk_rows as usize } else { 0 }];
        for c in 0..8 {
            let range = self.range(c);
            let mut seen = Bits::new(if track && c < 6 { range + 1 } else { 0 });
            for j in 0..chunks {
                snaps[(j as usize) * COLS + c] = rng.clone();
                let (s, e) = self.chunk_range(j);
                if track && c < 6 {
                    let b = &mut buf[..(e - s) as usize];
                    rng.fill_index(range, b);
                    for v in b.iter_mut() {
                        *v += 1;
                    }
                    first_appearance(b, &mut seen, &mut orders[c]);
                } else {
                    rng.skip_index(range, e - s);
                }
            }
        }
        for j in 0..chunks {
            snaps[(j as usize) * COLS + 8] = rng.clone();
            let (s, e) = self.chunk_range(j);
            rng.skip(e - s);
        }
        let na = track.then(|| {
            let frac = self.nas as f64 / 100.0;
            let ids = orders
                .iter()
                .enumerate()
                .map(|(c, ucol)| {
                    let nna = (ucol.len() as f64 * frac) as u64;
                    (nna > 0).then(|| {
                        let domain = self.range(c);
                        let mut b = Bits::new(domain + 1);
                        if c >= 3 && ucol.len() == 1 {
                            // sample(ucol, nna) on one number v is
                            // sample.int(v, nna): NA the values drawn
                            for v in sample_int(&mut rng, ucol[0] as u64, nna) {
                                if v as u64 <= domain {
                                    b.set(v as u64);
                                }
                            }
                        } else {
                            for i in sample_int(&mut rng, ucol.len() as u64, nna) {
                                b.set(ucol[i as usize - 1] as u64);
                            }
                        }
                        b
                    })
                })
                .collect();
            let nna = (self.n as f64 * frac) as u64;
            let rows = (0..3)
                .map(|_| {
                    (nna > 0).then(|| {
                        let mut b = Bits::new(self.n);
                        for r in sample_int(&mut rng, self.n, nna) {
                            b.set(r as u64 - 1);
                        }
                        b
                    })
                })
                .collect();
            Nas { ids, rows }
        });
        let mut plan = Plan {
            chunk_rows: self.chunk_rows,
            snaps,
            na,
            sorted: None,
        };
        if self.sort {
            plan.sorted = Some(self.sorted(&plan, threads));
        }
        plan
    }

    /// Generate chunk `j`'s columns from the plan's snapshots, NAs applied.
    fn generate(&self, plan: &Plan, j: u64) -> Vec<Vec<u32>> {
        let (s, e) = self.chunk_range(j);
        let rows = (e - s) as usize;
        let mut cols: Vec<Vec<u32>> = (0..COLS).map(|_| vec![0u32; rows]).collect();
        let snaps = &plan.snaps[(j as usize) * COLS..][..COLS];
        for c in 0..8 {
            let mut rng = snaps[c].clone();
            let col = &mut cols[c];
            rng.fill_index(self.range(c), col);
            for v in col.iter_mut() {
                *v += 1;
            }
        }
        {
            let mut rng = snaps[8].clone();
            let mut u = vec![0f64; rows];
            rng.fill_unif(&mut u);
            for (d, u) in cols[8].iter_mut().zip(&u) {
                *d = round6_micros(100.0 * u) as u32;
            }
        }
        if let Some(na) = &plan.na {
            for (c, b) in na.ids.iter().enumerate() {
                if let Some(b) = b {
                    for v in cols[c].iter_mut() {
                        if b.get(*v as u64) {
                            *v = NA;
                        }
                    }
                }
            }
            for (c, b) in na.rows.iter().enumerate() {
                if let Some(b) = b {
                    for (r, v) in cols[6 + c].iter_mut().enumerate() {
                        if b.get(s + r as u64) {
                            *v = NA;
                        }
                    }
                }
            }
        }
        cols
    }

    /// The whole table, NA'd, in `setkey(id1, ..., id6)` order.
    fn sorted(&self, plan: &Plan, threads: usize) -> Vec<Vec<u32>> {
        let n = self.n as usize;
        let mut cols: Vec<Vec<u32>> = (0..COLS).map(|_| Vec::with_capacity(n)).collect();
        par_ordered(
            self.num_chunks(),
            threads,
            |j| self.generate(plan, j),
            |chunk| {
                for (c, v) in cols.iter_mut().zip(chunk) {
                    c.extend_from_slice(&v);
                }
                Ok::<(), Infallible>(())
            },
        )
        .unwrap_or_else(|e| match e {});
        // sort keys: NA first, then C-locale string order / numeric order
        let rank = |width: usize, range: u64| -> Vec<u32> {
            // rank[v] for v in 1..=range of sprintf("id%0<width>d", v)
            let mut vs: Vec<u32> = (1..=range as u32).collect();
            if digits(range as u32) > width {
                vs.sort_by_cached_key(|&v| format!("{v:0width$}"));
            }
            let mut rank = vec![0u32; range as usize + 1];
            for (i, &v) in vs.iter().enumerate() {
                rank[v as usize] = i as u32 + 1;
            }
            rank
        };
        let mut perm: Vec<u32> = (0..n as u32).collect();
        for c in (0..6).rev() {
            let range = self.range(c);
            let col = &cols[c];
            let ranks = match c {
                0 | 1 => Some(rank(3, range)),
                2 => Some(rank(10, range)),
                _ => None,
            };
            let key = |r: u32| {
                let v = col[r as usize];
                if v == NA {
                    0
                } else if let Some(rk) = &ranks {
                    rk[v as usize]
                } else {
                    v
                }
            };
            counting_pass(&mut perm, range as usize + 1, key);
        }
        cols.into_iter().map(|c| gather(&c, &perm)).collect()
    }

    /// Chunk `j`'s columns: generated, or sliced from the sorted table.
    fn columns(&self, j: u64) -> Vec<Vec<u32>> {
        match &self.plan.sorted {
            Some(s) => {
                let (a, b) = self.chunk_range(j);
                s.iter()
                    .map(|c| c[a as usize..b as usize].to_vec())
                    .collect()
            }
            None => {
                debug_assert_eq!(self.plan.chunk_rows, self.chunk_rows);
                self.generate(&self.plan, j)
            }
        }
    }

    /// Arrow schema: ids 1-3 `Utf8View`, 4-6 and v1-v2 `Int32`, v3
    /// `Float64`; every column nullable when `nas > 0`.
    pub fn schema(&self) -> SchemaRef {
        let null = self.nas > 0;
        Arc::new(Schema::new(
            NAMES
                .iter()
                .enumerate()
                .map(|(c, name)| {
                    let t = match c {
                        0..=2 => DataType::Utf8View,
                        8 => DataType::Float64,
                        _ => DataType::Int32,
                    };
                    Field::new(*name, t, null)
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn batch(&self, cols: &[Vec<u32>], a: usize, b: usize) -> RecordBatch {
        let s = |c: usize| &cols[c][a..b];
        let arrays = vec![
            id_strings(s(0), b"id", 3),
            id_strings(s(1), b"id", 3),
            id_strings(s(2), b"id", 10),
            int32(s(3)),
            int32(s(4)),
            int32(s(5)),
            int32(s(6)),
            int32(s(7)),
            micros(s(8)),
        ];
        RecordBatch::try_new(self.schema(), arrays).expect("schema matches")
    }

    /// Append chunk `j`'s CSV text (with the header for chunk 0).
    pub fn chunk_csv(&self, j: u64, out: &mut Vec<u8>) {
        if j == 0 {
            out.extend_from_slice(NAMES.join(",").as_bytes());
            out.push(b'\n');
        }
        let cols = self.columns(j);
        let rows = cols[0].len();
        let id_w = |w: usize, r: u64| 2 + w.max(digits(r as u32));
        let line = 2 * id_w(3, self.k)
            + id_w(10, self.id3_groups())
            + 2 * digits(self.k as u32)
            + digits(self.groups() as u32)
            + 1
            + 2
            + 16 // v3, and slack for its 16-byte writes
            + COLS;
        write_with(out, rows * line, |w| unsafe {
            for r in 0..rows {
                let id = |w: &mut Cursor, v: u32, width| {
                    if v != NA {
                        w.bytes(b"id");
                        w.padded(v, width);
                    }
                };
                id(w, cols[0][r], 3);
                w.byte(b',');
                id(w, cols[1][r], 3);
                w.byte(b',');
                id(w, cols[2][r], 10);
                for c in cols.iter().take(8).skip(3) {
                    w.byte(b',');
                    let v = c[r];
                    if v != NA {
                        w.uint(v);
                    }
                }
                w.byte(b',');
                let v = cols[8][r];
                if v != NA {
                    w.micros(v);
                }
                w.byte(b'\n');
            }
        });
    }

    /// Write the CSV `fwrite` writes, on `threads` threads (0 = all cores).
    pub fn write_csv<W: Write + Send>(&self, w: W, threads: usize) -> io::Result<W> {
        let mut v = write_text_many(&[self], vec![w], threads)?;
        Ok(v.pop().expect("one writer"))
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
        let cols = self.columns(i);
        let rows = cols[0].len();
        let mut a = 0;
        while a < rows {
            let b = (a + self.batch_size).min(rows);
            emit(self.batch(&cols, a, b));
            a = b;
        }
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
