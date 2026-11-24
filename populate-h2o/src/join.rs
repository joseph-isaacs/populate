//! The join data sets: `join-datagen.R N 0 nas sort`.
//!
//! Keys come from three permutations (`split_xlr`: 90% shared by both
//! sides, 10% only left, 10% only right), the left table's key columns and
//! each right table's from `sample_all` (every key once, the rest drawn
//! with replacement, then shuffled). Each step is a permutation or depends
//! on all earlier draws, so the key columns are generated sequentially,
//! in memory (they are what R holds in memory too); the value columns are
//! one `runif` each. Formatting, the bulk of the work, runs on all cores.
//!
//! Four tables: `J1_<N>_NA_<nas>_<sort>` (N rows) and the right-hand
//! tables `small`, `medium` and `big` (N/1e6, N/1e3 and N rows), named
//! with that row count in place of `NA`.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_table::Chunks;
use populate_table::text::TextChunks;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::cols::{
    Bits, Cursor, NA, counting_pass, first_appearance, gather, id_strings, int32, micros,
    write_with,
};
use crate::fwrite::{digits, round6_micros};
use crate::groupby::{DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS};
use crate::rng::{RMt, sample_int, sample_noreplace, shuffle};
use crate::{SEED, pretty_sci};

/// One generated table: key columns (`id1..`), and its value column.
struct Tab {
    name: String,
    ids: Vec<Vec<u32>>,
    v: Vec<u32>,
    /// `v1` (left) or `v2` (right).
    v_name: &'static str,
}

/// The join data set for one `(N, nas, sort)`: four tables.
#[derive(Debug, Clone)]
pub struct Join {
    tables: Vec<JoinTable>,
}

/// One of the join data set's tables.
#[derive(Clone)]
pub struct JoinTable {
    data: Arc<Vec<Tab>>,
    which: usize,
    chunk_rows: u64,
    batch_size: usize,
}

impl Debug for JoinTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinTable")
            .field("name", &self.tab().name)
            .finish()
    }
}

/// R's `seq.int(from, to)` (`from:to`) as 0-based indices into a vector
/// of `len` (R truncates double indices).
fn seq_idx(from: f64, to: f64, len: usize) -> impl Iterator<Item = usize> {
    let r = (to - from).abs();
    let n = (r + 1.0 + f32::EPSILON as f64) as usize;
    let up = from <= to;
    (0..n).map(move |i| {
        let v = if up { from + i as f64 } else { from - i as f64 };
        let i = v as usize;
        assert!(i >= 1 && i <= len, "index {v} out of 1..={len}");
        i - 1
    })
}

/// The script's `split_xlr(n)`: a permutation of `1..=n*1.1`, split.
struct Xlr {
    x: Vec<u32>,
    l: Vec<u32>,
    r: Vec<u32>,
}

fn split_xlr(rng: &mut RMt, n: f64) -> Xlr {
    let dn = n * 1.1;
    // sample.int(dn): size = asVecSize(dn), n = (int) dn
    let key = sample_noreplace(rng, dn as u64, dn as u64);
    let pick = |a: f64, b: f64| -> Vec<u32> { seq_idx(a, b, key.len()).map(|i| key[i]).collect() };
    Xlr {
        x: pick(1.0, n * 0.9),
        l: pick(n * 0.9 + 1.0, n),
        r: pick(n + 1.0, n * 1.1),
    }
}

/// The script's `sample_all(x, size)`: every value of `x`, then
/// `size - length(x)` draws from it, shuffled.
fn sample_all(rng: &mut RMt, a: &[u32], b: &[u32], size: f64) -> Vec<u32> {
    let x: Vec<u32> = a.iter().chain(b).copied().collect();
    assert!(x.len() as f64 <= size);
    let more = (size - x.len() as f64).max(0.0) as usize;
    let mut y = Vec::with_capacity(x.len() + more);
    y.extend_from_slice(&x);
    let mut idx = vec![0u32; more];
    rng.fill_index(x.len() as u64, &mut idx);
    y.extend(idx.iter().map(|&i| x[i as usize]));
    shuffle(rng, &mut y);
    y
}

/// `round(runif(n, max = 100), 6)` as micros.
fn values(rng: &mut RMt, n: usize) -> Vec<u32> {
    let mut u = vec![0f64; n];
    rng.fill_unif(&mut u);
    u.iter().map(|&u| round6_micros(100.0 * u) as u32).collect()
}

/// `setkeyv(d, cols)`: stable sort of the rows by `keys` (indices into
/// `ids`), first most significant.
fn sort_by(ids: &mut [Vec<u32>], keys: &[usize]) {
    let n = ids[0].len();
    let mut perm: Vec<u32> = (0..n as u32).collect();
    for &c in keys.iter().rev() {
        let col = &ids[c];
        let max = col.iter().copied().max().unwrap_or(0) as usize;
        counting_pass(&mut perm, max + 1, |r| col[r as usize]);
    }
    for c in ids.iter_mut() {
        *c = gather(c, &perm);
    }
}

impl Join {
    /// `join-datagen.R n 0 nas sort`; generates every table's keys and
    /// values (sequential: R's draws are).
    pub fn new(n: u64, nas: u32, sort: bool) -> Self {
        Self::with_chunk_rows(n, nas, sort, DEFAULT_CHUNK_ROWS)
    }

    /// [`new`](Self::new) with `chunk_rows` rows per chunk.
    pub fn with_chunk_rows(n: u64, nas: u32, sort: bool, chunk_rows: u64) -> Self {
        // the script's own limits: N >= 1e7, uniqueN(id1) == N/1e6, and
        // handle_batches takes N <= 1e8 or exactly 1e9
        assert!(
            n >= 10_000_000 && n.is_multiple_of(1_000_000),
            "the join datagen needs N >= 1e7, a multiple of 1e6"
        );
        assert!(
            n <= 100_000_000 || n == 1_000_000_000,
            "the join datagen takes N <= 1e8 or N = 1e9"
        );
        assert!(nas <= 100 && chunk_rows > 0);
        let data = Arc::new(generate(n, nas, sort));
        Join {
            tables: (0..4)
                .map(|which| JoinTable {
                    data: data.clone(),
                    which,
                    chunk_rows,
                    batch_size: DEFAULT_BATCH_SIZE,
                })
                .collect(),
        }
    }

    /// The left table, then the small, medium and big right tables.
    pub fn tables(&self) -> Vec<JoinTable> {
        self.tables.clone()
    }
}

fn generate(n: u64, nas: u32, sort: bool) -> Vec<Tab> {
    let nf = n as f64;
    let mut rng = RMt::set_seed(SEED);
    let x_name = pretty_sci(n);
    let data_name = format!("J1_{x_name}_NA_{nas}_{}", sort as u8);
    // join_to_tbls: the right tables are named from the left's name,
    // `<d>e<p>`, as `<d>e<p - 6>`, `<d>e<p - 3>`, `<d>e<p>`
    let (d, p) = x_name.split_once('e').expect("pretty name");
    let p: u32 = p.parse().expect("exponent");
    let right_names = [p - 6, p - 3, p].map(|e| format!("{d}e{e}"));
    let sizes = [nf / 1e6, nf / 1e3, nf];
    let keys: Vec<Xlr> = sizes.iter().map(|&s| split_xlr(&mut rng, s)).collect();

    // left
    let mut ids: Vec<Vec<u32>> = keys
        .iter()
        .map(|k| sample_all(&mut rng, &k.x, &k.l, nf))
        .collect();
    if sort {
        sort_by(&mut ids, &[0, 1, 2]);
    }
    let mut v = values(&mut rng, n as usize);
    if nas > 0 {
        let frac = nas as f64 / 100.0;
        for col in ids.iter_mut() {
            let max = col.iter().copied().max().unwrap_or(0) as u64;
            let mut seen = Bits::new(max + 1);
            let mut ucol = vec![];
            first_appearance(col, &mut seen, &mut ucol);
            let nna = (ucol.len() as f64 * frac) as u64;
            if nna > 0 {
                let mut drop = Bits::new(max + 1);
                for i in sample_int(&mut rng, ucol.len() as u64, nna) {
                    drop.set(ucol[i as usize - 1] as u64);
                }
                for x in col.iter_mut() {
                    if drop.get(*x as u64) {
                        *x = NA;
                    }
                }
            }
        }
        let nna = (nf * frac) as u64;
        if nna > 0 {
            for r in sample_int(&mut rng, n, nna) {
                v[r as usize - 1] = NA;
            }
        }
    }
    let mut tabs = vec![Tab {
        name: data_name.clone(),
        ids,
        v,
        v_name: "v1",
    }];

    // right: small (id1), medium (id1, id2; key id2), big (key id3)
    for (t, &size) in sizes.iter().enumerate() {
        let mut ids: Vec<Vec<u32>> = keys[..=t]
            .iter()
            .map(|k| sample_all(&mut rng, &k.x, &k.r, size))
            .collect();
        if sort {
            sort_by(&mut ids, &[t]);
        }
        let v = values(&mut rng, ids[0].len());
        tabs.push(Tab {
            name: data_name.replace("NA", &right_names[t]),
            ids,
            v,
            v_name: "v2",
        });
    }
    tabs
}

impl JoinTable {
    fn tab(&self) -> &Tab {
        &self.data[self.which]
    }

    /// Rows per Arrow batch.
    pub fn batch_size(mut self, rows: usize) -> Self {
        assert!(rows > 0);
        self.batch_size = rows;
        self
    }

    /// The datagen's file name, e.g. `J1_1e7_1e1_0_0.csv`.
    pub fn file_name(&self) -> String {
        format!("{}.csv", self.tab().name)
    }

    pub fn rows(&self) -> u64 {
        self.tab().v.len() as u64
    }

    pub fn num_chunks(&self) -> u64 {
        self.rows().div_ceil(self.chunk_rows).max(1)
    }

    fn chunk_range(&self, j: u64) -> (usize, usize) {
        let s = j * self.chunk_rows;
        (s as usize, (s + self.chunk_rows).min(self.rows()) as usize)
    }

    fn names(&self) -> Vec<String> {
        let p = self.tab().ids.len();
        (1..=p)
            .map(|i| format!("id{i}"))
            .chain((4..4 + p).map(|i| format!("id{i}")))
            .chain([self.tab().v_name.to_string()])
            .collect()
    }

    /// Arrow schema: `id1..` `Int32`, `id4..` (`"id<id1>"`) `Utf8View`, the
    /// value `Float64`. Only the left table has NAs.
    pub fn schema(&self) -> SchemaRef {
        let p = self.tab().ids.len();
        let null = self.which == 0;
        Arc::new(Schema::new(
            self.names()
                .into_iter()
                .enumerate()
                .map(|(i, name)| {
                    let t = if i < p {
                        DataType::Int32
                    } else if i < 2 * p {
                        DataType::Utf8View
                    } else {
                        DataType::Float64
                    };
                    Field::new(name, t, null)
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Append chunk `j`'s CSV text (with the header for chunk 0).
    pub fn chunk_csv(&self, j: u64, out: &mut Vec<u8>) {
        let t = self.tab();
        if j == 0 {
            out.extend_from_slice(self.names().join(",").as_bytes());
            out.push(b'\n');
        }
        let (a, b) = self.chunk_range(j);
        let width: usize = t
            .ids
            .iter()
            .map(|c| {
                2 * digits(
                    c[a..b]
                        .iter()
                        .copied()
                        .filter(|&v| v != NA)
                        .max()
                        .unwrap_or(0),
                ) + 4
            })
            .sum();
        let line = width + 16 + 1;
        write_with(out, (b - a) * line, |w: &mut Cursor| unsafe {
            for r in a..b {
                for c in &t.ids {
                    let v = c[r];
                    if v != NA {
                        w.uint(v);
                    }
                    w.byte(b',');
                }
                for c in &t.ids {
                    let v = c[r];
                    if v != NA {
                        w.bytes(b"id");
                        w.uint(v);
                    }
                    w.byte(b',');
                }
                let v = t.v[r];
                if v != NA {
                    w.micros(v);
                }
                w.byte(b'\n');
            }
        });
    }
}

impl Chunks for JoinTable {
    fn schema(&self) -> SchemaRef {
        JoinTable::schema(self)
    }

    fn num_chunks(&self) -> u64 {
        JoinTable::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let t = self.tab();
        let (a, b) = self.chunk_range(i);
        let mut s = a;
        while s < b {
            let e = (s + self.batch_size).min(b);
            let mut arrays: Vec<_> = t.ids.iter().map(|c| int32(&c[s..e])).collect();
            arrays.extend(t.ids.iter().map(|c| id_strings(&c[s..e], b"id", 0)));
            arrays.push(micros(&t.v[s..e]));
            emit(RecordBatch::try_new(self.schema(), arrays).expect("schema matches"));
            s = e;
        }
    }
}

impl TextChunks for JoinTable {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        JoinTable::num_chunks(self)
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        self.chunk_csv(i, &mut bufs[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // see the R snippet in the comment of each assertion: set.seed(108), then
    // k = split_xlr(10); sample_all(c(k$x, k$l), 25); k2 = split_xlr(1e4);
    // head(k2$x); head(k2$r); sample_all(c(k$x, k$r), 10)
    #[test]
    fn keys_match_r() {
        let mut rng = RMt::set_seed(108);
        let k = split_xlr(&mut rng, 10.0);
        assert_eq!(k.x, [10, 7, 6, 5, 11, 8, 9, 3, 4]);
        assert_eq!(k.l, [2]);
        assert_eq!(k.r, [1]);
        assert_eq!(
            sample_all(&mut rng, &k.x, &k.l, 25.0),
            [
                5, 6, 8, 6, 4, 3, 2, 2, 6, 8, 3, 9, 11, 11, 11, 4, 9, 10, 9, 8, 8, 9, 5, 7, 8
            ]
        );
        let k2 = split_xlr(&mut rng, 1e4);
        assert_eq!(k2.x[..6], [3353, 8047, 10873, 9396, 6393, 741]);
        assert_eq!(k2.r[..6], [3128, 2947, 8111, 4873, 10596, 10932]);
        assert_eq!(
            sample_all(&mut rng, &k.x, &k.r, 10.0),
            [4, 8, 5, 9, 7, 1, 6, 11, 10, 3]
        );
    }

    #[test]
    fn r_colon() {
        let v: Vec<usize> = seq_idx(1.0, 9.0, 11).collect();
        assert_eq!(v, (0..9).collect::<Vec<_>>());
        // 10*1.1 = 11.000000000000002: 11:11.000000000000002 is just 11
        let v: Vec<usize> = seq_idx(11.0, 10.0 * 1.1, 11).collect();
        assert_eq!(v, [10]);
        let v: Vec<usize> = seq_idx(2.5, 4.0, 11).collect();
        assert_eq!(v, [1, 2]);
    }
}
