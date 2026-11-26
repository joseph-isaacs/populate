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
use arrow_schema::SchemaRef;
use populate_builtins::{checkpointed, column_as, named};
use populate_core::Distribution;
use populate_table::text::TextChunks;
use populate_table::{Chunks, resolve_threads};
use std::fmt::{self, Debug, Formatter};
use std::mem::take;
use std::sync::mpsc::channel;
use std::sync::{Arc, OnceLock};
use std::thread::{ScopedJoinHandle, scope};

use crate::cols::{Bits, first_appearance, gather, sort_rows, sort_u64};
use crate::groupby::{DEFAULT_BATCH_SIZE, DEFAULT_CHUNK_ROWS};
use crate::rng::{RMt, Runif100, apply_shuffle, sample_int, shuffle_draws};
use crate::values::{Id, Int, Micros, NaSet, Table, header, na_rows, na_values, table};
use crate::{SEED, pretty_sci};

/// One generated table: key columns (`id1..`) and their NA'd keys, and
/// where its value column's draws start, and its NA'd rows.
struct Tab {
    name: String,
    ids: Vec<Vec<u32>>,
    id_na: Vec<NaSet>,
    v: RMt,
    rows: u64,
    v_na: NaSet,
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
    name: String,
    rows: u64,
    chunk_rows: u64,
    table: Arc<dyn Table>,
}

impl Debug for JoinTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinTable")
            .field("name", &self.name)
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

/// `length(from:to)`.
fn seq_len(from: f64, to: f64) -> usize {
    ((to - from).abs() + 1.0 + f32::EPSILON as f64) as usize
}

/// The sizes of `split_xlr(n)`: the permutation, and `x`, `l`, `r`. They
/// depend only on `n`.
fn xlr_lens(n: f64) -> (usize, [usize; 3]) {
    (
        (n * 1.1) as usize,
        [
            seq_len(1.0, n * 0.9),
            seq_len(n * 0.9 + 1.0, n),
            seq_len(n + 1.0, n * 1.1),
        ],
    )
}

/// `split_xlr(n)` from its permutation's draws.
fn split_from(n: f64, js: &[u32]) -> Xlr {
    // sample.int(n * 1.1): size = asVecSize(dn) = (int) dn, a permutation
    let mut key: Vec<u32> = (1..=(n * 1.1) as u32).collect();
    apply_shuffle(&mut key, js);
    let pick = |a: f64, b: f64| -> Vec<u32> { seq_idx(a, b, key.len()).map(|i| key[i]).collect() };
    Xlr {
        x: pick(1.0, n * 0.9),
        l: pick(n * 0.9 + 1.0, n),
        r: pick(n + 1.0, n * 1.1),
    }
}

#[cfg(test)]
fn split_xlr(rng: &mut RMt, n: f64) -> Xlr {
    let js = shuffle_draws(rng, xlr_lens(n).0);
    split_from(n, &js)
}

/// The draws of the script's `sample_all(x, size)` (every value of `x`,
/// then `size - length(x)` draws from it, shuffled). They depend only on
/// `length(x)`, so they are made, in R's order, before the keys exist;
/// [`build`](Self::build) makes the column, on any thread.
struct SampleAll {
    idx: Vec<u32>,
    js: Vec<u32>,
}

/// Rows of `sample_all(x, size)` with `length(x) == len`.
fn sample_all_rows(len: usize, size: f64) -> usize {
    assert!(len as f64 <= size);
    len + (size - len as f64).max(0.0) as usize
}

impl SampleAll {
    fn draw(rng: &mut RMt, len: usize, size: f64) -> Self {
        let mut idx = vec![0u32; sample_all_rows(len, size) - len];
        rng.fill_index(len as u64, &mut idx);
        let js = shuffle_draws(rng, len + idx.len());
        SampleAll { idx, js }
    }

    fn build(self, a: &[u32], b: &[u32]) -> Vec<u32> {
        let mut y: Vec<u32> = a.iter().chain(b).copied().collect();
        y.reserve(self.idx.len());
        for &i in &self.idx {
            let i = i as usize;
            y.push(if i < a.len() { a[i] } else { b[i - a.len()] });
        }
        drop(self.idx);
        apply_shuffle(&mut y, &self.js);
        y
    }
}

/// Build each column from its draws and `(a, b)` keys, in parallel.
fn build_cols(draws: Vec<SampleAll>, keys: Vec<(&[u32], &[u32])>) -> Vec<Vec<u32>> {
    scope(|s| {
        let hs: Vec<_> = draws
            .into_iter()
            .zip(keys)
            .map(|(d, (a, b))| s.spawn(move || d.build(a, b)))
            .collect();
        hs.into_iter()
            .map(|h| h.join().expect("sample_all"))
            .collect()
    })
}

#[cfg(test)]
fn sample_all(rng: &mut RMt, a: &[u32], b: &[u32], size: f64) -> Vec<u32> {
    SampleAll::draw(rng, a.len() + b.len(), size).build(a, b)
}

/// `setkeyv(d, cols)`: stable sort of the rows by `keys` (indices into
/// `ids`), first most significant.
///
/// The key columns and the row number pack into one `u64` (most
/// significant first), so a plain sort of those is the stable sort; when
/// the key is every column, ties are identical rows and the row number is
/// left out. Other columns are gathered by the sorted row numbers.
fn sort_by(ids: &mut [Vec<u32>], keys: &[usize]) {
    let n = ids[0].len();
    let width = |c: &[u32]| 32 - c.iter().copied().max().unwrap_or(0).leading_zeros();
    let widths: Vec<u32> = keys.iter().map(|&k| width(&ids[k])).collect();
    let all = keys.len() == ids.len();
    let row_bits = if all {
        0
    } else {
        32 - (n as u32).leading_zeros()
    };
    let bits = widths.iter().sum::<u32>() + row_bits;
    if bits > 64 {
        let domains: Vec<u64> = widths.iter().map(|&w| 1u64 << w).collect();
        let ranks = vec![None; keys.len()];
        let perm = sort_rows(n, |k, r| ids[keys[k]][r], &ranks, &domains, 0);
        scope(|s| {
            for c in ids.iter_mut() {
                let perm = &perm;
                s.spawn(move || *c = gather(c, perm));
            }
        });
        return;
    }
    let mut packed = vec![0u64; n];
    let per = n.div_ceil(resolve_threads(0)).max(1);
    scope(|s| {
        for (t, part) in packed.chunks_mut(per).enumerate() {
            let (ids, keys, widths) = (&*ids, keys, &widths);
            s.spawn(move || {
                for (i, p) in part.iter_mut().enumerate() {
                    let r = t * per + i;
                    let mut k = 0u64;
                    for (&c, &w) in keys.iter().zip(widths) {
                        k = (k << w) | ids[c][r] as u64;
                    }
                    *p = if all { k } else { (k << row_bits) | r as u64 };
                }
            });
        }
    });
    let sorted = sort_u64(&packed, bits, 0);
    drop(packed);
    // unpack the keys; gather the other columns by row
    let row_mask = (1u64 << row_bits) - 1;
    let mut shift = vec![0u32; keys.len()];
    let mut at = row_bits;
    for i in (0..keys.len()).rev() {
        shift[i] = at;
        at += widths[i];
    }
    scope(|s| {
        for (c, col) in ids.iter_mut().enumerate() {
            let (sorted, keys, shift, widths) = (&sorted, keys, &shift, &widths);
            s.spawn(move || {
                if let Some(i) = keys.iter().position(|&k| k == c) {
                    let mask = (1u64 << widths[i]) - 1;
                    for (d, &k) in col.iter_mut().zip(sorted) {
                        *d = ((k >> shift[i]) & mask) as u32;
                    }
                } else {
                    let old = take(col);
                    *col = sorted
                        .iter()
                        .map(|&k| old[(k & row_mask) as usize])
                        .collect();
                }
            });
        }
    });
}

impl Join {
    /// `join-datagen.R n 0 nas sort`; generates every table's keys and
    /// values (sequential: R's draws are).
    pub fn new(n: u64, nas: u32, sort: bool) -> Self {
        Self::with_chunk_rows(n, nas, sort, DEFAULT_CHUNK_ROWS)
    }

    /// [`new`](Self::new) with `chunk_rows` rows per chunk.
    pub fn with_chunk_rows(n: u64, nas: u32, sort: bool, chunk_rows: u64) -> Self {
        // the script's own limits: N >= 1e7, uniqueN(id1) == N/1e6 (which
        // holds for multiples of 1e7), and
        // handle_batches takes N <= 1e8 or exactly 1e9
        assert!(
            n >= 10_000_000 && n.is_multiple_of(10_000_000),
            "the join datagen needs N >= 1e7, a multiple of 1e7"
        );
        assert!(
            n <= 100_000_000 || n == 1_000_000_000,
            "the join datagen takes N <= 1e8 or N = 1e9"
        );
        assert!(nas <= 100 && chunk_rows > 0);
        Join {
            tables: generate(n, nas, sort, chunk_rows),
        }
    }

    /// The left table, then the small, medium and big right tables.
    pub fn tables(&self) -> Vec<JoinTable> {
        self.tables.clone()
    }
}

/// Past this many rows the join is built a column, then a table, at a
/// time, to bound memory (otherwise the draws of every column are held).
const BOUNDED_ROWS: u64 = 1 << 27;

/// The left table's key columns: drawn, or (bounded) already built.
enum Lhs {
    Drawn(Vec<SampleAll>),
    Built(Vec<Vec<u32>>),
}

/// The NA draws for the left table: per id column, indices into its
/// distinct values (whose count is known: every key of `x` and `l`); and
/// the rows of `v1`.
struct NaDraws {
    /// Distinct values the draws assumed, per id column.
    distinct: Vec<usize>,
    ids: Vec<Option<Vec<u32>>>,
    rows: Option<Vec<u32>>,
}

/// Every draw R makes, in R's order, depends only on sizes known up front
/// (the keys' counts, and the left table's distinct values per column,
/// which are all of its keys). So one thread makes all the draws, in
/// order, while the tables are built from them on the others: the
/// permutations' swaps, the resampled columns, sorting, values and NAs.
fn generate(n: u64, nas: u32, sort: bool, chunk_rows: u64) -> Vec<JoinTable> {
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
    let lens: Vec<(usize, [usize; 3])> = sizes.iter().map(|&s| xlr_lens(s)).collect();
    let xl = |t: usize| lens[t].1[0] + lens[t].1[1];
    let xr = |t: usize| lens[t].1[0] + lens[t].1[2];
    let frac = nas as f64 / 100.0;
    let keys_cell = OnceLock::new();
    let bounded = n > BOUNDED_ROWS;

    scope(|s| {
        let key_hs: Vec<_> = sizes
            .iter()
            .zip(&lens)
            .map(|(&size, &(m, _))| {
                let js = shuffle_draws(&mut rng, m);
                s.spawn(move || split_from(size, &js))
            })
            .collect();
        let mut key_hs = Some(key_hs);
        let mut join_keys = || -> &Vec<Xlr> {
            keys_cell.get_or_init(|| {
                key_hs
                    .take()
                    .expect("once")
                    .into_iter()
                    .map(|h| h.join().expect("keys"))
                    .collect()
            })
        };
        // left: its key columns, then v1, then the NAs. Large tables build
        // each column as soon as it is drawn (its draws take 8 bytes a
        // row), and each table before the next is drawn.
        let lhs = if bounded {
            let keys = join_keys();
            Lhs::Built(
                (0..3)
                    .map(|t| SampleAll::draw(&mut rng, xl(t), nf).build(&keys[t].x, &keys[t].l))
                    .collect(),
            )
        } else {
            Lhs::Drawn(
                (0..3)
                    .map(|t| SampleAll::draw(&mut rng, xl(t), nf))
                    .collect(),
            )
        };
        let v1 = rng.clone();
        rng.skip(n);
        let keys: &Vec<Xlr> = join_keys();
        let lhs_name = data_name.clone();
        let (na_tx, na_rx) = channel::<NaDraws>();
        let lhs_h = s.spawn(move || {
            let mut ids = match lhs {
                Lhs::Built(ids) => ids,
                Lhs::Drawn(d) => build_cols(d, keys.iter().map(|k| (&k.x[..], &k.l[..])).collect()),
            };
            if sort {
                sort_by(&mut ids, &[0, 1, 2]);
            }
            let (id_na, v_na) = if nas > 0 {
                nas_of(&ids, n, na_rx.recv().expect("NA draws"))
            } else {
                (vec![None; 3], None)
            };
            let t = Tab {
                name: lhs_name,
                ids,
                id_na,
                v: v1,
                rows: n,
                v_na,
                v_name: "v1",
            };
            JoinTable::new(t, true, chunk_rows)
        });
        if nas > 0 {
            let ids = (0..3)
                .map(|t| {
                    let nna = (xl(t) as f64 * frac) as u64;
                    (nna > 0).then(|| sample_int(&mut rng, xl(t) as u64, nna))
                })
                .collect();
            let nna = (nf * frac) as u64;
            let rows = (nna > 0).then(|| sample_int(&mut rng, n, nna));
            let distinct = (0..3).map(xl).collect();
            na_tx
                .send(NaDraws {
                    distinct,
                    ids,
                    rows,
                })
                .expect("left table");
        }
        let mut lhs_h = Some(lhs_h);
        let mut tabs = vec![];
        if bounded {
            tabs.push(lhs_h.take().expect("once").join().expect("left table"));
        }
        // right: small (id1), medium (id1, id2; key id2), big (key id3)
        let mut rhs_hs: Vec<_> = vec![];
        for (t, &size) in sizes.iter().enumerate() {
            if bounded {
                tabs.extend(
                    rhs_hs
                        .drain(..)
                        .map(|h: ScopedJoinHandle<JoinTable>| h.join().expect("right table")),
                );
            }
            rhs_hs.push({
                let draws: Vec<SampleAll> = (0..=t)
                    .map(|k| SampleAll::draw(&mut rng, xr(k), size))
                    .collect();
                let rows = sample_all_rows(xr(t), size);
                let v2 = rng.clone();
                rng.skip(rows as u64);
                let name = data_name.replace("NA", &right_names[t]);
                s.spawn(move || {
                    let mut ids = build_cols(
                        draws,
                        keys[..=t].iter().map(|k| (&k.x[..], &k.r[..])).collect(),
                    );
                    if sort {
                        sort_by(&mut ids, &[t]);
                    }
                    let t = Tab {
                        name,
                        ids,
                        id_na: vec![None; t + 1],
                        v: v2,
                        rows: rows as u64,
                        v_na: None,
                        v_name: "v2",
                    };
                    JoinTable::new(t, false, chunk_rows)
                })
            });
        }
        if let Some(h) = lhs_h {
            tabs.insert(0, h.join().expect("left table"));
        }
        tabs.extend(rhs_hs.into_iter().map(|h| h.join().expect("right table")));
        tabs
    })
}

/// The left table's NAs: each id column's drawn distinct values (indices
/// into its values in order of first appearance), and `v1`'s drawn rows.
fn nas_of(ids: &[Vec<u32>], rows: u64, na: NaDraws) -> (Vec<NaSet>, NaSet) {
    scope(|s| {
        let hs: Vec<_> = ids
            .iter()
            .zip(na.ids)
            .zip(na.distinct)
            .map(|((col, draws), distinct)| {
                s.spawn(move || {
                    let draws = draws?;
                    let max = col.iter().copied().max().unwrap_or(0) as u64;
                    let mut seen = Bits::new(max + 1);
                    let mut ucol = vec![];
                    first_appearance(col, &mut seen, &mut ucol);
                    drop(seen);
                    assert_eq!(ucol.len(), distinct, "every key appears once at least");
                    let mut drop_set = Bits::new(max + 1);
                    for i in draws {
                        drop_set.set(ucol[i as usize - 1] as u64);
                    }
                    Some(Arc::new(drop_set))
                })
            })
            .collect();
        let v = na.rows.map(|r| {
            let mut b = Bits::new(rows);
            for r in r {
                b.set(r as u64 - 1);
            }
            Arc::new(b)
        });
        (hs.into_iter().map(|h| h.join().expect("NAs")).collect(), v)
    })
}

/// A key column, as the number (`id1..`), its keys in `na` NA.
fn key<const NULL: bool>((c, na): &(Arc<Vec<u32>>, NaSet)) -> impl Col<Int<NULL>> {
    na_values(column_as(c.clone()), na.clone())
}

/// A key column as `"id<number>"` (`id4..`): the same column.
fn key_name<const NULL: bool>((c, na): &(Arc<Vec<u32>>, NaSet)) -> impl Col<Id<0, NULL>> {
    na_values(column_as(c.clone()), na.clone())
}

/// Bounds of a table's column distributions.
trait Col<T>: Distribution<Item = T> + Clone + Send + Sync + 'static {}

impl<T, D: Distribution<Item = T> + Clone + Send + Sync + 'static> Col<T> for D {}

/// `t` as one distribution: `id1..idp` (`Int32`), `id4..` (the same keys
/// as `"id<key>"`, `Utf8View`), the value (`Float64`). Only the left
/// table has NAs (`NULL`).
fn dist_table<const NULL: bool>(t: Tab, chunk_rows: u64) -> Arc<dyn Table> {
    let ids: Vec<(Arc<Vec<u32>>, NaSet)> = t.ids.into_iter().map(Arc::new).zip(t.id_na).collect();
    let rows = t.rows;
    // `round(runif(rows, max = 100), 6)`, its state saved every chunk
    let (v, _) = checkpointed(Runif100::<Micros<NULL>>::new(t.v), chunk_rows, rows);
    let v = na_rows(v, t.v_na);
    let (n, b) = (&t.name, DEFAULT_BATCH_SIZE);
    let vn = t.v_name;
    match &ids[..] {
        [a] => table(
            n,
            rows,
            b,
            (
                named("id1", key::<NULL>(a)),
                named("id4", key_name::<NULL>(a)),
                named(vn, v.clone()),
            ),
        ),
        [a, c] => table(
            n,
            rows,
            b,
            (
                named("id1", key::<NULL>(a)),
                named("id2", key::<NULL>(c)),
                named("id4", key_name::<NULL>(a)),
                named("id5", key_name::<NULL>(c)),
                named(vn, v.clone()),
            ),
        ),
        [a, c, d] => table(
            n,
            rows,
            b,
            (
                named("id1", key::<NULL>(a)),
                named("id2", key::<NULL>(c)),
                named("id3", key::<NULL>(d)),
                named("id4", key_name::<NULL>(a)),
                named("id5", key_name::<NULL>(c)),
                named("id6", key_name::<NULL>(d)),
                named(vn, v.clone()),
            ),
        ),
        _ => unreachable!("1 to 3 key columns"),
    }
}

impl JoinTable {
    fn new(t: Tab, left: bool, chunk_rows: u64) -> Self {
        let name = t.name.clone();
        let rows = t.rows;
        let table = if left {
            dist_table::<true>(t, chunk_rows)
        } else {
            dist_table::<false>(t, chunk_rows)
        };
        JoinTable {
            name,
            rows,
            chunk_rows,
            table,
        }
    }

    /// Rows per Arrow batch.
    pub fn batch_size(mut self, rows: usize) -> Self {
        assert!(rows > 0);
        self.table = self.table.with_batch_rows(rows as u64);
        self
    }

    /// The datagen's file name, e.g. `J1_1e7_1e1_0_0.csv`.
    pub fn file_name(&self) -> String {
        format!("{}.csv", self.name)
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows).max(1)
    }

    fn chunk_range(&self, j: u64) -> (u64, u64) {
        let s = j * self.chunk_rows;
        (s, (s + self.chunk_rows).min(self.rows))
    }

    /// Arrow schema, read off the table's distribution: `id1..` `Int32`,
    /// `id4..` (`"id<id1>"`) `Utf8View`, the value `Float64`. Only the left
    /// table is nullable.
    pub fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    /// Append chunk `j`'s CSV text (with the header for chunk 0).
    pub fn chunk_csv(&self, j: u64, out: &mut Vec<u8>) {
        if j == 0 {
            header(&self.schema(), out);
        }
        let (a, b) = self.chunk_range(j);
        self.table.csv(a, b, out);
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
        let (a, b) = self.chunk_range(i);
        self.table.arrow(a, b, emit)
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

#[cfg(test)]
mod sort_tests {
    use super::*;

    #[test]
    fn sort_by_is_stable_lexicographic() {
        let mut rng = RMt::set_seed(1);
        let n = 5000;
        let mk = |rng: &mut RMt, m: u64| {
            let mut v = vec![0u32; n];
            rng.fill_index(m, &mut v);
            v
        };
        let cols = vec![mk(&mut rng, 7), mk(&mut rng, 300), mk(&mut rng, 100_000)];
        for keys in [vec![0, 1, 2], vec![1], vec![2]] {
            let mut got = cols.clone();
            sort_by(&mut got, &keys);
            let mut rows: Vec<usize> = (0..n).collect();
            rows.sort_by_key(|&r| keys.iter().map(|&k| cols[k][r]).collect::<Vec<_>>());
            for c in 0..3 {
                let want: Vec<u32> = rows.iter().map(|&r| cols[c][r]).collect();
                assert_eq!(got[c], want, "keys {keys:?} col {c}");
            }
        }
    }
}
