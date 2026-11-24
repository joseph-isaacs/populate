//! Shared pieces: NA-able `u32` columns, bit sets, the stable parallel
//! sort data.table's `setkey` amounts to, and Arrow arrays from columns.

use arrow_array::{ArrayRef, Float64Array, Int32Array, StringViewArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use std::cmp::Reverse;
use std::ptr::copy_nonoverlapping;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::thread::{available_parallelism, scope};

use crate::fwrite::{write_micros, write_u32, write_u32_padded};

/// NA in a `u32` column.
pub const NA: u32 = u32::MAX;

/// A fixed-size bit set.
#[derive(Debug, Clone)]
pub struct Bits(Vec<u64>);

impl Bits {
    pub fn new(n: u64) -> Self {
        Bits(vec![0; (n as usize).div_ceil(64)])
    }

    #[inline(always)]
    pub fn get(&self, i: u64) -> bool {
        self.0[(i / 64) as usize] >> (i % 64) & 1 == 1
    }

    #[inline(always)]
    pub fn set(&mut self, i: u64) {
        self.0[(i / 64) as usize] |= 1 << (i % 64);
    }
}

/// The distinct values of `col` (1-based, `< domain + 1`) in order of first
/// appearance, as R's `unique()` returns them. Appends to `order`, using
/// `seen` across calls.
pub fn first_appearance(col: &[u32], seen: &mut Bits, order: &mut Vec<u32>) {
    for &v in col {
        if !seen.get(v as u64) {
            seen.set(v as u64);
            order.push(v);
        }
    }
}

/// `col[perm]`.
pub fn gather(col: &[u32], perm: &[u32]) -> Vec<u32> {
    perm.iter().map(|&r| col[r as usize]).collect()
}

fn nulls(col: &[u32]) -> Option<NullBuffer> {
    col.contains(&NA)
        .then(|| NullBuffer::from_iter(col.iter().map(|&v| v != NA)))
}

/// `Int32` array of `col`, NA as null.
pub fn int32(col: &[u32]) -> ArrayRef {
    let v: Vec<i32> = col
        .iter()
        .map(|&v| if v == NA { 0 } else { v as i32 })
        .collect();
    Arc::new(Int32Array::new(ScalarBuffer::from(v), nulls(col)))
}

/// `Float64` array of `col` as micros (`k / 1e6`), NA as null.
pub fn micros(col: &[u32]) -> ArrayRef {
    let v: Vec<f64> = col
        .iter()
        .map(|&v| if v == NA { 0.0 } else { v as f64 / 1e6 })
        .collect();
    Arc::new(Float64Array::new(ScalarBuffer::from(v), nulls(col)))
}

/// `Utf8View` array of `prefix` and `v` zero-padded to `width` digits
/// (`width` 0: no padding), NA as null. Every value fits a view inline
/// (at most 12 bytes), so there are no data buffers.
pub fn id_strings(col: &[u32], prefix: &[u8], width: usize) -> ArrayRef {
    let views: Vec<u128> = col
        .iter()
        .map(|&v| {
            if v == NA {
                return 0;
            }
            let mut b = [0u8; 24];
            b[..prefix.len()].copy_from_slice(prefix);
            let n = prefix.len()
                + unsafe {
                    let p = b.as_mut_ptr().add(prefix.len());
                    if width == 0 {
                        write_u32(v, p)
                    } else {
                        write_u32_padded(v, width, p)
                    }
                };
            assert!(n <= 12, "id string longer than an inline view");
            let mut data = [0u8; 12];
            data[..n].copy_from_slice(&b[..n]);
            (n as u128)
                | (u128::from_le_bytes({
                    let mut w = [0u8; 16];
                    w[4..].copy_from_slice(&data);
                    w
                }))
        })
        .collect();
    // SAFETY: every view is inline, valid UTF-8 (ASCII) of its length.
    Arc::new(unsafe {
        StringViewArray::new_unchecked(ScalarBuffer::from(views), vec![].into(), nulls(col))
    })
}

/// A raw output cursor over a `Vec<u8>`'s spare capacity.
pub struct Cursor {
    p: *mut u8,
}

impl Cursor {
    /// # Safety
    /// Room for one byte.
    #[inline(always)]
    pub unsafe fn byte(&mut self, b: u8) {
        unsafe {
            *self.p = b;
            self.p = self.p.add(1);
        }
    }

    /// # Safety
    /// Room for `b.len()` bytes.
    #[inline(always)]
    pub unsafe fn bytes(&mut self, b: &[u8]) {
        unsafe {
            copy_nonoverlapping(b.as_ptr(), self.p, b.len());
            self.p = self.p.add(b.len());
        }
    }

    /// # Safety
    /// Room for 10 bytes.
    #[inline(always)]
    pub unsafe fn uint(&mut self, v: u32) {
        unsafe { self.p = self.p.add(write_u32(v, self.p)) }
    }

    /// # Safety
    /// Room for `max(width, 10)` bytes.
    #[inline(always)]
    pub unsafe fn padded(&mut self, v: u32, width: usize) {
        unsafe { self.p = self.p.add(write_u32_padded(v, width, self.p)) }
    }

    /// `fwrite` of `v / 1e6`.
    ///
    /// # Safety
    /// Room for 16 bytes.
    #[inline(always)]
    pub unsafe fn micros(&mut self, v: u32) {
        unsafe { self.p = self.p.add(write_micros(v as u64, self.p)) }
    }
}

/// Run `f` with a cursor at the end of `out`, which must have room for
/// `bound` more bytes; `f` must write at most that many.
pub fn write_with(out: &mut Vec<u8>, bound: usize, f: impl FnOnce(&mut Cursor)) {
    out.reserve(bound);
    let start = out.len();
    let base = unsafe { out.as_mut_ptr().add(start) };
    let mut c = Cursor { p: base };
    f(&mut c);
    let n = c.p as usize - base as usize;
    assert!(n <= bound, "text overran its bound");
    // SAFETY: `n` bytes were written into reserved capacity.
    unsafe { out.set_len(start + n) };
}

/// Most buckets [`sort_rows`] splits rows into.
const MAX_BUCKETS: u64 = 1 << 12;

/// Number of threads for `threads` (0 = all cores).
pub fn thread_count(threads: usize) -> usize {
    match threads {
        0 => available_parallelism().map_or(1, |t| t.get()),
        t => t,
    }
}

/// The row order of a stable sort of `n` rows by columns `get(c, r)`
/// (first most significant), each value mapped by its rank table (if any), NA first.
///
/// The leading columns whose combined domain is small (at most
/// [`MAX_BUCKETS`]), and the top of the next column's key, form a bucket
/// number; the rest pack into one `u64` (or `u128`) key per row.
/// One counting pass scatters `(key, row)` pairs into their buckets, which
/// keeps rows in order within each bucket, and the buckets are then sorted
/// by `(key, row)`, independently, on all threads. Rows are unique, so
/// that is the stable order.
pub fn sort_rows(
    n: usize,
    get: impl Fn(usize, usize) -> u32 + Sync,
    ranks: &[Option<Vec<u32>>],
    domains: &[u64],
    threads: usize,
) -> Vec<u32> {
    let cols = domains.len();
    let key = |c: usize, r: usize| -> u64 {
        let v = get(c, r);
        if v == NA {
            0
        } else if let Some(rk) = &ranks[c] {
            rk[v as usize] as u64
        } else {
            v as u64
        }
    };
    let mut lead = 0;
    let mut buckets = 1u64;
    while lead < cols && buckets * domains[lead] <= MAX_BUCKETS {
        buckets *= domains[lead];
        lead += 1;
    }
    // and the top of the next column's key, so buckets stay small; the
    // rest key still holds all of that column, so order within a bucket
    // is right
    let split = (lead < cols && buckets * 2 <= MAX_BUCKETS).then(|| {
        let parts = MAX_BUCKETS / buckets;
        let step = domains[lead].div_ceil(parts);
        (step, domains[lead].div_ceil(step))
    });
    let rest_from = lead;
    if let Some((_, parts)) = split {
        buckets *= parts;
    }
    let rest_bits: f64 = domains[rest_from..]
        .iter()
        .map(|&d| (d as f64).log2())
        .sum();
    let threads = thread_count(threads);
    let bucket_of = |r: usize| -> usize {
        let mut b = 0;
        for (c, &d) in domains[..lead].iter().enumerate() {
            b = b * d + key(c, r);
        }
        if let Some((step, parts)) = split {
            b = b * parts + key(lead, r) / step;
        }
        b as usize
    };
    macro_rules! run {
        ($t:ty) => {{
            let rest_of = |r: usize| -> $t {
                let mut k: $t = 0;
                for c in rest_from..cols {
                    k = k * domains[c] as $t + key(c, r) as $t;
                }
                k
            };
            bucket_sort::<$t>(n, buckets as usize, threads, bucket_of, rest_of)
        }};
    }
    if rest_bits < 63.0 {
        run!(u64)
    } else {
        run!(u128)
    }
}

fn bucket_sort<T: Ord + Copy + Default + Send + Sync>(
    n: usize,
    buckets: usize,
    threads: usize,
    bucket_of: impl Fn(usize) -> usize + Sync,
    rest_of: impl Fn(usize) -> T + Sync,
) -> Vec<u32> {
    // A parallel counting sort: each thread takes a contiguous run of
    // rows, counts its buckets, and scatters its rows to its own offsets
    // (after every earlier thread's rows of the same bucket), so rows stay
    // in order within a bucket.
    let per = n.div_ceil(threads).max(1);
    let parts = n.div_ceil(per);
    let counts: Vec<Vec<usize>> = scope(|s| {
        let hs: Vec<_> = (0..parts)
            .map(|t| {
                let f = &bucket_of;
                s.spawn(move || {
                    let mut c = vec![0usize; buckets];
                    for r in t * per..((t + 1) * per).min(n) {
                        c[f(r)] += 1;
                    }
                    c
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("count")).collect()
    });
    let mut start = vec![0usize; buckets + 1];
    let mut offsets: Vec<Vec<usize>> = vec![vec![0; buckets]; parts];
    let mut at = 0;
    for b in 0..buckets {
        start[b] = at;
        for t in 0..parts {
            offsets[t][b] = at;
            at += counts[t][b];
        }
    }
    start[buckets] = at;
    drop(counts);
    let mut pairs = vec![(T::default(), 0u32); n];
    {
        let out = SharedSlice(pairs.as_mut_ptr());
        scope(|s| {
            for (t, mut off) in offsets.into_iter().enumerate() {
                let (f, g, out) = (&bucket_of, &rest_of, &out);
                s.spawn(move || {
                    for r in t * per..((t + 1) * per).min(n) {
                        let i = &mut off[f(r)];
                        // SAFETY: the offsets give every row its own slot
                        unsafe { out.write(*i, (g(r), r as u32)) };
                        *i += 1;
                    }
                });
            }
        });
    }
    // sort the buckets on all threads, biggest first: with NAs one bucket
    // (key 0) holds nas% of the rows
    let mut order: Vec<usize> = (0..buckets).collect();
    order.sort_unstable_by_key(|&b| Reverse(start[b + 1] - start[b]));
    let next = AtomicUsize::new(0);
    let slices: Vec<&mut [(T, u32)]> = {
        let mut out = Vec::with_capacity(buckets);
        let mut rest: &mut [(T, u32)] = &mut pairs;
        for b in 0..buckets {
            let (h, t) = rest.split_at_mut(start[b + 1] - start[b]);
            out.push(h);
            rest = t;
        }
        out
    };
    let slices: Vec<Mutex<&mut [(T, u32)]>> = slices.into_iter().map(Mutex::new).collect();
    scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    let Some(&b) = order.get(i) else {
                        break;
                    };
                    slices[b].lock().expect("bucket").sort_unstable();
                }
            });
        }
    });
    drop(slices);
    pairs.into_iter().map(|(_, r)| r).collect()
}

/// A slice threads write disjoint elements of.
struct SharedSlice<T>(*mut T);

// SAFETY: writers only touch disjoint indices (see `write`).
unsafe impl<T: Send> Sync for SharedSlice<T> {}

impl<T> SharedSlice<T> {
    /// # Safety
    /// `i` in bounds, and no other thread writes or reads it meanwhile.
    unsafe fn write(&self, i: usize, v: T) {
        unsafe { self.0.add(i).write(v) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn appearance_order() {
        let mut seen = Bits::new(10);
        let mut o = vec![];
        first_appearance(&[3, 1, 3, 7, 1, 2], &mut seen, &mut o);
        assert_eq!(o, [3, 1, 7, 2]);
    }

    #[test]
    fn inline_id_views() {
        let a = id_strings(&[1, NA, 12345], b"id", 10);
        let a = a.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(a.value(0), "id0000000001");
        assert!(a.is_null(1));
        assert_eq!(a.value(2), "id0000012345");
    }
}
