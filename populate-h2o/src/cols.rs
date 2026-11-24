//! Shared pieces: NA-able `u32` columns, bit sets, the stable counting sort
//! data.table's `setkey` amounts to, and Arrow arrays from columns.

use arrow_array::{ArrayRef, Float64Array, Int32Array, StringViewArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use std::sync::Arc;

use crate::fwrite::{write_u32, write_u32_padded};

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

/// One stable counting-sort pass: reorder `perm` (row numbers) by
/// `key(row)` in `0..domain`, ties in their current order.
pub fn counting_pass(perm: &mut Vec<u32>, domain: usize, key: impl Fn(u32) -> u32) {
    let mut count = vec![0u32; domain + 1];
    for &r in perm.iter() {
        count[key(r) as usize + 1] += 1;
    }
    for i in 1..=domain {
        count[i] += count[i - 1];
    }
    let mut out = vec![0u32; perm.len()];
    for &r in perm.iter() {
        let k = key(r) as usize;
        out[count[k] as usize] = r;
        count[k] += 1;
    }
    *perm = out;
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
    pub p: *mut u8,
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
            std::ptr::copy_nonoverlapping(b.as_ptr(), self.p, b.len());
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
        unsafe { self.p = self.p.add(crate::fwrite::write_micros(v as u64, self.p)) }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn stable_counting_sort() {
        let keys = [3u32, 1, 3, 0, 1, 2];
        let mut perm: Vec<u32> = (0..6).collect();
        counting_pass(&mut perm, 4, |r| keys[r as usize]);
        assert_eq!(perm, [3, 1, 4, 5, 0, 2]);
    }

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
