//! Columnar string sink.
//!
//! [`StrCol`] is the sink for `String` and `&'static str` items. It is laid
//! out exactly like an Arrow `Utf8` array: `i32` offsets (`n + 1` entries,
//! first is 0) and one contiguous byte buffer. Distributions that produce
//! strings write their bytes straight into that buffer with
//! [`StrCol::push_with`], so a string column costs two allocations in total,
//! not one per value, and both buffers move into Arrow without a copy.

use crate::columnar::Columnar;
use crate::list::offset;
use crate::sink::Sink;

/// String column in Arrow `Utf8` layout.
///
/// Every byte goes in through `&str`, `char` or a checked ASCII byte, so the
/// buffer is always valid UTF-8 and the offsets always increase; Arrow
/// conversion can skip validation.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StrCol {
    offsets: Vec<i32>,
    data: Vec<u8>,
}

/// `v.extend_from_slice(s)`, with strings up to 16 bytes copied by two
/// overlapping fixed-size loads into a 16-byte block (no `memcpy` call,
/// which dominates for short values like category names).
#[inline(always)]
fn extend_small(v: &mut Vec<u8>, s: &[u8]) {
    let n = s.len();
    if n > 16 {
        v.extend_from_slice(s);
        return;
    }
    let mut b = [0u8; 16];
    if n >= 8 {
        b[..8].copy_from_slice(&s[..8]);
        b[n - 8..n].copy_from_slice(&s[n - 8..]);
    } else if n >= 4 {
        b[..4].copy_from_slice(&s[..4]);
        b[n - 4..n].copy_from_slice(&s[n - 4..]);
    } else if n > 0 {
        b[0] = s[0];
        b[n / 2] = s[n / 2];
        b[n - 1] = s[n - 1];
    }
    let start = v.len();
    v.extend_from_slice(&b);
    v.truncate(start + n);
}

/// Write access to the value being appended by [`StrCol::push_with`].
pub struct StrBuf<'a>(&'a mut Vec<u8>, usize);

impl StrBuf<'_> {
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        extend_small(self.0, s.as_bytes());
    }

    #[inline]
    pub fn push_char(&mut self, c: char) {
        let mut b = [0u8; 4];
        self.0.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
    }

    /// Append one ASCII byte. Panics on a non-ASCII byte (one compare, so the
    /// buffer stays valid UTF-8 without a validation pass).
    #[inline]
    pub fn push_ascii(&mut self, b: u8) {
        assert!(b.is_ascii(), "push_ascii: byte {b:#x} is not ASCII");
        self.0.push(b);
    }

    /// Append ASCII bytes. Panics if any byte is not ASCII.
    #[inline]
    pub fn push_ascii_bytes(&mut self, bytes: &[u8]) {
        assert!(bytes.is_ascii(), "push_ascii_bytes: input is not ASCII");
        self.0.extend_from_slice(bytes);
    }

    /// Append ASCII bytes from an iterator of known length (a `map` over a
    /// `Range`, say): one capacity check for the lot instead of one per
    /// byte, then one vectorised ASCII check of what was written. Panics,
    /// with the buffer as it was, if any byte is not ASCII.
    #[inline]
    pub fn extend_ascii(&mut self, bytes: impl Iterator<Item = u8>) {
        let start = self.0.len();
        self.0.extend(bytes);
        if !self.0[start..].is_ascii() {
            self.0.truncate(start);
            panic!("extend_ascii: input is not ASCII");
        }
    }

    /// Append the first `len` bytes of `padded`, a fixed-size block: the
    /// copy is a fixed-size move, not a `memcpy` call. The whole block is
    /// written and the tail cut off again, so it must be valid UTF-8 up to
    /// `len` and at every byte after it (pad with ASCII). Panics if `len`
    /// is past the block, or the bytes are not valid UTF-8 at `len`.
    #[inline]
    pub fn push_padded<const W: usize>(&mut self, padded: &[u8; W], len: usize) {
        assert!(len <= W, "push_padded: length past the block");
        let start = self.0.len();
        self.0.extend_from_slice(padded);
        self.0.truncate(start + len);
        debug_assert!(std::str::from_utf8(&self.0[start..]).is_ok());
    }

    /// Remove the last byte of this value if it is ASCII (so the buffer
    /// stays valid UTF-8), and return it. Earlier values are never touched.
    #[inline]
    pub fn pop_ascii(&mut self) -> Option<u8> {
        match self.0.last() {
            Some(&b) if b.is_ascii() && self.0.len() > self.1 => self.0.pop(),
            _ => None,
        }
    }

    /// Append `v` in decimal.
    #[inline]
    pub fn push_u64(&mut self, mut v: u64) {
        let mut b = [0u8; 20];
        let mut i = b.len();
        loop {
            i -= 1;
            b[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        self.0.extend_from_slice(&b[i..]);
    }

    /// Bytes in the whole column's buffer so far: every earlier value's
    /// and this one's (not this value's alone).
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the column's buffer is still empty (see [`Self::len`]).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl StrCol {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn start(&mut self) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
    }

    #[inline]
    fn end_value(&mut self) {
        let end = offset(self.data.len());
        self.offsets.push(end);
    }

    /// Reserve room for `bytes` more bytes of string data.
    pub fn reserve_bytes(&mut self, bytes: usize) {
        self.data.reserve(bytes);
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.start();
        extend_small(&mut self.data, s.as_bytes());
        self.end_value();
    }

    /// Append one value whose bytes `f` writes straight into the column's
    /// buffer (no intermediate `String`).
    #[inline]
    pub fn push_with(&mut self, f: impl FnOnce(&mut StrBuf<'_>)) {
        self.start();
        let start = self.data.len();
        f(&mut StrBuf(&mut self.data, start));
        self.end_value();
    }

    /// Append one value whose bytes `f` writes into the column's byte
    /// buffer, for writers that produce UTF-8 bytes themselves (a decimal
    /// formatter, a UTF-16 decoder). The bytes written are checked: ASCII
    /// in one vectorised pass, anything else as UTF-8.
    ///
    /// # Panics
    ///
    /// If the bytes `f` writes are not valid UTF-8, or `f` changes bytes of
    /// earlier values (the buffer is cut back to before `f` first).
    #[inline]
    pub fn push_utf8_with(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        self.start();
        let start = self.data.len();
        f(&mut self.data);
        assert!(
            self.data.len() >= start,
            "push_utf8_with: earlier values cut"
        );
        let new = &self.data[start..];
        if !new.is_ascii() && std::str::from_utf8(new).is_err() {
            self.data.truncate(start);
            panic!("push_utf8_with: bytes are not UTF-8");
        }
        self.end_value();
    }

    /// Append `n` values, each written by `f` straight into the buffer.
    /// The same as `n` calls of [`Self::push_with`], but the buffers are
    /// held in locals for the loop, so the compiler can keep their lengths
    /// in registers instead of reloading them after every byte written.
    #[inline]
    pub fn push_n_with(&mut self, n: usize, mut f: impl FnMut(&mut StrBuf<'_>)) {
        self.start();
        let mut data = std::mem::take(&mut self.data);
        let mut offsets = std::mem::take(&mut self.offsets);
        offsets.reserve(n);
        offsets.extend((0..n).map(|_| {
            let start = data.len();
            f(&mut StrBuf(&mut data, start));
            offset(data.len())
        }));
        self.data = data;
        self.offsets = offsets;
    }

    /// Offsets so far (`len() + 1` entries once anything is written).
    pub fn offsets(&self) -> &[i32] {
        &self.offsets
    }

    /// All string bytes so far.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// The value at `i`.
    pub fn get(&self, i: usize) -> &str {
        let (s, e) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        // SAFETY: only valid UTF-8 is ever written (see `StrBuf`), and every
        // offset is a boundary between two whole values.
        unsafe { std::str::from_utf8_unchecked(&self.data[s..e]) }
    }

    /// `(offsets, data)`: `n + 1` offsets starting at 0, and the bytes. The
    /// data is valid UTF-8 and the offsets are non-decreasing, ready for
    /// `StringArray::new_unchecked`.
    pub fn into_parts(mut self) -> (Vec<i32>, Vec<u8>) {
        self.start();
        (self.offsets, self.data)
    }
}

impl Sink<String> for StrCol {
    #[inline]
    fn push(&mut self, value: String) {
        self.push_str(&value);
    }

    fn push_n(&mut self, values: impl Iterator<Item = String>) {
        let (lo, _) = values.size_hint();
        let mut values = values;
        self.push_n_with(lo, |b| b.push_str(&values.next().expect("size_hint")));
        values.for_each(|v| self.push_str(&v));
    }

    fn reserve(&mut self, additional: usize) {
        self.start();
        self.offsets.reserve(additional);
    }
}

impl<'a> Sink<&'a str> for StrCol {
    #[inline]
    fn push(&mut self, value: &'a str) {
        self.push_str(value);
    }

    fn push_n(&mut self, values: impl Iterator<Item = &'a str>) {
        let (lo, _) = values.size_hint();
        let mut values = values;
        self.push_n_with(lo, |b| b.push_str(values.next().expect("size_hint")));
        values.for_each(|v| self.push_str(v));
    }

    fn reserve(&mut self, additional: usize) {
        self.start();
        self.offsets.reserve(additional);
    }
}

impl Columnar for String {
    type Columns = StrCol;
}

impl Columnar for &'static str {
    type Columns = StrCol;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_arrow_utf8() {
        let mut c = StrCol::new();
        assert_eq!(c.clone().into_parts(), (vec![0], vec![]));
        c.push_str("ab");
        c.push_with(|b| {
            b.push_ascii(b'x');
            b.push_u64(1024);
            b.push_char('é');
        });
        c.push_str("");
        assert_eq!(c.len(), 3);
        assert_eq!(c.get(1), "x1024é");
        let (o, d) = c.into_parts();
        assert_eq!(o, vec![0, 2, 9, 9]);
        assert_eq!(std::str::from_utf8(&d).unwrap(), "abx1024é");
    }

    #[test]
    fn small_copies_every_length() {
        let text = "abcdefghijklmnopqrstuvwxyzé0123";
        let mut c = StrCol::new();
        for n in 0..=text.len() {
            if text.is_char_boundary(n) {
                c.push_str(&text[..n]);
                c.push_with(|b| b.push_str(&text[..n]));
            }
        }
        let want: Vec<&str> = (0..=text.len())
            .filter(|&n| text.is_char_boundary(n))
            .flat_map(|n| [&text[..n], &text[..n]])
            .collect();
        assert_eq!((0..c.len()).map(|i| c.get(i)).collect::<Vec<_>>(), want);
    }

    #[test]
    fn push_n_and_block_writes() {
        let mut c = StrCol::new();
        Sink::<&str>::push_n(&mut c, ["red", "", "green"].into_iter());
        Sink::<String>::push_n(&mut c, ["blue".to_string()].into_iter());
        let mut block = [b' '; 8];
        block[..3].copy_from_slice(b"one");
        c.push_n_with(2, |b| {
            b.push_padded(&block, 4);
            b.push_padded(&block, 3);
            b.extend_ascii(b"xy".iter().copied());
            assert_eq!(b.pop_ascii(), Some(b'y'));
        });
        // pop_ascii never reaches into an earlier value
        c.push_with(|b| assert_eq!(b.pop_ascii(), None));
        let got: Vec<&str> = (0..c.len()).map(|i| c.get(i)).collect();
        assert_eq!(
            got,
            ["red", "", "green", "blue", "one onex", "one onex", ""]
        );
    }

    #[test]
    #[should_panic(expected = "not ASCII")]
    fn extend_ascii_checks() {
        StrCol::new().push_with(|b| b.extend_ascii([b'a', 0xc3].into_iter()));
    }

    #[test]
    #[should_panic(expected = "not ASCII")]
    fn non_ascii_byte_panics() {
        StrCol::new().push_with(|b| b.push_ascii(0xff));
    }
}
