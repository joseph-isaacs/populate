//! Columnar string sink.
//!
//! [`StrCol`] is the sink for `String` and `&'static str` items. It is laid
//! out exactly like an Arrow `Utf8` array: `i32` offsets (`n + 1` entries,
//! first is 0) and one contiguous byte buffer. Distributions that produce
//! strings write their bytes straight into that buffer with
//! [`StrCol::push_with`], so a string column costs two allocations in total,
//! not one per value, and both buffers move into Arrow without a copy.

use crate::hlist::{HCons, HNil, ValueHList};
use crate::traits::{Backend, DefaultSink, Sink};

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

/// Write access to the value being appended by [`StrCol::push_with`].
pub struct StrBuf<'a>(&'a mut Vec<u8>);

impl StrBuf<'_> {
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.0.extend_from_slice(s.as_bytes());
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
        let end = i32::try_from(self.data.len())
            .expect("string column exceeds i32 offsets (2 GiB per batch); use smaller batches");
        self.offsets.push(end);
    }

    /// Reserve room for `bytes` more bytes of string data.
    pub fn reserve_bytes(&mut self, bytes: usize) {
        self.data.reserve(bytes);
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.start();
        self.data.extend_from_slice(s.as_bytes());
        self.end_value();
    }

    /// Append one value whose bytes `f` writes straight into the column's
    /// buffer (no intermediate `String`).
    #[inline]
    pub fn push_with(&mut self, f: impl FnOnce(&mut StrBuf<'_>)) {
        self.start();
        f(&mut StrBuf(&mut self.data));
        self.end_value();
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

    fn reserve(&mut self, additional: usize) {
        self.start();
        self.offsets.reserve(additional);
    }
}

impl<B: Backend> DefaultSink<B> for String {
    type Sink = HCons<StrCol, HNil>;
}

impl<B: Backend> DefaultSink<B> for &'static str {
    type Sink = HCons<StrCol, HNil>;
}

impl ValueHList for String {
    type HList = HCons<String, HNil>;
    fn into_hlist(self) -> Self::HList {
        HCons(self, HNil)
    }
}

impl ValueHList for &'static str {
    type HList = HCons<&'static str, HNil>;
    fn into_hlist(self) -> Self::HList {
        HCons(self, HNil)
    }
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
    #[should_panic(expected = "not ASCII")]
    fn non_ascii_byte_panics() {
        StrCol::new().push_with(|b| b.push_ascii(0xff));
    }
}
