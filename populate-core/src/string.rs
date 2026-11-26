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
///
/// ```
/// use populate_core::StrCol;
///
/// let mut names = StrCol::new();
/// names.push_str("ada");
/// names.push_with(|b| {
///     b.push_str("grace ");
///     b.push_char('h');
/// });
/// assert_eq!(names.len(), 2);
/// assert_eq!(names.get(1), "grace h");
/// ```
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StrCol {
    offsets: Vec<i32>,
    data: Vec<u8>,
}

/// `v.extend_from_slice(s)`, with strings up to 16 bytes copied by two
/// overlapping fixed-size loads (no `memcpy` call, which dominates for
/// short values like category names). The loads are combined in registers,
/// not through a stack buffer: a 16-byte read of bytes just written as
/// smaller pieces stalls on store forwarding.
#[inline(always)]
fn extend_small(v: &mut Vec<u8>, s: &[u8]) {
    let n = s.len();
    if n > 16 {
        v.extend_from_slice(s);
        return;
    }
    let block: u128 = if n >= 8 {
        let lo = u64::from_le_bytes(s[..8].try_into().expect("8 bytes"));
        let hi = u64::from_le_bytes(s[n - 8..].try_into().expect("8 bytes"));
        // the two loads overlap on equal bytes, so OR keeps them
        lo as u128 | (hi as u128) << (8 * (n - 8))
    } else if n >= 4 {
        let lo = u32::from_le_bytes(s[..4].try_into().expect("4 bytes"));
        let hi = u32::from_le_bytes(s[n - 4..].try_into().expect("4 bytes"));
        (lo as u64 | (hi as u64) << (8 * (n - 4))) as u128
    } else if n > 0 {
        s[0] as u128 | (s[n / 2] as u128) << (8 * (n / 2)) | (s[n - 1] as u128) << (8 * (n - 1))
    } else {
        0
    };
    let start = v.len();
    v.extend_from_slice(&block.to_le_bytes());
    v.truncate(start + n);
}

/// A short string padded with spaces to `W` bytes, for
/// [`StrBuf::push_padded`]: a word list stored this way is appended by one
/// fixed-size move per word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Padded<const W: usize> {
    bytes: [u8; W],
    len: u8,
}

impl<const W: usize> Padded<W> {
    /// `s` padded with spaces to `W` bytes, of which the first `len` are
    /// the value: `s` cut at `len`, or `s` and `len - s.len()` spaces.
    /// `None` if `s` or `len` is longer than the block or `len` is not a
    /// character boundary of `s`.
    pub fn new(s: &str, len: usize) -> Option<Self> {
        let ok = s.len() <= W && len <= W && len <= u8::MAX as usize;
        if !ok || (len < s.len() && !s.is_char_boundary(len)) {
            return None;
        }
        let mut bytes = [b' '; W];
        bytes[..s.len()].copy_from_slice(s.as_bytes());
        Some(Padded {
            bytes,
            len: len as u8,
        })
    }

    /// `bytes`, each masked to 7 bits, of which the first `len` are the
    /// value: for a block written per row from an ASCII character set, made
    /// valid at any cut with no check. A non-ASCII byte panics in debug
    /// builds.
    ///
    /// # Panics
    ///
    /// If `len` is past the block.
    #[inline]
    pub fn ascii(bytes: [u8; W], len: usize) -> Self {
        assert!(len <= W && len <= u8::MAX as usize, "length past the block");
        debug_assert!(bytes.is_ascii(), "Padded::ascii: input is not ASCII");
        Padded {
            bytes: bytes.map(|b| b & 0x7f),
            len: len as u8,
        }
    }
}

/// Whether every byte of `b` is ASCII: an OR of its 8-byte words, which
/// vectorises. `<[u8]>::is_ascii` made YCSB's load (100-byte values) 40%
/// slower; this costs it a few percent.
#[inline(always)]
fn is_ascii(b: &[u8]) -> bool {
    let (words, rest) = b.as_chunks::<8>();
    let w = words
        .iter()
        .fold(0u64, |acc, w| acc | u64::from_ne_bytes(*w));
    let r = rest.iter().fold(0u8, |acc, &x| acc | x);
    w & 0x8080_8080_8080_8080 == 0 && r < 0x80
}

/// Write access to the value being appended by [`StrCol::push_with`].
pub struct StrBuf<'a>(&'a mut Vec<u8>, usize);

/// A string column a value can be written into in place: [`StrCol`], or a
/// generator's own string column.
pub trait StrSink {
    /// Append one value whose bytes `f` writes.
    fn push_with(&mut self, f: impl FnOnce(&mut StrBuf<'_>));
}

impl StrSink for StrCol {
    #[inline]
    fn push_with(&mut self, f: impl FnOnce(&mut StrBuf<'_>)) {
        StrCol::push_with(self, f)
    }
}

impl<'a> StrBuf<'a> {
    /// A value appended to the end of `buf`, which must hold valid UTF-8
    /// (every write keeps it so).
    #[inline]
    pub fn appending(buf: &'a mut Vec<u8>) -> Self {
        let start = buf.len();
        StrBuf(buf, start)
    }
}

impl StrBuf<'_> {
    /// Append `s` to the value.
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        extend_small(self.0, s.as_bytes());
    }

    /// Append `c` to the value, UTF-8 encoded.
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
        assert!(is_ascii(bytes), "push_ascii_bytes: input is not ASCII");
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
        if !is_ascii(&self.0[start..]) {
            self.0.truncate(start);
            panic!("extend_ascii: input is not ASCII");
        }
    }

    /// Append `len` ASCII bytes that `fill` writes into place (a random
    /// string drawn from a character set, say), then check them in one
    /// vectorised pass. Panics, with the buffer as it was, if any byte is
    /// not ASCII.
    #[inline]
    pub fn push_ascii_with(&mut self, len: usize, fill: impl FnOnce(&mut [u8])) {
        let start = self.0.len();
        self.0.resize(start + len, 0);
        fill(&mut self.0[start..]);
        if !is_ascii(&self.0[start..]) {
            self.0.truncate(start);
            panic!("push_ascii_with: input is not ASCII");
        }
    }

    /// Append `len` bytes, `N` at a time from `next` (the last block cut
    /// short), each masked to 7 bits: for a source whose blocks are ASCII
    /// by construction, written in one pass with no check. A non-ASCII
    /// byte panics in debug builds.
    #[inline]
    pub fn push_ascii_blocks<const N: usize>(
        &mut self,
        len: usize,
        mut next: impl FnMut() -> [u8; N],
    ) {
        let start = self.0.len();
        self.0.resize(start + len, 0);
        let mask = |b: [u8; N]| {
            debug_assert!(b.is_ascii(), "push_ascii_blocks: input is not ASCII");
            b.map(|x| x & 0x7f)
        };
        let mut blocks = self.0[start..].chunks_exact_mut(N);
        for c in &mut blocks {
            c.copy_from_slice(&mask(next()));
        }
        let rest = blocks.into_remainder();
        if !rest.is_empty() {
            let n = rest.len();
            rest.copy_from_slice(&mask(next())[..n]);
        }
    }

    /// Append a [`Padded`] block's value: the copy is a fixed-size move, not
    /// a `memcpy` call. The whole block is written and the tail cut off
    /// again, which [`Padded::new`] checked leaves valid UTF-8.
    #[inline]
    pub fn push_padded<const W: usize>(&mut self, padded: &Padded<W>) {
        let start = self.0.len();
        self.0.extend_from_slice(&padded.bytes);
        self.0.truncate(start + padded.len as usize);
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
    pub fn push_u64(&mut self, v: u64) {
        self.push_u64_padded(v, 1);
    }

    /// Append `v` in decimal, zero-padded to at least `width` digits (at
    /// most 20).
    #[inline]
    pub fn push_u64_padded(&mut self, mut v: u64, width: usize) {
        let mut b = [b'0'; 20];
        let mut i = b.len();
        while v > 0 {
            i -= 1;
            b[i] = b'0' + (v % 10) as u8;
            v /= 10;
        }
        self.0
            .extend_from_slice(&b[i.min(b.len() - width.clamp(1, 20))..]);
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
    /// An empty column.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Whether there are no values.
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

    /// Append `s` as one value.
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

    /// Append value `i` of `src` for every `(src, i)` of `picks`, in order:
    /// a gather from one or more columns. The bytes are counted first, so
    /// both buffers are allocated once.
    ///
    /// # Panics
    ///
    /// If the column would pass 2 GiB of string data.
    pub fn extend_picked<'a>(&mut self, picks: impl Iterator<Item = (&'a StrCol, usize)> + Clone) {
        let span = |c: &StrCol, i: usize| c.offsets[i] as usize..c.offsets[i + 1] as usize;
        let (n, bytes) = picks
            .clone()
            .fold((0, 0), |(n, b), (c, i)| (n + 1, b + span(c, i).len()));
        offset(self.data.len() + bytes);
        self.start();
        self.offsets.reserve(n);
        self.data.reserve(bytes);
        for (c, i) in picks {
            self.data.extend_from_slice(&c.data[span(c, i)]);
            // checked again: a clone of `picks` need not yield the same
            // values, and the offsets are trusted unchecked
            self.offsets.push(offset(self.data.len()));
        }
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

/// `String` and `&str` rows.
impl<S: AsRef<str>> Sink<S> for StrCol {
    #[inline]
    fn push(&mut self, value: S) {
        self.push_str(value.as_ref());
    }

    fn push_n(&mut self, values: impl Iterator<Item = S>) {
        let (lo, _) = values.size_hint();
        let mut values = values;
        self.push_n_with(lo, |b| {
            b.push_str(values.next().expect("size_hint").as_ref())
        });
        values.for_each(|v| self.push_str(v.as_ref()));
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
    fn padded_digits() {
        let mut c = StrCol::new();
        for (v, w) in [
            (7, 9),
            (0, 1),
            (0, 3),
            (1_234_567_890, 9),
            (u64::MAX, 20),
            (5, 0),
        ] {
            c.push_with(|b| b.push_u64_padded(v, w));
        }
        let got: Vec<&str> = (0..c.len()).map(|i| c.get(i)).collect();
        assert_eq!(
            got,
            [
                "000000007",
                "0",
                "000",
                "1234567890",
                "18446744073709551615",
                "5"
            ]
        );
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
        let space = Padded::<8>::new("one", 4).unwrap();
        let word = Padded::<8>::new("one", 3).unwrap();
        assert_eq!(Padded::<8>::new("é", 1), None);
        assert_eq!(Padded::<2>::new("one", 3), None);
        c.push_n_with(2, |b| {
            b.push_padded(&space);
            b.push_padded(&word);
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
    fn ascii_blocks_cut_at_their_length() {
        let mut c = StrCol::new();
        c.push_with(|b| b.push_padded(&Padded::ascii(*b"ab-cd   ", 5)));
        c.push_with(|b| b.push_padded(&Padded::ascii(*b"xyz", 0)));
        assert_eq!([c.get(0), c.get(1)], ["ab-cd", ""]);
        let past = std::panic::catch_unwind(|| Padded::ascii(*b"ab", 3));
        assert!(past.is_err());
    }

    #[test]
    fn push_ascii_with_fills_in_place() {
        let mut c = StrCol::new();
        c.push_with(|b| b.push_ascii_with(3, |s| s.copy_from_slice(b"abc")));
        assert_eq!(c.get(0), "abc");
    }

    #[test]
    fn push_ascii_blocks_cuts_the_last() {
        let mut c = StrCol::new();
        let mut i = 0u8;
        c.push_with(|b| {
            b.push_ascii_blocks(5, || {
                i += 1;
                [b'a' + i, b'-', b'z']
            })
        });
        assert_eq!(c.get(0), "b-zc-");
    }

    #[test]
    #[should_panic(expected = "not ASCII")]
    fn push_ascii_with_checks() {
        StrCol::new().push_with(|b| b.push_ascii_with(1, |s| s[0] = 0xc3));
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
