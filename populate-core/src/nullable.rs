//! `Option<T>` rows: a column with missing values.

use crate::columnar::{Columnar, Columns};
use crate::sink::Sink;
use std::iter::repeat_n;

/// The columns of `Option<T>`: `T`'s columns with a value in every row (a
/// missing one is `T::default()`), and whether each row is present.
///
/// A writer that fills `values` itself (a string written in place, say)
/// pushes one `valid` flag per row it writes.
///
/// ```
/// use populate_core::{Columns, Sink};
///
/// let mut notes: Columns<Option<i32>> = Default::default();
/// notes.push(Some(7));
/// notes.push(None);
/// assert_eq!(notes.values, [7, 0]);
/// assert_eq!(notes.valid.iter().collect::<Vec<_>>(), [true, false]);
/// assert_eq!(notes.valid.null_count(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Nullable<C> {
    /// A value for every row, present or not.
    pub values: C,
    /// Which rows are present.
    pub valid: Validity,
}

impl<C> Nullable<C> {
    /// Rows written so far.
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }
}

impl<T: Columnar + Default> Columnar for Option<T> {
    type Columns = Nullable<Columns<T>>;
}

impl<T: Default, C: Sink<T>> Sink<Option<T>> for Nullable<C> {
    #[inline(always)]
    fn push(&mut self, value: Option<T>) {
        self.valid.push(value.is_some());
        self.values.push(value.unwrap_or_default());
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
        self.valid.reserve(additional);
    }
}

/// One bit per row, `1` where the row has a value: packed little-endian
/// into `u64` words, the layout of an Arrow validity bitmap, so a sink
/// moves the words into a null buffer without a pass over them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Validity {
    words: Vec<u64>,
    len: usize,
}

impl Validity {
    /// Rows so far.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reserve room for `additional` more rows.
    pub fn reserve(&mut self, additional: usize) {
        self.words
            .reserve((self.len + additional).div_ceil(64) - self.words.len());
    }

    /// Drop every row, keeping the buffer.
    pub fn clear(&mut self) {
        self.words.clear();
        self.len = 0;
    }

    /// One more row.
    #[inline(always)]
    pub fn push(&mut self, valid: bool) {
        let bit = self.len % 64;
        if bit == 0 {
            self.words.push(valid as u64);
        } else if let Some(w) = self.words.last_mut() {
            *w |= (valid as u64) << bit;
        }
        self.len += 1;
    }

    /// `n` more rows, all present.
    pub fn push_valid(&mut self, n: usize) {
        let bit = self.len % 64;
        let head = if bit == 0 { 0 } else { (64 - bit).min(n) };
        if let Some(w) = self.words.last_mut().filter(|_| head > 0) {
            *w |= (u64::MAX >> (64 - head)) << bit;
        }
        let rest = n - head;
        self.words.extend(repeat_n(u64::MAX, rest / 64));
        if !rest.is_multiple_of(64) {
            self.words.push(u64::MAX >> (64 - rest % 64));
        }
        self.len += n;
    }

    /// `other`'s rows after these (a copy of its words if this is empty).
    pub fn append(&mut self, other: &Validity) {
        if self.is_empty() {
            self.clone_from(other);
        } else {
            self.extend(other.iter());
        }
    }

    /// Whether row `i` is present.
    pub fn get(&self, i: usize) -> bool {
        assert!(i < self.len, "row {i} of {}", self.len);
        self.words[i / 64] >> (i % 64) & 1 == 1
    }

    /// The packed flags: row `i` is bit `i % 64` of word `i / 64`.
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    /// Every row's flag, in order.
    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|i| self.words[i / 64] >> (i % 64) & 1 == 1)
    }

    /// Missing rows.
    pub fn null_count(&self) -> usize {
        self.len
            - self
                .words
                .iter()
                .map(|w| w.count_ones() as usize)
                .sum::<usize>()
    }

    /// The packed words (bits past [`len`](Self::len) are 0) and the row
    /// count.
    pub fn into_parts(self) -> (Vec<u64>, usize) {
        (self.words, self.len)
    }
}

/// Packs a word at a time once the last word is full.
impl Extend<bool> for Validity {
    fn extend<I: IntoIterator<Item = bool>>(&mut self, iter: I) {
        let mut it = iter.into_iter();
        while !self.len.is_multiple_of(64) {
            match it.next() {
                Some(b) => self.push(b),
                None => return,
            }
        }
        loop {
            let (mut w, mut k) = (0u64, 0);
            for b in it.by_ref().take(64) {
                w |= (b as u64) << k;
                k += 1;
            }
            if k == 0 {
                return;
            }
            self.words.push(w);
            self.len += k;
            if k < 64 {
                return;
            }
        }
    }
}

impl FromIterator<bool> for Validity {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let mut v = Validity::default();
        v.extend(iter);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_valid_matches_pushing_each_row() {
        for (start, n) in [
            (0, 0),
            (0, 64),
            (3, 2),
            (3, 61),
            (3, 200),
            (63, 1),
            (64, 130),
        ] {
            let mut got: Validity = (0..start).map(|i| i % 3 == 0).collect();
            let mut want = got.clone();
            got.push_valid(n);
            want.extend(repeat_n(true, n));
            assert_eq!(got, want, "{start} + {n}");
        }
    }

    #[test]
    fn missing_rows_hold_the_default() {
        let mut c = Columns::<Option<i32>>::default();
        c.push_n([Some(3), None, Some(5)].into_iter());
        assert_eq!(c.values, [3, 0, 5]);
        assert_eq!(c.valid.iter().collect::<Vec<_>>(), [true, false, true]);
        assert_eq!(c.len(), 3);
        assert_eq!(c.valid.null_count(), 1);
    }

    #[test]
    fn bits_pack_across_words() {
        let v: Validity = (0..130).map(|i| i % 3 != 0).collect();
        let mut w = Validity::default();
        w.push(false);
        w.extend((1..130).map(|i| i % 3 != 0));
        assert_eq!(v, w);
        let mut x = v.clone();
        x.append(&v);
        assert_eq!(x.len(), 260);
        assert!(x.iter().skip(130).eq(v.iter()));
        assert_eq!(v.len(), 130);
        assert!(!v.get(129) && v.get(128) && !v.get(0) && v.get(64));
        assert_eq!(v.null_count(), 44);
        let (words, len) = v.into_parts();
        assert_eq!((words.len(), len), (3, 130));
    }
}
