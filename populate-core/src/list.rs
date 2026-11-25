//! Columnar list sink.

use crate::columnar::Columnar;
use crate::sink::Sink;

/// A byte or element count as an `i32` offset.
#[inline]
pub(crate) fn offset(len: usize) -> i32 {
    i32::try_from(len).expect("column exceeds i32 offsets (2^31 values per batch)")
}

/// List column in Arrow `List` layout: `i32` offsets (`n + 1` entries, first
/// is 0) and the columns of every element of every row, end to end.
///
/// `Vec<(A, B)>` rows are a `ListCol<(A`'s columns`, B`'s columns`)>`, and
/// `Vec<Vec<T>>` rows a `ListCol<ListCol<..>>`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ListCol<V> {
    /// Empty until the first write, so an unused column allocates nothing
    /// and the first `reserve` sizes the buffer for all `n + 1` entries.
    offsets: Vec<i32>,
    /// The element columns. Whoever calls [`Self::push_lengths`] appends the
    /// elements here.
    pub values: V,
}

impl<V> ListCol<V> {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Offsets so far (`len() + 1` entries once anything is written).
    pub fn offsets(&self) -> &[i32] {
        &self.offsets
    }

    /// `(offsets, values)`: `n + 1` offsets starting at 0, and the element
    /// columns.
    pub fn into_parts(mut self) -> (Vec<i32>, V) {
        self.start(0);
        (self.offsets, self.values)
    }

    /// Write the leading 0 if it is not there yet, with room for
    /// `additional` rows.
    fn start(&mut self, additional: usize) {
        if self.offsets.is_empty() {
            self.offsets.reserve(additional + 1);
            self.offsets.push(0);
        }
    }

    /// Reserve room for `additional` more rows (their offsets only: the
    /// number of elements is not known yet).
    pub fn reserve_rows(&mut self, additional: usize) {
        if self.offsets.is_empty() {
            self.start(additional);
        } else {
            self.offsets.reserve(additional);
        }
    }

    /// Append one row per length, and return the sum of the lengths. The
    /// caller then appends that many elements to [`Self::values`].
    pub fn push_lengths(&mut self, lengths: impl Iterator<Item = usize>) -> usize {
        self.start(0);
        let rows = self.offsets.len();
        let start = *self.offsets.last().expect("leading 0") as usize;
        let mut end = start;
        // One `extend` (no capacity check per row), as the leaves do. The
        // offsets only grow, so one range check of the last covers them all.
        self.offsets.extend(lengths.map(|len| {
            end += len;
            end as i32
        }));
        if i32::try_from(end).is_err() {
            self.offsets.truncate(rows);
            offset(end);
        }
        end - start
    }
}

impl<V> ListCol<V> {
    /// Append rows whose lengths `write` appends to the offsets buffer,
    /// turned into offsets in place, and return the sum of the lengths: a
    /// column of `i32` lengths collected straight into the offsets, with no
    /// buffer of its own. The caller then appends that many elements to
    /// [`Self::values`].
    ///
    /// # Panics
    ///
    /// If a length is negative or the offsets pass `i32::MAX`.
    pub fn push_lengths_in_place(&mut self, write: impl FnOnce(&mut Vec<i32>)) -> usize {
        self.start(0);
        let rows = self.offsets.len();
        let start = self.offsets[rows - 1] as usize;
        write(&mut self.offsets);
        let mut end = start;
        let mut negative = false;
        for o in &mut self.offsets[rows..] {
            negative |= *o < 0;
            end = end.wrapping_add(*o as usize);
            *o = end as i32;
        }
        if negative || i32::try_from(end).is_err() {
            self.offsets.truncate(rows);
            assert!(!negative, "negative list length");
            offset(end);
        }
        end - start
    }
}

// Row at a time: explode into columns.
impl<T, V: Sink<T>> Sink<Vec<T>> for ListCol<V> {
    fn push(&mut self, value: Vec<T>) {
        let n = self.push_lengths(std::iter::once(value.len()));
        self.values.reserve(n);
        self.values.push_n(value.into_iter());
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve_rows(additional);
    }
}

impl<T: Columnar> Columnar for Vec<T> {
    type Columns = ListCol<T::Columns>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_arrow_list() {
        let mut c = ListCol::<Vec<i32>>::default();
        assert!(c.offsets().is_empty());
        assert_eq!(c.clone().into_parts(), (vec![0], vec![]));
        c.push(vec![1, 2]);
        c.push(vec![]);
        assert_eq!(c.push_lengths([3, 1].into_iter()), 4);
        c.values.extend([3, 4, 5, 6]);
        assert_eq!(c.len(), 4);
        assert_eq!(
            c.into_parts(),
            (vec![0, 2, 2, 5, 6], vec![1, 2, 3, 4, 5, 6])
        );
    }

    #[test]
    fn first_reserve_sizes_every_offset() {
        let mut c = ListCol::<Vec<i32>>::default();
        Sink::<Vec<i32>>::reserve(&mut c, 7);
        let ptr = c.offsets().as_ptr();
        c.push_lengths(std::iter::repeat_n(0, 7));
        assert_eq!(c.offsets().as_ptr(), ptr);
        assert_eq!(c.offsets().len(), 8);
    }
}
