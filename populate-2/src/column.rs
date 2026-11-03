//! Columnar value storage in Arrow's layout.
//!
//! | value type        | column sink          | layout                                    |
//! |-------------------|----------------------|-------------------------------------------|
//! | `String`          | [`StrCol`]           | `i32` offsets + one byte buffer           |
//! | `Option<T>`       | [`NullableCol<S>`]   | `T`'s own column + a validity [`Bitmap`]  |
//! | [`DictValue<T>`]  | [`DictCol<T>`]       | `u32` code per row + the value list once  |
//!
//! None of them allocates per row, and each converts to its Arrow array by
//! moving buffers (see [`crate::arrow`]).
//!
//! [`PushNull`] lets a column take a null placeholder (so it can sit inside a
//! [`NullableCol`]), [`PushRef`] lets a leaf write a borrowed value (a `&str`
//! from a label list) without cloning it, and [`TakeRows`] reorders a column.

use crate::traits::{ListCol, Sink};
use std::collections::VecDeque;
use std::fmt;
use std::ops::Deref;
use std::str::from_utf8;
use std::sync::Arc;

// ============================================================================
// Column helper traits
// ============================================================================

/// A column that can append a placeholder for a null row. The placeholder's
/// value is unspecified; a [`NullableCol`] masks it with its validity bitmap.
pub trait PushNull {
    fn push_null(&mut self);
}

/// A column that can append a value it is given by reference, copying only
/// what its layout needs (bytes for [`StrCol`], a code for [`DictCol`]).
pub trait PushRef<T: ?Sized> {
    fn push_ref(&mut self, value: &T);
}

/// A column that can be reordered: `out[i] = self[order[i]]`.
pub trait TakeRows: Sized {
    fn take_rows(&self, order: &[usize]) -> Self;
}

/// Number of rows in a column.
pub trait ColumnLen {
    fn column_len(&self) -> usize;

    fn column_is_empty(&self) -> bool {
        self.column_len() == 0
    }
}

impl<T: Default> PushNull for Vec<T> {
    fn push_null(&mut self) {
        self.push(T::default());
    }
}

impl<T: Default> PushNull for VecDeque<T> {
    fn push_null(&mut self) {
        self.push_back(T::default());
    }
}

impl<T: Clone> PushRef<T> for Vec<T> {
    fn push_ref(&mut self, value: &T) {
        self.push(value.clone());
    }
}

impl<T: Clone> PushRef<T> for VecDeque<T> {
    fn push_ref(&mut self, value: &T) {
        self.push_back(value.clone());
    }
}

impl<T: Clone> TakeRows for Vec<T> {
    fn take_rows(&self, order: &[usize]) -> Self {
        order.iter().map(|&i| self[i].clone()).collect()
    }
}

impl<T> ColumnLen for Vec<T> {
    fn column_len(&self) -> usize {
        self.len()
    }
}

/// A null list is an empty one (the offset doesn't advance).
impl<O: Sink<usize>, V> PushNull for ListCol<O, V> {
    fn push_null(&mut self) {
        self.offsets.push(self.len);
    }
}

// ============================================================================
// StrCol — strings as offsets + one byte buffer
// ============================================================================

/// A string column in Arrow's `Utf8` layout: `offsets[i]..offsets[i + 1]` is
/// row `i`'s byte range in `data`. Holds at most `i32::MAX` bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrCol {
    /// `rows + 1` offsets, starting at 0.
    offsets: Vec<i32>,
    data: Vec<u8>,
}

impl Default for StrCol {
    fn default() -> Self {
        StrCol {
            offsets: vec![0],
            data: Vec::new(),
        }
    }
}

impl StrCol {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        StrCol {
            offsets,
            data: Vec::with_capacity(bytes),
        }
    }

    pub fn push_str(&mut self, s: &str) {
        self.data.extend_from_slice(s.as_bytes());
        self.offsets.push(Self::offset(self.data.len()));
    }

    fn offset(len: usize) -> i32 {
        i32::try_from(len).expect("StrCol holds more than i32::MAX bytes (LargeUtf8 not supported)")
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `i`.
    pub fn get(&self, i: usize) -> &str {
        let (a, b) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        // Only whole `&str`s are ever appended, so every row is valid UTF-8.
        from_utf8(&self.data[a..b]).expect("StrCol rows are UTF-8")
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        (0..self.len()).map(|i| self.get(i))
    }

    /// `(offsets, data)`, for zero-copy conversion.
    pub fn into_parts(self) -> (Vec<i32>, Vec<u8>) {
        (self.offsets, self.data)
    }
}

impl<'a> FromIterator<&'a str> for StrCol {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let mut col = StrCol::new();
        for s in iter {
            col.push_str(s);
        }
        col
    }
}

impl Sink<String> for StrCol {
    fn push(&mut self, value: String) {
        self.push_str(&value);
    }
}

impl PushRef<String> for StrCol {
    fn push_ref(&mut self, value: &String) {
        self.push_str(value);
    }
}

impl PushRef<str> for StrCol {
    fn push_ref(&mut self, value: &str) {
        self.push_str(value);
    }
}

impl PushNull for StrCol {
    fn push_null(&mut self) {
        self.offsets.push(Self::offset(self.data.len()));
    }
}

impl TakeRows for StrCol {
    fn take_rows(&self, order: &[usize]) -> Self {
        let bytes = order
            .iter()
            .map(|&i| (self.offsets[i + 1] - self.offsets[i]) as usize)
            .sum();
        let mut out = StrCol::with_capacity(order.len(), bytes);
        for &i in order {
            out.push_str(self.get(i));
        }
        out
    }
}

impl ColumnLen for StrCol {
    fn column_len(&self) -> usize {
        self.len()
    }
}

// ============================================================================
// Bitmap — validity bits
// ============================================================================

/// A growable bitmap, LSB-first within little-endian `u64` words: the same
/// bit order as an Arrow validity buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    pub fn new() -> Self {
        Self::default()
    }

    /// `len` bits, all `value`.
    pub fn filled(len: usize, value: bool) -> Self {
        let mut words = vec![if value { u64::MAX } else { 0 }; len.div_ceil(64)];
        if value && !len.is_multiple_of(64) {
            *words.last_mut().expect("non-empty") = (1u64 << (len % 64)) - 1;
        }
        Bitmap { words, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(&mut self, bit: bool) {
        if self.len.is_multiple_of(64) {
            self.words.push(0);
        }
        if bit {
            self.words[self.len / 64] |= 1 << (self.len % 64);
        }
        self.len += 1;
    }

    pub fn get(&self, i: usize) -> bool {
        assert!(i < self.len, "bit {i} out of range (len {})", self.len);
        self.words[i / 64] >> (i % 64) & 1 == 1
    }

    pub fn set(&mut self, i: usize, bit: bool) {
        assert!(i < self.len, "bit {i} out of range (len {})", self.len);
        if bit {
            self.words[i / 64] |= 1 << (i % 64);
        } else {
            self.words[i / 64] &= !(1 << (i % 64));
        }
    }

    /// Number of set bits.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// `(words, len)`, for zero-copy conversion.
    pub fn into_parts(self) -> (Vec<u64>, usize) {
        (self.words, self.len)
    }
}

impl TakeRows for Bitmap {
    fn take_rows(&self, order: &[usize]) -> Self {
        let mut out = Bitmap {
            words: Vec::with_capacity(order.len().div_ceil(64)),
            len: 0,
        };
        for &i in order {
            out.push(self.get(i));
        }
        out
    }
}

// ============================================================================
// NullableCol — a column plus validity
// ============================================================================

/// The column for `Option<T>`: `T`'s own column (with a placeholder in null
/// rows) plus a validity bitmap (`true` = present).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NullableCol<S> {
    pub values: S,
    pub validity: Bitmap,
}

impl<S: ColumnLen> NullableCol<S> {
    /// Every row of `values` present.
    pub fn all_valid(values: S) -> Self {
        let validity = Bitmap::filled(values.column_len(), true);
        NullableCol { values, validity }
    }
}

impl<S> NullableCol<S> {
    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.validity.is_empty()
    }

    pub fn is_valid(&self, i: usize) -> bool {
        self.validity.get(i)
    }

    /// Mark row `i` null (its value stays as a masked placeholder).
    pub fn set_null(&mut self, i: usize) {
        self.validity.set(i, false);
    }

    pub fn null_count(&self) -> usize {
        self.len() - self.validity.count_ones()
    }
}

impl<T, S: Sink<T> + PushNull> Sink<Option<T>> for NullableCol<S> {
    fn push(&mut self, value: Option<T>) {
        match value {
            Some(v) => {
                self.values.push(v);
                self.validity.push(true);
            }
            None => {
                self.values.push_null();
                self.validity.push(false);
            }
        }
    }
}

impl<S: PushNull> PushNull for NullableCol<S> {
    fn push_null(&mut self) {
        self.values.push_null();
        self.validity.push(false);
    }
}

impl<S: TakeRows> TakeRows for NullableCol<S> {
    fn take_rows(&self, order: &[usize]) -> Self {
        NullableCol {
            values: self.values.take_rows(order),
            validity: self.validity.take_rows(order),
        }
    }
}

impl<S> ColumnLen for NullableCol<S> {
    fn column_len(&self) -> usize {
        self.len()
    }
}

// ============================================================================
// DictCol — codes into a shared value list
// ============================================================================

/// One value of a dictionary column: a code into a shared value list.
/// Dereferences to the value; row mode pays an `Arc` clone per row, the
/// columnar path ([`choose_dict`](crate::random::choose_dict)) only a code.
#[derive(Clone)]
pub struct DictValue<T> {
    pub code: u32,
    pub values: Arc<[T]>,
}

impl<T> Deref for DictValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.values[self.code as usize]
    }
}

impl<T: fmt::Debug> fmt::Debug for DictValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: PartialEq> PartialEq for DictValue<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

/// A dictionary-encoded column: `values[codes[i]]` is row `i`. The value
/// list is set by the first value pushed; every row must use the same list.
#[derive(Clone, Debug, PartialEq)]
pub struct DictCol<T> {
    pub codes: Vec<u32>,
    pub values: Option<Arc<[T]>>,
}

impl<T> Default for DictCol<T> {
    fn default() -> Self {
        DictCol {
            codes: Vec::new(),
            values: None,
        }
    }
}

impl<T> DictCol<T> {
    pub fn new(values: impl Into<Arc<[T]>>) -> Self {
        DictCol {
            codes: Vec::new(),
            values: Some(values.into()),
        }
    }

    /// Use `values` as the value list (must be the list already in use, if any).
    pub fn set_values(&mut self, values: &Arc<[T]>) {
        match &self.values {
            Some(v) => assert!(
                Arc::ptr_eq(v, values),
                "a DictCol takes values from one shared list"
            ),
            None => self.values = Some(Arc::clone(values)),
        }
    }

    /// The value list (empty if nothing was pushed yet).
    pub fn dictionary(&self) -> &[T] {
        self.values.as_deref().unwrap_or(&[])
    }

    pub fn len(&self) -> usize {
        self.codes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// Row `i`.
    pub fn get(&self, i: usize) -> &T {
        &self.dictionary()[self.codes[i] as usize]
    }
}

impl<T> Sink<DictValue<T>> for DictCol<T> {
    fn push(&mut self, value: DictValue<T>) {
        self.set_values(&value.values);
        self.codes.push(value.code);
    }
}

/// Null rows hold code 0, masked by the enclosing [`NullableCol`].
impl<T> PushNull for DictCol<T> {
    fn push_null(&mut self) {
        self.codes.push(0);
    }
}

impl<T> TakeRows for DictCol<T> {
    fn take_rows(&self, order: &[usize]) -> Self {
        DictCol {
            codes: self.codes.take_rows(order),
            values: self.values.clone(),
        }
    }
}

impl<T> ColumnLen for DictCol<T> {
    fn column_len(&self) -> usize {
        self.len()
    }
}
