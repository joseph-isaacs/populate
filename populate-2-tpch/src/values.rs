//! TPC-H value types and their columnar Arrow sinks.
//!
//! - [`Str`] -> [`StrCol`] -> `StringViewArray` (`Utf8View`)
//! - [`Dec`] -> [`DecCol`] -> `Decimal128(15, 2)`
//! - `i32` / `i64` use populate-2's `Vec` sinks unchanged.
//!
//! `StrCol` writes strings straight into one byte buffer and records Arrow
//! views; nothing is allocated per row. Strings that are slices of the TPC-H
//! text pool (all `*_comment` columns) aren't copied at all: their views point
//! into a shared, read-only Arrow buffer holding the whole pool.

use arrow_array::types::Decimal128Type;
use arrow_array::{ArrayRef, PrimitiveArray, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use populate_2::arrow::IntoArrowArray;
use populate_2::hlist::{HCons, HNil};
use populate_2::traits::{Backend, DefaultSink, Sink};
use std::alloc::{Layout, alloc, dealloc};
use std::borrow::Cow;
use std::ptr::NonNull;
use std::sync::{Arc, LazyLock};
use tpchgen::text::TextPool;

// ============================================================================
// Text pool as an Arrow buffer
// ============================================================================

pub fn text_pool() -> &'static TextPool {
    TextPool::get_or_init_default()
}

/// The 300 MB text pool, copied once into an Arrow buffer that every comment
/// column shares (an `Arc` clone per batch).
///
/// Comment views read a 4-byte prefix at a random spot in the pool, so with
/// 4 KiB pages nearly every row also misses the TLB. The buffer is 2 MiB
/// aligned and marked `MADV_HUGEPAGE` (Linux) before it is filled.
pub static POOL_BUFFER: LazyLock<Buffer> = LazyLock::new(|| {
    let pool = text_pool();
    let bytes = pool.text(0, pool.size()).as_bytes();
    HugeBuffer::copy_from(bytes)
});

const HUGE_PAGE: usize = 2 << 20;

/// Owner of a 2 MiB-aligned allocation backing an Arrow `Buffer`.
struct HugeBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: the allocation is plain bytes, never mutated after construction.
unsafe impl Send for HugeBuffer {}
unsafe impl Sync for HugeBuffer {}

impl HugeBuffer {
    fn copy_from(bytes: &[u8]) -> Buffer {
        let layout = Layout::from_size_align(bytes.len().next_multiple_of(HUGE_PAGE), HUGE_PAGE)
            .expect("layout");
        // SAFETY: non-zero size; we fill `bytes.len()` bytes before exposing them.
        unsafe {
            let ptr = NonNull::new(alloc(layout)).expect("allocation failed");
            #[cfg(target_os = "linux")]
            libc::madvise(ptr.as_ptr().cast(), layout.size(), libc::MADV_HUGEPAGE);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len());
            let owner = Arc::new(HugeBuffer { ptr, layout });
            Buffer::from_custom_allocation(ptr, bytes.len(), owner)
        }
    }
}

impl Drop for HugeBuffer {
    fn drop(&mut self) {
        // SAFETY: allocated in `copy_from` with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

/// Arrow `Utf8View` view, inlined (arrow's `make_view` is an out-of-line call).
/// Layout: len u32 | 12 inline bytes, or len u32 | prefix u32 | block u32 | offset u32.
#[inline(always)]
pub fn make_view(bytes: &[u8], block: u32, offset: u32) -> u128 {
    let len = bytes.len();
    if len <= 12 {
        let mut v = [0u8; 16];
        v[..4].copy_from_slice(&(len as u32).to_le_bytes());
        v[4..4 + len].copy_from_slice(bytes);
        u128::from_le_bytes(v)
    } else {
        let prefix = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        (len as u128)
            | ((prefix as u128) << 32)
            | ((block as u128) << 64)
            | ((offset as u128) << 96)
    }
}

/// View of `pool[offset..offset + len]` (block `POOL_BLOCK`) from one
/// unaligned 16-byte load: no variable-length copy for inline strings.
#[inline(always)]
pub fn pool_view(pool: &[u8], offset: usize, len: usize) -> u128 {
    let Some(chunk) = pool.get(offset..offset + 16) else {
        return make_view(&pool[offset..offset + len], POOL_BLOCK, offset as u32);
    };
    let raw = u128::from_le_bytes(chunk.try_into().expect("16 bytes"));
    if len <= 12 {
        let mask = (1u128 << (len * 8)) - 1;
        (len as u128) | ((raw & mask) << 32)
    } else {
        (len as u128)
            | (((raw as u32) as u128) << 32)
            | ((POOL_BLOCK as u128) << 64)
            | ((offset as u128) << 96)
    }
}

/// View block ids inside a [`StrCol`] array.
const DATA_BLOCK: u32 = 0;
const POOL_BLOCK: u32 = 1;

// ============================================================================
// Str / StrCol
// ============================================================================

/// A string value in row mode (`row_iter`, `map`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Str(pub Cow<'static, str>);

impl Str {
    pub fn borrowed(s: &'static str) -> Self {
        Str(Cow::Borrowed(s))
    }
}

impl<B: Backend> DefaultSink<B> for Str {
    type Sink = HCons<StrCol, HNil>;
}

/// Columnar `Utf8View` sink.
#[derive(Default, Debug)]
pub struct StrCol {
    views: Vec<u128>,
    data: Vec<u8>,
    uses_pool: bool,
}

impl StrCol {
    /// Pre-size for `rows` strings averaging `avg_bytes` copied bytes.
    pub fn reserve(&mut self, rows: usize, avg_bytes: usize) {
        self.views.reserve(rows);
        self.data.reserve(rows * avg_bytes);
    }

    /// Copy `s` in (inline if <= 12 bytes).
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        if bytes.len() <= 12 {
            self.views.push(make_view(bytes, 0, 0));
        } else {
            let offset = self.data.len() as u32;
            self.data.extend_from_slice(bytes);
            self.views.push(make_view(bytes, DATA_BLOCK, offset));
        }
    }

    /// Build the string in place: `f` appends bytes to the data buffer.
    #[inline]
    pub fn push_with(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        let start = self.data.len();
        f(&mut self.data);
        let bytes = &self.data[start..];
        let view = make_view(bytes, DATA_BLOCK, start as u32);
        if bytes.len() <= 12 {
            self.data.truncate(start); // inlined in the view
        }
        self.views.push(view);
    }

    /// Zero-copy view of `pool[offset .. offset + len]`, where `pool` is
    /// `POOL_BUFFER.as_slice()` (callers cache it instead of touching the
    /// `LazyLock` per row).
    #[inline(always)]
    pub fn push_pool(&mut self, pool: &[u8], offset: u32, len: u32) {
        self.uses_pool = true;
        self.views
            .push(pool_view(pool, offset as usize, len as usize));
    }

    /// The view for `s` in this sink: inline if <= 12 bytes, otherwise its
    /// bytes are stored once in the data buffer. Push it with
    /// [`Self::push_view`] for every repeat (dictionary-like columns).
    pub fn intern(&mut self, s: &str) -> u128 {
        let bytes = s.as_bytes();
        if bytes.len() <= 12 {
            return make_view(bytes, DATA_BLOCK, 0);
        }
        let offset = self.data.len() as u32;
        self.data.extend_from_slice(bytes);
        make_view(bytes, DATA_BLOCK, offset)
    }

    /// Push a view from [`Self::intern`] on this same sink.
    #[inline(always)]
    pub fn push_view(&mut self, view: u128) {
        self.views.push(view);
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }
}

impl Sink<Str> for StrCol {
    fn push(&mut self, value: Str) {
        self.push_str(&value.0);
    }
}

impl IntoArrowArray for StrCol {
    fn into_array(self) -> ArrayRef {
        let mut buffers = vec![Buffer::from_vec(self.data)];
        if self.uses_pool {
            buffers.push(POOL_BUFFER.clone());
        }
        // SAFETY: views were made by `make_view` over in-bounds slices of
        // these buffers, and all TPC-H text is ASCII.
        let array = unsafe {
            StringViewArray::new_unchecked(ScalarBuffer::from(self.views), buffers.into(), None)
        };
        Arc::new(array)
    }
}

// ============================================================================
// Dec / DecCol
// ============================================================================

/// TPC-H decimal: an integer number of cents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dec(pub i64);

impl<B: Backend> DefaultSink<B> for Dec {
    type Sink = HCons<DecCol, HNil>;
}

#[derive(Default, Debug)]
pub struct DecCol(pub Vec<i128>);

impl Sink<Dec> for DecCol {
    #[inline]
    fn push(&mut self, value: Dec) {
        self.0.push(value.0 as i128);
    }
}

impl IntoArrowArray for DecCol {
    fn into_array(self) -> ArrayRef {
        Arc::new(
            PrimitiveArray::<Decimal128Type>::new(ScalarBuffer::from(self.0), None)
                .with_precision_and_scale(15, 2)
                .expect("valid decimal(15,2)"),
        )
    }
}

// ============================================================================
// Number formatting without `fmt`
// ============================================================================

/// Append `v` in decimal, zero-padded to at least `width` digits.
#[inline]
pub fn push_int(out: &mut Vec<u8>, v: u64, width: usize) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut v = v;
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let digits = buf.len() - i;
    for _ in digits..width {
        out.push(b'0');
    }
    out.extend_from_slice(&buf[i..]);
}
