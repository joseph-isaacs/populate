//! TPC-H's text: the 300 MiB text pool comments are cut from, the value
//! distributions, and `Utf8View` columns that point into both without
//! copying.
//!
//! Every string column is a `StringViewArray` with the same three buffers:
//! [`DATA`] (the batch's own bytes: names, addresses, phone numbers),
//! [`POOL`] (the text pool, shared by every batch) and [`DICT`] (every
//! distribution value, shared too). A comment is a view of `(offset, len)`
//! into the pool and a distribution pick is a precomputed view, so neither
//! copies a byte.

use crate::rng::{Rng, scale};
use arrow_array::StringViewArray;
use arrow_buffer::{Buffer, ScalarBuffer};
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use tpchgen::distribution::{Distribution, Distributions};

/// Buffer index of a batch's own string bytes.
pub(crate) const DATA: u32 = 0;
/// Buffer index of the text pool.
pub(crate) const POOL: u32 = 1;
/// Buffer index of the distribution values.
pub(crate) const DICT: u32 = 2;

/// Size of dbgen's text pool.
pub(crate) const POOL_SIZE: usize = 300 * 1024 * 1024;
/// Longest sentence the grammar makes (the pool is generated past its
/// size by at most this, then truncated).
const MAX_SENTENCE: usize = 256;

/// The view of `bytes`, stored at `offset` of buffer `buffer` (ignored for
/// strings of up to 12 bytes, which live in the view).
#[inline(always)]
pub(crate) fn view(bytes: &[u8], buffer: u32, offset: u32) -> u128 {
    let len = bytes.len();
    if len <= 12 {
        let mut b = [0u8; 16];
        b[4..4 + len].copy_from_slice(bytes);
        u128::from_le_bytes(b) | len as u128
    } else {
        let prefix = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes"));
        len as u128 | (prefix as u128) << 32 | (buffer as u128) << 64 | (offset as u128) << 96
    }
}

/// A distribution: dbgen's weighted pick, as views into [`DICT`].
#[derive(Debug)]
pub(crate) struct Dist {
    /// Value index for each unit of weight.
    picks: Vec<u8>,
    /// Each value's view.
    pub(crate) views: Vec<u128>,
    pub(crate) values: Vec<&'static str>,
}

impl Dist {
    fn new(d: &Distribution, dict: &mut Vec<u8>) -> Self {
        let values = d.get_values().to_vec();
        let mut picks = Vec::new();
        let mut views = Vec::new();
        let mut prev = 0;
        for (i, v) in values.iter().enumerate() {
            let w = d.get_weight(i);
            picks.extend(std::iter::repeat_n(i as u8, (w - prev).max(0) as usize));
            prev = w;
            views.push(view(v.as_bytes(), DICT, dict.len() as u32));
            dict.extend_from_slice(v.as_bytes());
        }
        assert!(values.len() <= 256);
        Dist {
            picks,
            views,
            values,
        }
    }

    /// Total weight: picks are `0..weight`.
    #[inline(always)]
    pub(crate) fn weight(&self) -> u32 {
        self.picks.len() as u32
    }

    /// The value index draw `x` picks (`Distribution::random_value`).
    #[inline(always)]
    pub(crate) fn pick_draw(&self, x: u32) -> usize {
        // weights are small, so the integer mapping is exact (see `scale`)
        self.picks[scale(x, self.weight()) as usize] as usize
    }

    /// The value index the next draw of `r` picks.
    #[inline(always)]
    pub(crate) fn pick(&self, r: &mut Rng) -> usize {
        let x = r.next_rand();
        self.pick_draw(x)
    }
}

/// Everything shared by all batches: the pool, the distributions and the
/// buffers their views point into.
#[derive(Debug)]
pub(crate) struct Text {
    pub(crate) pool: &'static [u8],
    pool_buf: Buffer,
    dict_buf: Buffer,
    /// `[empty, pool, dict]`, for columns with no bytes of their own.
    shared: Arc<[Buffer]>,
    pub(crate) colors: Dist,
    pub(crate) types: Dist,
    pub(crate) containers: Dist,
    pub(crate) segments: Dist,
    pub(crate) priorities: Dist,
    pub(crate) return_flags: Dist,
    pub(crate) instructions: Dist,
    pub(crate) modes: Dist,
    pub(crate) nations: Dist,
    /// `n_regionkey` of each nation.
    pub(crate) nation_regions: Vec<i64>,
    pub(crate) regions: Dist,
}

static TEXT: OnceLock<Text> = OnceLock::new();

/// `bytes` as an Arrow buffer, without copying.
fn static_buffer(bytes: &'static [u8]) -> Buffer {
    let ptr = NonNull::new(bytes.as_ptr() as *mut u8).expect("non-null");
    // SAFETY: the bytes are 'static and never written, so they outlive the
    // buffer and are valid for `len` bytes; the owner is a no-op.
    unsafe { Buffer::from_custom_allocation(ptr, bytes.len(), Arc::new(())) }
}

impl Text {
    /// The shared text, built on first use (the pool takes ~0.3 s).
    pub(crate) fn get() -> &'static Text {
        TEXT.get_or_init(|| {
            let d = Distributions::static_default();
            let pool: &'static [u8] = Box::leak(build_pool(d).into_boxed_slice());
            let mut dict = Vec::new();
            let mut dist = |x: &Distribution| Dist::new(x, &mut dict);
            let colors = dist(d.part_colors());
            let types = dist(d.part_types());
            let containers = dist(d.part_containers());
            let segments = dist(d.market_segments());
            let priorities = dist(d.order_priority());
            let return_flags = dist(d.return_flags());
            let instructions = dist(d.ship_instructions());
            let modes = dist(d.ship_modes());
            let nations = dist(d.nations());
            let regions = dist(d.regions());
            let nation_regions = (0..d.nations().size())
                .map(|i| d.nations().get_weight(i) as i64)
                .collect();
            let dict: &'static [u8] = Box::leak(dict.into_boxed_slice());
            let (pool_buf, dict_buf) = (static_buffer(pool), static_buffer(dict));
            Text {
                pool,
                shared: Arc::from(vec![
                    Buffer::from_vec(Vec::<u8>::new()),
                    pool_buf.clone(),
                    dict_buf.clone(),
                ]),
                pool_buf,
                dict_buf,
                colors,
                types,
                containers,
                segments,
                priorities,
                return_flags,
                instructions,
                modes,
                nations,
                nation_regions,
                regions,
            }
        })
    }

    /// The view of pool bytes `offset..offset + len`.
    #[inline(always)]
    pub(crate) fn pool_view(&self, offset: u32, len: u32) -> u128 {
        let (o, n) = (offset as usize, len as usize);
        if n <= 12 {
            view(&self.pool[o..o + n], POOL, offset)
        } else {
            // SAFETY: `o + 4 <= o + n <= pool.len()`, checked by the caller's
            // range (dbgen's offsets stay `max_len` short of the end).
            let prefix = u32::from_le_bytes(self.pool[o..o + 4].try_into().expect("4 bytes"));
            n as u128 | (prefix as u128) << 32 | (POOL as u128) << 64 | (offset as u128) << 96
        }
    }

    /// A string column from views and the batch's own bytes.
    pub(crate) fn array(&self, views: Vec<u128>, data: Vec<u8>) -> StringViewArray {
        let buffers = if data.is_empty() {
            self.shared.clone()
        } else {
            Arc::from(vec![
                Buffer::from_vec(data),
                self.pool_buf.clone(),
                self.dict_buf.clone(),
            ])
        };
        // SAFETY: every view is built by `view`/`pool_view` from valid UTF-8
        // (ASCII) bytes at the offset of the buffer it names.
        unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), buffers, None) }
    }
}

/// A string column being built.
#[derive(Default)]
pub(crate) struct Views {
    pub(crate) views: Vec<u128>,
    pub(crate) data: Vec<u8>,
}

impl Views {
    pub(crate) fn with_capacity(n: usize, bytes: usize) -> Self {
        Views {
            views: Vec::with_capacity(n),
            data: Vec::with_capacity(bytes),
        }
    }

    /// Append the bytes `data[start..]` (just written) as the next string.
    #[inline(always)]
    pub(crate) fn end_string(&mut self, start: usize) {
        let v = view(&self.data[start..], DATA, start as u32);
        if self.data.len() - start <= 12 {
            self.data.truncate(start);
        }
        self.views.push(v);
    }

    pub(crate) fn finish(self, t: &Text) -> StringViewArray {
        t.array(self.views, self.data)
    }
}

/// dbgen's text pool (tpchgen's `TextPool::new`), generated with the
/// integer draw mapping and the distributions as byte tables. Tested equal
/// to tpchgen's pool.
pub(crate) fn build_pool(d: &Distributions) -> Vec<u8> {
    struct Words {
        picks: Vec<u8>,
        /// Each value, with a trailing space where the grammar adds one.
        words: Vec<Vec<u8>>,
    }
    impl Words {
        fn new(d: &Distribution) -> Self {
            let mut picks = Vec::new();
            let mut prev = 0;
            for i in 0..d.size() {
                let w = d.get_weight(i);
                picks.extend(std::iter::repeat_n(i as u8, (w - prev) as usize));
                prev = w;
            }
            let words = d
                .get_values()
                .iter()
                .map(|v| v.as_bytes().to_vec())
                .collect();
            Words { picks, words }
        }
        #[inline(always)]
        fn pick(&self, r: &mut Rng) -> &[u8] {
            let x = r.next_rand();
            &self.words[self.picks[scale(x, self.picks.len() as u32) as usize] as usize]
        }
    }
    let grammar = Words::new(d.grammar());
    let np = Words::new(d.noun_phrase());
    let vp = Words::new(d.verb_phrase());
    let prepositions = Words::new(d.prepositions());
    let terminators = Words::new(d.terminators());
    let articles = Words::new(d.articles());
    let adjectives = Words::new(d.adjectives());
    let adverbs = Words::new(d.adverbs());
    let nouns = Words::new(d.nouns());
    let verbs = Words::new(d.verbs());
    let auxiliaries = Words::new(d.auxiliaries());

    let noun_phrase = |out: &mut Vec<u8>, r: &mut Rng| {
        let syntax = np.pick(r);
        for &c in syntax {
            let src = match c {
                b'A' => &articles,
                b'J' => &adjectives,
                b'D' => &adverbs,
                b'N' => &nouns,
                b',' => {
                    out.pop();
                    out.extend_from_slice(b", ");
                    continue;
                }
                b' ' => continue,
                c => panic!("unknown noun phrase token {c}"),
            };
            out.extend_from_slice(src.pick(r));
            out.push(b' ');
        }
    };
    let verb_phrase = |out: &mut Vec<u8>, r: &mut Rng| {
        let syntax = vp.pick(r);
        for &c in syntax.iter().step_by(2) {
            let src = match c {
                b'D' => &adverbs,
                b'V' => &verbs,
                b'X' => &auxiliaries,
                c => panic!("unknown verb phrase token {c}"),
            };
            out.extend_from_slice(src.pick(r));
            out.push(b' ');
        }
    };

    // one stream that never skips (`per_row` = i32::MAX in tpchgen)
    let mut r = Rng::at(933588178, u32::MAX, 0);
    let mut out = Vec::with_capacity(POOL_SIZE + MAX_SENTENCE);
    while out.len() < POOL_SIZE {
        let syntax = grammar.pick(&mut r);
        for &c in syntax.iter().step_by(2) {
            match c {
                b'V' => verb_phrase(&mut out, &mut r),
                b'N' => noun_phrase(&mut out, &mut r),
                b'P' => {
                    out.extend_from_slice(prepositions.pick(&mut r));
                    out.extend_from_slice(b" the ");
                    noun_phrase(&mut out, &mut r);
                }
                b'T' => {
                    out.pop();
                    out.extend_from_slice(terminators.pick(&mut r));
                }
                c => panic!("unknown grammar token {c}"),
            }
            if out.last() != Some(&b' ') {
                out.push(b' ');
            }
        }
    }
    out.truncate(POOL_SIZE);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpchgen::text::TextPool;

    #[test]
    fn pool_matches_tpchgen() {
        let ours = &Text::get().pool;
        let theirs = TextPool::get_or_init_default();
        assert_eq!(ours.len(), theirs.size() as usize);
        assert!(*ours == theirs.text(0, theirs.size()).as_bytes());
    }

    #[test]
    fn picks_match_tpchgen() {
        use tpchgen::random::RowRandomInt;
        let d = Distributions::static_default();
        let t = Text::get();
        for (ours, theirs) in [
            (&t.types, d.part_types()),
            (&t.priorities, d.order_priority()),
            (&t.modes, d.ship_modes()),
            (&t.return_flags, d.return_flags()),
        ] {
            let mut a = Rng::at(42, 1, 0);
            let mut b = RowRandomInt::new(42, 1);
            for _ in 0..10_000 {
                assert_eq!(ours.values[ours.pick(&mut a)], theirs.random_value(&mut b));
            }
        }
    }
}
