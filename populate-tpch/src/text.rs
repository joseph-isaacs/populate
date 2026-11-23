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

use crate::rng::{A, Rng, mulmod, powmod, scale};
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
    /// The shared text, built on first use on all cores.
    pub(crate) fn get() -> &'static Text {
        Self::init(0)
    }

    /// The shared text, built on `threads` threads (0 = all cores) if this
    /// is the first use.
    pub(crate) fn init(threads: usize) -> &'static Text {
        TEXT.get_or_init(|| {
            let d = Distributions::static_default();
            let pool: &'static [u8] = Box::leak(build_pool(d, threads).into_boxed_slice());
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

/// A distribution as the pool's grammar reads it: for each unit of
/// weight, the picked value's bytes, padded to 16 for a fixed-size copy.
struct Words {
    picks: Vec<([u8; 16], u8)>,
    long: Vec<&'static [u8]>,
}

impl Words {
    fn new(d: &Distribution) -> Self {
        let mut picks = Vec::new();
        let mut long = Vec::new();
        let mut prev = 0;
        for i in 0..d.size() {
            let w = d.get_weight(i);
            let v = d.get_values()[i].as_bytes();
            let mut b = [0u8; 16];
            let tag = if v.len() <= 16 {
                b[..v.len()].copy_from_slice(v);
                v.len() as u8
            } else {
                long.push(v);
                b[..8].copy_from_slice(&(long.len() as u64 - 1).to_le_bytes());
                u8::MAX
            };
            picks.extend(std::iter::repeat_n((b, tag), (w - prev) as usize));
            prev = w;
        }
        Words { picks, long }
    }

    /// The next draw's value, appended to `out`.
    #[inline(always)]
    fn put(&self, r: &mut PoolRng, out: &mut Out) {
        let x = r.next();
        let (b, n) = &self.picks[scale(x, self.picks.len() as u32) as usize];
        if *n != u8::MAX {
            out.put16(b, *n as usize);
        } else {
            let i = u64::from_le_bytes(b[..8].try_into().expect("8 bytes"));
            out.bytes(self.long[i as usize]);
        }
    }

    /// The next draw's value (grammar syntax).
    #[inline(always)]
    fn syntax(&self, r: &mut PoolRng) -> ([u8; 16], usize) {
        let x = r.next();
        let (b, n) = self.picks[scale(x, self.picks.len() as u32) as usize];
        assert!(n != u8::MAX, "short syntax");
        (b, n as usize)
    }
}

/// Text being generated, written with fixed-size copies into reserved
/// room.
struct Out {
    buf: Vec<u8>,
}

impl Out {
    fn with_capacity(n: usize) -> Self {
        Out {
            buf: Vec::with_capacity(n + MAX_SENTENCE + 16),
        }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    /// Make room for one more sentence (and a 16-byte overrun).
    #[inline(always)]
    fn reserve_sentence(&mut self) {
        self.buf.reserve(MAX_SENTENCE + 32);
    }

    #[inline(always)]
    fn put16(&mut self, b: &[u8; 16], n: usize) {
        let len = self.buf.len();
        debug_assert!(len + 16 <= self.buf.capacity());
        // SAFETY: `reserve_sentence` left room for a whole sentence plus a
        // 16-byte overrun; only the first `n` bytes are kept.
        unsafe {
            self.buf
                .as_mut_ptr()
                .add(len)
                .cast::<[u8; 16]>()
                .write_unaligned(*b);
            self.buf.set_len(len + n);
        }
    }

    #[inline(always)]
    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    #[inline(always)]
    fn push(&mut self, b: u8) {
        // SAFETY: as `put16`.
        unsafe {
            let len = self.buf.len();
            self.buf.as_mut_ptr().add(len).write(b);
            self.buf.set_len(len + 1);
        }
    }

    #[inline(always)]
    fn pop(&mut self) {
        self.buf.pop();
    }

    fn last(&self) -> Option<u8> {
        self.buf.last().copied()
    }
}

/// The pool's stream: one stream that never skips, with its draw count.
#[derive(Clone, Copy)]
struct PoolRng {
    seed: u64,
    /// Draws made so far.
    k: u64,
}

const POOL_SEED: u64 = 933588178;

impl PoolRng {
    /// The stream after `k` draws.
    fn at(k: u64) -> Self {
        PoolRng {
            seed: mulmod(POOL_SEED, powmod(A, k)),
            k,
        }
    }

    #[inline(always)]
    fn next(&mut self) -> u32 {
        self.seed = mulmod(self.seed, A);
        self.k += 1;
        self.seed as u32
    }
}

/// dbgen's sentence grammar (tpchgen's `TextPool`).
struct Grammar {
    grammar: Words,
    np: Words,
    vp: Words,
    prepositions: Words,
    terminators: Words,
    articles: Words,
    adjectives: Words,
    adverbs: Words,
    nouns: Words,
    verbs: Words,
    auxiliaries: Words,
}

impl Grammar {
    fn new(d: &Distributions) -> Self {
        Grammar {
            grammar: Words::new(d.grammar()),
            np: Words::new(d.noun_phrase()),
            vp: Words::new(d.verb_phrase()),
            prepositions: Words::new(d.prepositions()),
            terminators: Words::new(d.terminators()),
            articles: Words::new(d.articles()),
            adjectives: Words::new(d.adjectives()),
            adverbs: Words::new(d.adverbs()),
            nouns: Words::new(d.nouns()),
            verbs: Words::new(d.verbs()),
            auxiliaries: Words::new(d.auxiliaries()),
        }
    }

    #[inline(always)]
    fn noun_phrase(&self, out: &mut Out, r: &mut PoolRng) {
        let (syntax, n) = self.np.syntax(r);
        for &c in &syntax[..n] {
            let src = match c {
                b'A' => &self.articles,
                b'J' => &self.adjectives,
                b'D' => &self.adverbs,
                b'N' => &self.nouns,
                b',' => {
                    out.pop();
                    out.push(b',');
                    out.push(b' ');
                    continue;
                }
                b' ' => continue,
                c => panic!("unknown noun phrase token {c}"),
            };
            src.put(r, out);
            out.push(b' ');
        }
    }

    #[inline(always)]
    fn verb_phrase(&self, out: &mut Out, r: &mut PoolRng) {
        let (syntax, n) = self.vp.syntax(r);
        for &c in syntax[..n].iter().step_by(2) {
            let src = match c {
                b'D' => &self.adverbs,
                b'V' => &self.verbs,
                b'X' => &self.auxiliaries,
                c => panic!("unknown verb phrase token {c}"),
            };
            src.put(r, out);
            out.push(b' ');
        }
    }

    /// One sentence, appended to `out`.
    fn sentence(&self, out: &mut Out, r: &mut PoolRng) {
        out.reserve_sentence();
        let (syntax, n) = self.grammar.syntax(r);
        for &c in syntax[..n].iter().step_by(2) {
            match c {
                b'V' => self.verb_phrase(out, r),
                b'N' => self.noun_phrase(out, r),
                b'P' => {
                    self.prepositions.put(r, out);
                    out.bytes(b" the ");
                    self.noun_phrase(out, r);
                }
                b'T' => {
                    out.pop();
                    self.terminators.put(r, out);
                }
                c => panic!("unknown grammar token {c}"),
            }
            if out.last() != Some(b' ') {
                out.push(b' ');
            }
        }
    }
}

/// Sentences parsed from draw `start` on, as if a sentence began there,
/// until one begins at or after draw `end`.
struct Segment {
    out: Vec<u8>,
    /// `(draw, offset in out)` where each sentence began.
    starts: Vec<(u64, usize)>,
    /// Where the next sentence after the last one begins.
    end: u64,
}

fn segment(g: &Grammar, start: u64, end: u64, bytes: usize) -> Segment {
    let mut r = PoolRng::at(start);
    let mut out = Out::with_capacity(bytes);
    let mut starts = Vec::new();
    while r.k < end {
        starts.push((r.k, out.len()));
        g.sentence(&mut out, &mut r);
    }
    Segment {
        out: out.buf,
        starts,
        end: r.k,
    }
}

/// dbgen's text pool (tpchgen's `TextPool::new`): sentences from one stream
/// until there are `POOL_SIZE` bytes. Tested equal to tpchgen's pool.
///
/// Built on all cores by speculation. The stream is split into segments of
/// draws, and each segment is parsed from its first draw as if a sentence
/// began there. That guess is usually wrong, but the parse is a function of
/// the draw index alone once it is at a sentence start, so as soon as the
/// true parse (continued serially from the previous segment's end) begins
/// a sentence at a draw where the speculative one did too, the rest of the
/// segment is exactly the true text and is taken as is. Each check is
/// exact, so the result is the serial pool whatever the speculation finds;
/// in practice the two parses meet within a few sentences.
pub(crate) fn build_pool(d: &Distributions, threads: usize) -> Vec<u8> {
    let g = Grammar::new(d);
    // draws per byte, from the first sentences
    let sample = segment(&g, 0, 4096, 0);
    let per_byte = sample.end as f64 / sample.out.len() as f64;
    let total = (POOL_SIZE as f64 * per_byte * 1.01) as u64;
    let threads = match threads {
        0 => std::thread::available_parallelism().map_or(1, |t| t.get()),
        t => t,
    };
    let parts = if threads == 1 { 1 } else { threads as u64 * 4 };
    let bounds: Vec<u64> = (0..=parts).map(|i| total * i / parts).collect();
    let bytes = POOL_SIZE / parts as usize + 1;
    if parts == 1 {
        let mut r = PoolRng::at(0);
        let mut out = Out::with_capacity(POOL_SIZE);
        while out.len() < POOL_SIZE {
            g.sentence(&mut out, &mut r);
        }
        out.buf.truncate(POOL_SIZE);
        return out.buf;
    }
    let segs: Vec<Segment> = {
        let next = std::sync::atomic::AtomicUsize::new(0);
        let done: Vec<std::sync::Mutex<Option<Segment>>> =
            (0..parts).map(|_| std::sync::Mutex::new(None)).collect();
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= parts as usize {
                            break;
                        }
                        let seg = segment(&g, bounds[i], bounds[i + 1], bytes);
                        *done[i].lock().expect("segment") = Some(seg);
                    }
                });
            }
        });
        done.into_iter()
            .map(|m| m.into_inner().expect("segment").expect("built"))
            .collect()
    };

    // Stitch: segment 0 is true. Continue the true parse into each next
    // segment (into a small bridge) until it meets one of that segment's
    // sentence starts; from there the segment is the true text.
    let mut pieces: Vec<(usize, &[u8])> = Vec::new(); // (segment, bytes)
    let mut bridges: Vec<Vec<u8>> = Vec::new();
    let mut at = segs[0].end;
    let mut len = segs[0].out.len();
    pieces.push((0, &segs[0].out));
    let mut bridge_of = Vec::new(); // piece index -> bridge index
    for (i, seg) in segs.iter().enumerate().skip(1) {
        if len >= POOL_SIZE {
            break;
        }
        let mut r = PoolRng::at(at);
        let mut bridge = Out::with_capacity(0);
        let mut taken = None;
        loop {
            if let Ok(k) = seg.starts.binary_search_by_key(&r.k, |&(k, _)| k) {
                taken = Some(&seg.out[seg.starts[k].1..]);
                r = PoolRng::at(seg.end);
                break;
            }
            if r.k >= seg.end {
                break; // never met: the true parse covered the segment
            }
            g.sentence(&mut bridge, &mut r);
        }
        at = r.k;
        len += bridge.len();
        bridge_of.push((pieces.len(), bridges.len()));
        pieces.push((usize::MAX, &[]));
        bridges.push(bridge.buf);
        if let Some(t) = taken {
            len += t.len();
            pieces.push((i, t));
        }
    }
    let mut tail = Out::with_capacity(0);
    let mut r = PoolRng::at(at);
    while len + tail.len() < POOL_SIZE {
        g.sentence(&mut tail, &mut r);
    }
    let tail = tail.buf;
    for &(p, b) in &bridge_of {
        pieces[p].1 = &bridges[b];
    }
    pieces.push((usize::MAX, &tail));

    // copy the pieces into place on all cores
    let mut out = vec![0u8; POOL_SIZE];
    let mut jobs = Vec::new();
    let mut rest: &mut [u8] = &mut out;
    for &(_, bytes) in &pieces {
        let n = bytes.len().min(rest.len());
        let (dst, r) = rest.split_at_mut(n);
        jobs.push((dst, &bytes[..n]));
        rest = r;
    }
    let next = std::sync::Mutex::new(jobs.into_iter());
    // a closure, so the lock is released before each copy
    let take = || next.lock().expect("jobs").next();
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                while let Some((dst, src)) = take() {
                    dst.copy_from_slice(src);
                }
            });
        }
    });
    drop(pieces);
    std::thread::scope(|s| {
        for seg in segs {
            s.spawn(move || drop(seg));
        }
    });
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
