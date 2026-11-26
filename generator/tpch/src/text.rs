//! TPC-H's text: the 300 MiB text pool comments are cut from, the value
//! distributions, and `Utf8View` columns that point into both without
//! copying.
//!
//! Every string column is a `StringViewArray` with the same three buffers:
//! [`DATA`] (the batch's own bytes: names, addresses, phone numbers),
//! [`POOL`] (the text pool, shared by every batch) and [`DICT`] (the
//! distribution values longer than a view holds, shared too). A comment is a view of `(offset, len)`
//! into the pool and a distribution pick is a precomputed view, so neither
//! copies a byte.

use populate_combinators::lehmer::{draws, scale};
#[cfg(feature = "formats")]
use populate_combinators::views::view;
use populate_combinators::views::{Buffers, Dict, Picks, Shared, by_weight, static_buffer};
use populate_core::sched::resolve_threads;
use std::sync::{LazyLock, Mutex, OnceLock};
use tpchgen::distribution::{Distribution, Distributions};

#[cfg(doc)]
use populate_combinators::views::DATA;
/// Buffer index of the text pool.
#[cfg(feature = "formats")]
pub(crate) const POOL: u32 = 1;
/// Buffer index of the distribution values.
pub(crate) const DICT: u32 = 2;

/// Size of dbgen's text pool.
pub(crate) const POOL_SIZE: usize = 300 * 1024 * 1024;
/// Longest sentence the grammar makes (the pool is generated past its
/// size by at most this, then truncated).
const MAX_SENTENCE: usize = 256;

/// The distributions, as views into [`DICT`]. Cheap to
/// build, so the generators hold a reference from the start.
#[derive(Debug)]
pub(crate) struct Dicts {
    bytes: &'static [u8],
    pub(crate) colors: Picks,
    pub(crate) types: Picks,
    pub(crate) containers: Picks,
    pub(crate) segments: Picks,
    pub(crate) priorities: Picks,
    pub(crate) return_flags: Picks,
    pub(crate) instructions: Picks,
    pub(crate) modes: Picks,
    pub(crate) nations: Picks,
    /// `n_regionkey` of each nation.
    pub(crate) nation_regions: Vec<i64>,
    pub(crate) regions: Picks,
}

static DICTS: LazyLock<Dicts> = LazyLock::new(|| {
    let d = Distributions::static_default();
    let mut dict = Dict::new(DICT);
    let mut dist = |x: &Distribution| Picks::new(x.get_values(), |i| x.get_weight(i), &mut dict);
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
    Dicts {
        bytes: dict.leak(),
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
});

impl Dicts {
    pub(crate) fn get() -> &'static Dicts {
        &DICTS
    }
}

/// Everything string columns share: the pool and the buffers views point
/// into.
#[derive(Debug)]
pub(crate) struct Text {
    /// The pool: ASCII (dbgen's grammar words and punctuation; the tests
    /// compare it to tpchgen's `&str` pool).
    pub(crate) pool: &'static str,
    /// `[pool, dict]`, buffers [`POOL`] and [`DICT`].
    pub(crate) shared: Shared,
}

static TEXT: OnceLock<Text> = OnceLock::new();

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
            // SAFETY: `build_pool` writes only the grammar's ASCII words and
            // punctuation (checked against tpchgen's pool by the tests), so
            // the bytes are UTF-8; a 300 MiB validation would slow startup
            let pool = unsafe { std::str::from_utf8_unchecked(pool) };
            Text {
                pool,
                shared: Shared::new(vec![
                    static_buffer(pool.as_bytes()),
                    static_buffer(Dicts::get().bytes),
                ]),
            }
        })
    }

    /// The view of pool bytes `offset..offset + len`.
    #[inline(always)]
    #[cfg(feature = "formats")]
    pub(crate) fn pool_view(&self, offset: u32, len: u32) -> u128 {
        let (o, n) = (offset as usize, len as usize);
        if n <= 12 {
            view(&self.pool.as_bytes()[o..o + n], POOL, offset)
        } else {
            // only the 4-byte prefix is read, not the whole span's bounds:
            // a cache miss less per comment
            let prefix =
                u32::from_le_bytes(self.pool.as_bytes()[o..o + 4].try_into().expect("4 bytes"));
            n as u128 | (prefix as u128) << 32 | (POOL as u128) << 64 | (offset as u128) << 96
        }
    }
}

/// TPC-H's string buffers ([`Text`]), for the column types.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Tpch;

impl Buffers for Tpch {
    fn shared() -> &'static Shared {
        &Text::get().shared
    }
}

/// One distribution's words, each with the text that follows it in the
/// pool (a space, `", "`, `" the "`), padded to 32 bytes for a fixed-size
/// copy: one entry per unit of weight, so a draw indexes it directly.
struct Table {
    picks: Vec<([u8; 32], u8)>,
}

impl Table {
    fn new(d: &Distribution, suffix: impl Fn(&[u8]) -> &'static [u8]) -> Self {
        let picks = by_weight(
            d.size(),
            |i| d.get_weight(i),
            |i| {
                let v = d.get_values()[i].as_bytes();
                let s = suffix(v);
                let n = v.len() + s.len();
                assert!(n <= 32, "pool word too long");
                let mut b = [0u8; 32];
                b[..v.len()].copy_from_slice(v);
                b[v.len()..n].copy_from_slice(s);
                (b, n as u8)
            },
        );
        Table { picks }
    }
}

/// A distribution's values, by weight unit (the grammar's syntax strings).
fn syntax(d: &Distribution) -> Vec<&'static str> {
    by_weight(d.size(), |i| d.get_weight(i), |i| d.get_values()[i])
}

/// Text being generated into `buf`, with fixed-size copies: a sentence
/// may only start while [`ROOM`] bytes are left.
struct Out<'a> {
    buf: &'a mut [u8],
    len: usize,
}

/// Room for one sentence and a 32-byte overrun.
const ROOM: usize = MAX_SENTENCE + 32;

impl<'a> Out<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Out { buf, len: 0 }
    }

    /// Whether a sentence can start.
    #[inline(always)]
    fn has_room(&self) -> bool {
        self.len + ROOM <= self.buf.len()
    }

    /// Drop the last `pop` bytes (0 or 1), then append the first `n` bytes
    /// of `b`.
    #[inline(always)]
    fn put32(&mut self, pop: usize, b: &[u8; 32], n: usize) {
        let len = self.len - pop;
        debug_assert!(len + 32 <= self.buf.len());
        // SAFETY: a sentence starts only with `ROOM` bytes left (checked in
        // `Grammar::sentence`), enough for the whole sentence plus a 32-byte
        // overrun; only the first `n` bytes are kept.
        unsafe {
            self.buf
                .as_mut_ptr()
                .add(len)
                .cast::<[u8; 32]>()
                .write_unaligned(*b);
        }
        self.len = len + n;
    }
}

/// The pool's stream: one stream that never skips, with its draw count.
/// Draws are made [`POOL_BLOCK`] at a time by independent lanes
/// ([`draws`]), so the parse never waits on the multiply chain.
struct PoolRng {
    buf: Box<[u32; POOL_BLOCK]>,
    /// Draws consumed from `buf`.
    i: usize,
    /// Draws made before `buf[0]`.
    base: u64,
}

const POOL_SEED: u64 = 933588178;
const POOL_BLOCK: usize = 4096;

impl PoolRng {
    /// The stream after `k` draws.
    fn at(k: u64) -> Self {
        let mut r = PoolRng {
            buf: Box::new([0; POOL_BLOCK]),
            i: 0,
            base: k,
        };
        draws(POOL_SEED as i64, k, &mut r.buf[..]);
        r
    }

    /// Draws made so far.
    #[cfg(test)]
    fn k(&self) -> u64 {
        self.base + self.i as u64
    }

    #[inline(always)]
    fn next(&mut self) -> u32 {
        if self.i == POOL_BLOCK {
            self.refill();
        }
        let x = self.buf[self.i];
        self.i += 1;
        x
    }

    #[cold]
    fn refill(&mut self) {
        self.base += POOL_BLOCK as u64;
        self.i = 0;
        draws(POOL_SEED as i64, self.base, &mut self.buf[..]);
    }
}

/// No table / no phrase.
const NONE: u8 = u8::MAX;

/// One word: a draw from table `table`, after dropping `pop` bytes.
#[derive(Clone, Copy)]
struct Op {
    table: u8,
    pop: u8,
}

/// A phrase's words, in draw order.
#[derive(Clone, Copy)]
struct Phrase {
    ops: [Op; 4],
    n: u8,
}

/// One token of a sentence: an optional word (`pre`), then an optional
/// phrase drawn from `set` (noun or verb phrases).
#[derive(Clone, Copy)]
struct Slot {
    pre: Op,
    set: u8,
}

#[derive(Clone, Copy)]
struct Sentence {
    slots: [Slot; 6],
    n: u8,
}

/// Phrases by weight unit, for one phrase distribution.
struct PhraseSet {
    picks: Vec<Phrase>,
}

/// dbgen's sentence grammar (tpchgen's `TextPool`), compiled: each
/// sentence, noun-phrase and verb-phrase syntax becomes a list of word
/// draws, each a fixed-size copy of a word with its following text, so
/// generating a sentence reads no syntax characters. Draws are made in
/// tpchgen's order: the sentence's syntax, then per token the phrase's
/// syntax and its words.
struct Grammar {
    tables: Vec<Table>,
    sets: [PhraseSet; 2],
    sentences: Vec<Sentence>,
}

impl Grammar {
    fn new(d: &Distributions) -> Self {
        let space: fn(&[u8]) -> &'static [u8] = |_| b" ";
        let comma: fn(&[u8]) -> &'static [u8] = |_| b", ";
        // tables: 0 adjectives, 1 adjectives + ", ", 2 adverbs, 3 nouns,
        // 4 articles, 5 verbs, 6 auxiliaries, 7 prepositions + " the ",
        // 8 terminators + " "
        let tables = vec![
            Table::new(d.adjectives(), space),
            Table::new(d.adjectives(), comma),
            Table::new(d.adverbs(), space),
            Table::new(d.nouns(), space),
            Table::new(d.articles(), space),
            Table::new(d.verbs(), space),
            Table::new(d.auxiliaries(), space),
            Table::new(d.prepositions(), |_| b" the "),
            // a sentence token is followed by a space unless it ends in one
            Table::new(d.terminators(), |v| {
                if v.last() == Some(&b' ') { b"" } else { b" " }
            }),
        ];
        let op = |table| Op { table, pop: 0 };
        let phrase = |ops: &[Op]| {
            assert!(ops.len() <= 4, "phrase too long");
            let mut p = Phrase {
                ops: [op(NONE); 4],
                n: ops.len() as u8,
            };
            p.ops[..ops.len()].copy_from_slice(ops);
            p
        };
        let noun_phrase = |s: &str| {
            let mut ops: Vec<Op> = Vec::new();
            for c in s.bytes() {
                match c {
                    b'A' => ops.push(op(4)),
                    b'J' => ops.push(op(0)),
                    b'D' => ops.push(op(2)),
                    b'N' => ops.push(op(3)),
                    // "J," : the adjective is followed by ", "
                    b',' => {
                        let last = ops.last_mut().expect("',' after a word");
                        assert!(last.table == 0, "',' after an adjective");
                        last.table = 1;
                    }
                    b' ' => {}
                    c => panic!("unknown noun phrase token {c}"),
                }
            }
            phrase(&ops)
        };
        let verb_phrase = |s: &str| {
            let ops: Vec<Op> = s
                .bytes()
                .step_by(2)
                .map(|c| match c {
                    b'D' => op(2),
                    b'V' => op(5),
                    b'X' => op(6),
                    c => panic!("unknown verb phrase token {c}"),
                })
                .collect();
            phrase(&ops)
        };
        let sets = [
            PhraseSet {
                picks: syntax(d.noun_phrase())
                    .into_iter()
                    .map(noun_phrase)
                    .collect(),
            },
            PhraseSet {
                picks: syntax(d.verb_phrase())
                    .into_iter()
                    .map(verb_phrase)
                    .collect(),
            },
        ];
        let sentence = |s: &str| {
            let slots: Vec<Slot> = s
                .bytes()
                .step_by(2)
                .map(|c| match c {
                    b'N' => Slot {
                        pre: op(NONE),
                        set: 0,
                    },
                    b'V' => Slot {
                        pre: op(NONE),
                        set: 1,
                    },
                    b'P' => Slot { pre: op(7), set: 0 },
                    // drop the space before the terminator
                    b'T' => Slot {
                        pre: Op { table: 8, pop: 1 },
                        set: NONE,
                    },
                    c => panic!("unknown grammar token {c}"),
                })
                .collect();
            assert!(slots.len() <= 6, "sentence too long");
            let mut t = Sentence {
                slots: [Slot {
                    pre: op(NONE),
                    set: NONE,
                }; 6],
                n: slots.len() as u8,
            };
            t.slots[..slots.len()].copy_from_slice(&slots);
            t
        };
        let sentences = syntax(d.grammar()).into_iter().map(sentence).collect();
        Grammar {
            tables,
            sets,
            sentences,
        }
    }

    #[inline(always)]
    fn word(&self, op: Op, out: &mut Out, r: &mut PoolRng) {
        let t = &self.tables[op.table as usize];
        let (b, n) = &t.picks[scale(r.next(), t.picks.len() as u32) as usize];
        out.put32(op.pop as usize, b, *n as usize);
    }

    /// One sentence, appended to `out`.
    fn sentence(&self, out: &mut Out, r: &mut PoolRng) {
        assert!(out.has_room(), "no room for a sentence");
        let s = &self.sentences[scale(r.next(), self.sentences.len() as u32) as usize];
        for slot in &s.slots[..s.n as usize] {
            if slot.pre.table != NONE {
                self.word(slot.pre, out, r);
            }
            if slot.set != NONE {
                let set = &self.sets[slot.set as usize].picks;
                let p = &set[scale(r.next(), set.len() as u32) as usize];
                for &op in &p.ops[..p.n as usize] {
                    self.word(op, out, r);
                }
            }
        }
    }
}

/// Sentences from draw `k` on into `buf`, until exactly `buf.len()`
/// bytes are written (`exact`, so `buf` must end at a sentence start), or
/// at least `buf.len()` (the pool's end, cut mid-sentence).
fn fill(g: &Grammar, k: u64, buf: &mut [u8], exact: bool) {
    let mut r = PoolRng::at(k);
    let end = buf.len();
    let mut out = Out::new(buf);
    while out.has_room() {
        g.sentence(&mut out, &mut r);
    }
    // the last sentences into scratch room, then copied
    let (at, mut tail) = (out.len, Vec::new());
    let mut scratch = vec![0u8; ROOM * 2];
    while at + tail.len() < end {
        let mut o = Out::new(&mut scratch);
        g.sentence(&mut o, &mut r);
        let n = o.len;
        tail.extend_from_slice(&scratch[..n]);
    }
    assert!(!exact || at + tail.len() == end, "pool index out of date");
    buf[at..].copy_from_slice(&tail[..end - at]);
}

/// Ask Linux to back `buf` with 2 MiB pages where it can (before it is
/// first written, so the pages come in huge). Comments are read from random
/// places in the 300 MiB pool, and with 4 KiB pages nearly every read also
/// misses the TLB.
fn huge_pages(buf: &mut [u8]) {
    #[cfg(target_os = "linux")]
    {
        const HUGE: usize = 2 << 20;
        let start = (buf.as_ptr() as usize).next_multiple_of(HUGE);
        let end = (buf.as_ptr() as usize + buf.len()) / HUGE * HUGE;
        if end > start {
            // SAFETY: the range is inside `buf`, page-aligned; MADV_HUGEPAGE
            // is advice only and never changes the memory's contents.
            unsafe {
                libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE);
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = buf;
}

/// dbgen's text pool (tpchgen's `TextPool::new`): sentences from one stream
/// until there are `POOL_SIZE` bytes. Tested equal to tpchgen's pool.
///
/// The parse is sequential: where a sentence starts depends on every
/// earlier one. [`POOL_INDEX`] holds, for evenly spaced points of the pool,
/// the draw and byte at which a sentence starts there (tested against the
/// serial parse), so the parts between them are generated independently,
/// on all cores, each straight into its place.
pub(crate) fn build_pool(d: &Distributions, threads: usize) -> Vec<u8> {
    let g = Grammar::new(d);
    let threads = resolve_threads(threads);
    // whole parts of the index, 4 per thread, the last to the pool's end
    let step = (POOL_INDEX.len() / (threads * 4)).max(1);
    let marks: Vec<(u64, usize)> = POOL_INDEX
        .iter()
        .step_by(step)
        .map(|&(k, b)| (k, b as usize))
        .collect();
    let mut out = vec![0u8; POOL_SIZE];
    huge_pages(&mut out);
    let mut parts = Vec::new();
    let mut rest: &mut [u8] = &mut out;
    for (i, &(k, b)) in marks.iter().enumerate() {
        let end = marks.get(i + 1).map_or(POOL_SIZE, |m| m.1);
        let (part, r) = rest.split_at_mut(end - b);
        parts.push((k, part, i + 1 < marks.len()));
        rest = r;
    }
    if threads == 1 {
        for (k, part, exact) in parts {
            fill(&g, k, part, exact);
        }
        return out;
    }
    let next = Mutex::new(parts.into_iter());
    // a closure, so the lock is released before each part
    let take = || next.lock().expect("parts").next();
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                while let Some((k, part, exact)) = take() {
                    fill(&g, k, part, exact);
                }
            });
        }
    });
    out
}

include!("pool_index.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use tpchgen::text::TextPool;

    #[test]
    fn pool_matches_tpchgen() {
        let ours = &Text::get().pool;
        let theirs = TextPool::get_or_init_default();
        assert_eq!(ours.len(), theirs.size() as usize);
        assert!(**ours == *theirs.text(0, theirs.size()));
    }

    /// Sentence starts in the pool: `(draws before, byte offset)` of the first
    /// sentence at or after each `i * POOL_SIZE / POOL_MARKS`.
    fn pool_marks(d: &Distributions) -> Vec<(u64, u32)> {
        let g = Grammar::new(d);
        let mut r = PoolRng::at(0);
        let mut buf = vec![0u8; ROOM * 2];
        let (mut at, mut marks) = (0usize, Vec::new());
        while marks.len() < POOL_MARKS {
            if at >= marks.len() * POOL_SIZE / POOL_MARKS {
                marks.push((r.k(), at as u32));
            }
            let mut o = Out::new(&mut buf);
            g.sentence(&mut o, &mut r);
            at += o.len;
        }
        marks
    }

    const POOL_MARKS: usize = 512;

    /// `POOL_INDEX` is the serial parse's. `POOL_INDEX_UPDATE=1` rewrites
    /// `src/pool_index.rs` from it.
    #[test]
    fn pool_index_matches_serial_parse() {
        let marks = pool_marks(Distributions::static_default());
        if std::env::var_os("POOL_INDEX_UPDATE").is_some() {
            let mut f = String::from(
                "// Generated by `POOL_INDEX_UPDATE=1 cargo test -p populate-gen-tpch pool_index`.\n\
                 /// `(draws before, byte offset)` of the pool's sentence starts\n\
                 /// ([`pool_marks`]).\n",
            );
            f += &format!("const POOL_INDEX: [(u64, u32); {POOL_MARKS}] = [\n");
            for (k, b) in &marks {
                f += &format!("    ({k}, {b}),\n");
            }
            f += "];\n";
            std::fs::write(concat!(env!("CARGO_MANIFEST_DIR"), "/src/pool_index.rs"), f)
                .expect("write pool_index.rs");
        }
        assert!(marks == POOL_INDEX, "run with POOL_INDEX_UPDATE=1");
    }

    #[test]
    fn serial_pool_matches_parallel() {
        let d = Distributions::static_default();
        assert!(build_pool(d, 1) == Text::get().pool.as_bytes());
    }

    #[test]
    fn picks_match_tpchgen() {
        use populate_combinators::lehmer::Rng;
        use tpchgen::random::RowRandomInt;
        let d = Distributions::static_default();
        let t = Dicts::get();
        for (ours, theirs) in [
            (&t.types, d.part_types()),
            (&t.priorities, d.order_priority()),
            (&t.modes, d.ship_modes()),
            (&t.return_flags, d.return_flags()),
        ] {
            let mut a = Rng::at(42, 1, 0);
            let mut b = RowRandomInt::new(42, 1);
            for _ in 0..10_000 {
                assert_eq!(
                    ours.values[ours.index(a.next_rand())],
                    theirs.random_value(&mut b)
                );
            }
        }
    }
}
