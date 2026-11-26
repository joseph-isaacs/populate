//! tpchgen's lineitem numeric columns, byte-identical, generated column by
//! column with a lane-parallel TPC-H RNG.
//!
//! TPC-H's RNG is the multiplicative LCG `x' = 16807 * x mod (2^31 - 1)`.
//! Every lineitem stream reserves 7 draws per order (`LINE_COUNT_MAX`) and
//! skips the unused ones, so line `j` of order `o` always takes draw
//! `7 * o + j + 1` of its stream: `seed * 16807^(7o + j + 1) mod m`. The
//! value is a function of its position, not of the previous row.
//!
//! So instead of one serial chain per stream we generate the dense draw
//! sequence (all 7 slots of every order) with [`LANES`] independent chains:
//! chain `l` starts at draw `l + 1` and steps by `16807^LANES`. The lanes
//! don't depend on each other, so the loop vectorises: a 32x32->64 multiply
//! and two shift-and-add folds per value, no division and no compare. The
//! bounded-int mapping is done in integers too (see [`Stream`] for why that
//! is exact). Then each order's first `line_count` slots are copied out with
//! fixed-size writes, and the derived columns are element-wise passes.
//!
//! Output is identical to [`crate::fair::tpch_direct_numeric`], which is
//! identical to tpchgen's `LineItemGenerator` (both tested). Unused slots
//! (3 of 7 on average) are generated and thrown away; it is still cheaper
//! than the serial chain.

use crate::fair::{Cols, SF, cols_to_batch};
use crate::{BATCH, Output, batch_bytes_60};
use std::hint::black_box;
use tpchgen::dates::{MIN_GENERATE_DATE, TOTAL_DATE_RANGE, TPCHDate};
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

pub(crate) const A: u64 = 16807;
pub(crate) const M: u64 = 2_147_483_647;
/// Lines reserved per order (`OrderGenerator::LINE_COUNT_MAX`).
const SLOTS: usize = 7;
/// Independent LCG chains per stream.
pub const LANES: usize = 16;
/// `[1, 2, ..., 8]`: line numbers, written 8 at a time.
const LINE_NUMBERS: [i32; SLOTS + 1] = [1, 2, 3, 4, 5, 6, 7, 8];
/// Orders per block: ~4 lines each, so a block is ~`BATCH` lineitems.
/// A multiple of `LANES`, so every stream consumes whole lane chunks.
pub const ORDERS_PER_BLOCK: usize = BATCH / 4;
/// `orders` rows at SF 10 (`OrderGenerator::SCALE_BASE * SF`).
pub const ORDERS: usize = 15_000_000;

/// `a * b mod (2^31 - 1)` for `a, b` in `[1, m)`, with no division and no
/// compare (so it vectorises even on baseline x86-64, which has no 64-bit
/// SIMD compare).
///
/// `2^31 = 1 (mod m)`, so folding the high bits onto the low ones keeps the
/// residue. The first fold leaves `r <= m + 2^31 - 2`, the second `r < m`
/// or `r = m`, and `r = m` would mean `a * b = 0 (mod m)`, impossible for a
/// prime `m`.
#[inline(always)]
pub(crate) fn mulmod(a: u64, b: u64) -> u64 {
    let p = a * b;
    let r = (p & M) + (p >> 31);
    (r & M) + (r >> 31)
}

pub(crate) fn powmod(mut base: u64, mut exp: u64) -> u64 {
    let mut acc = 1;
    while exp > 0 {
        if exp & 1 == 1 {
            acc = mulmod(acc, base);
        }
        base = mulmod(base, base);
        exp >>= 1;
    }
    acc
}

/// `floor(x * range / (2^31 - 1))`, without a division.
///
/// `p = q0 * 2^31 + r0 = q0 * m + t` with `t = q0 + r0 < 2m` (`p < 2^52`),
/// so the quotient is `q0 + (t >= m)`, and `t >= m` is `(t + 1) >> 31`.
#[inline(always)]
fn scale(x: u32, range: u32) -> u32 {
    let p = x as u64 * range as u64;
    let q0 = p >> 31;
    let t = (p & M) + q0;
    (q0 + ((t + 1) >> 31)) as u32
}

/// One TPC-H bounded-int stream, consumed as a dense sequence of draws.
///
/// tpchgen maps a draw to `lo + ((x as f64 / m as f64) * range) as i32`. For
/// every range used here that equals the integer `lo + floor(x * range / m)`
/// for all `x` in `[1, m)`: checked exhaustively (`exhaustive_scale_matches_f64`,
/// `#[ignore]`d because it takes a few seconds per range in release). The
/// integer form vectorises without a floating-point divide.
pub struct Stream {
    /// Draw `next + l + 1` for lane `l`.
    lanes: [u32; LANES],
    step: u32,
    lo: i32,
    range: u32,
}

impl Stream {
    pub fn new(seed: i64, lo: i32, hi: i32) -> Self {
        let mut lanes = [0; LANES];
        let mut x = seed as u64;
        for lane in &mut lanes {
            x = mulmod(x, A);
            *lane = x as u32;
        }
        let range = hi - lo + 1;
        assert!(range > 0 && range < 1 << 21, "range {range} not covered");
        Self {
            lanes,
            step: powmod(A, LANES as u64) as u32,
            lo,
            range: range as u32,
        }
    }

    /// Fill `out` with the next `out.len()` bounded values (a multiple of
    /// `LANES`), exactly as `RowRandomInt::next_int` computes them.
    pub fn fill(&mut self, out: &mut [i32]) {
        debug_assert_eq!(out.len() % LANES, 0);
        let (lo, range, step) = (self.lo, self.range, self.step as u64);
        let mut lanes = self.lanes;
        for chunk in out.as_chunks_mut::<LANES>().0 {
            for (o, x) in chunk.iter_mut().zip(&mut lanes) {
                *o = lo + scale(*x, range) as i32;
                *x = mulmod(*x as u64, step) as u32;
            }
        }
        self.lanes = lanes;
    }

    /// Skip the next `draws` values: every lane jumps by `16807^draws`.
    pub fn skip(&mut self, draws: u64) {
        let jump = powmod(A, draws);
        for x in &mut self.lanes {
            *x = mulmod(*x as u64, jump) as u32;
        }
    }
}

/// Dense draws for `orders` orders: `SLOTS` per order. With `orders` a
/// multiple of `LANES` this is too, so the lanes stay aligned across blocks.
fn dense_len(orders: usize) -> usize {
    orders * SLOTS
}

const SUPPLIERS: u32 = 100_000; // 10_000 * SF
const _: () = assert!(PARTS as u32 / SUPPLIERS < 25_000);

/// `PartSuppGeneratorIterator::select_part_supplier(p, s, 10.0)` in u32.
#[inline(always)]
fn supp_u32(p: u32, s: u32) -> u32 {
    (p + s * (SUPPLIERS / 4 + (p - 1) / SUPPLIERS)) % SUPPLIERS + 1
}

/// `PartGeneratorIterator::calculate_part_price(p)` in u32.
#[inline(always)]
fn price_u32(p: u32) -> u32 {
    90_000 + (p / 10) % 20_001 + (p % 1000) * 100
}

pub struct TpchLanes {
    order_date: Stream,
    line_count: Stream,
    lines: [Stream; 8],
    index: i64,
    // scratch
    od: Vec<i32>,
    lc: Vec<i32>,
    dense: Vec<i32>,
    od_line: Vec<i32>,
    vals: [Vec<i32>; 8],
}

// Seeds and bounds as in tpchgen's LineItemGenerator / OrderGenerator.
const PARTS: i32 = 2_000_000; // 200_000 * SF
const _: () = assert!(SF == 10.0);

impl Default for TpchLanes {
    fn default() -> Self {
        Self::new()
    }
}

impl TpchLanes {
    pub fn new() -> Self {
        Self {
            order_date: Stream::new(
                1066728069,
                MIN_GENERATE_DATE,
                MIN_GENERATE_DATE + (TOTAL_DATE_RANGE - LineItemGenerator::ITEM_SHIP_DAYS - 1),
            ),
            line_count: Stream::new(1434868289, 1, 7),
            lines: [
                Stream::new(209208115, 1, 50),     // quantity
                Stream::new(554590007, 0, 10),     // discount
                Stream::new(721958466, 0, 8),      // tax
                Stream::new(1808217256, 1, PARTS), // part key
                Stream::new(2095021727, 0, 3),     // supplier number
                Stream::new(1769349045, 1, 121),   // ship days
                Stream::new(904914315, 30, 90),    // commit days
                Stream::new(373135028, 1, 30),     // receipt days
            ],
            index: 0,
            od: Vec::new(),
            lc: Vec::new(),
            dense: Vec::new(),
            od_line: Vec::new(),
            vals: Default::default(),
        }
    }

    /// Start at order index `start` (0-based), as if `start` orders had
    /// already been generated: each per-order stream skips `start` draws and
    /// each line stream `7 * start`. Same output as generating from 0 and
    /// throwing the first `start` orders away.
    pub fn at_order(start: usize) -> Self {
        let mut g = Self::new();
        g.order_date.skip(start as u64);
        g.line_count.skip(start as u64);
        for s in &mut g.lines {
            s.skip((start * SLOTS) as u64);
        }
        g.index = start as i64;
        g
    }

    /// Generate the next `orders` whole orders (a multiple of `LANES`) and
    /// append their lineitems to `c`.
    pub fn fill_orders(&mut self, orders: usize, c: &mut Cols) {
        assert_eq!(orders % LANES, 0);
        self.od.resize(orders, 0);
        self.lc.resize(orders, 0);
        self.order_date.fill(&mut self.od);
        self.line_count.fill(&mut self.lc);
        let total: usize = self.lc.iter().map(|&n| n as usize).sum();

        // Each line stream: dense draws, then keep each order's first
        // `line_count` slots. Copy all 7 slots + 1 and advance by the count
        // (the overlap is overwritten by the next order), so the copy is a
        // fixed-size move with no branch on the count.
        self.dense.resize(dense_len(orders) + 1, 0);
        for (stream, vals) in self.lines.iter_mut().zip(&mut self.vals) {
            stream.fill(&mut self.dense[..dense_len(orders)]);
            vals.clear();
            vals.resize(total + SLOTS + 1, 0);
            let mut pos = 0;
            for (o, &n) in self.lc.iter().enumerate() {
                let s = o * SLOTS;
                vals[pos..pos + SLOTS + 1].copy_from_slice(&self.dense[s..s + SLOTS + 1]);
                pos += n as usize;
            }
            vals.truncate(total);
        }

        // Per-order values repeated per line, with the same fixed-size
        // write-and-advance trick: order key, line number, order date.
        let [keys, part_key, supp_key, quantity, price, discount, tax] = &mut c.i64s;
        let [line_no, ship_date, commit_date, receipt_date] = &mut c.i32s;
        let base = keys.len();
        keys.resize(base + total + SLOTS + 1, 0);
        line_no.resize(base + total + SLOTS + 1, 0);
        self.od_line.clear();
        self.od_line.resize(total + SLOTS + 1, 0);
        let mut pos = 0;
        for (o, (&n, &od)) in self.lc.iter().zip(&self.od).enumerate() {
            let key = OrderGenerator::make_order_key(self.index + o as i64 + 1);
            keys[base + pos..base + pos + SLOTS + 1].fill(key);
            line_no[base + pos..base + pos + SLOTS + 1].copy_from_slice(&LINE_NUMBERS);
            self.od_line[pos..pos + SLOTS + 1].fill(od);
            pos += n as usize;
        }
        keys.truncate(base + total);
        line_no.truncate(base + total);
        self.od_line.truncate(total);

        // Everything else is element-wise over the value columns.
        let [q, d, t, pk, sn, ship, commit, receipt] = &self.vals;
        let epoch = |date: i32| TPCHDate::new(date).to_unix_epoch();
        let od = &self.od_line;
        ship_date.extend(ship.iter().zip(od).map(|(&s, &o)| epoch(s + o)));
        commit_date.extend(commit.iter().zip(od).map(|(&c, &o)| epoch(c + o)));
        receipt_date.extend(
            receipt
                .iter()
                .zip(ship)
                .zip(od)
                .map(|((&r, &s), &o)| epoch(r + s + o)),
        );
        // tpchgen's `select_part_supplier` and `calculate_part_price` in
        // u32: at SF 10 every intermediate fits (part key <= 2M, suppliers
        // 100k), and 32-bit division by a constant vectorises where 64-bit
        // doesn't. Equality with tpchgen's i64 versions is covered by
        // `lanes_match_tpchgen_exactly`.
        part_key.extend(pk.iter().map(|&p| p as i64));
        supp_key.extend(
            pk.iter()
                .zip(sn)
                .map(|(&p, &s)| supp_u32(p as u32, s as u32) as i64),
        );
        quantity.extend(q.iter().map(|&v| v as i64));
        price.extend(
            pk.iter()
                .zip(q)
                .map(|(&p, &q)| (price_u32(p as u32) * q as u32) as i64),
        );
        discount.extend(d.iter().map(|&v| v as i64));
        tax.extend(t.iter().map(|&v| v as i64));
        self.index += orders as i64;
    }
}

/// ~`n` lineitems (whole orders, so slightly more) in ~`BATCH`-row batches.
pub fn tpch_lanes_numeric(n: usize) -> Output {
    let mut g = TpchLanes::new();
    let mut out = Output { rows: 0, bytes: 0 };
    while out.rows < n {
        let mut c = Cols::default();
        g.fill_orders(ORDERS_PER_BLOCK, &mut c);
        let b = cols_to_batch(c);
        out.rows += b.num_rows();
        out.bytes += batch_bytes_60(&b);
        black_box(b);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fair::TpchDirect;

    #[test]
    fn derived_match_tpchgen_for_every_part() {
        use tpchgen::generators::{PartGeneratorIterator, PartSuppGeneratorIterator};
        for p in 1..=PARTS as u32 {
            let price = PartGeneratorIterator::calculate_part_price(p as i64);
            assert_eq!(price_u32(p) as i64, price);
            for s in 0..4 {
                let want = PartSuppGeneratorIterator::select_part_supplier(p as i64, s, SF);
                assert_eq!(supp_u32(p, s as u32) as i64, want);
            }
        }
    }

    #[test]
    fn mulmod_matches_modulo() {
        for (a, b) in [
            (1u64, 1u64),
            (M - 1, M - 1),
            (A, 123_456_789),
            (M - 1, 2),
            (M - 1, 1),
            (2, 1 << 30),
        ] {
            assert_eq!(mulmod(a, b), (a * b) % M);
        }
    }

    /// Every range `TpchLanes` uses, as `hi - lo + 1`.
    const RANGES: [u32; 10] = [2406, 7, 50, 11, 9, 2_000_000, 4, 121, 61, 30];

    fn f64_scale(x: u32, range: u32) -> u32 {
        (((x as i32) as f64 / M as f64) * range as f64) as i32 as u32
    }

    #[test]
    fn scale_matches_f64_sampled() {
        for r in RANGES {
            for x in (1..M as u32).step_by(9973).chain([1, M as u32 - 1]) {
                assert_eq!(scale(x, r), f64_scale(x, r), "x {x} range {r}");
            }
        }
    }

    #[test]
    #[ignore = "exhaustive, ~2^31 values per range; run with --release --ignored"]
    fn exhaustive_scale_matches_f64() {
        for r in RANGES {
            for x in 1..M as u32 {
                assert_eq!(scale(x, r), f64_scale(x, r), "x {x} range {r}");
            }
        }
    }

    /// Straight against tpchgen's own `LineItemGenerator`, over ~1M rows.
    #[test]
    fn lanes_match_line_item_generator() {
        crate::tpchgen_init();
        let mut got = Cols::default();
        let mut g = TpchLanes::new();
        while got.i64s[0].len() < 1 << 20 {
            g.fill_orders(ORDERS_PER_BLOCK, &mut got);
        }
        let n = got.i64s[0].len();
        let rows: Vec<_> = LineItemGenerator::new(SF, 1, 1).iter().take(n).collect();
        let want = crate::tpch_numeric_batch(&rows, &crate::numeric_schema_60());
        assert_eq!(cols_to_batch(got), want);
    }

    /// Starting mid-table gives the same rows as generating from the start.
    #[test]
    fn at_order_matches_skipping() {
        let (skip, take) = (3 * ORDERS_PER_BLOCK + 48, 2 * ORDERS_PER_BLOCK);
        let mut all = Cols::default();
        let mut g = TpchLanes::new();
        g.fill_orders(skip, &mut all);
        let first = all.i64s[0].len();
        g.fill_orders(take, &mut all);
        let mut got = Cols::default();
        TpchLanes::at_order(skip).fill_orders(take, &mut got);
        let want = cols_to_batch(all);
        assert_eq!(
            cols_to_batch(got),
            want.slice(first, want.num_rows() - first)
        );
    }

    #[test]
    fn lanes_match_tpchgen_exactly() {
        let mut got = Cols::default();
        let mut g = TpchLanes::new();
        for _ in 0..12 {
            g.fill_orders(ORDERS_PER_BLOCK, &mut got);
        }
        let n = got.i64s[0].len();
        let mut want = Cols::default();
        TpchDirect::new(false).fill(n, &mut want);
        assert_eq!(cols_to_batch(got), cols_to_batch(want));
    }
}
