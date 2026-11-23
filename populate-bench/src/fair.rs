//! Like-for-like baselines from an adversarial review of the comparison.
//!
//! - [`tpch_direct_numeric`]: tpchgen's own RNG streams and row logic for the
//!   11 numeric columns, written straight into column `Vec`s. Output is
//!   identical to `tpch_numeric_arrow` (tested), without the string RNG
//!   streams and `LineItem` row structs that path pays for and discards.
//! - [`tpch_direct_with_string_draws`]: the same plus the 4 string streams
//!   (values discarded), to isolate their cost.
//! - [`p2_uniform_tpch_rng`]: the benchmarked populate workload with every
//!   draw taken from tpchgen's RNG (`RandomBoundedInt`), to isolate the RNG.
//! - [`tpch_list_direct`] / [`p2_list_fair`]: the list workload with only the
//!   3 tpchgen streams it needs, and the populate side computing the same
//!   `price(partkey) * quantity` derivation.

use crate::lanes::{A, LANES, M, mulmod, powmod};
use crate::{BATCH, LINES_PER_ORDER, Output, batch_bytes, batch_bytes_60, numeric_schema_60, rng};
use populate::arrow::{ArrowBackend, DistributionArrow};
use populate::dist::{Uniform, list, map, range, uniform};
use populate::hlist::{HCons, HNil};
use populate::traits::{DefaultSink, Distribution, SampleIter};
use rand::Rng;
use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::Arc;
use tpchgen::dates::TPCHDate;
use tpchgen::distribution::Distributions;
use tpchgen::generators::{
    LineItemGenerator, OrderGenerator, PartGeneratorIterator, PartSuppGeneratorIterator,
};
use tpchgen::random::{RandomBoundedInt, RandomBoundedLong, RandomString, RandomText};
use tpchgen::text::TextPool;
use tpchgen_arrow::arrow::array::{
    ArrayRef as ArrayRef60, Int32Array as Int32Array60, Int64Array as Int64Array60,
    ListArray as ListArray60, RecordBatch as RecordBatch60,
};
use tpchgen_arrow::arrow::buffer::OffsetBuffer as OffsetBuffer60;
use tpchgen_arrow::arrow::datatypes::{
    DataType as DataType60, Field as Field60, Schema as Schema60,
};

/// Same scale factor as `tpch_lineitems()` in lib.rs.
pub const SF: f64 = 10.0;
const PARTS: i64 = 2_000_000;
/// 1992-01-01 in unix days.
const D1992: i32 = 8035;

fn chunks(n: usize) -> impl Iterator<Item = usize> {
    (0..n.div_ceil(BATCH)).map(move |i| BATCH.min(n - i * BATCH))
}

fn price(part_key: i64) -> i64 {
    PartGeneratorIterator::calculate_part_price(part_key)
}

fn supp(part_key: i64, n: i64) -> i64 {
    PartSuppGeneratorIterator::select_part_supplier(part_key, n, SF)
}

// ============================================================================
// tpchgen's row logic, straight to columns
// ============================================================================

pub struct TpchDirect {
    order_date_random: RandomBoundedInt,
    line_count_random: RandomBoundedInt,
    quantity_random: RandomBoundedInt,
    discount_random: RandomBoundedInt,
    tax_random: RandomBoundedInt,
    part_key_random: RandomBoundedLong,
    supplier_random: RandomBoundedInt,
    ship_random: RandomBoundedInt,
    commit_random: RandomBoundedInt,
    receipt_random: RandomBoundedInt,
    strings: Option<(
        RandomString<'static>,
        RandomString<'static>,
        RandomString<'static>,
        RandomText<'static>,
    )>,
    index: i64,
    order_date: i32,
    line_count: i32,
    line_number: i32,
}

#[derive(Default)]
pub struct Cols {
    pub i64s: [Vec<i64>; 7],
    pub i32s: [Vec<i32>; 4],
}

impl TpchDirect {
    pub fn new(strings: bool) -> Self {
        let per = OrderGenerator::LINE_COUNT_MAX;
        let mut order_date_random = OrderGenerator::create_order_date_random();
        let mut line_count_random = OrderGenerator::create_line_count_random();
        let order_date = order_date_random.next_value();
        let line_count = line_count_random.next_value() - 1;
        let d = Distributions::static_default();
        Self {
            order_date_random,
            line_count_random,
            quantity_random: LineItemGenerator::create_quantity_random(),
            discount_random: LineItemGenerator::create_discount_random(),
            tax_random: LineItemGenerator::create_tax_random(),
            part_key_random: LineItemGenerator::create_part_key_random(SF),
            supplier_random: RandomBoundedInt::new_with_seeds_per_row(2095021727, 0, 3, per),
            ship_random: LineItemGenerator::create_ship_date_random(),
            commit_random: RandomBoundedInt::new_with_seeds_per_row(904914315, 30, 90, per),
            receipt_random: RandomBoundedInt::new_with_seeds_per_row(373135028, 1, 30, per),
            strings: strings.then(|| {
                (
                    RandomString::new_with_expected_row_count(717419739, d.return_flags(), per),
                    RandomString::new_with_expected_row_count(
                        1371272478,
                        d.ship_instructions(),
                        per,
                    ),
                    RandomString::new_with_expected_row_count(675466456, d.ship_modes(), per),
                    RandomText::new_with_expected_row_count(
                        1095462486,
                        TextPool::get_or_init_default(),
                        27.0,
                        per,
                    ),
                )
            }),
            index: 0,
            order_date,
            line_count,
            line_number: 0,
        }
    }

    pub fn fill(&mut self, len: usize, c: &mut Cols) {
        for v in &mut c.i64s {
            v.reserve(len);
        }
        for v in &mut c.i32s {
            v.reserve(len);
        }
        for _ in 0..len {
            let quantity = self.quantity_random.next_value() as i64;
            let discount = self.discount_random.next_value() as i64;
            let tax = self.tax_random.next_value() as i64;
            let part_key = self.part_key_random.next_value();
            let supplier_number = self.supplier_random.next_value() as i64;
            let ship = self.ship_random.next_value() + self.order_date;
            let commit = self.commit_random.next_value() + self.order_date;
            let receipt = self.receipt_random.next_value() + ship;
            if let Some((flag, instr, mode, comment)) = &mut self.strings {
                let f = if TPCHDate::is_in_past(receipt) {
                    flag.next_value()
                } else {
                    "N"
                };
                let status = if TPCHDate::is_in_past(ship) { "F" } else { "O" };
                black_box((
                    f,
                    status,
                    instr.next_value(),
                    mode.next_value(),
                    comment.next_value(),
                ));
            }
            c.i64s[0].push(OrderGenerator::make_order_key(self.index + 1));
            c.i64s[1].push(part_key);
            c.i64s[2].push(supp(part_key, supplier_number));
            c.i32s[0].push(self.line_number + 1);
            c.i64s[3].push(quantity);
            c.i64s[4].push(price(part_key) * quantity);
            c.i64s[5].push(discount);
            c.i64s[6].push(tax);
            c.i32s[1].push(TPCHDate::new(ship).to_unix_epoch());
            c.i32s[2].push(TPCHDate::new(commit).to_unix_epoch());
            c.i32s[3].push(TPCHDate::new(receipt).to_unix_epoch());

            self.line_number += 1;
            if self.line_number > self.line_count {
                self.order_date_random.row_finished();
                self.line_count_random.row_finished();
                self.quantity_random.row_finished();
                self.discount_random.row_finished();
                self.tax_random.row_finished();
                self.part_key_random.row_finished();
                self.supplier_random.row_finished();
                self.ship_random.row_finished();
                self.commit_random.row_finished();
                self.receipt_random.row_finished();
                if let Some((flag, instr, mode, comment)) = &mut self.strings {
                    flag.row_finished();
                    instr.row_finished();
                    mode.row_finished();
                    comment.row_finished();
                }
                self.index += 1;
                self.line_count = self.line_count_random.next_value() - 1;
                self.order_date = self.order_date_random.next_value();
                self.line_number = 0;
            }
        }
    }
}

pub fn cols_to_batch(c: Cols) -> RecordBatch60 {
    let [k, pk, sk, q, ep, d, t] = c.i64s;
    let [ln, sd, cd, rd] = c.i32s;
    let a = |v: Vec<i64>| Arc::new(Int64Array60::from(v)) as ArrayRef60;
    let b = |v: Vec<i32>| Arc::new(Int32Array60::from(v)) as ArrayRef60;
    RecordBatch60::try_new(
        numeric_schema_60(),
        vec![
            a(k),
            a(pk),
            a(sk),
            b(ln),
            a(q),
            a(ep),
            a(d),
            a(t),
            b(sd),
            b(cd),
            b(rd),
        ],
    )
    .expect("batch")
}

fn tpch_direct(n: usize, strings: bool) -> Output {
    let mut g = TpchDirect::new(strings);
    let mut out = Output { rows: 0, bytes: 0 };
    for len in chunks(n) {
        let mut c = Cols::default();
        g.fill(len, &mut c);
        let b = cols_to_batch(c);
        out.rows += b.num_rows();
        out.bytes += batch_bytes_60(&b);
        black_box(b);
    }
    out
}

pub fn tpch_direct_numeric(n: usize) -> Output {
    tpch_direct(n, false)
}

pub fn tpch_direct_with_string_draws(n: usize) -> Output {
    tpch_direct(n, true)
}

// ============================================================================
// Lists
// ============================================================================

/// populate list with tpchgen's derivation: price(partkey) * quantity
/// (2 draws per value instead of 1).
pub fn p2_list_fair(n: usize) -> Output {
    let elem: Uniform<i64> = uniform(1i64, PARTS);
    let mut d = (
        map(range(1i64, 1), |i: i64| OrderGenerator::make_order_key(i)),
        list(
            uniform(1usize, 7),
            map((elem, uniform(1i64, 50)), |(p, q): (i64, i64)| price(p) * q),
        ),
    );
    let b = DistributionArrow::<ArrowBackend>::collect_record_batch(
        &mut d,
        &mut rng(),
        n / LINES_PER_ORDER,
    )
    .expect("batch");
    black_box(Output {
        rows: b.num_rows(),
        bytes: batch_bytes(&b),
    })
}

/// tpchgen's RNG streams for just what the list needs: line count, quantity,
/// part key (spec-exact values), written straight into offsets + values.
pub fn tpch_list_direct_batch(n: usize) -> RecordBatch60 {
    let mut line_count_random = OrderGenerator::create_line_count_random();
    let mut quantity_random = LineItemGenerator::create_quantity_random();
    let mut part_key_random = LineItemGenerator::create_part_key_random(SF);
    let mut keys = Vec::with_capacity(n / LINES_PER_ORDER + 8);
    let mut offsets = Vec::with_capacity(n / LINES_PER_ORDER + 9);
    let mut prices: Vec<i64> = Vec::with_capacity(n + 8);
    offsets.push(0i32);
    let mut index = 0i64;
    while prices.len() < n {
        let lines = line_count_random.next_value();
        for _ in 0..lines {
            let q = quantity_random.next_value() as i64;
            let p = part_key_random.next_value();
            prices.push(price(p) * q);
        }
        line_count_random.row_finished();
        quantity_random.row_finished();
        part_key_random.row_finished();
        index += 1;
        keys.push(OrderGenerator::make_order_key(index));
        offsets.push(prices.len() as i32);
    }
    let item = Arc::new(Field60::new_list_field(DataType60::Int64, false));
    let l = ListArray60::new(
        item.clone(),
        OffsetBuffer60::new(offsets.into()),
        Arc::new(Int64Array60::from(prices)),
        None,
    );
    let schema = Arc::new(Schema60::new(vec![
        Field60::new("orderkey", DataType60::Int64, false),
        Field60::new("line_prices", DataType60::List(item), false),
    ]));
    RecordBatch60::try_new(
        schema,
        vec![Arc::new(Int64Array60::from(keys)), Arc::new(l)],
    )
    .expect("batch")
}

pub fn tpch_list_direct(n: usize) -> Output {
    let b = tpch_list_direct_batch(n);
    black_box(Output {
        rows: b.num_rows(),
        bytes: batch_bytes_60(&b),
    })
}

// ============================================================================
// RNG control
// ============================================================================

/// populate leaf drawing from tpchgen's RNG (`RandomBoundedInt`: LCG mod
/// 2^31-1 scaled through f64) instead of SmallRng + rand `Uniform`.
pub struct Lcg<T> {
    g: RandomBoundedInt,
    _t: PhantomData<T>,
}

pub fn lcg<T>(seed: i64, lo: i32, hi: i32) -> Lcg<T> {
    Lcg {
        g: RandomBoundedInt::new(seed, lo, hi),
        _t: PhantomData,
    }
}

pub struct LcgIter<T>(RandomBoundedInt, PhantomData<T>);

impl<T: From<i32>> SampleIter for LcgIter<T> {
    type Item = T;
    fn next(&mut self, _rng: &mut impl Rng) -> T {
        T::from(self.0.next_value())
    }
}

impl<T> Distribution<ArrowBackend> for Lcg<T>
where
    T: From<i32> + DefaultSink<ArrowBackend, Sink = HCons<Vec<T>, HNil>>,
{
    type Item = T;
    type RowIter = LcgIter<T>;

    fn row_iter(&mut self) -> LcgIter<T> {
        LcgIter(self.g, PhantomData)
    }

    fn collect_into(
        &mut self,
        mut sink: HCons<Vec<T>, HNil>,
        _rng: &mut impl Rng,
        n: usize,
    ) -> HCons<Vec<T>, HNil> {
        let g = &mut self.g;
        sink.0.extend((0..n).map(|_| T::from(g.next_value())));
        sink
    }
}

/// [`Lcg`] with the stream split into [`LANES`] interleaved chains (lane
/// `l` holds draw `i + l`, each steps by `16807^LANES`), so a column is
/// `LANES` independent multiply chains instead of one serial one and the
/// loop vectorises. The values are [`Lcg`]'s, in the same order (tested);
/// the bounded mapping is tpchgen's own f64 formula, so it is exact for any
/// range.
pub struct LaneLcg<T> {
    /// The next `LANES` draws.
    lanes: [u64; LANES],
    step: u64,
    lo: i32,
    range: f64,
    /// Values made but not yet handed out (a batch that is not a multiple
    /// of `LANES` leaves some).
    spare: Vec<i32>,
    _t: PhantomData<T>,
}

pub fn lane_lcg<T>(seed: i64, lo: i32, hi: i32) -> LaneLcg<T> {
    let mut lanes = [0; LANES];
    let mut x = seed as u64;
    for l in &mut lanes {
        x = mulmod(x, A);
        *l = x;
    }
    LaneLcg {
        lanes,
        step: powmod(A, LANES as u64),
        lo,
        range: (hi - lo + 1) as f64,
        spare: Vec::new(),
        _t: PhantomData,
    }
}

impl<T: From<i32>> LaneLcg<T> {
    #[inline(always)]
    fn chunk(&mut self) -> [i32; LANES] {
        let (lo, range, step) = (self.lo, self.range, self.step);
        let mut out = [0; LANES];
        for (o, x) in out.iter_mut().zip(&mut self.lanes) {
            // `RowRandomInt::next_int`
            *o = lo + ((*x as i32 as f64 / M as f64) * range) as i32;
            *x = mulmod(*x, step);
        }
        out
    }

    fn fill(&mut self, out: &mut Vec<T>, n: usize) {
        let take = n.min(self.spare.len());
        out.extend(self.spare.drain(..take).map(T::from));
        let n = n - take;
        for _ in 0..n / LANES {
            let c = self.chunk();
            out.extend(c.map(T::from));
        }
        if !n.is_multiple_of(LANES) {
            let c = self.chunk();
            out.extend(c[..n % LANES].iter().map(|&v| T::from(v)));
            self.spare.extend_from_slice(&c[n % LANES..]);
        }
    }
}

impl<T> Distribution<ArrowBackend> for LaneLcg<T>
where
    T: From<i32> + DefaultSink<ArrowBackend, Sink = HCons<Vec<T>, HNil>>,
{
    type Item = T;
    type RowIter = LcgIter<T>;

    fn row_iter(&mut self) -> LcgIter<T> {
        unimplemented!("LaneLcg is columnar only")
    }

    fn collect_into(
        &mut self,
        mut sink: HCons<Vec<T>, HNil>,
        _rng: &mut impl Rng,
        n: usize,
    ) -> HCons<Vec<T>, HNil> {
        sink.0.reserve(n);
        self.fill(&mut sink.0, n);
        sink
    }
}

/// [`p2_uniform_tpch_rng`] with [`LaneLcg`] leaves: the same values.
pub fn p2_uniform_tpch_rng_lanes(n: usize) -> Output {
    let mut d = (
        (
            range(1i64, 1),
            lane_lcg::<i64>(1808217256, 1, 200_000),
            lane_lcg::<i64>(2095021727, 1, 10_000),
            lane_lcg::<i32>(1434868289, 1, 7),
        ),
        (
            lane_lcg::<i64>(209208115, 1, 50),
            lane_lcg::<i64>(1, 90_100, 10_494_950),
            lane_lcg::<i64>(554590007, 0, 10),
            lane_lcg::<i64>(721958466, 0, 8),
        ),
        (
            lane_lcg::<i32>(1769349045, D1992 + 1, D1992 + 2405),
            lane_lcg::<i32>(904914315, D1992 + 30, D1992 + 2495),
            lane_lcg::<i32>(373135028, D1992 + 2, D1992 + 2435),
        ),
    );
    let mut r = rng();
    let mut out = Output { rows: 0, bytes: 0 };
    let mut schema = None;
    for len in chunks(n) {
        let b = match &schema {
            None => d.collect_record_batch(&mut r, len),
            Some(s) => d.collect_record_batch_with(s, &mut r, len),
        };
        let b = b.expect("batch");
        schema.get_or_insert_with(|| b.schema());
        out.rows += b.num_rows();
        out.bytes += batch_bytes(&b);
        black_box(b);
    }
    out
}

/// The benchmarked populate workload (10 independent uniform columns + key),
/// but every draw comes from tpchgen's RNG. Timing control only: the leaves
/// keep their RNG state across batches, but the orderkey `range` restarts.
pub fn p2_uniform_tpch_rng(n: usize) -> Output {
    let mut d = (
        (
            range(1i64, 1),
            lcg::<i64>(1808217256, 1, 200_000),
            lcg::<i64>(2095021727, 1, 10_000),
            lcg::<i32>(1434868289, 1, 7),
        ),
        (
            lcg::<i64>(209208115, 1, 50),
            lcg::<i64>(1, 90_100, 10_494_950),
            lcg::<i64>(554590007, 0, 10),
            lcg::<i64>(721958466, 0, 8),
        ),
        (
            lcg::<i32>(1769349045, D1992 + 1, D1992 + 2405),
            lcg::<i32>(904914315, D1992 + 30, D1992 + 2495),
            lcg::<i32>(373135028, D1992 + 2, D1992 + 2435),
        ),
    );
    let mut r = rng();
    let mut out = Output { rows: 0, bytes: 0 };
    let mut schema = None;
    for len in chunks(n) {
        let b = match &schema {
            None => d.collect_record_batch(&mut r, len),
            Some(s) => d.collect_record_batch_with(s, &mut r, len),
        };
        let b = b.expect("batch");
        schema.get_or_insert_with(|| b.schema());
        out.rows += b.num_rows();
        out.bytes += batch_bytes(&b);
        black_box(b);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tpch_list_batch, tpch_numeric_batch, tpchgen_init};

    #[test]
    fn direct_matches_tpchgen_exactly() {
        tpchgen_init();
        let n = 100_000;
        let rows: Vec<_> = LineItemGenerator::new(SF, 1, 1).iter().take(n).collect();
        let want = tpch_numeric_batch(&rows, &numeric_schema_60());
        for strings in [false, true] {
            let mut c = Cols::default();
            TpchDirect::new(strings).fill(n, &mut c);
            assert_eq!(cols_to_batch(c), want);
        }
    }

    #[test]
    fn list_direct_matches_tpchgen() {
        tpchgen_init();
        let n = 100_000;
        assert_eq!(tpch_list_direct_batch(n), tpch_list_batch(n));
    }

    #[test]
    fn lane_lcg_matches_lcg() {
        let mut r = rng();
        for (lo, hi) in [(1, 7), (90_100, 10_494_950), (D1992 + 1, D1992 + 2405)] {
            let (mut a, mut b) = (
                lcg::<i64>(1808217256, lo, hi),
                lane_lcg::<i64>(1808217256, lo, hi),
            );
            // uneven batches, so lanes straddle them
            for n in [1, 15, 16, 17, 8192, 5, 1000, 33] {
                let want = a.collect_into(HCons(Vec::new(), HNil), &mut r, n);
                let got = b.collect_into(HCons(Vec::new(), HNil), &mut r, n);
                assert_eq!(got, want, "{lo}..={hi}, batch {n}");
            }
        }
    }

    #[test]
    fn fair_shapes() {
        assert_eq!(p2_list_fair(80_000).rows, 20_000);
        let o = p2_uniform_tpch_rng(20_000);
        assert_eq!(o.rows, 20_000);
        assert!(o.bytes >= 20_000 * 72);
    }
}
