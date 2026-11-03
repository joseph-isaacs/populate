//! Where does the time go? Each `part` column alone at SF 1 (200k rows),
//! columnar into its Arrow sink, plus array conversion.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use populate_2::arrow::IntoArrowArray;
use populate_2::dist::{map, range};
use populate_2::traits::{DistributionScalar, VecBackend};
use populate_2_tpch::column::{
    MARKET_SEGMENTS, PART_CONTAINERS, PART_TYPES, alpha_num, bounded, col, key_name, phone, pick,
    text,
};
use populate_2_tpch::rng::Stream;
use populate_2_tpch::tables::NoRng;
use populate_2_tpch::tables::{Mfgr, PartName, retail_price};
use populate_2_tpch::values::{Dec, DecCol, POOL_BUFFER, StrCol};
use populate_2_tpch::{Customer, batches};
use tpchgen::text::TextPool;

const N: usize = 200_000;

fn main() {
    TextPool::get_or_init_default();
    std::sync::LazyLock::force(&POOL_BUFFER);
    divan::main();
}

macro_rules! col_bench {
    ($name:ident, $sink:ty, $dist:expr) => {
        #[divan::bench(sample_count = 30)]
        fn $name(b: Bencher) {
            b.counter(ItemsCount::new(N)).bench(|| {
                let mut d = $dist;
                let s = <_ as DistributionScalar<VecBackend>>::collect_col(
                    &mut d,
                    <$sink>::default(),
                    &mut NoRng,
                    black_box(N),
                );
                IntoArrowArray::into_array(s)
            });
        }
    };
}

col_bench!(p_partkey, Vec<i64>, range(1i64, 1));
col_bench!(p_name, StrCol, col(PartName, 0));
col_bench!(p_mfgr, StrCol, col(Mfgr { brand: false }, 0));
col_bench!(p_brand, StrCol, col(Mfgr { brand: true }, 0));
col_bench!(p_type, StrCol, col(pick(1841581359, &PART_TYPES), 0));
col_bench!(p_size, Vec<i32>, col(bounded::<i32>(1193163244, 1, 50), 0));
col_bench!(
    p_container,
    StrCol,
    col(pick(727633698, &PART_CONTAINERS), 0)
);
col_bench!(
    p_retailprice,
    DecCol,
    map(range(1i64, 1), |k: i64| Dec(retail_price(k)))
);
col_bench!(p_comment, StrCol, col(text(804159733, 14), 0));

/// p_comment, but copying the bytes like tpchgen-arrow does
#[divan::bench(sample_count = 30)]
fn p_comment_copied(b: Bencher) {
    let t = text(804159733, 14);
    let pool = POOL_BUFFER.as_slice();
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut st = Stream::ungrouped(t.seed, 2, 0);
        let mut s = StrCol::default();
        for _ in 0..black_box(N) {
            let (o, l) = t.draw(st.next_item());
            s.push_str(unsafe {
                std::str::from_utf8_unchecked(&pool[o as usize..(o + l) as usize])
            });
        }
        IntoArrowArray::into_array(s)
    });
}

// ---- diagnostics: RNG cost vs memory cost ----

/// p_comment draws only: offsets/lengths into a Vec, pool never touched
#[divan::bench(sample_count = 30)]
fn diag_comment_rng_only(b: Bencher) {
    let t = text(804159733, 14);
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut st = Stream::ungrouped(t.seed, 2, 0);
        let mut v = Vec::with_capacity(N);
        for _ in 0..black_box(N) {
            v.push(t.draw(st.next_item()));
        }
        v
    });
}

/// one Park-Miller draw + float scaling per row (the cost of one next_int)
#[divan::bench(sample_count = 30)]
fn diag_next_int_x5(b: Bencher) {
    b.counter(ItemsCount::new(N)).bench(|| {
        let mut st = Stream::ungrouped(709314158, 92, 0);
        let mut acc = 0i64;
        for _ in 0..black_box(N) {
            let r = st.next_item();
            for i in 0..5 {
                acc += r.next_int(i, 91) as i64;
            }
        }
        acc
    });
}

// ---- customer columns (150k rows at SF 1; measured at N = 200k) ----
col_bench!(c_name, StrCol, col(key_name("Customer#", 9), 0));
col_bench!(c_address, StrCol, col(alpha_num(881155353, 25), 0));
col_bench!(
    c_nationkey,
    Vec<i64>,
    col(bounded::<i64>(1489529863, 0, 24), 0)
);
col_bench!(c_phone, StrCol, col(phone(1489529863, 1521138112), 0));
col_bench!(
    c_acctbal,
    DecCol,
    col(bounded::<Dec>(298370230, -99999, 999999), 0)
);
col_bench!(
    c_mktsegment,
    StrCol,
    col(pick(1140279430, &MARKET_SEGMENTS), 0)
);
col_bench!(c_comment, StrCol, col(text(1335826707, 73), 0));

// ---- batch-size effect on a whole table ----
#[divan::bench(sample_count = 15, args = [8_000, 32_000, 150_000])]
fn customer_table_batch(b: Bencher, batch: usize) {
    b.counter(ItemsCount::new(150_000usize))
        .bench(|| batches::<Customer>(black_box(1.0), batch).collect::<Vec<_>>());
}
