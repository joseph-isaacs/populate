//! divan view of the populate vs tpchgen workloads (see `src/lib.rs`).
//!
//! `cargo bench -p populate-bench`. Each sample generates `N` rows from
//! scratch; throughput is reported in rows/s and output bytes/s, and the
//! `AllocProfiler` adds allocation counts and bytes per run. For peak heap,
//! run the report binary: `cargo run -p populate-bench --release`.

use divan::counter::{BytesCount, ItemsCount};
use divan::{AllocProfiler, Bencher, black_box};
use populate_bench::fair::{
    p2_list_fair, p2_uniform_tpch_rng, tpch_direct_numeric, tpch_direct_with_string_draws,
    tpch_list_direct,
};
use populate_bench::lanes::tpch_lanes_numeric;
use populate_bench::{
    Output, p2_list_arrow, p2_list_parquet, p2_numeric_arrow, p2_numeric_arrow_batched,
    p2_numeric_parquet, p2_numeric_parquet_arrow_writer, raw_columnar_arrow_batched,
    raw_row_builders_arrow_batched, tpch_full_arrow, tpch_full_parquet, tpch_list_arrow,
    tpch_list_parquet, tpch_numeric_arrow, tpch_numeric_parquet, tpch_rows, tpchgen_init,
};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    // tpchgen's text pool is a process-wide one-time cost; keep it out of
    // every sample.
    tpchgen_init();
    divan::main();
}

/// ~1M rows (or list values) per sample.
const N: usize = 1 << 20;

fn run(b: Bencher, f: fn(usize) -> Output) {
    let o = f(N);
    b.counter(ItemsCount::new(o.rows))
        .counter(BytesCount::new(o.bytes))
        .bench(|| f(black_box(N)));
}

macro_rules! benches {
    ($($group:ident { $($name:ident => $f:path),* $(,)? })*) => {
        $(mod $group {
            use super::*;
            $(
                #[divan::bench(sample_count = 10, sample_size = 1)]
                fn $name(b: Bencher) {
                    run(b, $f);
                }
            )*
        })*
    };
}

benches! {
    numeric_arrow {
        populate2 => p2_numeric_arrow,
        populate2_batched => p2_numeric_arrow_batched,
        hand_columnar => raw_columnar_arrow_batched,
        hand_row_builders => raw_row_builders_arrow_batched,
        tpchgen => tpch_numeric_arrow,
        tpchgen_rows_only => tpch_rows,
        tpchgen_direct_with_string_draws => tpch_direct_with_string_draws,
        tpchgen_direct_numeric => tpch_direct_numeric,
        populate2_tpchgen_rng => p2_uniform_tpch_rng,
        tpchgen_lanes_numeric => tpch_lanes_numeric,
    }
    numeric_parquet {
        populate2_zero_copy => p2_numeric_parquet,
        populate2_arrow_writer => p2_numeric_parquet_arrow_writer,
        tpchgen => tpch_numeric_parquet,
    }
    list_arrow {
        populate2 => p2_list_arrow,
        tpchgen_grouped => tpch_list_arrow,
        populate2_derived_price => p2_list_fair,
        tpchgen_direct => tpch_list_direct,
    }
    list_parquet {
        populate2_zero_copy => p2_list_parquet,
        tpchgen_grouped => tpch_list_parquet,
    }
    tpchgen_full_lineitem {
        arrow => tpch_full_arrow,
        parquet => tpch_full_parquet,
    }
}
