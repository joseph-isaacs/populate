//! The data sets against the R datagen scripts (R 4.3.3, data.table
//! 1.14.10), and chunked / multi-threaded output against one sequential
//! run.
//!
//! The expected values are the FNV-1a hash and length of the CSV that
//! `Rscript groupby-datagen.R N K nas sort` / `Rscript join-datagen.R N 0
//! nas sort` (h2oai/db-benchmark `_data/`) wrote. The 1e7 cases are
//! `#[ignore]`d (several seconds each); run them with
//! `cargo test -p populate-h2o --release -- --ignored`.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use populate_h2o::fwrite::write_f64;
use populate_h2o::{GroupBy, Join};
use populate_table::Chunks;
use populate_table::chunks::for_each_batch;
use populate_table::text::TextChunks;

fn fnv(b: &[u8]) -> u64 {
    b.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &x| {
        (h ^ x as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

fn csv(t: &impl TextChunks, threads: usize) -> Vec<u8> {
    populate_table::text::write_text_many(&[t as &dyn TextChunks], vec![Vec::new()], threads)
        .unwrap()
        .pop()
        .unwrap()
}

/// The CSV `fwrite` would write for these batches: the Arrow output's
/// values, formatted independently of the crate's CSV writer.
fn csv_of_batches(t: &impl Chunks) -> Vec<u8> {
    let mut out = Vec::new();
    let names: Vec<String> = t
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    out.extend_from_slice(names.join(",").as_bytes());
    out.push(b'\n');
    let mut batches: Vec<RecordBatch> = vec![];
    for_each_batch(t, 2, |b| batches.push(b));
    for b in &batches {
        for r in 0..b.num_rows() {
            for (c, a) in b.columns().iter().enumerate() {
                if c > 0 {
                    out.push(b',');
                }
                if a.is_null(r) {
                    continue;
                }
                match a.data_type() {
                    DataType::Int32 => out.extend_from_slice(
                        a.as_primitive::<Int32Type>()
                            .value(r)
                            .to_string()
                            .as_bytes(),
                    ),
                    DataType::Float64 => {
                        write_f64(a.as_primitive::<Float64Type>().value(r), &mut out)
                    }
                    DataType::Utf8View => {
                        out.extend_from_slice(a.as_string_view().value(r).as_bytes())
                    }
                    t => panic!("unexpected {t}"),
                }
            }
            out.push(b'\n');
        }
    }
    out
}

fn check(name: &str, got: &[u8], len: usize, hash: u64) {
    assert_eq!(
        (got.len(), fnv(got)),
        (len, hash),
        "{name} differs from R's"
    );
}

/// `(N, K, nas, sort)`, then R's CSV length and hash.
const GROUPBY_SMALL: [(u64, u64, u32, bool, usize, u64); 7] = [
    (2000, 100, 0, false, 95256, 0x2ae01561978a4b2d),
    (2000, 10, 5, false, 91079, 0x9cc7f06857ca3124),
    (2000, 100, 0, true, 95256, 0xf49cccc8cba148db),
    (2000, 2, 25, true, 81451, 0x42f746eb75811af1),
    // K >= 1000: "id1000" sorts before "id101"
    (3000, 1000, 10, true, 138885, 0x236176116175902b),
    (1500, 7, 50, false, 43358, 0x84faca64df760b7e),
    // every value NA
    (1000, 1, 100, true, 9033, 0xf51fa8bb488252ea),
];

#[test]
fn groupby_matches_r() {
    for (n, k, nas, sort, len, hash) in GROUPBY_SMALL {
        let g = GroupBy::with_chunk_rows(n, k, nas, sort, 333, 2);
        let name = g.file_name();
        check(&name, &csv(&g, 3), len, hash);
    }
}

#[test]
fn groupby_chunks_threads_and_arrow_agree() {
    for (n, k, nas, sort) in [
        (100_000, 100, 0, false),
        (50_000, 10, 5, false),
        (50_000, 3, 10, true),
        // two draws per id3 / id6 value (N/K > 2^16)
        (300_000, 2, 5, false),
    ] {
        let one = csv(&GroupBy::with_chunk_rows(n, k, nas, sort, n, 1), 1);
        for (rows, threads) in [(1, 4), (777, 3), (65_536, 2)] {
            if rows == 1 && n > 100_000 {
                continue;
            }
            let g = GroupBy::with_chunk_rows(n, k, nas, sort, rows.max(n / 2000), threads)
                .batch_size(1000);
            assert!(
                csv(&g, threads) == one,
                "{n} {k} {nas} {sort}, {rows} rows/chunk"
            );
        }
        let g = GroupBy::with_chunk_rows(n, k, nas, sort, 10_000, 0).batch_size(3000);
        assert!(csv_of_batches(&g) == one, "arrow {n} {k} {nas} {sort}");
    }
}

#[test]
fn join_chunks_threads_and_arrow_agree() {
    let one: Vec<Vec<u8>> = Join::with_chunk_rows(10_000_000, 5, false, 10_000_000)
        .tables()
        .iter()
        .map(|t| csv(t, 1))
        .collect();
    let j = Join::with_chunk_rows(10_000_000, 5, false, 100_000);
    for (t, want) in j.tables().iter().zip(&one) {
        assert!(&csv(t, 4) == want, "{}", t.file_name());
    }
    for (t, want) in j.tables().iter().zip(&one) {
        let t = t.clone().batch_size(7_000);
        assert!(&csv_of_batches(&t) == want, "arrow {}", t.file_name());
    }
}

/// `(N, K, nas, sort)`, then R's CSV length and hash.
const GROUPBY_1E7: [(u64, u64, u32, bool, usize, u64); 5] = [
    (10_000_000, 100, 0, false, 509181759, 0xdfdded69861ae321),
    (10_000_000, 10, 0, false, 502777058, 0xad5c0fd9506a1b0b),
    (10_000_000, 2, 0, false, 509663806, 0x0e3c76a10dcd0796),
    (10_000_000, 100, 0, true, 509181759, 0x47602d29dd024eff),
    (10_000_000, 100, 5, false, 488129414, 0x6b1a43bcf99724f0),
];

#[test]
#[ignore]
fn groupby_1e7_matches_r() {
    for (n, k, nas, sort, len, hash) in GROUPBY_1E7 {
        let g = GroupBy::new(n, k, nas, sort);
        check(&g.file_name(), &csv(&g, 0), len, hash);
    }
}

/// `(nas, sort)`, then each table's (length, hash) from R.
/// R's CSV: length and FNV-1a hash.
type Csv = (usize, u64);

const JOIN_1E7: [(u32, bool, [Csv; 4]); 3] = [
    (
        0,
        false,
        [
            (461502157, 0xc8c88994395ebe8c),
            (173, 0x9114cf52772c727d),
            (281813, 0xdad0a2debe4c28fe),
            (461490562, 0x1c8e42db3aa52b20),
        ],
    ),
    (
        5,
        false,
        [
            (444112079, 0x5aa8789702e71889),
            (174, 0x62f61feabdac25fc),
            (281554, 0x6dd0b9bf4a5b630b),
            (461488877, 0xd6e0426c476e35f9),
        ],
    ),
    (
        0,
        true,
        [
            (461502157, 0x33144850cc813620),
            (173, 0xb0dc9f23e6a29485),
            (281813, 0xa10b433b25a01046),
            (461490562, 0x608402bcb58cb0fc),
        ],
    ),
];

#[test]
#[ignore]
fn join_1e7_matches_r() {
    for (nas, sort, want) in JOIN_1E7 {
        let j = Join::new(10_000_000, nas, sort);
        for (t, (len, hash)) in j.tables().iter().zip(want) {
            check(&t.file_name(), &csv(t, 0), len, hash);
        }
    }
}
