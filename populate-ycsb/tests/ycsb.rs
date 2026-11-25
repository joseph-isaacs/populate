//! YCSB against YCSB 0.17.0 itself: its `ycsb load` output, written by the
//! `RefDB` binding in `ycsb-ref/` with one client thread, compared as TSV
//! (`key\tfield0\t...`). Random values use the client thread's
//! `ThreadLocalRandom` seed, set by `RefDB`; YCSB's client thread was
//! thread 21 on OpenJDK 21.

use populate_ycsb::{JavaTlr, Ycsb};

const THREAD: u64 = 21;

fn tlr(seed: u64) -> JavaTlr {
    JavaTlr {
        seed,
        thread_id: THREAD,
    }
}

fn fnv(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01b3)
    })
}

/// The TSV on `threads` threads, with small odd chunks and batches.
fn tsv(y: &Ycsb, threads: usize) -> Vec<u8> {
    y.clone()
        .chunk_rows(777)
        .batch_size(100)
        .write_tsv(Vec::new(), threads)
        .expect("tsv")
}

#[test]
fn fixtures() {
    // ycsb load -p recordcount=50 -p refdb.seed=12345 / -p dataintegrity=true
    let random = Ycsb::new(50).java_random(tlr(12345));
    assert_eq!(
        tsv(&random, 1),
        include_bytes!("data/ycsb-random-seed12345.tsv")
    );
    let di = Ycsb::new(50).data_integrity(true);
    assert_eq!(tsv(&di, 3), include_bytes!("data/ycsb-dataintegrity.tsv"));
}

#[test]
fn whole_loads() {
    // (YCSB properties, our load, FNV-1a-64 and length of YCSB's TSV)
    let cases: Vec<(&str, Ycsb, u64, usize)> = vec![
        (
            "recordcount=100000 refdb.seed=12345",
            Ycsb::new(100_000).java_random(tlr(12345)),
            0x1ab25657cd170c5a,
            103_388_007,
        ),
        (
            "recordcount=20000 dataintegrity=true",
            Ycsb::new(20_000).data_integrity(true),
            0x55d22e6001b90478,
            20_677_596,
        ),
        (
            "recordcount=3000 fieldcount=20 fieldlength=37 fieldnameprefix=c refdb.seed=99",
            Ycsb::new(3000)
                .field_count(20)
                .field_length(37)
                .field_prefix("c")
                .java_random(tlr(99)),
            0x68c68191ab964228,
            2_351_635,
        ),
        (
            "recordcount=5000 insertstart=1234 insertcount=777 insertorder=ordered \
             zeropadding=12 refdb.seed=99",
            Ycsb::new(5000)
                .insert_start(1234)
                .insert_count(777)
                .ordered(true)
                .zero_padding(12)
                .java_random(tlr(99)),
            0x29ddb49c8a8f5659,
            797_979,
        ),
        (
            "recordcount=5000 insertstart=1234 insertcount=777 dataintegrity=true \
             fieldlength=16 fieldcount=3",
            Ycsb::new(5000)
                .insert_start(1234)
                .insert_count(777)
                .data_integrity(true)
                .field_length(16)
                .field_count(3),
            0x585eb35309c2f44c,
            58_180,
        ),
        (
            "recordcount=300 insertstart=100 refdb.seed=4 (300 records from 100)",
            Ycsb::new(300).insert_start(100).java_random(tlr(4)),
            0x6b4152255cd0b194,
            310_160,
        ),
    ];
    for (props, y, hash, len) in cases {
        for threads in [1, 4] {
            let got = tsv(&y, threads);
            assert_eq!(
                (fnv(&got), got.len()),
                (hash, len),
                "{props}, {threads} threads"
            );
        }
    }
}

#[test]
fn arrow_equals_tsv() {
    let y = Ycsb::new(3000).java_random(tlr(5));
    let mut text = Vec::new();
    y.for_each_batch(4, |b| {
        let cols: Vec<_> = b
            .columns()
            .iter()
            .map(|c| arrow_array::cast::AsArray::as_string_view(c).clone())
            .collect();
        for r in 0..b.num_rows() {
            for (k, c) in cols.iter().enumerate() {
                if k > 0 {
                    text.push(b'\t');
                }
                text.extend_from_slice(c.value(r).as_bytes());
            }
            text.push(b'\n');
        }
    });
    assert_eq!(text, tsv(&y, 2));
    assert_eq!(y.schema().field(0).name(), "ycsb_key");
    assert_eq!(y.schema().field(10).name(), "field9");
}

#[test]
fn checks_like_ycsb() {
    assert!(
        Ycsb::new(10)
            .insert_start(5)
            .insert_count(6)
            .check()
            .is_err()
    );
    assert!(
        Ycsb::new(10)
            .insert_start(5)
            .insert_count(5)
            .check()
            .is_ok()
    );
    assert_eq!(Ycsb::new(10).insert_start(5).rows(), 10);
}

#[test]
#[should_panic(expected = "printable ASCII")]
fn non_ascii_prefix() {
    let _ = Ycsb::new(1).field_prefix("fieldé");
}
