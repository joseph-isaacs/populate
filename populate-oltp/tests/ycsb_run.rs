//! The YCSB run phase against YCSB 0.17.0 itself: `ycsb run` with the
//! `RefDB` binding in `ycsb-ref/` (`refdb.run=true`), one client thread
//! (thread 21 on OpenJDK 21) whose `ThreadLocalRandom` seed `RefDB` sets,
//! compared as TSV: one line per binding call, `OP\tkey\tscanlength\t
//! readfields\tfield0...`.

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, RecordBatch};
use populate_oltp::{JavaTlr, RequestDistribution, Ycsb, YcsbRun};

const THREAD: u64 = 21;

fn load(records: u64, seed: u64) -> Ycsb {
    Ycsb::new(records).java_random(JavaTlr {
        seed,
        thread_id: THREAD,
    })
}

fn fnv(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01b3)
    })
}

/// The TSV on `threads` threads, with small odd chunks and batches.
fn tsv(r: &YcsbRun, threads: usize) -> Vec<u8> {
    r.clone()
        .chunk_ops(777)
        .batch_size(100)
        .write_tsv(Vec::new(), threads)
        .expect("tsv")
}

#[test]
fn fixture() {
    // ycsb run -p recordcount=100 -p operationcount=40 -p refdb.seed=77
    // -p requestdistribution=zipfian -p readproportion=0.4
    // -p updateproportion=0.1 -p readmodifywriteproportion=0.2
    // -p scanproportion=0.2 -p insertproportion=0.1 -p fieldlength=10
    // -p fieldcount=4 -p readallfields=false
    let r = YcsbRun::new(load(100, 77).field_length(10).field_count(4), 40)
        .proportions(0.4, 0.1, 0.1, 0.2, 0.2)
        .distribution(RequestDistribution::Zipfian)
        .read_all_fields(false);
    assert_eq!(tsv(&r, 1), include_bytes!("data/ycsb-run-f.tsv"));
}

#[test]
fn whole_runs() {
    use RequestDistribution::*;
    let hot = |data_fraction, op_fraction| Hotspot {
        data_fraction,
        op_fraction,
    };
    // (YCSB properties, our run, FNV-1a-64 and length of YCSB's TSV)
    let cases: Vec<(&str, YcsbRun, u64, usize)> = vec![
        (
            "workload a: recordcount=1000 operationcount=10000 seed 1",
            YcsbRun::workload('a', load(1000, 1), 10_000),
            0xbe11460d130ee803,
            912_476,
        ),
        (
            "workload b: 1000 records, 20000 ops, seed 1",
            YcsbRun::workload('b', load(1000, 1), 20_000),
            0xb9a8ab72ef460700,
            919_446,
        ),
        (
            "workload c, seed 2",
            YcsbRun::workload('c', load(1000, 2), 20_000),
            0x8898c6d54ec1dc4d,
            817_709,
        ),
        (
            "workload d (latest, inserts), seed 3",
            YcsbRun::workload('d', load(1000, 3), 20_000),
            0x8f55fed1fc03d174,
            1_805_535,
        ),
        (
            "workload e (scans 1..100, inserts), 5000 ops, seed 4",
            YcsbRun::workload('e', load(1000, 4), 5000),
            0xa0c119be1155a7cf,
            464_650,
        ),
        (
            "workload f (read-modify-write), seed 5",
            YcsbRun::workload('f', load(1000, 5), 20_000),
            0x39c27bc33cd17df3,
            2_241_470,
        ),
        (
            "defaults: 5000 records, 20000 ops, seed 6",
            YcsbRun::new(load(5000, 6), 20_000),
            0x58257746105ac3e5,
            914_665,
        ),
        (
            "hotspot 0.2/0.8, read 0.5 update 0.5, seed 7",
            YcsbRun::new(load(5000, 7), 20_000)
                .proportions(0.5, 0.5, 0.0, 0.0, 0.0)
                .distribution(hot(0.2, 0.8)),
            0x45181a1c4d1883c4,
            1_831_497,
        ),
        (
            "hotspot 0.01/0.99, insertproportion=0.1, seed 8",
            YcsbRun::new(load(5000, 8), 20_000)
                .proportions(0.95, 0.05, 0.1, 0.0, 0.0)
                .distribution(hot(0.01, 0.99)),
            0xe9dd187e30238c0d,
            2_701_637,
        ),
        (
            "zipfian, all five operations, readallfields=false writeallfields=true, seed 9",
            YcsbRun::new(load(2000, 9), 20_000)
                .proportions(0.3, 0.3, 0.1, 0.2, 0.1)
                .distribution(Zipfian)
                .read_all_fields(false)
                .write_all_fields(true),
            0x5192c6c92a587559,
            11_042_812,
        ),
        (
            "dataintegrity=true, reads, updates, inserts, read-modify-writes",
            YcsbRun::new(load(2000, 10).data_integrity(true), 10_000)
                .proportions(0.4, 0.3, 0.1, 0.0, 0.2),
            0x29167c25471cec5c,
            2_235_911,
        ),
        (
            "scans only, zipfian lengths 5..500, seed 11",
            YcsbRun::new(load(2000, 11), 10_000)
                .proportions(0.0, 0.0, 0.0, 1.0, 0.0)
                .scan_length(5, 500, true),
            0x5831727a42d6705b,
            427_657,
        ),
        (
            "latest, insertproportion=0.5, fieldcount=3 fieldlength=13 prefix c, seed 12",
            YcsbRun::new(
                load(3000, 12)
                    .field_count(3)
                    .field_length(13)
                    .field_prefix("c"),
                10_000,
            )
            .proportions(0.3, 0.2, 0.5, 0.0, 0.0)
            .distribution(Latest),
            0x16c06501461586f3,
            576_007,
        ),
        (
            "insertorder=ordered zeropadding=12, zipfian with inserts, seed 13",
            YcsbRun::new(load(3000, 13).ordered(true).zero_padding(12), 10_000)
                .proportions(0.8, 0.0, 0.2, 0.0, 0.0)
                .distribution(Zipfian),
            0x7082bdd1e9f4b4ce,
            2_392_096,
        ),
        (
            "insertstart=500 insertcount=1000, uniform with inserts, seed 14",
            YcsbRun::new(load(3000, 14).insert_start(500).insert_count(1000), 10_000)
                .proportions(0.95, 0.05, 0.1, 0.0, 0.0)
                .distribution(Uniform),
            0x509b26f9f1308b90,
            1_402_693,
        ),
        (
            "insertstart=500 insertcount=1000, zipfian with inserts, seed 15",
            YcsbRun::new(load(3000, 15).insert_start(500).insert_count(1000), 10_000)
                .proportions(0.95, 0.05, 0.1, 0.0, 0.0)
                .distribution(Zipfian),
            0x64acc67bd62e1466,
            1_370_254,
        ),
        (
            "fieldcount=1 fieldlength=1, seed 16",
            YcsbRun::new(load(1000, 16).field_count(1).field_length(1), 10_000)
                .proportions(0.5, 0.5, 0.0, 0.0, 0.0),
            0x4693983021253173,
            333_860,
        ),
        (
            "fieldcount=20 fieldlength=1000 writeallfields=true, seed 17",
            YcsbRun::new(load(1000, 17).field_count(20).field_length(1000), 3000)
                .proportions(0.5, 0.5, 0.0, 0.0, 0.0)
                .write_all_fields(true),
            0x19b28885b9529255,
            31_335_758,
        ),
        (
            "dataintegrity=true readallfields=false writeallfields=true, scans",
            YcsbRun::new(load(1000, 18).data_integrity(true), 10_000)
                .proportions(0.4, 0.3, 0.0, 0.3, 0.0)
                .read_all_fields(false)
                .write_all_fields(true),
            0x484f4b49fb42861c,
            3_547_215,
        ),
        (
            "insertcount=0, latest with inserts, seed 51",
            YcsbRun::new(load(1000, 51).insert_count(0), 3000)
                .proportions(0.95, 0.05, 0.1, 0.0, 0.0)
                .distribution(Latest),
            0x75f348336b1741c1,
            419_509,
        ),
        (
            "insertcount=0, zipfian with inserts, seed 52",
            YcsbRun::new(load(1000, 52).insert_count(0), 3000)
                .proportions(0.95, 0.05, 0.1, 0.0, 0.0)
                .distribution(Zipfian),
            0x71a2426b4f8a0f58,
            396_433,
        ),
        (
            "zipfian scan lengths 0..9, seed 53",
            YcsbRun::new(load(1000, 53), 3000)
                .proportions(0.0, 0.0, 0.0, 1.0, 0.0)
                .scan_length(0, 9, true),
            0xd54347f9a5e85ab0,
            125_649,
        ),
    ];
    for (props, r, hash, len) in cases {
        for threads in [1, 4] {
            let got = tsv(&r, threads);
            assert_eq!(
                (fnv(&got), got.len()),
                (hash, len),
                "{props}, {threads} threads"
            );
        }
    }
}

/// The Arrow batches, written as the TSV is.
fn batches_as_tsv(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    for b in batches {
        let op = b.column(0).as_string_view();
        let key = b.column(1).as_string_view();
        let scan = b.column(2).as_primitive::<Int32Type>();
        let read = b.column(3).as_string_view();
        let fields: Vec<_> = (4..b.num_columns())
            .map(|c| b.column(c).as_string_view())
            .collect();
        for r in 0..b.num_rows() {
            out.extend_from_slice(op.value(r).as_bytes());
            out.push(b'\t');
            out.extend_from_slice(key.value(r).as_bytes());
            out.push(b'\t');
            if scan.is_valid(r) {
                out.extend_from_slice(scan.value(r).to_string().as_bytes());
            }
            out.push(b'\t');
            if read.is_valid(r) {
                out.extend_from_slice(read.value(r).as_bytes());
            }
            for f in &fields {
                out.push(b'\t');
                if f.is_valid(r) {
                    out.extend_from_slice(f.value(r).as_bytes());
                }
            }
            out.push(b'\n');
        }
    }
    out
}

#[test]
fn arrow_equals_tsv() {
    for r in [
        YcsbRun::workload('f', load(500, 3), 5000).read_all_fields(false),
        YcsbRun::workload('e', load(500, 4), 3000),
        YcsbRun::workload('d', load(500, 5).data_integrity(true), 3000),
    ] {
        let r = r.chunk_ops(333).batch_size(64);
        let mut batches = Vec::new();
        r.for_each_batch(3, |b| batches.push(b));
        assert_eq!(batches_as_tsv(&batches), tsv(&r, 2));
        let s = r.schema();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            &names[..5],
            ["op", "ycsb_key", "scan_length", "read_fields", "field0"]
        );
    }
}

#[test]
fn checks() {
    assert!(YcsbRun::new(load(0, 1), 10).check().is_err());
    assert!(
        YcsbRun::new(load(1, 1), 10)
            .distribution(RequestDistribution::Latest)
            .check()
            .is_err()
    );
    let hot = |d, o| {
        YcsbRun::new(load(100, 1), 10)
            .distribution(RequestDistribution::Hotspot {
                data_fraction: d,
                op_fraction: o,
            })
            .check()
    };
    assert!(hot(0.0, 0.5).is_err(), "YCSB divides by a zero hot set");
    assert!(hot(1.0, 0.5).is_err(), "and by a zero cold set");
    assert!(hot(0.2, 0.8).is_ok());
    assert!(YcsbRun::workload('a', load(100, 1), 10).check().is_ok());
    // a field is chosen for one-field updates; all-field writes need none
    let no_fields = || YcsbRun::new(load(100, 1).field_count(0), 10);
    assert!(no_fields().check().is_err());
    assert!(
        no_fields()
            .proportions(0.0, 1.0, 0.0, 0.0, 0.0)
            .write_all_fields(true)
            .check()
            .is_ok()
    );
    assert!(
        YcsbRun::new(load(100, 1).insert_count(0), 10)
            .check()
            .is_err(),
        "uniform keys from no records"
    );
    assert!(
        YcsbRun::new(load(1 << 32, 1), 10).check().is_err(),
        "insertcount is an int"
    );
}
