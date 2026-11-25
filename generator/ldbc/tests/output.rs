//! Output tests. They need the datagen's dictionaries
//! (`scripts/fetch-ldbc-resources.sh`, or `LDBC_DATAGEN_RESOURCES`) and are
//! skipped without them.
//!
//! `LDBC_REFERENCE_SF0_1=DIR` also compares SF 0.1 with a Spark datagen
//! `--mode raw --format csv --scale-factor 0.1` output directory, byte for
//! byte.

use populate_ldbc::rows::{DYNAMIC, Rows, STATIC};
use populate_ldbc::{Ldbc, csv, default_resources, persons_for_scale_factor};
use populate_table::text::write_text_many;
use populate_table::write_parquet_many;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::Hasher;
use std::io::{self, Write};
use std::path::PathBuf;

fn resources() -> Option<PathBuf> {
    let r = default_resources();
    if r.is_none() {
        eprintln!("skipped: no datagen resources (scripts/fetch-ldbc-resources.sh)");
    }
    r
}

/// A writer that keeps only a hash and a length of what it was given.
#[derive(Default)]
struct Digest {
    h: DefaultHasher,
    len: u64,
}

impl Write for Digest {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.h.write(b);
        self.len += b.len() as u64;
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn digests(ws: Vec<Digest>) -> Vec<(u64, u64)> {
    ws.into_iter().map(|d| (d.h.finish(), d.len)).collect()
}

fn csv_digests(g: &Ldbc, threads: usize) -> Vec<(u64, u64)> {
    let ws = (0..DYNAMIC.len()).map(|_| Digest::default()).collect();
    digests(write_text_many(&[&g.csv()], ws, threads).expect("csv"))
}

/// Chunked, multi-threaded output is the same as one sequential run, from
/// the pre-pass on: three activity blocks of 21 chunks, the last one short.
#[test]
fn any_thread_count_same_output() {
    let Some(res) = resources() else { return };
    let persons = 20_500;
    let one = Ldbc::new(&res, persons, 1).expect("generate");
    // three blocks on three threads: chunk states found up front
    let many = Ldbc::new(&res, persons, 3).expect("generate");
    assert_eq!(one.num_blocks(), 3);
    assert_eq!(one.num_chunks(), 21);
    assert_eq!(one.num_knows(), many.num_knows());
    let a = csv_digests(&one, 1);
    // a fresh network on four threads: states found as chunks need them
    let lazy = Ldbc::new(&res, persons, 4).expect("generate");
    assert_eq!(csv_digests(&lazy, 4), a, "lazily found states");
    assert_eq!(csv_digests(&many, 3), a, "states found up front");
    assert_eq!(csv_digests(&many, 2), a, "two threads");
    assert_eq!(one.count_rows(1), many.count_rows(4));

    let pq = |threads| {
        let ws = (0..DYNAMIC.len()).map(|_| Digest::default()).collect();
        digests(write_parquet_many(&[&many], ws, threads, None).expect("parquet"))
    };
    assert_eq!(pq(1), pq(4), "parquet differs by thread count");
}

/// A chunk generated first, before its block's earlier chunks, starts
/// from the state those would leave (found by the skipping pass).
#[test]
fn chunks_in_any_order() {
    let Some(res) = resources() else { return };
    let persons = 12_000;
    let seq = Ldbc::new(&res, persons, 1).expect("generate");
    let rev = Ldbc::new(&res, persons, 1).expect("generate");
    let n = seq.num_chunks();
    let mut a = Ldbc::dynamic_rows();
    let mut b = Ldbc::dynamic_rows();
    let mut want = Vec::new();
    for i in 0..n {
        seq.chunk_rows(i, &mut a);
        want.push(csv_of(&a));
    }
    // last first, then the middle of the first block, then the rest
    let mut order: Vec<u64> = (0..n).rev().collect();
    order.swap(1, 6);
    for i in order {
        rev.chunk_rows(i, &mut b);
        assert!(csv_of(&b) == want[i as usize], "chunk {i} differs");
    }
}

/// A digest of a chunk's rows as CSV.
fn csv_of(rows: &[Rows]) -> (u64, u64) {
    let mut d = Digest::default();
    let mut out = Vec::new();
    for (def, r) in DYNAMIC.iter().zip(rows) {
        out.clear();
        csv::write(def, r, &mut out);
        d.write_all(&out).expect("digest");
    }
    (d.h.finish(), d.len)
}

/// SF 0.1 (one block) equals the Spark datagen's CSV byte for byte.
#[test]
fn matches_spark_reference() {
    let Some(res) = resources() else { return };
    let Some(dir) = std::env::var_os("LDBC_REFERENCE_SF0_1") else {
        eprintln!("skipped: LDBC_REFERENCE_SF0_1 not set");
        return;
    };
    let root = PathBuf::from(dir).join("graphs/csv/raw/composite-merged-fk");
    let g = Ldbc::new(&res, persons_for_scale_factor("0.1").expect("sf"), 0).expect("generate");
    // the reference's part files, headers dropped, in part order
    let reference = |kind: &str, name: &str| -> Vec<u8> {
        let mut parts: Vec<PathBuf> = fs::read_dir(root.join(kind).join(name))
            .expect("reference table")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "csv"))
            .collect();
        parts.sort();
        let mut out = Vec::new();
        for p in parts {
            let f = fs::read(p).expect("part");
            let body = f
                .iter()
                .position(|&c| c == b'\n')
                .map_or(&f[..0], |i| &f[i + 1..]);
            out.extend_from_slice(body);
        }
        out
    };
    let ws = vec![Vec::new(); DYNAMIC.len()];
    let files = write_text_many(&[&g.csv()], ws, 0).expect("csv");
    let strip = |f: &[u8]| f[f.iter().position(|&c| c == b'\n').expect("header") + 1..].to_vec();
    for (def, f) in DYNAMIC.iter().zip(files) {
        assert!(
            strip(&f) == reference("dynamic", def.name),
            "{} differs",
            def.name
        );
    }
    for (def, r) in STATIC.iter().zip(g.static_rows()) {
        let mut f = Vec::new();
        csv::write(def, &r, &mut f);
        assert!(f == reference("static", def.name), "{} differs", def.name);
    }
}
