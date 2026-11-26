//! Tests of the BI mode, the exploded / epoch-millis layouts and the factor
//! tables. They need the datagen's dictionaries
//! (`scripts/fetch-ldbc-resources.sh`, or `LDBC_DATAGEN_RESOURCES`) and are
//! skipped without them.
//!
//! With a Spark datagen output directory at SF 0.1 they also compare byte
//! for byte (CSV):
//!
//! - `LDBC_REFERENCE_BI_SF0_1`: `--mode bi --format csv`;
//! - `LDBC_REFERENCE_LAYOUT_SF0_1`: `--mode raw --format csv
//!   --explode-edges --explode-attrs --epoch-millis`;
//! - `LDBC_REFERENCE_FACTORS_SF0_1`: `--mode raw --generate-factors
//!   --factor-format csv` (tables with no defined row order as sets).

use populate_gen_ldbc::bi::{Batches, Bi, BiRun};
use populate_gen_ldbc::factors::{Factor, UNORDERED};
use populate_gen_ldbc::layout::{Layout, LayoutOptions, LayoutRun};
use populate_gen_ldbc::rows::{Rows, TableDef};
use populate_gen_ldbc::{Ldbc, csv, default_resources, persons_for_scale_factor};
use populate_table::text::write_text_many;
use std::env::var_os;
use std::fs;
use std::path::{Path, PathBuf};
use std::slice::from_ref;

fn resources() -> Option<PathBuf> {
    let r = default_resources();
    if r.is_none() {
        eprintln!("skipped: no datagen resources (scripts/fetch-ldbc-resources.sh)");
    }
    r
}

fn reference(var: &str) -> Option<PathBuf> {
    let d = var_os(var).map(PathBuf::from);
    if d.is_none() {
        eprintln!("skipped: {var} not set");
    }
    d
}

/// A reference table directory's part files, in part order: the first
/// header and the rows (`header` false: no header lines at all).
fn parts(dir: &Path, header: bool) -> (Vec<u8>, Vec<u8>) {
    let mut ps: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "csv"))
        .collect();
    ps.sort();
    let (mut head, mut body) = (Vec::new(), Vec::new());
    for p in ps {
        let f = fs::read(p).expect("part");
        let (h, b) = if header {
            split_header(&f)
        } else {
            (&f[..0], &f[..])
        };
        if head.is_empty() {
            head = h.to_vec();
        }
        body.extend_from_slice(b);
    }
    (head, body)
}

/// A CSV file's header line (with its line feed) and the rest.
fn split_header(f: &[u8]) -> (&[u8], &[u8]) {
    let i = f.iter().position(|&c| c == b'\n').map_or(0, |i| i + 1);
    f.split_at(i)
}

fn with_header(def: &TableDef, rows: &Rows) -> Vec<u8> {
    parts_with_header(def, from_ref(rows))
}

/// A header, then the rows of every part.
fn parts_with_header(def: &TableDef, parts: &[Rows]) -> Vec<u8> {
    let mut f = Vec::new();
    csv::header(def, &mut f);
    for rows in parts {
        csv::write(def, rows, &mut f);
    }
    f
}

/// `(table, batch id, CSV with header)` of every insert and delete batch.
fn batches_csv(tables: &[Batches]) -> Vec<(String, String, Vec<u8>)> {
    tables
        .iter()
        .flat_map(|t| {
            t.batches.iter().map(|(id, r)| {
                (
                    t.def.name.to_string(),
                    id.clone(),
                    parts_with_header(t.def, r),
                )
            })
        })
        .collect()
}

/// A BI run's snapshot CSV, inserts and deletes, on `threads` threads.
type BiOut = (
    Vec<Vec<u8>>,
    Vec<(String, String, Vec<u8>)>,
    Vec<(String, String, Vec<u8>)>,
);

fn bi_output(g: &Ldbc, layout: &Layout, threads: usize) -> BiOut {
    let run = BiRun::new(g, layout, Bi::default());
    let ws = vec![Vec::new(); run.tables.snapshot.len()];
    let snap = write_text_many(&[&run], ws, threads).expect("snapshot");
    let (ins, del) = run.finish(threads);
    (snap, batches_csv(&ins), batches_csv(&del))
}

/// The BI snapshot and batches are the same for any thread count, with
/// chunk states found up front or lazily (two blocks).
#[test]
fn bi_any_thread_count() {
    let Some(res) = resources() else { return };
    let layout = Layout::new(LayoutOptions::default());
    let one = Ldbc::new(&res, 12_000, 1).expect("generate");
    let a = bi_output(&one, &layout, 1);
    assert!(!a.1.is_empty() && !a.2.is_empty(), "no inserts or deletes");
    let many = Ldbc::new(&res, 12_000, 4).expect("generate");
    assert!(
        bi_output(&many, &layout, 4) == a,
        "BI output differs by thread count"
    );
}

/// Factor tables are the same for any thread count.
#[test]
fn factors_any_thread_count() {
    let Some(res) = resources() else { return };
    let run = |threads| -> Vec<Vec<u8>> {
        let mut g = Ldbc::new(&res, 12_000, threads).expect("generate");
        g.collect_factors();
        g.count_rows(threads);
        g.factors(threads)
            .iter()
            .map(|f| {
                let mut out = Vec::new();
                csv::write_factor(f.def, &f.rows, &mut out);
                out
            })
            .collect()
    };
    let a = run(1);
    assert_eq!(a.len(), 34);
    assert!(run(4) == a, "factor tables differ by thread count");
}

/// SF 0.1 in BI mode equals the Spark datagen's: snapshot, static tables,
/// and every insert and delete batch.
#[test]
fn bi_matches_spark_reference() {
    let Some(res) = resources() else { return };
    let Some(dir) = reference("LDBC_REFERENCE_BI_SF0_1") else {
        return;
    };
    let root = dir.join("graphs/csv/bi/composite-merged-fk");
    let g = Ldbc::new(&res, persons_for_scale_factor("0.1").expect("sf"), 0).expect("generate");
    let layout = Layout::new(LayoutOptions::default());
    let run = BiRun::new(&g, &layout, Bi::default());
    let ws = vec![Vec::new(); run.tables.snapshot.len()];
    let snap = write_text_many(&[&run], ws, 0).expect("snapshot");
    let snap_dir = root.join("initial_snapshot");
    for (p, f) in run.tables.snapshot.iter().zip(&snap) {
        let (h, b) = parts(&snap_dir.join("dynamic").join(p.def.name), true);
        let (mh, mb) = split_header(f);
        assert!(mh == h && mb == b, "snapshot {} differs", p.def.name);
    }
    let lr = LayoutRun {
        g: &g,
        layout: &layout,
    };
    for (t, r) in layout.statics.iter().zip(lr.static_rows()) {
        let (h, b) = parts(&snap_dir.join("static").join(t.def.name), true);
        assert!(
            with_header(t.def, &r) == [h, b].concat(),
            "static {} differs",
            t.def.name
        );
    }
    let (ins, del) = run.finish(0);
    for (kind, tables) in [("inserts", ins), ("deletes", del)] {
        for t in &tables {
            let tdir = root.join(kind).join("dynamic").join(t.def.name);
            let mut want: Vec<String> = fs::read_dir(&tdir)
                .expect("batches")
                .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("batch_id="))
                .collect();
            want.sort();
            let got: Vec<String> = t
                .batches
                .iter()
                .map(|(id, _)| format!("batch_id={id}"))
                .collect();
            assert_eq!(got, want, "{kind} {} batches", t.def.name);
            for (id, r) in &t.batches {
                let (h, b) = parts(&tdir.join(format!("batch_id={id}")), true);
                assert!(
                    parts_with_header(t.def, r) == [h, b].concat(),
                    "{kind} {} batch {id} differs",
                    t.def.name
                );
            }
        }
    }
}

/// SF 0.1 with exploded edges and attributes and epoch milliseconds equals
/// the Spark datagen's (36 tables).
#[test]
fn layout_matches_spark_reference() {
    let Some(res) = resources() else { return };
    let Some(dir) = reference("LDBC_REFERENCE_LAYOUT_SF0_1") else {
        return;
    };
    let layout = Layout::new(LayoutOptions {
        explode_edges: true,
        explode_attrs: true,
        epoch_millis: true,
    });
    let root = dir.join("graphs/csv/raw").join(layout.dir());
    let g = Ldbc::new(&res, persons_for_scale_factor("0.1").expect("sf"), 0).expect("generate");
    let lr = LayoutRun {
        g: &g,
        layout: &layout,
    };
    let ws = vec![Vec::new(); layout.dynamic.len()];
    let files = write_text_many(&[&lr], ws, 0).expect("csv");
    for (t, f) in layout.dynamic.iter().zip(&files) {
        let (h, b) = parts(&root.join("dynamic").join(t.def.name), true);
        let (mh, mb) = split_header(f);
        assert!(mh == h && mb == b, "{} differs", t.def.name);
    }
    for (t, r) in layout.statics.iter().zip(lr.static_rows()) {
        let (h, b) = parts(&root.join("static").join(t.def.name), true);
        assert!(
            with_header(t.def, &r) == [h, b].concat(),
            "static {} differs",
            t.def.name
        );
    }
}

/// SF 0.1's factor tables equal the Spark datagen's.
#[test]
fn factors_match_spark_reference() {
    let Some(res) = resources() else { return };
    let Some(dir) = reference("LDBC_REFERENCE_FACTORS_SF0_1") else {
        return;
    };
    let root = dir.join("factors/csv/raw/composite-merged-fk");
    let mut g = Ldbc::new(&res, persons_for_scale_factor("0.1").expect("sf"), 0).expect("generate");
    g.collect_factors();
    g.count_rows(0);
    let lines = |b: &[u8]| -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = b
            .split_inclusive(|&c| c == b'\n')
            .map(<[u8]>::to_vec)
            .collect();
        v.sort();
        v
    };
    let fs: Vec<Factor> = g.factors(0);
    assert_eq!(fs.len(), 34);
    for f in &fs {
        let mut mine = Vec::new();
        csv::write_factor(f.def, &f.rows, &mut mine);
        let (_, want) = parts(&root.join(f.def.name), false);
        if UNORDERED.contains(&f.def.name) {
            assert!(
                lines(&mine) == lines(&want),
                "{} differs (as a set)",
                f.def.name
            );
        } else {
            assert!(mine == want, "{} differs", f.def.name);
        }
    }
}
