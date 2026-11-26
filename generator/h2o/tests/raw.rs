use populate_core::{DistChunks, Distribution};
use populate_gen_h2o::GroupBy;
use populate_gen_h2o::raw::{ColumnKind, NA, Native};

fn render(raw: &DistChunks<Native>) -> Vec<u8> {
    let names = raw.dist().names();
    let kinds = raw.dist().column_types();
    let mut out = format!("{}\n", names.join(","));
    raw.for_each_batch(2, |cols| {
        for row in 0..cols.0[0].len() {
            for (i, (col, ty)) in cols.0.iter().zip(&kinds).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let value = col[row];
                if value == NA {
                    assert!(ty.nullable);
                    continue;
                }
                match ty.kind {
                    ColumnKind::Id { width } => out.push_str(&format!("id{value:0width$}")),
                    ColumnKind::Int => out.push_str(&value.to_string()),
                    ColumnKind::Micros => out.push_str(&(value as f64 / 1_000_000.0).to_string()),
                }
            }
            out.push('\n');
        }
    });
    out.into_bytes()
}

#[test]
fn native_groupby_preserves_values_and_nulls() {
    for sort in [false, true] {
        for nas in [0, 10] {
            let g = GroupBy::with_chunk_rows(1000, 10, nas, sort, 113, 1);
            let raw = g.raw().batch_rows(71);
            let text = render(&raw);
            assert!(text.starts_with(b"id1,id2,id3,id4,id5,id6,v1,v2,v3\n"));
            assert_eq!(raw.dist().column_types().len(), 9);
            let mut names = Vec::new();
            raw.dist().column_names(&mut names);
            assert_eq!(names, raw.dist().names());
            #[cfg(feature = "formats")]
            assert_eq!(text, g.write_csv(Vec::new(), 1).unwrap());
        }
    }
}
