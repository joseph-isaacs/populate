use arrow_array::cast::AsArray;
use populate_2_tpch::{Part, Supplier, Table, batches};
use tpchgen::generators::PartGenerator;
use tpchgen_arrow::PartArrow;

#[test]
fn supplier_bbb_rows_present() {
    let n: usize = batches::<Supplier>(1.0, 10_000)
        .map(|b| {
            b.column(6)
                .as_string_view()
                .iter()
                .flatten()
                .filter(|s| {
                    s.contains("Customer") && (s.contains("Complaints") || s.contains("Recommends"))
                })
                .count()
        })
        .sum();
    println!("BBB rows: {n}");
    assert!(n > 0);
}

#[test]
fn comparison_detects_one_char_change() {
    let ours = Part::batch(0.01, 0, 100);
    let theirs = PartArrow::new(PartGenerator::new(0.01, 1, 1))
        .with_batch_size(100)
        .next()
        .unwrap();
    assert_eq!(ours, theirs);
    // flip one byte in one comment and rebuild
    let mut s: Vec<String> = ours
        .column(8)
        .as_string_view()
        .iter()
        .map(|v| v.unwrap().to_string())
        .collect();
    s[57].replace_range(3..4, "#");
    let changed = arrow_array::StringViewArray::from_iter_values(s);
    assert_ne!(ours.column(8).as_ref(), &changed as &dyn arrow_array::Array);
}
