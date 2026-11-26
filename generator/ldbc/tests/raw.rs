use populate_gen_ldbc::factors::FACTOR;
use populate_gen_ldbc::rows::{DYNAMIC, STATIC, rows_of};
use populate_gen_ldbc::{Ldbc, csv, default_resources};

#[test]
fn native_dynamic_static_and_factor_chunks() {
    let Some(resources) = default_resources() else {
        return;
    };
    let mut g = Ldbc::new(&resources, 1000, 1).unwrap();
    let raw = g.raw_chunks();
    assert_eq!(raw.rows(), g.num_chunks());
    let native = rows_of(raw.batch(0, 1));
    let expected = g.chunk_rows(0);
    assert_eq!(native.len(), 14);
    for ((def, actual), expected) in DYNAMIC.iter().zip(&native).zip(&expected) {
        assert_eq!(actual.cols.len(), def.cols.len());
        let (mut a, mut b) = (Vec::new(), Vec::new());
        csv::write(def, actual, &mut a);
        csv::write(def, expected, &mut b);
        assert_eq!(a, b, "{}", def.name);
    }
    let statics = rows_of(g.static_chunks().batch(0, 1));
    assert_eq!(statics.len(), STATIC.len());
    assert!(statics.iter().all(|r| !r.is_empty()));
    drop(raw);
    g.collect_factors();
    g.raw_chunks().for_each_batch(2, |_| {});
    let factors = rows_of(g.factor_chunks(1).batch(0, 1));
    assert_eq!(factors.len(), FACTOR.len());
    assert!(factors.iter().any(|r| !r.is_empty()));
}
