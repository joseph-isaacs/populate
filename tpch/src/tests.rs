use crate::tables::{nation, part, region};
use populate_core::{Distribution, IntoColumnIters, SampleIter};
use rand::SeedableRng;
use rand::rngs::StdRng;
use tpchgen::generators::{NationGenerator, PartGenerator, RegionGenerator};

#[test]
fn test_regions_match_reference() {
    let reference: Vec<_> = RegionGenerator::new(1.0, 1, 1).into_iter().collect();

    let mut dist = region::regions();
    let mut iter = dist.sample();
    let mut rng = StdRng::seed_from_u64(0);

    for r in &reference {
        let (key, name) = iter.next(&mut rng);
        assert_eq!(key, r.r_regionkey);
        assert_eq!(name, r.r_name);
    }
}

#[test]
fn test_nations_match_reference() {
    let reference: Vec<_> = NationGenerator::new(1.0, 1, 1).into_iter().collect();

    let mut dist = nation::nations();
    let mut iter = dist.sample();
    let mut rng = StdRng::seed_from_u64(0);

    for n in &reference {
        let (key, name, region_key) = iter.next(&mut rng);
        assert_eq!(key, n.n_nationkey);
        assert_eq!(name, n.n_name);
        assert_eq!(region_key, n.n_regionkey);
    }
}

#[test]
fn test_parts_match_reference() {
    let reference: Vec<_> = PartGenerator::new(1.0, 1, 1)
        .into_iter()
        .take(100)
        .collect();

    let mut dist = part::parts();
    let mut iter = dist.sample();
    let mut rng = StdRng::seed_from_u64(0);

    for (i, p) in reference.iter().enumerate() {
        let (key, name, typ, size, container, mfgr, brand, retailprice) = iter.next(&mut rng);

        // p_partkey
        assert_eq!(key, p.p_partkey, "partkey mismatch at row {}", i);

        // p_name
        assert_eq!(
            name,
            p.p_name.to_string(),
            "name mismatch at row {}: ours={}, ref={}",
            i,
            name,
            p.p_name
        );

        // p_type
        assert_eq!(
            typ, p.p_type,
            "type mismatch at row {}: ours={}, ref={}",
            i, typ, p.p_type
        );

        // p_size
        assert_eq!(
            size, p.p_size as i64,
            "size mismatch at row {}: ours={}, ref={}",
            i, size, p.p_size
        );

        // p_container
        assert_eq!(
            container, p.p_container,
            "container mismatch at row {}: ours={}, ref={}",
            i, container, p.p_container
        );

        // p_mfgr (extract number from "Manufacturer#N" format)
        let ref_mfgr: i32 = p
            .p_mfgr
            .to_string()
            .trim_start_matches("Manufacturer#")
            .parse()
            .unwrap();
        assert_eq!(
            mfgr, ref_mfgr,
            "mfgr mismatch at row {}: ours={}, ref={}",
            i, mfgr, ref_mfgr
        );

        // p_brand (extract number from "Brand#NN" format)
        let ref_brand: i32 = p
            .p_brand
            .to_string()
            .trim_start_matches("Brand#")
            .parse()
            .unwrap();
        assert_eq!(
            brand, ref_brand,
            "brand mismatch at row {}: ours={}, ref={}",
            i, brand, ref_brand
        );

        // p_retailprice (stored as cents, ref uses TPCHDecimal)
        let ref_price = p.p_retailprice.0;
        assert_eq!(
            retailprice, ref_price,
            "retailprice mismatch at row {}: ours={}, ref={}",
            i, retailprice, ref_price
        );
    }
}

/// Test that demonstrates the TupleUnnest7At5Iter structure supports IntoColumnIters.
/// This is what parts() uses internally - 7 column groups with position 5 coupled.
#[test]
fn test_parts_structure_supports_column_by_column() {
    use populate_builtins::{MapExt, Range, inc};
    use populate_core::unnest;

    // Construct a distribution with the same structure as parts():
    // (D1, D2, D3, D4, D5, Unnest<D6>, D7) -> 8-tuple output
    let mut dist = (
        inc(1i64),               // col 1: independent
        Range::new(0i32, 100),   // col 2: independent
        Range::new(0i64, 1000),  // col 3: independent
        Range::new(0.0f64, 1.0), // col 4: independent
        Range::new(100i32, 200), // col 5: independent
        unnest(
            (Range::new(1i32, 6), Range::new(1i32, 6)).map(|(a, b)| (a, a * 10 + b)), // col 6: coupled pair
        ),
        inc(1i64).map(part::calculate_retail_price), // col 7: independent
    );

    let iter = dist.sample();

    // Split into 7 column groups
    let (mut col1, mut col2, mut col3, mut col4, mut col5, mut col6, mut col7) =
        iter.into_columns();

    let mut rng = StdRng::seed_from_u64(42);
    let n = 10;

    // Collect column-by-column (each column fully generated before the next)
    let col1_vals: Vec<i64> = (0..n).map(|_| col1.next(&mut rng)).collect();
    let col2_vals: Vec<i32> = (0..n).map(|_| col2.next(&mut rng)).collect();
    let col3_vals: Vec<i64> = (0..n).map(|_| col3.next(&mut rng)).collect();
    let col4_vals: Vec<f64> = (0..n).map(|_| col4.next(&mut rng)).collect();
    let col5_vals: Vec<i32> = (0..n).map(|_| col5.next(&mut rng)).collect();
    // col6 is coupled: produces (i32, i32) pairs
    let col6_vals: Vec<(i32, i32)> = (0..n).map(|_| col6.next(&mut rng)).collect();
    let col7_vals: Vec<i64> = (0..n).map(|_| col7.next(&mut rng)).collect();

    // Verify all columns have correct length
    assert_eq!(col1_vals.len(), n);
    assert_eq!(col2_vals.len(), n);
    assert_eq!(col3_vals.len(), n);
    assert_eq!(col4_vals.len(), n);
    assert_eq!(col5_vals.len(), n);
    assert_eq!(col6_vals.len(), n);
    assert_eq!(col7_vals.len(), n);

    // Verify col1 (inc) produces 1, 2, 3, ...
    for (i, &val) in col1_vals.iter().enumerate() {
        assert_eq!(val, (i + 1) as i64);
    }

    // Verify col7 (retailprice) matches the formula
    for (i, &val) in col7_vals.iter().enumerate() {
        let expected = part::calculate_retail_price((i + 1) as i64);
        assert_eq!(val, expected);
    }

    // Verify coupled column produces valid pairs (a, a*10+b where a,b in 1..6)
    for &(a, combined) in &col6_vals {
        assert!(a >= 1 && a <= 5);
        assert!(combined >= a * 10 + 1 && combined <= a * 10 + 5);
    }
}
