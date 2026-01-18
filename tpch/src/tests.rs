use crate::tables::{nation, part, region};
use populate_core::{Distribution, SampleIter};
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
