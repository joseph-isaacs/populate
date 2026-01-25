use crate::tables::{nation, part, region};
use populate_core::{DistributionCollectColumnarExt, Distribution, SampleIter};
use populate_sink_vec::VecSink;
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
    let reference: Vec<_> = PartGenerator::new(1.0, 1, 1).into_iter().take(4).collect();

    let mut dist = part::parts();
    let mut rng = StdRng::seed_from_u64(0);

    // Create sinks for each column
    let sinks: (
        VecSink<i64>,
        VecSink<i64>,
        VecSink<String>,
        VecSink<&'static String>,
        VecSink<i64>,
        VecSink<&'static String>,
        VecSink<i32>,
        VecSink<i32>,
    ) = (
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
        VecSink::new(),
    );

    // Collect using columnar method - this should do column-by-column for independent columns
    type Sinks = (
        VecSink<i64>,
        VecSink<i64>,
        VecSink<String>,
        VecSink<&'static String>,
        VecSink<i64>,
        VecSink<&'static String>,
        VecSink<i32>,
        VecSink<i32>,
    );
    let (keys, retailprices, names, typs, sizes, containers, mfgrs, brands) =
        DistributionCollectColumnarExt::collect_columnar::<Sinks>(&mut dist, &mut rng, 4, sinks);

    for (i, p) in reference.iter().enumerate() {
        // p_partkey
        assert_eq!(keys[i], p.p_partkey, "partkey mismatch at row {}", i);

        // p_retailprice (stored as cents, ref uses TPCHDecimal)
        let ref_price = p.p_retailprice.0;
        assert_eq!(
            retailprices[i], ref_price,
            "retailprice mismatch at row {}: ours={}, ref={}",
            i, retailprices[i], ref_price
        );

        // p_name
        assert_eq!(
            names[i],
            p.p_name.to_string(),
            "name mismatch at row {}: ours={}, ref={}",
            i,
            names[i],
            p.p_name
        );

        // p_type
        assert_eq!(
            typs[i].as_str(),
            p.p_type,
            "type mismatch at row {}: ours={}, ref={}",
            i,
            typs[i].as_str(),
            p.p_type
        );

        // p_size
        assert_eq!(
            sizes[i], p.p_size as i64,
            "size mismatch at row {}: ours={}, ref={}",
            i, sizes[i], p.p_size
        );

        // p_container
        assert_eq!(
            containers[i].as_str(),
            p.p_container,
            "container mismatch at row {}: ours={}, ref={}",
            i,
            containers[i].as_str(),
            p.p_container
        );

        // p_mfgr (extract number from "Manufacturer#N" format)
        let ref_mfgr: i32 = p
            .p_mfgr
            .to_string()
            .trim_start_matches("Manufacturer#")
            .parse()
            .unwrap();
        assert_eq!(
            mfgrs[i], ref_mfgr,
            "mfgr mismatch at row {}: ours={}, ref={}",
            i, mfgrs[i], ref_mfgr
        );

        // p_brand (extract number from "Brand#NN" format)
        let ref_brand: i32 = p
            .p_brand
            .to_string()
            .trim_start_matches("Brand#")
            .parse()
            .unwrap();
        assert_eq!(
            brands[i], ref_brand,
            "brand mismatch at row {}: ours={}, ref={}",
            i, brands[i], ref_brand
        );
    }
}
