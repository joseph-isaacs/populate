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

    for p in &reference {
        let (key, name, typ, size, container, mfgr, brand) = iter.next(&mut rng);

        // Keys are deterministic - must match exactly
        assert_eq!(key, p.p_partkey);

        // Validate our generated fields are valid
        assert!(part::is_valid_name(&name), "Invalid name: {}", name);
        assert!(part::is_valid_type(&typ), "Invalid type: {}", typ);
        assert!(
            part::is_valid_container(&container),
            "Invalid container: {}",
            container
        );
        assert!(size >= 1 && size <= 50);
        assert!(mfgr >= 1 && mfgr <= 5);
        assert!(brand >= 1 && brand <= 5);

        // Validate reference fields use correct distributions
        assert!(part::is_valid_name(&p.p_name.to_string()));
        assert!(part::is_valid_type(p.p_type));
        assert!(part::is_valid_container(p.p_container));
        assert!(p.p_size >= 1 && p.p_size <= 50);
    }
}

#[test]
fn test_parts_size_exact_match() {
    let reference: Vec<_> = PartGenerator::new(1.0, 1, 1)
        .into_iter()
        .take(100)
        .collect();

    let mut dist = part::parts();
    let mut iter = dist.sample();
    let mut rng = StdRng::seed_from_u64(0);

    for (i, p) in reference.iter().enumerate() {
        let (key, _, _, size, _, _, _) = iter.next(&mut rng);

        assert_eq!(key, p.p_partkey, "Key mismatch at row {}", i);
        assert_eq!(
            size, p.p_size as i64,
            "Size mismatch at row {}: ours={}, ref={}",
            i, size, p.p_size
        );
    }
}
