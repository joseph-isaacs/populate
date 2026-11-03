//! Sampler dialects against their references: Go `math/rand` (golden dump
//! from `validate/gorand`, real Go) and R 4.3.3 (values printed by R).

use crate::column::{DictCol, DictValue, StrCol};
use crate::dist::map;
use crate::random::{choose, choose_dict, uniform, uniform_int};
use crate::sampler::{FastSampler, GoRand, GoRandExt, GoSampler, RRng, RSampler, Sampler, r_round};
use crate::traits::{Distribution, DistributionScalar, SampleIter, VecBackend};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::HashSet;
use std::fs::read_to_string;

fn golden(name: &str) -> String {
    read_to_string(format!(
        "{}/tests/golden/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

// ============================================================================
// Go
// ============================================================================

#[test]
fn go_matches_go_stdlib() {
    let text = golden("gorand.txt");
    let mut rng = GoRand::new(0);
    let mut checked = 0;
    for line in text.lines() {
        let parts: Vec<&str> = line.split(' ').collect();
        match parts[..] {
            ["seed", s] => rng = GoRand::new(s.parse().unwrap()),
            ["int63", v] => assert_eq!(rng.go_int63(), v.parse::<i64>().unwrap()),
            ["float64", v] => assert_eq!(
                GoSampler.unif(&mut rng).to_bits(),
                u64::from_str_radix(v, 16).unwrap()
            ),
            ["intn", n, v] => assert_eq!(
                GoSampler.index(&mut rng, n.parse().unwrap()),
                v.parse::<usize>().unwrap()
            ),
            ["int63n", n, v] => {
                assert_eq!(rng.go_int63n(n.parse().unwrap()), v.parse::<i64>().unwrap())
            }
            ["norm", v] => assert_eq!(
                GoSampler.normal(&mut rng).to_bits(),
                u64::from_str_radix(v, 16).unwrap(),
                "line {line}"
            ),
            ["uint32", v] => assert_eq!(rng.go_uint32(), v.parse::<u32>().unwrap()),
            ["perm", n, ref rest @ ..] => {
                let want: Vec<usize> = rest.iter().map(|v| v.parse().unwrap()).collect();
                assert_eq!(GoSampler.permutation(&mut rng, n.parse().unwrap()), want);
            }
            _ => panic!("unexpected golden line {line}"),
        }
        checked += 1;
    }
    assert!(checked > 6600);
}

// ============================================================================
// R
// ============================================================================

fn make_r_sample_int(n: usize) -> impl Distribution<VecBackend, Item = i32> {
    uniform_int(RSampler, 1, n)
}

/// set.seed(42); sample.int(100, 6, TRUE); sample.int(20, 6); runif(3); sample.int(3e7, 4)
#[test]
fn r_matches_r() {
    let rng = &mut RRng::new(42);
    let ints: Vec<i32> = make_r_sample_int(100).collect_col(Vec::new(), rng, 6);
    assert_eq!(ints, vec![49, 65, 25, 74, 100, 18]);

    let no_repl: Vec<usize> = RSampler
        .sample_without_replacement(rng, 20, 6)
        .into_iter()
        .map(|i| i + 1)
        .collect();
    assert_eq!(no_repl, vec![17, 15, 7, 4, 9, 5]);

    let u: Vec<f64> = (0..3).map(|_| RSampler.runif(rng, 0.0, 1.0)).collect();
    assert_eq!(
        u,
        vec![
            0.978_226_428_385_824,
            0.117_487_361_654_639_24,
            0.474_997_081_561_014_06
        ]
    );

    // n > 1e7 and k <= n/2: R's hash-rejection algorithm.
    let hashed: Vec<usize> = RSampler
        .sample_without_replacement(rng, 30_000_000, 4)
        .into_iter()
        .map(|i| i + 1)
        .collect();
    assert_eq!(hashed, vec![24_242_031, 25_361_705, 5_772_571, 27_485_157]);
}

fn make_r_runif() -> impl Distribution<VecBackend, Item = f64> {
    uniform(RSampler, 0.0, 1.0)
}

#[test]
fn uniform_leaf_draws_like_runif() {
    let u: Vec<f64> = make_r_runif().collect_col(Vec::new(), &mut RRng::new(42), 3);
    let mut rng = RRng::new(42);
    let want: Vec<f64> = (0..3).map(|_| RSampler.runif(&mut rng, 0.0, 1.0)).collect();
    assert_eq!(u, want);
    // R: set.seed(42); runif(1)
    assert_eq!(u[0], 0.914_806_043_496_355_4);
}

/// round(c(0.0000005, 2.5e-6, 1.2345675, 99.9999995, 0.1234565), 6)
#[test]
fn r_round_matches_r() {
    let got: Vec<f64> = [0.000_000_5, 2.5e-6, 1.234_567_5, 99.999_999_5, 0.123_456_5]
        .into_iter()
        .map(|x| r_round(x, 6))
        .collect();
    assert_eq!(got, vec![0.0, 3e-6, 1.234_568, 100.0, 0.123_456]);
}

// ============================================================================
// Dialect-independent behaviour
// ============================================================================

fn make_labels<S: Sampler>(s: S) -> impl Distribution<VecBackend, Item = String> {
    choose(s, vec!["a".to_string(), "b".to_string(), "c".to_string()])
}

fn make_rounded<S: Sampler>(s: S) -> impl Distribution<VecBackend, Item = f64> {
    map(uniform(s, 0.0, 100.0), |x: f64| r_round(x, 2))
}

#[test]
fn leaves_work_with_every_dialect() {
    fn check<S: Sampler>(s: S, rng: &mut impl rand::Rng) {
        let labels: StrCol = make_labels(s).collect_col(StrCol::new(), rng, 300);
        assert_eq!(labels.len(), 300);
        let distinct: HashSet<&str> = labels.iter().collect();
        assert_eq!(distinct, HashSet::from(["a", "b", "c"]));
        let xs: Vec<f64> = make_rounded(s).collect_col(Vec::new(), rng, 300);
        assert!(xs.iter().all(|x| (0.0..=100.0).contains(x)));
        assert!(xs.iter().all(|x| r_round(*x, 2) == *x));
        let mut p = s.permutation(rng, 50);
        p.sort_unstable();
        assert_eq!(p, (0..50).collect::<Vec<_>>());
        let k = s.sample_without_replacement(rng, 50, 20);
        assert_eq!(k.iter().collect::<HashSet<_>>().len(), 20);
        let mean = (0..2000).map(|_| s.normal(rng)).sum::<f64>() / 2000.0;
        assert!(mean.abs() < 0.1, "mean {mean}");
    }
    check(FastSampler, &mut StdRng::seed_from_u64(1));
    check(RSampler, &mut RRng::new(1));
    check(GoSampler, &mut GoRand::new(1));
    // a dialect's algorithms over a foreign RNG ("fast mode")
    check(GoSampler, &mut StdRng::seed_from_u64(2));
}

fn make_label_codes<S: Sampler>(s: S) -> impl Distribution<VecBackend, Item = DictValue<String>> {
    choose_dict(s, vec!["a".to_string(), "b".to_string(), "c".to_string()])
}

/// `choose_dict` draws the same picks as `choose`, column-wise and row-wise.
#[test]
fn choose_dict_matches_choose() {
    let plain: StrCol = make_labels(RSampler).collect_col(StrCol::new(), &mut RRng::new(5), 200);
    let dict: DictCol<String> =
        make_label_codes(RSampler).collect_col(DictCol::default(), &mut RRng::new(5), 200);
    assert_eq!(dict.len(), 200);
    let decoded: Vec<&str> = (0..200).map(|i| dict.get(i).as_str()).collect();
    assert_eq!(decoded, plain.iter().collect::<Vec<_>>());

    let rng = &mut RRng::new(5);
    let mut rows = make_label_codes(RSampler).row_iter();
    let row_wise: Vec<String> = (0..200).map(|_| (*rows.next(rng)).clone()).collect();
    assert_eq!(row_wise, decoded);
}
