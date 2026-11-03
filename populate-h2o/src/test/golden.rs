//! Values produced by R 4.3.3 / data.table 1.14.10 (see validate/validate.sh
//! for the full byte-for-byte comparison against the reference scripts).

use crate::dist::{runif_rounded, sample_int};
use crate::ops::{covering_sample, split_keys};
use crate::rng::{RRng, r_round};
use crate::sampler::{RSampler, Sampler, sample_without_replacement};
use crate::{GroupByConfig, GroupByTable};
use populate_2::traits::{Distribution, DistributionScalar, VecBackend};

fn csv_head(table: GroupByTable, lines: usize) -> String {
    let mut out = Vec::new();
    table.into_table().write_csv(&mut out).unwrap();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .take(lines)
        .collect::<Vec<_>>()
        .join("\n")
}

fn make_sample_int(n: usize) -> impl Distribution<VecBackend, Item = i32> {
    sample_int(RSampler, n)
}

fn make_runif() -> impl Distribution<VecBackend, Item = f64> {
    runif_rounded(RSampler, 0.0, 1.0, 17)
}

/// set.seed(42); sample.int(100, 6, TRUE); sample.int(20, 6); runif(3); sample.int(3e7, 4)
#[test]
fn r_primitives() {
    let rng = &mut RRng::new(42);
    let ints: Vec<i32> = make_sample_int(100).collect_col(Vec::new(), rng, 6);
    assert_eq!(ints, vec![49, 65, 25, 74, 100, 18]);

    let no_repl: Vec<usize> = sample_without_replacement(RSampler, rng, 20, 6)
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
    let hashed: Vec<usize> = sample_without_replacement(RSampler, rng, 30_000_000, 4)
        .into_iter()
        .map(|i| i + 1)
        .collect();
    assert_eq!(hashed, vec![24_242_031, 25_361_705, 5_772_571, 27_485_157]);
}

#[test]
fn r_runif_leaf_matches_runif() {
    // runif via the Leaf distribution consumes the stream like runif(3).
    let mut d = make_runif();
    let u: Vec<f64> = d.collect_col(Vec::new(), &mut RRng::new(42), 1);
    let mut rng = RRng::new(42);
    assert_eq!(u, vec![RSampler.runif(&mut rng, 0.0, 1.0)]);
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

/// set.seed(108); k = split_xlr(10); sample_all(c(k$x, k$l), 15)
#[test]
fn split_and_covering_sample_match_r() {
    let rng = &mut RRng::new(108);
    let k = split_keys(RSampler, rng, 10.0, 0.1);
    assert_eq!(k.matched, vec![10, 7, 6, 5, 11, 8, 9, 3, 4]);
    assert_eq!(k.left_only, vec![2]);
    assert_eq!(k.right_only, vec![1]);
    let col = covering_sample(RSampler, rng, &k.left(), 15);
    assert_eq!(col, vec![5, 8, 3, 11, 2, 8, 6, 9, 4, 9, 9, 8, 10, 7, 4]);
}

/// head of `Rscript groupby-datagen.R 1e4 1e2 0 0`
#[test]
fn groupby_1e4_1e2_0_0_head() {
    let t = GroupByTable::reference(GroupByConfig::new(10_000, 100, 0, false));
    assert_eq!(
        csv_head(t, 4),
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n\
         id016,id056,id0000000042,82,64,50,4,3,26.407777\n\
         id039,id075,id0000000067,30,28,17,1,6,7.913725\n\
         id047,id077,id0000000027,40,67,23,5,14,54.691464"
    );
}

/// head of `Rscript groupby-datagen.R 1e4 1e1 5 1` (NAs and sorted)
#[test]
fn groupby_1e4_1e1_5_1_head() {
    let t = GroupByTable::reference(GroupByConfig::new(10_000, 10, 5, true));
    assert_eq!(
        csv_head(t, 6),
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n\
         id001,id001,,4,6,204,4,9,97.212754\n\
         id001,id001,,7,1,276,3,7,1.078204\n\
         id001,id001,,7,9,53,3,10,\n\
         id001,id001,,7,9,721,5,1,37.63933\n\
         id001,id001,,8,5,970,3,15,14.464437"
    );
}
