// ========================================================================
// Random leaves and string columns
// ========================================================================

use crate::dist::{chars, choose, list, text, uniform, weighted};
use crate::traits::{Distribution, DistributionScalar, SampleIter, StrCol, VecBackend};
use rand::SeedableRng;
use rand::rngs::SmallRng;

fn rng() -> SmallRng {
    SmallRng::seed_from_u64(7)
}

const WORDS: &[&str] = &["ab", "cde", "f"];

fn d_uniform() -> impl Distribution<VecBackend, Item = i32> {
    uniform(3i32, 9)
}

fn d_choose() -> impl Distribution<VecBackend, Item = u8> {
    choose(&[1u8, 2, 3])
}

fn d_weighted() -> impl Distribution<VecBackend, Item = u8> {
    weighted(&[(0u8, 1), (1, 0), (2, 9)])
}

fn d_text() -> impl Distribution<VecBackend, Item = String> {
    text(WORDS, 1, 3)
}

fn d_chars() -> impl Distribution<VecBackend, Item = String> {
    chars(b"XY", 4, 4)
}

fn d_colors() -> impl Distribution<VecBackend, Item = &'static str> {
    choose(&["red", "green"])
}

fn d_list_of_str() -> impl Distribution<VecBackend, Item = Vec<&'static str>> {
    list(uniform(2usize, 2), choose(&["x"]))
}

#[test]
fn uniform_choose_weighted_stay_in_domain() {
    let v: Vec<i32> = DistributionScalar::<VecBackend>::collect_col(
        &mut d_uniform(),
        Vec::new(),
        &mut rng(),
        10_000,
    );
    assert!(v.iter().all(|x| (3..=9).contains(x)));
    assert_eq!(v.capacity(), 10_000, "reserved once, exactly");

    let c: Vec<u8> = DistributionScalar::<VecBackend>::collect_col(
        &mut d_choose(),
        Vec::new(),
        &mut rng(),
        3000,
    );
    assert!((1..=3).all(|k| c.iter().filter(|&&x| x == k).count() > 800));

    let w: Vec<u8> = DistributionScalar::<VecBackend>::collect_col(
        &mut d_weighted(),
        Vec::new(),
        &mut rng(),
        10_000,
    );
    assert_eq!(w.iter().filter(|&&x| x == 1).count(), 0);
    let zeros = w.iter().filter(|&&x| x == 0).count();
    assert!((800..1200).contains(&zeros), "{zeros}");
}

#[test]
fn strings_collect_into_one_buffer() {
    let col: StrCol = DistributionScalar::<VecBackend>::collect_col(
        &mut d_text(),
        StrCol::new(),
        &mut rng(),
        1000,
    );
    assert_eq!(col.len(), 1000);
    for i in 0..1000 {
        let words: Vec<_> = col.get(i).split(' ').collect();
        assert!((1..=3).contains(&words.len()));
        assert!(words.iter().all(|w| WORDS.contains(w)));
    }

    let ids: StrCol = DistributionScalar::<VecBackend>::collect_col(
        &mut d_chars(),
        StrCol::new(),
        &mut rng(),
        100,
    );
    assert!(
        (0..100)
            .all(|i| ids.get(i).len() == 4 && ids.get(i).bytes().all(|b| b == b'X' || b == b'Y'))
    );

    let picked: StrCol = DistributionScalar::<VecBackend>::collect_col(
        &mut d_colors(),
        StrCol::new(),
        &mut rng(),
        100,
    );
    assert!((0..100).all(|i| ["red", "green"].contains(&picked.get(i))));
}

#[test]
fn row_mode_matches_types() {
    let mut it = d_chars().row_iter();
    let s = it.next(&mut rng());
    assert!(s.len() == 4 && s.bytes().all(|b| b == b'X' || b == b'Y'));
    let mut it = d_list_of_str().row_iter();
    assert_eq!(it.next(&mut rng()), vec!["x", "x"]);
}

#[cfg(feature = "arrow")]
#[test]
fn string_column_moves_into_arrow_without_copy() {
    use crate::arrow::{ArrowBackend, DistributionArrow, IntoArrowArray};
    use arrow_array::cast::AsArray;

    let mut col = StrCol::new();
    col.push_str("hello");
    col.push_str("arrow");
    let (offsets_ptr, data_ptr) = (col.offsets().as_ptr(), col.data().as_ptr());
    let a = col.into_array();
    let s = a.as_string::<i32>();
    assert_eq!((s.value(0), s.value(1)), ("hello", "arrow"));
    assert_eq!(s.value_offsets().as_ptr(), offsets_ptr);
    assert_eq!(s.values().as_ptr(), data_ptr);

    // end to end: a (key, text) distribution becomes (Int64, Utf8)
    let mut d = d_keyed_text();
    let b =
        DistributionArrow::<ArrowBackend>::collect_record_batch(&mut d, &mut rng(), 50).unwrap();
    assert_eq!(b.column(1).data_type(), &arrow_schema::DataType::Utf8);
    let t = b.column(1).as_string::<i32>();
    assert!((0..50).all(|i| t.value(i).split(' ').all(|w| WORDS.contains(&w))));
}

#[cfg(feature = "arrow")]
fn d_keyed_text() -> impl Distribution<crate::arrow::ArrowBackend, Item = (i64, String)> {
    (crate::dist::range(0i64, 1), text(WORDS, 1, 2))
}
