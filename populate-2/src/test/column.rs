use crate::column::{Bitmap, NullableCol, StrCol, TakeRows};
use crate::traits::Sink;

#[test]
fn bitmap_filled_and_pushed_agree_across_word_boundaries() {
    for len in [0, 1, 63, 64, 65, 128, 130] {
        let filled = Bitmap::filled(len, true);
        let mut pushed = Bitmap::new();
        for _ in 0..len {
            pushed.push(true);
        }
        assert_eq!(filled, pushed, "len {len}");
        assert_eq!(filled.count_ones(), len);
        assert_eq!(Bitmap::filled(len, false).count_ones(), 0);
    }
}

#[test]
fn nullable_strings_keep_values_and_validity() {
    let mut col: NullableCol<StrCol> = NullableCol::default();
    for v in [Some("ab"), None, Some(""), Some("c")] {
        col.push(v.map(str::to_string));
    }
    assert_eq!(col.len(), 4);
    assert_eq!(col.null_count(), 1);
    assert_eq!(
        col.values.iter().collect::<Vec<_>>(),
        vec!["ab", "", "", "c"]
    );
    let r = col.take_rows(&[3, 1, 0]);
    assert_eq!(r.values.iter().collect::<Vec<_>>(), vec!["c", "", "ab"]);
    assert_eq!(
        (r.is_valid(0), r.is_valid(1), r.is_valid(2)),
        (true, false, true)
    );
}
