//! `Table::col` output for a fixed seed must not change: the hash below was
//! taken before the core `Distribution` trait was reworked, and guards any
//! later change to how distributions draw from the RNG.

use populate_builtins::{chars, choose, constant, list, map, range, text, uniform, weighted};
use populate_table::Table;
use populate_table::hash_str;

static WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "a-very-long-word-indeed"];

fn table() -> Table {
    Table::new("stable", 100)
        .seed(7)
        .key("id")
        .col("n", uniform(-5i64, 1_000_000))
        .col("small", uniform(1i32, 7))
        .col("f", uniform(0.0f64, 1.0))
        .col("pick", choose(&["north", "south", "east"]))
        .col("w", weighted(&[(1i32, 10), (2, 30), (3, 5)]))
        .col("heavy", weighted(&[(1i64, 5000), (2, 30), (3, 5)]))
        .col("note", text(WORDS, 0, 4))
        .col("code", chars(b"ABCDEF0123", 3, 9))
        .col("seq", range(10i32, 3))
        .col("tags", list(uniform(0usize, 3), uniform(0i64, 99)))
        .col(
            "pairs",
            list(constant(2usize), (range(0i32, 1), choose(&["x", "y"]))),
        )
        .col("mapped", map(uniform(0i64, 9), |x| x * x))
        .col("nullable", uniform(0i32, 9))
        .nullable(0.3)
}

fn hash(t: &Table, batch_size: usize) -> u64 {
    let mut all = String::new();
    for b in t.batches(batch_size) {
        all.push_str(&format!("{:?}\n", b.columns()));
    }
    hash_str(&all)
}

#[test]
fn col_output_is_stable() {
    let t = table();
    assert_eq!(
        (hash(&t, 16), hash(&t, 7)),
        (9386204772377903847, 11379804772069039474)
    );
}
