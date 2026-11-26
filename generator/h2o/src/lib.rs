//! The H2O db-benchmark data sets, byte-identical to the CSV the R datagen
//! scripts (`groupby-datagen.R`, `join-datagen.R`) write, on all cores.
//!
//! [`GroupBy`] is one table, [`Join`] four ([`JoinTable`]s). Each is a
//! `populate_table` `Chunks` (Arrow batches, Parquet) and `TextChunks`
//! (the scripts' CSV, on all cores).
//!
//! ```
//! use populate_gen_h2o::GroupBy;
//!
//! let g = GroupBy::new(1_000, 10, 0, false);
//! assert_eq!(g.file_name(), "G1_1e3_1e1_0_0.csv");
//! let csv = g.write_csv(Vec::new(), 0).unwrap();
//! assert!(csv.starts_with(b"id1,id2,id3,id4,id5,id6,v1,v2,v3\nid010,id009,"));
//! ```

mod cols;
pub mod fwrite;
mod fwrite_tables;
pub mod groupby;
pub mod join;
pub mod rng;
pub mod values;

pub use groupby::GroupBy;
pub use join::{Join, JoinTable};

/// The scripts' `set.seed(108)`.
pub const SEED: u32 = 108;

/// The scripts' `pretty_sci(x)` of an integer `N` or `K`: R prints an
/// integer in full, which the script shortens to `<first digit>e<digits -
/// 1>` (`100` → `1e2`, `15000000` → `1e7`).
pub fn pretty_sci(x: u64) -> String {
    let s = x.to_string();
    format!("{}e{}", &s[..1], s.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::pretty_sci;

    #[test]
    fn pretty_names() {
        for (x, s) in [
            (10_000_000, "1e7"),
            (100, "1e2"),
            (2, "2e0"),
            (10, "1e1"),
            (100_000, "1e5"),
            (1_000_000_000, "1e9"),
            (15_000_000, "1e7"),
            (1500, "1e3"),
        ] {
            assert_eq!(pretty_sci(x), s, "{x}");
        }
    }
}
