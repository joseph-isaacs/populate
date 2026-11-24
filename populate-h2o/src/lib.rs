//! The H2O db-benchmark data sets, byte-identical to the CSV the R datagen
//! scripts (`groupby-datagen.R`, `join-datagen.R`) write, on all cores.
//!
//! [`GroupBy`] is one table, [`Join`] four ([`JoinTable`]s). Each is a
//! `populate_table` `Chunks` (Arrow batches, Parquet) and `TextChunks`
//! (the scripts' CSV, on all cores).
//!
//! ```
//! use populate_h2o::GroupBy;
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

pub use groupby::GroupBy;
pub use join::{Join, JoinTable};

/// The scripts' `set.seed(108)`.
pub const SEED: u32 = 108;

/// The scripts' `pretty_sci(x)`: R's `as.character(x)` of a whole number,
/// shortened to `<first digit>e<digits - 1>` (`100` → `1e2`) or, when R
/// prints it in scientific notation, with the exponent's `+` and leading
/// zeros dropped (`1e+07` → `1e7`).
pub fn pretty_sci(x: f64) -> String {
    let fixed = format!("{x:.0}");
    // R's scientific form: mantissa with trailing zeros dropped, e+XX
    let sci = {
        let s = format!("{x:e}"); // e.g. "1.5e7"
        let (m, e) = s.split_once('e').expect("exponent");
        let e: i32 = e.parse().expect("exponent");
        format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
    };
    if fixed.len() <= sci.len() {
        format!("{}e{}", &fixed[..1], fixed.len() - 1)
    } else {
        let (m, e) = sci.split_once("e+").expect("positive exponent");
        format!("{m}e{}", e.parse::<i32>().expect("exponent"))
    }
}

#[cfg(test)]
mod tests {
    use super::pretty_sci;

    #[test]
    fn pretty_names() {
        for (x, s) in [
            (1e7, "1e7"),
            (1e2, "1e2"),
            (2.0, "2e0"),
            (10.0, "1e1"),
            (1e4, "1e4"),
            (1e5, "1e5"),
            (1e9, "1e9"),
            (5e6, "5e6"),
            (1.5e7, "1.5e7"),
        ] {
            assert_eq!(pretty_sci(x), s, "{x}");
        }
    }
}
