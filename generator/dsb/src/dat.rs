//! dsdgen's `.dat` text format. The tables write it from their columns
//! ([`populate_table::text::write_columns`]); [`write_batch`] writes it from
//! an Arrow batch of a table.
//!
//! One line per row: every field followed by `|`, NULL as an empty field,
//! decimals with exactly `scale` digits (`-0.05`), dates as `YYYY-MM-DD`.
//! This is ~10x faster than formatting dsbgen's rows with `Display`, and
//! the test suite checks it against that `Display` output for every table.

use arrow_array::RecordBatch;
use populate_table::text::write_batch_with;

pub use populate_table::text::Encoding;

/// Append `batch` to `out` as `.dat` lines. dsdgen keeps GMT offsets as
/// decimal(5,2) but prints them with no decimals (`-5`).
pub fn write_batch(batch: &RecordBatch, encoding: Encoding, out: &mut Vec<u8>) {
    write_batch_with(
        batch,
        encoding,
        |f| f.name().ends_with("gmt_offset").then_some(0),
        out,
    )
}
