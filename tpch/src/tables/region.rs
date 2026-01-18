use populate_builtins::{cycle, inc};
use populate_core::Distribution;

pub fn regions() -> impl Distribution<(i64, &'static str)> {
    (
        inc(0i64),
        cycle(["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]),
    )
}
