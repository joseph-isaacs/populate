mod constants;

use constants::NATIONS;
use populate_builtins::{cycle, inc, unnest};
use populate_core::Distribution;

pub fn nations() -> impl Distribution<(i64, &'static str, i64)> {
    (inc(0i64), unnest(cycle(NATIONS)))
}
