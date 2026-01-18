use populate::{CollectExt, JsonRows, pretty_print, range, list};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // 3-column table: user_id (i64), amount (i32), score (f64)
    let mut table = (
        range(1i64, 1000),
        range(100i32, 10000),
        list(range(0, 10), range(0i32, 100)),
    );

    let json = table.collect::<JsonRows>(&mut rng, 5);
    println!("{}", pretty_print(&json));
}
