use populate::{CollectExt, Cols, list, range};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // 3-column table: user_id (i64), amount (i32), tags (list of i32)
    let mut table = (
        range(1i64, 100_000),
        range(1i32, 10_000),
        list(range(0, 10), range(0i32, 100)),
    );
    let table: (Vec<i64>, Vec<i32>, Vec<Vec<i32>>) = table.collect::<Cols>(&mut rng, 10);

    println!("Table {:?}", table);
}
