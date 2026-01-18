use arrow_cast::pretty::print_batches;
use populate::{CollectExt, RecordBatchW, list, range};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // 3-column table: user_id (i64), amount (i32), score (f64)
    let mut table = (
        range(1i64, 100_000),
        range(1i32, 10_000),
        list(range(0, 10), range(0i32, 100)),
    );
    let batch = table.collect::<RecordBatchW>(&mut rng, 10);
    println!("RecordBatch:");
    print_batches(&[batch]).unwrap();
}
