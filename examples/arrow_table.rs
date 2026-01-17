use arrow::util::pretty::{pretty_format_columns, print_batches};
use populate::{CollectExt, ListArrayW, RecordBatchW, constant, list, range};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // 3-column table: user_id (i64), amount (i32), score (f64)
    let mut table = (
        range(1i64, 100_000),
        range(1i32, 10_000),
        range(0.0f64, 100.0),
    );
    let batch = table.collect::<RecordBatchW>(&mut rng, 10);
    println!("RecordBatch:");
    print_batches(&[batch]).unwrap();

    // List column: variable-length arrays of i32
    let list_array = list(constant(2usize), range(0i32, 100)).collect::<ListArrayW>(&mut rng, 10);
    println!("\nListArray:");
    println!(
        "{}",
        pretty_format_columns("list", &[Arc::new(list_array)]).unwrap()
    );
}
