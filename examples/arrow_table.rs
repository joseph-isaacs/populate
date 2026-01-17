use arrow::array::{Array, AsArray, ListArray};
use data_comb::collect::Arrow;
use data_comb::{Constant, Distribution, List, Range};
use rand::rngs::StdRng;
use rand::SeedableRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);
    let n = 10;

    // 3-column table: user_id (i64), amount (i32), score (f64)
    let mut table = (
        Range::new(1i64, 100_000),
        Range::new(1i32, 10_000),
        Range::new(0.0f64, 100.0),
    );

    let batch = Arrow::collect3(table.sample(), &mut rng, n);

    // Extract typed arrays
    let user_ids = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>();
    let amounts = batch.column(1).as_primitive::<arrow::datatypes::Int32Type>();
    let scores = batch.column(2).as_primitive::<arrow::datatypes::Float64Type>();

    println!("Generated RecordBatch ({} rows):", batch.num_rows());
    println!("{:>10} {:>10} {:>10}", "user_id", "amount", "score");
    println!("{:-^10} {:-^10} {:-^10}", "", "", "");
    for i in 0..n {
        println!(
            "{:>10} {:>10} {:>10.2}",
            user_ids.value(i),
            amounts.value(i),
            scores.value(i)
        );
    }

    // List column: variable-length arrays of i32
    let mut list_dist = List::new(
        Constant::new(2),
        Range::new(0i32, 100),  // element values
    );

    let list_array = Arrow::collect_list(list_dist.sample(), &mut rng, n);
    let list_array = list_array.as_any().downcast_ref::<ListArray>().unwrap();

    println!("\nGenerated ListArray ({} lists):", list_array.len());
    for i in 0..n {
        let inner = list_array.value(i);
        let values = inner.as_primitive::<arrow::datatypes::Int32Type>();
        let vals: Vec<i32> = (0..values.len()).map(|j| values.value(j)).collect();
        println!("  [{}]: {:?}", i, vals);
    }
}
