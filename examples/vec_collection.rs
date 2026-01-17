use populate::{Col, CollectExt, range};
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);

    // Collect into Vec
    let mut dist = range(0i32, 100);
    let data: Vec<i32> = dist.collect::<Col>(&mut rng, 10);
    println!("Vec collection: {:?}", data);
}
