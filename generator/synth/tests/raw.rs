use populate_core::RowCount;
use populate_gen_synth::{Embeddings, Features, Hits, Shape, Taxi};

#[test]
fn native_shapes_expose_all_types_and_chunking() {
    let taxi = Taxi::new(42).raw(12).batch(0, 4);
    assert_eq!(taxi.rows(), 4);
    assert!(taxi.0.2[0].0 >= taxi.0.1[0].0);
    assert!(taxi.1.0[0].is_finite());
    let hits = Hits::new(42).raw(12).batch(0, 4);
    assert_eq!(hits.rows(), 4);
    hits.1.2.with_str(0, |s| assert!(s.starts_with("http")));
    let features = Features::<4, 2>::with_sizes(42, 0.1).raw(12).batch(0, 4);
    assert_eq!(features.0, [0, 1, 2, 3]);
    features
        .2
        .with_str(0, |s| assert!(["train", "valid", "test"].contains(&s)));
    let raw = Embeddings::<8>::with_sizes(42, 4, 0.1)
        .raw(12)
        .chunk_rows(5)
        .batch_rows(3);
    let c = raw.batch(0, 4);
    assert_eq!(c.0, [0, 1, 2, 3]);
    for vector in &c.1.1 {
        let norm: f32 = vector.iter().map(|x| x * x).sum();
        assert!((norm - 1.0).abs() < 1e-5);
    }
    let mut ids = Vec::new();
    raw.for_each_batch(2, |c| ids.extend(c.0));
    assert_eq!(ids, (0..12).collect::<Vec<_>>());
}

#[cfg(feature = "formats")]
mod adapters {
    use super::*;
    use populate_sink_arrow::{into_record_batch_with, schema_of};
    use std::sync::Arc;

    #[test]
    fn all_shapes_match_their_adapters() {
        macro_rules! check {
            ($shape:expr) => {{
                let shape = $shape;
                let raw = shape.raw(16);
                let schema = Arc::new(schema_of(raw.dist()));
                let actual = into_record_batch_with(&schema, raw.batch(0, 16)).unwrap();
                assert_eq!(actual, shape.table(16).collect());
            }};
        }
        check!(Taxi::new(42));
        check!(Hits::new(42));
        check!(Features::<4, 2>::with_sizes(42, 0.1));
        check!(Embeddings::<8>::with_sizes(42, 4, 0.1));
    }
}
