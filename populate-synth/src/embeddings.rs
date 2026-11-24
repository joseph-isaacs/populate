//! Vector embeddings as `FixedSizeList<Float32>`: clustered, L2-normalised
//! vectors, the shape of a vector-search corpus.
//!
//! There are `clusters` unit-length centres in `dim` dimensions. Row `r`
//! belongs to a Zipf-skewed cluster (some topics are much bigger than
//! others) and its vector is that centre plus Gaussian noise of total
//! length about `noise`, normalised to length 1, so cosine similarity is
//! high within a cluster and near zero across clusters.

use crate::Shape;
use crate::kernel::{Quantiles, Zipf, prim_ref};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use populate_table::Stream;
use std::sync::Arc;

/// Embeddings. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Embeddings {
    seed: u64,
    dim: usize,
    noise: f32,
    schema: SchemaRef,
    item: FieldRef,
    /// `clusters x dim`, row-major, each row unit length.
    centres: Vec<f32>,
    cluster: Zipf,
    normal: Quantiles,
    s_cluster: Stream,
    s_noise: Stream,
}

impl Embeddings {
    /// 384 dimensions, 256 clusters, noise 0.6.
    pub fn new(seed: u64) -> Self {
        Self::with_sizes(seed, 384, 256, 0.6)
    }

    pub fn with_sizes(seed: u64, dim: usize, clusters: usize, noise: f32) -> Self {
        assert!(dim > 0 && clusters > 0, "dim and clusters must be > 0");
        let normal = Quantiles::normal(0.0, 1.0);
        let c = Stream::new(seed, "embeddings.centre");
        let mut centres = Vec::with_capacity(clusters * dim);
        for k in 0..clusters {
            let row = c.sub(k as u64);
            let v: Vec<f64> = (0..dim).map(|j| normal.sample(row.at(j as u64))).collect();
            let n = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            centres.extend(v.iter().map(|x| (x / n) as f32));
        }
        let item = Arc::new(Field::new_list_field(DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("cluster", DataType::Int32, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(item.clone(), dim as i32),
                false,
            ),
        ]));
        Embeddings {
            seed,
            dim,
            noise,
            schema,
            item,
            centres,
            cluster: Zipf::new(clusters as u64, 0.8),
            normal,
            s_cluster: Stream::new(seed, "embeddings.cluster"),
            s_noise: Stream::new(seed, "embeddings.noise"),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl Shape for Embeddings {
    fn name(&self) -> &'static str {
        "embeddings"
    }

    fn is_plain(&self, column: &str) -> bool {
        matches!(column, "id" | "embedding")
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn batch(&self, start: u64, len: usize) -> RecordBatch {
        let dim = self.dim;
        let rows = || start..start + len as u64;
        let cluster: Vec<i32> = rows()
            .map(|r| self.cluster.sample(self.s_cluster.at(r)) as i32)
            .collect();
        let scale = self.noise / (dim as f32).sqrt();
        let mut values: Vec<f32> = Vec::with_capacity(len * dim);
        for (r, &k) in rows().zip(&cluster) {
            let centre = &self.centres[k as usize * dim..(k as usize + 1) * dim];
            let base = r.wrapping_mul(dim as u64);
            let at = values.len();
            values.extend(centre.iter().enumerate().map(|(j, &c)| {
                c + scale * self.normal.sample(self.s_noise.at(base + j as u64)) as f32
            }));
            let v = &mut values[at..];
            let inv = 1.0 / v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter_mut().for_each(|x| *x *= inv);
        }
        let embedding = FixedSizeListArray::new(
            self.item.clone(),
            dim as i32,
            Arc::new(Float32Array::new(ScalarBuffer::from(values), None)),
            None,
        );
        let cols: Vec<ArrayRef> = vec![
            prim_ref::<Int64Type>(rows().map(|r| r as i64).collect(), None),
            prim_ref::<Int32Type>(cluster, None),
            Arc::new(embedding),
        ];
        RecordBatch::try_new(self.schema.clone(), cols).expect("columns match schema")
    }
}
