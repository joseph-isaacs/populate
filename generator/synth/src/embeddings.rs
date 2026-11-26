//! Vector embeddings as `FixedSizeList<Float32>`: clustered, L2-normalised
//! vectors, the shape of a vector-search corpus.
//!
//! There are `clusters` unit-length centres in `dim` dimensions. Row `r`
//! belongs to a Zipf-skewed cluster (some topics are much bigger than
//! others) and its vector is that centre plus Gaussian noise of total
//! length about `noise`, normalised to length 1, so cosine similarity is
//! high within a cluster and near zero across clusters. The noise is
//! [`FastNormal`]: five draws per hash from 4,096 normal quantiles.

use crate::Shape;
use crate::kernel::{FastNormal, Quantiles, Zipf};
use populate_builtins::{Rows, derive_columns, named, named_all, range, row_numbers};
use populate_core::{Columnar, Distribution};
use populate_sink_arrow::IntoArrowColumns;
use populate_table::Stream;
use std::array::from_fn;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;

/// Embeddings of `DIM` dimensions. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Embeddings<const DIM: usize = 384> {
    inner: Arc<Inner<DIM>>,
}

#[derive(Debug)]
struct Inner<const DIM: usize> {
    seed: u64,
    noise: f32,
    /// One unit-length centre per cluster.
    centres: Vec<[f32; DIM]>,
    cluster: Zipf,
    fast: FastNormal,
    s_cluster: Stream,
    s_noise: Stream,
}

impl Embeddings<384> {
    /// 384 dimensions, 256 clusters, noise 0.6.
    pub fn new(seed: u64) -> Self {
        Self::with_sizes(seed, 256, 0.6)
    }
}

impl<const DIM: usize> Embeddings<DIM> {
    pub fn with_sizes(seed: u64, clusters: usize, noise: f32) -> Self {
        assert!(DIM > 0 && clusters > 0, "DIM and clusters must be > 0");
        let normal = Quantiles::normal(0.0, 1.0);
        let c = Stream::new(seed, "embeddings.centre");
        let centres = (0..clusters)
            .map(|k| {
                let row = c.sub(k as u64);
                let v: [f64; DIM] = from_fn(|j| normal.sample(row.at(j as u64)));
                let n = v.iter().map(|x| x * x).sum::<f64>().sqrt();
                v.map(|x| (x / n) as f32)
            })
            .collect();
        Embeddings {
            inner: Arc::new(Inner {
                seed,
                noise,
                centres,
                cluster: Zipf::new(clusters as u64, 0.8),
                fast: FastNormal::default(),
                s_cluster: Stream::new(seed, "embeddings.cluster"),
                s_noise: Stream::new(seed, "embeddings.noise"),
            }),
        }
    }

    pub fn dim(&self) -> usize {
        DIM
    }

    pub fn seed(&self) -> u64 {
        self.inner.seed
    }
}

impl<const DIM: usize> Inner<DIM> {
    /// The cluster and vector of each of `rows`.
    fn write(&self, rows: &Rows, (cluster, vectors): &mut (Vec<i32>, Vec<[f32; DIM]>)) {
        let first = cluster.len();
        cluster.extend(
            rows.0
                .clone()
                .map(|r| self.cluster.sample(self.s_cluster.at(r)) as i32),
        );
        let scale = self.noise / (DIM as f32).sqrt();
        let hashes = FastNormal::hashes(DIM) as u64;
        let mut noise = [0.0f32; DIM];
        vectors.reserve(rows.len());
        let spare = &mut vectors.spare_capacity_mut()[..rows.len()];
        for (slot, (r, &k)) in spare.iter_mut().zip(rows.0.clone().zip(&cluster[first..])) {
            let centre = &self.centres[k as usize];
            self.fast
                .fill(self.s_noise, r.wrapping_mul(hashes), &mut noise);
            // written in place: a row is too big to build and copy in
            let dst: *mut f32 = slot.as_mut_ptr().cast();
            for (j, (&c, &z)) in centre.iter().zip(&noise).enumerate() {
                // SAFETY: `j < DIM`, inside this row's slot
                unsafe { dst.add(j).write(c + scale * z) };
            }
            // SAFETY: all `DIM` values of the slot were just written
            let v = unsafe { from_raw_parts_mut(dst, DIM) };
            let inv = 1.0 / sum_squares(v).sqrt();
            v.iter_mut().for_each(|x| *x *= inv);
        }
        // SAFETY: the first `rows.len()` spare slots are written
        unsafe { vectors.set_len(vectors.len() + rows.len()) };
    }
}

impl<const DIM: usize> Shape for Embeddings<DIM> {
    fn name(&self) -> &'static str {
        "embeddings"
    }

    fn is_plain(&self, column: &str) -> bool {
        matches!(column, "id" | "embedding")
    }

    fn is_random(&self, column: &str) -> bool {
        column == "embedding"
    }

    fn dist(
        &self,
    ) -> impl Distribution<Item: Columnar<Columns: IntoArrowColumns>> + Clone + Send + Sync + use<DIM>
    {
        let e = self.inner.clone();
        (
            named("id", range(0i64, 1)),
            named_all(
                &["cluster", "embedding"],
                derive_columns::<_, _, (i32, [f32; DIM])>(row_numbers(), move |rows, out| {
                    e.write(rows, out)
                }),
            ),
        )
    }
}

/// `sum(x^2)` in eight independent lanes (a plain `f32` sum is one serial
/// chain the compiler may not reorder).
fn sum_squares(v: &[f32]) -> f32 {
    let mut acc = [0.0f32; 8];
    let mut chunks = v.chunks_exact(8);
    for c in &mut chunks {
        for (a, x) in acc.iter_mut().zip(c) {
            *a += x * x;
        }
    }
    let tail: f32 = chunks.remainder().iter().map(|x| x * x).sum();
    acc.iter().sum::<f32>() + tail
}
