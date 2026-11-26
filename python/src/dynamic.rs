//! Distributions whose types are chosen at run time.
//!
//! populate's distributions are generic: the row type of a tuple of
//! combinators is known when it is compiled. A caller that builds a table
//! from a description (a Python script, a config file) only knows it when
//! it runs. This module closes that gap without giving up columnar
//! collection:
//!
//! - [`BoxDist<T>`] is any `Distribution<Item = T>` behind a pointer. It is
//!   itself a `Distribution`, so boxed and unboxed combinators mix freely,
//!   and collecting a box still hands the whole column to the distribution
//!   inside (one virtual call per column, not per row).
//! - [`AnyDist`] is a `BoxDist` of one of the row types an Arrow column can
//!   be made from here, so a column's type is a value.
//! - [`DynTable`] is named `AnyDist` columns as a [`Chunks`] source, so the
//!   multi-core Arrow and Parquet drivers of `populate-table` run it.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_core::{Columnar, Columns, Date32, Distribution, Nullable};
use populate_sink_arrow::{IntoArrowArray, IntoArrowColumns};
use populate_table::{Chunks, mix64};
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use std::sync::Arc;

/// The object-safe part of [`Distribution`], for one row type `T`.
///
/// Every `Distribution + Clone + Send + Sync` is one. `sample` takes the
/// caller's RNG as a trait object; `collect` takes a concrete [`SmallRng`],
/// so the inner distribution's per-row draws are not virtual calls.
pub trait DynDist<T: Columnar>: Send + Sync {
    fn sample_dyn(&mut self, rng: &mut dyn RngCore) -> T;
    fn collect_dyn(&mut self, cols: &mut Columns<T>, rng: &mut SmallRng, n: usize);
    fn skip_dyn(&mut self, n: u64);
    fn columns_dyn(&self) -> Columns<T>;
    fn clone_box(&self) -> Box<dyn DynDist<T>>;
}

impl<D> DynDist<D::Item> for D
where
    D: Distribution + Clone + Send + Sync + 'static,
{
    fn sample_dyn(&mut self, mut rng: &mut dyn RngCore) -> D::Item {
        self.sample(&mut rng)
    }

    fn collect_dyn(&mut self, cols: &mut Columns<D::Item>, rng: &mut SmallRng, n: usize) {
        self.collect_into(cols, rng, n)
    }

    fn skip_dyn(&mut self, n: u64) {
        self.skip(n)
    }

    fn columns_dyn(&self) -> Columns<D::Item> {
        self.columns()
    }

    fn clone_box(&self) -> Box<dyn DynDist<D::Item>> {
        Box::new(self.clone())
    }
}

/// A distribution of `T` whose type is erased.
pub struct BoxDist<T: Columnar>(Box<dyn DynDist<T>>);

impl<T: Columnar> BoxDist<T> {
    pub fn new<D>(dist: D) -> Self
    where
        D: Distribution<Item = T> + Clone + Send + Sync + 'static,
    {
        BoxDist(Box::new(dist))
    }
}

impl<T: Columnar> Clone for BoxDist<T> {
    fn clone(&self) -> Self {
        BoxDist(self.0.clone_box())
    }
}

impl<T: Columnar> Distribution for BoxDist<T> {
    type Item = T;

    fn sample(&mut self, rng: &mut impl Rng) -> T {
        self.0.sample_dyn(rng)
    }

    /// The box draws from its own [`SmallRng`], seeded from `rng`: a
    /// generic RNG can't cross the trait object, and one seed per column
    /// is cheaper than a virtual call per draw.
    fn collect_into(&mut self, cols: &mut Columns<T>, rng: &mut impl Rng, n: usize) {
        let mut own = SmallRng::from_rng(rng);
        self.0.collect_dyn(cols, &mut own, n)
    }

    fn skip(&mut self, n: u64) {
        self.0.skip_dyn(n)
    }

    fn columns(&self) -> Columns<T> {
        self.0.columns_dyn()
    }
}

// ============================================================================
// Adapters the dynamic builders need
// ============================================================================

/// `f` of each value of a distribution of plain scalars, collected a
/// column at a time.
pub struct MapVec<D: Distribution, U> {
    inner: D,
    f: fn(D::Item) -> U,
}

impl<D: Distribution + Clone, U> Clone for MapVec<D, U> {
    fn clone(&self) -> Self {
        MapVec {
            inner: self.inner.clone(),
            f: self.f,
        }
    }
}

impl<D: Distribution, U> MapVec<D, U> {
    pub fn new(inner: D, f: fn(D::Item) -> U) -> Self {
        MapVec { inner, f }
    }
}

impl<D, T, U> Distribution for MapVec<D, U>
where
    D: Distribution<Item = T>,
    T: Columnar<Columns = Vec<T>>,
    U: Columnar<Columns = Vec<U>>,
{
    type Item = U;

    fn sample(&mut self, rng: &mut impl Rng) -> U {
        (self.f)(self.inner.sample(rng))
    }

    fn collect_into(&mut self, cols: &mut Vec<U>, rng: &mut impl Rng, n: usize) {
        let src = self.inner.collect(rng, n);
        cols.extend(src.into_iter().map(self.f));
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n)
    }
}

/// `&'static str` rows as `String` rows: both collect into a `StrCol`, so
/// collecting passes the column straight through.
#[derive(Clone)]
pub struct Owned<D>(pub D);

impl<D: Distribution<Item = &'static str>> Distribution for Owned<D> {
    type Item = String;

    fn sample(&mut self, rng: &mut impl Rng) -> String {
        self.0.sample(rng).to_owned()
    }

    fn collect_into(&mut self, cols: &mut Columns<String>, rng: &mut impl Rng, n: usize) {
        self.0.collect_into(cols, rng, n)
    }

    fn skip(&mut self, n: u64) {
        self.0.skip(n)
    }
}

/// Each row of `inner` is missing with probability `rate`. A missing row
/// still holds the value `inner` drew for it, under a cleared validity bit.
#[derive(Clone)]
pub struct NullRate<D> {
    inner: D,
    rate: f64,
}

impl<D> NullRate<D> {
    pub fn new(inner: D, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "nullable: rate in 0..=1");
        NullRate { inner, rate }
    }
}

impl<D> Distribution for NullRate<D>
where
    D: Distribution,
    D::Item: Default,
{
    type Item = Option<D::Item>;

    fn sample(&mut self, rng: &mut impl Rng) -> Option<D::Item> {
        let v = self.inner.sample(rng);
        (rng.random::<f64>() >= self.rate).then_some(v)
    }

    fn collect_into(
        &mut self,
        cols: &mut Nullable<Columns<D::Item>>,
        rng: &mut impl Rng,
        n: usize,
    ) {
        self.inner.collect_into(&mut cols.values, rng, n);
        let rate = self.rate;
        cols.valid
            .extend((0..n).map(|_| rng.random::<f64>() >= rate));
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n)
    }

    fn columns(&self) -> Nullable<Columns<D::Item>> {
        Nullable {
            values: self.inner.columns(),
            valid: Default::default(),
        }
    }
}

// ============================================================================
// AnyDist
// ============================================================================

macro_rules! any_dist {
    ($($v:ident => $t:ty, $name:literal;)*) => {
        /// A distribution of one Arrow column, its row type chosen at run
        /// time.
        #[derive(Clone)]
        pub enum AnyDist {
            $($v(BoxDist<$t>),)*
        }

        $(impl From<BoxDist<$t>> for AnyDist {
            fn from(d: BoxDist<$t>) -> Self {
                AnyDist::$v(d)
            }
        })*

        impl AnyDist {
            /// The row type, as a short name (`int64`, `list<string>`, ...).
            pub fn type_name(&self) -> &'static str {
                match self { $(AnyDist::$v(_) => $name,)* }
            }

            pub fn skip(&mut self, n: u64) {
                match self { $(AnyDist::$v(d) => d.skip(n),)* }
            }

            /// Arrow type and nullability of the column.
            pub fn data_type(&self) -> (DataType, bool) {
                let mut out = Vec::with_capacity(1);
                match self { $(AnyDist::$v(d) => d.columns().column_types(&mut out),)* }
                out.pop().expect("one column")
            }

            /// The next `n` rows as one Arrow array.
            pub fn collect_array(&mut self, rng: &mut SmallRng, n: usize) -> ArrayRef {
                match self { $(AnyDist::$v(d) => IntoArrowArray::into_array(Distribution::collect(d, rng, n)),)* }
            }
        }
    };
}

any_dist! {
    I32 => i32, "int32";
    I64 => i64, "int64";
    F64 => f64, "float64";
    Bool => bool, "bool";
    Str => String, "string";
    Date => Date32, "date32";
    NullI32 => Option<i32>, "int32?";
    NullI64 => Option<i64>, "int64?";
    NullF64 => Option<f64>, "float64?";
    NullBool => Option<bool>, "bool?";
    NullStr => Option<String>, "string?";
    NullDate => Option<Date32>, "date32?";
    ListI64 => Vec<i64>, "list<int64>";
    ListF64 => Vec<f64>, "list<float64>";
    ListStr => Vec<String>, "list<string>";
}

impl AnyDist {
    /// The same column with each row missing with probability `rate`;
    /// `None` for a column that is already nullable or a list.
    pub fn nullable(self, rate: f64) -> Option<AnyDist> {
        use AnyDist::*;
        fn n<T: Columnar + Default + Send + Sync + 'static>(
            d: BoxDist<T>,
            rate: f64,
        ) -> BoxDist<Option<T>> {
            BoxDist::new(NullRate::new(d, rate))
        }
        Some(match self {
            I32(d) => NullI32(n(d, rate)),
            I64(d) => NullI64(n(d, rate)),
            F64(d) => NullF64(n(d, rate)),
            Bool(d) => NullBool(n(d, rate)),
            Str(d) => NullStr(n(d, rate)),
            Date(d) => NullDate(n(d, rate)),
            _ => return None,
        })
    }

    /// Integers as days since 1970-01-01; `None` for other columns.
    pub fn date(self) -> Option<AnyDist> {
        Some(match self {
            AnyDist::I32(d) => AnyDist::Date(BoxDist::new(MapVec::new(d, Date32))),
            AnyDist::I64(d) => {
                AnyDist::Date(BoxDist::new(MapVec::new(d, |v: i64| Date32(v as i32))))
            }
            d @ AnyDist::Date(_) => d,
            _ => return None,
        })
    }

    /// An integer column as list lengths (negative values are 0); `None`
    /// for other columns.
    pub fn lengths(self) -> Option<BoxDist<usize>> {
        Some(match self {
            AnyDist::I32(d) => BoxDist::new(MapVec::new(d, |v: i32| v.max(0) as usize)),
            AnyDist::I64(d) => BoxDist::new(MapVec::new(d, |v: i64| v.max(0) as usize)),
            _ => return None,
        })
    }
}

// ============================================================================
// DynTable
// ============================================================================

/// Named [`AnyDist`] columns, `rows` rows of them, as a [`Chunks`] source.
///
/// Rows `first..first + n` of column `j` are the column cloned, skipped
/// `first` rows and collected with an RNG seeded from `(seed, j, first)`,
/// as `DistTable` does for a whole row: a batch is the same rows on any
/// thread, so the output depends on `seed` and `batch_rows`, not on the
/// thread count.
#[derive(Clone)]
pub struct DynTable {
    cols: Vec<AnyDist>,
    rows: u64,
    seed: u64,
    schema: SchemaRef,
    chunk_rows: u64,
    batch_rows: u64,
}

impl DynTable {
    /// `rows` rows of `cols`, in chunks of 256Ki rows and batches of 64Ki.
    pub fn new(cols: Vec<(String, AnyDist)>, rows: u64, seed: u64) -> Self {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, d)| {
                let (t, null) = d.data_type();
                Field::new(name, t, null)
            })
            .collect();
        DynTable {
            cols: cols.into_iter().map(|(_, d)| d).collect(),
            rows,
            seed,
            schema: Arc::new(Schema::new(fields)),
            chunk_rows: 256 * 1024,
            batch_rows: 64 * 1024,
        }
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Rows per chunk (one Parquet row group).
    pub fn chunk_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "chunk_rows must be > 0");
        self.chunk_rows = n;
        self
    }

    /// Rows per batch.
    pub fn batch_rows(mut self, n: u64) -> Self {
        assert!(n > 0, "batch_rows must be > 0");
        self.batch_rows = n;
        self
    }

    /// Rows `first..first + n` as one batch.
    pub fn batch(&self, first: u64, n: u64) -> RecordBatch {
        let n = usize::try_from(n).expect("batch fits in memory");
        let arrays = self
            .cols
            .iter()
            .enumerate()
            .map(|(j, col)| {
                let mut d = col.clone();
                d.skip(first);
                let key = mix64(self.seed ^ mix64(j as u64)) ^ first;
                let mut rng = SmallRng::seed_from_u64(mix64(key));
                d.collect_array(&mut rng, n)
            })
            .collect();
        RecordBatch::try_new(self.schema.clone(), arrays).expect("columns match the schema")
    }
}

impl Chunks for DynTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_chunks(&self) -> u64 {
        self.rows.div_ceil(self.chunk_rows)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        let end = ((i + 1) * self.chunk_rows).min(self.rows);
        let mut at = i * self.chunk_rows;
        while at < end {
            let n = self.batch_rows.min(end - at);
            emit(self.batch(at, n));
            at += n;
        }
    }
}

// ============================================================================
// A distribution written for this crate
// ============================================================================

/// Normally distributed `f64`s (Box–Muller). populate has no normal
/// distribution; this is the whole of what one takes, and the Python
/// module exposes it as `normal`.
#[derive(Clone)]
pub struct Normal {
    pub mean: f64,
    pub std: f64,
}

impl Distribution for Normal {
    type Item = f64;

    fn sample(&mut self, rng: &mut impl Rng) -> f64 {
        // 1 - u is in (0, 1], so the log is finite
        let u: f64 = 1.0 - rng.random::<f64>();
        let v: f64 = rng.random();
        self.mean + self.std * (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }

    fn skip(&mut self, _n: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use populate_builtins::{choose, list, range, uniform};
    use populate_table::chunks::for_each_batch;

    fn table() -> DynTable {
        let cols = vec![
            ("id".into(), AnyDist::I64(BoxDist::new(range(0i64, 1)))),
            (
                "x".into(),
                AnyDist::I64(BoxDist::new(uniform(0i64, 9)))
                    .nullable(0.5)
                    .unwrap(),
            ),
            (
                "s".into(),
                AnyDist::Str(BoxDist::new(Owned(choose(&["a", "bb"])))),
            ),
            (
                "l".into(),
                AnyDist::ListI64(BoxDist::new(list(
                    AnyDist::I64(BoxDist::new(uniform(0i64, 3)))
                        .lengths()
                        .unwrap(),
                    BoxDist::new(range(0i64, 1)),
                ))),
            ),
            (
                "d".into(),
                AnyDist::I32(BoxDist::new(uniform(0i32, 100)))
                    .date()
                    .unwrap(),
            ),
        ];
        DynTable::new(cols, 1000, 7).chunk_rows(300).batch_rows(70)
    }

    #[test]
    fn schema_comes_from_the_columns() {
        let s = table().schema();
        let types: Vec<_> = s.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(types[0], DataType::Int64);
        assert!(s.field(1).is_nullable() && !s.field(0).is_nullable());
        assert_eq!(types[2], DataType::Utf8);
        assert!(matches!(types[3], DataType::List(_)));
        assert_eq!(types[4], DataType::Date32);
    }

    #[test]
    fn any_thread_count_gives_the_same_rows() {
        let t = table();
        let run = |threads| {
            let mut v = Vec::new();
            for_each_batch(&t, threads, |b| v.push(b));
            v
        };
        let one = run(1);
        assert_eq!(one, run(4));
        assert_eq!(one.iter().map(|b| b.num_rows()).sum::<usize>(), 1000);
        // the key counts across chunks: each batch skipped to its first row
        let ids: Vec<i64> = one
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        assert_eq!(ids, (0..1000).collect::<Vec<_>>());
        assert!(one[0].column(1).null_count() > 0);
    }

    #[test]
    fn a_box_is_a_distribution() {
        let mut d = (
            BoxDist::new(range(5i64, 2)),
            BoxDist::new(Normal {
                mean: 0.0,
                std: 1.0,
            }),
        );
        let (a, b) = d.collect(&mut SmallRng::seed_from_u64(1), 4);
        assert_eq!(a, [5, 7, 9, 11]);
        assert_eq!(b.len(), 4);
    }
}
