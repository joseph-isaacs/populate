//! Leaves and combinators for tables whose columns come from seekable
//! random streams and depend on each other: [`draws`], [`derive()`],
//! [`children`], [`flat_map`] and [`named`].
//!
//! ```
//! use populate_builtins::{children, derive, draws, named};
//! use populate_core::{Date32, Distribution, SeekRng};
//!
//! /// A toy seekable stream: draw `k` is `k * 7 mod 100`.
//! #[derive(Clone)]
//! struct Toy(u64);
//!
//! impl SeekRng for Toy {
//!     type Draw = u32;
//!     fn next_draw(&mut self) -> u32 {
//!         self.0 += 1;
//!         (self.0 * 7 % 100) as u32
//!     }
//!     fn advance(&mut self, n: u64) {
//!         self.0 += n;
//!     }
//! }
//!
//! // per order: a date, and up to 3 lines, each with a quantity
//! let orders = (
//!     named("day", derive(draws::<_, 1>(Toy(0)), |&[x]| Date32(x as i32))),
//!     named("lines", derive(draws::<_, 1>(Toy(0)), |&[x]| (x % 3 + 1) as usize)),
//! );
//! let qty = children(
//!     derive(draws::<_, 1>(Toy(0)), |&[x]| (x % 3 + 1) as usize),
//!     derive(draws::<_, 3>(Toy(5)), |xs| xs.map(|x| x as i64)),
//! );
//! let mut rng = rand::rng();
//! let (_, lines) = orders.clone().collect(&mut rng, 4);
//! let q = named("qty", qty).collect(&mut rng, 4);
//! assert_eq!(q.values.len(), lines.iter().sum::<usize>());
//! ```

use crate::with_column::{ReadRows, RowRefs};
use populate_core::{ChildCols, Children, Columnar, Columns, Distribution, SeekRng, Sink};
use rand::Rng;
use std::marker::PhantomData;

// ============================================================================
// Draws
// ============================================================================

/// `K` draws of `stream` per row, as a `[Draw; K]`. See [`draws`].
#[derive(Debug, Clone)]
pub struct Draws<S, const K: usize> {
    pub stream: S,
    /// Draws reserved per row: `K`, or more for a row that uses only its
    /// first `K`.
    pub per_row: u64,
}

/// The next `K` draws of `stream` per row: a stream that reserves `K` draws
/// per row, as dbgen's do. The caller's RNG is not used, so the rows are a
/// function of the stream and the row number alone.
///
/// Collected, the draws of all rows come from one [`SeekRng::fill`];
/// skipped, the stream jumps `K * n` draws ([`SeekRng::advance`]).
pub fn draws<S: SeekRng, const K: usize>(stream: S) -> Draws<S, K> {
    draws_reserving(stream, K as u64)
}

/// The first `K` of `per_row` draws of `stream` reserved per row: for a
/// row that uses a few of the draws its stream sets aside for it (dbgen
/// reserves one draw per color for a part name and uses 5).
pub fn draws_reserving<S: SeekRng, const K: usize>(stream: S, per_row: u64) -> Draws<S, K> {
    assert!(per_row >= K as u64, "a row uses {K} draws of its {per_row}");
    Draws { stream, per_row }
}

impl<S: SeekRng, const K: usize> Distribution for Draws<S, K>
where
    S::Draw: Default,
{
    type Item = [S::Draw; K];

    fn sample(&mut self, _rng: &mut impl Rng) -> [S::Draw; K] {
        let row = std::array::from_fn(|_| self.stream.next_draw());
        self.stream.advance(self.per_row - K as u64);
        row
    }

    fn collect_into(&mut self, cols: &mut Vec<[S::Draw; K]>, _rng: &mut impl Rng, n: usize) {
        let start = cols.len();
        cols.resize(start + n, [S::Draw::default(); K]);
        if self.per_row == K as u64 {
            self.stream.fill(cols[start..].as_flattened_mut());
        } else {
            for row in &mut cols[start..] {
                self.stream.fill(row);
                self.stream.advance(self.per_row - K as u64);
            }
        }
    }

    fn skip(&mut self, n: u64) {
        self.stream.advance(n * self.per_row);
    }
}

// ============================================================================
// Derive
// ============================================================================

/// `f` of every row of `source`, collected columnar. See [`derive()`].
#[derive(Debug, Clone)]
pub struct Derive<D, F, Out> {
    pub source: D,
    pub f: F,
    /// Fixed here rather than read off `F`, so a source's `Item` is a
    /// plain type wherever it is used.
    _out: PhantomData<fn() -> Out>,
}

/// `f` of every row of `source`: a column computed from others.
///
/// Unlike [`map`](crate::map), `source` is collected column by column
/// first (a tuple of streams fills each stream's block in one go), then
/// `f` runs in one pass over those columns. The source columns are
/// scratch: only `f`'s output is kept. Rows are read in place
/// ([`RowRefs`]): a row of a `Vec<T>` column is a `&T`, so a wide row
/// (blocks of draws from several streams) is never copied out.
pub fn derive<D, F, Out>(source: D, f: F) -> Derive<D, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>) -> Out,
{
    Derive {
        source,
        f,
        _out: PhantomData,
    }
}

impl<D, F, Out> Distribution for Derive<D, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>) -> Out,
    Out: Columnar,
{
    type Item = Out;

    /// One source row, collected (so read the way `collect` reads it).
    fn sample(&mut self, rng: &mut impl Rng) -> Out {
        let src = self.source.collect(rng, 1);
        (self.f)(src.row_refs().next().expect("one row"))
    }

    fn collect_into(&mut self, cols: &mut Columns<Out>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        cols.reserve(n);
        cols.push_n(src.row_refs().map(&self.f));
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

// ============================================================================
// Children
// ============================================================================

/// The first `count` of each source row's `K` slots. See [`children`].
#[derive(Debug, Clone)]
pub struct ChildSlots<C, E> {
    pub counts: C,
    pub slots: E,
}

/// Child rows: for every source row, the first `counts` of the `K` values
/// `slots` makes for it (`K` is the most children a row can have).
///
/// This is how dbgen generates line items: every line stream reserves 7
/// draws per order and an order with 4 lines uses the first 4. Each
/// column of a child table is one `children` over the same counts, so a
/// tuple of them is the child table, its rows the children.
pub fn children<C, E>(counts: C, slots: E) -> ChildSlots<C, E> {
    ChildSlots { counts, slots }
}

impl<C, E, T, const K: usize> Distribution for ChildSlots<C, E>
where
    C: Distribution<Item = usize>,
    E: Distribution<Item = [T; K]>,
    T: Columnar + Copy,
{
    type Item = Children<T>;

    fn sample(&mut self, rng: &mut impl Rng) -> Children<T> {
        let n = self.counts.sample(rng);
        Children(self.slots.sample(rng)[..n].to_vec())
    }

    fn collect_into(&mut self, cols: &mut ChildCols<Columns<T>>, rng: &mut impl Rng, n: usize) {
        let counts = self.counts.collect(rng, n);
        let slots = self.slots.collect(rng, n);
        cols.values.reserve(counts.iter().sum());
        for (s, &c) in slots.iter().zip(&counts) {
            cols.values.push_n(s[..c].iter().copied());
        }
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        self.counts.skip(n);
        self.slots.skip(n);
    }

    fn column_names(&self, out: &mut Vec<String>) {
        self.slots.column_names(out);
    }
}

// ============================================================================
// FlatMap
// ============================================================================

/// Child rows that `f` writes for each source row. See [`flat_map`].
#[derive(Debug, Clone)]
pub struct FlatMap<D, C, F, Out> {
    pub source: D,
    pub count: C,
    pub f: F,
    _out: PhantomData<fn() -> Out>,
}

/// Child rows: `f(row, children)` appends `count(row)` rows of `Out` to the
/// child columns for every row of `source`. For children whose columns
/// share work (a line's ship date decides its status, its return flag and
/// its receipt date), where a [`children`] per column would repeat it.
///
/// `source` is collected column by column first; then the child columns
/// are reserved for exactly the rows the counts add up to, and `f` runs
/// once per source row, writing straight into them. Rows are read in
/// place ([`RowRefs`]): a row of a `Vec<T>` column is a `&T`.
pub fn flat_map<D, Out, C, F>(source: D, count: C, f: F) -> FlatMap<D, C, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    Out: Columnar,
    C: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>) -> usize,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Columns<Out>),
{
    FlatMap {
        source,
        count,
        f,
        _out: PhantomData,
    }
}

impl<D, C, F, Out> Distribution for FlatMap<D, C, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    Out: Columnar,
    Columns<Out>: ReadRows<Row = Out>,
    C: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>) -> usize,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Columns<Out>),
{
    type Item = Children<Out>;

    /// One source row, collected (so read the way `collect` reads it).
    fn sample(&mut self, rng: &mut impl Rng) -> Children<Out> {
        let src = self.source.collect(rng, 1);
        let mut out = Columns::<Out>::default();
        for row in src.row_refs() {
            (self.f)(row, &mut out);
        }
        Children(out.rows_from(0).collect())
    }

    fn collect_into(&mut self, cols: &mut ChildCols<Columns<Out>>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        let total = src.row_refs().map(&self.count).sum();
        let start = cols.values.rows();
        cols.values.reserve(total);
        for row in src.row_refs() {
            (self.f)(row, &mut cols.values);
        }
        debug_assert_eq!(
            cols.values.rows() - start,
            total,
            "flat_map: children written differ from their counts"
        );
        cols.sources += n;
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

// ============================================================================
// Named
// ============================================================================

/// `inner`, with its columns named. See [`named`].
#[derive(Debug, Clone)]
pub struct Named<D> {
    pub names: Vec<String>,
    pub inner: D,
}

/// `inner` with its column named `name` (for an `inner` of one column).
/// Names are how a distribution carries its schema: with the column types
/// its rows are collected as, they are the schema
/// (`populate_sink_arrow::schema_of`).
pub fn named<D: Distribution>(name: &str, inner: D) -> Named<D> {
    Named {
        names: vec![name.to_owned()],
        inner,
    }
}

/// `inner` with its columns named `names`, in order (for an `inner` whose
/// rows are tuples, one name per field).
pub fn named_all<D: Distribution>(names: &[&str], inner: D) -> Named<D> {
    Named {
        names: names.iter().map(|&n| n.to_owned()).collect(),
        inner,
    }
}

impl<D: Distribution> Distribution for Named<D> {
    type Item = D::Item;

    fn sample(&mut self, rng: &mut impl Rng) -> D::Item {
        self.inner.sample(rng)
    }

    fn collect_into(&mut self, cols: &mut Columns<D::Item>, rng: &mut impl Rng, n: usize) {
        self.inner.collect_into(cols, rng, n)
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n)
    }

    fn column_names(&self, out: &mut Vec<String>) {
        out.extend(self.names.iter().cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constant, range};
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    /// Draw `k` is `k`.
    #[derive(Clone)]
    struct Count(u64);

    impl SeekRng for Count {
        type Draw = u64;
        fn next_draw(&mut self) -> u64 {
            self.0 += 1;
            self.0
        }
        fn advance(&mut self, n: u64) {
            self.0 += n;
        }
    }

    fn rng() -> SmallRng {
        SmallRng::seed_from_u64(0)
    }

    #[test]
    fn draws_reserve_and_skip() {
        let mut d = draws_reserving::<_, 2>(Count(0), 3);
        d.skip(2);
        assert_eq!(d.collect(&mut rng(), 2), [[7, 8], [10, 11]]);
        assert_eq!(d.sample(&mut rng()), [13, 14]);
        let mut e = draws::<_, 2>(Count(0));
        assert_eq!(e.collect(&mut rng(), 2), [[1, 2], [3, 4]]);
    }

    #[test]
    fn children_keep_the_first_count_slots() {
        let counts = derive(range(0usize, 1), |&i| i % 3);
        let mut k = children(counts, derive(draws::<_, 2>(Count(0)), |x| *x));
        let c = k.collect(&mut rng(), 4);
        assert_eq!(c.sources, 4);
        // row 0: none, row 1: [3], row 2: [5, 6], row 3: none
        assert_eq!(c.values, [3, 5, 6]);
        assert_eq!(k.sample(&mut rng()).0, [9]);
    }

    #[test]
    fn flat_map_writes_counted_children() {
        let src = (range(1i64, 1), derive(range(0usize, 1), |&i| i % 3));
        let mut f = flat_map::<_, (i64, i32), _, _>(
            src,
            |(_, n): (&i64, &usize)| *n,
            |(k, n): (&i64, &usize), out: &mut (Vec<i64>, Vec<i32>)| {
                for j in 0..*n {
                    out.push((*k, j as i32));
                }
            },
        );
        let mut g = f.clone();
        let c = f.collect(&mut rng(), 4);
        assert_eq!(c.values, (vec![2, 3, 3], vec![0, 0, 1]));
        g.skip(2);
        assert_eq!(g.sample(&mut rng()).0, [(3, 0), (3, 1)]);
    }

    #[test]
    fn names_follow_the_tuple() {
        let d = (
            named("a", constant(1i32)),
            named_all(&["b", "c"], (constant(2i64), constant(3i64))),
        );
        let mut names = Vec::new();
        d.column_names(&mut names);
        assert_eq!(names, ["a", "b", "c"]);
    }
}
