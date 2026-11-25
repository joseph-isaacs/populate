//! Leaves and combinators for tables whose columns come from seekable
//! random streams and depend on each other: [`draws`], [`derive()`],
//! [`derive_columns`], [`children`], [`flat_map`] and [`named`].
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

use crate::RowRefs;
use populate_core::{
    ChildCols, Children, Columnar, Columns, Distribution, RowCount, SeekRng, Sink, TableCols,
    Tables,
};
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
/// `source` is collected column by column
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

    fn collect_into(&mut self, cols: &mut Columns<Out>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        cols.reserve(n);
        cols.push_n(src.row_refs().map(&self.f));
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

/// Columns `f` writes from blocks of `source`'s columns. See
/// [`derive_columns`].
#[derive(Debug, Clone)]
pub struct DeriveColumns<D, F, Out> {
    pub source: D,
    pub f: F,
    _out: PhantomData<fn() -> Out>,
}

/// Columns computed a block at a time: `source` collects `n` rows, then
/// `f(source_columns, out)` appends exactly `n` rows to `out`.
///
/// For columns that share intermediate values (a trip's distance sets its
/// fare, tip and total), so they are worked out once per block and written
/// with plain loops over whole columns, and for column types a writer
/// fills in place (strings into a shared buffer). `Out` is usually a
/// tuple, each field one or more output columns.
pub fn derive_columns<D, F, Out>(source: D, f: F) -> DeriveColumns<D, F, Out>
where
    D: Distribution,
    Out: Columnar,
    F: Fn(&Columns<D::Item>, &mut Columns<Out>),
{
    DeriveColumns {
        source,
        f,
        _out: PhantomData,
    }
}

impl<D, F, Out> Distribution for DeriveColumns<D, F, Out>
where
    D: Distribution,
    Out: Columnar,
    Columns<Out>: RowCount,
    F: Fn(&Columns<D::Item>, &mut Columns<Out>),
{
    type Item = Out;

    fn collect_into(&mut self, cols: &mut Columns<Out>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        let before = cols.rows();
        (self.f)(&src, cols);
        assert_eq!(
            cols.rows() - before,
            n,
            "derive_columns: f must write one row per source row"
        );
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
pub struct FlatMap<D, F, Out> {
    pub source: D,
    pub f: F,
    /// Children to reserve per source row, per table ([`FlatMap::reserving`]).
    pub reserve: Vec<f64>,
    _out: PhantomData<fn() -> Out>,
}

impl<D, F, Out> FlatMap<D, F, Out> {
    /// Reserve `per_source[k]` children per source row in table `k` before
    /// collecting, so the columns are allocated once instead of grown (each
    /// growth copies them). A table's mean children per source row, with a
    /// little headroom, is enough; more are grown into as usual.
    pub fn reserving(mut self, per_source: &[f64]) -> Self {
        self.reserve = per_source.to_vec();
        self
    }
}

/// Child rows: `f(row, out)` appends any number of rows, none included, to
/// the child columns `out` for every row of `source`.
///
/// `Out` is one child table, [`Children<T>`](Children) (`f` writes
/// `out.values`), or several made in the same loop,
/// [`Tables`](populate_core::Tables) of a tuple of `Children` (`f` writes
/// `out.0.values`, `out.1.values`, ...): a sale and its return, decided
/// while making the sale, or a graph's nodes and edges. It is for children
/// whose columns share work (a line's ship date decides its status, its
/// return flag and its receipt date), where a [`children`] per column would
/// repeat it.
///
/// `source` is collected column by column first, then `f` runs once per
/// source row, writing straight into the child columns. Rows are read in
/// place ([`RowRefs`]): a row of a `Vec<T>` column is a `&T`. Names given
/// with [`named_all`] cover every table's columns in order.
pub fn flat_map<Out, D, F>(source: D, f: F) -> FlatMap<D, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    Out: ChildTables,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Columns<Out>),
{
    FlatMap {
        source,
        f,
        reserve: Vec::new(),
        _out: PhantomData,
    }
}

impl<D, F, Out> Distribution for FlatMap<D, F, Out>
where
    D: Distribution,
    Columns<D::Item>: RowRefs,
    Out: ChildTables,
    F: for<'a> Fn(<Columns<D::Item> as RowRefs>::Ref<'a>, &mut Columns<Out>),
{
    type Item = Out;

    fn collect_into(&mut self, cols: &mut Columns<Out>, rng: &mut impl Rng, n: usize) {
        let src = self.source.collect(rng, n);
        if !self.reserve.is_empty() {
            let per: Vec<usize> = self
                .reserve
                .iter()
                .map(|r| (r * n as f64).ceil() as usize)
                .collect();
            Out::reserve_children(cols, &per);
        }
        for row in src.row_refs() {
            (self.f)(row, cols);
        }
        Out::add_sources(cols, n);
    }

    fn skip(&mut self, n: u64) {
        self.source.skip(n);
    }
}

/// The rows [`flat_map`] writes: one child table ([`Children`]), a tuple
/// of them, or [`Tables`] of a tuple of them.
pub trait ChildTables: Columnar {
    /// Count `n` more source rows in every table.
    fn add_sources(cols: &mut Self::Columns, n: usize);

    /// Reserve `children[k]` more rows in table `k`, for as many tables as
    /// `children` covers; returns the rest of `children`.
    fn reserve_children<'a>(cols: &mut Self::Columns, children: &'a [usize]) -> &'a [usize];
}

impl<T: Columnar> ChildTables for Children<T> {
    fn add_sources(cols: &mut ChildCols<Columns<T>>, n: usize) {
        cols.sources += n;
    }

    fn reserve_children<'a>(
        cols: &mut ChildCols<Columns<T>>,
        children: &'a [usize],
    ) -> &'a [usize] {
        match children.split_first() {
            Some((&n, rest)) => {
                cols.values.reserve(n);
                rest
            }
            None => children,
        }
    }
}

macro_rules! child_tables {
    ($($T:ident $i:tt),+) => {
        impl<$($T: ChildTables),+> ChildTables for ($($T,)+) {
            fn add_sources(cols: &mut Self::Columns, n: usize) {
                $($T::add_sources(&mut cols.$i, n);)+
            }

            fn reserve_children<'a>(cols: &mut Self::Columns, children: &'a [usize]) -> &'a [usize] {
                $(let children = $T::reserve_children(&mut cols.$i, children);)+
                children
            }
        }
    };
}

child_tables!(A 0, B 1);
child_tables!(A 0, B 1, C 2);
child_tables!(A 0, B 1, C 2, D 3);
child_tables!(A 0, B 1, C 2, D 3, E 4);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
child_tables!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

impl<T: ChildTables> ChildTables for Tables<T> {
    fn add_sources(cols: &mut TableCols<Columns<T>>, n: usize) {
        T::add_sources(&mut cols.tables, n);
    }

    fn reserve_children<'a>(
        cols: &mut TableCols<Columns<T>>,
        children: &'a [usize],
    ) -> &'a [usize] {
        T::reserve_children(&mut cols.tables, children)
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

    fn collect_into(&mut self, cols: &mut Columns<D::Item>, rng: &mut impl Rng, n: usize) {
        self.inner.collect_into(cols, rng, n)
    }

    fn skip(&mut self, n: u64) {
        self.inner.skip(n)
    }

    fn column_names(&self, out: &mut Vec<String>) {
        out.extend(self.names.iter().cloned());
    }

    fn columns(&self) -> Columns<D::Item> {
        self.inner.columns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constant, range};
    use populate_core::Nullable;
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
        assert_eq!(d.collect(&mut rng(), 1), [[13, 14]]);
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
        assert_eq!(k.collect(&mut rng(), 1).values, [9]);
    }

    #[test]
    fn flat_map_writes_children() {
        let src = (range(1i64, 1), derive(range(0usize, 1), |&i| i % 3));
        let mut f = flat_map::<Children<(i64, i32)>, _, _>(
            src,
            |(k, n): (&i64, &usize), out: &mut ChildCols<(Vec<i64>, Vec<i32>)>| {
                for j in 0..*n {
                    out.values.push((*k, j as i32));
                }
            },
        )
        .reserving(&[1.0]);
        let mut g = f.clone();
        let c = f.collect(&mut rng(), 4);
        assert_eq!(c.values, (vec![2, 3, 3], vec![0, 0, 1]));
        assert_eq!(c.sources, 4);
        g.skip(2);
        assert_eq!(g.collect(&mut rng(), 1).values, (vec![3, 3], vec![0, 1]));
    }

    #[test]
    fn flat_map_writes_several_tables() {
        // per key: `k` lines, and a return for every even line
        type Out = Tables<(Children<(i64, i32)>, Children<i64>)>;
        let mut f = flat_map::<Out, _, _>(range(1i64, 1), |k: &i64, out: &mut Columns<Out>| {
            let (lines, returns) = &mut out.tables;
            for j in 0..*k as i32 {
                lines.values.push((*k, j));
                if j % 2 == 0 {
                    returns.values.push(*k * 10 + j as i64);
                }
            }
        });
        let mut g = f.clone();
        let (lines, returns) = f.collect(&mut rng(), 3).tables;
        assert_eq!(
            lines.values,
            (vec![1, 2, 2, 3, 3, 3], vec![0, 0, 1, 0, 1, 2])
        );
        assert_eq!(returns.values, [10, 20, 30, 32]);
        assert_eq!((lines.sources, returns.sources), (3, 3));
        g.skip(2);
        let (l, r) = g.collect(&mut rng(), 1).tables;
        assert_eq!(l.values, (vec![3, 3, 3], vec![0, 1, 2]));
        assert_eq!(r.values, [30, 32]);
    }

    #[test]
    fn derive_columns_writes_blocks() {
        let mut d = derive_columns::<_, _, (i64, Option<i32>)>(
            range(10i64, 1),
            |src: &Vec<i64>, out: &mut (Vec<i64>, Nullable<Vec<i32>>)| {
                out.0.extend(src.iter().map(|x| x * 2));
                for &x in src {
                    out.1.push((x % 2 == 0).then_some(x as i32));
                }
            },
        );
        let mut e = d.clone();
        let (a, b) = d.collect(&mut rng(), 3);
        assert_eq!(a, [20, 22, 24]);
        assert_eq!(b.values, [10, 0, 12]);
        assert_eq!(b.valid.iter().collect::<Vec<_>>(), [true, false, true]);
        e.skip(1);
        let (a, b) = e.collect(&mut rng(), 1);
        assert_eq!((a, b.valid.get(0)), (vec![22], false));
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
