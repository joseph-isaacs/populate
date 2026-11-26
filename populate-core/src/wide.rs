//! Rows whose width is chosen when the distribution is built.
//!
//! A tuple's width is fixed by its type. Some tables only know theirs at
//! run time (a column per configured field, say), so a `Vec` of
//! distributions is a distribution of [`Wide`] rows: one value per
//! element, each stored in its own column, as if the elements were the
//! fields of a tuple. [`Distribution::columns`] gives the columns their
//! width before the first row arrives, so a schema can be read off them;
//! columns made by `Default` take their width from the first rows.
//!
//! A wrapper whose rows are its inner distribution's (`named`, tuples,
//! lists) forwards `columns`. One whose rows a closure makes (`derive`,
//! `map`, `flat_map`) can't know a `Wide` width before it runs, so put the
//! `Vec` outside it, not inside.

use crate::{Columnar, Columns, Distribution, Sink};
use rand::Rng;

/// One value per column of a run-time-width group.
#[derive(Debug, Clone, PartialEq)]
pub struct Wide<T>(pub Vec<T>);

/// The columns of [`Wide`] rows: one column per value.
#[derive(Debug, Clone, Default)]
pub struct WideCols<C>(pub Vec<C>);

impl<T: Columnar> Columnar for Wide<T> {
    type Columns = WideCols<T::Columns>;
}

impl<T, C: Sink<T> + Default> Sink<Wide<T>> for WideCols<C> {
    fn push(&mut self, row: Wide<T>) {
        if self.0.is_empty() {
            self.0.resize_with(row.0.len(), C::default);
        }
        assert_eq!(
            row.0.len(),
            self.0.len(),
            "a row of {} values for {} columns",
            row.0.len(),
            self.0.len()
        );
        for (c, v) in self.0.iter_mut().zip(row.0) {
            c.push(v);
        }
    }

    fn reserve(&mut self, additional: usize) {
        for c in &mut self.0 {
            c.reserve(additional);
        }
    }
}

/// Each element is a column (or group of columns), in order.
impl<D: Distribution> Distribution for Vec<D> {
    type Item = Wide<D::Item>;

    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize) {
        if cols.0.is_empty() {
            *cols = self.columns();
        }
        assert_eq!(cols.0.len(), self.len(), "columns of another width");
        for (d, c) in self.iter_mut().zip(&mut cols.0) {
            d.collect_into(c, rng, n);
        }
    }

    fn skip(&mut self, n: u64) {
        for d in self {
            d.skip(n);
        }
    }

    fn column_names(&self, out: &mut Vec<String>) {
        for d in self {
            d.column_names(out);
        }
    }

    fn columns(&self) -> Columns<Self::Item> {
        WideCols(self.iter().map(Distribution::columns).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    #[derive(Clone)]
    struct Count(i64);

    impl Distribution for Count {
        type Item = i64;
        fn collect_into(&mut self, cols: &mut Vec<i64>, _: &mut impl Rng, n: usize) {
            cols.extend(self.0 + 1..=self.0 + n as i64);
            self.0 += n as i64;
        }
        fn skip(&mut self, n: u64) {
            self.0 += n as i64;
        }
    }

    #[test]
    fn collects_one_column_per_element() {
        let mut d = vec![Count(0), Count(10), Count(20)];
        let mut rng = SmallRng::seed_from_u64(0);
        let c = d.collect(&mut rng, 2);
        assert_eq!(c.0, vec![vec![1, 2], vec![11, 12], vec![21, 22]]);
        assert_eq!(d.collect(&mut rng, 1).0, vec![vec![3], vec![13], vec![23]]);
        let mut cols = d.columns();
        cols.push(Wide(vec![0, 0, 0]));
        assert_eq!(cols.0.len(), 3);
    }

    #[test]
    fn default_columns_take_their_width() {
        let mut rng = SmallRng::seed_from_u64(0);
        let mut d = vec![Count(0), Count(10)];
        let mut cols = WideCols::<Vec<i64>>::default();
        d.collect_into(&mut cols, &mut rng, 1);
        let mut pushed = WideCols::<Vec<i64>>::default();
        pushed.push(Wide(vec![2, 12]));
        assert_eq!(cols.0, vec![vec![1], vec![11]]);
        assert_eq!(pushed.0, vec![vec![2], vec![12]]);
    }

    #[test]
    fn skips_and_empty() {
        let mut rng = SmallRng::seed_from_u64(0);
        let mut d = vec![Count(0), Count(10)];
        d.skip(5);
        assert_eq!(d.collect(&mut rng, 1).0, vec![vec![6], vec![16]]);
        let mut none: Vec<Count> = Vec::new();
        assert!(none.collect(&mut rng, 3).0.is_empty());
    }
}
