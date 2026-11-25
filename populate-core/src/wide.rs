//! Rows whose width is chosen when the distribution is built.
//!
//! A tuple's width is fixed by its type. Some tables only know theirs at
//! run time (a column per configured field, say), so a `Vec` of
//! distributions is a distribution of [`Wide`] rows: one value per
//! element, each stored in its own column, as if the elements were the
//! fields of a tuple. [`Distribution::columns`] gives the columns their
//! width before the first row arrives.

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

impl<T, C: Sink<T>> Sink<Wide<T>> for WideCols<C> {
    fn push(&mut self, row: Wide<T>) {
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

    fn sample(&mut self, rng: &mut impl Rng) -> Self::Item {
        Wide(self.iter_mut().map(|d| d.sample(rng)).collect())
    }

    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize) {
        assert_eq!(
            cols.0.len(),
            self.len(),
            "columns from `Distribution::columns` have the distribution's width"
        );
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
        fn sample(&mut self, _: &mut impl Rng) -> i64 {
            self.0 += 1;
            self.0
        }
    }

    #[test]
    fn collects_one_column_per_element() {
        let mut d = vec![Count(0), Count(10), Count(20)];
        let mut rng = SmallRng::seed_from_u64(0);
        let c = d.collect(&mut rng, 2);
        assert_eq!(c.0, vec![vec![1, 2], vec![11, 12], vec![21, 22]]);
        assert_eq!(d.sample(&mut rng), Wide(vec![3, 13, 23]));
        let mut cols = d.columns();
        cols.push(Wide(vec![0, 0, 0]));
        assert_eq!(cols.0.len(), 3);
    }
}
