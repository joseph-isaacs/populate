//! Row patterns used in benchmark fixtures.

use crate::collect_batch;
use populate_core::{Columnar, Columns, Distribution, Nullable, Sink};
use rand::Rng;
use std::iter::{FlatMap, RepeatN, repeat_n};

/// Compute each value from its zero-based absolute row number.
pub fn indexed<T: Columnar, F: Fn(usize) -> T>(f: F) -> Indexed<F> {
    Indexed { next: 0, f }
}

#[derive(Clone)]
pub struct Indexed<F> {
    next: usize,
    f: F,
}

impl<F> Indexed<F> {
    fn next_rows<T>(&mut self, n: usize) -> impl Iterator<Item = T> + '_
    where
        F: Fn(usize) -> T,
    {
        let start = self.next;
        self.next = start.checked_add(n).expect("row index overflow");
        (start..self.next).map(&self.f)
    }
}

impl<T: Columnar, F: Fn(usize) -> T> Distribution for Indexed<F> {
    type Item = T;

    fn collect_into(&mut self, cols: &mut Columns<T>, _: &mut impl Rng, n: usize) {
        cols.reserve(n);
        cols.push_n(self.next_rows(n));
    }

    fn collect_as<B: FromIterator<T>>(&mut self, _: &mut impl Rng, n: usize) -> B {
        collect_batch(self.next_rows(n))
    }

    fn skip(&mut self, n: u64) {
        let n = usize::try_from(n).expect("row index fits in usize");
        self.next = self.next.checked_add(n).expect("row index overflow");
    }
}

/// Stream an iterator into columns. Collecting or skipping past its end panics.
/// An iterator owning its RNG preserves draw order across batches and skips.
pub fn from_iter<I: IntoIterator<Item: Columnar>>(rows: I) -> IterRows<I::IntoIter> {
    IterRows(rows.into_iter())
}

#[derive(Clone)]
pub struct IterRows<I>(I);

impl<I: Iterator<Item: Columnar>> Distribution for IterRows<I> {
    type Item = I::Item;

    fn collect_into(&mut self, cols: &mut Columns<I::Item>, _: &mut impl Rng, n: usize) {
        cols.reserve(n);
        cols.push_n((0..n).map(|_| self.0.next().expect("row iterator exhausted")));
    }

    fn skip(&mut self, n: u64) {
        // Map::nth may skip calling a stateful mapper on intervening rows.
        for _ in 0..n {
            self.0.next().expect("row iterator exhausted");
        }
    }
}

/// An expanded run stream. It can be cloned when its source can be cloned.
pub type Runs<I, T> = IterRows<FlatMap<I, RepeatN<T>, fn((T, usize)) -> RepeatN<T>>>;

/// Expand (value, length) pairs, retaining partially consumed runs between batches.
pub fn runs<T: Clone + Columnar, I: IntoIterator<Item = (T, usize)>>(
    source: I,
) -> Runs<I::IntoIter, T> {
    let expand: fn((T, usize)) -> RepeatN<T> = |(value, len)| repeat_n(value, len);
    from_iter(source.into_iter().flat_map(expand))
}

/// Apply a presence mask, consuming one value even for each null row.
pub fn nullable<D: Distribution<Item: Default>, M: Distribution<Item = bool>>(
    values: D,
    present: M,
) -> Masked<D, M> {
    Masked {
        values,
        present,
        scratch: Vec::new(),
    }
}

#[derive(Clone)]
pub struct Masked<D, M> {
    values: D,
    present: M,
    scratch: Vec<bool>,
}

impl<D: Distribution<Item: Default>, M: Distribution<Item = bool>> Distribution for Masked<D, M> {
    type Item = Option<D::Item>;

    fn columns(&self) -> Nullable<Columns<D::Item>> {
        Nullable {
            values: self.values.columns(),
            valid: Default::default(),
        }
    }

    fn collect_into(
        &mut self,
        cols: &mut Nullable<Columns<D::Item>>,
        rng: &mut impl Rng,
        n: usize,
    ) {
        self.values.collect_into(&mut cols.values, rng, n);
        self.present.collect_into(&mut self.scratch, rng, n);
        cols.valid.extend(self.scratch.drain(..));
    }

    fn skip(&mut self, n: u64) {
        self.values.skip(n);
        self.present.skip(n);
    }
}

/// Read a mask row, then draw a value only if that row is present.
/// Both inputs share the caller's RNG in row order. Skipping advances the
/// mask only: without drawing the mask, the number of values is unknown.
pub fn draw_if<M, D>(present: M, values: D) -> DrawIf<M, D>
where
    M: Distribution<Item = bool>,
    D: Distribution<Item: Default>,
{
    DrawIf {
        present,
        values,
        flag: Vec::new(),
    }
}

#[derive(Clone)]
pub struct DrawIf<M, D> {
    present: M,
    values: D,
    flag: Vec<bool>,
}

impl<M: Distribution<Item = bool>, D: Distribution<Item: Default>> Distribution for DrawIf<M, D> {
    type Item = Option<D::Item>;

    fn columns(&self) -> Nullable<Columns<D::Item>> {
        Nullable {
            values: self.values.columns(),
            valid: Default::default(),
        }
    }

    fn collect_into(
        &mut self,
        cols: &mut Nullable<Columns<D::Item>>,
        rng: &mut impl Rng,
        n: usize,
    ) {
        cols.reserve(n);
        for _ in 0..n {
            self.present.collect_into(&mut self.flag, rng, 1);
            if self.flag.pop().expect("one mask row") {
                self.values.collect_into(&mut cols.values, rng, 1);
                cols.valid.push(true);
            } else {
                cols.push(None);
            }
        }
    }

    fn skip(&mut self, n: u64) {
        self.present.skip(n);
    }
}

/// Group consecutive scalar elements into fixed-width rows.
pub fn fixed_size_list<const N: usize, D: Distribution>(elements: D) -> FixedSizeList<N, D> {
    FixedSizeList {
        elements,
        scratch: Vec::new(),
    }
}

#[derive(Clone)]
pub struct FixedSizeList<const N: usize, D: Distribution> {
    elements: D,
    scratch: Vec<D::Item>,
}

impl<const N: usize, T: Copy + Columnar<Columns = Vec<T>>, D: Distribution<Item = T>> Distribution
    for FixedSizeList<N, D>
{
    type Item = [T; N];

    fn collect_into(&mut self, cols: &mut Vec<[T; N]>, rng: &mut impl Rng, n: usize) {
        if N == 0 {
            cols.resize_with(cols.len() + n, || std::array::from_fn(|_| unreachable!()));
            return;
        }
        // A batch of large feature vectors can be hundreds of megabytes.
        // Keep only a block of scalar elements alongside the output.
        const BLOCK_ELEMENTS: usize = 8192;
        let block_rows = (BLOCK_ELEMENTS / N).max(1);
        cols.reserve(n);
        for start in (0..n).step_by(block_rows) {
            let rows = (n - start).min(block_rows);
            self.elements.collect_into(&mut self.scratch, rng, rows * N);
            cols.extend_from_slice(self.scratch.as_chunks::<N>().0);
            self.scratch.clear();
        }
    }

    fn skip(&mut self, n: u64) {
        self.elements
            .skip(n.checked_mul(N as u64).expect("element count overflow"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bernoulli, range, sample, sample_range, shuffled, sorted, sorted_unique};
    use rand::{SeedableRng, distr::StandardUniform, rngs::StdRng};

    #[test]
    fn conditional_draws_match_seeded_rows_across_batches() {
        let mut rng = StdRng::seed_from_u64(5);
        let mut reference = rng.clone();
        let mut dist = draw_if(bernoulli(0.5), sample_range(-1000i32..1000));
        let mut cols = dist.columns();
        let mut expected = Vec::new();
        for n in [0, 1, 64, 0, 173] {
            dist.collect_into(&mut cols, &mut rng, n);
            expected.extend((0..n).map(|_| {
                reference
                    .random_bool(0.5)
                    .then(|| reference.random_range(-1000..1000))
            }));
            let actual: Vec<_> = cols
                .valid
                .iter()
                .zip(&cols.values)
                .map(|(valid, &v)| valid.then_some(v))
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(
                rng.clone().random::<u64>(),
                reference.clone().random::<u64>()
            );
        }
    }

    #[test]
    fn conditional_draws_advance_values_only_on_present_rows() {
        let mut dist = draw_if(
            from_iter([true, false, true, false, true]),
            from_iter([7, 8]),
        );
        dist.skip(1);
        let cols = dist.collect(&mut rand::rng(), 4);
        assert_eq!(cols.values, [0, 7, 0, 8]);
        assert_eq!(
            cols.valid.iter().collect::<Vec<_>>(),
            [false, true, false, true]
        );
    }

    #[test]
    fn masks_and_vectors_append_after_skipping() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = nullable(
            fixed_size_list::<2, _>(range(0i32, 1)),
            indexed(|i| i % 2 == 0),
        );
        let mut cols = dist.collect(&mut rng, 1);
        dist.skip(1);
        dist.collect_into(&mut cols, &mut rng, 2);
        assert_eq!(cols.values, [[0, 1], [4, 5], [6, 7]]);
        assert_eq!(cols.valid.iter().collect::<Vec<_>>(), [true, true, false]);
        assert_eq!(
            fixed_size_list::<0, _>(range(0i32, 1))
                .collect(&mut rng, 4)
                .len(),
            4
        );
    }

    #[test]
    fn collecting_into_a_short_reading_destination_consumes_the_batch() {
        struct First(Option<u32>);
        impl FromIterator<u32> for First {
            fn from_iter<I: IntoIterator<Item = u32>>(iter: I) -> Self {
                Self(iter.into_iter().next())
            }
        }

        let mut rng = StdRng::seed_from_u64(9);
        let mut reference = rng.clone();
        let mut dist = sample::<u32, _>(StandardUniform);
        let expected = dist.collect(&mut reference, 11);
        assert_eq!(dist.collect_as::<First>(&mut rng, 11).0, Some(expected[0]));
        assert_eq!(dist.collect_as::<First>(&mut rng, 0).0, None);
        assert_eq!(rng.random::<u32>(), reference.random::<u32>());
    }

    #[test]
    fn iterator_skip_advances_each_random_draw() {
        let mut source = StdRng::seed_from_u64(9);
        let mut rows = from_iter((0..32).map(move |_| source.random::<u32>()));
        let mut rng = rand::rng();
        let all = rows.clone().collect(&mut rng, 32);
        rows.skip(23);
        assert_eq!(rows.collect(&mut rng, 9), all[23..]);
    }

    #[test]
    fn runs_resume_inside_a_run() {
        let mut rng = rand::rng();
        let mut data = runs([(1i32, 0), (2, 4), (3, 2)]);
        data.skip(1);
        assert_eq!(data.collect(&mut rng, 2), [2, 2]);
        assert_eq!(data.clone().collect(&mut rng, 3), [2, 3, 3]);
        data.skip(3);
        assert!(data.collect(&mut rng, 0).is_empty());
    }

    #[test]
    fn sorted_columns_keep_or_remove_duplicates() {
        let values = [3u64, 0, 3, 1, 0];
        let mut rng = rand::rng();
        assert_eq!(sorted(values).collect(&mut rng, 5), [0, 0, 1, 3, 3]);
        let mut codes = sorted_unique(values);
        codes.skip(1);
        assert_eq!(codes.remaining(), 2);
        assert_eq!(codes.collect(&mut rng, codes.remaining()), [1, 3]);
        assert_eq!(codes.remaining(), 0);
    }

    #[test]
    fn random_leaves_and_shuffled_chunks() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut other = rng.clone();
        let expected: Vec<u32> = (0..12).map(|_| other.random()).collect();
        assert_eq!(
            sample::<u32, _>(StandardUniform).collect(&mut rng, 12),
            expected
        );
        let expected: Vec<i64> = (0..43).map(|_| other.random_range(-19..37)).collect();
        assert_eq!(sample_range(-19i64..37).collect(&mut rng, 43), expected);
        assert_eq!(rng.random::<u64>(), other.random::<u64>());
        assert_eq!(bernoulli(0.0).collect(&mut rng, 7), [false; 7]);
        let mut perm = shuffled(0i32..21, &mut rng);
        let mut all = perm.clone().collect(&mut rng, 21);
        perm.skip(5);
        assert_eq!(perm.collect(&mut rng, 16), all[5..]);
        all.sort_unstable();
        assert_eq!(all, (0..21).collect::<Vec<_>>());
    }
}
