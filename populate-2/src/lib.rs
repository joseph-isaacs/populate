use rand::Rng;

pub trait Distribution<I> {
    type Iter: SampleIter<Item = I>;

    type Cols;

    fn sample(&mut self) -> Self::Iter;

    fn cols(self) -> Self::Cols;
}

pub trait SampleIter {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;

    fn reset(&mut self);
}
pub struct Constant<T> {
    v: T,
}
pub struct ConstantIter<T> {
    v: T,
}

impl<T: Copy> SampleIter for ConstantIter<T> {
    type Item = T;

    fn next(&mut self, _rng: &mut impl Rng) -> Self::Item {
        self.v
    }

    fn reset(&mut self) {}
}

impl<T: Copy> Distribution<T> for Constant<T> {
    type Iter = ConstantIter<T>;

    type Cols = Constant<T>;
    fn sample(&mut self) -> Self::Iter {
        ConstantIter { v: self.v }
    }

    fn cols(self) -> Self::Cols {
        self
    }
}

#[derive(Clone)]
pub struct TupleIter2<I1, I2>(pub I1, pub I2);

impl<I1: SampleIter, I2: SampleIter> SampleIter for TupleIter2<I1, I2> {
    type Item = (I1::Item, I2::Item);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        (self.0.next(rng), self.1.next(rng)) // Produces pair each call
    }

    fn reset(&mut self) {
        self.0.reset();
        self.1.reset();
    }
}

// Distribution<(T1, T2) for D1 :: Distribution<T1>, D2 :: Distribution<T2>

impl<T1, T2, D1, D2> Distribution<(T1, T2)> for (D1, D2)
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Iter = TupleIter2<D1::Iter, D2::Iter>;
    type Cols = (D1::Cols, D2::Cols); // Tuple of column iters!

    fn sample(&mut self) -> Self::Iter {
        TupleIter2(self.0.sample(), self.1.sample())
    }
    fn cols(self) -> Self::Cols {
        (self.0.cols(), self.1.cols()) // Split!
    }
}
struct MapCol<T1, T2>(pub T1, pub T2);

struct MapColIter<T1, T2>(pub T1, pub T2);

impl<T1, T2> SampleIter for MapCol<T1, T2> {
    type Item = (T1, T2);

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        todo!()
    }

    fn reset(&mut self) {
        todo!()
    }
}

impl<T1, T2, D1, D2> Distribution<(T1, T2)> for MapCol<D1, D2>
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
{
    type Iter = TupleIter2<D1::Iter, D2::Iter>;
    type Cols = MapCol<D1::Cols, D2::Cols>;

    fn sample(&mut self) -> Self::Iter {
        todo!()
    }

    fn cols(self) -> Self::Cols {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Constant, Distribution, SampleIter};

    fn new_dist() -> impl Distribution<(i32, i32)> {
        let d1 = Constant { v: 1 };
        let d2 = Constant { v: 2 };

        (d1, d2)
    }

    #[test]
    fn it_works() {
        let mut d = new_dist();

        let rng = &mut rand::rng();
        let mut iter = d.sample();
        for _ in 0..10 {
            println!("{:?}", iter.next(rng))
        }

        let mut d = new_dist();

        let exact = {
            let d1 = Constant { v: 1 };
            let d2 = Constant { v: 2 };

            (d1, d2)
        };

        let (d1, d2) = exact.cols();
        for _ in 0..10 {
            println!("{:?}", iter.next(rng))
        }
    }
}
