//! Tuples, as rows and as distributions.
//!
//! - A tuple of row types is a row type: `(A, B)` is stored as `(A`'s
//!   columns`, B`'s columns`)`.
//! - A tuple of sinks takes tuples: pushing `(a, b)` pushes `a` to the first
//!   and `b` to the second.
//! - A tuple of distributions is a distribution of tuples. Collected, it
//!   fills its columns one after the other, each by its own distribution's
//!   `collect_into`, so every column goes at full columnar speed.
//!
//! Up to 12 fields; nest tuples for more.
//!
//! These impls live in this crate because only the crate that defines a
//! trait may implement it for tuples (the orphan rule).

use crate::{Columnar, Columns, Distribution, Sink};
use rand::Rng;

macro_rules! tuple_impls {
    ($($T:ident $S:ident $idx:tt),+) => {
        impl<$($T: Columnar),+> Columnar for ($($T,)+) {
            type Columns = ($($T::Columns,)+);
        }

        impl<$($T, $S: Sink<$T>),+> Sink<($($T,)+)> for ($($S,)+) {
            fn push(&mut self, value: ($($T,)+)) {
                $(self.$idx.push(value.$idx);)+
            }

            fn reserve(&mut self, additional: usize) {
                $(self.$idx.reserve(additional);)+
            }
        }

        impl<$($T: Distribution),+> Distribution for ($($T,)+) {
            type Item = ($($T::Item,)+);

            fn sample(&mut self, rng: &mut impl Rng) -> Self::Item {
                ($(self.$idx.sample(rng),)+)
            }

            fn collect_into(
                &mut self,
                cols: &mut Columns<Self::Item>,
                rng: &mut impl Rng,
                n: usize,
            ) {
                $(self.$idx.collect_into(&mut cols.$idx, rng, n);)+
            }

            fn skip(&mut self, n: u64) {
                $(self.$idx.skip(n);)+
            }
        }
    };
}

tuple_impls!(A SA 0, B SB 1);
tuple_impls!(A SA 0, B SB 1, C SC 2);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6, H SH 7);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6, H SH 7, I SI 8);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6, H SH 7, I SI 8, J SJ 9);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6, H SH 7, I SI 8, J SJ 9, K SK 10);
tuple_impls!(A SA 0, B SB 1, C SC 2, D SD 3, E SE 4, F SF 5, G SG 6, H SH 7, I SI 8, J SJ 9, K SK 10, L SL 11);
