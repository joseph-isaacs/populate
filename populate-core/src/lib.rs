//! Core traits for `populate`: [`traits::Distribution`], [`traits::SampleIter`],
//! [`traits::Sink`], [`traits::Backend`], the HList sink plumbing in [`hlist`],
//! the tuple-of-distributions combinators in [`tuple`], and the columnar
//! string sink in [`string`].
//!
//! Tuple impls live here rather than in `populate-builtins` because
//! `impl Distribution for (D1, D2)` is only allowed in the crate that defines
//! `Distribution` (orphan rule).

pub mod hlist;
pub mod string;
pub mod traits;
pub mod tuple;
