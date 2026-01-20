//! Generator traits - the core abstraction for producing values.
//!
//! Two variants:
//! - `Gen<S>`: Fully typed generator (Tier 1)
//! - `ErasedGen`: Type-erased generator (Tier 2)

use rand::RngCore;

use crate::Schema;
use crate::value::{ErasedValue, IntoErased, TypeTag};

/// A typed generator that produces values of schema type S.
///
/// Key design decisions:
/// - `reset()` restores generator to initial state (critical for list generation)
/// - RNG passed explicitly to `generate()` for flexibility with seeded substreams
/// - Generators are stateful - they may track position, counters, etc.
pub trait Gen<S: Schema> {
    /// Generate the next value.
    fn generate(&mut self, rng: &mut dyn RngCore) -> S::Value;

    /// Reset generator to initial state.
    ///
    /// After reset, the generator should produce the same sequence
    /// (given the same RNG state) as a freshly constructed generator.
    /// This is critical for list generation where each list needs
    /// a fresh element generator.
    fn reset(&mut self);

    /// Get the type tag for this generator's output.
    fn type_tag(&self) -> TypeTag {
        S::type_tag()
    }
}

/// A type-erased generator for dynamic/Tier 2 operations.
///
/// Any `Gen<S>` can be converted to `Box<dyn ErasedGen>` via the blanket impl.
pub trait ErasedGen: Send + Sync {
    /// Get the runtime type tag for values produced by this generator.
    fn type_tag(&self) -> TypeTag;

    /// Generate the next value as an erased value.
    fn gen_erased(&mut self, rng: &mut dyn RngCore) -> ErasedValue;

    /// Reset generator to initial state.
    fn reset(&mut self);
}

/// Wrapper to convert a typed Gen<S> into a Box<dyn ErasedGen>.
pub struct TypedGenWrapper<S: Schema, G: Gen<S>> {
    generator: G,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Schema, G: Gen<S>> TypedGenWrapper<S, G> {
    pub fn new(generator: G) -> Self {
        Self {
            generator,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, G> ErasedGen for TypedGenWrapper<S, G>
where
    S: Schema + Send + Sync + 'static,
    G: Gen<S> + Send + Sync,
    S::Value: IntoErased,
{
    fn type_tag(&self) -> TypeTag {
        S::type_tag()
    }

    fn gen_erased(&mut self, rng: &mut dyn RngCore) -> ErasedValue {
        self.generator.generate(rng).into_erased()
    }

    fn reset(&mut self) {
        self.generator.reset();
    }
}

/// Extension trait for converting typed generators to erased.
#[allow(dead_code)]
pub trait GenExt<S: Schema>: Gen<S> + Sized {
    /// Convert this typed generator into a boxed erased generator.
    fn erase(self) -> Box<dyn ErasedGen>
    where
        Self: Send + Sync + 'static,
        S: Send + Sync + 'static,
        S::Value: IntoErased,
    {
        Box::new(TypedGenWrapper::new(self))
    }
}

impl<S: Schema, G: Gen<S>> GenExt<S> for G {}

/// A boxed erased generator that can be cloned.
///
/// This is useful for storing generators in collections where Clone is needed.
pub struct BoxedGen {
    inner: Box<dyn ErasedGen>,
    clone_fn: fn(&dyn ErasedGen) -> Box<dyn ErasedGen>,
}

impl BoxedGen {
    /// Create a new boxed generator from a typed generator.
    pub fn new<S, G>(generator: G) -> Self
    where
        S: Schema + Send + Sync + 'static,
        G: Gen<S> + Clone + Send + Sync + 'static,
        S::Value: IntoErased,
    {
        fn clone_gen<S, G>(erased: &dyn ErasedGen) -> Box<dyn ErasedGen>
        where
            S: Schema + Send + Sync + 'static,
            G: Gen<S> + Clone + Send + Sync + 'static,
            S::Value: IntoErased,
        {
            // SAFETY: We know the type because we created this BoxedGen with the same G
            let wrapper =
                unsafe { &*(erased as *const dyn ErasedGen as *const TypedGenWrapper<S, G>) };
            Box::new(TypedGenWrapper::new(wrapper.generator.clone()))
        }

        BoxedGen {
            inner: Box::new(TypedGenWrapper::new(generator)),
            clone_fn: clone_gen::<S, G>,
        }
    }

    pub fn type_tag(&self) -> TypeTag {
        self.inner.type_tag()
    }

    pub fn gen_erased(&mut self, rng: &mut dyn RngCore) -> ErasedValue {
        self.inner.gen_erased(rng)
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }
}

impl Clone for BoxedGen {
    fn clone(&self) -> Self {
        BoxedGen {
            inner: (self.clone_fn)(self.inner.as_ref()),
            clone_fn: self.clone_fn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, SInt64};

    #[test]
    fn test_typed_gen_erase() {
        let generator = Constant::new(42i64);
        let mut erased = generator.erase();

        assert_eq!(erased.type_tag(), TypeTag::Int64);

        let mut rng = rand::rng();
        let value = erased.gen_erased(&mut rng);
        assert_eq!(value.as_i64(), Some(42));
    }

    #[test]
    fn test_boxed_gen_clone() {
        let generator = Constant::new(42i64);
        let mut boxed = BoxedGen::new::<SInt64, _>(generator);

        let mut rng = rand::rng();
        assert_eq!(boxed.gen_erased(&mut rng).as_i64(), Some(42));

        let mut cloned = boxed.clone();
        assert_eq!(cloned.gen_erased(&mut rng).as_i64(), Some(42));
    }
}
