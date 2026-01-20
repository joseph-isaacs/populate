//! Built-in generators for common data patterns.

use std::ops::Range as StdRange;

use rand::RngCore;
use rand::distr::uniform::SampleUniform;
use rand::distr::{Distribution, Uniform};

use crate::generator::Gen;
use crate::schema::{SBool, SFloat32, SFloat64, SInt32, SInt64, SList, SString, Schema};

/// Helper to sample from a uniform distribution using dyn RngCore.
fn sample_range<T: SampleUniform + PartialOrd>(rng: &mut dyn RngCore, range: StdRange<T>) -> T {
    Uniform::new(range.start, range.end)
        .expect("valid range")
        .sample(rng)
}

/// Helper to sample a random bool.
fn sample_bool(rng: &mut dyn RngCore) -> bool {
    rng.next_u32() & 1 == 1
}

/// A generator that always produces the same value.
#[derive(Debug, Clone)]
pub struct Constant<T> {
    value: T,
}

impl<T> Constant<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

impl Gen<SInt64> for Constant<i64> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> i64 {
        self.value
    }

    fn reset(&mut self) {}
}

impl Gen<SInt32> for Constant<i32> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> i32 {
        self.value
    }

    fn reset(&mut self) {}
}

impl Gen<SFloat64> for Constant<f64> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> f64 {
        self.value
    }

    fn reset(&mut self) {}
}

impl Gen<SFloat32> for Constant<f32> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> f32 {
        self.value
    }

    fn reset(&mut self) {}
}

impl Gen<SBool> for Constant<bool> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> bool {
        self.value
    }

    fn reset(&mut self) {}
}

impl Gen<SString> for Constant<String> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> String {
        self.value.clone()
    }

    fn reset(&mut self) {}
}

impl Gen<SString> for Constant<&'static str> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> String {
        self.value.to_string()
    }

    fn reset(&mut self) {}
}

/// A generator that produces uniformly distributed random values in a range.
#[derive(Debug, Clone)]
pub struct Range<T> {
    range: StdRange<T>,
}

impl<T> Range<T> {
    pub fn new(range: StdRange<T>) -> Self {
        Self { range }
    }
}

impl Gen<SInt64> for Range<i64> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> i64 {
        sample_range(rng, self.range.clone())
    }

    fn reset(&mut self) {}
}

impl Gen<SInt32> for Range<i32> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> i32 {
        sample_range(rng, self.range.clone())
    }

    fn reset(&mut self) {}
}

impl Gen<SFloat64> for Range<f64> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> f64 {
        sample_range(rng, self.range.clone())
    }

    fn reset(&mut self) {}
}

impl Gen<SFloat32> for Range<f32> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> f32 {
        sample_range(rng, self.range.clone())
    }

    fn reset(&mut self) {}
}

/// A generator that produces monotonically incrementing values.
#[derive(Debug, Clone)]
pub struct Inc<T> {
    current: T,
    start: T,
    step: T,
}

impl<T: Clone> Inc<T> {
    pub fn new(start: T, step: T) -> Self {
        Self {
            current: start.clone(),
            start,
            step,
        }
    }
}

impl Inc<i64> {
    pub fn from(start: i64) -> Self {
        Self::new(start, 1)
    }
}

impl Inc<i32> {
    pub fn from_i32(start: i32) -> Self {
        Self::new(start, 1)
    }
}

macro_rules! impl_inc {
    ($t:ty, $schema:ty) => {
        impl Gen<$schema> for Inc<$t> {
            fn generate(&mut self, _rng: &mut dyn RngCore) -> $t {
                let value = self.current;
                self.current += self.step;
                value
            }

            fn reset(&mut self) {
                self.current = self.start;
            }
        }
    };
}

impl_inc!(i64, SInt64);
impl_inc!(i32, SInt32);

/// A generator that cycles through a fixed set of values.
#[derive(Debug, Clone)]
pub struct Cycle<T, const N: usize> {
    values: [T; N],
    index: usize,
}

impl<T, const N: usize> Cycle<T, N> {
    pub fn new(values: [T; N]) -> Self {
        Self { values, index: 0 }
    }
}

macro_rules! impl_cycle {
    ($t:ty, $schema:ty) => {
        impl<const N: usize> Gen<$schema> for Cycle<$t, N> {
            fn generate(&mut self, _rng: &mut dyn RngCore) -> $t {
                let value = self.values[self.index].clone();
                self.index = (self.index + 1) % N;
                value
            }

            fn reset(&mut self) {
                self.index = 0;
            }
        }
    };
}

impl_cycle!(i64, SInt64);
impl_cycle!(i32, SInt32);
impl_cycle!(f64, SFloat64);
impl_cycle!(f32, SFloat32);
impl_cycle!(bool, SBool);
impl_cycle!(String, SString);

// Special case for &'static str producing String
impl<const N: usize> Gen<SString> for Cycle<&'static str, N> {
    fn generate(&mut self, _rng: &mut dyn RngCore) -> String {
        let value = self.values[self.index].to_string();
        self.index = (self.index + 1) % N;
        value
    }

    fn reset(&mut self) {
        self.index = 0;
    }
}

/// A generator that picks a random element from a slice.
#[derive(Debug, Clone)]
pub struct PickOne<'a, T> {
    values: &'a [T],
}

impl<'a, T> PickOne<'a, T> {
    pub fn new(values: &'a [T]) -> Self {
        Self { values }
    }
}

impl Gen<SInt64> for PickOne<'_, i64> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> i64 {
        let idx = sample_range(rng, 0..self.values.len());
        self.values[idx]
    }

    fn reset(&mut self) {}
}

impl Gen<SString> for PickOne<'_, &'static str> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> String {
        let idx = sample_range(rng, 0..self.values.len());
        self.values[idx].to_string()
    }

    fn reset(&mut self) {}
}

impl Gen<SString> for PickOne<'_, String> {
    fn generate(&mut self, rng: &mut dyn RngCore) -> String {
        let idx = sample_range(rng, 0..self.values.len());
        self.values[idx].clone()
    }

    fn reset(&mut self) {}
}

/// A generator that produces random booleans.
#[derive(Debug, Clone, Default)]
pub struct RandomBool;

impl RandomBool {
    pub fn new() -> Self {
        Self
    }
}

impl Gen<SBool> for RandomBool {
    fn generate(&mut self, rng: &mut dyn RngCore) -> bool {
        sample_bool(rng)
    }

    fn reset(&mut self) {}
}

/// A generator that produces lists of random length.
#[derive(Debug, Clone)]
pub struct List<L, E> {
    length_gen: L,
    element_gen: E,
    initial_element_gen: E,
}

impl<L, E: Clone> List<L, E> {
    pub fn new(length_gen: L, element_gen: E) -> Self {
        Self {
            length_gen,
            initial_element_gen: element_gen.clone(),
            element_gen,
        }
    }
}

impl<L, E, ES> Gen<SList<ES>> for List<L, E>
where
    L: Gen<SInt64>,
    E: Gen<ES> + Clone,
    ES: Schema,
{
    fn generate(&mut self, rng: &mut dyn RngCore) -> Vec<ES::Value> {
        let len = self.length_gen.generate(rng) as usize;
        // Reset element generator for each list to ensure consistent behavior.
        self.element_gen.reset();
        (0..len).map(|_| self.element_gen.generate(rng)).collect()
    }

    fn reset(&mut self) {
        self.length_gen.reset();
        self.element_gen = self.initial_element_gen.clone();
    }
}

// Convenience functions for creating generators.

/// Create a constant generator.
pub fn constant<T>(value: T) -> Constant<T> {
    Constant::new(value)
}

/// Create a range generator.
pub fn range<T>(range: StdRange<T>) -> Range<T> {
    Range::new(range)
}

/// Create an incrementing generator starting from a value.
pub fn inc(start: i64) -> Inc<i64> {
    Inc::from(start)
}

/// Create an incrementing generator with custom step.
pub fn inc_by(start: i64, step: i64) -> Inc<i64> {
    Inc::new(start, step)
}

/// Create a cycle generator.
pub fn cycle<T, const N: usize>(values: [T; N]) -> Cycle<T, N> {
    Cycle::new(values)
}

/// Create a pick-one generator.
pub fn pick_one<T>(values: &[T]) -> PickOne<'_, T> {
    PickOne::new(values)
}

/// Create a random bool generator.
pub fn random_bool() -> RandomBool {
    RandomBool::new()
}

/// Create a list generator.
pub fn list<L, E>(length_gen: L, element_gen: E) -> List<L, E>
where
    E: Clone,
{
    List::new(length_gen, element_gen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant() {
        let mut generator = Constant::new(42i64);
        let mut rng = rand::rng();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 42);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 42);
    }

    #[test]
    fn test_range() {
        let mut generator = Range::new(0i64..10);
        let mut rng = rand::rng();
        for _ in 0..100 {
            let v = Gen::<SInt64>::generate(&mut generator, &mut rng);
            assert!((0..10).contains(&v));
        }
    }

    #[test]
    fn test_inc() {
        let mut generator = Inc::from(0i64);
        let mut rng = rand::rng();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 0);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 1);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 2);

        generator.reset();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 0);
    }

    #[test]
    fn test_inc_step() {
        let mut generator = Inc::new(0i64, 10);
        let mut rng = rand::rng();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 0);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 10);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 20);
    }

    #[test]
    fn test_cycle() {
        let mut generator = Cycle::new([1i64, 2, 3]);
        let mut rng = rand::rng();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 1);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 2);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 3);
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 1);

        generator.reset();
        assert_eq!(Gen::<SInt64>::generate(&mut generator, &mut rng), 1);
    }

    #[test]
    fn test_cycle_strings() {
        let mut generator = Cycle::new(["a", "b", "c"]);
        let mut rng = rand::rng();
        assert_eq!(Gen::<SString>::generate(&mut generator, &mut rng), "a");
        assert_eq!(Gen::<SString>::generate(&mut generator, &mut rng), "b");
        assert_eq!(Gen::<SString>::generate(&mut generator, &mut rng), "c");
        assert_eq!(Gen::<SString>::generate(&mut generator, &mut rng), "a");
    }

    #[test]
    fn test_list() {
        let mut generator = List::new(Constant::new(3i64), Inc::from(0i64));
        let mut rng = rand::rng();

        // First list: [0, 1, 2]
        let list1: Vec<i64> = Gen::<SList<SInt64>>::generate(&mut generator, &mut rng);
        assert_eq!(list1, vec![0, 1, 2]);

        // Second list: also [0, 1, 2] because element generator is reset per list
        let list2: Vec<i64> = Gen::<SList<SInt64>>::generate(&mut generator, &mut rng);
        assert_eq!(list2, vec![0, 1, 2]);
    }
}
