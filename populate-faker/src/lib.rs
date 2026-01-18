//! # populate-faker
//!
//! Integration between [fake-rs](https://github.com/cksac/fake-rs) and populate
//! for generating semantic fake data (names, emails, addresses) as composable distributions.
//!
//! ## Example
//!
//! ```rust,ignore
//! use populate_faker::en;
//! use populate_core::Distribution;
//!
//! let mut first_name = en::first_name();
//! let mut iter = first_name.sample();
//! let name: String = iter.next(&mut rng);
//! ```

use std::marker::PhantomData;

use fake::Dummy;
use populate_core::{Distribution, Rng, SampleIter};

/// A distribution that generates values using fake-rs.
///
/// Uses a factory function to create the faker on each call,
/// avoiding the need for the faker type to implement Clone.
pub struct FakerFn<F, T> {
    factory: F,
    _phantom: PhantomData<fn() -> T>,
}

impl<F: Clone, T> Clone for FakerFn<F, T> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<F: Copy, T> Copy for FakerFn<F, T> {}

impl<F, T> FakerFn<F, T> {
    /// Create a new FakerFn distribution from a factory function.
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            _phantom: PhantomData,
        }
    }
}

/// Iterator for FakerFn distribution.
pub struct FakerFnIter<F, T> {
    factory: F,
    _phantom: PhantomData<fn() -> T>,
}

impl<F: Clone, T> Clone for FakerFnIter<F, T> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<F: Copy, T> Copy for FakerFnIter<F, T> {}

impl<Fac, Faker, T> SampleIter for FakerFnIter<Fac, T>
where
    Fac: Clone + Fn() -> Faker,
    T: Dummy<Faker>,
{
    type Item = T;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        let faker = (self.factory)();
        T::dummy_with_rng(&faker, rng)
    }
}

impl<Fac, Faker, T> Distribution<T> for FakerFn<Fac, T>
where
    Fac: Clone + Fn() -> Faker,
    T: Dummy<Faker>,
{
    type Iter = FakerFnIter<Fac, T>;

    fn sample(&mut self) -> Self::Iter {
        FakerFnIter {
            factory: self.factory.clone(),
            _phantom: PhantomData,
        }
    }
}

// Re-export commonly used fake-rs types for convenience
pub use fake::locales;

/// Convenience module with pre-configured distributions for common fakers (English locale).
pub mod en {
    use super::*;
    use fake::faker::address::raw as address;
    use fake::faker::company::raw as company;
    use fake::faker::internet::raw as internet;
    use fake::faker::lorem::raw as lorem;
    use fake::faker::name::raw as name;
    use fake::faker::phone_number::raw as phone;
    use fake::locales::EN;

    // === Name ===

    /// Distribution for first names (English).
    pub fn first_name() -> FakerFn<fn() -> name::FirstName<EN>, String> {
        FakerFn::new(|| name::FirstName(EN))
    }

    /// Distribution for last names (English).
    pub fn last_name() -> FakerFn<fn() -> name::LastName<EN>, String> {
        FakerFn::new(|| name::LastName(EN))
    }

    /// Distribution for full names (English).
    pub fn name() -> FakerFn<fn() -> name::Name<EN>, String> {
        FakerFn::new(|| name::Name(EN))
    }

    /// Distribution for name suffixes like Jr., Sr. (English).
    pub fn name_suffix() -> FakerFn<fn() -> name::Suffix<EN>, String> {
        FakerFn::new(|| name::Suffix(EN))
    }

    /// Distribution for professional titles (English).
    pub fn title() -> FakerFn<fn() -> name::Title<EN>, String> {
        FakerFn::new(|| name::Title(EN))
    }

    // === Internet ===

    /// Distribution for free email addresses (gmail, yahoo, etc).
    pub fn free_email() -> FakerFn<fn() -> internet::FreeEmail<EN>, String> {
        FakerFn::new(|| internet::FreeEmail(EN))
    }

    /// Distribution for safe/example email addresses.
    pub fn safe_email() -> FakerFn<fn() -> internet::SafeEmail<EN>, String> {
        FakerFn::new(|| internet::SafeEmail(EN))
    }

    /// Distribution for usernames.
    pub fn username() -> FakerFn<fn() -> internet::Username<EN>, String> {
        FakerFn::new(|| internet::Username(EN))
    }

    /// Distribution for domain suffixes.
    pub fn domain_suffix() -> FakerFn<fn() -> internet::DomainSuffix<EN>, String> {
        FakerFn::new(|| internet::DomainSuffix(EN))
    }

    /// Distribution for IPv4 addresses.
    pub fn ipv4() -> FakerFn<fn() -> internet::IPv4<EN>, String> {
        FakerFn::new(|| internet::IPv4(EN))
    }

    /// Distribution for IPv6 addresses.
    pub fn ipv6() -> FakerFn<fn() -> internet::IPv6<EN>, String> {
        FakerFn::new(|| internet::IPv6(EN))
    }

    /// Distribution for user agent strings.
    pub fn user_agent() -> FakerFn<fn() -> internet::UserAgent<EN>, String> {
        FakerFn::new(|| internet::UserAgent(EN))
    }

    // === Address ===

    /// Distribution for city names.
    pub fn city_name() -> FakerFn<fn() -> address::CityName<EN>, String> {
        FakerFn::new(|| address::CityName(EN))
    }

    /// Distribution for street names.
    pub fn street_name() -> FakerFn<fn() -> address::StreetName<EN>, String> {
        FakerFn::new(|| address::StreetName(EN))
    }

    /// Distribution for state names.
    pub fn state_name() -> FakerFn<fn() -> address::StateName<EN>, String> {
        FakerFn::new(|| address::StateName(EN))
    }

    /// Distribution for state abbreviations.
    pub fn state_abbr() -> FakerFn<fn() -> address::StateAbbr<EN>, String> {
        FakerFn::new(|| address::StateAbbr(EN))
    }

    /// Distribution for country names.
    pub fn country_name() -> FakerFn<fn() -> address::CountryName<EN>, String> {
        FakerFn::new(|| address::CountryName(EN))
    }

    /// Distribution for country codes.
    pub fn country_code() -> FakerFn<fn() -> address::CountryCode<EN>, String> {
        FakerFn::new(|| address::CountryCode(EN))
    }

    /// Distribution for postal/zip codes.
    pub fn zip_code() -> FakerFn<fn() -> address::ZipCode<EN>, String> {
        FakerFn::new(|| address::ZipCode(EN))
    }

    /// Distribution for postal codes.
    pub fn post_code() -> FakerFn<fn() -> address::PostCode<EN>, String> {
        FakerFn::new(|| address::PostCode(EN))
    }

    /// Distribution for building numbers.
    pub fn building_number() -> FakerFn<fn() -> address::BuildingNumber<EN>, String> {
        FakerFn::new(|| address::BuildingNumber(EN))
    }

    /// Distribution for latitude values.
    pub fn latitude() -> FakerFn<fn() -> address::Latitude<EN>, String> {
        FakerFn::new(|| address::Latitude(EN))
    }

    /// Distribution for longitude values.
    pub fn longitude() -> FakerFn<fn() -> address::Longitude<EN>, String> {
        FakerFn::new(|| address::Longitude(EN))
    }

    // === Company ===

    /// Distribution for company names.
    pub fn company_name() -> FakerFn<fn() -> company::CompanyName<EN>, String> {
        FakerFn::new(|| company::CompanyName(EN))
    }

    /// Distribution for company industry names.
    pub fn industry() -> FakerFn<fn() -> company::Industry<EN>, String> {
        FakerFn::new(|| company::Industry(EN))
    }

    /// Distribution for company buzzwords.
    pub fn buzzword() -> FakerFn<fn() -> company::Buzzword<EN>, String> {
        FakerFn::new(|| company::Buzzword(EN))
    }

    /// Distribution for company catch phrases.
    pub fn catch_phrase() -> FakerFn<fn() -> company::CatchPhrase<EN>, String> {
        FakerFn::new(|| company::CatchPhrase(EN))
    }

    /// Distribution for company profession names.
    pub fn profession() -> FakerFn<fn() -> company::Profession<EN>, String> {
        FakerFn::new(|| company::Profession(EN))
    }

    // === Phone ===

    /// Distribution for phone numbers.
    pub fn phone_number() -> FakerFn<fn() -> phone::PhoneNumber<EN>, String> {
        FakerFn::new(|| phone::PhoneNumber(EN))
    }

    /// Distribution for cell/mobile phone numbers.
    pub fn cell_number() -> FakerFn<fn() -> phone::CellNumber<EN>, String> {
        FakerFn::new(|| phone::CellNumber(EN))
    }

    // === Lorem ===

    /// Distribution for lorem ipsum words.
    pub fn word() -> FakerFn<fn() -> lorem::Word<EN>, String> {
        FakerFn::new(|| lorem::Word(EN))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_faker_distribution() {
        let mut dist = en::first_name();
        let mut rng = StdRng::seed_from_u64(42);
        let mut iter = dist.sample();

        let name1 = iter.next(&mut rng);
        let name2 = iter.next(&mut rng);

        // Names should be non-empty strings
        assert!(!name1.is_empty());
        assert!(!name2.is_empty());

        // With same seed, should be reproducible
        let mut rng2 = StdRng::seed_from_u64(42);
        let mut iter2 = dist.sample();
        assert_eq!(iter2.next(&mut rng2), name1);
        assert_eq!(iter2.next(&mut rng2), name2);
    }

    #[test]
    fn test_convenience_functions() {
        let mut rng = StdRng::seed_from_u64(123);

        let mut first = en::first_name();
        let mut last = en::last_name();
        let mut email = en::free_email();
        let mut city = en::city_name();

        let mut iter_first = first.sample();
        let mut iter_last = last.sample();
        let mut iter_email = email.sample();
        let mut iter_city = city.sample();

        let first_name = iter_first.next(&mut rng);
        let last_name = iter_last.next(&mut rng);
        let email_addr = iter_email.next(&mut rng);
        let city_name = iter_city.next(&mut rng);

        assert!(!first_name.is_empty());
        assert!(!last_name.is_empty());
        assert!(email_addr.contains('@'));
        assert!(!city_name.is_empty());
    }

    #[test]
    fn test_clone_semantics() {
        let mut dist = en::first_name();

        let iter1 = dist.sample();
        let iter2 = iter1.clone();

        // Cloned iterators should produce same sequence with same RNG
        let mut rng1 = StdRng::seed_from_u64(99);
        let mut rng2 = StdRng::seed_from_u64(99);

        let mut iter1 = iter1;
        let mut iter2 = iter2;

        assert_eq!(iter1.next(&mut rng1), iter2.next(&mut rng2));
        assert_eq!(iter1.next(&mut rng1), iter2.next(&mut rng2));
    }
}
