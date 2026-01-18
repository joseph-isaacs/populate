# fake-rs Integration Design

This document explores how to integrate [fake-rs](https://github.com/cksac/fake-rs) with data-comb/populate to provide semantic fake data (names, addresses, emails, etc.) as composable distributions.

## Why Integrate?

data-comb excels at:
- Statistical distributions (Range, Constant, Pick, Runs)
- Columnar output (Arrow RecordBatch)
- Combinator composition

fake-rs excels at:
- Semantic data (realistic names, addresses, companies)
- Localization (EN, FR, ZH_TW, etc.)
- 100+ data providers

**Together**: Generate realistic, localized test datasets with controlled statistical properties in columnar format.

---

## fake-rs Overview

### Core Traits

```rust
// The main trait - generate fake data with RNG
pub trait Dummy<T>: Sized {
    fn dummy_with_rng<R: Rng + ?Sized>(config: &T, rng: &mut R) -> Self;
}

// Convenience trait (auto-implemented for Dummy)
pub trait Fake: Sized {
    fn fake<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::Output;
}
```

### Available Faker Modules

| Module | Examples |
|--------|----------|
| `faker::name` | Name, FirstName, LastName, Title, Suffix |
| `faker::address` | StreetName, City, State, Country, PostCode, Latitude, Longitude |
| `faker::company` | CompanyName, Industry, Buzzword, CatchPhrase |
| `faker::internet` | Email, Username, DomainName, IPv4, IPv6, UserAgent |
| `faker::lorem` | Word, Words, Sentence, Paragraph |
| `faker::phone_number` | PhoneNumber, CellNumber |
| `faker::creditcard` | CreditCardNumber |
| `faker::currency` | CurrencyCode, CurrencyName |
| `faker::filesystem` | FileName, FilePath, FileExtension |
| `faker::boolean` | Boolean (with configurable ratio) |
| `faker::chrono` | Date, Time, DateTime (with chrono feature) |
| `faker::uuid` | Uuid (with uuid feature) |

### Locale Support

```rust
use fake::locales::{EN, FR, ZH_TW, JA_JP, DE_DE, ...};

let name_en: String = Name(EN).fake(&mut rng);    // "John Smith"
let name_fr: String = Name(FR).fake(&mut rng);    // "Jean Dupont"
let name_zh: String = Name(ZH_TW).fake(&mut rng); // "王小明"
```

---

## Integration Design

### Option 1: Wrapper Distribution (Recommended)

Create a `Faker<F, L>` distribution that wraps any fake-rs faker:

```rust
use fake::Fake;
use fake::locales::Data;
use populate_core::{Distribution, SampleIter};

/// Wraps a fake-rs faker as a populate Distribution
pub struct Faker<F, L> {
    faker: F,
    locale: L,
}

impl<F, L> Faker<F, L> {
    pub fn new(faker: F, locale: L) -> Self {
        Self { faker, locale }
    }
}

impl<T, F, L> Distribution<T> for Faker<F, L>
where
    F: Copy,
    L: Data + Copy,
    F: Fake + fake::Dummy<L, Output = T>,
{
    type Iter = FakerIter<F, L, T>;

    fn sampler(&self) -> Self::Iter {
        FakerIter {
            faker: self.faker,
            locale: self.locale,
            _phantom: PhantomData,
        }
    }
}

pub struct FakerIter<F, L, T> {
    faker: F,
    locale: L,
    _phantom: PhantomData<T>,
}

impl<T, F, L, R: Rng> SampleIter<R> for FakerIter<F, L, T>
where
    F: Fake + fake::Dummy<L, Output = T>,
    L: Data,
{
    type Item = T;

    fn next(&mut self, rng: &mut R) -> Self::Item {
        self.faker.fake_with_rng::<L, R>(rng)
    }
}
```

### Usage Example

```rust
use populate_builtins::Faker;
use fake::faker::name::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::address::raw::*;
use fake::locales::EN;

// Define a users table with fake data
let users = (
    Range::new(1i64, 1_000_000),                    // user_id
    Faker::new(FirstName, EN),                      // first_name
    Faker::new(LastName, EN),                       // last_name
    Faker::new(FreeEmail, EN),                      // email
    Faker::new(City, EN),                           // city
    Faker::new(PostCode, EN),                       // zip_code
    Range::new(18i32, 80),                          // age
);

let batch: RecordBatch = users.collect(&mut rng, 100_000);
```

---

### Option 2: Convenience Functions

Provide pre-built distributions for common fakers:

```rust
pub mod faker {
    use fake::locales::EN;

    pub fn first_name() -> Faker<fake::faker::name::raw::FirstName, EN> {
        Faker::new(fake::faker::name::raw::FirstName, EN)
    }

    pub fn last_name() -> Faker<fake::faker::name::raw::LastName, EN> {
        Faker::new(fake::faker::name::raw::LastName, EN)
    }

    pub fn email() -> Faker<fake::faker::internet::raw::FreeEmail, EN> {
        Faker::new(fake::faker::internet::raw::FreeEmail, EN)
    }

    pub fn city() -> Faker<fake::faker::address::raw::CityName, EN> {
        Faker::new(fake::faker::address::raw::CityName, EN)
    }

    // With locale parameter
    pub fn first_name_locale<L: Data>(locale: L) -> Faker<fake::faker::name::raw::FirstName, L> {
        Faker::new(fake::faker::name::raw::FirstName, locale)
    }
}
```

### Simplified Usage

```rust
use populate_builtins::faker::*;

let users = (
    Range::new(1i64, 1_000_000),
    first_name(),
    last_name(),
    email(),
    city(),
);
```

---

### Option 3: Derive Macro Integration

Allow `#[derive(Dummy)]` structs to work with populate:

```rust
use fake::{Dummy, Fake};
use populate_core::Distribution;

#[derive(Debug, Dummy)]
pub struct User {
    #[dummy(faker = "1..1000000")]
    id: i64,
    #[dummy(faker = "fake::faker::name::en::FirstName()")]
    first_name: String,
    #[dummy(faker = "fake::faker::name::en::LastName()")]
    last_name: String,
    #[dummy(faker = "fake::faker::internet::en::FreeEmail()")]
    email: String,
}

// Wrapper to use Dummy types as Distributions
pub struct DummyDist<T>(PhantomData<T>);

impl<T: Dummy<Faker>> Distribution<T> for DummyDist<T> {
    // ...
}

// Usage
let users: Vec<User> = DummyDist::<User>::new().collect(&mut rng, 1000);
```

---

## Composing with Other Distributions

The power comes from **combining** fake-rs with data-comb's statistical patterns:

### Nullable Names

```rust
let names = Nullable::new(
    Faker::new(FirstName, EN),
    0.05  // 5% null rate
);
```

### Weighted Company Types

```rust
let company_type = Pick::weighted(&[
    (Faker::new(CompanyName, EN), 70),      // 70% real company names
    (Constant::new("Acme Corp".into()), 20), // 20% Acme Corp
    (Constant::new("Unknown".into()), 10),   // 10% Unknown
]);
```

### Clustered Cities (Geographic Locality)

```rust
// Users in same region appear together (better compression)
let cities = Runs::new(
    Faker::new(CityName, EN),
    Range::new(100, 500)  // 100-500 users per city cluster
);
```

### Foreign Key with Names

```rust
// Pre-generate a pool of names, then sample with replacement
let name_pool: Vec<String> = Faker::new(FullName, EN)
    .collect_vec(&mut rng, 1000);

let customer_names = PickOne::new(name_pool);
```

---

## Crate Structure

```
populate-faker/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── distribution.rs   # Faker<F, L> wrapper
│   ├── convenience.rs    # first_name(), email(), etc.
│   └── locales.rs        # Re-exports of fake::locales
```

### Cargo.toml

```toml
[package]
name = "populate-faker"
version = "0.1.0"

[dependencies]
populate-core = { path = "../populate-core" }
fake = { version = "4", default-features = false }
rand = "0.8"

[features]
default = ["derive"]
derive = ["fake/derive"]
chrono = ["fake/chrono"]
uuid = ["fake/uuid"]
# ... pass through fake features
```

---

## API Design Principles

### 1. Zero-Cost Abstraction

The `Faker<F, L>` wrapper should compile to the same code as calling `faker.fake_with_rng()` directly.

### 2. Compose, Don't Wrap

Don't create a parallel hierarchy. Use fake-rs types directly:

```rust
// Good: Use fake-rs types directly
Faker::new(fake::faker::name::raw::FirstName, EN)

// Bad: Create wrapper types
faker::name::FirstName::new(EN)
```

### 3. Feature Flags Match fake-rs

Enable fake-rs features via populate-faker features:

```toml
populate-faker = { version = "0.1", features = ["chrono", "uuid"] }
```

### 4. Locale is Explicit

Don't default to EN. Make locale explicit for clarity:

```rust
// Good: Explicit locale
Faker::new(FirstName, EN)
Faker::new(FirstName, FR)

// Convenience functions can default to EN
faker::first_name()  // Defaults to EN
faker::first_name_locale(FR)  // Explicit
```

---

## Example: Full Schema

```rust
use populate_builtins::{Range, Pick, Constant, Nullable, Runs, list};
use populate_faker::{Faker, faker::*};
use fake::locales::EN;
use fake::faker::{name::raw::*, internet::raw::*, address::raw::*, company::raw::*};

let customers = (
    Range::new(1i64, 10_000_000),           // customer_id
    Faker::new(FirstName, EN),              // first_name
    Faker::new(LastName, EN),               // last_name
    Faker::new(FreeEmail, EN),              // email
    Nullable::new(Faker::new(PhoneNumber, EN), 0.3), // phone (30% null)
    Faker::new(StreetAddress, EN),          // address
    Runs::new(Faker::new(CityName, EN), Range::new(50, 200)), // city (clustered)
    Faker::new(StateAbbr, EN),              // state
    Faker::new(PostCode, EN),               // zip
    Pick::new(["US", "CA", "MX"]),          // country
    Faker::new(CompanyName, EN),            // company
    Range::new(0i32, 100),                  // loyalty_points
);

let schema = Schema::new(vec![
    Field::new("customer_id", DataType::Int64, false),
    Field::new("first_name", DataType::Utf8, false),
    Field::new("last_name", DataType::Utf8, false),
    Field::new("email", DataType::Utf8, false),
    Field::new("phone", DataType::Utf8, true),
    Field::new("address", DataType::Utf8, false),
    Field::new("city", DataType::Utf8, false),
    Field::new("state", DataType::Utf8, false),
    Field::new("zip", DataType::Utf8, false),
    Field::new("country", DataType::Utf8, false),
    Field::new("company", DataType::Utf8, false),
    Field::new("loyalty_points", DataType::Int32, false),
]);

let batch: RecordBatch = customers.collect_with_schema(&mut rng, 1_000_000, schema);
```

---

## Implementation Checklist

- [ ] Create `populate-faker` crate
- [ ] Implement `Faker<F, L>` distribution wrapper
- [ ] Implement `FakerIter<F, L, T>` sample iterator
- [ ] Add convenience functions for common fakers
- [ ] Add feature flags matching fake-rs
- [ ] Write tests with proptest for Distribution laws
- [ ] Add examples in `examples/` directory
- [ ] Document locale support
- [ ] Benchmark vs direct fake-rs usage (should be zero-cost)

---

## References

- [fake-rs GitHub](https://github.com/cksac/fake-rs)
- [fake-rs docs.rs](https://docs.rs/fake)
- [fake-rs crates.io](https://crates.io/crates/fake)
- [Dummy derive macro](https://docs.rs/fake/latest/fake/derive.Dummy.html)
- [faker module reference](https://docs.rs/fake/latest/fake/faker/)
