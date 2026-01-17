# Real World Examples

Stretch goal: model TPC-H tables using our combinators.

## TPC-H Tables (Simple)

These tables have no/minimal foreign key dependencies and can be modeled with current combinators.

### Region (5 rows, static)

```rust
let region = (
    Monotonic::new(Constant::new(1u32)),  // region_key: 0,1,2,3,4
    OneOf::new(vec!["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]),
    Constant::new(""),  // comment
);

let regions: Vec<_> = Repeat::new(region, 5).sample(&mut rng);
```

### Nation (25 rows, FK to region)

```rust
let nation = (
    Monotonic::new(Constant::new(1u32)),  // nation_key: 0-24
    OneOf::new(vec![
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
    ]),
    Range::new(0u32, 5),  // region_key (0-4)
    Constant::new(""),    // comment
);

let nations: Vec<_> = Repeat::new(nation, 25).sample(&mut rng);
```

### Part (200k × scale factor, no FKs)

```rust
const COLORS: &[&str] = &[
    "almond", "antique", "aquamarine", "azure", "beige",
    "bisque", "black", "blanched", "blue", "blush",
    "brown", "burlywood", "burnished", "chartreuse", "chiffon",
    "chocolate", "coral", "cornflower", "cornsilk", "cream",
];

const TYPES: &[&str] = &[
    "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO",
];

const CONTAINERS: &[&str] = &[
    "SM CASE", "SM BOX", "SM BAG", "SM JAR", "SM PACK",
    "MED CASE", "MED BOX", "MED BAG", "MED JAR", "MED PACK",
    "LG CASE", "LG BOX", "LG BAG", "LG JAR", "LG PACK",
];

let part = (
    // part_key (sequential)
    Monotonic::new(Constant::new(1u64)),

    // name: combination of colors and type
    (
        OneOf::new(COLORS.to_vec()),
        OneOf::new(COLORS.to_vec()),
        OneOf::new(TYPES.to_vec()),
    ).map(|(a, b, c)| format!("{} {} {}", a, b, c)),

    // mfgr: "Manufacturer#N" where N is 1-5
    Range::new(1u8, 6).map(|n| format!("Manufacturer#{}", n)),

    // brand: "Brand#NM"
    (Range::new(1u8, 6), Range::new(1u8, 6))
        .map(|(n, m)| format!("Brand#{}{}", n, m)),

    // type
    OneOf::new(TYPES.to_vec()),

    // size: 1-50
    Range::new(1i32, 51),

    // container
    OneOf::new(CONTAINERS.to_vec()),

    // retail_price in cents
    Range::new(10000i64, 200000),

    // comment (simplified)
    Constant::new(""),
);

let scale_factor = 1;
let mut rng = StdRng::seed_from_u64(42);
let parts: Vec<_> = Repeat::new(part, 200_000 * scale_factor).sample(&mut rng);
```

## Future: Complex Tables

Tables with foreign key dependencies and derived columns (Orders, LineItem, Customer, PartSupp, Supplier) require additional combinators:

- `FlatMap` for derived columns
- `ForeignKey<D>` for referential integrity
- `Grammar<Rules>` for text generation

See [FUTURE.md](FUTURE.md) for planned combinators.
