# Research: Existing Solutions for Test Data Generation

This document surveys existing solutions that are similar to or overlap with **data-comb/populate** — a Rust library for composable, deterministic test data generation for columnar data structures (especially Apache Arrow).

## Summary of data-comb/populate

**Key characteristics:**
- **Combinator-based**: Inspired by parser combinators (like nom), uses trait-based composition
- **Pull-based/Lazy**: Infinite iterators that produce values on-demand
- **Deterministic**: Seeded RNGs for reproducible output
- **Columnar-first**: Native Arrow RecordBatch output, not just row-based
- **Zero-copy design**: Combinators wrap iterators, not values
- **Statistical control**: Supports distributions, clustering, nullability, weighted selection

---

## 1. Closest Rust Alternatives

### 1.1 Synth (shuttle-hq/synth)
**Repository**: https://github.com/shuttle-hq/synth

A declarative data generator written in Rust. Uses JSON/YAML schema files to define data models.

**Similarities:**
- Rust-based, designed for large-scale generation
- Deterministic output
- Supports relational constraints

**Differences:**
- **Declarative config** vs programmatic combinators
- No native Arrow output
- Uses fake-rs for semantic data (names, addresses)
- Schema inference from databases
- Not combinator-based in code

### 1.2 tpchgen-rs
**Repository**: https://github.com/clflushopt/tpchgen-rs
**Docs**: https://docs.rs/tpchgen

Blazing fast TPC-H benchmark data generator in pure Rust.

**Similarities:**
- Native Arrow RecordBatch output (tpchgen-arrow crate)
- Pure Rust, high performance
- Deterministic

**Differences:**
- **Specific to TPC-H schema** — not a general-purpose library
- Not combinator-based
- Fixed schema, not composable

### 1.3 test-data-generation
**Repository**: https://github.com/dsietz/test-data-generation
**Crates.io**: https://crates.io/crates/test-data-generation

Analyzes sample data to build generation algorithms.

**Differences:**
- Profile-based (learns from samples)
- No Arrow support
- Not combinator-based
- Row-oriented

### 1.4 fake-rs
**Crates.io**: https://crates.io/crates/fake

General-purpose fake data library for Rust.

**Differences:**
- Semantic data focus (names, emails, addresses)
- No columnar/Arrow support
- Not combinator-based for composition
- Single-value generation, not streaming

### 1.5 datagen (arunma)
**Repository**: https://github.com/arunma/datagen

CLI tool to generate fake data in bulk, export to JSON, CSV, Avro, Parquet.

**Differences:**
- CLI tool, not a library
- Schema via YAML config
- Not programmatic combinator approach

---

## 2. Property-Based Testing Libraries (Combinator-Based)

These libraries share the **combinator pattern** but are designed for testing, not bulk data generation.

### 2.1 proptest (Rust)
**Repository**: https://github.com/proptest-rs/proptest
**Docs**: https://docs.rs/proptest

Hypothesis-like property testing with explicit Strategy objects.

**Similarities:**
- **Combinator-based** (`prop_map`, `prop_flat_map`, strategies compose)
- Generates structured data
- Supports constraints

**Differences:**
- **Testing-focused**, not bulk generation
- Has shrinking (not needed for data generation)
- No Arrow/columnar output
- Not optimized for generating millions of rows

### 2.2 quickcheck (Rust)
**Repository**: https://github.com/BurntSushi/quickcheck

Type-class based property testing.

**Differences:**
- Type-driven generation (Arbitrary trait)
- Less composable than proptest
- No columnar output

### 2.3 arbitrary (Rust)
**Repository**: https://github.com/rust-fuzz/arbitrary

Generates structured data from unstructured bytes (for fuzzing).

**Differences:**
- Fuzz-testing focused
- Byte-buffer input driven
- No statistical distributions

### 2.4 Hypothesis (Python)
**Docs**: https://hypothesis.readthedocs.io

The gold standard for property-based testing.

**Similarities:**
- **Rich combinator composition** (strategies, `map`, `flatmap`, `filter`)
- Composite strategies via `@composite`

**Differences:**
- Python, not Rust
- Testing-focused with shrinking
- No Arrow output

### 2.5 ScalaCheck (Scala)
**Website**: https://scalacheck.org

Scala property testing library.

**Similarities:**
- **Gen combinators** (`choose`, `pick`, `frequency`, `sized`)

**Differences:**
- JVM-based
- Testing-focused

### 2.6 QuickCheck / Hedgehog (Haskell)
**Hedgehog**: https://hackage.haskell.org/package/hedgehog

The original property testing libraries.

**Similarities:**
- Pioneered the combinator approach

**Differences:**
- Haskell ecosystem
- Integrated shrinking (Hedgehog)

---

## 3. Fake Data Libraries (Other Languages)

### 3.1 Faker / Datafaker (Java/Kotlin)
**Website**: https://www.datafaker.net
**Repository**: https://github.com/datafaker-net/datafaker

250+ data providers for realistic fake data.

**Differences:**
- Semantic data focus
- JVM-based
- No columnar output
- Not combinator-based

### 3.2 Mimesis (Python)
**Repository**: https://github.com/lk-geimfari/mimesis
**PyPI**: https://pypi.org/project/mimesis/

Fast fake data generator for Python, 46+ locales.

**Differences:**
- Python
- Semantic data providers
- No Arrow/columnar output

### 3.3 Faker (Python)
**PyPI**: https://pypi.org/project/Faker/

Popular Python fake data library.

**Differences:**
- Row-oriented
- Semantic data focus

---

## 4. Database Benchmark Generators

### 4.1 TPC-H dbgen (Original C)
**Website**: https://www.tpc.org/tpch/

The original TPC-H data generator.

**Differences:**
- C-based, outputs text files
- Fixed TPC-H schema only
- Not a library

### 4.2 tpctools (Rust wrapper)
**Crates.io**: https://crates.io/crates/tpctools

Rust wrapper around TPC-H/TPC-DS generators.

**Differences:**
- Wraps C tools
- Schema-specific

### 4.3 SSB-dbgen
**Repository**: https://github.com/eyalroz/ssb-dbgen

Star Schema Benchmark data generator.

**Differences:**
- C-based
- Fixed schema
- "Not easy to adapt to different data distributions"

---

## 5. Columnar / Arrow-Adjacent Tools

### 5.1 Polars Testing Module
**Docs**: https://docs.pola.rs/py-polars/html/reference/testing.html

Polars provides testing utilities and Hypothesis integration.

**Differences:**
- DataFrame testing, not generation library
- Python API
- Uses Hypothesis for property testing

### 5.2 Apache Arrow record_batch! macro
**Docs**: https://docs.rs/arrow/latest/arrow/

Arrow's built-in macro for creating test RecordBatches.

**Differences:**
- Literal values only, not generated
- No distributions or patterns
- For testing Arrow code, not generating datasets

### 5.3 serde_arrow
**Repository**: https://github.com/chmp/serde_arrow

Convert Rust structs to Arrow.

**Differences:**
- Conversion tool, not generation
- Uses Serde

### 5.4 typed-arrow
**Repository**: https://github.com/tonbo-io/typed-arrow

Compile-time Arrow schemas via derive macros.

**Differences:**
- Schema definition, not data generation

---

## 6. Lazy Iterator Libraries

These share the **pull-based/lazy evaluation** pattern:

### 6.1 Lazy.js (JavaScript)
**Website**: https://danieltao.com/lazy.js/

Lazy evaluation with indefinite sequences.

### 6.2 itertools (Python)
Standard library for lazy iteration.

### 6.3 Rust's std::iter
Native lazy iterators in Rust.

**data-comb builds on these patterns** but adds:
- RNG threading through iterators
- Distribution abstractions
- Columnar sink traits

---

## 7. Unique Aspects of data-comb/populate

Based on this research, **data-comb/populate occupies a unique niche**:

| Feature | data-comb | Synth | tpchgen-rs | proptest | fake-rs |
|---------|-----------|-------|------------|----------|---------|
| Combinator-based | ✅ | ❌ | ❌ | ✅ | ❌ |
| Native Arrow output | ✅ | ❌ | ✅ | ❌ | ❌ |
| General-purpose | ✅ | ✅ | ❌ | ✅ | ✅ |
| Pull-based/Lazy | ✅ | ❌ | ❌ | ✅ | ❌ |
| Statistical distributions | ✅ | ✅ | ❌ | ✅ | ❌ |
| Clustering patterns | ✅ | ❌ | ❌ | ❌ | ❌ |
| Tuple composition | ✅ | ❌ | ❌ | ✅ | ❌ |
| Bulk generation focus | ✅ | ✅ | ✅ | ❌ | ❌ |
| Deterministic | ✅ | ✅ | ✅ | ✅ | ✅ |

### Key differentiators:

1. **Combinator + Arrow**: No other library combines parser-combinator-style composition with native Arrow output
2. **Pull-based columnar**: Lazy iteration directly into columnar format without intermediate row representation
3. **Statistical patterns**: Built-in support for clustering, runs, monotonic sequences — not just random values
4. **Tuple schema composition**: Define table schemas as tuples that materialize as columns
5. **Multiple output formats**: Same distribution produces Vec rows, columnar Vecs, or Arrow RecordBatch

---

## 8. Potential Inspirations / Feature Ideas from Other Libraries

From research, features worth considering:

- **Synth**: Database import/schema inference
- **Hypothesis**: `@composite` decorator pattern for complex generation
- **proptest**: Built-in shrinking (if testing use case emerges)
- **Datafaker**: Semantic providers (names, addresses) via integration with fake-rs
- **tpchgen-rs**: Parquet direct output

---

## 9. Conclusion

**data-comb/populate appears to be novel** in combining:
1. Combinator-based composition (like proptest/Hypothesis)
2. Native Arrow/columnar output (like tpchgen-arrow)
3. Statistical distribution control (like property testing strategies)
4. Bulk data generation focus (like Synth/dbgen)

The closest conceptual relatives are:
- **proptest** (combinator pattern, but testing-focused)
- **tpchgen-arrow** (Arrow output, but schema-specific)
- **Synth** (bulk generation, but declarative not combinator-based)

No existing library combines all of these into a **programmatic, combinator-based, columnar-first bulk data generation library**.

---

## Sources

### Rust Ecosystem
- [Synth](https://github.com/shuttle-hq/synth) - Declarative data generator
- [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) - TPC-H generator with Arrow
- [proptest](https://github.com/proptest-rs/proptest) - Property testing
- [quickcheck](https://github.com/BurntSushi/quickcheck) - Property testing
- [fake-rs](https://crates.io/crates/fake) - Fake data
- [arbitrary](https://github.com/rust-fuzz/arbitrary) - Fuzzing
- [test-data-generation](https://crates.io/crates/test-data-generation) - Profile-based generation
- [datagen](https://github.com/arunma/datagen) - CLI bulk generator

### Python Ecosystem
- [Hypothesis](https://hypothesis.readthedocs.io/) - Property testing
- [Mimesis](https://github.com/lk-geimfari/mimesis) - Fake data
- [Polars Testing](https://docs.pola.rs/py-polars/html/reference/testing.html) - DataFrame testing

### JVM Ecosystem
- [Datafaker](https://www.datafaker.net/) - Java/Kotlin fake data
- [ScalaCheck](https://scalacheck.org/) - Scala property testing

### Haskell Ecosystem
- [Hedgehog](https://hackage.haskell.org/package/hedgehog) - Property testing

### Apache Arrow
- [arrow-rs](https://github.com/apache/arrow-rs) - Official Rust Arrow
- [tpchgen-arrow](https://lib.rs/crates/tpchgen-arrow) - TPC-H Arrow output
