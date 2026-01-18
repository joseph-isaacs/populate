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

## Part I: Academic Literature & Foundational Papers

### 1. The QuickCheck Paper (ICFP 2000) — The Origin

**"QuickCheck: A Lightweight Tool for Random Testing of Haskell Programs"**
- **Authors**: Koen Claessen and John Hughes (Chalmers University)
- **Venue**: ICFP 2000, Montreal, pages 268–279
- **PDF**: https://www.cs.tufts.edu/~nr/cs257/archive/john-hughes/quick.pdf
- **ACM**: https://dl.acm.org/doi/10.1145/351240.351266

This seminal paper introduced:
- **Generator combinators** for composing test data generators
- **Type-directed generation** via the `Arbitrary` typeclass
- **Shrinking** to find minimal counterexamples
- The idea that "QuickCheck provides combinators" — ordinary functions that compose well

The paper spawned ports to 40+ languages and established the combinator pattern for data generation.

---

### 2. Monadic Parser Combinators — The Theoretical Foundation

**"Monadic Parser Combinators"** (1996)
- **Authors**: Graham Hutton and Erik Meijer
- **PDF**: https://people.cs.nott.ac.uk/pszgmh/monparsing.pdf

**"Functional Pearl: Monadic Parsing in Haskell"** (1998)
- **Authors**: Graham Hutton and Erik Meijer
- **Journal**: Journal of Functional Programming
- **PDF**: https://www.cs.tufts.edu/~nr/cs257/archive/graham-hutton/monadic-parsing-jfp.pdf

These papers established that **parsers form a monad**, enabling composition via `bind` (>>=). The same insight applies to generators:
- Parsers: `String → Maybe (a, String)`
- Generators: `RNG → (a, RNG)`

Both are stateful computations that compose monadically. **data-comb/populate uses this exact pattern** — `SampleIter` threads RNG state through composed iterators.

---

### 3. SmallCheck and Lazy SmallCheck (Haskell 2008)

**"SmallCheck and Lazy SmallCheck: Automatic Exhaustive Testing for Small Values"**
- **Authors**: Colin Runciman, Matthew Naylor, Fredrik Lindblad
- **Venue**: Haskell Symposium 2008
- **ACM**: https://dl.acm.org/doi/10.1145/1543134.1411292
- **PDF**: https://people.irisa.fr/Thomas.Genet/ACF/Bibliography/Isabelle/Advanced/exhaustiveQuickcheck.pdf

Key insight: **Lazy SmallCheck** uses partially-defined inputs to explore large search spaces efficiently. This relates to data-comb's lazy/pull-based design.

---

### 4. Generating Good Generators (POPL 2018)

**"Generating Good Generators for Inductive Relations"**
- **Authors**: Leonidas Lampropoulos, Zoe Paraskevopoulou, Benjamin C. Pierce
- **Venue**: POPL 2018
- **ACM**: https://dl.acm.org/doi/10.1145/3158133
- **PDF**: https://zoep.github.io/GeneratingGoodGenerators.pdf

Shows how to compile Coq inductive relations into efficient generators with **soundness and completeness proofs**. Implemented in QuickChick.

---

### 5. Hedgehog: Integrated Shrinking

**Well-Typed Blog: "Integrated versus Manual Shrinking"**
- **Author**: Edsko de Vries
- **URL**: https://www.well-typed.com/blog/2019/05/integrated-shrinking/

Explains Hedgehog's key innovation: instead of separate generation and shrinking, **generate all possible shrinks ahead of time** as a tree. This ensures shrinks always satisfy generator invariants.

---

### 6. Monads for Functional Programming

**"Monads for Functional Programming"**
- **Author**: Philip Wadler
- **PDF**: https://homepages.inf.ed.ac.uk/wadler/papers/marktoberdorf/baastad.pdf

The foundational paper on using monads to structure programs. Directly relevant because:
- Random generation is a **State monad** (threading RNG)
- Generators compose via **monadic bind**
- The `Gen` monad in QuickCheck is an instance of this pattern

---

### 7. Test Data Generator Monad (Blog)

**"Test Data Generator monad"**
- **Author**: Mark Seemann
- **URL**: https://blog.ploeh.dk/2023/02/27/test-data-generator-monad/

Demonstrates that test data generators satisfy the monad laws, explaining why `flatMap`/`chain` is essential for dependent generation.

---

### 8. Columnar Storage Benchmarks

**"An Empirical Evaluation of Columnar Storage Formats"**
- **Authors**: Xinyu Zeng et al.
- **Venue**: VLDB 2023
- **arXiv**: https://arxiv.org/abs/2304.05028

Designed benchmarks to stress-test Parquet and ORC under different workload configurations. Relevant for understanding what data patterns matter for columnar analytics.

**"On Big Data Benchmarking"**
- **arXiv**: https://arxiv.org/pdf/1402.5194

Describes layered architecture with data generators covering different data types while keeping 4V properties (Volume, Velocity, Variety, Veracity).

---

## Part II: Blog Posts & Tutorials

### Building Your Own Property Testing Framework

**"Your own property based testing framework"** (Multi-part series)
- **Author**: Nicolas Dubien (creator of fast-check)
- **Part 1 - Generators**: https://dev.to/dubzzz/your-own-property-based-testing-framework-part-1-generators-3afi
- **Medium**: https://medium.com/@nicolasdubien/your-own-property-based-testing-framework-part-1-generators-eeb6cccb84d0

Walks through building a property testing framework from scratch, explaining generator design decisions.

---

### Property Testing Introductions

**"Introduction to Property Based Testing"** (Criteo Tech Blog)
- **Author**: Nicolas Dubien
- **URL**: https://medium.com/criteo-engineering/introduction-to-property-based-testing-f5236229d237

**"An Introduction to Property-Based Testing in Rust"**
- **Author**: Luca Palmieri
- **URL**: https://lpalmieri.com/posts/an-introduction-to-property-based-testing-in-rust/

**"Property-based testing in Rust with Proptest"** (LogRocket)
- **URL**: https://blog.logrocket.com/property-based-testing-in-rust-with-proptest/

---

### QuickCheck Tutorials

**"A QuickCheck Tutorial: Generators"** (Stack Builders)
- **URL**: https://www.stackbuilders.com/insights/a-quickcheck-tutorial-generators/

**"An introduction to property-based testing with QuickCheck"**
- **Author**: Jesper Cockx
- **URL**: https://jesper.sikanda.be/posts/quickcheck-intro.html

---

### Comparing Frameworks

**"The Properties of QuickCheck, Hedgehog and Hypothesis"**
- **URL**: https://seelengrab.github.io/articles/The%20properties%20of%20QuickCheck,%20Hedgehog%20and%20Hypothesis/

Detailed comparison of shrinking strategies and generator composition across frameworks.

---

### Datagen Combinators (Theory)

**"Random Test Data Generation for Arbitrary Types"**
- **Author**: thautwarm
- **URL**: https://thautwarm.github.io/Site-33/1-programming/03-tech/datagen/

Calls the pattern "datagen combinators" or "datagenc" — using GADTs to represent generation processes in a type-safe, composable way.

---

## Part III: Existing Tools & Libraries

### A. Rust Ecosystem

#### A.1 Synth (shuttle-hq/synth)
- **Repository**: https://github.com/shuttle-hq/synth
- **Approach**: Declarative JSON/YAML schema files
- **Differences**: Config-driven (not programmatic combinators), no Arrow output

#### A.2 tpchgen-rs
- **Repository**: https://github.com/clflushopt/tpchgen-rs
- **Blog**: https://datafusion.apache.org/blog/2025/04/10/fastest-tpch-generator/
- **Approach**: World's fastest TPC-H generator, native Arrow output
- **Differences**: Fixed TPC-H schema only, not general-purpose

#### A.3 proptest
- **Repository**: https://github.com/proptest-rs/proptest
- **Docs**: https://docs.rs/proptest
- **Approach**: Hypothesis-inspired, Strategy-based composition
- **Key difference from QuickCheck**: "Generation and shrinking is defined on a per-value basis instead of per-type"
- **Differences from data-comb**: Testing-focused (shrinking), no Arrow output

#### A.4 quickcheck
- **Repository**: https://github.com/BurntSushi/quickcheck
- **Approach**: Type-class based (Arbitrary trait)
- **Differences**: Less composable, no columnar output

#### A.5 arbitrary
- **Repository**: https://github.com/rust-fuzz/arbitrary
- **Approach**: Generate from unstructured bytes (for fuzzing)
- **Differences**: Fuzz-focused, not statistical distributions

#### A.6 fake-rs
- **Crates.io**: https://crates.io/crates/fake
- **Approach**: Semantic fake data (names, emails)
- **Differences**: Single-value, no columnar support

---

### B. Python Ecosystem

#### B.1 Hypothesis
- **Docs**: https://hypothesis.readthedocs.io
- **Approach**: Gold standard for property testing, rich strategy composition
- **Key features**: `@composite`, `map`, `flatmap`, `filter`, integrated shrinking
- **Differences**: Python, testing-focused, no Arrow output

#### B.2 Synthetic Data Vault (SDV)
- **Website**: https://sdv.dev/
- **Repository**: https://github.com/sdv-dev/SDV
- **Approach**: ML-based (GaussianCopula, CTGAN, TVAE) to learn from real data
- **Differences**: Learns distributions from data, not programmatic

#### B.3 CTGAN
- **Repository**: https://github.com/sdv-dev/CTGAN
- **Approach**: Deep learning synthetic data via GANs
- **Differences**: ML-based, not combinator-based

#### B.4 Mimesis / Faker
- **Mimesis**: https://github.com/lk-geimfari/mimesis
- **Faker**: https://pypi.org/project/Faker/
- **Approach**: Semantic fake data providers
- **Differences**: Row-oriented, no statistical patterns

---

### C. JVM Ecosystem

#### C.1 ScalaCheck
- **Website**: https://scalacheck.org
- **Docs**: https://www.scala-exercises.org/scalacheck/generators
- **Approach**: `Gen` combinators (`choose`, `oneOf`, `frequency`, `sized`)
- **Key quote**: "Generators can be combined using a for-comprehension"
- **Differences**: JVM, testing-focused

#### C.2 Datafaker
- **Website**: https://www.datafaker.net
- **Repository**: https://github.com/datafaker-net/datafaker
- **Approach**: 250+ data providers, modern fork of java-faker
- **Differences**: Semantic data, not combinator-based

#### C.3 QuickTheories
- **Repository**: https://github.com/quicktheories/QuickTheories
- **Approach**: Property testing for Java 8
- **Differences**: Testing-focused

---

### D. .NET Ecosystem

#### D.1 FsCheck
- **Website**: https://fscheck.github.io/FsCheck/
- **Repository**: https://github.com/fscheck/FsCheck
- **Approach**: F#/C# QuickCheck port with `Gen<T>` combinators
- **Differences**: .NET, testing-focused

---

### E. Haskell Ecosystem

#### E.1 QuickCheck (Original)
- **Hackage**: https://hackage.haskell.org/package/QuickCheck
- **Approach**: The original, type-class based with Arbitrary

#### E.2 Hedgehog
- **Repository**: https://github.com/hedgehogqa/haskell-hedgehog
- **Hackage**: https://hackage.haskell.org/package/hedgehog
- **Key innovation**: Integrated shrinking via shrink trees
- **Differences**: Testing-focused

#### E.3 SmallCheck / Lazy SmallCheck
- **Hackage**: https://hackage.haskell.org/package/lazysmallcheck
- **Approach**: Exhaustive testing up to depth bound
- **Differences**: Bounded exhaustive, not random

---

### F. JavaScript/TypeScript Ecosystem

#### F.1 fast-check
- **Repository**: https://github.com/dubzzz/fast-check
- **Website**: https://fast-check.dev/
- **Author**: Nicolas Dubien (since 2017)
- **Approach**: TypeScript property testing with arbitraries
- **Design goals**: Strong types, easy `map`/`chain`, `fc.pre()` for preconditions
- **Differences**: Testing-focused, no columnar output

---

### G. Database Benchmark Generators

#### G.1 DuckDB TPC Extensions
- **Docs**: https://duckdb.org/docs/stable/core_extensions/tpch
- **Usage**: `CALL dbgen(sf = 1);`
- **Blog**: https://xuanwo.io/links/2025/02/duckdb-is-the-best-tpc-data-generator/

#### G.2 TPC-H dbgen (Original C)
- **Repository**: https://github.com/electrum/tpch-dbgen
- **Limitations**: Not open source, fails on newer GCC

#### G.3 HammerDB
- **Website**: https://www.hammerdb.com/
- **Approach**: GPL-licensed database benchmarking

---

### H. Synthetic Data Platforms

#### H.1 Awesome Lists
- **statice/awesome-synthetic-data**: https://github.com/statice/awesome-synthetic-data
- **gretelai/awesome-synthetic-data**: https://github.com/gretelai/awesome-synthetic-data
- **joofio/awesome-data-synthesis**: https://github.com/joofio/awesome-data-synthesis

#### H.2 Notable Tools
- **ydata-synthetic**: https://github.com/ydataai/ydata-synthetic — GANs for tabular/time-series
- **Synthea**: Open source synthetic patient generator
- **DoppelGANger**: GAN-based synthetic data
- **Gretel**: Commercial synthetic data APIs

---

## Part IV: Comparison Matrix

| Feature | data-comb | proptest | Hypothesis | SDV | tpchgen-rs | Synth |
|---------|-----------|----------|------------|-----|------------|-------|
| Combinator-based | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Native Arrow output | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| Pull-based/Lazy | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Statistical distributions | ✅ | ✅ | ✅ | ✅ (ML) | ❌ | ✅ |
| Clustering patterns | ✅ | ❌ | ❌ | ✅ (learned) | ❌ | ❌ |
| Bulk generation focus | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ |
| General-purpose | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Deterministic | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| Shrinking | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ |

---

## Part V: What Makes data-comb Unique

Based on this research, **data-comb/populate occupies a unique niche**:

### 1. Combinator + Arrow
No other library combines parser-combinator-style composition with native Arrow RecordBatch output.

### 2. Pull-based Columnar
Lazy iteration directly into columnar format without intermediate row representation. Property testing libraries (proptest, Hypothesis) are pull-based but produce single values, not columns.

### 3. Statistical Patterns for Analytics
Built-in support for clustering, runs, monotonic sequences — patterns that matter for columnar compression and query performance. Property testing libraries focus on edge cases, not realistic data distributions.

### 4. Tuple Schema Composition
Define table schemas as tuples `(Col1, Col2, Col3)` that materialize directly as Arrow columns. No other tool has this.

### 5. Multiple Sink Formats
Same distribution produces Vec rows, columnar Vecs, Arrow RecordBatch, or JSON — without changing the generator definition.

---

## Part VI: Theoretical Connections

### The Gen Monad Pattern

data-comb's `Distribution<T>` trait is essentially the `Gen` monad from QuickCheck:

```
Gen a ≈ RNG → (a, RNG)
Distribution<T> ≈ impl Fn(&mut RNG) → SampleIter<T>
```

The key combinators map directly:
- `map` → transform values
- `flatMap`/`chain` → dependent generation
- `oneOf`/`frequency` → weighted choice
- `tuple` → product types

### Parser Combinator Duality

| Parser | Generator |
|--------|-----------|
| Consumes input | Produces output |
| `String → Maybe (a, String)` | `RNG → (a, RNG)` |
| Sequencing via `>>=` | Sequencing via `>>=` |
| Choice via `<|>` | Choice via `oneOf` |

data-comb makes this duality explicit by adopting parser combinator vocabulary (combinators, composition) for the generation domain.

---

## Part VII: Potential Feature Ideas from Research

1. **From Hypothesis**: `@composite` pattern for complex multi-step generation
2. **From Hedgehog**: Integrated shrinking trees (if testing use case emerges)
3. **From SDV**: Distribution fitting from sample data
4. **From QuickChick**: Proof-carrying generators (for formal verification)
5. **From fast-check**: Race condition detection via replay
6. **From BigBench**: Multi-source data fusion patterns

---

## Conclusion

**data-comb/populate is novel** in combining:
1. Combinator-based composition (like proptest/Hypothesis/QuickCheck)
2. Native Arrow/columnar output (like tpchgen-arrow)
3. Statistical distribution control (like SDV but programmatic)
4. Bulk data generation focus (like Synth/dbgen)

The closest conceptual relatives are:
- **proptest** (combinator pattern, but testing-focused, no Arrow)
- **tpchgen-arrow** (Arrow output, but fixed schema)
- **Hypothesis** (rich combinators, but Python, testing-focused)

No existing library combines all of these into a **programmatic, combinator-based, columnar-first bulk data generation library for Rust**.

---

## References

### Foundational Papers
- Claessen & Hughes. [QuickCheck: A Lightweight Tool for Random Testing of Haskell Programs](https://www.cs.tufts.edu/~nr/cs257/archive/john-hughes/quick.pdf). ICFP 2000.
- Hutton & Meijer. [Monadic Parser Combinators](https://people.cs.nott.ac.uk/pszgmh/monparsing.pdf). 1996.
- Hutton & Meijer. [Monadic Parsing in Haskell](https://www.cs.tufts.edu/~nr/cs257/archive/graham-hutton/monadic-parsing-jfp.pdf). JFP 1998.
- Wadler. [Monads for Functional Programming](https://homepages.inf.ed.ac.uk/wadler/papers/marktoberdorf/baastad.pdf). 1995.
- Runciman et al. [SmallCheck and Lazy SmallCheck](https://dl.acm.org/doi/10.1145/1543134.1411292). Haskell 2008.
- Lampropoulos et al. [Generating Good Generators for Inductive Relations](https://dl.acm.org/doi/10.1145/3158133). POPL 2018.

### Blog Posts & Tutorials
- Dubien. [Your own property based testing framework](https://dev.to/dubzzz/your-own-property-based-testing-framework-part-1-generators-3afi)
- de Vries. [Integrated versus Manual Shrinking](https://www.well-typed.com/blog/2019/05/integrated-shrinking/)
- Seemann. [Test Data Generator monad](https://blog.ploeh.dk/2023/02/27/test-data-generator-monad/)
- Palmieri. [Introduction to Property-Based Testing in Rust](https://lpalmieri.com/posts/an-introduction-to-property-based-testing-in-rust/)

### Tools & Libraries
- [proptest](https://github.com/proptest-rs/proptest) — Rust property testing
- [Hypothesis](https://hypothesis.readthedocs.io/) — Python property testing
- [fast-check](https://github.com/dubzzz/fast-check) — TypeScript property testing
- [SDV](https://sdv.dev/) — ML-based synthetic data
- [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) — TPC-H with Arrow
- [Hedgehog](https://github.com/hedgehogqa/haskell-hedgehog) — Haskell with integrated shrinking
- [FsCheck](https://fscheck.github.io/FsCheck/) — .NET property testing
- [ScalaCheck](https://scalacheck.org/) — Scala property testing

### Columnar/Analytics Research
- Zeng et al. [An Empirical Evaluation of Columnar Storage Formats](https://arxiv.org/abs/2304.05028). VLDB 2023.
- [On Big Data Benchmarking](https://arxiv.org/pdf/1402.5194). 2014.
- [BigBench](https://dl.acm.org/doi/10.1145/2463676.2463712). SIGMOD 2013.
