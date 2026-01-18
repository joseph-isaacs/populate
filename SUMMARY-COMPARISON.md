# Summary: data-comb vs Existing Solutions

## What is data-comb/populate?

A Rust library for **composable, deterministic test data generation** targeting columnar formats (Arrow). Uses iterator combinators to define data distributions that materialize into columns.

```rust
let dist = (
    Range::new(1i64, 1000),           // user_id
    Pick::new(["active", "inactive"]), // status
    list(Range::new(0, 5), Range::new(0, 100)), // scores
);
let batch: RecordBatch = dist.collect(&mut rng, 10_000);
```

---

## The Landscape: Three Worlds

### World 1: Property Testing Libraries (Combinator-Based)

| Library | Language | Arrow? | Bulk? | Focus |
|---------|----------|--------|-------|-------|
| **proptest** | Rust | ❌ | ❌ | Testing |
| **Hypothesis** | Python | ❌ | ❌ | Testing |
| **QuickCheck** | Haskell | ❌ | ❌ | Testing |
| **ScalaCheck** | Scala | ❌ | ❌ | Testing |
| **fast-check** | TypeScript | ❌ | ❌ | Testing |
| **FsCheck** | .NET | ❌ | ❌ | Testing |

**What they do well:**
- Rich combinator composition (`map`, `flatMap`, `filter`, `oneOf`)
- Shrinking to find minimal counterexamples
- Type-directed generation

**What they lack:**
- No columnar/Arrow output
- Optimized for single values, not millions of rows
- Testing focus (shrinking) adds overhead for bulk generation

---

### World 2: Bulk Data Generators

| Tool | Language | Combinators? | Arrow? | Schema |
|------|----------|--------------|--------|--------|
| **Synth** | Rust | ❌ (YAML) | ❌ | Flexible |
| **tpchgen-rs** | Rust | ❌ | ✅ | Fixed TPC-H |
| **DuckDB dbgen** | C++/SQL | ❌ | ❌ | Fixed TPC-H |
| **Datafaker** | Java | ❌ | ❌ | Flexible |
| **SDV/CTGAN** | Python | ❌ | ❌ | Learned |

**What they do well:**
- Generate millions of rows quickly
- Some have Arrow output (tpchgen-rs)
- Some learn from real data (SDV)

**What they lack:**
- Not programmatically composable
- Config-driven (YAML/JSON) or fixed schemas
- Can't define custom statistical patterns in code

---

### World 3: Fake Data Libraries

| Library | Language | Focus |
|---------|----------|-------|
| **fake-rs** | Rust | Names, emails, addresses |
| **Faker** | Python | Names, emails, addresses |
| **Mimesis** | Python | Names, emails, addresses |
| **Datafaker** | Java | Names, emails, addresses |

**What they do well:**
- Realistic semantic data (person names, addresses, etc.)

**What they lack:**
- Single-value generation, not streaming
- No statistical distributions or patterns
- No columnar output

---

## Where data-comb Fits

```
                    Combinator-Based
                          │
           ┌──────────────┼──────────────┐
           │              │              │
      proptest        data-comb      Hypothesis
      (testing)     ◄── UNIQUE ──►   (testing)
           │              │              │
           │         Arrow Output        │
           │              │              │
           └──────────────┼──────────────┘
                          │
                    Bulk Generation
                          │
           ┌──────────────┼──────────────┐
           │              │              │
        Synth        tpchgen-rs        SDV
      (config)      (fixed schema)    (ML)
```

**data-comb is the only library in the center** — combining:
- ✅ Combinator composition (like proptest)
- ✅ Arrow output (like tpchgen-rs)
- ✅ Bulk generation (like Synth)
- ✅ Programmatic (unlike YAML configs)
- ✅ General-purpose (unlike fixed TPC-H)

---

## Direct Comparisons

### data-comb vs proptest

| Aspect | data-comb | proptest |
|--------|-----------|----------|
| Primary use | Bulk data generation | Property testing |
| Output | Arrow RecordBatch, Vec columns | Single values |
| Shrinking | ❌ Not needed | ✅ Core feature |
| Scale | Millions of rows | Hundreds of test cases |
| Patterns | Clustering, runs, monotonic | Edge cases, boundaries |

**Verdict**: Different goals. proptest finds bugs; data-comb creates datasets.

---

### data-comb vs tpchgen-rs

| Aspect | data-comb | tpchgen-rs |
|--------|-----------|------------|
| Schema | Any (you define it) | Fixed TPC-H only |
| Composition | Combinators | Hardcoded |
| Arrow output | ✅ | ✅ |
| Extensibility | Add new distributions | Fork and modify |

**Verdict**: tpchgen-rs is faster for TPC-H specifically. data-comb is for everything else.

---

### data-comb vs Synth

| Aspect | data-comb | Synth |
|--------|-----------|-------|
| Definition | Rust code | YAML/JSON files |
| Type safety | Compile-time | Runtime |
| Composition | Programmatic | Declarative |
| Arrow output | ✅ | ❌ |

**Verdict**: Synth is easier for non-programmers. data-comb is more powerful for Rust developers.

---

### data-comb vs Hypothesis

| Aspect | data-comb | Hypothesis |
|--------|-----------|------------|
| Language | Rust | Python |
| Combinators | ✅ Similar | ✅ Rich |
| `@composite` | Tuples | Decorator |
| Arrow | ✅ Native | ❌ |
| Focus | Bulk generation | Testing |

**Verdict**: Hypothesis is the closest conceptually, but Python and testing-focused.

---

## Lessons Learned

### 1. The Gen Monad is Universal

Every property testing library independently discovered the same pattern:

```
Gen a ≈ RNG → (a, RNG)
```

This is a **State monad** threading random state. data-comb's `Distribution<T>` trait is the same pattern, just with iterators instead of single values.

**Implication**: The combinator vocabulary (`map`, `flatMap`, `oneOf`, `frequency`) is battle-tested across 40+ language ports. data-comb should use the same names.

---

### 2. Parser/Generator Duality

Parsers and generators are **duals**:

| Parser | Generator |
|--------|-----------|
| Consumes input | Produces output |
| `String → (a, String)` | `RNG → (a, RNG)` |
| Fails on invalid input | Always succeeds |

The monadic parser combinator papers (Hutton & Meijer) provide the theoretical foundation. data-comb could cite these to explain its design.

---

### 3. Shrinking is Optional for Bulk Generation

Property testing libraries invest heavily in shrinking (finding minimal counterexamples). But for bulk data generation:
- No counterexamples to minimize
- Shrinking adds overhead
- Not needed

**Implication**: data-comb correctly omits shrinking. If testing use cases emerge, it could be added later (see Hedgehog's integrated shrinking).

---

### 4. The Gap: Columnar + Combinators

No existing library combines:
- Programmatic combinator composition
- Native columnar (Arrow) output
- Bulk generation focus

This is data-comb's unique value proposition.

---

### 5. Statistical Patterns Matter for Analytics

Property testing focuses on **edge cases** (empty lists, max values, special characters).

Analytics benchmarks need **realistic distributions**:
- Clustering (sorted runs, locality)
- Skew (Zipf, power law)
- Nullability patterns
- Monotonic sequences

These affect columnar compression ratios and query performance. data-comb's `Runs`, `Monotonic`, etc. address this — proptest/Hypothesis don't.

---

### 6. Tuple Composition is Powerful

data-comb's tuple-based schema definition:

```rust
let dist = (Col1, Col2, Col3);
```

...that materializes directly into Arrow columns is unique. Other libraries either:
- Generate single values (proptest)
- Use config files (Synth)
- Have fixed schemas (tpchgen)

---

## Feature Ideas from Research

| Source | Feature | Applicability |
|--------|---------|---------------|
| **Hypothesis** | `@composite` decorator | Could add `composite!` macro |
| **Hedgehog** | Integrated shrinking | If testing use case emerges |
| **SDV** | Fit distributions from data | Learn from sample datasets |
| **fake-rs** | Semantic providers | Integrate for names/emails |
| **fast-check** | Race condition replay | For concurrent data pipelines |
| **tpchgen-rs** | Direct Parquet output | Add `populate-sink-parquet` |

---

## Conclusion

**data-comb/populate occupies a unique niche:**

| Capability | data-comb | Closest Alternative |
|------------|-----------|---------------------|
| Rust + combinators | ✅ | proptest (but no Arrow) |
| Arrow output | ✅ | tpchgen-rs (but fixed schema) |
| Statistical patterns | ✅ | SDV (but ML-based, Python) |
| Bulk generation | ✅ | Synth (but YAML, no Arrow) |

**No existing library combines all four.**

The theoretical foundation (Gen monad, parser combinator duality) is well-established. data-comb applies these ideas to a new domain: columnar analytics data generation.
