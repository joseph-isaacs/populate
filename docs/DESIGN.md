# Synthetic Data Combinator Library Design

This document describes the proposed architecture for data-comb, comparing it with the current implementation and outlining migration paths.

## Table of Contents

1. [Core Philosophy](#core-philosophy)
2. [Current Implementation](#current-implementation)
3. [Proposed Architecture](#proposed-architecture)
4. [Detailed Comparison](#detailed-comparison)
5. [Migration Strategy](#migration-strategy)

---

## Core Philosophy

The proposed design maintains the current strengths while adding:

- **Zero intermediate allocation** during generation
- **Type-safe boundaries**, runtime flexibility in middle
- **Columnar when possible**, row-wise when necessary (dependencies)
- **Two tiers** that interoperate (typed and erased)
- **Automatic optimization** via dependency analysis

---

## Current Implementation

### Overview

The current implementation uses a pull-based infinite iterator model:

```
Distribution (factory)
    ↓
sample() → SampleIter (infinite stream)
    ↓
next(rng) → Item (pull on demand)
    ↓
collect<C>() with Collector marker
    ↓
Sink implementations (Vec/Arrow/JSON)
    ↓
Final Output
```

### Core Traits

```rust
/// Factory that creates sample iterators
pub trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}

/// Infinite iterator that pulls RNG on each call
pub trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}
```

### Dependency Handling

Dependencies are implicit via composition:

```rust
let dist = (
    Range::new(1i64, 100),           // col A
    Range::new(100i32, 10_000),      // col B
).map(|(a, b)| (a, b, a * 10));      // col C depends on A
```

### Sink Abstraction

```rust
pub trait Sink<T> {
    type Output;
    fn push(&mut self, value: T);
    fn extend(&mut self, iter: impl Iterator<Item = T>);
    fn finish(self) -> Self::Output;
}

pub trait ListSink<T> {
    type Output;
    fn start_list(&mut self);
    fn push_element(&mut self, value: T);
    fn end_list(&mut self);
    fn finish(self) -> Self::Output;
}
```

### Collection API

```rust
let data: Vec<i32> = dist.collect::<Col>(&mut rng, 1000);
let cols: (Vec<i32>, Vec<String>) = dist.collect::<Cols>(&mut rng, 1000);
let batch: RecordBatch = dist.collect::<RecordBatchW>(&mut rng, 1000);
```

### Strengths

| Feature | Benefit |
|---------|---------|
| Infinite iterator model | Clean separation of generation from collection |
| Rich combinators | Domain-specific operations (Pick, Join, Runs) |
| Zero intermediate allocation | Achieved via wrapper iterators |
| Tuple composition (up to 7) | Ergonomic multi-column definitions |
| `Seeded<R, D>` | Independent RNG subtrees |
| Multiple sink backends | Vec, Arrow, JSON already implemented |

### Limitations

| Limitation | Impact |
|------------|--------|
| No explicit graph | Cannot analyze dependencies |
| Always row-wise | No columnar optimization for independent columns |
| No runtime type info | Cannot build dynamic/config-driven tables |
| No automatic parallelization | Independent subgraphs still sequential |

---

## Proposed Architecture

### Type System

#### Schema Types (compile-time type tags)

```rust
SInt64, SFloat64, SBool, SString, SList<E>, SStruct<Fields>
```

#### Type Tags (runtime mirror)

```rust
enum TypeTag {
    Int64,
    Float64,
    Bool,
    String,
    List(Box<TypeTag>),
    Struct(Vec<(String, TypeTag)>),
}
```

#### Erased Values (runtime value carrier)

```rust
enum ErasedValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    List(Vec<ErasedValue>),
    Struct(Vec<ErasedValue>),
}
```

### Generator Trait

#### Typed Generator

```rust
trait Gen<S: Schema> {
    fn gen(&self, ctx: &mut Ctx) -> S::Value;
}
```

#### Erased Generator

```rust
trait ErasedGen {
    fn type_tag(&self) -> TypeTag;
    fn gen_erased(&self, ctx: &mut Ctx) -> ErasedValue;
}
```

#### Bridge

Any `Gen<S>` can become `Box<dyn ErasedGen>` via blanket impl. `TypeTag` derived from `S`.

### Graph Model

#### Node Types

| Node | Description |
|------|-------------|
| `Source(ErasedGen)` | Root, no dependencies |
| `Map(NodeId, Fn)` | Single dependency |
| `Map2(NodeId, NodeId, Fn)` | Two dependencies |
| `MapN(Vec<NodeId>, Fn)` | N dependencies |
| `Filter(NodeId, Pred)` | Conditional |
| `FlatMap(NodeId, Fn)` | List expansion |

#### Graph Structure

```rust
struct Table {
    arena: Vec<Node>,      // all nodes
    outputs: Vec<NodeId>,  // which nodes are outputs
}
```

#### Column Handle

```rust
struct Col<S> {
    id: NodeId,
    _marker: PhantomData<S>,
}
```

Lightweight, `Copy`, typed. Just points into arena.

### Dependency Analysis

#### Classification

| Category | Definition | Strategy |
|----------|------------|----------|
| Root | No dependencies | Source of data |
| Leaf | No dependents (is output) | Final sink target |
| Interior | Has both deps and dependents | Value consumed immediately |
| Independent | Subgraph with no cross-edges | Can parallelize |

#### Rules

1. **Interior nodes** → must be row-wise (value needed same iteration)
2. **Roots feeding interior** → row-wise (can't buffer without allocation)
3. **Independent leaf-only roots** → can be columnar
4. **Independent subgraphs** → can run in parallel

#### Algorithm

```
1. Build adjacency list (deps) and reverse (dependents)
2. Topo-sort
3. Mark nodes: has_dependent? is_output?
4. Partition into row-wise set vs columnar set
5. Identify independent subgraphs (union-find or DFS)
```

### Generation Strategy

#### Single-Pass Row-Wise (for dependent subgraph)

```rust
for i in 0..n {
    val_a = gen_a(ctx);      // → sink_a.push(val_a)
    val_b = gen_b(ctx);      // → sink_b.push(val_b)
    val_c = f(val_a, val_b); // → sink_c.push(val_c)
    ctx.advance();
}
```

No intermediate storage. Values flow directly to sinks.

#### Columnar (for independent roots with no dependents)

```rust
gen_d.gen_batch(ctx, n, &mut sink_d);  // can be SIMD optimized
```

#### Mixed

Run independent columnar generations (possibly parallel), then row-wise pass for dependent subgraph.

### Two-Tier System

#### Tier 1: Fully Typed

| Aspect | Description |
|--------|-------------|
| When | Static graph, small arity, known at compile time |
| Graph | Encoded in tuple types |
| Dispatch | Monomorphized, inlined |
| Output | Directly typed, no checks |
| Performance | Maximum, zero overhead |

#### Tier 2: Erased (Dynamic)

| Aspect | Description |
|--------|-------------|
| When | Dynamic graph, config-driven, large/variable columns |
| Graph | Arena + NodeId handles |
| Dispatch | Trait objects, match on TypeTag |
| Output | Assert expected types at runtime |
| Performance | Small overhead, but flexible |

### Interop Between Tiers

#### Typed → Erased

Any typed `Col<S>` can be added to dynamic table:

```rust
typed_col.erase() -> (NodeId, TypeTag)
```

#### Erased → Typed

At collection time, declare expected output type:

```rust
dyn_table.collect::<(Vec<i64>, Vec<f64>)>()
```

Runtime checks TypeTags match, then generates.

#### Mixed Graph

Can have typed subgraph embedded in erased graph:

```rust
TypedBundle<(Col<A>, Col<B>, Col<C>)> implements ErasedSubgraph
```

Optimizer sees typed bundle, generates optimal code for that part.

#### Gradual Typing

Start erased (prototyping), progressively add type annotations to hot paths. System uses typed fast path where annotations exist.

### Sink Abstraction

#### Traits

```rust
trait Sink<S: Schema> {
    fn push(&mut self, value: S::Value);
    fn reserve(&mut self, n: usize);
}

trait ErasedSink {
    fn push_erased(&mut self, value: ErasedValue);
}
```

#### Implementations

| Sink | Output |
|------|--------|
| `VecSink<S>` | `Vec<S::Value>` |
| `ArrowSink` | Arrow arrays (primitive, list, struct) |
| `JsonSink` | JSON string or writer |
| `CsvSink` | CSV writer |
| `ParquetSink` | Parquet file |
| `NullSink` | Discard (benchmarking) |

#### Columnar Sinks (Arrow)

| Type | Representation |
|------|----------------|
| Primitive | `Vec<T>` buffer |
| List | `offsets: Vec<i64>`, `values: Vec<T>` |
| Struct | Tuple of child sinks |
| Nullable | `values` + `null_bitmap: BitVec` |

### Collection API

#### Batch Creation

```rust
gen.batch(n)                    // Batch with count
gen.batch(n).with_seed(42)      // Reproducible
```

#### Collection Methods

```rust
.to_vec()                       // → Vec<T>
.collect::<ToJson>()            // → String
.collect::<ToArrow>()           // → ArrowArray
.iter()                         // → impl Iterator
```

#### Table Collection

```rust
table.outputs(a, b, c)
     .batch(n)
     .collect::<(ArrowInt64, ArrowFloat64, ArrowInt64)>()
```

Runtime verifies types match, then generates.

### Combinator Operations

| Combinator | Signature | Description |
|------------|-----------|-------------|
| `map` | `Col<A> → (A→B) → Col<B>` | Transform |
| `map2` | `Col<A>, Col<B> → (A,B→C) → Col<C>` | Combine two |
| `zip` | `Col<A>, Col<B> → Col<(A,B)>` | Tuple |
| `flat_map` | `Col<A> → (A→Iter<B>) → Col<List<B>>` | Expand to list |
| `filter_map` | `Col<A> → (A→Option<B>) → Col<Option<B>>` | Partial transform |
| `bind` | `Col<A> → (A→Gen<B>) → Col<B>` | Dependent generator |

### Context (Threaded State)

```rust
struct Ctx {
    idx: usize,    // current row index
    rng: Rng,      // random state
    // extensible: add more state as needed
}
```

Passed through all generation. Stateful generators (random) mutate it.

### Optimization Opportunities

| Optimization | When |
|--------------|------|
| SIMD batch generation | Independent primitive columns |
| Parallel subgraphs | Independent subgraphs identified |
| Columnar write | Leaf nodes with no dependents |
| Inline monomorphization | Tier 1 typed paths |
| Arena allocation | Reuse buffers across batches |

---

## Detailed Comparison

### Architecture Summary

| Aspect | Current | Proposed |
|--------|---------|----------|
| Model | Pull-based infinite iterator | Explicit DAG with arena |
| Dependencies | Implicit via composition | Explicit graph nodes |
| Type handling | Monomorphic only | Two-tier: Typed + Erased |
| Schema | None (types via generics) | Compile-time tags + runtime TypeTag |
| Optimization | Manual | Automatic via dependency analysis |

### Generator Model

**Current:**
```rust
trait Distribution<T> {
    type Iter: SampleIter<Item = T>;
    fn sample(&mut self) -> Self::Iter;
}

trait SampleIter: Clone {
    type Item;
    fn next(&mut self, rng: &mut impl Rng) -> Self::Item;
}
```

**Proposed:**
```rust
trait Gen<S: Schema> {
    fn gen(&self, ctx: &mut Ctx) -> S::Value;
}

trait ErasedGen {
    fn type_tag(&self) -> TypeTag;
    fn gen_erased(&self, ctx: &mut Ctx) -> ErasedValue;
}
```

**Key Difference:** Current separates "create iterator" from "generate value". Proposed collapses them and adds `Ctx` carrying RNG and row index. Proposed also has explicit erasure for dynamic graphs.

### Column Representation

**Current:** Implicit in tuple structure
```rust
let dist = (
    Range::new(1i64, 100),
    Range::new(100i32, 10_000),
).map(|(a, b)| (a, b, a * 10));
```

**Proposed:** Explicit graph with handles
```rust
let a = table.source(rand_int);         // NodeId 0
let b = table.source(rand_float);       // NodeId 1
let c = table.map2(a, b, |x, y| ...);   // NodeId 2, deps: [0, 1]

struct Col<S> { id: NodeId, _marker: PhantomData<S> }
```

### Generation Strategy

**Current:** Always row-wise
```rust
for _ in 0..n {
    let row = iter.next(&mut rng);
    sink.push(row);
}
```

**Proposed:** Automatic mixed strategy
```rust
// Columnar for independent subgraphs (can be parallel/SIMD)
gen_d.gen_batch(ctx, n, &mut sink_d);

// Row-wise for dependent subgraph
for i in 0..n {
    val_a = gen_a(ctx);
    val_b = gen_b(ctx);
    val_c = f(val_a, val_b);
    // push to sinks...
}
```

### Feature Comparison

| Feature | Current | Proposed |
|---------|---------|----------|
| Explicit dependency graph | ✗ | ✓ |
| Automatic columnar optimization | ✗ | ✓ |
| Parallel subgraph execution | ✗ | ✓ |
| Runtime type checking | ✗ | ✓ |
| Dynamic/config-driven tables | ✗ | ✓ |
| Rich domain combinators | ✓ | Needs porting |
| Zero intermediate allocation | ✓ | ✓ |
| Multiple sink backends | ✓ | ✓ |
| Independent RNG streams | ✓ (Seeded) | ✓ (via Ctx) |

---

## Migration Strategy

### Phase 1: Foundation

Add core types alongside existing implementation:

```rust
// New module: src/graph/
mod schema;     // SInt64, SFloat64, TypeTag, ErasedValue
mod gen;        // Gen<S>, ErasedGen traits
mod ctx;        // Ctx struct
mod node;       // Node enum, NodeId
mod table;      // Table struct with arena
```

### Phase 2: Bridge Existing Distributions

Implement `Gen<S>` for existing `Distribution<T>` types:

```rust
impl<S, D> Gen<S> for DistributionAdapter<D>
where
    D: Distribution<S::Value>,
    S: Schema,
{
    fn gen(&self, ctx: &mut Ctx) -> S::Value {
        self.iter.next(&mut ctx.rng)
    }
}
```

### Phase 3: Graph Builder API

Add ergonomic table building:

```rust
let mut table = Table::new();
let a = table.source(range(1i64, 100));
let b = table.source(range(1.0f64, 10.0));
let c = table.map2(a, b, |x, y| x as f64 + y);
table.output(a);
table.output(b);
table.output(c);
```

### Phase 4: Dependency Analysis

Implement the analysis algorithm:

```rust
impl Table {
    fn analyze(&self) -> ExecutionPlan {
        // 1. Build adjacency lists
        // 2. Topo-sort
        // 3. Classify nodes
        // 4. Partition row-wise vs columnar
        // 5. Identify independent subgraphs
    }
}
```

### Phase 5: Optimized Execution

Add execution strategies:

```rust
impl ExecutionPlan {
    fn execute<S: SinkSet>(&self, ctx: &mut Ctx, n: usize, sinks: S) {
        // Run columnar passes (possibly parallel)
        // Run row-wise pass for dependent subgraph
    }
}
```

### Phase 6: Gradual Typing

Add typed bundle optimization:

```rust
// Hot path with full type info
let typed_bundle = TypedBundle::new((a, b, c));
table.add_typed_subgraph(typed_bundle);
```

### Compatibility Notes

1. **Existing distributions remain valid** - wrapped via adapter
2. **Existing sinks remain valid** - implement new trait via wrapper
3. **Existing collection API preserved** - add new API alongside
4. **Incremental adoption** - use new features only where needed

---

## Example: Mental Model

```rust
// User defines columns
let a = table.source(rand_int);        // Col<SInt64>
let b = table.source(rand_float);      // Col<SFloat64>
let c = table.map2(a, b, |x, y| ...);  // Col<SFloat64>
let d = table.source(inc);             // Col<SInt64>

// Graph built internally:
// [0: Source(rand_int)]
// [1: Source(rand_float)]
// [2: Map2(0, 1, fn)]
// [3: Source(inc)]

// Analysis determines:
// Subgraph {0, 1, 2}: dependent, must be row-wise
// Subgraph {3}: independent, can be columnar

// Generation executes:
// Row-wise pass for {a, b, c}
for i in 0..n {
    a_val = gen_a(ctx);  // → sink_a
    b_val = gen_b(ctx);  // → sink_b
    c_val = f(a, b);     // → sink_c
}

// Columnar pass for {d} (can be parallel with above)
d.gen_batch(n, sink_d);

// Collection with type checking:
table.collect::<(Arrow64, ArrowF64, ArrowF64, Arrow64)>()
// Runtime verifies [Int64, Float64, Float64, Int64] matches
```

---

## Summary

| Concept | Typed (Tier 1) | Erased (Tier 2) |
|---------|----------------|-----------------|
| Column | `Col<S>` | `NodeId + TypeTag` |
| Generator | `impl Gen<S>` | `Box<dyn ErasedGen>` |
| Graph | Implicit in types | Explicit arena |
| Dependencies | Compile-time | Runtime analysis |
| Output | Direct | Assert + cast |
| Interop | Can erase | Can assert types |
| Performance | Optimal | Small overhead |

The proposed design enables automatic optimization while maintaining the ergonomics and zero-allocation properties of the current implementation. Migration can be incremental, with existing code continuing to work via adapters.
