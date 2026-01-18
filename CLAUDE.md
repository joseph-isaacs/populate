# Claude Code Instructions

## Build & Test Commands

```bash
cargo build
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

## Code Quality

- Run `cargo fmt` before committing
- Run `cargo clippy -- -D warnings` and fix all warnings
- Treat all compiler warnings as errors (`-D warnings`)

## Import Style

- Import names directly (not module-qualified) when there are no clashes
- `use` statements should be at module scope, never function scope unless needed to avoid conflicts

## Architecture: Core Traits

### `Distribution<T>` (populate-core)
Factory that produces sample iterators. The central abstraction—everything that generates data implements this.

### `SampleIter` (populate-core)
Infinite iterator producing values via `next(&mut rng) -> T`. Must be `Clone` with no accumulated state (clones produce same sequence given same RNG).

### `Sink<T>` / `ListSink<T>` (populate-core)
Consumers that accumulate values into outputs (Vec, Arrow RecordBatch, JSON). Sinks are the "write" side of the system.

### `Collector<D, T>` (populate-core)
Marker-based dispatch for collecting distributions into different output formats (e.g., `Col`, `Cols`, `RecordBatchW`).

## Design Principles

### 1. Compose existing combinators first

Before creating a new combinator, try to express it using existing ones:

```rust
// GOOD: Compose existing combinators
let phone = (
    constant("+1-"),
    range(100i32, 999),
    constant("-"),
    range(1000i32, 9999),
).map(|(a, b, c, d)| format!("{}{}{}{}", a, b, c, d));

// AVOID: New PhoneNumber combinator unless composition is insufficient
```

Only create a new combinator when:
- Composition would be significantly less efficient (e.g., repeated allocations)
- The pattern is used frequently across the codebase
- Type-level guarantees are needed that composition can't provide

### 2. Convenience functions over raw Distribution structs

Expose `fn foo(...) -> impl Distribution<T>` rather than expecting users to construct structs directly:

```rust
// GOOD: Convenience function
pub fn range<T: Clone>(min: T, max: T) -> Range<T, ()> {
    Range::new(min, max)
}

// Users write:
let dist = range(0, 100);

// NOT:
let dist = Range::<i32, ()>::new(0, 100);
```

### 3. Extension traits for fluent composition

Add `.method()` syntax via extension traits when it improves ergonomics:

```rust
// Extension trait enables:
let dist = pick_n::<3>(&COLORS).join(" ");

// Instead of:
let dist = Join::new(PickN::<_, _, 3>::new(&COLORS), " ");
```

### 4. Combinator categories (informal)

**Generators** - Create values from RNG alone:
- `range`, `constant`, `inc`, `cycle`, `pick_n`

**Transforms** - Wrap and modify another distribution:
- `map`, `join`, `seeded`

**Composites** - Combine multiple distributions:
- Tuples `(D1, D2, ...)`, `list`, `pick` (uses index distribution)

When adding new functionality, identify which category it belongs to and follow existing patterns in that category.
