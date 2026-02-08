# Claude Code Instructions for populate-2

## Three-Layer Architecture

The system has three layers. Values and Sinks are user-facing and must be flat, simple tuples. Distribution impls are internal and absorb all complexity.

- **Values** (`type Value`): flat tuples the user iterates over — `(i32, i64, bool)`, never HLists
- **Sinks** (`type Sink` / `SinkOf`): flat tuples of columns the user passes in and gets back — `(Vec<i32>, Vec<i64>)`
- **Distribution impls**: where tree sinks, HAppend, field splitting, SinkFlatten all live — the "complexity budget"

When making design changes: keep values and sinks as simple flat tuples for the user. If complexity must go somewhere, push it into Distribution impls, not into user-facing types.

## Tests

Tests MUST use `impl Distribution` return types, not concrete types. Always define helper functions that return `impl Distribution<Item = T>` and use those in tests.

```rust
// GOOD - use impl Distribution
fn make_my_dist() -> impl Distribution<Item = (i32, i64)> {
    (constant(1), range(0, 10))
}

#[test]
fn test_something() {
    let mut dist = make_my_dist();
    // ...
}

// BAD - using concrete type directly in test
#[test]
fn test_something() {
    let mut dist = (constant(1), range(0, 10));  // Don't do this
    // ...
}
```

This ensures we test the trait interface, not implementation details.

## Test Assertions

Every test MUST assert that the sink contains the expected values. Don't just check iteration order - verify the actual data:

```rust
#[test]
fn test_col_by_col_order() {
    let log: Log = Log::default();
    let mut dist = make_tracked_tuple(&log);
    let sink = dist.collect_into(&mut rng(), (Vec::new(), Vec::new()), 3);

    // MUST assert sink values
    assert_eq!(sink.0, vec![0, 1, 2]);
    assert_eq!(sink.1, vec![100, 101, 102]);

    // Then check order
    assert_eq!(*log.borrow(), vec!["COL1", "COL1", "COL1", "COL2", "COL2", "COL2"]);
}
```
