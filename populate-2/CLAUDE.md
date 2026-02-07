# Claude Code Instructions for populate-2

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
