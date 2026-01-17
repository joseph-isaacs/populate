# Future Work

## Complex Boolean Patterns

Beyond the basic `Bernoulli` + `Runs`, these patterns could be useful:

| Pattern | Description | Use case |
|---------|-------------|----------|
| `Periodic<N>` | Repeat every N: `[T,T,F,T,T,F,...]` | Predictable patterns |
| `Burst<D>` | Mostly one value, occasional bursts of other | Sparse events, validity masks |
| `Gradient` | Probability changes over sequence (0%→100%) | Testing boundary conditions |
| `Markov` | P(next) depends on current value | Realistic clustering |
| `Segments` | Different densities per segment | Mixed workloads |
| `ZipfRuns` | Run lengths follow Zipf (few long, many short) | Realistic run distributions |

### Examples

```rust
// Bursts: 95% false, but true values come in clusters of 5-10
Burst::new(false, 0.05, Range::new(5, 10))

// Gradient: starts 0% true, ends 100% true
Gradient::new(0.0, 1.0)

// Markov: 90% chance to stay same, 10% to flip
Markov::new(0.9)

// Segments: 3 segments with different densities
Segments::new(vec![
    (0.1, 1000),  // 10% true for 1000 values
    (0.9, 1000),  // 90% true for 1000 values
    (0.5, 1000),  // 50% true for 1000 values
])
```
