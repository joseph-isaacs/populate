# populate-sink-vortex

Import `DistributionVortex` to collect a Populate distribution directly into a
Vortex `ArrayRef`. The sink constructs native arrays from the generated columns.
It supports 8–64 bit signed and unsigned integers, f32, f64, booleans, UTF-8
strings, lists, fixed-size lists, nullable values, and tuple structs.

```rust
use populate_builtins::{constant, list, named, range};
use populate_sink_vortex::DistributionVortex;

let mut rows = (named("id", range(0u64, 1)), named("tags", list(constant(3i32), constant("abc"))));
let array = rows.collect_vortex(&mut rng, 1_000, &mut ctx)?;
```

Pass a Populate-compatible rand 0.9 RNG and a Vortex execution context. List
and string constructors use the context to validate offsets. Tuple fields
default to c0, c1, and so on. Use named fields to supply a schema.

The sink has its own Cargo workspace because the pinned Vortex revision
requires a newer Rust toolchain than Populate. To run its tests:

```sh
cargo +1.100.0 test --locked --manifest-path sink/vortex/Cargo.toml
```

A benchmark inside the Vortex workspace should use the same Vortex crates
as the sink. When depending on this sink by path, add the following overrides
to the Vortex workspace root so Cargo uses its local array types:

```toml
[patch."https://github.com/vortex-data/vortex"]
vortex-array = { path = "vortex-array" }
vortex-buffer = { path = "vortex-buffer" }
vortex-error = { path = "vortex-error" }
vortex-session = { path = "vortex-session" }
```
