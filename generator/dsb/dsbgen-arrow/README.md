<!-- Modified from tpcdsgen-arrow 4.0.0 for the populate DSB variant. -->

> **Vendored copy.** This is tpcdsgen-arrow 4.0 from
> [tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs), modified to
> run on `dsbgen` (DSB's dsdgen changes). It is not published, and only
> arrow 60 is supported. The upstream README follows. See
> [`../NOTICE.md`](../NOTICE.md).

# TPC-DS Data Generator in Arrow format

Generate TPC-DS data directly into [Apache Arrow] format using the [tpcdsgen] and [arrow] crates.

[Apache Arrow]: https://arrow.apache.org/
[tpcdsgen]: https://crates.io/crates/tpcdsgen
[arrow]: https://crates.io/crates/arrow

Supports arrow 60 (default) and arrow 59:

```toml
# arrow 60
tpcdsgen-arrow = "..."
# arrow 59
tpcdsgen-arrow = { version = "...", default-features = false, features = ["arrow_59"] }
```

## Example usage

See [docs.rs page](https://docs.rs/tpcdsgen-arrow/latest/tpcdsgen_arrow/)

## Testing

This crate ensures correct results using two methods.

1. Basic functional tests are in Rust doc tests in the source code (`cargo test --locked -p tpcdsgen-arrow --doc`)
2. The `reparse` integration test ensures that the Arrow generators
   produce the same results as parsing the original DAT format (`cargo test --locked -p tpcdsgen-arrow --test reparse`)

## Contributing

Please see [CONTRIBUTING.md] for more information on how to contribute to this project.

[CONTRIBUTING.md]: https://github.com/datafusion-contrib/tpcgen-rs/blob/main/CONTRIBUTING.md
