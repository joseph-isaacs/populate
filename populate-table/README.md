# populate-table

Named, typed tables of any width from populate distributions, generated
as Arrow record batches on all cores with output that does not depend on
the thread count. The `parquet` feature writes them straight to Parquet.

Part of [populate](https://github.com/joseph-isaacs/populate); most users depend on
the `populate` crate, which re-exports this one. API docs: <https://docs.rs/populate-table>.
