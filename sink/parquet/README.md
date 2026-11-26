# populate-sink-parquet

Parquet output for populate: collected columns are encoded into Parquet
pages in place, without building Arrow arrays first.

Part of [populate](https://github.com/joseph-isaacs/populate); most users depend on
the `populate` crate, which re-exports this one. API docs: <https://docs.rs/populate-sink-parquet>.
