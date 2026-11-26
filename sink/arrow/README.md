# populate-sink-arrow

Arrow output for populate: collected columns become Arrow arrays and
`RecordBatch`es without copying, with the schema read off the distribution.

Part of [populate](https://github.com/joseph-isaacs/populate); most users depend on
the `populate` crate, which re-exports this one. API docs: <https://docs.rs/populate-sink-arrow>.
