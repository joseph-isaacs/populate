# Introduction

populate generates tabular data column by column, straight into Arrow
arrays (and, with the `parquet` feature, Parquet files). A table is
described as a *distribution*: a value that knows how to fill the columns
of the next `n` rows, and how to jump ahead to row `n` without making the
rows before it. Every batch of a table can then be generated on its own,
on any thread, with the same result.

This book is a short guide to the ideas, in the order you need them:

1. [Rows, columns and distributions](model.md): the three core traits.
2. [A first table](first-table.md): leaves, names and `DistTable`.
3. [Columns computed from others](row-writers.md): `derive`,
   `derive_columns` and `format`, and which to use.
4. [Child tables](child-tables.md): `flat_map`, `children`, and several
   tables from one pass.
5. [Writing a leaf](leaf.md): implementing `Distribution` yourself.

The API reference (every type, its column layout and Arrow type) is the
rustdoc of each crate; `cargo doc --open -p populate` starts at the facade.
The crate map, install instructions and the generators are in the
[README](https://github.com/joseph-isaacs/populate#readme).

Build this book with [mdBook](https://rust-lang.github.io/mdBook/):

```sh
mdbook serve book
```

Its examples are compiled and run as doctests of the `populate` crate, so
`cargo test -p populate --doc` checks them.
