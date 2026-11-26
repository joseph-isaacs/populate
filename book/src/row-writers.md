# Columns computed from others

Most real columns depend on others: a total is a quantity times a price, a
receipt date follows a ship date. populate has three combinators for this.
Each one collects its *source* column by column into scratch columns, runs
your closure over them, and keeps only what the closure writes. The scratch
columns are reused from one collect to the next.

| you have | you want | use |
|---|---|---|
| one source row | one value | `derive(source, f)` |
| one source row | one string, written in place | `format(source, f)` |
| a block of source columns | whole output columns | `derive_columns(source, f)` |
| one source row | any number of child rows | `flat_map` ([next chapter](child-tables.md)) |

## `derive`: one row in, one value out

`f` gets each source row *in place*: a row of a `Vec<T>` column is a `&T`,
and a row of a tuple of columns is a tuple of such references, so a wide
row (a block of draws) is never copied onto the stack.

```rust
use populate::Distribution;
use populate::dist::{derive, uniform};

fn totals() -> impl Distribution<Item = i64> {
    derive((uniform(1i64, 10), uniform(100i64, 999)), |(&qty, &price)| qty * price)
}

let t = totals().collect(&mut rand::rng(), 1_000);
assert!(t.iter().all(|v| (100..=9_990).contains(v)));
```

## Sharing a source between columns

Two `derive`s over the same source each collect their own copy of it:

```rust,ignore
(derive(key(), f), derive(key(), g))   // key() collected twice per batch
```

That is fine for a counter. When the source is expensive, or `f` and `g`
share work, use one `derive` that returns a tuple. The source is collected
once and the tuple's fields become separate columns:

```rust
use populate::Distribution;
use populate::dist::{derive, named_all, uniform};

/// Ship date, and a receipt date 1 to 30 days after it.
fn dates() -> impl Distribution<Item = (i32, i32)> {
    named_all(
        &["ship", "receipt"],
        derive((uniform(0i32, 2_000), uniform(1i32, 30)), |(&ship, &lag)| (ship, ship + lag)),
    )
}

let (ship, receipt) = dates().collect(&mut rand::rng(), 1_000);
assert!(ship.iter().zip(&receipt).all(|(s, r)| (s + 1..=s + 30).contains(r)));
```

## `format`: strings written in place

`format` is `derive` for strings: `f(row, out)` writes each row's string
straight into the column's bytes through a `StrBuf`, so no `String` is
allocated per row. The row type picks the column: `String` for `StrCol`
(Arrow `Utf8`), or a generator's own string column.

```rust
use populate::Distribution;
use populate::dist::{format, range};

fn names() -> impl Distribution<Item = String> {
    format::<_, String, _>(range(1u64, 1), |&i, out| {
        out.push_str("Customer#");
        out.push_u64_padded(i, 9);
    })
}

let c = names().collect(&mut rand::rng(), 2);
assert_eq!(c.get(0), "Customer#000000001");
assert_eq!(c.get(1), "Customer#000000002");
```

## `derive_columns`: a block at a time

When several output columns share intermediate values, or the writer is
easier as plain loops over whole columns, `derive_columns(source, f)` hands
`f` the source's columns for the whole block and the output columns to
append to. `f` must append exactly one row per source row; that is checked.

```rust
use populate::Distribution;
use populate::dist::{derive_columns, uniform};

/// A trip's fare, tip and total, all from its distance in tenths of a mile.
fn fares() -> impl Distribution<Item = (i64, i64, i64)> {
    derive_columns(uniform(1i64, 300), |dist: &Vec<i64>, (fare, tip, total): &mut (Vec<i64>, Vec<i64>, Vec<i64>)| {
        for &d in dist {
            let f = 250 + d * 25;
            let t = f / 5;
            fare.push(f);
            tip.push(t);
            total.push(f + t);
        }
    })
}

let (fare, tip, total) = fares().collect(&mut rand::rng(), 100);
assert!(fare.iter().zip(&tip).zip(&total).all(|((f, t), s)| f + t == *s));
```

`derive_columns` is also how a column type with its own writer (a string
column filled from a shared buffer, say) is filled: `f` can call any method
the output columns have.

## How they skip

All three pass `skip(n)` on to their source: an output row depends only on
its source row, so skipping the source is skipping the output.
