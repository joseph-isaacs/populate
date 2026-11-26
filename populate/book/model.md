# Rows, columns and distributions

Three traits in `populate-core` carry everything else.

**`Columnar`** says how rows of a type are stored. An `i64` row is stored
in a `Vec<i64>`, a `String` in a `StrCol` (offsets and bytes, Arrow's
`Utf8` layout), an `Option<T>` in `T`'s columns plus a validity bitmap, and
a tuple `(A, B)` in a tuple of `A`'s and `B`'s columns. `Columns<T>` names
that storage type.

**`Sink`** is something values are appended to. Every column type is one.

**`Distribution`** is a stream of rows. It has two required methods:

```rust,ignore
trait Distribution {
    type Item: Columnar;
    fn collect_into(&mut self, cols: &mut Columns<Self::Item>, rng: &mut impl Rng, n: usize);
    fn skip(&mut self, n: u64);
}
```

`collect_into` appends the next `n` rows to `cols`. There is no way to ask
for a single row: every distribution works on whole columns, so a leaf
fills its column in one tight loop and a combinator runs its closure over
columns that are already in memory.

`skip` moves past `n` rows without producing them. A counter adds `n`, a
random leaf does nothing (its rows don't depend on their position), and a
combinator passes `n` on to its source. This is what lets a table start a
batch at row 1,000,000 without generating the rows before it.

## Tuples are rows of several columns

A tuple of distributions is a distribution of tuples, and it fills its
first column completely before it starts the second:

```rust
use populate::Distribution;
use populate::dist::{range, uniform};

fn rows() -> impl Distribution<Item = (i64, i32)> {
    (range(0i64, 1), uniform(1i32, 6))
}

let (ids, dice): (Vec<i64>, Vec<i32>) = rows().collect(&mut rand::rng(), 4);
assert_eq!(ids, [0, 1, 2, 3]);
assert!(dice.iter().all(|d| (1..=6).contains(d)));
```

Tuples nest, and go up to twelve fields; a wider table is a tuple of
tuples.

## A distribution is a stream, not a recipe

Collecting moves a distribution on: `range(0, 1)` gives `0..4`, then `4..8`
the next time. Clone a distribution to start again from the same point, and
`skip` it to start further on:

```rust
use populate::Distribution;
use populate::dist::range;

fn ids() -> impl Distribution<Item = i64> + Clone {
    range(0i64, 1)
}

let mut d = ids();
let mut later = d.clone();
later.skip(10);

let rng = &mut rand::rng();
assert_eq!(d.collect(rng, 3), [0, 1, 2]);
assert_eq!(d.collect(rng, 2), [3, 4]);
assert_eq!(later.collect(rng, 2), [10, 11]);
```

Table drivers rely on exactly this: rows `first..first + n` of a table are
the distribution, cloned, skipped `first` rows and collected `n`.
