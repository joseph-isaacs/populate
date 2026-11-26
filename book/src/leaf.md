# Writing a leaf

A leaf implements `Distribution` directly: `collect_into` fills its column
for `n` rows, and `skip` moves past `n` rows. Here is a leaf of square
numbers:

```rust
use populate::{Columns, Distribution};
use rand::Rng;

/// 0, 1, 4, 9, ...
#[derive(Clone)]
struct Squares {
    next: u64,
}

impl Distribution for Squares {
    type Item = u64;

    fn collect_into(&mut self, cols: &mut Columns<u64>, _rng: &mut impl Rng, n: usize) {
        let end = self.next + n as u64;
        cols.extend((self.next..end).map(|i| i * i));
        self.next = end;
    }

    // the next row is a function of the row number alone, so skipping is
    // moving the counter
    fn skip(&mut self, n: u64) {
        self.next += n;
    }
}

fn squares() -> impl Distribution<Item = u64> + Clone {
    Squares { next: 0 }
}

let mut d = squares();
d.skip(3);
assert_eq!(d.collect(&mut rand::rng(), 3), [9, 16, 25]);
```

Two rules make a leaf work in a table:

- **`collect_into` appends.** `cols` may already hold rows (a tuple
  collects into columns it is building), so push to the end and never
  clear.
- **`skip` must land exactly on row `n`, and should be quick.** A table
  starts every batch by cloning the distribution and skipping to the
  batch's first row, so a `skip` that generated and dropped `n` rows would
  make the last batch cost as much as the whole table. A leaf whose rows
  don't depend on their position (most random leaves) does nothing.

## Randomness that skips: `SeekRng` and `draws`

The `rng` passed to `collect_into` is seeded per batch, so it is fine for
values that need only be random. Generators that must match a reference
tool row for row instead give each column its own stream that can jump
ahead, a `SeekRng`, and read it through `draws`, which takes `K` draws per
row and skips by advancing the stream `K * n` draws:

```rust
use populate::{Distribution, SeekRng};
use populate::dist::{derive, draws};

/// A tiny linear congruential stream.
#[derive(Clone)]
struct Lcg(u64);

impl SeekRng for Lcg {
    type Draw = u32;

    fn next_draw(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.0 >> 33) as u32
    }

    fn advance(&mut self, n: u64) {
        for _ in 0..n {
            self.next_draw();
        }
    }
}

/// A die roll per row, the same rows however the table is split.
fn dice() -> impl Distribution<Item = u32> + Clone {
    derive(draws::<_, 1>(Lcg(42)), |&[x]| x % 6 + 1)
}

let rng = &mut rand::rng();
let whole = dice().collect(rng, 10);
let mut tail = dice();
tail.skip(6);
assert_eq!(tail.collect(rng, 4), whole[6..]);
```

A real stream jumps in constant time (a Lehmer generator raises its
multiplier to the `n`th power); the loop above is only for brevity.
