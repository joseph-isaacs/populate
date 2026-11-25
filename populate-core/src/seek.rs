//! [`SeekRng`]: a random stream that can jump ahead.

/// A deterministic stream of random draws that can move `n` draws ahead
/// without making them.
///
/// The benchmark generators' reference tools give every column its own
/// stream and reserve a fixed number of draws per row, so row `r` starts at
/// draw `r * per_row` whatever the rows before it used. A distribution built
/// on such a stream skips by jumping (`advance(n * per_row)`), which is what
/// lets a chunk of a table start at its first row without generating the
/// rows before it. `populate_builtins::draws` is the leaf that does this.
pub trait SeekRng: Clone {
    /// One draw.
    type Draw: Copy;

    /// The next draw.
    fn next_draw(&mut self) -> Self::Draw;

    /// Move past the next `n` draws.
    fn advance(&mut self, n: u64);

    /// The next `out.len()` draws, in order. Override it where a block of
    /// draws can be made faster than one at a time (independent lanes of a
    /// linear congruential generator, say).
    fn fill(&mut self, out: &mut [Self::Draw]) {
        for x in out {
            *x = self.next_draw();
        }
    }
}
