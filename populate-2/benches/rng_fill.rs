use rand::{Rng, SeedableRng, rngs::SmallRng};
use rand_chacha::ChaCha20Rng;

const N: usize = 1_000_000;
const CHUNK: usize = 100;

// Iterator with internal buffer that uses fill() in chunks
struct ChunkedFillIter<'a, R, const CHUNK_SIZE: usize> {
    rng: &'a mut R,
    buf: [u64; CHUNK_SIZE],
    buf_pos: usize,
    buf_len: usize,
    remaining: usize,
}

impl<'a, R: Rng, const CHUNK_SIZE: usize> ChunkedFillIter<'a, R, CHUNK_SIZE> {
    fn new(rng: &'a mut R, n: usize) -> Self {
        Self {
            rng,
            buf: [0; CHUNK_SIZE],
            buf_pos: 0,
            buf_len: 0,
            remaining: n,
        }
    }

    fn refill(&mut self) {
        let to_fill = self.remaining.min(CHUNK_SIZE);
        self.rng.fill(&mut self.buf[..to_fill]);
        self.buf_pos = 0;
        self.buf_len = to_fill;
    }
}

impl<R: Rng, const CHUNK_SIZE: usize> Iterator for ChunkedFillIter<'_, R, CHUNK_SIZE> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        if self.buf_pos >= self.buf_len {
            self.refill();
        }
        let val = self.buf[self.buf_pos];
        self.buf_pos += 1;
        self.remaining -= 1;
        Some(val)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<R: Rng, const CHUNK_SIZE: usize> ExactSizeIterator for ChunkedFillIter<'_, R, CHUNK_SIZE> {}

// Iterator that calls rng.random() for each element
struct NextIter<'a, R> {
    rng: &'a mut R,
    remaining: usize,
}

impl<R: Rng> Iterator for NextIter<'_, R> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(self.rng.random())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<R: Rng> ExactSizeIterator for NextIter<'_, R> {}

// ===== SmallRng (fast) =====

#[divan::bench]
fn smallrng_next_iter() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(42);
    NextIter { rng: &mut rng, remaining: N }.collect()
}

#[divan::bench]
fn smallrng_chunked_fill() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(42);
    ChunkedFillIter::<_, CHUNK>::new(&mut rng, N).collect()
}

#[divan::bench]
fn smallrng_fill_direct(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| vec![0u64; N])
        .bench_local_values(|mut buf| {
            let mut rng = SmallRng::seed_from_u64(42);
            rng.fill(&mut buf[..]);
            buf
        })
}

#[divan::bench]
fn smallrng_next_direct(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| vec![0u64; N])
        .bench_local_values(|mut buf| {
            let mut rng = SmallRng::seed_from_u64(42);
            for slot in buf.iter_mut() {
                *slot = rng.random();
            }
            buf
        })
}

// ===== ChaCha20Rng (slower, cryptographic) =====

#[divan::bench]
fn chacha_next_iter() -> Vec<u64> {
    let mut rng = ChaCha20Rng::seed_from_u64(42);
    NextIter { rng: &mut rng, remaining: N }.collect()
}

#[divan::bench]
fn chacha_chunked_fill() -> Vec<u64> {
    let mut rng = ChaCha20Rng::seed_from_u64(42);
    ChunkedFillIter::<_, CHUNK>::new(&mut rng, N).collect()
}

#[divan::bench]
fn chacha_fill_direct(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| vec![0u64; N])
        .bench_local_values(|mut buf| {
            let mut rng = ChaCha20Rng::seed_from_u64(42);
            rng.fill(&mut buf[..]);
            buf
        })
}

#[divan::bench]
fn chacha_next_direct(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| vec![0u64; N])
        .bench_local_values(|mut buf| {
            let mut rng = ChaCha20Rng::seed_from_u64(42);
            for slot in buf.iter_mut() {
                *slot = rng.random();
            }
            buf
        })
}

fn main() {
    divan::main();
}
