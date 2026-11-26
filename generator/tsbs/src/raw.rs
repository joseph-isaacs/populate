//! Backend-independent points, including their original scalar types and metadata.

use crate::chunked::Chunked;
pub use crate::model::{Measurement, Point, Statics, TagVal, Type, Val};
use populate_core::{Children, Columnar, Columns, DistChunks, Distribution};
use rand::Rng;
use std::sync::{Arc, Mutex};

impl Columnar for Point {
    type Columns = Vec<Point>;
}

/// One source row per simulator chunk, with its emitted points as children.
/// Clones share the checkpoint frontier. Reading each chunk once uses the
/// frontier's replay optimization; reading a consumed chunk again replays
/// from the beginning without retaining an unbounded checkpoint cache.
#[derive(Clone)]
pub struct PointChunks {
    source: Arc<Chunked>,
    // Atomically choose a checkpoint or a replay when clones read the same chunk.
    cursor_lock: Arc<Mutex<()>>,
    next: u64,
}

impl PointChunks {
    pub(crate) fn new(source: Chunked) -> Self {
        Self {
            source: Arc::new(source),
            cursor_lock: Arc::new(Mutex::new(())),
            next: 0,
        }
    }

    /// Measurement names, field types, tag values, and timestamp origin.
    pub fn statics(&self) -> Arc<Statics> {
        self.source.statics()
    }

    /// Remaining source chunks, each emitted as one native batch.
    pub fn into_chunks(self) -> DistChunks<Self> {
        let n = self.source.num_chunks() - self.next;
        DistChunks::new(n, self).chunk_rows(1).batch_rows(1)
    }
}

impl Distribution for PointChunks {
    type Item = Children<Point>;

    fn collect_into(&mut self, out: &mut Columns<Self::Item>, _rng: &mut impl Rng, n: usize) {
        assert!(
            n as u64 <= self.source.num_chunks() - self.next,
            "source range out of bounds"
        );
        for k in self.next..self.next + n as u64 {
            let cursor = {
                let _guard = self.cursor_lock.lock().expect("native cursor");
                self.source.replay_cursor(k)
            };
            self.source
                .run_cursor(k, cursor, |p| out.values.push(p.clone()));
        }
        out.sources += n;
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        assert!(
            n <= self.source.num_chunks() - self.next,
            "source range out of bounds"
        );
        self.next += n;
    }
}
