//! TSBS data, byte-identical to `tsbs_generate_data`, on all cores.
//!
//! The devops use cases (`devops`, `cpu-only`, `cpu-single`,
//! `devops-generic`) and `iot`, for any `--seed`, scale, initial scale,
//! time range, log interval, point limit and interleaved group, as the text
//! formats TSBS writes (influx, victoriametrics, questdb, timescaledb,
//! clickhouse, timestream, cratedb, cassandra), or as Arrow tables and
//! Parquet files (one per measurement).
//!
//! TSBS draws every value from Go's global `math/rand` source, in one fixed
//! order, so the simulation is a port of Go's: the lagged Fibonacci
//! generator and its seeding, `Int63n`/`Intn`/`Float64`/`NormFloat64`/
//! `Zipf` exactly as Go computes them, and `math.Exp`/`math.Log` as Go's
//! amd64 assembly does. The run is split into resumable chunks (see
//! [`chunked`]): one frontier skips ahead and leaves checkpoints, and
//! workers generate and format the chunks on all cores through
//! [`populate_table`]'s ordered drivers.
//!
//! ```
//! use populate_tsbs::{Config, Format, Tsbs, UseCase};
//!
//! let mut cfg = Config::new(UseCase::CpuOnly, 2, 123);
//! cfg.end = cfg.start + 20_000_000_000; // two 10 s epochs
//! let t = Tsbs::new(cfg).unwrap();
//! let text = t.write_text(Format::Influx, Vec::new(), 0, 1).unwrap();
//! assert!(text.starts_with(b"cpu,hostname=host_0,"));
//! assert_eq!(text.split(|&b| b == b'\n').count(), 5);
//! assert_eq!(text, t.write_text_sequential(Format::Influx, Vec::new()).unwrap());
//! ```

pub mod arrow;
pub mod binary;
pub mod chunked;
pub mod config;
pub mod devops;
pub mod gorand;
mod gotables;
pub mod iot;
pub mod model;
pub mod sim;
pub mod text;

pub use arrow::TsbsArrow;
use binary::PROMETHEUS_HEADER;
pub use chunked::{Chunked, DEFAULT_CHUNK_POINTS};
pub use config::{Config, UseCase};
pub use text::Format;

use model::{Point, Statics};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use populate_table::MultiChunks;
use populate_table::text::{TextChunks, write_text_many};
use sim::{AnySim, Sim};
use std::io::{self, Write};
use std::sync::Arc;

/// A validated TSBS configuration, ready to generate.
#[derive(Debug, Clone)]
pub struct Tsbs {
    cfg: Config,
}

impl Tsbs {
    /// Validate `cfg` as `tsbs_generate_data` does (a zero seed becomes the
    /// clock's nanoseconds, as in Go).
    pub fn new(cfg: Config) -> Result<Self, String> {
        Ok(Tsbs {
            cfg: cfg.validate()?,
        })
    }

    /// The validated configuration.
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// The run as chunks of about `chunk_points` points.
    pub fn chunks(&self, chunk_points: u64) -> Chunked {
        Chunked::new(&self.cfg, chunk_points)
    }

    /// `format`'s text as a [`TextChunks`] source. For prometheus the
    /// chunks hold the points only: write [`binary::PROMETHEUS_HEADER`]
    /// before the first byte, as [`Tsbs::write_text`] does.
    pub fn text(&self, format: Format, chunk_points: u64) -> Result<TsbsText, String> {
        TsbsText::new(self.chunks(chunk_points), format)
    }

    /// Write `format`'s text to `w` on `threads` threads (0 = all cores)
    /// in chunks of about `chunk_points` points (0 = the default). The
    /// bytes are the same for any thread count and chunk size.
    pub fn write_text<W: Write + Send>(
        &self,
        format: Format,
        w: W,
        threads: usize,
        chunk_points: u64,
    ) -> io::Result<W> {
        let cp = if chunk_points == 0 {
            DEFAULT_CHUNK_POINTS
        } else {
            chunk_points
        };
        let src = self.text(format, cp).map_err(io::Error::other)?;
        let w = HeaderOnFirstWrite::new(w, format);
        let mut w = write_text_many(&[&src as &dyn TextChunks], vec![w], threads)?;
        Ok(w.pop().expect("one writer").w)
    }

    /// `format`'s text from one simulator, point by point, exactly as
    /// `tsbs_generate_data`'s `runSimulator` loop: the reference the
    /// chunked output is tested against.
    pub fn write_text_sequential<W: Write>(&self, format: Format, w: W) -> io::Result<W> {
        let mut w = HeaderOnFirstWrite::new(w, format);
        let mut sim = AnySim::new(&self.cfg);
        let st = sim.statics().clone();
        format.check(&st).map_err(io::Error::other)?;
        let mut buf = Vec::with_capacity(1 << 16);
        if format.has_header() {
            text::write_header(&st, &mut buf);
        }
        let mut p = Point::default();
        let mut group = 0;
        while !sim.finished() {
            if !sim.next(&mut p) {
                continue;
            }
            if group == self.cfg.group_id {
                text::write_point(format, &st, &p, &mut buf);
            }
            group = (group + 1) % self.cfg.groups;
            if buf.len() > 1 << 16 {
                w.write_all(&buf)?;
                buf.clear();
            }
        }
        w.write_all(&buf)?;
        w.flush()?;
        Ok(w.w)
    }

    /// The run as Arrow tables, one per measurement.
    pub fn arrow(&self, chunk_points: u64) -> TsbsArrow {
        let iot = self.cfg.use_case == UseCase::Iot;
        let generic = self.cfg.use_case == UseCase::DevopsGeneric;
        TsbsArrow::new(self.chunks(chunk_points), iot, generic)
    }

    /// Write one Parquet file per measurement (`writers` in
    /// [`TsbsArrow::table_names`] order), one row group per chunk,
    /// generated and encoded on `threads` threads.
    pub fn write_parquet<W: Write + Send>(
        &self,
        writers: Vec<W>,
        threads: usize,
        chunk_points: u64,
        props: Option<WriterProperties>,
    ) -> Result<Vec<W>, ParquetError> {
        let a = self.arrow(chunk_points);
        populate_table::write_parquet_many(&[&a as &dyn MultiChunks], writers, threads, props)
    }
}

/// Writes prometheus's version header just before the first byte of the
/// first point, as Go does (no points, no header).
struct HeaderOnFirstWrite<W> {
    w: W,
    header: &'static [u8],
}

impl<W: Write> HeaderOnFirstWrite<W> {
    fn new(w: W, format: Format) -> Self {
        let header = if format == Format::Prometheus {
            PROMETHEUS_HEADER
        } else {
            &[]
        };
        HeaderOnFirstWrite { w, header }
    }
}

impl<W: Write> Write for HeaderOnFirstWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !buf.is_empty() && !self.header.is_empty() {
            self.w.write_all(self.header)?;
            self.header = &[];
        }
        self.w.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

/// A run's text in one format, chunk by chunk ([`TextChunks`]).
pub struct TsbsText {
    chunks: Chunked,
    format: Format,
    statics: Arc<Statics>,
}

impl TsbsText {
    /// `format`'s text of `chunks`; an error where Go would panic (cassandra
    /// with IoT's numeric tags).
    pub fn new(chunks: Chunked, format: Format) -> Result<Self, String> {
        let statics = chunks.statics();
        format.check(&statics)?;
        Ok(TsbsText {
            chunks,
            format,
            statics,
        })
    }
}

impl TextChunks for TsbsText {
    fn outputs(&self) -> usize {
        1
    }

    fn num_chunks(&self) -> u64 {
        self.chunks.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let out = &mut bufs[0];
        let st = &*self.statics;
        if i == 0 && self.format.has_header() {
            text::write_header(st, out);
        }
        let format = self.format;
        self.chunks
            .run(i, |_: &AnySim, p| text::write_point(format, st, p, out));
    }
}
