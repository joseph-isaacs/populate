//! LDBC SNB data, byte-identical to the LDBC Spark datagen, on all cores.
//!
//! A port of the generator of [ldbc_snb_datagen_spark] (Apache-2.0) to
//! `populate-table`'s [`Chunks`](populate_table::Chunks) model. It reads
//! the datagen's own dictionaries (its `src/main/resources` directory) and
//! produces its `--mode raw` output: the 14 dynamic tables (persons,
//! friendships, forums, posts, comments, likes, ...) and the 4 static ones.
//!
//! How the datagen's passes map onto chunks:
//!
//! 1. **Persons** are generated in blocks of 10,000, each block from its
//!    own seed (`java.util.Random` farms reseeded with the block number).
//! 2. **Friendships** come from three passes. Each sorts all persons by a
//!    key (university, main interest, random id), cuts the ranking into
//!    blocks of 10,000 and connects persons within a block, each block from
//!    its own seed. A person's friendships of the three passes are merged.
//! 3. **Activity** (forums, posts, comments, photos, likes) and every
//!    person table are generated per block of the random-id ranking, again
//!    one seed per block.
//!
//! Steps 1 and 2 are a global pre-pass ([`Ldbc::new`], blocks in
//! parallel). Step 3 is the [`MultiChunks`] source: a chunk is
//! [`SEGMENT`] persons of an activity block, generated from the state the
//! block reaches at its first person, so it is a pure function of its
//! index and the pre-pass, and any number of threads gives the same
//! output.
//!
//! [ldbc_snb_datagen_spark]: https://github.com/ldbc/ldbc_snb_datagen_spark

mod activity;
pub mod csv;
mod dates;
mod dict;
mod fdlibm;
mod java;
mod jrand;
mod knows;
pub mod params;
mod person;
pub mod rows;

use activity::{BlockGen, Resume, World};
use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use dates::Dates;
use dict::{Dictionaries, PlaceType};
use knows::Knows;
use params::Params;
use person::{Person, PersonGenerator};
use populate_table::MultiChunks;
use populate_table::text::TextChunks;
use rows::{Col, DYNAMIC, Kind, Rows, STATIC, TableDef};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub use jrand::JavaRandom;
pub use params::{SCALE_FACTORS, persons_for_scale_factor};

/// Run `f(i)` for `i in 0..n` on `threads` threads (0 = all cores),
/// results in order.
fn par_map<T: Send>(n: usize, threads: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let threads = match threads {
        0 => thread::available_parallelism().map_or(1, |t| t.get()),
        t => t,
    }
    .min(n.max(1));
    if threads <= 1 {
        return (0..n).map(f).collect();
    }
    let next = AtomicUsize::new(0);
    let mut parts: Vec<Vec<(usize, T)>> = thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    let mut v = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        v.push((i, f(i)));
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("worker")).collect()
    });
    let mut all: Vec<(usize, T)> = parts.iter_mut().flat_map(std::mem::take).collect();
    all.sort_unstable_by_key(|&(i, _)| i);
    all.into_iter().map(|(_, t)| t).collect()
}

/// Time spent in each pre-pass step.
#[derive(Debug, Clone, Default)]
pub struct Timings {
    pub dictionaries: Duration,
    pub persons: Duration,
    pub knows: Duration,
    /// Finding each chunk's starting state (when done up front).
    pub states: Duration,
}

/// A generated social network, ready to produce its tables.
pub struct Ldbc {
    params: Params,
    dates: Dates,
    dicts: Dictionaries,
    persons: Vec<Person>,
    knows: Vec<Vec<Knows>>,
    blocks: Vec<Vec<u32>>,
    /// Chunk `i` is persons `start..end` of block `b`: `(b, start, end)`.
    segments: Vec<(usize, usize, usize)>,
    /// Per block, the state before each of its segments, filled in as
    /// segments are generated (or skipped over to reach a later one).
    resume: Vec<Mutex<Vec<Option<Resume>>>>,
    num_bits: u32,
    pub timings: Timings,
}

/// Persons per chunk. A block of 10,000 persons is generated in order from
/// one seed; a chunk starts from the state its block reaches at its first
/// person (found by a cheaper pass over the block's earlier persons, once
/// per block), so a block's chunks run in parallel and a chunk's rows stay
/// small (a whole block is ~2 GB of CSV).
pub const SEGMENT: usize = 1000;

impl Ldbc {
    /// Load the dictionaries from `resources` (the datagen's
    /// `src/main/resources`) and generate the persons and friendships of a
    /// network of `num_persons` persons on `threads` threads (0 = all
    /// cores).
    pub fn new(resources: &Path, num_persons: u64, threads: usize) -> io::Result<Self> {
        let params = Params::new(num_persons);
        let t0 = Instant::now();
        let power_law = java::lines(&std::fs::read_to_string(
            resources.join("dictionaries/powerLawActivityDeleteDate.txt"),
        )?)
        .into_iter()
        .map(java::parse_f64)
        .collect();
        let dates = Dates::new(&params, power_law);
        let dicts = Dictionaries::load(resources, &params, &dates)?;
        let t1 = Instant::now();
        let nb = params.num_blocks() as usize;
        let blocks: Vec<Vec<Person>> = par_map(nb, threads, |b| {
            let mut g = PersonGenerator::new(&params, &dicts, &dates);
            let mut v = Vec::new();
            g.block(b as u64, &mut v);
            v
        });
        let persons: Vec<Person> = blocks.into_iter().flatten().collect();
        let t2 = Instant::now();
        let bs = params.block_size as usize;
        let mut all: Vec<Vec<Knows>> = vec![Vec::new(); persons.len()];
        let mut random_rank = Vec::new();
        for step in 0..3 {
            let ranked = knows::ranked(&persons, step);
            let per_block = par_map(ranked.len().div_ceil(bs), threads, |b| {
                let members = &ranked[b * bs..((b + 1) * bs).min(ranked.len())];
                knows::distance_block(&params, &dicts, &dates, &persons, members, b as u64, step)
            });
            for (b, lists) in per_block.into_iter().enumerate() {
                for (k, l) in lists.into_iter().enumerate() {
                    all[ranked[b * bs + k] as usize].extend(l);
                }
            }
            if step == 2 {
                random_rank = ranked;
            }
        }
        let knows: Vec<Vec<Knows>> = {
            let persons = &persons;
            let all = std::sync::Mutex::new(all.into_iter().map(Some).collect::<Vec<_>>());
            par_map(persons.len(), threads, |i| {
                let l = all.lock().expect("knows")[i].take().expect("once");
                knows::merge(persons, l)
            })
        };
        // activity blocks: the random-id ranking, each block by account id
        let blocks: Vec<Vec<u32>> = random_rank
            .chunks(bs)
            .map(|c| {
                let mut v = c.to_vec();
                v.sort_unstable_by_key(|&i| persons[i as usize].account_id);
                v
            })
            .collect();
        let num_bits = ((nb as f64).log10() / 2f64.log10()).ceil() as u32;
        let mut segments = Vec::new();
        let mut resume = Vec::with_capacity(blocks.len());
        for (b, m) in blocks.iter().enumerate() {
            let n = m.len().div_ceil(SEGMENT);
            for k in 0..n {
                segments.push((b, k * SEGMENT, ((k + 1) * SEGMENT).min(m.len())));
            }
            let mut r = vec![None; n.max(1)];
            r[0] = Some(Resume::start(b as u64));
            resume.push(Mutex::new(r));
        }
        let t3 = Instant::now();
        let g = Ldbc {
            params,
            dates,
            dicts,
            persons,
            knows,
            blocks,
            segments,
            resume,
            num_bits,
            timings: Timings {
                dictionaries: t1 - t0,
                persons: t2 - t1,
                knows: t3 - t2,
                states: Duration::ZERO,
            },
        };
        Ok(g.with_states(threads))
    }

    /// With enough blocks to keep `threads` busy, find every segment's
    /// state up front, blocks in parallel, so that no chunk waits for its
    /// block's earlier ones. (Chunks start in order, so otherwise all
    /// threads work on one block and wait in turn for its skipping pass.)
    /// With fewer blocks, states are found as chunks need them, which
    /// overlaps one skipping pass with the chunks it unblocks.
    fn with_states(mut self, threads: usize) -> Self {
        let threads = match threads {
            0 => thread::available_parallelism().map_or(1, |t| t.get()),
            t => t,
        };
        if threads > 1 && self.blocks.len() >= threads {
            let t0 = Instant::now();
            let g = &self;
            par_map(g.blocks.len(), threads, |b| {
                let n = g.blocks[b].len().div_ceil(SEGMENT);
                g.state_before(b, n - 1);
            });
            self.timings.states = t0.elapsed();
        }
        self
    }

    /// Persons in the network.
    pub fn num_persons(&self) -> u64 {
        self.params.num_persons
    }

    /// Friendships (each counted once).
    pub fn num_knows(&self) -> usize {
        self.knows.iter().map(Vec::len).sum::<usize>() / 2
    }

    /// Activity blocks.
    pub fn num_blocks(&self) -> u64 {
        self.blocks.len() as u64
    }

    /// Chunks: [`SEGMENT`]-person pieces of the activity blocks.
    pub fn num_chunks(&self) -> u64 {
        self.segments.len() as u64
    }

    fn world(&self) -> World<'_> {
        World {
            p: &self.params,
            d: &self.dicts,
            dates: &self.dates,
            persons: &self.persons,
            knows: &self.knows,
            num_bits: self.num_bits,
        }
    }

    /// Fresh buffers for the dynamic tables.
    pub fn dynamic_rows() -> Vec<Rows> {
        DYNAMIC.iter().map(|d| Rows::new(d)).collect()
    }

    /// The state before segment `k` of block `b`, skipping from the
    /// nearest known one if need be (the block's lock is held meanwhile, so
    /// each stretch is skipped once).
    fn state_before(&self, b: usize, k: usize) -> Resume {
        let mut states = self.resume[b].lock().expect("resume");
        if states[k].is_none() {
            let j = (0..k)
                .rev()
                .find(|&j| states[j].is_some())
                .expect("block start");
            let members = &self.blocks[b][..];
            let w = self.world();
            let mut g = BlockGen::new(&w, Vec::new());
            for s in j..k {
                let range = s * SEGMENT..(s + 1) * SEGMENT;
                let next = g.skip(b as u64, members, range, states[s].as_ref().expect("state"));
                states[s + 1] = Some(next);
            }
        }
        states[k].clone().expect("state")
    }

    /// Generate chunk `i` into `out` (one [`Rows`] per [`rows::DYNAMIC`]
    /// table, cleared first).
    pub fn chunk_rows(&self, i: u64, out: &mut Vec<Rows>) {
        for r in out.iter_mut() {
            r.clear();
        }
        let (b, start, end) = self.segments[i as usize];
        let k = start / SEGMENT;
        let members = &self.blocks[b][..];
        let w = self.world();
        let at = self.state_before(b, k);
        let mut g = BlockGen::new(&w, std::mem::take(out));
        let next = g.run(b as u64, members, start..end, &at);
        *out = g.out;
        let mut states = self.resume[b].lock().expect("resume");
        if let Some(s) = states.get_mut(k + 1) {
            s.get_or_insert(next);
        }
    }

    /// Generate every chunk on `threads` threads (0 = all cores)
    /// and count the dynamic tables' rows, without writing them.
    pub fn count_rows(&self, threads: usize) -> usize {
        par_map(self.segments.len(), threads, |i| {
            let mut rows = Ldbc::dynamic_rows();
            self.chunk_rows(i as u64, &mut rows);
            rows.iter().map(Rows::len).sum::<usize>()
        })
        .into_iter()
        .sum()
    }

    /// The four static tables ([`rows::STATIC`]).
    pub fn static_rows(&self) -> Vec<Rows> {
        let d = &self.dicts;
        let mut out: Vec<Rows> = STATIC.iter().map(|t| Rows::new(t)).collect();
        const DBP: &str = "http://dbpedia.org/resource/";
        const DBPOWL: &str = "http://dbpedia.org/ontology/";
        // organisations: companies, then universities
        {
            let r = &mut out[0];
            for (id, name) in d.companies.name.iter().enumerate() {
                let name = java::clamp(name, 256);
                r.cols[0].i32(id as i32);
                r.cols[1].str(b"Company");
                r.cols[2].str(name.as_bytes());
                r.cols[3].str(format!("{DBP}{name}").as_bytes());
                r.cols[4].i32(d.companies.country[id]);
            }
            for (k, name) in d.universities.name.iter().enumerate() {
                let name = java::clamp(name, 256);
                r.cols[0].i32((d.universities.start + k as i64) as i32);
                r.cols[1].str(b"University");
                r.cols[2].str(name.as_bytes());
                r.cols[3].str(format!("{DBP}{name}").as_bytes());
                r.cols[4].i32(d.universities.city[k]);
            }
        }
        {
            let r = &mut out[1];
            for (id, pl) in d.places.places.iter().enumerate() {
                let name = java::clamp(&pl.name, 256);
                r.cols[0].i32(id as i32);
                r.cols[1].str(name.as_bytes());
                r.cols[2].str(format!("{DBP}{name}").as_bytes());
                r.cols[3].str(pl.kind.as_str().as_bytes());
                r.cols[4].opt_i32(match pl.kind {
                    PlaceType::City | PlaceType::Country => Some(d.places.belongs_to(id as i32)),
                    PlaceType::Continent => None,
                });
            }
        }
        {
            let mut exported = std::collections::HashSet::new();
            let (tags, classes) = out.split_at_mut(3);
            let (rt, rc) = (&mut tags[2], &mut classes[0]);
            for (id, name) in d.tags.names.iter().enumerate() {
                let name = java::clamp(name, 256).replace('"', "\\\"");
                let class = d.tags.tag_class[id];
                rt.cols[0].i32(id as i32);
                rt.cols[1].str(name.as_bytes());
                rt.cols[2].str(format!("{DBP}{name}").as_bytes());
                rt.cols[3].i32(class);
                let mut c = class;
                while c != -1 && exported.insert(c) {
                    let parent = d.tags.class_parent(c);
                    let cname = java::clamp(d.tags.class_name(c), 256);
                    rc.cols[0].i32(c);
                    rc.cols[1].str(cname.as_bytes());
                    let url = if cname == "Thing" {
                        "http://www.w3.org/2002/07/owl#Thing".to_string()
                    } else {
                        format!("{DBPOWL}{cname}")
                    };
                    rc.cols[2].str(url.as_bytes());
                    rc.cols[3].opt_i32((parent != -1).then_some(parent));
                    c = parent;
                }
            }
        }
        out
    }

    /// CSV text of the dynamic tables, for
    /// [`write_text_many`](populate_table::text::write_text_many).
    pub fn csv(&self) -> DynamicCsv<'_> {
        DynamicCsv { g: self }
    }
}

/// The dynamic tables as CSV chunks (one output per [`rows::DYNAMIC`]
/// table; chunk 0 starts with the header).
pub struct DynamicCsv<'a> {
    g: &'a Ldbc,
}

impl TextChunks for DynamicCsv<'_> {
    fn outputs(&self) -> usize {
        DYNAMIC.len()
    }

    fn num_chunks(&self) -> u64 {
        self.g.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        let mut rows = Ldbc::dynamic_rows();
        self.g.chunk_rows(i, &mut rows);
        for ((def, r), buf) in DYNAMIC.iter().zip(&rows).zip(bufs.iter_mut()) {
            if i == 0 {
                csv::header(def, buf);
            }
            csv::write(def, r, buf);
        }
    }
}

impl MultiChunks for Ldbc {
    fn schemas(&self) -> Vec<SchemaRef> {
        DYNAMIC.iter().map(|d| d.schema()).collect()
    }

    fn num_chunks(&self) -> u64 {
        Ldbc::num_chunks(self)
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        let mut rows = Ldbc::dynamic_rows();
        self.chunk_rows(i, &mut rows);
        for (k, (def, r)) in DYNAMIC.iter().zip(&rows).enumerate() {
            if !r.is_empty() {
                emit(k, to_batch(def, r));
            }
        }
    }
}

/// An Arrow batch of `rows`.
pub fn to_batch(def: &TableDef, rows: &Rows) -> RecordBatch {
    let cols: Vec<ArrayRef> = rows
        .cols
        .iter()
        .zip(def.cols)
        .map(|(c, &(_, k))| -> ArrayRef {
            match c {
                Col::I64(v) => match k {
                    Kind::Ts => {
                        Arc::new(TimestampMillisecondArray::from(v.clone()).with_timezone("UTC"))
                    }
                    Kind::Date => Arc::new(Date32Array::from(
                        v.iter()
                            .map(|&ms| ms.div_euclid(java::MS_PER_DAY) as i32)
                            .collect::<Vec<_>>(),
                    )),
                    _ => Arc::new(Int64Array::from(v.clone())),
                },
                Col::I32(v) => Arc::new(Int32Array::from(v.clone())),
                Col::Bool(v) => Arc::new(BooleanArray::from(v.clone())),
                Col::Str(d, o) => Arc::new(string_array(d, o, None)),
                Col::OptI64(v, n) => Arc::new(Int64Array::new(
                    ScalarBuffer::from(v.clone()),
                    Some(NullBuffer::from(n.clone())),
                )),
                Col::OptI32(v, n) => Arc::new(Int32Array::new(
                    ScalarBuffer::from(v.clone()),
                    Some(NullBuffer::from(n.clone())),
                )),
                Col::OptStr(d, o, n) => Arc::new(string_array(d, o, Some(n))),
            }
        })
        .collect();
    RecordBatch::try_new(def.schema(), cols).expect("schema matches")
}

fn string_array(d: &[u8], ends: &[u32], valid: Option<&Vec<bool>>) -> StringArray {
    let mut offs = Vec::with_capacity(ends.len() + 1);
    offs.push(0i32);
    offs.extend(ends.iter().map(|&e| e as i32));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offs));
    let nulls = valid.map(|v| NullBuffer::from(v.clone()));
    StringArray::try_new(offsets, arrow_buffer::Buffer::from(d.to_vec()), nulls).expect("utf-8")
}

/// The resources directory: `$LDBC_DATAGEN_RESOURCES`, else
/// `ldbc_snb_datagen_spark/src/main/resources` next to the workspace.
pub fn default_resources() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("LDBC_DATAGEN_RESOURCES") {
        return Some(PathBuf::from(p));
    }
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/ldbc_snb_datagen_spark/src/main/resources");
    p.join("dictionaries").is_dir().then_some(p)
}
