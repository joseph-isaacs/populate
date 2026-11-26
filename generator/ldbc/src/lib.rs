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
//! parallel). Step 3 is [`Ldbc::raw`], one distribution of all 14 tables
//! (a `fan_out` over chunks, its schemas read off its row types). A chunk
//! is [`SEGMENT`] persons of an activity block, generated from the state
//! the block reaches at its first person, so it is a pure function of its
//! index and the pre-pass, and any number of threads gives the same
//! output.
//!
//! [ldbc_snb_datagen_spark]: https://github.com/ldbc/ldbc_snb_datagen_spark

mod activity;
pub mod bi;
pub mod csv;
mod dates;
mod dict;
pub mod factors;
mod fdlibm;
mod java;
mod jrand;
mod knows;
pub mod layout;
pub mod params;
mod person;
pub mod rows;

use activity::{BlockGen, Resume, World};
use arrow_array::RecordBatch;
use dates::Dates;
use dict::{Dictionaries, PlaceType};
use factors::{FACTOR, FACTOR_TABLES, Factor, FactorCols, FactorInput, Factors};
use knows::Knows;
use params::Params;
use person::{Hot, Person, PersonGenerator};
use populate_builtins::{fan_out, named_all, range};
use populate_core::{Distribution, Sink};
use populate_table::text::TextChunks;
use populate_table::{DistTables, par_map, resolve_threads};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rows::{
    DYNAMIC, DYNAMIC_COLUMNS, DYNAMIC_NAMES, Raw, RawCols, Rows, STATIC_TABLES, StaticCols,
    Statics, TableDef, rows_of,
};
use std::collections::HashSet;
use std::env::var_os;
use std::fs::read_to_string;
use std::io;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub use jrand::JavaRandom;
pub use params::{SCALE_FACTORS, persons_for_scale_factor};

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
    hot: Vec<Hot>,
    knows: Vec<Vec<Knows>>,
    blocks: Vec<Vec<u32>>,
    /// Chunk `i` is persons `start..end` of block `b`: `(b, start, end)`.
    segments: Vec<(usize, usize, usize)>,
    /// Per block, the state before each of its segments, filled in as
    /// segments are generated (or skipped over to reach a later one).
    resume: Vec<Mutex<Vec<Option<Resume>>>>,
    num_bits: u32,
    pub timings: Timings,
    /// Gathers the factor tables' inputs from each chunk, when asked to.
    factor_input: Option<FactorInput>,
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
        let power_law = java::lines(&read_to_string(
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
            let all = Mutex::new(all.into_iter().map(Some).collect::<Vec<_>>());
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
        // the datagen refuses more (`Possible id overlap`); ids, and the
        // caches keyed by them, are unique per block only up to here
        assert!(num_bits <= 20, "too many blocks: ids would overlap");
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
            hot: persons.iter().map(Hot::of).collect(),
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
            factor_input: None,
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
        let threads = resolve_threads(threads);
        if threads > 1 && self.blocks.len() >= threads {
            let t0 = Instant::now();
            let g = &self;
            par_map(g.blocks.len(), threads, |b| {
                let n = g.blocks[b].len().div_ceil(SEGMENT);
                if n > 0 {
                    g.state_before(b, n - 1);
                }
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
            hot: &self.hot,
            knows: &self.knows,
            num_bits: self.num_bits,
        }
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
            let mut g = BlockGen::new(&w, RawCols::default());
            for s in j..k {
                let range = s * SEGMENT..(s + 1) * SEGMENT;
                let next = g.skip(b as u64, members, range, states[s].as_ref().expect("state"));
                states[s + 1] = Some(next);
            }
        }
        states[k].clone().expect("state")
    }

    /// Append chunk `i`'s rows of every dynamic table to `out`.
    ///
    /// With [`Self::collect_factors`] on, `out` must hold no other chunk:
    /// the factor tables take in every row of it.
    pub fn chunk_into(&self, i: u64, out: &mut RawCols) {
        if self.factor_input.is_some() {
            assert!(
                out.0.0.values.1.is_empty(),
                "the factor tables take one chunk at a time"
            );
        }
        let (b, start, end) = self.segments[i as usize];
        let k = start / SEGMENT;
        let members = &self.blocks[b][..];
        let w = self.world();
        let at = self.state_before(b, k);
        let mut g = BlockGen::new(&w, take(out));
        let next = g.run(b as u64, members, start..end, &at);
        *out = g.out;
        let mut states = self.resume[b].lock().expect("resume");
        if let Some(s) = states.get_mut(k + 1) {
            s.get_or_insert(next);
        }
        drop(states);
        if let Some(f) = &self.factor_input {
            f.add(i, out);
        }
    }

    /// Chunk `i`'s rows of every dynamic table: row `i` of
    /// [`Self::raw_dist`].
    fn chunk(&self, i: u64) -> RawCols {
        let mut d = self.raw_dist();
        d.skip(i);
        // the chunks draw from their own `java.util.Random` farms
        d.collect(&mut SmallRng::seed_from_u64(0), 1)
    }

    /// Chunk `i`'s rows, one [`Rows`] per [`rows::DYNAMIC`] table.
    pub fn chunk_rows(&self, i: u64) -> Vec<Rows> {
        rows_of(self.chunk(i))
    }

    /// The 14 dynamic tables as one distribution: a source row is a chunk,
    /// and its children are the chunk's rows of each table. With
    /// [`Self::collect_factors`] on, collect one source row at a time (as
    /// [`Self::raw`] does).
    pub fn raw_dist(&self) -> impl Distribution<Item = Raw> + Clone + Sync + '_ {
        let names: Vec<&str> = DYNAMIC_COLUMNS.concat();
        named_all(
            &names,
            fan_out::<_, Raw, _>(range(0u64, 1), |&i: &u64, out: &mut RawCols| {
                self.chunk_into(i, out)
            }),
        )
    }

    /// The dynamic tables, a chunk (one Parquet row group per table) per
    /// chunk of the network.
    pub fn raw(&self) -> DistTables<impl Distribution<Item = Raw> + Clone + Sync + '_> {
        DistTables::new(&DYNAMIC_NAMES, self.num_chunks(), self.raw_dist())
            .chunk_rows(1)
            .batch_rows(1)
    }

    /// Gather the factor tables' inputs from every chunk generated from
    /// now on (each chunk once), for [`Self::factors`].
    pub fn collect_factors(&mut self) {
        self.factor_input = Some(FactorInput::new(self.num_chunks()));
    }

    /// The datagen's factor tables (`--generate-factors`), on `threads`
    /// threads (0 = all cores), once every chunk has been generated after
    /// [`Self::collect_factors`].
    pub fn factors(&self, threads: usize) -> Vec<Factor> {
        let cols = self
            .factor_dist(threads)
            .collect(&mut SmallRng::seed_from_u64(0), 1);
        FACTOR
            .iter()
            .zip(rows_of(cols))
            .map(|(def, rows)| Factor { def, rows })
            .collect()
    }

    /// The 34 factor tables as one distribution of a single source row,
    /// its children the tables' rows, computed on `threads` threads (0 =
    /// all cores). Collect it once every chunk has been generated after
    /// [`Self::collect_factors`].
    pub fn factor_dist(&self, threads: usize) -> impl Distribution<Item = Factors> + Clone + '_ {
        let names: Vec<&str> = FACTOR_TABLES
            .iter()
            .flat_map(|t| t.1.iter().copied())
            .collect();
        named_all(
            &names,
            fan_out::<_, Factors, _>(range(0u64, 1), move |_: &u64, out: &mut FactorCols| {
                let f = self.factor_input.as_ref().expect("collect_factors first");
                let mut statics = StaticCols::default();
                self.static_into(&mut statics);
                f.finish_into(&statics, threads, out);
            }),
        )
    }

    /// Generate every chunk on `threads` threads (0 = all cores)
    /// and count the dynamic tables' rows, without writing them.
    pub fn count_rows(&self, threads: usize) -> usize {
        par_map(self.segments.len(), threads, |i| {
            self.chunk_rows(i as u64)
                .iter()
                .map(Rows::len)
                .sum::<usize>()
        })
        .into_iter()
        .sum()
    }

    /// The four static tables as one distribution of a single source row,
    /// its children the tables' rows.
    pub fn static_dist(&self) -> impl Distribution<Item = Statics> + Clone + Sync + '_ {
        let names: Vec<&str> = STATIC_TABLES
            .iter()
            .flat_map(|t| t.1.iter().copied())
            .collect();
        named_all(
            &names,
            fan_out::<_, Statics, _>(range(0u64, 1), |_: &u64, out: &mut StaticCols| {
                self.static_into(out)
            }),
        )
    }

    /// The four static tables ([`rows::STATIC`]).
    pub fn static_rows(&self) -> Vec<Rows> {
        rows_of(
            self.static_dist()
                .collect(&mut SmallRng::seed_from_u64(0), 1),
        )
    }

    fn static_into(&self, out: &mut StaticCols) {
        let d = &self.dicts;
        const DBP: &str = "http://dbpedia.org/resource/";
        const DBPOWL: &str = "http://dbpedia.org/ontology/";
        let (orgs, places, tags, classes) = out;
        // organisations: companies, then universities
        for (id, name) in d.companies.name.iter().enumerate() {
            let name = java::clamp(name, 256);
            let url = format!("{DBP}{name}");
            let country = d.companies.country[id];
            orgs.values
                .push((id as i32, "Company", name, url.as_str(), country));
        }
        for (k, name) in d.universities.name.iter().enumerate() {
            let name = java::clamp(name, 256);
            let id = (d.universities.start + k as i64) as i32;
            let url = format!("{DBP}{name}");
            let city = d.universities.city[k];
            orgs.values
                .push((id, "University", name, url.as_str(), city));
        }
        for (id, pl) in d.places.places.iter().enumerate() {
            let name = java::clamp(&pl.name, 256);
            let url = format!("{DBP}{name}");
            let part_of = match pl.kind {
                PlaceType::City | PlaceType::Country => Some(d.places.belongs_to(id as i32)),
                PlaceType::Continent => None,
            };
            places
                .values
                .push((id as i32, name, url.as_str(), pl.kind.as_str(), part_of));
        }
        let mut exported = HashSet::new();
        for (id, name) in d.tags.names.iter().enumerate() {
            let name = java::clamp(name, 256).replace('"', "\\\"");
            let class = d.tags.tag_class[id];
            let url = format!("{DBP}{name}");
            tags.values
                .push((id as i32, name.as_str(), url.as_str(), class));
            let mut c = class;
            while c != -1 && exported.insert(c) {
                let parent = d.tags.class_parent(c);
                let cname = java::clamp(d.tags.class_name(c), 256);
                let url = if cname == "Thing" {
                    "http://www.w3.org/2002/07/owl#Thing".to_string()
                } else {
                    format!("{DBPOWL}{cname}")
                };
                let parent_id = (parent != -1).then_some(parent);
                classes.values.push((c, cname, url.as_str(), parent_id));
                c = parent;
            }
        }
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
        let rows = self.g.chunk_rows(i);
        for ((def, r), buf) in DYNAMIC.iter().zip(&rows).zip(bufs.iter_mut()) {
            if i == 0 {
                csv::header(def, buf);
            }
            csv::write(def, r, buf);
        }
    }
}

/// An Arrow batch of `rows`, a table of `def`.
pub fn to_batch(def: &TableDef, rows: Rows) -> RecordBatch {
    let cols = rows.cols.into_iter().map(|c| c.into_array()).collect();
    RecordBatch::try_new(def.schema(), cols).expect("schema matches")
}

/// The resources directory: `$LDBC_DATAGEN_RESOURCES`, else
/// `ldbc_snb_datagen_spark/src/main/resources` next to the workspace.
pub fn default_resources() -> Option<PathBuf> {
    if let Some(p) = var_os("LDBC_DATAGEN_RESOURCES") {
        return Some(PathBuf::from(p));
    }
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/ldbc_snb_datagen_spark/src/main/resources");
    p.join("dictionaries").is_dir().then_some(p)
}
