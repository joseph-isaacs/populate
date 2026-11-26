// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! The datagen's factor tables (`--generate-factors`): 34 summary tables
//! of the raw graph (counts per country, day, tag, person, ...; friends of
//! friends; 2- and 4-hop person pairs; connected components), as
//! `FactorGenerationStage` computes them with Spark.
//!
//! Inputs are gathered while the graph is generated ([`FactorInput::add`],
//! one call per chunk): the person-sized tables in raw order, and the
//! message-sized ones folded into counts. [`FactorInput::finish_into`] then
//! computes every table, in parallel.
//!
//! Spark's semantics are reproduced: `frequency` tables are ordered by
//! frequency (descending, nulls last), then by their grouping columns
//! (ascending, nulls first); left outer joins count 0 and sum to null;
//! `personNumFriendTags` groups by the interest's (nullable) person id;
//! the n-hop tables keep the datagen's bug (person 2's dates are person
//! 1's) and its walk semantics (a person is two hops from itself), and
//! sample with Spark's `XORShiftRandom` (`Dataset.sample(f, 42)` over one
//! partition).

use crate::csv::spark_micros;
use crate::par_map;
use crate::rows::{OrgEdgeRow, RawCols, Rows, StaticCols, TableDef, Ts, rows_of};
use populate_core::{Children, Columns, Sink, Timestamp};
use populate_table::resolve_threads;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::{swap, take};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{LazyLock, Mutex};
use std::thread;

const DAY_US: i64 = 86_400_000_000;

/// A multiply-rotate hash (as rustc's `FxHasher`) with a final mix, for
/// the integer keys here: SipHash is most of the cost otherwise.
#[derive(Default)]
struct Fx(u64);

impl Fx {
    #[inline]
    fn add(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for Fx {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, tail) = bytes.as_chunks::<8>();
        for w in words {
            self.add(u64::from_le_bytes(*w));
        }
        for &b in tail {
            self.add(b as u64);
        }
    }

    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.add(x);
    }

    #[inline]
    fn write_i64(&mut self, x: i64) {
        self.add(x as u64);
    }

    #[inline]
    fn write_u32(&mut self, x: u32) {
        self.add(x as u64);
    }

    #[inline]
    fn write_i32(&mut self, x: i32) {
        self.add(x as u32 as u64);
    }

    #[inline]
    fn write_usize(&mut self, x: usize) {
        self.add(x as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        let h = self.0;
        (h ^ (h >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ (h >> 32)
    }
}

type Map<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

/// Midnight (epoch milliseconds) of the day of a timestamp, as Spark's
/// `date_trunc("day", ts)` in UTC.
fn day(ms: i64) -> i64 {
    spark_micros(ms).div_euclid(DAY_US) * 86_400_000
}

/// A person, as the factors use it.
struct Person {
    id: i64,
    created: i64,
    deleted: i64,
    first_name: Box<str>,
    city: i32,
}

/// One chunk's person-sized rows, in raw order.
#[derive(Default)]
struct Part {
    persons: Vec<Person>,
    /// `(Person1Id, Person2Id, creationDate, deletionDate)`.
    knows: Vec<[i64; 4]>,
    /// `(PersonId, UniversityId, creationDate, deletionDate)`.
    study: Vec<[i64; 4]>,
    /// `(PersonId, CompanyId, creationDate, deletionDate)`.
    work: Vec<[i64; 4]>,
    /// `(PersonId, TagId)`.
    interests: Vec<(i64, i32)>,
}

/// Counts over the message-sized tables (order does not matter).
#[derive(Default)]
struct Counts {
    /// Messages per `LocationCountryId`.
    country: Map<i64, i64>,
    /// Messages per creation day.
    day: Map<i64, i64>,
    /// Messages per creation day and `length / 10`.
    day_len: Map<(i64, i64), i64>,
    /// Messages per length.
    len: Map<i32, i64>,
    /// Message tags per creation day and tag (a tag's `creationDate` is its
    /// message's).
    day_tag: Map<(i64, i32), i64>,
    /// Posts per language (non-null).
    lang: Map<Box<str>, i64>,
    /// Posts, comments, likes and memberships per person.
    posts: Map<i64, i64>,
    comments: Map<i64, i64>,
    likes: Map<i64, i64>,
    forums: Map<i64, i64>,
}

fn add_all<K: Hash + Eq>(into: &mut Map<K, i64>, from: Map<K, i64>) {
    if into.is_empty() {
        *into = from;
        return;
    }
    for (k, v) in from {
        *into.entry(k).or_default() += v;
    }
}

impl Counts {
    fn merge(&mut self, o: Counts) {
        add_all(&mut self.country, o.country);
        add_all(&mut self.day, o.day);
        add_all(&mut self.day_len, o.day_len);
        add_all(&mut self.len, o.len);
        add_all(&mut self.day_tag, o.day_tag);
        add_all(&mut self.lang, o.lang);
        add_all(&mut self.posts, o.posts);
        add_all(&mut self.comments, o.comments);
        add_all(&mut self.likes, o.likes);
        add_all(&mut self.forums, o.forums);
    }
}

/// The factor tables' inputs, gathered chunk by chunk.
pub struct FactorInput {
    parts: Mutex<Vec<Option<Part>>>,
    counts: Mutex<Counts>,
    /// The inputs are taken by the one [`FactorInput::finish_into`].
    finished: AtomicBool,
}

/// Chunk `i` was taken in twice, or not at all.
fn chunk_once(i: usize) -> String {
    format!("chunk {i}: the factor tables need every chunk generated exactly once")
}

/// A factor table: its definition and rows, in the datagen's order, for
/// the writers.
pub struct Factor {
    pub def: &'static TableDef,
    pub rows: Rows,
}

impl FactorInput {
    pub fn new(num_chunks: u64) -> Self {
        FactorInput {
            parts: Mutex::new((0..num_chunks).map(|_| None).collect()),
            counts: Mutex::new(Counts::default()),
            finished: AtomicBool::new(false),
        }
    }

    /// Take in chunk `i`'s raw rows.
    pub fn add(&self, i: u64, raw: &RawCols) {
        let (
            (person, interest, study, work, knows),
            (_, _, member),
            (post, post_tag, comment, comment_tag, likes_post, likes_comment),
        ) = raw;
        let mut p = Part::default();
        {
            let ((c, d, _), id, first, _, _, _, _, _, city, _, _) = &person.values;
            p.persons = (0..id.len())
                .map(|j| Person {
                    id: id[j],
                    created: c[j].0,
                    deleted: d[j].0,
                    first_name: first.get(j).into(),
                    city: city[j],
                })
                .collect();
        }
        let quad = |(c, d, x, y, _): &Columns<OrgEdgeRow>| -> Vec<[i64; 4]> {
            (0..x.len()).map(|j| [x[j], y[j], c[j].0, d[j].0]).collect()
        };
        {
            let (c, d, _, x, y) = &knows.values;
            p.knows = (0..x.len()).map(|j| [x[j], y[j], c[j].0, d[j].0]).collect();
        }
        p.study = quad(&study.values);
        p.work = quad(&work.values);
        {
            let (_, _, pid, tag) = &interest.values;
            p.interests = pid.iter().copied().zip(tag.iter().copied()).collect();
        }

        let mut k = Counts::default();
        let count = |m: &mut Map<i64, i64>, ids: &[i64]| {
            for &x in ids {
                *m.entry(x).or_default() += 1;
            }
        };
        {
            let ((c, _, _), _, _, _, _, lang, _, len, creator, _, country) = &post.values;
            for j in 0..len.len() {
                message(&mut k, c[j].0, len[j], country[j]);
                if lang.valid.get(j) {
                    *k.lang.entry(lang.values.get(j).into()).or_default() += 1;
                }
            }
            count(&mut k.posts, creator);
        }
        {
            let (c, _, _, _, _, _, _, len, creator, country, _, _) = &comment.values;
            for j in 0..len.len() {
                message(&mut k, c[j].0, len[j], country[j] as i64);
            }
            count(&mut k.comments, creator);
        }
        for t in [post_tag, comment_tag] {
            let (c, _, _, tag) = &t.values;
            for j in 0..tag.len() {
                *k.day_tag.entry((day(c[j].0), tag[j])).or_default() += 1;
            }
        }
        count(&mut k.likes, &likes_post.values.3);
        count(&mut k.likes, &likes_comment.values.3);
        count(&mut k.forums, &member.values.4);

        {
            let mut parts = self.parts.lock().expect("parts");
            let slot = &mut parts[i as usize];
            assert!(slot.is_none(), "{}", chunk_once(i as usize));
            *slot = Some(p);
        }
        self.counts.lock().expect("counts").merge(k);
    }

    /// Write every factor table to `out`, on `threads` threads (0 = all
    /// cores), from the chunks added and the static tables. Takes the
    /// inputs, so it runs once.
    pub fn finish_into(&self, statics: &StaticCols, threads: usize, out: &mut FactorCols) {
        assert!(
            !self.finished.swap(true, AtomicOrdering::Relaxed),
            "the factor tables are made once"
        );
        let parts = take(&mut *self.parts.lock().expect("parts"));
        let counts = take(&mut *self.counts.lock().expect("counts"));
        let mut all = Part::default();
        for (i, p) in parts.into_iter().enumerate() {
            let p = p.unwrap_or_else(|| panic!("{}", chunk_once(i)));
            all.persons.extend(p.persons);
            all.knows.extend(p.knows);
            all.study.extend(p.study);
            all.work.extend(p.work);
            all.interests.extend(p.interests);
        }
        let ctx = Ctx::new(all, counts, statics, threads);
        tables_into(&ctx, out, threads);
    }
}

fn message(k: &mut Counts, created: i64, len: i32, country: i64) {
    let d = day(created);
    *k.country.entry(country).or_default() += 1;
    *k.day.entry(d).or_default() += 1;
    *k.day_len.entry((d, (len / 10) as i64)).or_default() += 1;
    *k.len.entry(len).or_default() += 1;
}

/// A place of the static `Place` table.
struct Place {
    name: Box<str>,
    kind: Box<str>,
    part_of: Option<i32>,
}

/// Everything the factors read, indexed.
struct Ctx {
    g: Part,
    k: Counts,
    /// Person id to index into `g.persons`.
    idx: Map<i64, u32>,
    /// Undirected friendships (both directions of each `knows` row, in
    /// raw order), by person index.
    adj: Vec<Vec<u32>>,
    places: Map<i32, Place>,
    /// Tag id to `(name, TypeTagClassId)`.
    tags: Vec<(i32, Box<str>, i32)>,
    tag_of: Map<i32, usize>,
    /// Tag class id to name.
    classes: Map<i32, Box<str>>,
    /// Company id to name (organisations of type `Company`).
    companies: Map<i64, Box<str>>,
    threads: usize,
}

impl Ctx {
    fn new(g: Part, k: Counts, statics: &StaticCols, threads: usize) -> Self {
        let idx: Map<i64, u32> = g
            .persons
            .iter()
            .enumerate()
            .map(|(i, p)| (p.id, i as u32))
            .collect();
        let mut adj = vec![Vec::new(); g.persons.len()];
        for e in &g.knows {
            let (a, b) = (idx[&e[0]], idx[&e[1]]);
            adj[a as usize].push(b);
            adj[b as usize].push(a);
        }
        let (org, place, tag, class) = statics;
        let (place_id, place_name, _, place_kind, part_of) = &place.values;
        let places = (0..place_id.len())
            .map(|j| {
                let p = Place {
                    name: place_name.get(j).into(),
                    kind: place_kind.get(j).into(),
                    part_of: part_of.valid.get(j).then(|| part_of.values[j]),
                };
                (place_id[j], p)
            })
            .collect();
        let (tag_id, tag_name, _, tag_class) = &tag.values;
        let tags: Vec<(i32, Box<str>, i32)> = (0..tag_id.len())
            .map(|j| (tag_id[j], tag_name.get(j).into(), tag_class[j]))
            .collect();
        let tag_of = tags.iter().enumerate().map(|(i, t)| (t.0, i)).collect();
        let (class_id, class_name, _, _) = &class.values;
        let classes = (0..class_id.len())
            .map(|j| (class_id[j], class_name.get(j).into()))
            .collect();
        let (org_id, org_kind, org_name, _, _) = &org.values;
        let companies = (0..org_id.len())
            .filter(|&j| org_kind.get(j) == "Company")
            .map(|j| (org_id[j] as i64, org_name.get(j).into()))
            .collect();
        Ctx {
            g,
            k,
            idx,
            adj,
            places,
            tags,
            tag_of,
            classes,
            companies,
            threads,
        }
    }

    fn of_kind(&self, id: i32, kind: &str) -> Option<&Place> {
        self.places.get(&id).filter(|p| &*p.kind == kind)
    }

    /// A person's city and its country, if both are what they should be.
    fn city_country(&self, p: &Person) -> Option<(i32, &Place, i32, &Place)> {
        let city = self.of_kind(p.city, "City")?;
        let cid = city.part_of?;
        let country = self.of_kind(cid, "Country")?;
        Some((p.city, city, cid, country))
    }

    fn tag(&self, id: i32) -> Option<&(i32, Box<str>, i32)> {
        self.tag_of.get(&id).map(|&i| &self.tags[i])
    }

    /// Messages per tag (`tagNumMessages` before sorting).
    fn tag_messages(&self) -> Map<i32, i64> {
        let mut m: Map<i32, i64> = Map::default();
        for (&(_, tag), &n) in &self.k.day_tag {
            *m.entry(tag).or_default() += n;
        }
        m
    }
}

// ---- output helpers ----

/// Spark's `orderBy(frequency.desc, by.asc...)`: `(by, frequency)` pairs
/// by frequency (nulls last), then by the grouping columns (nulls first,
/// as `Option`'s order has them).
fn frequency<K: Ord>(rows: &mut [(K, Option<i64>)]) {
    rows.sort_by(|(ka, fa), (kb, fb)| {
        let desc = match (fa, fb) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
            (x, y) => y.cmp(x),
        };
        desc.then_with(|| ka.cmp(kb))
    });
}

/// `(key, count)` pairs as frequency rows, written to `out`: `row(k, n)`
/// gives the row, or `None` to drop the group (an inner join that
/// misses). The rows must order as `K` does (they start with `K`'s
/// values, the rest are functions of them), so the pairs are sorted as
/// they are.
fn counted<K: Copy + Ord, R>(
    out: &mut impl Sink<R>,
    m: impl IntoIterator<Item = (K, i64)>,
    row: impl Fn(K, i64) -> Option<R>,
) {
    let mut m: Vec<(K, i64)> = m.into_iter().collect();
    m.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    for (k, n) in m {
        if let Some(r) = row(k, n) {
            out.push(r);
        }
    }
}

/// Group `items` by key and count them.
fn group_count<K: Hash + Eq>(items: impl IntoIterator<Item = K>) -> Map<K, i64> {
    let mut m = Map::default();
    for k in items {
        *m.entry(k).or_default() += 1;
    }
    m
}

/// Spark's `sum` over a group: null if every value is null (or there are
/// none).
fn sum(vals: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    vals.into_iter().fold(None, |acc, v| match (acc, v) {
        (a, None) => a,
        (None, Some(x)) => Some(x),
        (Some(a), Some(x)) => Some(a + x),
    })
}

// ---- the tables ----

/// `(id, name, frequency)`: a count per place, tag, tag class or company.
pub type NamedCountRow = (i32, String, i64);
pub type CityPairsRow = (i32, i32, String, String, i32, i32, String, String, i64);
pub type CountryPairsRow = (i32, i32, String, String, i64);
/// `(creationDay, id, name, frequency)`.
pub type DayNamedCountRow = (Ts, i32, String, i64);
/// `(value, frequency)` of a string.
pub type StrCountRow = (String, i64);
/// `(Person1Id, Person2Id)` and both persons' dates.
pub type HopsRow = (i64, i64, Ts, Ts, Ts, Ts);
/// `(id, creationDate, deletionDate, frequency)` of a person.
pub type PersonCountRow = (i64, Ts, Ts, i64);
/// A person, its direct count, and its friends' and friends' friends' sums.
pub type FofRow = (i64, Ts, Ts, i64, Option<i64>, Option<i64>);
/// `(Person1Id, creationDate, deletionDate, numDirectTags, numFriendTags)`:
/// the id is null for the group of persons without interests.
pub type FriendTagsRow = (Option<i64>, Ts, Ts, i64, Option<i64>);
/// `(PersonId, Component, count)`.
pub type ComponentRow = (i64, i64, i64);
/// An edge's two ids and its creation and deletion days.
pub type EdgeDaysRow = (i64, i64, Ts, Ts);

/// The 34 factor tables, in [`FACTOR_TABLES`] order (nested to stay
/// within a tuple's 12 fields).
pub type Factors = (
    (
        Children<NamedCountRow>,
        Children<CityPairsRow>,
        Children<NamedCountRow>,
        Children<(i64, String, i64)>,
        Children<NamedCountRow>,
        Children<CountryPairsRow>,
        Children<(Ts, i64, i64)>,
        Children<DayNamedCountRow>,
        Children<DayNamedCountRow>,
        Children<(Ts, i64)>,
        Children<StrCountRow>,
        Children<(i32, i64)>,
    ),
    (
        Children<HopsRow>,
        Children<HopsRow>,
        Children<(i64, Ts, Ts)>,
        Children<(i64, String, i32, Ts, Ts)>,
        Children<StrCountRow>,
        Children<ComponentRow>,
        Children<EdgeDaysRow>,
        Children<PersonCountRow>,
        Children<FofRow>,
        Children<FofRow>,
        Children<FofRow>,
        Children<FofRow>,
    ),
    (
        Children<FriendTagsRow>,
        Children<PersonCountRow>,
        Children<FofRow>,
        Children<EdgeDaysRow>,
        Children<EdgeDaysRow>,
        Children<ComponentRow>,
        Children<NamedCountRow>,
        Children<NamedCountRow>,
        Children<NamedCountRow>,
        Children<NamedCountRow>,
    ),
);

/// The columns of [`Factors`].
pub type FactorCols = Columns<Factors>;

const PERSON_FOF: [&str; 3] = ["Person1Id", "creationDate", "deletionDate"];
const HOPS: &[&str] = &[
    "Person1Id",
    "Person2Id",
    "Person1CreationDate",
    "Person1DeletionDate",
    "Person2CreationDate",
    "Person2DeletionDate",
];
const COMPONENT: &[&str] = &["PersonId", "Component", "count"];
const ID_NAME_FREQ: &[&str] = &["id", "name", "frequency"];
const PERSON_FREQ: &[&str] = &["Person1Id", "creationDate", "deletionDate", "frequency"];

/// Every factor table's name and column names, by name order of the
/// datagen's output.
pub const FACTOR_TABLES: [(&str, &[&str]); 34] = [
    ("cityNumPersons", ID_NAME_FREQ),
    (
        "cityPairsNumFriends",
        &[
            "city1Id",
            "city2Id",
            "city1Name",
            "city2Name",
            "country1Id",
            "country2Id",
            "country1Name",
            "country2Name",
            "frequency",
        ],
    ),
    ("companyNumEmployees", ID_NAME_FREQ),
    (
        "countryNumMessages",
        &["LocationCountryId", "name", "frequency"],
    ),
    ("countryNumPersons", ID_NAME_FREQ),
    (
        "countryPairsNumFriends",
        &[
            "country1Id",
            "country2Id",
            "country1Name",
            "country2Name",
            "frequency",
        ],
    ),
    (
        "creationDayAndLengthCategoryNumMessages",
        &["creationDay", "lengthCategory", "frequency"],
    ),
    (
        "creationDayAndTagClassNumMessages",
        &["creationDay", "id", "name", "frequency"],
    ),
    (
        "creationDayAndTagNumMessages",
        &["creationDay", "id", "name", "frequency"],
    ),
    ("creationDayNumMessages", &["creationDay", "frequency"]),
    ("languageNumPosts", &["language", "frequency"]),
    ("lengthNumMessages", &["length", "frequency"]),
    ("people2Hops", HOPS),
    ("people4Hops", HOPS),
    ("personDays", &["id", "creationDay", "deletionDay"]),
    (
        "personDisjointEmployerPairs",
        &[
            "person2id",
            "companyName",
            "companyId",
            "person2creationDate",
            "person2deletionDate",
        ],
    ),
    ("personFirstNames", &["firstName", "frequency"]),
    ("personKnowsPersonConnected", COMPONENT),
    (
        "personKnowsPersonDays",
        &["Person1Id", "Person2Id", "creationDay", "deletionDay"],
    ),
    ("personLikesNumMessages", PERSON_FREQ),
    (
        "personNumFriendOfFriendComments",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numDirectComments",
            "numFriendComments",
            "numFriendOfFriendComments",
        ],
    ),
    (
        "personNumFriendOfFriendCompanies",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numDirectCompanies",
            "numFriendCompanies",
            "numFriendOfFriendCompanies",
        ],
    ),
    (
        "personNumFriendOfFriendForums",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numDirectForums",
            "numFriendForums",
            "numFriendOfFriendForums",
        ],
    ),
    (
        "personNumFriendOfFriendPosts",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numDirectPosts",
            "numFriendPosts",
            "numFriendOfFriendPosts",
        ],
    ),
    (
        "personNumFriendTags",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numDirectTags",
            "numFriendTags",
        ],
    ),
    (
        "personNumFriends",
        &["id", "creationDate", "deletionDate", "frequency"],
    ),
    (
        "personNumFriendsOfFriendsOfFriends",
        &[
            PERSON_FOF[0],
            PERSON_FOF[1],
            PERSON_FOF[2],
            "numFriends",
            "numFriendsOfFriends",
            "numFriendsOfFriendsOfFriends",
        ],
    ),
    (
        "personStudyAtUniversityDays",
        &["PersonId", "UniversityId", "creationDay", "deletionDay"],
    ),
    (
        "personWorkAtCompanyDays",
        &["PersonId", "CompanyId", "creationDay", "deletionDay"],
    ),
    ("sameUniversityConnected", COMPONENT),
    ("tagClassNumMessages", ID_NAME_FREQ),
    ("tagClassNumTags", ID_NAME_FREQ),
    ("tagNumMessages", &["tagId", "tagName", "frequency"]),
    ("tagNumPersons", ID_NAME_FREQ),
];

/// The factor tables, as [`FACTOR_TABLES`] names them: their columns are
/// the kinds of [`FactorCols`]' buffers.
pub static FACTOR: LazyLock<Vec<TableDef>> = LazyLock::new(|| {
    rows_of(FactorCols::default())
        .iter()
        .zip(FACTOR_TABLES)
        .map(|(r, (name, names))| TableDef::of(name, names, &r.cols))
        .collect()
});

/// The tables whose row order is not defined by the datagen (a `distinct`,
/// or a graph's connected components): compare them as sets.
pub const UNORDERED: [&str; 3] = [
    "personDisjointEmployerPairs",
    "personKnowsPersonConnected",
    "sameUniversityConnected",
];

/// Write every table of `c` into `out`, on `threads` threads (0 = all
/// cores).
fn tables_into(c: &Ctx, out: &mut FactorCols, threads: usize) {
    let (
        (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11),
        (b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11),
        (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
    ) = out;
    let jobs: Vec<Box<dyn FnOnce() + Send + '_>> = vec![
        // the n-hop tables, the slowest, start first
        Box::new(|| hops(c, &mut b0.values, 2)),
        Box::new(|| hops(c, &mut b1.values, 4)),
        Box::new(|| city_num_persons(c, &mut a0.values)),
        Box::new(|| city_pairs(c, &mut a1.values)),
        Box::new(|| company_employees(c, &mut a2.values)),
        Box::new(|| country_num_messages(c, &mut a3.values)),
        Box::new(|| country_num_persons(c, &mut a4.values)),
        Box::new(|| country_pairs(c, &mut a5.values)),
        Box::new(|| day_len_messages(c, &mut a6.values)),
        Box::new(|| day_class_messages(c, &mut a7.values)),
        Box::new(|| day_tag_messages(c, &mut a8.values)),
        Box::new(|| day_messages(c, &mut a9.values)),
        Box::new(|| language_posts(c, &mut a10.values)),
        Box::new(|| len_messages(c, &mut a11.values)),
        Box::new(|| person_days(c, &mut b2.values)),
        Box::new(|| disjoint_employers(c, &mut b3.values)),
        Box::new(|| first_names(c, &mut b4.values)),
        Box::new(|| knows_connected(c, &mut b5.values)),
        Box::new(|| days(&mut b6.values, &c.g.knows)),
        Box::new(|| likes_messages(c, &mut b7.values)),
        Box::new(|| fof_comments(c, &mut b8.values)),
        Box::new(|| fof_companies(c, &mut b9.values)),
        Box::new(|| fof_forums(c, &mut b10.values)),
        Box::new(|| fof_posts(c, &mut b11.values)),
        Box::new(|| friend_tags(c, &mut c0.values)),
        Box::new(|| person_num_friends(c, &mut c1.values)),
        Box::new(|| friends_3(c, &mut c2.values)),
        Box::new(|| days(&mut c3.values, &c.g.study)),
        Box::new(|| days(&mut c4.values, &c.g.work)),
        Box::new(|| same_university(c, &mut c5.values)),
        Box::new(|| class_messages(c, &mut c6.values)),
        Box::new(|| class_tags(c, &mut c7.values)),
        Box::new(|| tag_messages(c, &mut c8.values)),
        Box::new(|| tag_persons(c, &mut c9.values)),
    ];
    run_all(jobs, threads);
}

/// Run every job, on up to `threads` threads (0 = all cores).
fn run_all(jobs: Vec<Box<dyn FnOnce() + Send + '_>>, threads: usize) {
    let threads = resolve_threads(threads).min(jobs.len()).max(1);
    // in the order given
    let queue = Mutex::new(VecDeque::from(jobs));
    let next = || queue.lock().expect("jobs").pop_front();
    thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                while let Some(job) = next() {
                    job();
                }
            });
        }
    });
}

fn ts(ms: i64) -> Ts {
    Timestamp(ms)
}

fn country_num_persons(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let m = group_count(
        c.g.persons
            .iter()
            .filter_map(|p| Some(c.city_country(p)?.2)),
    );
    counted(out, m, |id, n| Some((id, &*c.places[&id].name, n)));
}

fn city_num_persons(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let m = group_count(
        c.g.persons
            .iter()
            .filter(|p| c.of_kind(p.city, "City").is_some())
            .map(|p| p.city),
    );
    counted(out, m, |id, n| Some((id, &*c.places[&id].name, n)));
}

fn country_num_messages(c: &Ctx, out: &mut Columns<(i64, String, i64)>) {
    counted(out, c.k.country.iter().map(|(&k, &n)| (k, n)), |id, n| {
        let p = c.of_kind(i32::try_from(id).ok()?, "Country")?;
        Some((id, &*p.name, n))
    });
}

/// Both directions of every friendship, as person indices.
fn undirected(c: &Ctx) -> impl Iterator<Item = (u32, u32)> + '_ {
    c.g.knows.iter().flat_map(|e| {
        let (a, b) = (c.idx[&e[0]], c.idx[&e[1]]);
        [(a, b), (b, a)]
    })
}

fn city_pairs(c: &Ctx, out: &mut Columns<CityPairsRow>) {
    let m = group_count(undirected(c).filter_map(|(a, b)| {
        let x = c.city_country(&c.g.persons[a as usize])?;
        let y = c.city_country(&c.g.persons[b as usize])?;
        (x.0 < y.0).then_some((x.0, y.0))
    }));
    counted(out, m, |(a, b), n| {
        let (ca, cb) = (&c.places[&a], &c.places[&b]);
        let (na, nb) = (ca.part_of?, cb.part_of?);
        Some((
            a,
            b,
            &*ca.name,
            &*cb.name,
            na,
            nb,
            &*c.places[&na].name,
            &*c.places[&nb].name,
            n,
        ))
    });
}

fn country_pairs(c: &Ctx, out: &mut Columns<CountryPairsRow>) {
    let m = group_count(undirected(c).filter_map(|(a, b)| {
        let x = c.city_country(&c.g.persons[a as usize])?;
        let y = c.city_country(&c.g.persons[b as usize])?;
        (x.2 < y.2).then_some((x.2, y.2))
    }));
    counted(out, m, |(a, b), n| {
        Some((a, b, &*c.places[&a].name, &*c.places[&b].name, n))
    });
}

fn day_messages(c: &Ctx, out: &mut Columns<(Ts, i64)>) {
    counted(out, c.k.day.iter().map(|(&k, &n)| (k, n)), |d, n| {
        Some((ts(d), n))
    });
}

fn day_class_messages(c: &Ctx, out: &mut Columns<DayNamedCountRow>) {
    let mut m: Map<(i64, i32), i64> = Map::default();
    for (&(d, tag), &n) in &c.k.day_tag {
        if let Some(t) = c.tag(tag) {
            *m.entry((d, t.2)).or_default() += n;
        }
    }
    counted(out, m, |(d, class), n| {
        Some((ts(d), class, &**c.classes.get(&class)?, n))
    });
}

fn day_tag_messages(c: &Ctx, out: &mut Columns<DayNamedCountRow>) {
    counted(
        out,
        c.k.day_tag.iter().map(|(&k, &n)| (k, n)),
        |(d, tag), n| Some((ts(d), tag, &*c.tag(tag)?.1, n)),
    );
}

fn day_len_messages(c: &Ctx, out: &mut Columns<(Ts, i64, i64)>) {
    counted(
        out,
        c.k.day_len.iter().map(|(&k, &n)| (k, n)),
        |(d, l), n| Some((ts(d), l, n)),
    );
}

fn len_messages(c: &Ctx, out: &mut Columns<(i32, i64)>) {
    counted(out, c.k.len.iter().map(|(&k, &n)| (k, n)), |l, n| {
        Some((l, n))
    });
}

fn tag_messages(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    counted(out, c.tag_messages(), |tag, n| {
        Some((tag, &*c.tag(tag)?.1, n))
    });
}

fn class_messages(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let mut m: Map<i32, i64> = Map::default();
    for (tag, n) in c.tag_messages() {
        if let Some(t) = c.tag(tag) {
            *m.entry(t.2).or_default() += n;
        }
    }
    counted(out, m, |class, n| {
        Some((class, &**c.classes.get(&class)?, n))
    });
}

/// A frequency table of strings, `(value, frequency)`.
fn str_frequency<'a>(out: &mut Columns<StrCountRow>, m: impl IntoIterator<Item = (&'a str, i64)>) {
    let mut rows: Vec<(&str, Option<i64>)> = m.into_iter().map(|(s, n)| (s, Some(n))).collect();
    frequency(&mut rows);
    for (s, n) in rows {
        out.push((s, n.expect("a count")));
    }
}

fn language_posts(c: &Ctx, out: &mut Columns<StrCountRow>) {
    str_frequency(out, c.k.lang.iter().map(|(l, &n)| (&**l, n)));
}

fn tag_persons(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let m = group_count(c.g.interests.iter().map(|&(_, tag)| tag));
    counted(out, m, |tag, n| Some((tag, &*c.tag(tag)?.1, n)));
}

fn class_tags(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let m = group_count(c.tags.iter().map(|t| t.2));
    counted(out, m, |class, n| {
        Some((class, &**c.classes.get(&class)?, n))
    });
}

fn company_employees(c: &Ctx, out: &mut Columns<NamedCountRow>) {
    let m = group_count(c.g.work.iter().map(|w| w[1]));
    counted(out, m, |id, n| {
        Some((id as i32, &**c.companies.get(&id)?, n))
    });
}

fn first_names(c: &Ctx, out: &mut Columns<StrCountRow>) {
    let mut m: Map<&str, i64> = Map::default();
    for p in &c.g.persons {
        *m.entry(&p.first_name).or_default() += 1;
    }
    str_frequency(out, m);
}

/// A frequency table of `(id, creationDate, deletionDate, count)`, one row
/// per person.
fn person_frequency(
    c: &Ctx,
    out: &mut Columns<PersonCountRow>,
    count: impl Fn(usize, &Person) -> i64,
) {
    let mut rows: Vec<((i64, i64, i64), Option<i64>)> =
        c.g.persons
            .iter()
            .enumerate()
            .map(|(i, p)| ((p.id, p.created, p.deleted), Some(count(i, p))))
            .collect();
    frequency(&mut rows);
    for ((id, created, deleted), n) in rows {
        out.push((id, ts(created), ts(deleted), n.expect("a count")));
    }
}

fn person_num_friends(c: &Ctx, out: &mut Columns<PersonCountRow>) {
    person_frequency(c, out, |i, _| c.adj[i].len() as i64);
}

fn likes_messages(c: &Ctx, out: &mut Columns<PersonCountRow>) {
    person_frequency(c, out, |_, p| c.k.likes.get(&p.id).copied().unwrap_or(0));
}

/// The friend-of-friend pattern: per person its direct count, the sum of
/// its friends' direct counts and the sum of its friends' friend sums
/// (sums over no friends are null).
fn friends_of_friends(c: &Ctx, out: &mut Columns<FofRow>, direct: impl Fn(usize, &Person) -> i64) {
    let d: Vec<i64> =
        c.g.persons
            .iter()
            .enumerate()
            .map(|(i, p)| direct(i, p))
            .collect();
    let f1: Vec<Option<i64>> = c
        .adj
        .iter()
        .map(|fs| sum(fs.iter().map(|&f| Some(d[f as usize]))))
        .collect();
    let mut rows: Vec<_> =
        c.g.persons
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let f2 = sum(c.adj[i].iter().map(|&f| f1[f as usize]));
                ((p.id, p.created, p.deleted, d[i], f1[i]), f2)
            })
            .collect();
    frequency(&mut rows);
    for ((id, created, deleted, d, f1), f2) in rows {
        out.push((id, ts(created), ts(deleted), d, f1, f2));
    }
}

fn friends_3(c: &Ctx, out: &mut Columns<FofRow>) {
    friends_of_friends(c, out, |i, _| c.adj[i].len() as i64);
}

fn fof_posts(c: &Ctx, out: &mut Columns<FofRow>) {
    friends_of_friends(c, out, |_, p| c.k.posts.get(&p.id).copied().unwrap_or(0));
}

fn fof_comments(c: &Ctx, out: &mut Columns<FofRow>) {
    friends_of_friends(c, out, |_, p| c.k.comments.get(&p.id).copied().unwrap_or(0));
}

fn fof_forums(c: &Ctx, out: &mut Columns<FofRow>) {
    friends_of_friends(c, out, |_, p| c.k.forums.get(&p.id).copied().unwrap_or(0));
}

fn fof_companies(c: &Ctx, out: &mut Columns<FofRow>) {
    let m = group_count(c.g.work.iter().map(|w| w[0]));
    friends_of_friends(c, out, |_, p| m.get(&p.id).copied().unwrap_or(0));
}

fn friend_tags(c: &Ctx, out: &mut Columns<FriendTagsRow>) {
    // grouped by (interest.PersonId, creationDate, deletionDate): a person
    // without interests has a null id, so such persons with equal dates
    // are one group, and none of them is anyone's friend in the join
    let tags = group_count(c.g.interests.iter().map(|&(p, _)| p));
    let direct = |p: &Person| tags.get(&p.id).copied();
    let mut none: Map<(i64, i64), ()> = Map::default();
    let mut rows = Vec::new();
    for (i, p) in c.g.persons.iter().enumerate() {
        match direct(p) {
            Some(n) => {
                let f = sum(c.adj[i].iter().map(|&f| direct(&c.g.persons[f as usize])));
                rows.push(((Some(p.id), p.created, p.deleted, n), f));
            }
            None => {
                none.insert((p.created, p.deleted), ());
            }
        }
    }
    for (created, deleted) in none.into_keys() {
        rows.push(((None, created, deleted, 0), None));
    }
    frequency(&mut rows);
    for ((id, created, deleted, n), f) in rows {
        out.push((id, ts(created), ts(deleted), n, f));
    }
}

fn disjoint_employers(c: &Ctx, out: &mut Columns<(i64, String, i32, Ts, Ts)>) {
    let mut sample: Vec<u32> = (0..c.g.persons.len() as u32).collect();
    sample.sort_unstable_by_key(|&i| c.g.persons[i as usize].id);
    sample.truncate(20);
    let mut works: Map<i64, Vec<i64>> = Map::default();
    for w in &c.g.work {
        works.entry(w[0]).or_default().push(w[1]);
    }
    let mut rows = BTreeSet::new();
    for &i in &sample {
        let p = &c.g.persons[i as usize];
        for &f in &c.adj[i as usize] {
            for co in works.get(&c.g.persons[f as usize].id).into_iter().flatten() {
                if let Some(name) = c.companies.get(co) {
                    rows.insert((p.id, &**name, *co as i32, p.created, p.deleted));
                }
            }
        }
    }
    for (id, name, co, created, deleted) in rows {
        out.push((id, name, co, ts(created), ts(deleted)));
    }
}

/// Connected components of `edges` over all persons: `(PersonId,
/// Component, count)`, the component being its least person id (as
/// graphx labels them), persons in raw order.
fn components(c: &Ctx, out: &mut Columns<ComponentRow>, edges: impl Iterator<Item = (u32, u32)>) {
    let n = c.g.persons.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            p[x as usize] = p[p[x as usize] as usize];
            x = p[x as usize];
        }
        x
    }
    for (a, b) in edges {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let mut min_id: Map<u32, i64> = Map::default();
    let mut size: Map<u32, i64> = Map::default();
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    for (i, &r) in roots.iter().enumerate() {
        let id = c.g.persons[i].id;
        min_id
            .entry(r)
            .and_modify(|m| *m = (*m).min(id))
            .or_insert(id);
        *size.entry(r).or_default() += 1;
    }
    out.reserve(n);
    for (i, r) in roots.iter().enumerate() {
        out.push((c.g.persons[i].id, min_id[r], size[r]));
    }
}

fn knows_connected(c: &Ctx, out: &mut Columns<ComponentRow>) {
    let edges: Vec<(u32, u32)> =
        c.g.knows
            .iter()
            .map(|e| (c.idx[&e[0]], c.idx[&e[1]]))
            .collect();
    components(c, out, edges.into_iter());
}

fn same_university(c: &Ctx, out: &mut Columns<ComponentRow>) {
    let mut study: Map<i64, Vec<&[i64; 4]>> = Map::default();
    for s in &c.g.study {
        study.entry(s[0]).or_default().push(s);
    }
    let mut edges = Vec::new();
    for e in &c.g.knows {
        for (a, b) in [(e[0], e[1]), (e[1], e[0])] {
            for s1 in study.get(&a).into_iter().flatten() {
                for s2 in study.get(&b).into_iter().flatten() {
                    let created = e[2].max(s1[2]).max(s2[2]);
                    let deleted = e[3].min(s1[3]).min(s2[3]);
                    if s1[1] == s2[1] && created < deleted {
                        edges.push((c.idx[&a], c.idx[&b]));
                    }
                }
            }
        }
    }
    components(c, out, edges.into_iter());
}

/// `(id1, id2, creationDay, deletionDay)` of every edge, in raw order.
fn days(out: &mut Columns<EdgeDaysRow>, edges: &[[i64; 4]]) {
    out.reserve(edges.len());
    for r in edges {
        out.push((r[0], r[1], ts(day(r[2])), ts(day(r[3]))));
    }
}

fn person_days(c: &Ctx, out: &mut Columns<(i64, Ts, Ts)>) {
    out.reserve(c.g.persons.len());
    for p in &c.g.persons {
        out.push((p.id, ts(day(p.created)), ts(day(p.deleted))));
    }
}

// ---- people2Hops / people4Hops ----

/// `nHops(n)` from a sample of the persons in China (country 1): pairs
/// `(s, x)` where the shortest walk from `s` to `x` has `n` steps (so `s`
/// is two steps from itself), sorted, then sampled down to ~10,000.
fn hops(c: &Ctx, out: &mut Columns<HopsRow>, n: u8) {
    let mut china: Vec<u32> = (0..c.g.persons.len() as u32)
        .filter(|&i| {
            let p = &c.g.persons[i as usize];
            c.of_kind(p.city, "City").and_then(|x| x.part_of) == Some(1)
        })
        .collect();
    china.sort_unstable_by_key(|&i| c.g.persons[i as usize].id);
    let count = china.len() as f64;
    let size = count.min(100f64.max(count / (1.0 + (count * 1e-3).exp()) * 2.0));
    let sources = bernoulli(&china, (size / count).min(1.0));

    // one BFS per source, sources in parallel: `seen[x] == k` marks `x`
    // reached from the `k`-th source
    let per = sources.len().div_ceil(64).max(1);
    let mut pairs: Vec<(i64, i64, u32)> = par_map(sources.len().div_ceil(per), c.threads, |b| {
        let n_persons = c.g.persons.len();
        let mut seen = vec![u32::MAX; n_persons];
        let (mut frontier, mut next) = (Vec::new(), Vec::new());
        let mut out = Vec::new();
        for (k, &s) in sources.iter().enumerate().skip(b * per).take(per) {
            let mark = k as u32;
            frontier.clear();
            for &f in &c.adj[s as usize] {
                if seen[f as usize] != mark {
                    seen[f as usize] = mark;
                    frontier.push(f);
                }
            }
            for _ in 2..=n {
                next.clear();
                for &x in &frontier {
                    for &f in &c.adj[x as usize] {
                        if seen[f as usize] != mark {
                            seen[f as usize] = mark;
                            next.push(f);
                        }
                    }
                }
                swap(&mut frontier, &mut next);
            }
            let sid = c.g.persons[s as usize].id;
            out.extend(
                frontier
                    .iter()
                    .map(|&x| (sid, c.g.persons[x as usize].id, s)),
            );
        }
        out
    })
    .into_iter()
    .flatten()
    .collect();
    pairs.sort_unstable();
    let pairs = bernoulli(&pairs, (10000.0 / pairs.len() as f64).min(1.0));
    out.reserve(pairs.len());
    for &(a, b, s) in &pairs {
        // the datagen joins person 2 on Person1Id too
        let p = &c.g.persons[s as usize];
        let (created, deleted) = (ts(p.created), ts(p.deleted));
        out.push((a, b, created, deleted, created, deleted));
    }
}

/// Spark's `sample(fraction, seed = 42)` of one partition (index 0): a
/// `BernoulliCellSampler` keeping the items whose `XORShiftRandom` draw is
/// below `fraction`.
fn bernoulli<T: Copy>(items: &[T], fraction: f64) -> Vec<T> {
    if fraction <= 0.0 {
        return Vec::new();
    }
    let mut rng = XorShift::new(42);
    items
        .iter()
        .copied()
        .filter(|_| rng.next_double() < fraction)
        .collect()
}

/// Spark's `org.apache.spark.util.random.XORShiftRandom`.
struct XorShift(u64);

impl XorShift {
    fn new(seed: i64) -> Self {
        XorShift(hash_seed(seed))
    }

    fn next(&mut self, bits: u32) -> u32 {
        let mut s = self.0;
        s ^= s << 21;
        s ^= s >> 35;
        s ^= s << 4;
        self.0 = s;
        (s & ((1u64 << bits) - 1)) as u32
    }

    /// `java.util.Random.nextDouble` over this `next`.
    fn next_double(&mut self) -> f64 {
        let hi = self.next(26) as u64;
        let lo = self.next(27) as u64;
        ((hi << 27) + lo) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// `XORShiftRandom.hashSeed`: MurmurHash3 of the seed's big-endian bytes,
/// twice.
fn hash_seed(seed: i64) -> u64 {
    let b = seed.to_be_bytes();
    let lo = bytes_hash(&b, 0x3c07_4a61);
    let hi = bytes_hash(&b, lo);
    ((hi as u32 as u64) << 32) | (lo as u32 as u64)
}

/// Scala's `MurmurHash3.bytesHash`.
fn bytes_hash(data: &[u8], seed: i32) -> i32 {
    fn mix_last(h: u32, k: u32) -> u32 {
        let k = k
            .wrapping_mul(0xcc9e_2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b87_3593);
        h ^ k
    }
    let mut h = seed as u32;
    let (words, tail) = data.as_chunks::<4>();
    for w in words {
        h = mix_last(h, u32::from_le_bytes(*w))
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, &b) in tail.iter().enumerate() {
            k ^= (b as u32) << (8 * i);
        }
        h = mix_last(h, k);
    }
    let mut h = h ^ data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn xorshift_matches_spark() {
        // org.apache.spark.util.random.XORShiftRandom(42).nextDouble(), Spark 3.2.1
        let mut r = XorShift::new(42);
        let want = [
            0.619189370225301,
            0.5096018842446481,
            0.8325259388871524,
            0.26322809041172357,
            0.6702867696264135,
        ];
        for w in want {
            assert_eq!(r.next_double(), w);
        }
    }

    #[test]
    fn sums_are_null_over_nothing() {
        assert_eq!(sum([]), None);
        assert_eq!(sum([None, None]), None);
        assert_eq!(sum([None, Some(2), Some(3)]), Some(5));
    }

    #[test]
    fn frequency_order() {
        let mut rows = vec![(2, None), (1, Some(5)), (-1, Some(5)), (0, Some(7))];
        frequency(&mut rows);
        assert_eq!(rows, [(0, Some(7)), (-1, Some(5)), (1, Some(5)), (2, None)]);
        let mut keys = vec![(Some(1), Some(5)), (None, Some(5))];
        frequency(&mut keys);
        assert_eq!(keys, [(None, Some(5)), (Some(1), Some(5))]);
    }

    #[test]
    fn unordered_names_exist() {
        for n in UNORDERED {
            assert!(FACTOR_TABLES.iter().any(|(t, _)| *t == n));
        }
        let names: HashSet<&str> = FACTOR_TABLES.iter().map(|(t, _)| *t).collect();
        assert_eq!(names.len(), FACTOR_TABLES.len());
        let mut sorted = FACTOR_TABLES.map(|(t, _)| t);
        sorted.sort_unstable();
        assert_eq!(sorted, FACTOR_TABLES.map(|(t, _)| t));
    }
}
